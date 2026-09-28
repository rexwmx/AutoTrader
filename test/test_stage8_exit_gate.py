# -*- coding: utf-8 -*-
"""
回归测试：hedge_trade 阶段8"盯盘 + 强平"退出门槛硬化（2026-09-22）

事故场景（用户代码审查发现）：
  旧代码用宽松版 get_positions —— 请求一失败就静默返回 {} →
  "实际对冲=0" → 落入"TWS中无实际对冲持仓，跳过数据订阅和平仓管理" →
  断开退出。TWS 一次抖动，当天全部对冲仓位就无人盯盘、不强制平仓、留到隔夜。

覆盖断言：
  G1 退出模式判定真值表：
     - 两快照成功且均空      → safe_exit（TWS 明确确认无持仓，是事实）
     - 任一账户有持仓（含单边）→ monitor（盯盘 + 收市前强平）
     - 任一快照失败（空未确认）→ defensive（绝不带仓早退）← 本次修复的关键行
  G2 工具边界不变量（失败 ≠ 空）：
     - 严格快照连续失败 → 抛 PositionSnapshotError（绝不返回 {}）
     - 严格快照成功且确实无持仓 → 返回 {}（这是事实，可被信任）
  G3 接线断言：main() 实际调用 _decide_exit_mode（防止助手函数沦为死代码），
     且旧的危险文案"跳过数据订阅和平仓管理"已从退出路径移除。

运行：python test/test_stage8_exit_gate.py
"""
import asyncio
import inspect
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

# ---- 加速：固定 sleep 立即让出 ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

import position as P                 # noqa: E402
import hedge_trade as HT             # noqa: E402


class FailingIB:
    """reqPositionsAsync 永远失败（模拟 TWS 抖动/断连）"""

    def __init__(self):
        self.calls = 0

    async def reqPositionsAsync(self):
        self.calls += 1
        raise ConnectionError("simulated TWS hiccup")


class EmptyOKIB:
    """reqPositionsAsync 健康返回空持仓列表（TWS 明确确认无持仓）"""

    async def reqPositionsAsync(self):
        return []


class FakePos:
    def __init__(self, symbol, position, avg_cost, account):
        self.symbol = symbol
        self.position = position
        self.avgCost = avg_cost
        self.account = account
        self.contract = type('C', (), {'symbol': symbol, 'conId': 1})()


class OnePosIB:
    def __init__(self, sym='X', pos=-10, acct='D1'):
        self._sym, self._pos, self._acct = sym, pos, acct

    async def reqPositionsAsync(self):
        return [FakePos(self._sym, self._pos, 10.0, self._acct)]


def check(name: str, ok: bool, detail: str = '') -> bool:
    print(f'[{"ok" if ok else "FAIL"}] {name}' + (f' — {detail}' if detail else ''))
    return ok


def scenario_g1() -> bool:
    """G1 退出模式判定真值表"""
    D = HT._decide_exit_mode
    ok = True

    # 两快照成功且均空 → safe_exit
    ok &= check('G1a 双成功双空 → safe_exit',
                D(True, {}, {}) == 'safe_exit', D(True, {}, {}))

    # 正常对称对冲持仓 → monitor
    ok &= check('G1b 对称持仓 → monitor',
                D(True, {'X': {}}, {'X': {}}) == 'monitor', D(True, {'X': {}}, {'X': {}}))

    # 单边持仓（账户1 有、账户2 空 / 反之）→ monitor（对冲不对称也要强平兜底）
    ok &= check('G1c 单边持仓(账户1) → monitor',
                D(True, {'X': {}}, {}) == 'monitor', D(True, {'X': {}}, {}))
    ok &= check('G1d 单边持仓(账户2) → monitor',
                D(True, {}, {'X': {}}) == 'monitor', D(True, {}, {'X': {}}))

    # === 本次修复的关键场景：快照失败 → 空字典不可信 ===
    ok &= check('G1e 双失败+空字典 → defensive（绝不带仓早退）',
                D(False, {}, {}) == 'defensive', D(False, {}, {}))
    ok &= check('G1f 单侧失败+另一侧有持仓 → defensive',
                D(False, {'X': {}}, {}) == 'defensive', D(False, {'X': {}}, {}))
    ok &= check('G1g 单侧失败+另一侧空 → defensive',
                D(False, {}, {'X': {}}) == 'defensive', D(False, {}, {'X': {}}))
    return ok


async def scenario_g2() -> bool:
    """G2 工具边界不变量：失败≠空（严格快照失败必抛，成功的空是事实）"""
    ok = True
    fib = FailingIB()
    raised = None
    try:
        await P.get_positions_strict_with_retry(fib, 'D1', retries=3, delay=0.001)
    except P.PositionSnapshotError as e:
        raised = e
    ok &= check('G2a 严格快照连续3次失败 → 抛 PositionSnapshotError（不返回{}）',
                raised is not None and fib.calls >= 3,
                f'raised={raised is not None}, calls={fib.calls}')

    eok = EmptyOKIB()
    res = await P.get_positions_strict_with_retry(eok, 'D1', retries=3, delay=0.001)
    ok &= check('G2b 成功且确实无持仓 → 返回 {}（TWS 明确确认，可信）',
                res == {}, repr(res))

    # 宽松版在同一失败场景下的行为（对照：这正是旧阶段8 的陷阱）
    lenient = await P.get_positions(fib, 'D1')
    ok &= check('G2c （对照）宽松版失败静默返回 {} —— 不可用于退出门槛',
                lenient == {}, repr(lenient))
    return ok


def scenario_g3() -> bool:
    """G3 接线断言：main() 真正调用 _decide_exit_mode；旧危险文案已移除"""
    src = inspect.getsource(HT.main)
    # 只在"可执行代码行"里找危险文案（解释性注释中引用旧文案是合理的）
    code_only = '\n'.join(l for l in src.splitlines()
                          if l.strip() and not l.strip().startswith('#'))
    ok = True
    ok &= check('G3a main() 调用 _decide_exit_mode（助手函数非死代码）',
                '_decide_exit_mode(' in src)
    ok &= check('G3b 旧危险退出文案已从可执行代码移除',
                '跳过数据订阅和平仓管理' not in code_only)
    ok &= check('G3c defensive 分支进入强平等待（CRITICAL 提示 + force_close 保留）',
                'defensive' in src and 'force_close_until_flat' in src
                and '防御性平仓模式' in src)
    ok &= check('G3d 阶段8 快照取数改用严格重试版',
                'get_positions_strict_with_retry' in src
                and 'PositionSnapshotError' in src)
    return ok


async def main():
    failures = []
    if not scenario_g1():
        failures.append('G1')
    if not await scenario_g2():
        failures.append('G2')
    if not scenario_g3():
        failures.append('G3')

    asyncio.sleep = _real_sleep
    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 阶段8退出门槛硬化回归测试全部通过（失败≠空持仓，所有退出路径过强平）')


# Windows 控制台默认代码页无法输出中文/emoji，统一 UTF-8
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
