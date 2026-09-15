# -*- coding: utf-8 -*-
"""
回归测试：持仓快照（get_positions）与调平（reconcile）竞态修复
覆盖场景：
  P1 快照按账户过滤、剔除零仓，且不受 .positions() 幽灵缓存影响
  P2 并发调用同一实例的 reqPositionsAsync 被锁串行化，两个调用都能拿到结果
  P3 W1 幽灵缓存：账户2缓存残留旧多头 → 新代码无视幽灵，正确补买，定点收敛
  P4 W2 账户2孤儿多头 → 正确卖出超额
  P5 调平持续失败 → 3轮尝试 + 终态 CRITICAL 告警
  P6 已对称 → 不提交任何订单
  H1-H4 verify_buy_position：全量通过 / 部分成交判负 / 幽灵缓存判负 / 无持仓判负
运行：python test/test_positions_logic.py   （在 AutoTrader 根目录下执行；任意 cwd 均可）
"""
import asyncio
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import position as P
import hedge as H
from logger import get_logger

# ---- 加速：跳过调平轮间等待等固定 sleep ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

# ---- Fake 对象 ----


class FakePos:
    def __init__(self, symbol, position, avg_cost, account):
        self.symbol = symbol
        self.position = position
        self.avgCost = avg_cost
        self.account = account
        self.contract = type('C', (), {'symbol': symbol, 'conId': hash(symbol)})()


class ScriptedIB:
    """
    模拟 ib_async 关键语义：
    - reqPositionsAsync 返回脚本快照（权威结果）；
    - .positions() 缓存可带幽灵条目（stale），用于证明新代码不再读它。
    snaps 列表按调用序号索引；超出后沿用最后一个。
    """

    def __init__(self, snaps, ghost=None):
        self._snaps = list(snaps) or [[]]
        self.ghost = ghost or {}
        self.calls = 0

    async def reqPositionsAsync(self):
        self.calls += 1
        await _real_sleep(0)
        return list(self._snaps[min(self.calls, len(self._snaps)) - 1])

    def positions(self, account=''):
        if account:
            return list(self.ghost.get(account, []).values()) if isinstance(self.ghost, dict) else self.ghost.get(account, [])
        return [p for v in self.ghost.values() for p in (v if isinstance(v, list) else v.values())]


class FakeStatus:
    def __init__(self, filled, avg):
        self.filled = filled
        self.avgFillPrice = avg
        self.status = 'Filled'


class FakeTrade:
    def __init__(self, vol, price, symbol='X'):
        self.orderStatus = FakeStatus(vol, price)
        self.fills = []
        self.contract = type('C', (), {'symbol': symbol})()
        self.order = None


# ---- 打桩：调平执行路径 ----
P.submit_buy_order = None   # placeholder, set inside each scenario
P.submit_sell_order = None
P.wait_for_trade_completion = None
P.append_trade_record = lambda record, path: True

_log = get_logger()
records = []


class _Capture(logging.Handler):
    def emit(self, r):
        records.append(r)


_cap = _Capture()
_cap.setLevel(logging.DEBUG)
_log.addHandler(_cap)


def crit(text_kw):
    return any(r.levelno >= logging.CRITICAL and text_kw in r.getMessage() for r in records)


def clear():
    records.clear()


async def main():
    failures = []

    # ---------- P1: get_positions 过滤 + 幽灵缓存隔离 ----------
    ib = ScriptedIB([[FakePos('X', -10, 100, 'D1'), FakePos('X', +9, 50, 'D2'),
                      FakePos('Z', 0, 10, 'D1')]],
                    ghost={'D1': {999: FakePos('GHOST', +7, 1, 'D1')}})
    snap = await P.get_positions(ib, 'D1')
    ok = (set(snap.keys()) == {'X'} and snap['X']['position'] == -10)
    print(f'[{"ok" if ok else "FAIL"}] P1 快照按账户过滤/剔除零仓/忽略幽灵缓存: {snap.keys()} ghost in snap={"GHOST" in snap}')
    if not ok:
        failures.append('P1')

    # ---------- P2: 并发串行化且均可完成 ----------
    class ConcurrentIB:
        def __init__(self):
            self.n = 0

        async def reqPositionsAsync(self):
            self.n += 1
            await _real_sleep(0)  # 允许并发交错
            return [FakePos('X', +1, 1, 'D1')]

    cib = ConcurrentIB()
    r1, r2 = await asyncio.gather(P.get_positions(cib, 'D1'), P.get_positions(cib, 'D1'))
    ok = (cib.n == 2 and 'X' in r1 and 'X' in r2)
    print(f'[{"ok" if ok else "FAIL"}] P2 并发调用串行化且均可完成: calls={cib.n}')
    if not ok:
        failures.append('P2')

    # ---------- 通用：打桩提交/等待 ----------
    buy_calls, sell_calls = [], []

    async def fake_buy(ib, symbol, volume):
        buy_calls.append((ib, symbol, volume))
        return FakeTrade(volume, 50.0, symbol)

    async def fake_sell(ib, symbol, volume):
        sell_calls.append((ib, symbol, volume))
        return FakeTrade(volume, 50.0, symbol)

    P.submit_buy_order = fake_buy
    P.submit_sell_order = fake_sell

    async def wait_filled(trade, timeout_seconds=120):
        return 'Filled'

    async def wait_cancelled(trade, timeout_seconds=120):
        return 'Cancelled'

    P.wait_for_trade_completion = wait_filled

    # ---------- P3: W1 幽灵缓存 → 正确补买并定点收敛 ----------
    clear()
    buy_calls.clear()
    ib1 = ScriptedIB([[FakePos('X', -10, 100, 'D1')]])
    ib2 = ScriptedIB([[], [FakePos('X', +10, 100, 'D2')]],
                     ghost={'D2': [FakePos('X', +10, 999, 'D2')]})
    await P.reconcile_positions(ib1, ib2, 'D1', 'D2')
    ok = (buy_calls == [(ib2, 'X', 10)] and not crit('调平后仍存在不一致'))
    print(f'[{"ok" if ok else "FAIL"}] P3 幽灵缓存不被误用, 补买10股, 定点收敛: buy_calls={buy_calls}')
    if not ok:
        failures.append('P3')

    # ---------- P4: 账户2孤儿多头 → 卖出超额 ----------
    clear()
    buy_calls.clear()
    sell_calls.clear()
    ib1 = ScriptedIB([[]])
    ib2 = ScriptedIB([[FakePos('X', +20, 100, 'D2')], []])
    await P.reconcile_positions(ib1, ib2, 'D1', 'D2')
    ok = (sell_calls == [(ib2, 'X', 20)] and not buy_calls)
    print(f'[{"ok" if ok else "FAIL"}] P4 孤儿多头卖出超额20股: sell_calls={sell_calls}')
    if not ok:
        failures.append('P4')

    # ---------- P5: 调平持续失败 → 3轮尝试 + 终态CRITICAL ----------
    clear()
    buy_calls.clear()
    P.wait_for_trade_completion = wait_cancelled
    ib1 = ScriptedIB([[FakePos('X', -10, 100, 'D1')]])
    ib2 = ScriptedIB([[]])
    await P.reconcile_positions(ib1, ib2, 'D1', 'D2')
    ok = (len(buy_calls) == 3 and all(b == (ib2, 'X', 10) for b in buy_calls)
          and crit('调平后仍存在不一致'))
    print(f'[{"ok" if ok else "FAIL"}] P5 3轮尝试+终态CRITICAL: attempts={len(buy_calls)}, critical={crit("调平后仍存在不一致")}')
    if not ok:
        failures.append('P5')
    P.wait_for_trade_completion = wait_filled

    # ---------- P6: 已对称 → 不下单 ----------
    clear()
    buy_calls.clear()
    sell_calls.clear()
    ib1 = ScriptedIB([[FakePos('X', -10, 100, 'D1')]])
    ib2 = ScriptedIB([[FakePos('X', +10, 100, 'D2')]])
    await P.reconcile_positions(ib1, ib2, 'D1', 'D2')
    ok = (not buy_calls and not sell_calls and not crit('调平后仍存在不一致'))
    print(f'[{"ok" if ok else "FAIL"}] P6 已对称不下单: buy={buy_calls}, sell={sell_calls}')
    if not ok:
        failures.append('P6')

    # ---------- P7: 两账户对称但 CSV 单边缺开仓行（9.8 PL 事故）→ 终态补写 ----------
    # 旧代码：对称 → 无调平单 → 补写钩子（挂在调平单成交后）永不执行 → buy.csv 永久缺 PL。
    # 新代码：终态快照逐边核对 CSV，缺行即按该账户 avgCost 补写。
    import tempfile
    import pandas as pd
    from constants import TRADE_RECORD_COLUMNS

    P7_TMP = Path(tempfile.mkdtemp(prefix='pos_backfill_test_'))
    buy_p7 = P7_TMP / 'buy.csv'
    sell_p7 = P7_TMP / 'sell.csv'
    # sell.csv 有 X 的开仓行；buy.csv 空表（X 的 buy 开仓行缺失，模拟 PL）
    pd.DataFrame([{
        'datetime': '2026-09-08 09:30:16', 'code': 'X', 'exchange': 'NYSE', 'industry': 'Tech',
        'action': 'sell', 'entry_price': 17.87, 'vol': 56, 'total_cost': 1000.72, 'fund_used': 1000.72,
        'close_datetime': '', 'close_price': '', 'close_vol': '', 'close_fund': '',
        'gross_profit': '', 'profit': ''
    }]).reindex(columns=TRADE_RECORD_COLUMNS).to_csv(sell_p7, index=False, encoding='utf-8-sig')
    pd.DataFrame(columns=TRADE_RECORD_COLUMNS).to_csv(buy_p7, index=False, encoding='utf-8-sig')

    clear()
    buy_calls.clear()
    sell_calls.clear()
    ib1 = ScriptedIB([[FakePos('X', -56, 17.85, 'D1')]])
    ib2 = ScriptedIB([[FakePos('X', +56, 18.02, 'D2')]])

    # P7 走真实写盘路径（csv_writer.append_trade_record），端到端验证 CSV 内容
    import csv_writer as CW
    orig_append = P.append_trade_record
    P.append_trade_record = CW.append_trade_record
    try:
        await P.reconcile_positions(ib1, ib2, 'D1', 'D2',
                                    sell_csv_path=sell_p7, buy_csv_path=buy_p7)
    finally:
        P.append_trade_record = orig_append  # 恢复打桩，不影响后续用例

    df7 = pd.read_csv(buy_p7) if buy_p7.exists() else None
    written = (df7 is not None and not df7.empty
               and df7.iloc[0]['code'] == 'X' and df7.iloc[0]['action'] == 'buy'
               and int(float(df7.iloc[0]['vol'])) == 56
               and abs(float(df7.iloc[0]['entry_price']) - 18.02) < 1e-9)
    df_sell7 = pd.read_csv(sell_p7)
    no_sell_dup = int((df_sell7['code'] == 'X').sum()) == 1  # 已有行不得重复补写
    ok = (not buy_calls and not sell_calls and written and no_sell_dup)
    print(f'[{"ok" if ok else "FAIL"}] P7 对称持仓+buy侧缺行→补写buy.csv(账户2均价), 无重复补写: '
          f'orders={buy_calls}+{sell_calls}, written={written}, no_dup={no_sell_dup}')
    if not ok:
        failures.append('P7')

    # ---------- H1: 验证全量成交 ----------
    ib = ScriptedIB([[FakePos('X', +100, 10, 'D2')]])
    r = await H.verify_buy_position(ib, 'X', 100)
    ok = r['exists'] and r['volume'] == 100
    print(f'[{"ok" if ok else "FAIL"}] H1 全量成交判定通过: {r}')
    if not ok:
        failures.append('H1')

    # ---------- H2: 部分成交 → 判负（交给调平补差） ----------
    ib = ScriptedIB([[FakePos('X', +60, 10, 'D2')]])
    r = await H.verify_buy_position(ib, 'X', 100)
    ok = not r['exists']
    print(f'[{"ok" if ok else "FAIL"}] H2 部分成交(60<100)判负: {r}')
    if not ok:
        failures.append('H2')

    # ---------- H3: 幽灵缓存有、快照无 → 判负（旧代码会判正） ----------
    ib = ScriptedIB([[]], ghost={'D2': [FakePos('X', +100, 1, 'D2')]})
    r = await H.verify_buy_position(ib, 'X', 100)
    ok = not r['exists']
    print(f'[{"ok" if ok else "FAIL"}] H3 幽灵缓存判负: {r}')
    if not ok:
        failures.append('H3')

    # ---------- H4: 无持仓 → 判负 ----------
    ib = ScriptedIB([[]])
    r = await H.verify_buy_position(ib, 'X', 100)
    ok = not r['exists']
    print(f'[{"ok" if ok else "FAIL"}] H4 无持仓判负: {r}')
    if not ok:
        failures.append('H4')

    asyncio.sleep = _real_sleep
    _log.removeHandler(_cap)

    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部持仓逻辑回归测试通过')


# Windows 控制台默认代码页无法输出中文/emoji，统一 UTF-8，避免测试进程崩溃
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
