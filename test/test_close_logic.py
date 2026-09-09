# -*- coding: utf-8 -*-
"""
回归测试：验证 CloseManager 盘中运行时平仓（check_runtime_conditions）触发逻辑
背景：旧版 cond2 写成 min < entry*(1-peak_pct/100)（右边恒等于 min）/ max > entry*(1+peak_pct/100)
     （右边恒等于 max），化简后恒为 False，导致运行时平仓从不触发。
运行：python test/test_close_logic.py   （在 AutoTrader 根目录下执行；任意 cwd 均可）
"""
import asyncio
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

from close import CloseManager, get_close_pct


class FakeContract:
    def __init__(self, symbol):
        self.symbol = symbol


class FakePosition:
    def __init__(self, symbol, position, avg_cost, account):
        self.contract = FakeContract(symbol)
        self.position = position
        self.avgCost = avg_cost
        self.account = account


class FakeIB:
    def __init__(self, positions):
        self._pos = positions

    def positions(self, account):
        return self._pos

    async def reqPositionsAsync(self):
        return list(self._pos)


def build_manager(short_pos=None, long_pos=None):
    manager = CloseManager(
        FakeIB([p for p in [short_pos] if p]),
        FakeIB([p for p in [long_pos] if p]),
        'D1', 'D2', 'sell.csv', 'buy.csv'
    )
    manager.start_runtime_close(delay_minutes=0)  # 立即激活监控
    manager.close_calls = []

    async def fake_close(ib, account, symbol, close_action, volume, open_price, open_action, csv_path):
        manager.close_calls.append({
            'symbol': symbol, 'action': close_action,
            'volume': volume, 'open_action': open_action,
        })
        return

    manager._execute_close_with_retry = fake_close
    return manager


def main():
    failures = []

    # ---- 表函数 sanity ----
    assert get_close_pct(0.5) == -1.0
    assert get_close_pct(2.0) == 1.0
    assert abs(get_close_pct(5.0) - 2.5) < 1e-9
    assert abs(get_close_pct(50.0) - 12.5) < 1e-9
    assert abs(get_close_pct(120.0) - 12.0) < 1e-9
    print('[ok] get_close_pct 查表行为符合预期')

    async def scenario():
        results = {}

        # ---- 用例1：做空腿应触发 ----
        # 建仓100，最低98（峰值跌2% → 保留1%），反弹至99.2
        # 平仓线 = 100*(1-0.02+0.01) = 99.0 → 99.2 >= 99.0 触发买入平空
        m = build_manager(short_pos=FakePosition('AAA', -10, 100.0, 'D1'))
        await m.check_runtime_conditions('AAA', 99.2, 98.0, 98.5)
        await asyncio.sleep(0.05)
        results['case1'] = list(m.close_calls)

        # ---- 用例2：做多腿应触发 ----
        # 建仓100，最高103（峰值涨3% → 保留1.5%），回落至101.0
        # 平仓线 = 100*(1+0.015) = 101.5 → 101.0 <= 101.5 触发卖出平多
        m = build_manager(long_pos=FakePosition('BBB', 20, 100.0, 'D2'))
        await m.check_runtime_conditions('BBB', 101.0, 99.0, 103.0)
        await asyncio.sleep(0.05)
        results['case2'] = list(m.close_calls)

        # ---- 用例3：峰值 < 1% 不应触发（两腿都不动） ----
        m = build_manager(
            short_pos=FakePosition('CCC', -10, 100.0, 'D1'),
            long_pos=FakePosition('DDD', 10, 100.0, 'D2'),
        )
        await m.check_runtime_conditions('CCC', 99.7, 99.5, 99.8)  # 峰值跌 0.5%
        await m.check_runtime_conditions('DDD', 100.2, 99.9, 100.5)  # 峰值涨 0.5%
        await asyncio.sleep(0.05)
        results['case3'] = list(m.close_calls)

        # ---- 用例4：有峰值但未回到平仓线 → 不应触发 ----
        m = build_manager(short_pos=FakePosition('EEE', -10, 100.0, 'D1'))
        await m.check_runtime_conditions('EEE', 98.5, 98.0, 98.5)  # 98.5 < 平仓线 99.0
        await asyncio.sleep(0.05)
        results['case4'] = list(m.close_calls)

        # ---- 用例5：同一腿只触发一次（防重复平仓） ----
        m = build_manager(short_pos=FakePosition('FFF', -10, 100.0, 'D1'))
        await m.check_runtime_conditions('FFF', 99.2, 98.0, 98.5)
        await m.check_runtime_conditions('FFF', 99.3, 98.0, 98.6)
        await asyncio.sleep(0.05)
        results['case5'] = list(m.close_calls)

        # ---- 用例6：两腿独立 → 同一股票做空触发不影响做多腿（反向亦然） ----
        m = build_manager(
            short_pos=FakePosition('GGG', -10, 100.0, 'D1'),
            long_pos=FakePosition('GGG', 10, 100.0, 'D2'),
        )
        # 只满足做空腿条件（价格回落，无涨幅峰值）
        await m.check_runtime_conditions('GGG', 99.2, 98.0, 98.5)
        await asyncio.sleep(0.05)
        results['case6'] = list(m.close_calls)

        # ---- 用例7：强制平仓窗口内不再触发运行时平仓 ----
        m = build_manager(short_pos=FakePosition('HHH', -10, 100.0, 'D1'))
        m.force_close_active = True
        await m.check_runtime_conditions('HHH', 99.2, 98.0, 98.5)
        await asyncio.sleep(0.05)
        results['case7'] = list(m.close_calls)
        return results

    results = asyncio.run(scenario())

    def check(name, calls, expect):
        ok = calls == expect
        print(f'[{"ok" if ok else "FAIL"}] {name}: calls={calls}')
        if not ok:
            failures.append(name)

    check('用例1 做空腿触发（buy 平 sell，10股）', results['case1'],
          [{'symbol': 'AAA', 'action': 'buy', 'volume': 10, 'open_action': 'sell'}])
    check('用例2 做多腿触发（sell 平 buy，20股）', results['case2'],
          [{'symbol': 'BBB', 'action': 'sell', 'volume': 20, 'open_action': 'buy'}])
    check('用例3 峰值<1% 均不触发', results['case3'], [])
    check('用例4 未到平仓线不触发', results['case4'], [])
    check('用例5 同腿不重复触发', results['case5'],
          [{'symbol': 'FFF', 'action': 'buy', 'volume': 10, 'open_action': 'sell'}])
    check('用例6 两腿独立（仅做空腿触发）', results['case6'],
          [{'symbol': 'GGG', 'action': 'buy', 'volume': 10, 'open_action': 'sell'}])
    check('用例7 强制平仓窗口内不触发运行时平仓', results['case7'], [])

    if failures:
        print('❌ 失败用例:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部回归测试通过：运行时平仓触发逻辑恢复正常')


if __name__ == '__main__':
    main()
