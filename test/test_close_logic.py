# -*- coding: utf-8 -*-
"""
回归测试：策略剥离后的盘中运行时平仓触发逻辑
背景：策略决策（DynamicTPStrategy）与执行（StrategyRunner + CloseManager）已分层：
  - 策略：Bar + PositionView → CloseSignal（不依赖 IB/CSV，可单测）
  - Runner：拉权威持仓快照 → 问策略 → 执行信号 → 回传结果（失败回滚去重标记）
原 CloseManager.check_runtime_conditions 的 7 个场景逐一平移覆盖，另新增：
  - 执行失败回滚去重标记 / 执行成功保留标记
  - Runner 让路闸门（active=False）与建仓前历史 bar 早退
  - naive 本地时间建仓成交 + aware 美东 Bar 的时区比较（防 TypeError 回归）
运行：python test/test_close_logic.py   （在 AutoTrader 根目录下执行；任意 cwd 均可）
"""
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import pytz

import strategy_runner as SR
from strategy import DynamicTPStrategy, Bar, PositionView
from strategy.close_strategy import get_close_pct
from strategy_runner import StrategyRunner

EST = pytz.timezone('US/Eastern')
T0 = datetime(2026, 9, 10, 10, 0, 0)
# naive 本地时间的建仓成交（hedge.py 风格）。选在 T0 前一天中午，
# 无论本机位于哪个时区 (UTC-12 ~ UTC+14)，其绝对时刻都必然早于 T0 当天的美东 bar，
# 保证跨时区比较结果确定。
FILL_NAIVE = datetime(2026, 9, 9, 12, 0, 0)


def bar_of(symbol: str, dt: datetime, close: float) -> Bar:
    return Bar(symbol, dt, close, close, close, close, 1000)


def run_bars(strat, symbol, pos, closes, t0):
    """按顺序喂 bar，收集全部信号"""
    sigs = []
    for i, c in enumerate(closes):
        sigs.extend(strat.on_bar(bar_of(symbol, t0 + timedelta(minutes=i + 1), c), pos))
    return sigs


class FakeCloseManager:
    def __init__(self):
        self.calls = []
        self.result = True

    async def run_close_signal(self, ib, account, symbol, close_action, volume,
                               open_price, open_action, csv_path, **kwargs):
        # **kwargs 兼容事件流扩展参数（target_lot_id / strategy / reason）
        self.calls.append({
            'ib': ib, 'account': account, 'symbol': symbol,
            'action': close_action, 'volume': volume,
            'open_price': open_price, 'open_action': open_action, 'csv': csv_path,
            **kwargs,
        })
        return self.result


def build_runner(strategy=None, snap1=None, snap2=None):
    """构造已激活（无延迟）的 runner；持仓快照用假数据注入"""
    strategy = strategy or DynamicTPStrategy()
    fcm = FakeCloseManager()
    runner = StrategyRunner(
        strategy=strategy,
        ib1=object(), ib2=object(),
        account1='D1', account2='D2',
        sell_csv_path='sell.csv', buy_csv_path='buy.csv',
        close_manager=fcm,
    )
    runner.active = True       # 测试环境直接激活，绕过 start()/delay
    runner.activate_at = None

    calls = {'n': 0}

    async def fake_get_positions(ib, account=''):
        calls['n'] += 1
        return dict((snap1 if account == 'D1' else snap2) or {})

    SR.get_positions = fake_get_positions
    return runner, fcm, calls


def main():
    # Windows 控制台默认代码页（如 cp1252）无法输出中文/emoji，统一 UTF-8，避免测试进程崩溃
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    failures = []

    def check(name, ok, detail=''):
        print(f'[{"ok" if ok else "FAIL"}] {name}{(": " + str(detail)) if detail else ""}')
        if not ok:
            failures.append(name)

    # ---- T0: 查表函数 sanity ----
    ok0 = (get_close_pct(0.5) == -1.0 and get_close_pct(2.0) == 1.0
           and abs(get_close_pct(5.0) - 2.5) < 1e-9
           and abs(get_close_pct(50.0) - 12.5) < 1e-9
           and abs(get_close_pct(120.0) - 12.0) < 1e-9)
    check('T0 get_close_pct 查表行为符合预期', ok0)

    # ---- S1: 做空腿触发（buy 平空）----
    # 建仓100，最低98（峰值跌2% → 保留1%），平仓线 100*(1-0.02+0.01)=99.0，反弹至99.2触发
    s = DynamicTPStrategy()
    s.on_entry('AAA', 'account1', 100.0, 10, T0)
    sigs = run_bars(s, 'AAA', PositionView('AAA', -10, 100.0, 0, 0.0), [98.0, 98.0, 98.5, 99.2], T0)
    check('S1 做空腿触发（buy 平 sell，10股）',
          len(sigs) == 1 and sigs[0].side == 'buy' and sigs[0].account == 'account1'
          and sigs[0].volume == 10 and sigs[0].open_action == 'sell', len(sigs))

    # ---- S2: 做多腿触发（sell 平多）----
    # 建仓100，最高103（峰值涨3% → 保留1.5%），平仓线 101.5，回落至101.0触发
    s = DynamicTPStrategy()
    s.on_entry('BBB', 'account2', 100.0, 20, T0)
    sigs = run_bars(s, 'BBB', PositionView('BBB', 0, 0.0, 20, 100.0), [101.0, 103.0, 102.0, 101.0], T0)
    check('S2 做多腿触发（sell 平 buy，20股）',
          len(sigs) == 1 and sigs[0].side == 'sell' and sigs[0].account == 'account2'
          and sigs[0].volume == 20 and sigs[0].open_action == 'buy', len(sigs))

    # ---- S3: 峰值 < 1% 两腿均不触发 ----
    s = DynamicTPStrategy()
    s.on_entry('CCC', 'account1', 100.0, 10, T0)
    s.on_entry('DDD', 'account2', 100.0, 10, T0)
    sigs = run_bars(s, 'CCC', PositionView('CCC', -10, 100.0, 0, 0.0), [99.5, 99.4, 99.6], T0)
    sigs += run_bars(s, 'DDD', PositionView('DDD', 0, 0.0, 10, 100.0), [100.5, 100.4, 100.2], T0)
    check('S3 峰值<1% 均不触发', sigs == [])

    # ---- S4: 有峰值但未回到平仓线 → 不触发 ----
    s = DynamicTPStrategy()
    s.on_entry('EEE', 'account1', 100.0, 10, T0)
    sigs = run_bars(s, 'EEE', PositionView('EEE', -10, 100.0, 0, 0.0), [98.0, 98.5], T0)
    check('S4 未到平仓线不触发（98.5 < 线99.0）', sigs == [])

    # ---- S5: 同一腿只触发一次（防重复平仓）----
    s = DynamicTPStrategy()
    s.on_entry('FFF', 'account1', 100.0, 10, T0)
    sigs = run_bars(s, 'FFF', PositionView('FFF', -10, 100.0, 0, 0.0), [98.0, 98.0, 99.2, 99.5], T0)
    check('S5 同腿不重复触发',
          len(sigs) == 1 and sigs[0].side == 'buy' and sigs[0].volume == 10, len(sigs))

    # ---- S6: 两腿独立 → 做空触发不影响做多腿 ----
    s = DynamicTPStrategy()
    s.on_entry('GGG', 'account1', 100.0, 10, T0)
    s.on_entry('GGG', 'account2', 100.0, 10, T0)
    sigs = run_bars(s, 'GGG', PositionView('GGG', -10, 100.0, 10, 100.0), [98.0, 99.2], T0)
    check('S6 两腿独立（仅做空腿触发）',
          len(sigs) == 1 and sigs[0].side == 'buy' and sigs[0].volume == 10, len(sigs))

    # ---- S7: 执行失败回滚去重标记，下一根 bar 可重新触发 ----
    s = DynamicTPStrategy()
    s.on_entry('RRR', 'account1', 100.0, 10, T0)
    pos = PositionView('RRR', -10, 100.0, 0, 0.0)
    sigs = run_bars(s, 'RRR', pos, [98.0, 98.0, 99.2], T0)
    s.on_execution_result(sigs[0], success=False)   # 模拟执行失败
    rolled = 'RRR' not in s.closed_acc1
    again = s.on_bar(bar_of('RRR', T0 + timedelta(minutes=10), 99.6), pos)
    check('S7 执行失败回滚去重标记，下一根bar重新触发', rolled and len(again) == 1,
          f'rolled={rolled}, re_sigs={len(again)}')

    # ---- S8: 执行成功保持去重标记 ----
    s = DynamicTPStrategy()
    s.on_entry('SSS', 'account1', 100.0, 10, T0)
    pos = PositionView('SSS', -10, 100.0, 0, 0.0)
    sigs = run_bars(s, 'SSS', pos, [98.0, 98.0, 99.2], T0)
    s.on_execution_result(sigs[0], success=True)
    extra = s.on_bar(bar_of('SSS', T0 + timedelta(minutes=5), 99.9), pos)
    check('S8 执行成功保留去重标记（不重复触发）', 'SSS' in s.closed_acc1 and extra == [])

    # ---- S9: 展示字段（给实时 CSV）----
    s = DynamicTPStrategy()
    s.on_entry('ZZZ', 'account1', 100.0, 10, T0)
    s.on_entry('ZZZ', 'account2', 102.0, 10, T0 + timedelta(minutes=1))
    s.on_bar(bar_of('ZZZ', T0 + timedelta(minutes=2), 99.0),
             PositionView('ZZZ', -10, 100.0, 10, 102.0))
    d = s.get_display_state('ZZZ')
    ok9 = (d['day_min_since_entry'] == 99.0 and d['day_max_since_entry'] == 99.0
           and d['day_min_since_entry_pct'] == -1.0
           and abs(d['day_max_since_entry_pct'] - round((99.0 - 102.0) / 102.0 * 100, 4)) < 1e-12)
    d_unknown = s.get_display_state('UNKNOWN')
    ok9 = ok9 and d_unknown['day_min_since_entry'] == ''
    check('S9 展示字段（建仓后极值 + 两腿百分比 + 未知标的空值）', ok9)

    # ==================== Runner 级（策略 ↔ 执行衔接）====================
    async def runner_scenarios():
        results = {}

        # ---- R1: 端到端 + naive 本地时间建仓（时区比较回归，防 TypeError）----
        strat = DynamicTPStrategy()
        r, fcm, _ = build_runner(strategy=strat,
                                 snap1={'AAA': {'position': -10, 'avgCost': 100.0}},
                                 snap2={})
        r.on_entry('AAA', 'account1', 100.0, 10, FILL_NAIVE)   # naive 本地（hedge 风格）
        aware_ok = r.entry_times['AAA'].tzinfo is not None
        strat_ok = strat.states['AAA'].entry_time.tzinfo is not None
        await r.on_bar(bar_of('AAA', EST.localize(T0 + timedelta(minutes=1)), 98.0))
        await r.on_bar(bar_of('AAA', EST.localize(T0 + timedelta(minutes=5)), 99.2))
        c = fcm.calls[0] if fcm.calls else {}
        results['R1'] = (aware_ok and strat_ok and len(fcm.calls) == 1
                         and c.get('account') == 'D1' and c.get('action') == 'buy'
                         and c.get('volume') == 10 and c.get('open_action') == 'sell'
                         and c.get('open_price') == 100.0 and c.get('csv') == 'sell.csv')

        # ---- R2: 让路闸门 active=False → 不拉持仓、不下单（强制平仓窗口让路）----
        strat2 = DynamicTPStrategy()
        r2, fcm2, calls2 = build_runner(strategy=strat2,
                                        snap1={'BBB': {'position': -10, 'avgCost': 100.0}})
        r2.stop()   # 模拟强制平仓窗口接管
        r2.on_entry('BBB', 'account1', 100.0, 10, FILL_NAIVE)
        await r2.on_bar(bar_of('BBB', EST.localize(T0 + timedelta(minutes=1)), 98.0))
        await r2.on_bar(bar_of('BBB', EST.localize(T0 + timedelta(minutes=5)), 99.2))
        results['R2'] = (calls2['n'] == 0 and fcm2.calls == [])

        # ---- R3: 建仓前历史 bar 早退 → 不拉持仓 ----
        strat3 = DynamicTPStrategy()
        r3, fcm3, calls3 = build_runner(strategy=strat3,
                                        snap1={'CCC': {'position': -10, 'avgCost': 100.0}})
        r3.on_entry('CCC', 'account1', 100.0, 10, FILL_NAIVE)
        await r3.on_bar(bar_of('CCC', EST.localize(datetime(2026, 9, 8, 12, 0, 0)), 99.0))
        results['R3'] = (calls3['n'] == 0 and fcm3.calls == [])

        # ---- R4: 执行失败 → runner 回传结果 → 去重标记回滚 → 再次触发 ----
        strat4 = DynamicTPStrategy()
        r4, fcm4, _ = build_runner(strategy=strat4,
                                   snap1={'DDD4': {'position': -10, 'avgCost': 100.0}})
        fcm4.result = False   # 模拟平仓执行失败
        r4.on_entry('DDD4', 'account1', 100.0, 10, FILL_NAIVE)
        await r4.on_bar(bar_of('DDD4', EST.localize(T0 + timedelta(minutes=1)), 98.0))
        await r4.on_bar(bar_of('DDD4', EST.localize(T0 + timedelta(minutes=5)), 99.2))
        rolled = 'DDD4' not in strat4.closed_acc1
        await r4.on_bar(bar_of('DDD4', EST.localize(T0 + timedelta(minutes=9)), 99.6))
        results['R4'] = (rolled and len(fcm4.calls) == 2)

        # ---- R5: 展示字段经 runner 透传（realtime 拉取路径）----
        r5, _, _ = build_runner(strategy=strat, snap1={})
        d5 = r5.get_display_state('AAA')
        results['R5'] = (d5 is not None and 'day_min_since_entry' in d5)

        return results

    results = asyncio.run(runner_scenarios())
    check('R1 端到端: naive建仓+aware Bar → 触发并正确分发（无TypeError）', results['R1'])
    check('R2 让路闸门: active=False 不拉持仓不下单', results['R2'])
    check('R3 建仓前历史bar早退: 不拉持仓', results['R3'])
    check('R4 执行失败回滚后再次触发（2次下单）', results['R4'])
    check('R5 展示字段经runner透传', results['R5'])

    if failures:
        print('❌ 失败用例:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部回归测试通过：策略剥离后触发逻辑、失败回滚与让路闸门均正常')


if __name__ == '__main__':
    main()
