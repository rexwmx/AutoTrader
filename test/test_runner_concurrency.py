# -*- coding: utf-8 -*-
"""StrategyRunner 并发模型回归测试（2026-10-19 并发修订）

背景：旧设计中 on_bar 用一把全局 asyncio.Lock 串行所有标的，且锁内包含
平仓执行（等成交 120s + 重试 10s + 再等 120s）——一只标的平仓期间（最坏 ≈4 分钟）
其余全部标的的 bar 排队等待。修订后：
- 稳态：同标的 FIFO 门链（严格保序）、跨标的完全并发；
- 快速阶段（拉快照+策略决策）受信号量限流；慢速下单等待不占并发名额。

用例：
  C1 跨标的并发：A、B 同时触发平仓 → 两笔订单并行（总时长 ≈ max，而非串行求和）
  C2 同标的 FIFO：同标的 3 根 bar 突发 → 决策与执行都严格按 dt 顺序、无插队
  C3 A 平仓等待期间，B 的评估与执行不受阻塞（B 在 A 结束前已启动）
  C4 调平重放路径与新 bar 执行同标的串行（不并发双下单），结果回传完整
  C5 执行失败回滚语义在并发路径下保持（失败 → 回滚标记 → 下一根 bar 可重试）

运行：python test/test_runner_concurrency.py   （任意 cwd 均可）
"""
import asyncio
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import pytz
from strategy.base import Bar, PositionView, CloseSignal, BaseStrategy
import strategy_runner as SR
from strategy_runner import StrategyRunner

EST = pytz.timezone('US/Eastern')
T0 = datetime(2026, 9, 10, 10, 0, 0)
ENTRY = EST.localize(T0)


def bar(symbol: str, minute: int) -> Bar:
    dt = EST.localize(T0 + timedelta(minutes=minute))
    return Bar(symbol, dt, 100.0, 101.0, 99.0, 100.5, 1000)


class ScriptedStrategy(BaseStrategy):
    """脚本化策略：记录评估顺序；对 emit 集合中的标的每根 bar 都发一个平仓信号"""

    def __init__(self, emit: set):
        self.emit = set(emit)
        self.eval_order: list = []          # (symbol, dt) 按实际调用顺序
        self.result_order: list = []        # (symbol, account, success)

    def on_entry(self, symbol, account, price, volume, dt):
        pass

    def get_display_state(self, symbol):
        return {}

    def on_bar(self, bar, pos):
        self.eval_order.append((bar.symbol, bar.dt))
        if bar.symbol not in self.emit:
            return []
        return [CloseSignal(symbol=bar.symbol, account='account2',
                            side='sell', volume=10, open_action='buy',
                            reason=f'test-{bar.symbol}', strategy='test')]

    def on_execution_result(self, sig, success):
        self.result_order.append((sig.symbol, sig.account, bool(success)))


class TimingCloseManager:
    """记录每笔订单的 (开始, 结束) 单调时刻，支持按标的设置执行耗时/失败"""

    def __init__(self, delays=None, fail_symbols=None):
        self.delays = delays or {}          # symbol -> 秒
        self.fail = set(fail_symbols or [])
        self.spans: list = []               # (symbol, start, end)
        self.result = True

    async def run_close_signal(self, ib, account, symbol, close_action, volume,
                               open_price, open_action, csv_path, **kwargs):
        start = time.monotonic()
        await asyncio.sleep(self.delays.get(symbol, 0.0))
        end = time.monotonic()
        self.spans.append({'symbol': symbol, 'start': start, 'end': end,
                           'account': account, 'action': close_action})
        return symbol not in self.fail


def build_runner(strategy, fcm, snap2=None):
    runner = StrategyRunner(
        strategy=strategy,
        ib1=object(), ib2=object(),
        account1='D1', account2='D2',
        sell_csv_path='sell.csv', buy_csv_path='buy.csv',
        close_manager=fcm,
        max_concurrent_evals=8,
    )
    runner.active = True
    runner.activate_at = None        # 直接激活（_activation_done=True，无过渡阶段）

    async def fake_get_positions(ib, account=''):
        return dict((snap2 or {}) if account == 'D2' else {})

    SR.get_positions = fake_get_positions
    return runner


def main():
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

    snap2 = {'A': {'position': 10, 'avgCost': 100.0},
             'B': {'position': 10, 'avgCost': 100.0},
             'X': {'position': 10, 'avgCost': 100.0}}

    # ---- C1 跨标的并发：A、B 同时触发 → 两笔订单并行执行 ----
    strat = ScriptedStrategy({'A', 'B'})
    fcm = TimingCloseManager(delays={'A': 0.7, 'B': 0.7})
    runner = build_runner(strat, fcm, snap2)
    runner.entry_times['A'] = ENTRY
    runner.entry_times['B'] = ENTRY

    async def c1():
        t0 = time.monotonic()
        await asyncio.gather(
            runner.on_bar(bar('A', 1)),
            runner.on_bar(bar('B', 1)),
        )
        return time.monotonic() - t0

    elapsed = asyncio.run(c1())
    a_span = next(s for s in fcm.spans if s['symbol'] == 'A')
    b_span = next(s for s in fcm.spans if s['symbol'] == 'B')
    overlap = min(a_span['end'], b_span['end']) - max(a_span['start'], b_span['start'])
    check('C1 跨标的并发: A/B 平仓订单并行（重叠>0, 总时长≈max 而非求和）',
          len(fcm.spans) == 2 and overlap > 0.3 and elapsed < 1.3,
          detail=f'overlap={overlap:.2f}s elapsed={elapsed:.2f}s '
                 f'A=[{a_span["start"]:.2f},{a_span["end"]:.2f}] B=[{b_span["start"]:.2f},{b_span["end"]:.2f}]')

    # ---- C2 同标的 FIFO：3 根 bar 突发，决策/执行严格按 dt 顺序 ----
    strat = ScriptedStrategy({'X'})
    fcm = TimingCloseManager(delays={'X': 0.3})
    runner = build_runner(strat, fcm, snap2)
    runner.entry_times['X'] = ENTRY

    async def c2():
        # 以"同一时刻并发到达"的方式突发 3 根 bar（任务按 dt 顺序创建）
        await asyncio.gather(
            runner.on_bar(bar('X', 1)),
            runner.on_bar(bar('X', 2)),
            runner.on_bar(bar('X', 3)),
        )

    asyncio.run(c2())
    xeval = [dt for (sym, dt) in strat.eval_order if sym == 'X']
    xspans = [s for s in fcm.spans if s['symbol'] == 'X']
    serial_ok = all(xspans[i]['end'] <= xspans[i + 1]['start'] + 1e-6
                    for i in range(len(xspans) - 1))
    check('C2 同标的FIFO: 3根bar决策/执行严格按dt顺序、无插队',
          len(xeval) == 3 and xeval[0] < xeval[1] < xeval[2]
          and len(xspans) == 3 and serial_ok,
          detail=f'eval_dt={[d.strftime("%H:%M") for d in xeval]} '
                 f'spans={[(round(s["start"],2), round(s["end"],2)) for s in xspans]}')

    # ---- C3 A 平仓等待期间 B 评估执行不受阻塞 ----
    strat = ScriptedStrategy({'A', 'B'})
    fcm = TimingCloseManager(delays={'A': 0.9, 'B': 0.05})
    strat.eval_start: list = []
    orig_on_bar = strat.on_bar

    def on_bar_rec(bar, pos):
        strat.eval_start.append((bar.symbol, time.monotonic()))
        return orig_on_bar(bar, pos)

    strat.on_bar = on_bar_rec
    runner = build_runner(strat, fcm, snap2)
    runner.entry_times['A'] = ENTRY
    runner.entry_times['B'] = ENTRY

    async def c3():
        await asyncio.gather(runner.on_bar(bar('A', 1)), runner.on_bar(bar('B', 1)))

    asyncio.run(c3())
    a_span = next(s for s in fcm.spans if s['symbol'] == 'A')
    b_span = next(s for s in fcm.spans if s['symbol'] == 'B')
    b_eval_t = dict((s, t) for s, t in strat.eval_start)['B']
    check('C3 A平仓等待期间B不受阻塞: B在A订单开始/结束前已完成评估',
          b_eval_t <= a_span['start'] + 0.25,
          detail=f'b_eval={b_eval_t:.3f} a_start={a_span["start"]:.3f} a_end={a_span["end"]:.3f}')

    # ---- C4 调平重放与新 bar 执行同标的串行（不并发双下单） ----
    strat = ScriptedStrategy({'X'})
    fcm = TimingCloseManager(delays={'X': 0.4})
    runner = build_runner(strat, fcm, snap2)
    runner.entry_times['X'] = ENTRY

    async def c4():
        runner.pause_symbols(['X'])
        await runner.on_bar(bar('X', 1))            # 信号排队（调平中不执行）
        pending = len(runner._paused_pending)
        runner.unpause_symbols(['X'])
        await asyncio.gather(
            runner.replay_paused_signals(),          # 重放执行（0.4s）
            runner.on_bar(bar('X', 2)),              # 新 bar 同时评估并触发执行
        )
        return pending

    pending = asyncio.run(c4())
    xspans = sorted([s for s in fcm.spans if s['symbol'] == 'X'], key=lambda s: s['start'])
    no_overlap = len(xspans) == 2 and xspans[0]['end'] <= xspans[1]['start'] + 1e-6
    check('C4 重放与新bar同标的执行严格串行（pending入队、2笔订单不重叠、2次回传）',
          pending == 1 and no_overlap and len(strat.result_order) == 2
          and all(r[2] for r in strat.result_order),
          detail=f'pending={pending} spans={[(round(s["start"],2), round(s["end"],2)) for s in xspans]} results={strat.result_order}')

    # ---- C5 执行失败回滚：失败信号回滚去重标记后，下一根 bar 可再触发 ----
    strat = ScriptedStrategy({'X'})
    fcm = TimingCloseManager(fail_symbols={'X'})
    runner = build_runner(strat, fcm, snap2)
    runner.entry_times['X'] = ENTRY

    async def c5():
        await runner.on_bar(bar('X', 1))
        await runner.on_bar(bar('X', 2))

    asyncio.run(c5())
    check('C5 失败回滚: 两根bar均尝试执行且结果均回传失败（去重标记已回滚）',
          len(fcm.spans) == 2 and len(strat.result_order) == 2
          and all(not r[2] for r in strat.result_order),
          detail=f'spans={len(fcm.spans)} results={strat.result_order}')

    # ---- 汇总 ----
    if failures:
        print('❌ 失败用例:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部通过：跨标的并发 / 同标的FIFO / 慢等待不拖累他标的 / 重放串行 / 失败回滚 均正常')


if __name__ == '__main__':
    main()
