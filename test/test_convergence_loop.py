# -*- coding: utf-8 -*-
"""
回归测试：R1 建仓收敛循环 + P0 差额幂等补记 + R5 标的级暂停
（2026-09-21 对冲建仓超 1 分钟事故改造方案的验收新增项）

覆盖场景：
  C1 收敛循环"同轮超平"分支（mock 快照 0→480→240 序列）
     —— KEEL 型：补买成交 → 核验见 480（超目标 240）→ 当轮立即卖出超额 240 → 240 收敛
        账实一致断言：无超买/超卖残留；每笔持仓均有开仓账；每次平仓均有平仓账
  C2 "原单 filled 早于持仓落账不再双买"（mock 超时后 filled 才到）
     —— MSTR 型：等待超时 → 撤单前重读原单 filled=6 → 按成交、撤单可省；
        下一轮按"最新快照 + pending 下限"双确认，绝不二次补买
  C3 标的级暂停：非暂停标的 bar 正常执行；暂停标的信号轮末重放成功；
     重放失败 → 回滚去重标记 → 下一根 bar 可再次触发
  C4 差额补记幂等：重复调用不新增 lot（含"lot 已平尽但实际仍有持仓"的 KEEL 判重盲区）
  C5 终态失败路径：持续失败 3 轮停止重试 → 收尾 CRITICAL（与 P5 行为断言一致）

运行：python test/test_convergence_loop.py   （AutoTrader 根目录下执行；任意 cwd 均可）
"""
import asyncio
import logging
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import pytz  # noqa: E402

# ---- 加速：所有固定 sleep 立即让出 ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

import position as P                    # noqa: E402
import strategy_runner as SR            # noqa: E402
from strategy import DynamicTPStrategy, Bar, PositionView  # noqa: E402
from trade_store import TradeStore      # noqa: E402
from logger import get_logger           # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix='converge_test_'))

# ---- 日志捕获 ----
_log = get_logger()
records = []


class _Capture(logging.Handler):
    def emit(self, r):
        records.append(r)


_cap = _Capture()
_cap.setLevel(logging.DEBUG)
_log.addHandler(_cap)


def crit(kw):
    return any(r.levelno >= logging.CRITICAL and kw in r.getMessage() for r in records)


def clear_log():
    records.clear()


# ---- Fake 对象 ----
class FakePos:
    def __init__(self, symbol, position, avg_cost, account):
        self.symbol = symbol
        self.position = position
        self.avgCost = avg_cost
        self.account = account
        self.contract = type('C', (), {'symbol': symbol, 'conId': hash(symbol)})()


class ScriptedIB:
    """reqPositionsAsync 按调用序号返回脚本快照（超出后沿用最后一个）"""

    def __init__(self, snaps):
        self._snaps = list(snaps) or [[]]
        self.calls = 0

    async def reqPositionsAsync(self):
        self.calls += 1
        await _real_sleep(0)
        return list(self._snaps[min(self.calls, len(self._snaps)) - 1])


class FakeOrder:
    def __init__(self, oid=777):
        self.orderId = oid


class FakeTrade:
    def __init__(self, filled, avg, symbol='X', status='Filled'):
        os_ = type('OS', (), {})()
        os_.filled = filled
        os_.avgFillPrice = avg
        os_.status = status
        self.orderStatus = os_
        self.fills = []
        self.contract = type('C', (), {'symbol': symbol})()
        self.order = FakeOrder()


# 调用记录（打桩时填充）
CALLS = {'buy': [], 'sell': [], 'cancel': 0, 'close_mcm': []}


def make_stubs(wait_result: str, buy_trade_factory, sell_trade_factory=None):
    async def fake_buy(ib, symbol, vol):
        CALLS['buy'].append((ib, symbol, vol))
        return buy_trade_factory(vol)

    async def fake_sell(ib, symbol, vol):
        CALLS['sell'].append((ib, symbol, vol))
        return (sell_trade_factory or buy_trade_factory)(vol)

    async def fake_wait(trade, timeout_seconds=120):
        return wait_result

    async def fake_cancel(ib, trade):
        CALLS['cancel'] += 1
        return True

    P.submit_buy_order = fake_buy
    P.submit_sell_order = fake_sell
    P.wait_for_trade_completion = fake_wait
    P.cancel_order = fake_cancel


def reset_calls():
    CALLS['buy'].clear()
    CALLS['sell'].clear()
    CALLS['cancel'] = 0
    CALLS['close_mcm'].clear()


EST = pytz.timezone('US/Eastern')


async def scenario_c1():
    """C1 同轮超平：快照 0→480→240（账户2），账户1 空头 240 固定。
    期望：第 1 轮内 买240成交 → 核验见480 → 同轮卖出超额240 → 收敛；
    账实一致：无超买/超卖残留，每笔持仓有开仓账，每次平仓有平仓账。"""
    reset_calls()
    clear_log()
    make_stubs('Filled',
               buy_trade_factory=lambda vol: FakeTrade(vol, 4.12, 'X'))
    ib1 = ScriptedIB([[FakePos('X', -240, 4.14, 'D1')]])
    ib2 = ScriptedIB([
        [FakePos('X', 0, 0.0, 'D2')],          # 第1轮快照：尚未落账（0）
        [FakePos('X', 480, 4.14, 'D2')],       # 批量核验：落账 480（超目标）
        [FakePos('X', 240, 4.14, 'D2')],       # 收尾终态：240（卖出超额后）
    ])
    store = TradeStore(TMP / 'c1')
    store.init_summary_files()

    res = await P.reconcile_positions(ib1, ib2, 'D1', 'D2', trade_store=store)

    # —— 行为断言：一次买入、一次同轮超平卖出，且收敛 ——
    ok_flow = (
        res['converged'] is True
        and CALLS['buy'] == [(ib2, 'X', 240)]
        and CALLS['sell'] == [(ib2, 'X', 240)]
        and CALLS['cancel'] == 0
        and not crit('调平后仍存在不一致')
    )

    # —— 账实一致断言（验收标准）——
    # 无超买/超卖残留：终态 240/240 对称（ib2 最后一帧 240，ib1 240）
    final1 = res['final_positions1'].get('X', {})
    final2 = res['final_positions2'].get('X', {})
    ok_flat = (final1.get('position') == -240 and final2.get('position') == 240)

    # 每笔持仓均有开仓账
    lots1 = store.get_open_lots('account1', 'X')
    lots2 = store.get_open_lots('account2', 'X')
    rem1 = sum(int(l['remaining']) for l in lots1)
    rem2 = sum(int(l['remaining']) for l in lots2)
    # 账户2 事件流：开仓行（OPEN/ADD）总量 与 平仓行（RECONCILE）总量
    ev2 = store._read(store.stock_csv('account2', 'X'))
    open_rows = ev2[ev2['event_type'].isin(('OPEN', 'ADD'))]
    opened2 = int(open_rows['volume'].astype(float).sum()) if not open_rows.empty else 0
    close_rows = ev2[ev2['event_type'] == 'RECONCILE']
    closed2 = int(close_rows['volume'].astype(float).sum()) if not close_rows.empty else 0
    ok_book = (
        rem1 == 240 and rem2 == 240          # 未平批次 == 实际持仓（账实一致）
        and opened2 == 480                    # 开仓账覆盖全部实际持仓 480
        and closed2 == 240                    # 平仓账存在且为超额 240
    )
    ok = ok_flow and ok_flat and ok_book
    detail = (f"converged={res['converged']} buys={CALLS['buy']} sells={CALLS['sell']} "
              f"rem1={rem1}/{240} opened2={opened2}/480 closed2={closed2}/240")
    print(f'[{"ok" if ok else "FAIL"}] C1 同轮超平(0→480→240): 一轮内买+卖超额, 账实一致 — {detail}')
    return ok


async def scenario_c2():
    """C2 filled 早于持仓落账不再双买：超时 → 撤单前重读 filled=6 → 按成交；
    下一轮快照落账 6 → 缺口关闭；全程只买一次、零撤单。"""
    reset_calls()
    clear_log()

    def mstr_trade(vol):
        return FakeTrade(6, 166.0, 'X', status='Timeout')

    make_stubs('Timeout', buy_trade_factory=lambda vol: mstr_trade(vol))
    ib1 = ScriptedIB([[FakePos('X', -6, 166.0, 'D1')]])
    ib2 = ScriptedIB([
        [FakePos('X', 0, 0.0, 'D2')],     # 第1轮快照
        [FakePos('X', 0, 0.0, 'D2')],     # 批量核验（未落账）
        [FakePos('X', 0, 0.0, 'D2')],     # 核验重试1
        [FakePos('X', 0, 0.0, 'D2')],     # 核验重试2
        [FakePos('X', 6, 166.0, 'D2')],   # 第2轮快照：落账 6
    ])
    store = TradeStore(TMP / 'c2')
    store.init_summary_files()

    res = await P.reconcile_positions(ib1, ib2, 'D1', 'D2', trade_store=store)

    ok = (
        len(CALLS['buy']) == 1                      # 只买一次（不再双买）
        and CALLS['buy'][0] == (ib2, 'X', 6)
        and CALLS['cancel'] == 0                    # filled 早到 → 撤单动作可省
        and CALLS['sell'] == []
        and res['converged'] is True
        and not crit('调平后仍存在不一致')
    )
    lots1 = store.get_open_lots('account1', 'X')
    lots2 = store.get_open_lots('account2', 'X')
    ok &= (sum(int(l['remaining']) for l in lots1) == 6
           and sum(int(l['remaining']) for l in lots2) == 6)
    detail = (f"buys={CALLS['buy']} cancels={CALLS['cancel']} converged={res['converged']} "
              f"booked1={sum(int(l['remaining']) for l in lots1)} "
              f"booked2={sum(int(l['remaining']) for l in lots2)}")
    print(f'[{"ok" if ok else "FAIL"}] C2 原单filled早于落账不再双买: {detail}')
    return ok


def _mk_runner(fcm_result: bool):
    strat = DynamicTPStrategy()
    fcm = type('M', (), {
        'result': fcm_result,
        'calls': [],
    })()

    async def run_close_signal(self, ib, account, symbol, close_action,
                               volume, open_price, open_action, csv_path, **kw):
        fcm.calls.append({'symbol': symbol, 'account': account,
                          'side': close_action, 'volume': volume})
        return fcm_result

    import types
    fcm.run_close_signal = types.MethodType(run_close_signal, fcm)

    runner = SR.StrategyRunner(
        strategy=strat, ib1=object(), ib2=object(),
        account1='D1', account2='D2',
        sell_csv_path='sell.csv', buy_csv_path='buy.csv',
        close_manager=fcm,
    )
    runner.active = True
    runner.activate_at = None

    snap = {'D1': {'X': {'position': -10, 'avgCost': 10.0},
                   'Y': {'position': -10, 'avgCost': 10.0}},
            'D2': {'X': {'position': 0, 'avgCost': 0.0},
                   'Y': {'position': 0, 'avgCost': 0.0}}}

    async def fake_get_positions(ib, account=''):
        return dict(snap.get(account, {}))

    SR.get_positions = fake_get_positions
    return runner, strat, fcm


def _trigger_bars(sym):
    """两根 bar：bar1 建仓后创新低（置极值，不触发），bar2 反弹过平仓线（触发做空腿平仓）"""
    t0 = EST.localize(datetime(2026, 9, 18, 9, 30, 0))
    b1 = Bar(sym, t0 + timedelta(minutes=2), 9.7, 9.7, 9.5, 9.6, 1000)
    b2 = Bar(sym, t0 + timedelta(minutes=3), 9.8, 10.0, 9.8, 9.9, 1000)
    return t0, b1, b2


async def scenario_c3():
    """C3 标的级暂停：暂停标的 X 的信号排队、非暂停标的 Y 照常执行；
    轮末重放 X 成功（保留去重标记）；失败分支回滚去重、下一 bar 重新触发。"""
    # ---------- C3a：暂停期间 Y 正常执行，X 排队 → 轮末重放成功 ----------
    reset_calls()
    runner, strat, fcm = _mk_runner(fcm_result=True)
    t0, xb1, xb2 = _trigger_bars('X')
    _, yb1, yb2 = _trigger_bars('Y')
    for s in ('X', 'Y'):
        runner.on_entry(s, 'account1', 10.0, 10, t0)

    runner.pause_symbols({'X'})
    await runner.on_bar(xb1)
    await runner.on_bar(xb2)          # X 触发做空腿平仓信号 → 应排队，不下单
    paused_blocked = (not any(c['symbol'] == 'X' for c in fcm.calls)
                      and 'X' in strat.closed_acc1)

    await runner.on_bar(yb1)
    await runner.on_bar(yb2)          # Y 非暂停 → 立即执行（做空腿：buy 平 account1 的空头）
    y_executed_during_pause = any(c['symbol'] == 'Y' and c['side'] == 'buy' and c['volume'] == 10
                                  for c in fcm.calls)

    runner.unpause_symbols({'X'})
    replayed = await runner.replay_paused_signals()
    x_replayed_ok = (replayed == 1
                     and any(c['symbol'] == 'X' and c['volume'] == 10
                             for c in fcm.calls)
                     and 'X' in strat.closed_acc1)   # 成功 → 去重标记保留
    ok_a = paused_blocked and y_executed_during_pause and x_replayed_ok and not crit('')
    print(f'[{"ok" if ok_a else "FAIL"}] C3a 标的级暂停+轮末重放成功: '
          f'X排队={paused_blocked}, Y照常={y_executed_during_pause}, 重放={x_replayed_ok}')

    # ---------- C3b：重放失败 → 回滚去重标记 → 下一 bar 可再次触发 ----------
    reset_calls()
    runner2, strat2, fcm2 = _mk_runner(fcm_result=False)
    t0b, xb1b, xb2b = _trigger_bars('X')
    runner2.on_entry('X', 'account1', 10.0, 10, t0b)
    runner2.pause_symbols({'X'})
    await runner2.on_bar(xb1b)
    await runner2.on_bar(xb2b)         # 触发 → 排队（标记已置）
    runner2.unpause_symbols({'X'})
    n2 = await runner2.replay_paused_signals()
    rolled_back = (n2 == 1 and 'X' not in strat2.closed_acc1)   # 失败 → 去重标记回滚

    # 下一根 bar 应能再次触发（标记已回滚，状态仍在）
    xb3 = Bar('X', t0b + timedelta(minutes=4), 9.9, 10.0, 9.8, 9.95, 1000)
    fcm2.result = True
    await runner2.on_bar(xb3)
    refired = any(c['symbol'] == 'X' for c in fcm2.calls)
    ok_b = rolled_back and refired
    print(f'[{"ok" if ok_b else "FAIL"}] C3b 重放失败回滚+下一bar重新触发: '
          f'回滚={rolled_back}, 重试={refired}')
    return ok_a and ok_b


def scenario_c4():
    """C4 差额补记幂等：重复调用不新增 lot；
    KEEL 判重盲区：lot 已平尽但账户仍有延迟成交持仓 → 只补差额，不重复、不漏补。"""
    ok = True

    # C4a：同一 target 重复 ensure_cover → 第二次 0，lot 数不变
    s = TradeStore(TMP / 'c4a')
    s.init_summary_files()
    g1 = s.ensure_cover('account2', 'X', 480, 4.10, exchange='NASDAQ',
                        industry='Fin', reason='c4a')
    g2 = s.ensure_cover('account2', 'X', 480, 4.10, reason='c4a-重复')
    lots = s.get_open_lots('account2', 'X')
    a1 = (g1 == 480 and g2 == 0 and len(lots) == 1
          and int(lots[0]['remaining']) == 480)
    ok &= a1
    print(f'[{"ok" if a1 else "FAIL"}] C4a 重复调用不新增 lot: g1={g1}, g2={g2}, lots={len(lots)}')

    # C4b：KEEL 型 —— 补买 lot 已被超额卖出平尽，实际仍留 240 延迟成交持仓
    s2 = TradeStore(TMP / 'c4b')
    s2.init_summary_files()
    s2.append_open('account2', 'K', price=4.14, volume=240,
                   exchange='NASDAQ', industry='Fin',
                   strategy='reconcile', reason='收敛补买')
    s2.append_close('account2', 'K', close_action='sell', volume=240, price=4.08,
                    strategy='reconcile', reason='同轮超平卖出', reconcile=True)
    remaining_before = sum(int(l['remaining']) for l in s2.get_open_lots('account2', 'K'))
    g = s2.ensure_cover('account2', 'K', 240, 4.14, exchange='NASDAQ',
                        industry='Fin', reason='延迟成交补写')
    g_dup = s2.ensure_cover('account2', 'K', 240, 4.14, reason='重复')
    lots_k = s2.get_open_lots('account2', 'K')
    rem_after = sum(int(l['remaining']) for l in lots_k)
    evk = s2._read(s2.stock_csv('account2', 'K'))
    opened_k = int(evk[evk['event_type'].isin(('OPEN', 'ADD'))]['volume'].astype(float).sum()) \
        if not evk[evk['event_type'].isin(('OPEN', 'ADD'))].empty else 0
    closed_k = int(evk[evk['event_type'].isin(('RECONCILE', 'CLOSE', 'REDUCE', 'FORCE_CLOSE'))]['volume'].astype(float).sum()) \
        if not evk[evk['event_type'].isin(('RECONCILE', 'CLOSE', 'REDUCE', 'FORCE_CLOSE'))].empty else 0
    a2 = (remaining_before == 0        # 原 lot 已平尽
          and g == 240                 # 实际 240 − 已入账 0 → 补 240（旧 has_open_events 会漏）
          and g_dup == 0               # 重复调用不再补
          and rem_after == 240         # 补后未平批次覆盖实际
          and opened_k == 480          # 开仓账 = 240(补买) + 240(延迟成交补记)
          and closed_k == 240)         # 平仓账存在（超额卖出 240）
    ok &= a2
    print(f'[{"ok" if a2 else "FAIL"}] C4b KEEL型差额补记: before={remaining_before}, '
          f'gap={g}, dup={g_dup}, after={rem_after}, lots={len(lots_k)}')

    # C4c：backfill_missing_open_records 重复调用幂等（P0 统一入口）
    s3 = TradeStore(TMP / 'c4c')
    s3.init_summary_files()
    pos1 = {'X': {'position': -240, 'avgCost': 4.14, 'contract': object()}}
    pos2 = {'X': {'position': 240, 'avgCost': 4.10, 'contract': object()}}
    P.backfill_missing_open_records(pos1, pos2, None, None, None, s3)
    n1 = len(s3.get_open_lots('account1', 'X')) + len(s3.get_open_lots('account2', 'X'))
    P.backfill_missing_open_records(pos1, pos2, None, None, None, s3)
    n2 = len(s3.get_open_lots('account1', 'X')) + len(s3.get_open_lots('account2', 'X'))
    a3 = (n1 == 2 and n2 == 2)
    ok &= a3
    print(f'[{"ok" if a3 else "FAIL"}] C4c 终态补平重复调用幂等: lots={n1} → {n2}')
    return ok


async def scenario_c5():
    """C5 持续失败 → 连续 3 轮停止重试 → 终态 CRITICAL（保持旧 P5 语义）"""
    reset_calls()
    clear_log()
    make_stubs('Cancelled',
               buy_trade_factory=lambda vol: FakeTrade(10, 50.0, 'X', status='Filled'))
    ib1 = ScriptedIB([[FakePos('X', -10, 10.0, 'D1')]])
    ib2 = ScriptedIB([[]])   # 账户2 始终无持仓（调平单永不落账）
    res = await P.reconcile_positions(ib1, ib2, 'D1', 'D2')
    ok = (len(CALLS['buy']) == 3 and all(b == (ib2, 'X', 10) for b in CALLS['buy'])
          and res['converged'] is False
          and crit('调平后仍存在不一致'))
    print(f'[{"ok" if ok else "FAIL"}] C5 3轮失败停止重试+终态CRITICAL: '
          f'attempts={len(CALLS["buy"])}, critical={crit("调平后仍存在不一致")}')
    return ok


async def main():
    failures = []
    for name, coro in [
        ('C1', scenario_c1()),
        ('C2', scenario_c2()),
        ('C3', scenario_c3()),
        ('C5', scenario_c5()),
    ]:
        clear_log()
        if not await coro:
            failures.append(name)
    if not scenario_c4():
        failures.append('C4')

    _log.removeHandler(_cap)
    asyncio.sleep = _real_sleep

    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 收敛循环 / 差额幂等补记 / 标的级暂停 新增回归测试全部通过')


# Windows 控制台默认代码页无法输出中文/emoji，统一 UTF-8
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
