# -*- coding: utf-8 -*-
"""
回归测试：统一"等订单完成"（monitor.wait_for_order_final）+ 重试前复核已平按实际成交补账
（2026-09-27 代码审查发现的两条问题链）

问题一（竞态）：旧 wait_for_trade_completion 在收到第一笔成交就返回 'Filled'，此时订单
仍在工作（filled=部分量）。调用方（close._execute_close）见"未全部成交"立刻撤销剩余
工作单 —— 市价单分几笔成交（间隔仅几毫秒）时剩余量被这次撤销杀死。
影响：多一次撤单 + 10s 重试等待 + 一次补单（多付佣金/滑点）。
修法：统一 wait 等"全量成交 或 订单终态 或 超时"才结算；部分成交+仍在工作不唤醒。
平仓与收敛循环（后者相反：不撤剩余量，交给 ensure_cover 差额补记）共用。

问题二（账本缺口）：_execute_close_with_retry 重试前复核发现目标方向仓位已归零
（前单延迟成交/fake-Cancelled 落账）→ 旧代码直接 return True 且不写平仓账，
缺口只能等退出补写兜底（而退出补写当时还有第2条缺陷）。
修法：复核已平时按实际成交补记平仓账（补记量=账上未平批次全部余量，
价格=最近一单实际成交均价，无信息退开仓价并告警；已入账则幂等不重记）。

覆盖：
  W1 分笔成交(毫秒级)不提前结算：两笔 100+140 → 返回时 filled 必须=240（不在首笔处醒来）
  W2 部分成交+终态(Cancelled) → 'Filled'，由调用方按数量判部分（契约不变）
  W3 Paper 假Cancelled 零成交 + 延迟落账 → 'Filled'
  W4 Paper 假Cancelled 零成交无落账 → 'Cancelled'
  W5 超时：有(部分)成交 → 'Filled'；零成交 → 'Timeout'
  W6 无 totalQuantity 的 fake 订单 → 退化旧语义（任何成交即结算）
  W7 close._execute_close 经真实 wait（无打桩）：部分成交 → 不写CSV/撤单/False（F1契约）
  B1 事件流：重试前复核已平 → 未平批次全部补记(1000股@实际均价)，不二次下单、幂等
  B2 旧CSV：重试前复核已平 → 该行标记已平(close_vol=1000, close_price=实际均价)
运行：python test/test_wait_final.py
"""
import asyncio
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import pandas as pd

# ---- 加速：所有 sleep 立即让出 ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

import monitor as M                                        # noqa: E402
import close as C                                         # noqa: E402
from close import CloseManager                            # noqa: E402
from trade_store import TradeStore                        # noqa: E402
from constants import TRADE_RECORD_COLUMNS                # noqa: E402
from logger import get_logger                             # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix='wait_final_'))

# ---- 日志捕获 ----
records = []
_log = get_logger()


class _Capture(logging.Handler):
    def emit(self, r):
        records.append(r)


_cap = _Capture()
_cap.setLevel(logging.DEBUG)
_log.addHandler(_cap)


def crit(*kw) -> bool:
    return any(r.levelno >= logging.CRITICAL and any(k in r.getMessage() for k in kw)
               for r in records)


# ---- Fake 对象 ----
class Signal:
    """最小 ib_async 事件语义：+= 绑定 / -= 解绑 / fire 触发回调"""

    def __init__(self):
        self.handlers = []

    def __iadd__(self, h):
        self.handlers.append(h)
        return self

    def __isub__(self, h):
        if h in self.handlers:
            self.handlers.remove(h)
        return self

    def fire(self, trade):
        for h in list(self.handlers):
            h(trade)


class WaitFakeTrade:
    """可脚本化推送的 fake 订单（statusEvent 可触发回调，total 控制 expected_volume）"""

    def __init__(self, symbol='XXX', total=None, filled=0, price=0.0, status='PreSubmitted'):
        self.contract = SimpleNamespace(symbol=symbol)
        if total is None:
            self.order = SimpleNamespace()          # 无 totalQuantity → expected=0 旧语义
        else:
            self.order = SimpleNamespace(orderId=1, totalQuantity=total)
        self.orderStatus = SimpleNamespace(
            filled=filled, avgFillPrice=price, status=status)
        self.fills = []
        self.statusEvent = Signal()

    def commission(self):
        return 1.0


class FakeIB:
    def __init__(self):
        self.cancel_calls = []

    def cancelOrder(self, order, manualCancelOrderTime=""):
        self.cancel_calls.append(order)
        return None

    async def reqPositionsAsync(self):
        return []


async def drive(trade, steps, wait_fn):
    """steps: [(秒, 变更函数)]；变更+触发事件在 wait 等待期间依次发生（sleep 已加速）"""
    async def script():
        for d, mut in steps:
            await asyncio.sleep(d)
            mut(trade)
            trade.statusEvent.fire(trade)

    task = asyncio.create_task(script())
    result = await wait_fn()
    await task
    return result


# ---- CSV 工具（旧路径场景） ----
def make_csv(name, rows):
    p = TMP / name
    base = {
        'datetime': '2026-07-15 09:31:00', 'exchange': 'NASDAQ', 'industry': 'X',
        'total_cost': 0, 'fund_used': 0,
        'close_datetime': '', 'close_price': '', 'close_vol': '',
        'close_fund': '', 'gross_profit': '', 'profit': ''
    }
    df = pd.DataFrame([{**base, **r} for r in rows], columns=TRADE_RECORD_COLUMNS)
    df.to_csv(p, index=False, encoding='utf-8-sig')
    return p


def check(name, ok, detail=''):
    print(f'[{"ok" if ok else "FAIL"}] {name}' + (f' — {detail}' if detail else ''))
    return ok


submit_log = []
scripted_trades = []


async def fake_submit_buy(ib, symbol, volume):
    submit_log.append(('buy', symbol, volume))
    return scripted_trades[min(len(submit_log) - 1, len(scripted_trades) - 1)]


async def main():
    failures = []

    # ---------- W1: 分笔成交不提前结算（竞态核心） ----------
    trade = WaitFakeTrade(total=240)

    def step_partial(t):
        t.orderStatus.filled = 100
        t.orderStatus.avgFillPrice = 10.0
        t.orderStatus.status = 'PreSubmitted'   # 仍在工作（剩余量还在途）

    def step_full(t):
        t.orderStatus.filled = 240
        t.orderStatus.status = 'Filled'

    res = await drive(trade, [(0.05, step_partial), (0.10, step_full)],
                      lambda: M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120))
    filled_at_return = int(trade.orderStatus.filled)
    ok1 = (res == 'Filled' and filled_at_return == 240)
    print(f'   返回={res} 返回时filled={filled_at_return}/240（首笔100处不唤醒则必为240）')
    if not check('W1 分笔成交毫秒级→不提前结算(不在首笔100处醒来)', ok1,
                 f'res={res} filled={filled_at_return}'):
        failures.append('W1')

    # ---------- W2: 部分成交 + 终态(Cancelled) → 'Filled'（契约不变，调用方判部分） ----------
    trade = WaitFakeTrade(total=240)

    def to_cancel_partial(t):
        t.orderStatus.status = 'Cancelled'   # filled 保持 100

    res = await drive(trade, [(0.05, step_partial), (0.10, to_cancel_partial)],
                      lambda: M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120))
    ok2 = (res == 'Filled' and int(trade.orderStatus.filled) == 100)
    print(f'   返回={res} filled={int(trade.orderStatus.filled)}/100（调用方 get_filled_volume<240 判部分）')
    if not check('W2 部分成交+终态→Filled由调用方判部分', ok2):
        failures.append('W2')

    # ---------- W3: 假Cancelled(零成交) + 延迟落账 → Filled ----------
    trade = WaitFakeTrade(total=240)

    def to_cancel_zero(t):
        t.orderStatus.filled = 0
        t.orderStatus.status = 'Cancelled'

    def delayed_fill(t):
        t.orderStatus.filled = 240
        t.orderStatus.avgFillPrice = 9.5

    res = await drive(trade, [(0.05, to_cancel_zero), (0.10, delayed_fill)],
                      lambda: M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120))
    ok3 = (res == 'Filled' and int(trade.orderStatus.filled) == 240)
    print(f'   返回={res} filled={int(trade.orderStatus.filled)}/240')
    if not check('W3 假Cancelled延迟落账→Filled(Paper保护保留)', ok3):
        failures.append('W3')

    # ---------- W4: 假Cancelled 零成交无落账 → Cancelled ----------
    trade = WaitFakeTrade(total=240)
    res = await drive(trade, [(0.05, to_cancel_zero)],
                      lambda: M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120))
    ok4 = (res == 'Cancelled' and int(trade.orderStatus.filled) == 0)
    print(f'   返回={res} filled={int(trade.orderStatus.filled)}/0')
    if not check('W4 假Cancelled无成交→Cancelled', ok4):
        failures.append('W4')

    # ---------- W5: 超时（有/无成交） ----------
    _real_wait_for = asyncio.wait_for

    async def _timeout_wait_for(aw, timeout=None):
        await _real_sleep(0)
        try:
            aw.close()
        except Exception:
            pass
        raise asyncio.TimeoutError

    asyncio.wait_for = _timeout_wait_for
    try:
        trade = WaitFakeTrade(total=240, filled=100, price=9.9)
        res = await M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120)
        ok5a = (res == 'Filled' and int(trade.orderStatus.filled) == 100)

        trade = WaitFakeTrade(total=240, filled=0)
        res = await M.wait_for_order_final(trade, expected_volume=240, timeout_seconds=120)
        ok5b = (res == 'Timeout')
    finally:
        asyncio.wait_for = _real_wait_for
    ok5 = ok5a and ok5b
    print(f'   部分成交超时→{res if not ok5a else "Filled"} / 零成交超时→{res}/Timeout')
    if not check('W5 超时: 有成交→Filled(调用方自决) / 零成交→Timeout', ok5):
        failures.append('W5')

    # ---------- W6: 无 totalQuantity → 旧语义（任何成交即结算） ----------
    trade = WaitFakeTrade(total=None, filled=100, price=9.9, status='PreSubmitted')
    res = await M.wait_for_order_final(trade, expected_volume=0, timeout_seconds=120)
    ok6 = (res == 'Filled')
    print(f'   返回={res}')
    if not check('W6 fake订单(expected=0)退化旧语义: 任何成交即结算', ok6):
        failures.append('W6')

    # ---------- W7: close._execute_close 经真实 wait 的部分成交契约（F1语义不回归） ----------
    sell_csv = make_csv('sell_w7.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = CloseManager(FakeIB(), FakeIB(), 'D1', 'D2', sell_csv, TMP / 'buy_w7.csv')
    scripted_trades.clear()
    scripted_trades.append(WaitFakeTrade(total=None, filled=400, price=99.0, status='PreSubmitted'))
    submit_log.clear()
    _orig_submit = C.submit_buy_order
    C.submit_buy_order = fake_submit_buy
    try:
        ok = await m._execute_close(m.ib1, 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    finally:
        C.submit_buy_order = _orig_submit
    df = pd.read_csv(sell_csv)
    unmarked = (pd.isna(df.iloc[0]['close_datetime'])
                or str(df.iloc[0]['close_datetime']).strip() in ('', 'nan'))
    ok7 = (ok is False and unmarked and len(m.ib1.cancel_calls) == 1 and len(submit_log) == 1)
    print(f'   ok={ok} unmarked={unmarked} cancels={len(m.ib1.cancel_calls)} submits={len(submit_log)}')
    if not check('W7 close真实wait部分成交: 不写CSV/撤单/False(F1契约)', ok7):
        failures.append('W7')

    # ---------- B1: 事件流 —— 重试前复核已平 → 未平批次全部补记、不二次下单 ----------
    s = TradeStore(TMP / 'b1')
    s.init_summary_files()
    s.append_open('account1', 'ZZZ', price=50.0, volume=1000, strategy='hedge', reason='开仓')
    m = CloseManager(FakeIB(), FakeIB(), 'D1', 'D2',
                     TMP / 'sell_b1.csv', TMP / 'buy_b1.csv', trade_store=s)
    scripted_trades.clear()
    scripted_trades.append(WaitFakeTrade(total=None, filled=400, price=52.0, status='PreSubmitted'))
    submit_log.clear()

    async def strict_flat(ib, account=''):
        return {}

    _orig_pos = C.get_positions_strict
    C.get_positions_strict = strict_flat
    C.submit_buy_order = fake_submit_buy
    try:
        okb1 = await m._execute_close_with_retry(
            m.ib1, 'D1', 'ZZZ', 'buy', 1000, 50.0, 'sell', TMP / 'sell_b1.csv')
    finally:
        C.get_positions_strict = _orig_pos
        C.submit_buy_order = _orig_submit
    lots = s.get_open_lots('account1', 'ZZZ')
    dfz = s._read(s.stock_csv('account1', 'ZZZ'))
    close_rows = dfz[dfz['event_type'].isin(('REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE'))]
    booked = sum(int(float(r['volume'])) for _, r in close_rows.iterrows())
    prices = [float(r['price']) for _, r in close_rows.iterrows()]
    vols = [v for (_, _, v) in submit_log]
    okb1 = (okb1 is True and vols == [1000] and not lots
            and booked == 1000 and prices == [52.0])
    print(f'   ok={okb1} submits={vols}/[1000] 未平批次={len(lots)}/0 '
          f'补记={booked}/1000 价格={prices}/[52.0]')
    if not check('B1 复核已平→未平批次全部补记@实际均价且不二次下单', okb1):
        failures.append('B1')

    # B1b: 幂等 —— 再次补记 0 新增
    nb = await m._book_close_confirmed_flat(
        m.ib1, 'D1', 'ZZZ', 'buy', 1000, 50.0, 'sell', TMP / 'sell_b1.csv')
    dfz2 = s._read(s.stock_csv('account1', 'ZZZ'))
    close_rows2 = dfz2[dfz2['event_type'].isin(('REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE'))]
    booked2 = sum(int(float(r['volume'])) for _, r in close_rows2.iterrows())
    okb1b = (nb == 0 and booked2 == 1000)
    print(f'   再次补记返回={nb}/0 累计={booked2}/1000')
    if not check('B1b 已入账则幂等(0新增)', okb1b):
        failures.append('B1b')

    # ---------- B2: 旧CSV —— 复核已平 → 该行标记已平 ----------
    sell_csv = make_csv('sell_b2.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = CloseManager(FakeIB(), FakeIB(), 'D1', 'D2', sell_csv, TMP / 'buy_b2.csv')
    scripted_trades.clear()
    scripted_trades.append(WaitFakeTrade(total=None, filled=400, price=99.0, status='PreSubmitted'))
    submit_log.clear()
    C.get_positions_strict = strict_flat
    C.submit_buy_order = fake_submit_buy
    try:
        okb2 = await m._execute_close_with_retry(
            m.ib1, 'D1', 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    finally:
        C.get_positions_strict = _orig_pos
        C.submit_buy_order = _orig_submit
    df = pd.read_csv(sell_csv)
    marked = not (pd.isna(df.iloc[0]['close_datetime'])
                  or str(df.iloc[0]['close_datetime']).strip() in ('', 'nan'))
    vols = [v for (_, _, v) in submit_log]
    okb2 = (okb2 is True and vols == [1000] and marked
            and int(float(df.iloc[0]['close_vol'])) == 1000
            and abs(float(df.iloc[0]['close_price']) - 99.0) < 1e-9)
    print(f'   ok={okb2} submits={vols}/[1000] marked={marked} '
          f'close_vol={df.iloc[0]["close_vol"]} close_price={df.iloc[0]["close_price"]}')
    if not check('B2 旧CSV复核已平→行标记已平(1000@99)且不二次下单', okb2):
        failures.append('B2')

    asyncio.sleep = _real_sleep
    _log.removeHandler(_cap)

    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 统一等订单完成 + 复核已平补账 回归测试全部通过（分笔成交竞态消除 / 账本不再等退出补写）')


try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
