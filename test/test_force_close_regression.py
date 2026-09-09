# -*- coding: utf-8 -*-
"""
回归测试：收市前强制平仓正确性
针对事故："CSV 显示所有股票已平仓，但两个账户仍有部分持仓" 的修复验证。
覆盖：
  F1 部分成交 ≠ 平仓完成：不得写CSV、返回 False、撤销剩余工作单
  F2 全量成交 → 平仓完成，CSV 正常写入
  F3 部分成交后重试只补平实况剩余数量；平仓前仓位已归零则不得二次下单
  F4 收敛循环：第1轮部分成交、第2轮全平 → 成功；持仓快照获取失败绝不判平
  F5 异常隔离：一只股票的平仓任务抛异常不得中断其他股票的平仓
  F6 backfill 覆盖校验：平成交>=开仓量才允许标记已平仓；不足则保留未平仓标记并 CRITICAL
运行：python test/test_force_close_regression.py
"""
import asyncio
import datetime
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import pandas as pd

import close as C
from close import CloseManager
from constants import TRADE_RECORD_COLUMNS

# ---- 加速：所有 sleep 立即让出 ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

# ---- Fake 对象 ----


class FakeOrder:
    def __init__(self, oid=1000):
        self.orderId = oid


class FakeStatus:
    def __init__(self):
        self.filled = 0
        self.avgFillPrice = 0.0
        self.status = 'Submitted'


class FakeContract:
    def __init__(self, symbol):
        self.symbol = symbol


class FakeTrade:
    def __init__(self, filled, price, symbol='AAA', status='Submitted'):
        self.orderStatus = FakeStatus()
        self.orderStatus.filled = filled
        self.orderStatus.avgFillPrice = price
        self.orderStatus.status = status
        self.contract = FakeContract(symbol)
        self.order = FakeOrder()
        self.fills = []

    def commission(self):
        return 1.0


class FakePos:
    def __init__(self, symbol, position, avg_cost, account):
        self.contract = FakeContract(symbol)
        self.position = position
        self.avgCost = avg_cost
        self.account = account


class FakeIB:
    def __init__(self, snaps=None):
        self.cancel_calls = []
        self._snaps = list(snaps or [])
        self._i = 0

    def cancelOrder(self, order, manualCancelOrderTime=""):
        self.cancel_calls.append(order)
        return None

    async def reqPositionsAsync(self):
        item = self._snaps[min(self._i, len(self._snaps) - 1)]
        self._i += 1
        if isinstance(item, BaseException):
            raise item
        return list(item)


class FakeFill:
    def __init__(self, side, symbol, shares, price, t=None):
        self.execution = SimpleNamespace(
            side=side, shares=shares, price=price,
            time=t or datetime.datetime(2026, 7, 15, 15, 55, 0)
        )
        self.contract = SimpleNamespace(symbol=symbol)


# ---- 日志捕获 ----
from logger import get_logger

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


# ---- CSV 工具 ----
TMP = Path(tempfile.mkdtemp(prefix='force_close_test_'))


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


def csv_row_unmarked(p):
    df = pd.read_csv(p)
    assert len(df) == 1
    v = str(df.iloc[0]['close_datetime']).strip()
    return v in ('', 'nan', 'NaN') or pd.isna(df.iloc[0]['close_datetime'])


def build_manager(sell_csv, buy_csv, ib1=None, ib2=None):
    m = CloseManager(ib1 or FakeIB(), ib2 or FakeIB(), 'D1', 'D2', sell_csv, buy_csv)
    return m


# ---- 打桩 ----
async def wait_filled(trade, timeout_seconds=120):
    return 'Filled'


C.wait_for_trade_completion = wait_filled

submit_log = []
scripted_trades = []


async def fake_submit_buy(ib, symbol, volume):
    submit_log.append(('buy', symbol, volume))
    tr = scripted_trades[min(len(submit_log) - 1, len(scripted_trades) - 1)]
    return tr


async def main():
    failures = []

    # ---------- F1: 部分成交 ≠ 平仓完成 ----------
    sell_csv = make_csv('sell_f1.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f1.csv')
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=400, price=99.0, status='Submitted'))
    C.submit_buy_order = fake_submit_buy
    submit_log.clear()

    ok = await m._execute_close(m.ib1, 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    ok1 = (ok is False and csv_row_unmarked(sell_csv) and len(m.ib1.cancel_calls) == 1
           and len(submit_log) == 1)
    print(f'[{"ok" if ok1 else "FAIL"}] F1 部分成交(400/1000)判未平仓: 不写CSV/返回False/撤销工作单 '
          f'(ok={ok}, unmarked={csv_row_unmarked(sell_csv)}, cancels={len(m.ib1.cancel_calls)})')
    if not ok1:
        failures.append('F1')

    # ---------- F2: 全量成交 → 平仓完成 ----------
    sell_csv = make_csv('sell_f2.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f2.csv')
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=1000, price=99.0, status='Filled'))
    submit_log.clear()

    ok = await m._execute_close(m.ib1, 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    df = pd.read_csv(sell_csv)
    ok2 = (ok is True and not csv_row_unmarked(sell_csv)
           and int(float(df.iloc[0]['close_vol'])) == 1000)
    print(f'[{"ok" if ok2 else "FAIL"}] F2 全量成交判已平仓: 写CSV/close_vol=1000 (ok={ok})')
    if not ok2:
        failures.append('F2')

    # ---------- F3: 重试只补平剩余；已归零不得二次下单 ----------
    sell_csv = make_csv('sell_f3.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f3.csv')
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=400, price=99.0, status='Submitted'))  # 第1次: 部分
    scripted_trades.append(FakeTrade(filled=600, price=98.0, status='Filled'))      # 第2次: 补600
    submit_log.clear()

    async def strict_600(ib, account=''):
        return {'AAA': {'position': -600, 'avgCost': 99.0, 'contract': FakeContract('AAA')}}

    C.get_positions_strict = strict_600
    ok = await m._execute_close_with_retry(
        m.ib1, 'D1', 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    df = pd.read_csv(sell_csv)
    vols = [v for (_, _, v) in submit_log]
    ok3 = (ok is True and vols == [1000, 600] and not csv_row_unmarked(sell_csv)
           and int(float(df.iloc[0]['close_vol'])) == 600)
    print(f'[{"ok" if ok3 else "FAIL"}] F3 部分成交后按实况(600股)补平且不超量: ok={ok}, vols={vols}')
    if not ok3:
        failures.append('F3')

    # F3b: 剩余部分已被前单成交（仓位归零）→ 不得二次下单
    sell_csv = make_csv('sell_f3b.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f3b.csv')
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=400, price=99.0, status='Submitted'))
    scripted_trades.append(FakeTrade(filled=600, price=98.0, status='Filled'))
    submit_log.clear()

    async def strict_flat(ib, account=''):
        return {}

    C.get_positions_strict = strict_flat
    ok = await m._execute_close_with_retry(
        m.ib1, 'D1', 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    vols = [v for (_, _, v) in submit_log]
    ok3b = (ok is True and vols == [1000])
    print(f'[{"ok" if ok3b else "FAIL"}] F3b 仓位已归零时停止补单（防双重平仓）: submits={vols}')
    if not ok3b:
        failures.append('F3b')

    # F3c: 重试前持仓快照获取失败 → 不得误判"已平完"，继续按原量补平
    sell_csv = make_csv('sell_f3c.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f3c.csv')
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=400, price=99.0, status='Submitted'))
    scripted_trades.append(FakeTrade(filled=1000, price=98.0, status='Filled'))  # 快照未知仍按原量1000补单并全成
    submit_log.clear()

    async def strict_raises(ib, account=''):
        raise RuntimeError('TWS snapshot down')

    C.get_positions_strict = strict_raises
    ok = await m._execute_close_with_retry(
        m.ib1, 'D1', 'AAA', 'buy', 1000, 100.0, 'sell', sell_csv)
    vols = [v for (_, _, v) in submit_log]
    ok3c = (ok is True and vols == [1000, 1000])
    print(f'[{"ok" if ok3c else "FAIL"}] F3c 快照失败按"未知"处理并继续补平: ok={ok}, vols={vols}')
    if not ok3c:
        failures.append('F3c')

    # F3d: sell 腿（平多），重试前仓位已归零 → 必须停止补单
    # 9.8 ASAN 事故镜像：旧代码 (close_action=='buy')==(live<0) 在 sell 腿 live==0 时
    # False==False→True，误判"仍持仓"而对 0 仓盲目补卖 114 股 → 120秒超时失败。
    # （F3b 只覆盖了 buy 腿 —— 恰是表达式正确的一侧，故旧代码漏测了 sell 腿）
    buy_csv_f3d = make_csv('buy_f3d.csv', [
        {'code': 'AAA', 'action': 'buy', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(TMP / 'sell_f3d.csv', buy_csv_f3d)
    scripted_trades.clear()
    scripted_trades.append(FakeTrade(filled=400, price=99.0, status='Submitted'))  # 第1次: 部分
    submit_log.clear()

    async def fake_submit_sell(ib, symbol, volume):
        submit_log.append(('sell', symbol, volume))
        tr = scripted_trades[min(len(submit_log) - 1, len(scripted_trades) - 1)]
        return tr

    C.submit_sell_order = fake_submit_sell

    async def strict_flat_d(ib, account=''):
        return {}

    C.get_positions_strict = strict_flat_d
    ok = await m._execute_close_with_retry(
        m.ib2, 'D2', 'AAA', 'sell', 1000, 100.0, 'buy', buy_csv_f3d)
    vols = [v for (_, _, v) in submit_log]
    ok3d = (ok is True and vols == [1000])
    print(f'[{"ok" if ok3d else "FAIL"}] F3d sell腿仓位归零时停止补单（防对0仓超卖）: ok={ok}, submits={vols}')
    if not ok3d:
        failures.append('F3d')

    # ---------- F4: 收敛循环成功路径 ----------
    sell_csv = make_csv('sell_f4.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f4.csv')
    close_log = []

    async def fake_close_position(ib, account, symbol, position, csv_path,
                                  open_action, standard, avg_cost=0.0):
        close_log.append((account, symbol, position))

    m._close_position = fake_close_position
    seq = [
        {'AAA': {'position': -1000, 'avgCost': 100.0, 'contract': FakeContract('AAA')}},
        {},  # round1 ib2
        {},  # round2 ib1 → flat
        {},  # round2 ib2
    ]

    async def strict_seq(ib, account=''):
        nonlocal seq
        r = seq.pop(0) if seq else {}
        return dict(r)

    C.get_positions_strict = strict_seq
    flat = await m.force_close_until_flat(timeout_minutes=10)
    ok4 = (flat is True and close_log == [('D1', 'AAA', -1000)]
           and m.force_close_active is False and m.is_runtime_close_active is False)
    print(f'[{"ok" if ok4 else "FAIL"}] F4 两轮收敛后判平: flat={flat}, closes={close_log}, '
          f'gate_reset={not m.force_close_active}')
    if not ok4:
        failures.append('F4')

    # ---------- F4b: 快照失败绝不判平 ----------
    m = build_manager(TMP / 'sell_f4b.csv', TMP / 'buy_f4b.csv')

    async def strict_down(ib, account=''):
        raise RuntimeError('TWS snapshot down')

    C.get_positions_strict = strict_down
    never_flat = False
    try:
        await asyncio.wait_for(m.force_close_until_flat(timeout_minutes=10), timeout=1.0)
        never_flat = False  # 快速返回 → 不合格（快照失败时不该快速"判平"退出）
    except asyncio.TimeoutError:
        never_flat = True  # 持续重试中 = 没有把失败误判为已清仓
    ok4b = never_flat and m.force_close_active is False
    print(f'[{"ok" if ok4b else "FAIL"}] F4b 快照持续失败时不宣称已清仓（持续重试）: {never_flat}')
    if not ok4b:
        failures.append('F4b')

    # ---------- F5: 异常隔离 ----------
    m = build_manager(TMP / 'sell_f5.csv', TMP / 'buy_f5.csv')
    closed = []

    async def close_raises_for_aaa(ib, account, symbol, position, csv_path,
                                   open_action, standard, avg_cost=0.0):
        if symbol == 'AAA':
            raise RuntimeError('boom')
        closed.append(symbol)

    m._close_position = close_raises_for_aaa
    pos1 = {
        'AAA': {'position': -10, 'avgCost': 100.0, 'contract': FakeContract('AAA')},
        'BBB': {'position': -20, 'avgCost': 50.0, 'contract': FakeContract('BBB')},
    }
    raised = False
    try:
        await m._force_close_positions(pos1, {})
    except Exception as e:
        raised = True
        print('   异常上抛:', e)
    ok5 = (not raised and closed == ['BBB'])
    print(f'[{"ok" if ok5 else "FAIL"}] F5 单标的异常不中断其余平仓: closed={closed}, raised={raised}')
    if not ok5:
        failures.append('F5')

    # ---------- F6: backfill 覆盖校验 ----------
    # F6a: 成交覆盖 → 标记已平仓
    sell_csv = make_csv('sell_f6a.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f6a.csv')
    # 注意：side 必须用 TWS 真实协议值 'BOT'/'SLD'（官方文档: BOT for bought, SLD for sold），
    # 9.8 ASAN 事故的补写正是用 'BUY'/'SELL' 比较导致永不匹配 —— 旧代码在此数据下必失败
    fills = [FakeFill('BOT', 'AAA', 1000, 99.0)]
    m._backfill_csv(fills, 'BUY', 'sell', sell_csv)
    df = pd.read_csv(sell_csv)
    ok6a = (not csv_row_unmarked(sell_csv) and int(float(df.iloc[0]['close_vol'])) == 1000)
    print(f'[{"ok" if ok6a else "FAIL"}] F6a 平仓成交1000>=开仓1000 → 补写标记已平仓: {not csv_row_unmarked(sell_csv)}')
    if not ok6a:
        failures.append('F6a')

    # F6b: 成交不足 → 不得标记已平仓，且 CRITICAL
    sell_csv = make_csv('sell_f6b.csv', [
        {'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(sell_csv, TMP / 'buy_f6b.csv')
    records.clear()
    fills = [FakeFill('BOT', 'AAA', 400, 99.0)]
    m._backfill_csv(fills, 'BUY', 'sell', sell_csv)
    ok6b = csv_row_unmarked(sell_csv) and crit('可能真正未平仓')
    print(f'[{"ok" if ok6b else "FAIL"}] F6b 平仓成交400<开仓1000 → 保留未平仓标记+CRITICAL: '
          f'unmarked={csv_row_unmarked(sell_csv)}, critical={crit("可能真正未平仓")}')
    if not ok6b:
        failures.append('F6b')

    # F6c: 多行 FIFO —— 第一行被覆盖，第二行不足覆盖保留未平仓
    sell_csv = TMP / 'sell_f6c.csv'
    base = {
        'datetime': '2026-07-15 09:31:00', 'exchange': 'NASDAQ', 'industry': 'X',
        'total_cost': 0, 'fund_used': 0,
        'close_datetime': '', 'close_price': '', 'close_vol': '',
        'close_fund': '', 'gross_profit': '', 'profit': ''
    }
    dfc = pd.DataFrame([
        {**base, 'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 1000},
        {**base, 'code': 'AAA', 'action': 'sell', 'entry_price': 100.0, 'vol': 500},
    ], columns=TRADE_RECORD_COLUMNS)
    dfc.to_csv(sell_csv, index=False, encoding='utf-8-sig')
    m = build_manager(sell_csv, TMP / 'buy_f6c.csv')
    records.clear()
    fills = [FakeFill('BOT', 'AAA', 700, 99.0), FakeFill('BOT', 'AAA', 500, 98.5)]
    m._backfill_csv(fills, 'BUY', 'sell', sell_csv)
    df = pd.read_csv(sell_csv)
    r0_closed = not (pd.isna(df.iloc[0]['close_datetime']) or str(df.iloc[0]['close_datetime']).strip() in ('', 'nan'))
    r1_unmarked = pd.isna(df.iloc[1]['close_datetime']) or str(df.iloc[1]['close_datetime']).strip() in ('', 'nan')
    ok6c = r0_closed and r1_unmarked and crit('500')
    print(f'[{"ok" if ok6c else "FAIL"}] F6c FIFO覆盖: 行1(1000)标记已平, 行2(500)未覆盖保留: '
          f'r0={r0_closed}, r1_unmarked={r1_unmarked}')
    if not ok6c:
        failures.append('F6c')

    # F6d: buy.csv 平仓回写（平仓=卖出，真实协议 side='SLD'）
    # 9.8 ASAN 事故直接复现：旧代码 side=='SELL' 对 'SLD' 永不匹配 → "无需补写"死分支
    buy_csv_f6d = make_csv('buy_f6d.csv', [
        {'code': 'AAA', 'action': 'buy', 'entry_price': 100.0, 'vol': 1000}
    ])
    m = build_manager(TMP / 'sell_f6d.csv', buy_csv_f6d)
    records.clear()
    fills = [FakeFill('SLD', 'AAA', 1000, 101.0)]
    m._backfill_csv(fills, 'SELL', 'buy', buy_csv_f6d)
    df = pd.read_csv(buy_csv_f6d)
    ok6d = (not csv_row_unmarked(buy_csv_f6d)
            and int(float(df.iloc[0]['close_vol'])) == 1000
            and abs(float(df.iloc[0]['close_price']) - 101.0) < 1e-9)
    print(f'[{"ok" if ok6d else "FAIL"}] F6d 平多成交(SLD)匹配补写: '
          f'marked={not csv_row_unmarked(buy_csv_f6d)}, '
          f'close_vol={df.iloc[0]["close_vol"]}, close_price={df.iloc[0]["close_price"]}')
    if not ok6d:
        failures.append('F6d')

    asyncio.sleep = _real_sleep
    _log.removeHandler(_cap)

    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 强制平仓回归测试全部通过：部分成交不再被误判为已平仓')


asyncio.run(main())
