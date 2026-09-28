# -*- coding: utf-8 -*-
"""
回归测试：2026-09-27 五处缺陷修复（代码审查确认 + 修复验证）

  F1 持仓快照只保留股票：账户里同名期权（secType=OPT）不得混入决策快照
     （旧行为：dict 按 symbol 建键，期权条目会顶掉真实股票持仓，
       调平/强平/策略全按"错误的股票持仓"动作）；fills 匹配同类过滤
  F2 开盘第一根 bar 不再被直接跳过：策略激活前到达的 bar 入队，
     激活到点后立即按序补评估（旧行为：一次性消费 + 直接 return，
       开盘窗口内这条 bar 永远没有参与判断）
  F3 分钟数据文件"当天开盘价"取 09:30 正式开盘 bar，
     而不是 09:29 盘前 bar（旧行为：day_open 被盘前 bar 播种）
  F4 收盘时间解析失败时回退默认 15:30（美东），force_close_dt 永远存在
     （旧行为：force_close_dt=None → 主循环条件永不成立 → 永远不强平）
  F5 退出补写前严格持仓快照失败后：禁止任何路径补记"已平"
     （旧行为：旧 CSV 兜底路径仍按 fills 把账上未平行全部标记已平，
       把账本强行写成"两账户 0 持仓"）
运行：python test/test_issue_fixes_regression.py   （任意 cwd 均可）
"""
import asyncio
import logging
import sys
import tempfile
import pytz
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

# ---- 加速：跳过固定 sleep（快照重试 / 退出补写等待 / 早到 bar 冲刷等待） ----
_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

import position as P                                  # noqa: E402
import close as C                                     # noqa: E402
import strategy_runner as SR                          # noqa: E402
import realtime                                       # noqa: E402
import hedge_trade as HT                              # noqa: E402
from constants import TRADE_RECORD_COLUMNS            # noqa: E402
from trade_store import TradeStore                    # noqa: E402
from strategy.base import Bar                         # noqa: E402
from logger import get_logger                         # noqa: E402

EST = pytz.timezone('US/Eastern')

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


def clear_log():
    records.clear()


# ---- Fake 对象 ----
def mk_contract(symbol, sec_type='STK', con_id=1):
    return type('C', (), {'symbol': symbol, 'conId': con_id, 'secType': sec_type})()


class FakePos:
    def __init__(self, symbol, position, avg_cost, account, sec_type='STK', con_id=1):
        self.position = position
        self.avgCost = avg_cost
        self.account = account
        self.contract = mk_contract(symbol, sec_type, con_id)


class ScriptedIB:
    def __init__(self, snap=None, raise_on_snap=False):
        self._snaps = list(snap or [])
        self.raise_on_snap = raise_on_snap
        self.calls = 0
        self._fills = []

    async def reqPositionsAsync(self):
        self.calls += 1
        if self.raise_on_snap:
            raise RuntimeError('TWS down (position snapshot)')
        await _real_sleep(0)
        return list(self._snaps)

    def fills(self):
        return list(self._fills)


def mk_fill(symbol, side, shares, price, sec_type='STK'):
    return type('Fill', (), {
        'execution': type('Exec', (), {'side': side, 'shares': shares,
                                       'price': price, 'time': 1_700_000_000})(),
        'contract': mk_contract(symbol, sec_type),
        'time': 1_700_000_000,
    })()


def check(name, ok, detail=''):
    print(f'[{"ok" if ok else "FAIL"}] {name}' + (f' — {detail}' if detail else ''))
    return bool(ok)


def _bar(sym, dt, symbol2=None):
    return Bar(symbol2 or sym, dt, 100.0, 101.0, 99.0, 100.5, 1000)


# ============================================================
# F1 持仓快照只保留股票
# ============================================================
async def f1():
    ok = True
    clear_log()
    # 账户里 股票 X 空头-10 + 同名期权 X(OPT) 多头+50（dict 同键：后到覆盖先到）
    ib = ScriptedIB([FakePos('X', -10, 100.0, 'D1', 'STK'),
                     FakePos('X', 50, 2.5, 'D1', 'OPT')])
    snap = await P.get_positions(ib, 'D1')
    ok &= check('F1a get_positions 剔除同名期权, 保留股票 -10',
                set(snap.keys()) == {'X'} and snap['X']['position'] == -10, str(snap))

    # 顺序颠倒（旧代码结果取决于 TWS 推送顺序，股票可能被期权顶掉）
    ib2 = ScriptedIB([FakePos('X', 50, 2.5, 'D1', 'OPT'),
                      FakePos('X', -10, 100.0, 'D1', 'STK')])
    snap2 = await P.get_positions(ib2, 'D1')
    ok &= check('F1b 顺序无关: 股票持仓恒为 -10',
                snap2.get('X', {}).get('position') == -10, str(snap2))

    ib3 = ScriptedIB([FakePos('Y', 30, 9.0, 'D1', 'FUT'),
                      FakePos('X', -10, 100.0, 'D1', 'STK')])
    snap3 = await P.get_positions(ib3, 'D1')
    ok &= check('F1c 期货等非股票类型同样剔除',
                set(snap3.keys()) == {'X'} and snap3['X']['position'] == -10, str(snap3))

    # 严格版同口径
    ib4 = ScriptedIB([FakePos('X', -10, 100.0, 'D1', 'STK'),
                      FakePos('X', 50, 2.5, 'D1', 'OPT')])
    snap4 = await P.get_positions_strict(ib4, 'D1')
    ok &= check('F1d get_positions_strict 同口径剔除期权',
                snap4.get('X', {}).get('position') == -10, str(snap4))

    # fills 匹配同类过滤（close.py 静态工具）
    cm = C.CloseManager(object(), object(), 'D1', 'D2', 's.csv', 'b.csv', trade_store=None)
    fills = [mk_fill('X', 'BUY', 100, 10.0, 'STK'),
             mk_fill('X', 'BUY', 30, 0.5, 'OPT')]
    totals = cm._fills_total_by_symbol(fills, 'BUY')
    ok &= check('F1e fills 按标的合并剔除期权成交', totals == {'X': 100}, str(totals))

    # 事件流回填：期权 fills 不得把成交量虚增、不得触发"成交>未平批次"误报
    clear_log()
    store = TradeStore(Path(tempfile.mkdtemp(prefix='f1f_')) / 'acc')
    store.init_summary_files()
    store.append_open('account1', 'X', price=100.0, volume=100,
                      strategy='hedge', reason='开仓')
    cm2 = C.CloseManager(object(), object(), 'D1', 'D2', 's.csv', 'b.csv', trade_store=store)
    p = cm2._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    rem = sum(int(l['remaining']) for l in store.get_open_lots('account1', 'X'))
    false_alarm = crit('> 未平批次', '重复成交')
    ok &= check('F1f 事件流回填按纯股票fills记账(100股, 无"成交>未平"误报)',
                p == 1 and rem == 0 and not false_alarm,
                f'patched={p} 未平={rem} false_alarm={false_alarm}')
    return ok


# ============================================================
# F2 激活前早到的 bar 排队、激活后按序补评估
# ============================================================
class SpyStrategy:
    def __init__(self):
        self.bars = []

    def on_entry(self, *a, **k):
        pass

    def get_display_state(self, sym):
        return {}

    def on_execution_result(self, sig, success):
        pass

    def on_bar(self, bar, pos):
        self.bars.append((bar.symbol, bar.dt))
        return []


def build_runner(spy):
    runner = SR.StrategyRunner(
        strategy=spy,
        ib1=object(), ib2=object(),
        account1='D1', account2='D2',
        sell_csv_path='sell.csv', buy_csv_path='buy.csv',
        close_manager=object(),
    )

    async def fake_get_positions(ib, account=''):
        return {}

    SR.get_positions = fake_get_positions
    return runner


async def f2():
    ok = True
    base_dt = EST.localize(datetime(2026, 9, 27, 10, 0, 0))
    entry_dt = EST.localize(datetime(2026, 9, 27, 9, 30, 10))  # 建仓早于 bar

    # ---- A: 定时冲刷 —— 激活前到达的 bar 必须在激活后被评估（不再永久丢弃） ----
    spyA = SpyStrategy()
    rA = build_runner(spyA)
    rA.active = True
    rA.activate_at = datetime.now() + timedelta(seconds=0.15)
    rA.on_entry('X', 'account1', 100.0, 10, entry_dt)
    barA1 = _bar('X', base_dt)                 # 模拟"开盘窗口内的第一根 bar"早到
    await rA.on_bar(barA1)                     # now < activate_at → 入队，不立即评估
    await _real_sleep(0.4)                     # 等激活到点冲刷（内部 sleep 已被加速）
    ok &= check('F2a 激活前早到的 bar 被保留并补评估（旧行为: 直接丢弃）',
                spyA.bars == [('X', base_dt)], f'spy={spyA.bars}')

    # ---- B: 下一根 bar 事件冲刷 + 重复入队去重 + 顺序保持 ----
    spyB = SpyStrategy()
    rB = build_runner(spyB)
    rB.active = True
    rB.activate_at = datetime.now() + timedelta(seconds=0.15)
    rB.on_entry('X', 'account1', 100.0, 10, entry_dt)
    rB._early_flush_scheduled = True           # 屏蔽定时冲刷，专测 on_bar 的 drain 路径
    b1 = _bar('X', base_dt)
    b2 = _bar('X', base_dt + timedelta(minutes=1))
    b3 = _bar('X', base_dt + timedelta(minutes=2))
    await rB.on_bar(b1)
    await rB.on_bar(b1)                        # 重复投递 → 去重只入队一次
    await rB.on_bar(b2)
    rB.activate_at = datetime.now() - timedelta(seconds=1)  # 激活到点
    await rB.on_bar(b3)                        # 新 bar 触发按序冲刷: b1, b2, b3
    got = list(spyB.bars)
    expect = [('X', base_dt), ('X', base_dt + timedelta(minutes=1)),
              ('X', base_dt + timedelta(minutes=2))]
    ok &= check('F2b 早到队列按序补评估且重复bar去重', got == expect, f'got={got}')
    return ok


# ============================================================
# F3 分钟文件"当天开盘价"取 09:30 正式开盘 bar
# ============================================================
class _FakeEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, h):
        self.handlers.append(h)
        return self


class _FakeBarsList(list):
    def __init__(self, items):
        super().__init__(items)
        self.updateEvent = _FakeEvent()


class _FakeBar:
    def __init__(self, dt, o, h, l, c, vol=100):
        self.date = dt
        self.open = o
        self.high = h
        self.low = l
        self.close = c
        self.volume = vol


class _RunnerStub:
    def get_display_state(self, symbol):
        return {}

    async def on_bar(self, bar):
        pass  # F3 只验证 CSV 开盘价口径，策略路径与 bar 无关


def f3():
    today = datetime.now(EST).date()

    def mk(h, m, o, c):
        dt = EST.localize(datetime.combine(today, datetime.min.time()).replace(hour=h, minute=m))
        return _FakeBar(dt, o, max(o, c), min(o, c), c)

    # 09:29 盘前 bar（旧行为用它播种 day_open=100）; 09:30 正式开盘 o=105
    bars = _FakeBarsList([mk(9, 29, 100.0, 105.0),
                          mk(9, 30, 105.0, 110.0),
                          mk(9, 31, 110.0, 108.0)])
    tmp = Path(tempfile.mkdtemp(prefix='f3_'))
    ib = type('IB', (), {'errorEvent': _FakeEvent(), 'connectedEvent': _FakeEvent(),
                         'disconnectedEvent': _FakeEvent()})()
    rec = realtime.RealtimeDataRecorder(ib, tmp, _RunnerStub())
    csv_path = rec._init_csv('DAYO')
    rec._create_bar_handler('DAYO', bars, csv_path)
    for h in list(bars.updateEvent.handlers):
        h(bars, False)

    import time as _t
    _t.sleep(0.1)  # 等待 create_task 的展示字段/写入完成（写入本身是同步的，仅留余量）

    df = pd.read_csv(csv_path)
    pct = list(df['day_change_pct'])
    pct_0930 = round((110.0 - 105.0) / 105.0 * 100, 4)          # 4.7619
    pct_0931 = round((108.0 - 105.0) / 105.0 * 100, 4)          # 2.8571
    pmax_last = float(df['day_change_pct_max'].iloc[-1])
    pmin_last = float(df['day_change_pct_min'].iloc[-1])
    return check(
        'F3 开盘价=09:30正式bar(105)而非09:29盘前bar(100): pct序列正确',
        len(df) == 3
        and abs(pct[0] - 0.0) < 1e-9            # 09:29 盘前bar: 开盘价未定 → 0.0 占位
        and abs(pct[1] - pct_0930) < 1e-9
        and abs(pct[2] - pct_0931) < 1e-9       # 旧行为此处为 (108-100)/100 = 8.0
        and abs(pmax_last - pct_0930) < 1e-9
        and abs(pmin_last - pct_0931) < 1e-9,
        f'pct={pct} max={pmax_last} min={pmin_last} (期望 {pct_0930} / {pct_0931})')


# ============================================================
# F4 收盘时间解析失败 → 回退默认 15:30，强平价期限必存在
# ============================================================
def f4():
    ok = True
    cd, fcd = HT.derive_close_deadlines('16:00')
    ok &= check('F4a 正常解析 16:00 → 收盘16:00 / 强平15:50',
                (cd.hour, cd.minute) == (16, 0) and (fcd.hour, fcd.minute) == (15, 50),
                f'{cd} / {fcd}')

    cd, fcd = HT.derive_close_deadlines('16:00:10')
    ok &= check('F4b 兼容三段式 16:00:10',
                (cd.hour, cd.minute) == (16, 0) and (fcd.hour, fcd.minute) == (15, 50),
                f'{cd} / {fcd}')

    for bad in ('garbage', '', None, '99:99'):
        clear_log()
        try:
            cd, fcd = HT.derive_close_deadlines(bad)
            raised = False
        except Exception as e:
            cd = fcd = None
            raised = True
        ok &= check(f'F4c 解析失败 {bad!r} → 不抛异常, 回退 15:30 / 15:20 + CRITICAL',
                    not raised and cd is not None and fcd is not None
                    and (cd.hour, cd.minute) == (15, 30)
                    and (fcd.hour, fcd.minute) == (15, 20)
                    and crit('解析收盘时间失败'),
                    f'{cd} / {fcd} critical={crit("解析收盘时间失败")}')
    return ok


# ============================================================
# F5 退出补写：严格快照失败后禁止任何"已平"补记
# ============================================================
async def f5():
    tmp = Path(tempfile.mkdtemp(prefix='f5_'))
    sell_p = tmp / 'sell.csv'
    buy_p = tmp / 'buy.csv'
    common = {'datetime': '2026-09-27 09:30:15', 'code': 'X', 'exchange': 'NYSE',
              'industry': 'Tech', 'entry_price': 10.0, 'vol': 100,
              'total_cost': 1000.0, 'fund_used': 1000.0}
    pd.DataFrame([dict(common, action='sell')]).reindex(
        columns=TRADE_RECORD_COLUMNS).to_csv(sell_p, index=False, encoding='utf-8-sig')
    pd.DataFrame([dict(common, action='buy')]).reindex(
        columns=TRADE_RECORD_COLUMNS).to_csv(buy_p, index=False, encoding='utf-8-sig')

    ib1 = ScriptedIB(raise_on_snap=True)        # 两账户严格快照均失败
    ib2 = ScriptedIB(raise_on_snap=True)
    # 会话内"恰好"有平仓方向 fills（旧行为据此把账本标记成已平/两账户0持仓）
    ib1._fills = [mk_fill('X', 'BUY', 100, 10.1, 'STK')]
    ib2._fills = [mk_fill('X', 'SELL', 100, 10.2, 'STK')]

    cm = C.CloseManager(ib1, ib2, 'D1', 'D2', sell_p, buy_p, trade_store=None)
    clear_log()
    await cm.reconcile_csv_with_fills()         # 旧行为: 走到 _backfill_csv 把两行都标记已平

    def _is_closed(path):
        df = pd.read_csv(path)
        v = df['close_datetime'].iloc[0]
        return pd.notna(v) and str(v).strip() != ''

    sell_closed = _is_closed(sell_p)
    buy_closed = _is_closed(buy_p)
    ok = check(
        'F5 快照失败 → 旧CSV路径也不再标记已平（账本不强制"两账户0持仓"）',
        not sell_closed and not buy_closed
        and crit('退出补写整体取消', '退出前持仓终态确认失败'),
        f'sell_closed={sell_closed} buy_closed={buy_closed}')
    return ok


async def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    failures = []
    for name, fn in [('F1', f1), ('F2', f2), ('F3', f3), ('F4', f4), ('F5', f5)]:
        r = await fn() if asyncio.iscoroutinefunction(fn) else fn()
        if not r:
            failures.append(name)

    asyncio.sleep = _real_sleep
    _log.removeHandler(_cap)
    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 五处缺陷修复回归测试全部通过 '
          '(同名期权不入快照 / 早到bar不丢弃 / 开盘价取正式bar / 收盘兜底恒存在 / 快照失败不记已平)')


# Windows 控制台默认代码页无法输出中文/emoji，统一 UTF-8，避免测试进程崩溃
asyncio.run(main())
