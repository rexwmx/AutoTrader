# -*- coding: utf-8 -*-
"""
回归测试：退出前事件流回填（close.py _backfill_store_account）硬化（2026-09-22）

修复的三个缺陷（用户代码审查发现，全部复现并断言）：
  B1 补记数量写死为全部未平批次，不按实际成交封顶
     —— 强平失败时，只要当天有过同方向成交，残留仓位就被补记成"已平"
  B2 成交总量/均价按"全天同方向所有成交"计算，包含当日已入账的平仓
     —— 数量/均价双污染、"成交 > 未平批次"严重告警误报
  B3 账户仍持仓（强平失败/残留）也照常补记"已平"；快照失败同样照补
     —— 旧 CSV 路径有"成交覆盖开仓量"保护，事件流路径没有，也没有测试

覆盖断言：
  T1 守卫：账户仍持仓（严格快照确认）→ 一律不补"已平"，只 CRITICAL
  T2 去重：会话 fills 全部已入账 → 0 补记、无"成交>未平批次"误报
  T3 封顶：只记 min(未入账成交, 未平批次)；未入账<未平 → 缺口 CRITICAL
  T4 价格：只用未入账部分的成交定价（不混已入账旧成交）
  T5 重复成交：未入账 > 未平批次 → 只记未平部分 + CRITICAL
  T6 跨日：账户已平、会话无同方向成交、账本仍有未平 → 不盲补 + CRITICAL
  T7 幂等：同一场景补写两次，第二次 0 新增
  T8 快照失败：holding=None → 整账户禁止补记"已平"
  T9 R9 覆盖校验改用"未入账 fills"：已入账旧成交不再凑够数
运行：python test/test_exit_backfill.py
"""
import asyncio
import logging
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

_real_sleep = asyncio.sleep


async def _fast_sleep(d, *a, **k):
    await _real_sleep(0)


asyncio.sleep = _fast_sleep

import close as C                                  # noqa: E402
from trade_store import TradeStore                 # noqa: E402
from logger import get_logger                      # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix='exit_backfill_'))

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
    """记录中是否存在 CRITICAL 且包含任一关键词（全匹配任一即 True）"""
    return any(r.levelno >= logging.CRITICAL and any(k in r.getMessage() for k in kw)
               for r in records)


def clear_log():
    records.clear()


# ---- Fake 对象 ----
class _Exec:
    def __init__(self, side, shares, price, ts):
        self.side = side
        self.shares = shares
        self.price = price
        self.time = ts  # epoch 秒（升序）


class FakeFill:
    def __init__(self, symbol, side, shares, price, ts=1_700_000_000):
        self.execution = _Exec(side, shares, price, ts)
        self.contract = type('C', (), {'symbol': symbol})()
        self.time = ts


class FakePos:
    def __init__(self, symbol, position, account):
        self.symbol = symbol
        self.position = position
        self.avgCost = 10.0
        self.account = account
        self.contract = type('C', (), {'symbol': symbol, 'conId': 1})()


def make_cm(store, snap1=None, snap2=None):
    class _IB:
        def __init__(s, snap):
            s._snap = snap or []

        async def reqPositionsAsync(s):
            return list(s._snap)

        def fills(s):
            return []

    return C.CloseManager(
        _IB(snap1), _IB(snap2), 'D1', 'D2',
        'sell.csv', 'buy.csv', trade_store=store,
    )


def close_rows(store, account, symbol):
    """读回该标的事件流中的平仓行（CLOSE 系）"""
    df = store._read(store.stock_csv(account, symbol))
    if df.empty:
        return []
    rows = df[df['event_type'].isin(('REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE'))]
    return rows


def new_store(name):
    s = TradeStore(TMP / name)
    s.init_summary_files()
    return s


def check(name, ok, detail=''):
    print(f'[{"ok" if ok else "FAIL"}] {name}' + (f' — {detail}' if detail else ''))
    return ok


NOW = datetime.now()  # 与生产 append_close(event_datetime=now()) 同款"今日"日期


def scenario_t1():
    """T1 守卫：账户仍持仓 → 不补"已平"（强平失败残留的核心防护）"""
    clear_log()
    s = new_store('t1')
    s.append_open('account1', 'AAA', price=10.0, volume=240, strategy='hedge', reason='开仓')
    s.append_close('account1', 'AAA', close_action='buy', volume=60, price=10.10,
                   event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')
    # 会话 fills：60（已入账）+ 40（强平部分成交，未入账）
    fills = [FakeFill('AAA', 'BUY', 60, 10.10, ts=1_700_000_100),
             FakeFill('AAA', 'BUY', 40, 10.20, ts=1_700_000_200)]
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY',
                                   holding_symbols={'AAA'})  # 严格快照：仍持有 AAA
    lots = s.get_open_lots('account1', 'AAA')
    rem = sum(int(l['remaining']) for l in lots)
    rows = close_rows(s, 'account1', 'AAA')
    booked = sum(int(float(r['volume'])) for _, r in rows.iterrows())
    ok = (p == 0 and rem == 180 and booked == 60
          and crit('仍持有实际持仓', '不能记成已平', '不补记'))
    print(f'   补记行={p} 未平批次={rem}/180 已入账={booked}/60 critical={crit("仍持有实际持仓")}')
    return check('T1 仍持仓(强平失败)不补记已平', ok)


def scenario_t2():
    """T2 去重：会话 fills 全部已入账 → 0 补记，且无"成交>未平批次"误报"""
    clear_log()
    s = new_store('t2')
    s.append_open('account1', 'BBB', price=10.0, volume=200, strategy='hedge', reason='开仓')
    s.append_close('account1', 'BBB', close_action='buy', volume=100, price=10.0,
                   event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')
    fills = [FakeFill('BBB', 'BUY', 100, 10.0, ts=1_700_000_100)]  # 与已入账的同一笔
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'BBB'))
    booked = sum(int(float(r['volume']))
                 for _, r in close_rows(s, 'account1', 'BBB').iterrows())
    # 误报断言：不允许出现把已入账 fills 当成未入账的"成交>未平批次"CRITICAL
    false_alarm = any(r.levelno >= logging.CRITICAL
                      and '> 未平批次' in r.getMessage() for r in records)
    ok = p == 0 and rem == 100 and booked == 100 and not false_alarm
    print(f'   补记行={p} 未平批次={rem}/100 已入账={booked}/100 误报={false_alarm}')
    return check('T2 已入账成交不重复补记(去重+无误报)', ok)


def scenario_t3():
    """T3 封顶：只记 min(未入账, 未平批次)；缺口必须 CRITICAL"""
    clear_log()
    s = new_store('t3')
    s.append_open('account1', 'CCC', price=10.0, volume=240, strategy='hedge', reason='开仓')
    s.append_close('account1', 'CCC', close_action='buy', volume=60, price=10.10,
                   event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')
    # 会话 fills：60（已入账）+ 100（未入账）= 160；未入账 100 < 未平 180
    fills = [FakeFill('CCC', 'BUY', 60, 10.10, ts=1_700_000_100),
             FakeFill('CCC', 'BUY', 100, 10.25, ts=1_700_000_200)]
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'CCC'))
    new_rows = [r for _, r in close_rows(s, 'account1', 'CCC').iterrows()
                if r['strategy'] == 'reconcile']
    booked_new = sum(int(float(r['volume'])) for r in new_rows)
    ok = (p == 1 and booked_new == 100 and rem == 80 and crit('缺口'))
    print(f'   补记行={p} 新补股数={booked_new}/100 未平={rem}/80 critical={crit("缺口")}')
    return check('T3 补记量按未入账成交封顶+缺口告警', ok)


def scenario_t4():
    """T4 价格纯净：补记价 = 未入账成交价（不混已入账旧成交）"""
    clear_log()
    s = new_store('t4')
    s.append_open('account2', 'DDD', price=10.0, volume=200, strategy='hedge', reason='开仓')
    s.append_close('account2', 'DDD', close_action='sell', volume=100, price=10.0,
                   event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')
    # 会话 fills：100@10.0（已入账旧单）+ 100@20.0（未入账新单）
    fills = [FakeFill('DDD', 'SELL', 100, 10.0, ts=1_700_000_100),
             FakeFill('DDD', 'SELL', 100, 20.0, ts=1_700_000_200)]
    cm = make_cm(s)
    p = cm._backfill_store_account('account2', fills, 'SELL', holding_symbols=set())
    new_rows = [r for _, r in close_rows(s, 'account2', 'DDD').iterrows()
                if r['strategy'] == 'reconcile']
    prices = [float(r['price']) for r in new_rows if int(float(r['volume'])) == 100]
    ok = (p == 1 and prices == [20.0])
    print(f'   补记行={p} 补记价={prices}/[20.0]（旧版会记混合均价 15.0）')
    return check('T4 价格只用未入账成交(不混旧成交)', ok)


def scenario_t5():
    """T5 重复成交：未入账 > 未平批次 → 只记未平部分 + CRITICAL"""
    clear_log()
    s = new_store('t5')
    s.append_open('account1', 'EEE', price=10.0, volume=100, strategy='hedge', reason='开仓')
    fills = [FakeFill('EEE', 'BUY', 100, 10.0, ts=1_700_000_100),
             FakeFill('EEE', 'BUY', 60, 10.5, ts=1_700_000_200)]  # 160 > 未平 100
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'EEE'))
    new_rows = [r for _, r in close_rows(s, 'account1', 'EEE').iterrows()
                if r['strategy'] == 'reconcile']
    booked = sum(int(float(r['volume'])) for r in new_rows)
    ok = (p >= 1 and booked == 100 and rem == 0 and crit('重复成交', '延迟开仓'))
    print(f'   补记行={p} 补记股数={booked}/100 未平={rem}/0 critical={crit("重复成交", "延迟开仓")}')
    return check('T5 重复成交只记未平部分+告警', ok)


def scenario_t6():
    """T6 跨日：账户已平、会话无同方向成交、账本仍有未平 → 不盲补 + CRITICAL"""
    clear_log()
    s = new_store('t6')
    s.append_open('account1', 'FFF', price=10.0, volume=240, strategy='hedge', reason='开仓')
    s.append_close('account1', 'FFF', close_action='buy', volume=100, price=10.0,
                   event_datetime=NOW, strategy='reconcile', reason='跨日延迟入账')
    fills = []  # 会话内没有任何同方向成交
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'FFF'))
    ok = (p == 0 and rem == 140 and crit('跨日'))
    print(f'   补记行={p} 未平={rem}/140 critical={crit("跨日")}')
    return check('T6 无会话成交不盲补+人工核对告警', ok)


def scenario_t7():
    """T7 幂等：同一 fills 补写两次 → 第二次 0 新增（不重复入账）"""
    clear_log()
    s = new_store('t7')
    s.append_open('account1', 'GGG', price=10.0, volume=100, strategy='hedge', reason='开仓')
    fills = [FakeFill('GGG', 'BUY', 100, 12.0, ts=1_700_000_100)]
    cm = make_cm(s)
    p1 = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    n1 = sum(int(float(r['volume']))
             for _, r in close_rows(s, 'account1', 'GGG').iterrows() if r['strategy'] == 'reconcile')
    p2 = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=set())
    n2 = sum(int(float(r['volume']))
             for _, r in close_rows(s, 'account1', 'GGG').iterrows() if r['strategy'] == 'reconcile')
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'GGG'))
    ok = (p1 == 1 and n1 == 100 and p2 == 0 and n2 == 100 and rem == 0)
    print(f'   第一次={p1}行/{n1}股  第二次={p2}行(累计{n2}股) 未平={rem}')
    return check('T7 重复补写幂等(第二次0新增)', ok)


def scenario_t8():
    """T8 快照失败：holding=None → 整账户禁止补记"已平"（绝不把未知状态记成已平）"""
    clear_log()
    s = new_store('t8')
    s.append_open('account1', 'HHH', price=10.0, volume=100, strategy='hedge', reason='开仓')
    fills = [FakeFill('HHH', 'BUY', 100, 12.0, ts=1_700_000_100)]
    cm = make_cm(s)
    p = cm._backfill_store_account('account1', fills, 'BUY', holding_symbols=None)
    rem = sum(int(l['remaining']) for l in s.get_open_lots('account1', 'HHH'))
    ok = (p == 0 and rem == 100 and crit('整体禁用', '无法确认'))
    print(f'   补记行={p} 未平={rem}/100 critical={crit("整体禁用", "无法确认")}')
    return check('T8 快照失败整账户禁用补记', ok)


async def scenario_t9():
    """T9 R9 覆盖校验：have = 未入账 fills（已入账旧成交不再凑够数）"""
    ok = True
    # T9a：open 240，盘中已入账 60（含其 fills 60），会话共 240 → 未入账 180 ≥ 180 → 无需等待重取
    clear_log()
    s = new_store('t9a')
    s.append_open('account1', 'JJJ', price=10.0, volume=240, strategy='hedge', reason='开仓')
    s.append_close('account1', 'JJJ', close_action='buy', volume=60, price=10.0,
                   event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')

    class _IB2:
        calls = 0

        def fills(self):
            self.calls += 1
            return [FakeFill('JJJ', 'BUY', 60, 10.0, ts=1_700_000_100),
                    FakeFill('JJJ', 'BUY', 180, 11.0, ts=1_700_000_300)]

    cm = C.CloseManager(_IB2(), object(), 'D1', 'D2', 'sell.csv', 'buy.csv', trade_store=s)
    await cm._ensure_fills_coverage(cm.ib1.fills(), [])
    a = (cm.ib1.calls == 1)  # 覆盖校验通过 → 不等 5s 重取
    print(f'   fills()调用次数={cm.ib1.calls}/1（未入账180≥计划180，无等待重取）')
    ok &= check('T9a 已入账旧成交不凑够覆盖数(无需重取)', a, f'calls={cm.ib1.calls}')

    # T9b（反例锚定）："已入账 fills 被当成 have"的旧口径假通过：
    # open 120、已入账 60、会话 fills 仅 60（全部已入账）→ 未入账 have 必须 = 0
    # → 覆盖校验判"不足"（旧版 60 ≥ 60 会假通过 → 拿旧 fills 顶替补记）
    clear_log()
    s2 = new_store('t9b')
    s2.append_open('account1', 'KKK', price=10.0, volume=120, strategy='hedge', reason='开仓')
    s2.append_close('account1', 'KKK', close_action='buy', volume=60, price=10.0,
                    event_datetime=NOW, strategy='dynamic_tp', reason='盘中平仓')
    cm2 = make_cm(s2)
    fills = [FakeFill('KKK', 'BUY', 60, 10.0, ts=1_700_000_100)]
    have = cm2._unbooked_have_by_symbol('account1', fills, 'BUY')
    b = (have.get('KKK', 0) == 0)   # 60(会话) − 60(已入账) = 0
    print(f'   未入账have={have.get("KKK")}/0（旧口径 would be 60 → 假覆盖）')
    ok &= check('T9b 已入账fills不再凑够覆盖数', b, f'have={have}')
    return ok


async def main():
    failures = []
    for name, fn in [
        ('T1', scenario_t1), ('T2', scenario_t2), ('T3', scenario_t3),
        ('T4', scenario_t4), ('T5', scenario_t5), ('T6', scenario_t6),
        ('T7', scenario_t7), ('T8', scenario_t8),
    ]:
        clear_log()
        if not fn():
            failures.append(name)
    clear_log()
    if not await scenario_t9():
        failures.append('T9')

    _log.removeHandler(_cap)
    asyncio.sleep = _real_sleep
    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 退出前事件流回填硬化回归测试全部通过（仍持仓不记已平 / 去重 / 封顶 / 价格纯净 / 幂等）')


try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
