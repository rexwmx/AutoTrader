# -*- coding: utf-8 -*-
"""回归测试：退出前 CSV 补写的平仓时间时区处理（9.14/9.16/9.18 事故）

背景：
  TWS(paper) 以「UTC 墙钟」回报成交时间。旧代码直接取 ib_async execution.time 的
  strftime 写入 CSV，在本机(美东)上被系统性多写 8 小时
  （09-14 SBET/ASST、09-16 EOSE、09-18 GNRC/RXRX/ABSI 各一条）。
  修复后：close.py _backfill_csv 将时间归一到 UTC 时刻 → 转美东(US/Eastern) 写入，
  并用「成交回报收到时刻」(fill.time, 机器时钟) 交叉校验，偏差>10分钟自动改用收到时刻。

断言（本机需为美东时区；EDT 期间 UTC-4）：
  T1  正确解码(aware UTC 13:38:07)          → CSV 写 09:38:07
  T2  15:50:09 EDT 成交(UTC 19:50:09)      → CSV 写 15:50:09（不再成 23:50:09）
  T3  naive 墙钟(约定为 UTC, 19:50:09)     → CSV 写 15:50:09
  T4  时区约定失效(exec 偏 8h) + 收到时刻   → 自动降级收到时刻 15:50:12 + CRITICAL
  T5  execution.time 完全不可解析           → 回退收到时刻/机器时钟，不写坏值
"""
import datetime
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from constants import TRADE_RECORD_COLUMNS
from close import CloseManager, _to_utc_instant

import close as C

UTC = datetime.timezone.utc
EST4 = datetime.timezone(datetime.timedelta(hours=-4))  # EDT, 仅用于构造期望值

TMP = Path(tempfile.mkdtemp(prefix='close_tz_test_'))

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


def has_critical(kw):
    return any(r.levelno >= logging.CRITICAL and kw in r.getMessage() for r in records)


# ---- fake fills ----
def make_fill(side, symbol, shares, price, exec_time, recv_time=None):
    return SimpleNamespace(
        execution=SimpleNamespace(
            side=side, shares=shares, price=price, time=exec_time
        ),
        contract=SimpleNamespace(symbol=symbol),
        time=recv_time,
    )


def make_csv(name, code, action, vol, entry):
    p = TMP / name
    base = {
        'datetime': '2026-09-18 09:30:22', 'exchange': 'NASDAQ', 'industry': 'X',
        'total_cost': 0, 'fund_used': 0,
        'close_datetime': '', 'close_price': '', 'close_vol': '',
        'close_fund': '', 'gross_profit': '', 'profit': ''
    }
    df = pd.DataFrame([{**base, 'code': code, 'action': action,
                        'vol': vol, 'entry_price': entry}], columns=TRADE_RECORD_COLUMNS)
    df.to_csv(p, index=False, encoding='utf-8-sig')
    return p


def read_close(p, code):
    df = pd.read_csv(p)
    row = df[df['code'] == code].iloc[0]
    v = str(row['close_datetime']).strip()
    assert v not in ('nan', 'NaN', ''), 'close_datetime 仍为空!'
    return v


class _NIB:
    pass


def build_mgr(sell_csv, buy_csv):
    return CloseManager(_NIB(), _NIB(), 'D1', 'D2', sell_csv, buy_csv)


def main():
    # Windows 控制台默认代码页（如 cp1252/cp936）无法输出中文/emoji，统一 UTF-8，避免测试进程崩溃
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    failures = []

    def chk(tag, cond, detail=''):
        line = ('ok  ' if cond else 'FAIL') + f' {tag}' + ((' | ' + detail) if detail else '')
        print(line)
        if not cond:
            failures.append(tag)

    # ---------- 单元: _to_utc_instant ----------
    chk('T0a aware UTC 原样', _to_utc_instant(datetime.datetime(2026, 9, 18, 13, 38, 7))
        == datetime.datetime(2026, 9, 18, 13, 38, 7, tzinfo=UTC))
    aware = datetime.datetime(2026, 9, 18, 19, 50, 9, tzinfo=EST4)
    chk('T0b aware EST→UTC', _to_utc_instant(aware)
        == datetime.datetime(2026, 9, 18, 23, 50, 9, tzinfo=UTC))
    chk('T0c naive当作UTC墙钟', _to_utc_instant(datetime.datetime(2026, 9, 18, 19, 50, 9))
        == datetime.datetime(2026, 9, 18, 19, 50, 9, tzinfo=UTC))
    chk('T0d epoch float', _to_utc_instant(1790000000.0)
        == datetime.datetime.fromtimestamp(1790000000.0, tz=UTC))
    chk('T0e None→None', _to_utc_instant(None) is None)
    chk('T0f 垃圾→None', _to_utc_instant('not-a-time') is None)

    # ---------- T1: 正确解码（execution.time = 真正 UTC 时刻, aware） ----------
    p1 = make_csv('t1.csv', 'ABSI', 'sell', 107, 9.59)
    m1 = build_mgr(p1, p1)
    fills1 = [
        make_fill('BUY', 'ABSI', 7, 9.44,
                  exec_time=datetime.datetime(2026, 9, 18, 13, 38, 7, tzinfo=UTC),
                  recv_time=datetime.datetime(2026, 9, 18, 13, 38, 8, tzinfo=UTC)),
        make_fill('BUY', 'ABSI', 100, 9.48,
                  exec_time=datetime.datetime(2026, 9, 18, 13, 38, 10, tzinfo=UTC),
                  recv_time=datetime.datetime(2026, 9, 18, 13, 38, 11, tzinfo=UTC)),
    ]
    m1._backfill_csv(fills1, 'BUY', 'sell', p1)
    v1 = read_close(p1, 'ABSI')
    # 取最后一笔成交(13:38:10 UTC)
    chk('T1 aware-UTC 解码 → 09:38:10(EDT)', v1 == '2026-09-18 09:38:10', f'got={v1}')

    # ---------- T2: 15:50:09 EDT 强平成交 (UTC 19:50:09) ----------
    p2 = make_csv('t2.csv', 'GNRC', 'buy', 4, 213.25)
    m2 = build_mgr(p2, p2)
    fills2 = [make_fill('SELL', 'GNRC', 4, 208.04,
                        exec_time=datetime.datetime(2026, 9, 18, 19, 50, 9, tzinfo=UTC),
                        recv_time=datetime.datetime(2026, 9, 18, 19, 50, 10, tzinfo=UTC))]
    m2._backfill_csv(fills2, 'SELL', 'buy', p2)
    v2 = read_close(p2, 'GNRC')
    chk('T2 15:50:09 EDT 成交 → 15:50:09（旧 bug 会写 23:50:09）',
        v2 == '2026-09-18 15:50:09', f'got={v2}')

    # ---------- T3: naive 墙钟（约定 UTC） ----------
    p3 = make_csv('t3.csv', 'EOSE', 'buy', 241, 4.11)
    m3 = build_mgr(p3, p3)
    fills3 = [make_fill('SELL', 'EOSE', 241, 3.94,
                        exec_time=datetime.datetime(2026, 9, 16, 19, 50, 9),
                        recv_time=datetime.datetime(2026, 9, 16, 19, 50, 9, tzinfo=UTC))]
    m3._backfill_csv(fills3, 'SELL', 'buy', p3)
    v3 = read_close(p3, 'EOSE')
    chk('T3 naive(UTC墙钟) 19:50:09 → 15:50:09', v3 == '2026-09-16 15:50:09', f'got={v3}')

    # ---------- T4: 时区约定失效（exec 偏移 8h）→ 降级收到时刻 + CRITICAL ----------
    p4 = make_csv('t4.csv', 'RXRX', 'sell', 280, 3.56)
    m4 = build_mgr(p4, p4)
    # 错误解码的样子: exchange 真实 15:50:09 EDT = 19:50:09 UTC，
    # 但被当成 UTC+8 墙钟解码 → 时刻 = 27:50:09 UTC (次日03:50:09)
    bad_exec = datetime.datetime(2026, 9, 19, 3, 50, 9, tzinfo=UTC)
    good_recv = datetime.datetime(2026, 9, 18, 19, 50, 12, tzinfo=UTC)  # 收到时刻≈真实
    fills4 = [make_fill('BUY', 'RXRX', 280, 3.74, exec_time=bad_exec, recv_time=good_recv)]
    m4._backfill_csv(fills4, 'BUY', 'sell', p4)
    v4 = read_close(p4, 'RXRX')
    chk('T4 偏差>10分钟 → 降级收到时刻 15:50:12', v4 == '2026-09-18 15:50:12', f'got={v4}')
    chk('T4 CRITICAL 告警已记', has_critical('时区约定很可能已失效'))

    # ---------- T5: execution.time 不可解析 → 回退收到时刻 ----------
    p5 = make_csv('t5.csv', 'TEM', 'sell', 12, 81.16)
    m5 = build_mgr(p5, p5)
    recv5 = datetime.datetime(2026, 9, 18, 19, 50, 20, tzinfo=UTC)
    fills5 = [make_fill('SELL', 'TEM', 12, 77.84, exec_time='garbage', recv_time=recv5)]
    m5._backfill_csv(fills5, 'SELL', 'sell', p5)
    v5 = read_close(p5, 'TEM')
    chk('T5 不可解析 → 回退收到时刻 15:50:20', v5 == '2026-09-18 15:50:20', f'got={v5}')

    # ---------- T6: 无收到时刻 + 正常 exec 时刻 → 直接采用 + WARNING ----------
    p6 = make_csv('t6.csv', 'LIFE', 'buy', 27, 37.04)
    m6 = build_mgr(p6, p6)
    fills6 = [make_fill('SELL', 'LIFE', 27, 36.25,
                        exec_time=datetime.datetime(2026, 9, 18, 19, 50, 3, tzinfo=UTC),
                        recv_time=None)]
    m6._backfill_csv(fills6, 'SELL', 'buy', p6)
    v6 = read_close(p6, 'LIFE')
    chk('T6 无收到时刻 → 用 execution.time 15:50:03', v6 == '2026-09-18 15:50:03', f'got={v6}')

    print()
    if failures:
        print(f'❌ {len(failures)} 项失败: {failures}')
        return 1
    print('✅ 时区回归测试全部通过（本机时区应为美东；其他时区运行请先确认期望值）')
    return 0


if __name__ == '__main__':
    sys.exit(main())
