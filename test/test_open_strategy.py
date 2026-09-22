# -*- coding: utf-8 -*-
"""开盘前两分钟特殊策略（OpenWindowStrategy）详细测试

覆盖矩阵（做多/做空镜像）：
  - 情况1（不操作，窗口内抑制原策略）+ 窗口结束后原策略恢复
  - 情况2/3/4（bar1 立即平仓）含 H/2、C 边界值
  - 情况5/6（bar2 close 高于/低于 bar1 close 的两个分支）
  - 情况7 修订版（下跌<2% 且守住中间值；中间值/跌幅两个失败分支）
  - 情况4 的 C=H/2 边界、C=0.5 下边界、H=1.0/2.0/2.99/3.0/4.0 带宽边界
  - 双腿独立（同一 bar 一腿平仓、一腿不操作）
  - 特殊平仓与原策略同腿信号的去重抑制 + 去重标记回滚
  - 特殊平仓执行失败 → 定案回滚 → 原策略接管
  - 跨交易日状态重置
  - Runner 层：开盘窗口 bar 放行建仓前门控 / 窗口外仍门控 / 无持仓不误动 / 失败重放
运行：python test/test_open_strategy.py   （AutoTrader 根目录下；任意 cwd 均可）
"""
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import pytz

import strategy_runner as SR
from strategy import DynamicTPStrategy, OpenWindowStrategy, Bar, PositionView
from strategy.open_window import UNSET
from strategy_runner import StrategyRunner

EST = pytz.timezone('US/Eastern')
OPEN = datetime(2026, 9, 10, 9, 30, 0)                 # 开盘（美东）
O1 = EST.localize(OPEN)                                # bar1：第一分钟 [09:30, 09:31)
O2 = EST.localize(OPEN + timedelta(seconds=60))        # bar2：第二分钟 [09:31, 09:32)
O3 = EST.localize(OPEN + timedelta(seconds=120))       # bar3：窗口外 [09:32, ...)
O_PRE29 = EST.localize(datetime(2026, 9, 10, 9, 29, 0))
DAY2_O1 = EST.localize(OPEN + timedelta(days=1))       # 次日第一分钟
ENTRY = EST.localize(OPEN + timedelta(seconds=20))     # 生产式建仓时刻（开盘+20s）
ENTRY_EARLY = EST.localize(datetime(2026, 9, 10, 9, 25, 0))  # 提前建仓（用于验证窗口内原策略信号抑制/去重）


def bar_ohlc(sym: str, dt: datetime, o: float, h: float, l: float, c: float) -> Bar:
    return Bar(sym, dt, o, h, l, c, 1000)


def bar1(o, h, l, c, sym='X'):
    return bar_ohlc(sym, O1, o, h, l, c)


def bar2(o, h, l, c, sym='X'):
    return bar_ohlc(sym, O2, o, h, l, c)


def bar3(o, h, l, c, sym='X'):
    return bar_ohlc(sym, O3, o, h, l, c)


POS_LONG = PositionView('X', 0, 0.0, 10, 100.0)     # 做多腿：账户2 +10 @100
POS_SHORT = PositionView('X', -10, 100.0, 0, 0.0)   # 做空腿：账户1 -10 @100
POS_BOTH = PositionView('X', -10, 100.0, 10, 100.0)
POS_NONE = PositionView('X', 0, 0.0, 0, 0.0)


def mk(open_time=O1):
    """新建（外壳, 内层原策略）"""
    inner = DynamicTPStrategy()
    return OpenWindowStrategy(inner, open_time=open_time), inner


def feed(strat, pos, bars):
    sigs = []
    for b in bars:
        sigs.extend(strat.on_bar(b, pos))
    return sigs


def is_special(sig):
    return '开盘前两分钟特殊策略' in sig.reason


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


def build_runner(snap1=None, snap2=None, open_time=O1):
    inner = DynamicTPStrategy()
    strategy = OpenWindowStrategy(inner, open_time=open_time)
    fcm = FakeCloseManager()
    runner = StrategyRunner(
        strategy=strategy,
        ib1=object(), ib2=object(),
        account1='D1', account2='D2',
        sell_csv_path='sell.csv', buy_csv_path='buy.csv',
        close_manager=fcm,
        open_time=open_time,
    )
    runner.active = True       # 测试环境直接激活，绕过 start()/delay
    runner.activate_at = None
    calls = {'n': 0}

    async def fake_get_positions(ib, account=''):
        calls['n'] += 1
        return dict((snap1 if account == 'D1' else snap2) or {})

    SR.get_positions = fake_get_positions
    return runner, strategy, inner, fcm, calls


def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    failures = []
    total = [0]

    def check(name, ok, detail=''):
        total[0] += 1
        print(f'[{"ok" if ok else "FAIL"}] {name}{(": " + str(detail)) if detail else ""}')
        if not ok:
            failures.append(name)

    # ==================== 做多腿（账户2） ====================
    # ---- L1 情况1：H<1% 且 C<1% → 不操作；窗口内抑制原策略，窗口结束后原策略恢复 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 100.4, 99.9, 100.3),            # bar1: H=+0.4%, C=+0.3% → 情况1
        bar2(100.3, 101.2, 99.0, 101.1),            # bar2: 窗口内
        bar3(101.1, 101.2, 100.4, 100.5),           # bar3: 窗口外 → 原策略接管
    ])
    check('L1 情况1(不操作)：前两分钟0信号，第三分钟原策略触发1信号',
          len(s) == 1 and not is_special(s[0]) and s[0].account == 'account2'
          and s[0].side == 'sell' and s[0].volume == 10,
          f'sigs={[(x.account, x.side, x.volume) for x in s]}')

    # ---- L2 情况2：H∈[1,2) 且 C∈[0.5,1) → bar1 立即平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 101.2, 99.9, 100.7),            # H=+1.2%, C=+0.7% → 情况2
        bar2(100.7, 100.9, 100.4, 100.5),
    ])
    check('L2 情况2：bar1立即平多（sell 10，账户2）',
          len(s) == 1 and is_special(s[0]) and '情况2' in s[0].reason
          and s[0].account == 'account2' and s[0].side == 'sell'
          and s[0].volume == 10 and s[0].open_action == 'buy',
          f'sigs={[(x.account, x.side, x.volume) for x in s]} {s and s[0].reason}')

    # ---- L3 情况3：H∈[2,3) 且 C∈[1,2) → 立即平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [bar1(100.0, 102.1, 99.8, 101.3)])   # H=+2.1%, C=+1.3%
    check('L3 情况3：bar1立即平多',
          len(s) == 1 and is_special(s[0]) and '情况3' in s[0].reason
          and s[0].side == 'sell' and s[0].volume == 10, len(s))

    # ---- L4 情况4：H∈[3,4) 且 C≤2 且 C≥H/2 → 立即平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [bar1(100.0, 103.2, 99.9, 101.8)])   # H=+3.2%, C=+1.8%≥1.6
    check('L4 情况4：bar1立即平多',
          len(s) == 1 and is_special(s[0]) and '情况4' in s[0].reason
          and s[0].side == 'sell', len(s))

    # ---- L4b 情况4 边界未命中（C < H/2）→ 走原策略 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 103.0, 99.9, 101.4),            # H=+3.0%, C=+1.4% < H/2=1.5 → 未命中
        bar2(101.4, 101.6, 100.8, 101.3),           # 窗口内，无特殊动作
        bar3(101.3, 101.5, 100.4, 100.5),           # 窗口外 → 原策略触发
    ])
    check('L4b 情况4未命中(C<H/2)：前两分钟0信号，原策略第三分钟接管',
          len(s) == 1 and not is_special(s[0]), f'sigs={len(s)}')

    # ---- L4c 情况4 边界 C = H/2（下界含）→ 命中 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [bar1(100.0, 103.2, 99.9, 101.6)])   # C=+1.6% = H/2
    check('L4c 情况4边界：C=H/2 命中，bar1立即平多',
          len(s) == 1 and is_special(s[0]) and '情况4' in s[0].reason, len(s))

    # ---- L5 情况5：H∈[1,2) 且 C∈[1,2)，bar2 close 低于 bar1 → 第二分钟平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 101.4, 99.9, 101.1),            # H=+1.4%, C=+1.1% → 情况5(待bar2)
        bar2(101.1, 101.3, 100.4, 100.5),           # 100.5 < 101.1 → 平仓
    ])
    check('L5 情况5：bar2 close低于bar1 → 第二分钟平多（共1单）',
          len(s) == 1 and is_special(s[0]) and '情况5' in s[0].reason
          and s[0].side == 'sell' and s[0].volume == 10, len(s))

    # ---- L5b 情况5：bar2 close 高于 bar1 → 不平仓，窗口后原策略恢复 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 101.4, 99.9, 101.1),
        bar2(101.1, 101.3, 101.0, 101.2),           # 101.2 > 101.1 → 不平
        bar3(101.2, 101.3, 100.4, 100.5),           # 窗口外 → 原策略触发
    ])
    check('L5b 情况5未触发分支：前两分钟0信号，第三分钟原策略接管',
          len(s) == 1 and not is_special(s[0]), f'sigs={len(s)}')

    # ---- L6 情况6：H∈[2,3) 且 C∈[2,3)，bar2 close 低于 bar1 → 平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 102.4, 99.9, 102.2),            # H=+2.4%, C=+2.2% → 情况6
        bar2(102.2, 102.4, 101.8, 101.9),           # 101.9 < 102.2 → 平仓
    ])
    check('L6 情况6：bar2 close低于bar1 → 第二分钟平多',
          len(s) == 1 and is_special(s[0]) and '情况6' in s[0].reason, len(s))

    # ---- L6b 情况6：bar2 close 高于 bar1 → 不平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 102.4, 99.9, 102.2),
        bar2(102.2, 102.5, 102.1, 102.3),           # 102.3 > 102.2 → 不平
        bar3(102.3, 102.4, 100.4, 100.5),           # 窗口外 → 原策略触发
    ])
    check('L6b 情况6未触发分支：前两分钟0信号，第三分钟原策略接管',
          len(s) == 1 and not is_special(s[0]), f'sigs={len(s)}')

    # ---- L7 情况7(修订)：H∈[3,4) 且 C≥2；bar2 下跌<2% 且守住 (o,h) 中间值 → 平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 103.4, 99.9, 102.5),            # H=+3.4%, C=+2.5% → 情况7(待bar2)
        bar2(102.5, 102.6, 101.85, 101.9),          # 跌0.59%<2% 且 101.9>中间值101.7 → 平仓
    ])
    check('L7 情况7(修订)：bar2温和回落仍守住中间值 → 第二分钟平多',
          len(s) == 1 and is_special(s[0]) and '情况7' in s[0].reason, len(s))

    # ---- L7b 情况7：跌破中间值 → 不平仓，窗口后原策略恢复 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 103.4, 99.9, 102.5),
        bar2(102.5, 102.6, 101.0, 101.1),           # 跌1.27%<2% 但 101.1<101.7 → 不平
        bar3(101.1, 101.3, 100.4, 100.5),           # 窗口外 → 原策略触发
    ])
    check('L7b 情况7跌破中间值分支：前两分钟0信号，第三分钟原策略接管',
          len(s) == 1 and not is_special(s[0]), f'sigs={len(s)}')

    # ---- L7c 情况7：bar2 下跌≥2% → 不平仓（修订后该分支不触发） ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 103.4, 99.9, 102.5),
        bar2(102.4, 102.6, 100.2, 100.3),           # 跌2.14%≥2% → 不平
    ])
    check('L7c 情况7下跌≥2%分支：窗口内0信号', len(s) == 0, f'sigs={len(s)}')

    # ---- L8 无持仓股票：同形态也不动作 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_NONE, [bar1(100.0, 101.2, 99.9, 100.7)])
    check('L8 无持仓：情况2形态也不产生信号', len(s) == 0, len(s))

    # ---- L9 跨交易日重置：首日情况1不应封死次日 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar_ohlc('X', O1, 100.0, 100.4, 99.9, 100.3),            # 首日 bar1 情况1
        bar_ohlc('X', O2, 100.3, 100.5, 99.8, 100.2),            # 首日 bar2
        bar_ohlc('X', DAY2_O1, 100.0, 101.2, 99.9, 100.7),       # 次日 bar1 情况2
    ])
    check('L9 跨日重置：次日第一分钟命中情况2仍平仓',
          len(s) == 1 and is_special(s[0]) and '情况2' in s[0].reason, f'sigs={len(s)}')

    # ---- L10 去重：特殊平仓与原策略同腿信号同时出现 → 只保留1单，并回滚原策略去重标记 ----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY_EARLY)       # 提前建仓 → 原策略可见 bar1
    s = feed(strat, POS_LONG, [
        bar1(100.0, 101.4, 99.9, 101.1),            # 原策略: 峰值1.1%未达平仓线; 特殊: 情况5
        bar2(101.1, 101.3, 100.4, 100.5),           # 特殊平仓 且 原策略回落触发 → 应只留1单
    ])
    check('L10 去重抑制：同腿只出1单（特殊策略），原策略去重标记已回滚',
          len(s) == 1 and is_special(s[0]) and 'X' not in inner.closed_acc2,
          f'sigs={len(s)} closed={inner.closed_acc2}')

    # ---- L11 情况1(不操作)在窗口内抑制原策略同腿信号（提前建仓使原策略可触发的场景）----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY_EARLY)
    s = feed(strat, POS_LONG, [
        bar_ohlc('X', O_PRE29, 101.0, 101.0, 101.0, 101.0),      # 盘前bar：原策略建立峰值(1.0%)
        bar1(100.0, 100.4, 99.9, 100.3),                          # 情况1 → 不操作；原策略bar1会触发→抑制
        bar2(100.3, 100.6, 100.1, 100.2),                         # 原策略bar2也会触发→抑制
        bar3(100.2, 100.5, 100.0, 100.55),                        # 窗口外 → 原策略恢复触发
    ])
    check('L11 情况1窗口抑制：窗口内0信号，第三分钟原策略恢复1信号',
          len(s) == 1 and not is_special(s[0]), f'sigs={len(s)}')

    # ==================== 做空腿（账户1，镜像） ====================
    # ---- S1 情况1镜像：不操作，窗口后原策略恢复 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 99.7, 99.5, 99.8),             # H跌0.3%, C跌0.2% → 情况1
        bar2(99.8, 99.9, 98.9, 99.0),              # 窗口内
        bar3(99.0, 99.1, 99.5, 99.7),              # 窗口外 → 原策略触发(做空腿买入回补)
    ])
    check('S1 情况1镜像：前两分钟0信号，第三分钟原策略触发1信号',
          len(s) == 1 and not is_special(s[0]) and s[0].account == 'account1'
          and s[0].side == 'buy' and s[0].volume == 10, f'sigs={len(s)}')

    # ---- S2 情况2镜像：H跌∈[1,2) 且 C跌∈[0.5,1) → 立即买入回补 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [bar1(100.0, 98.8, 98.6, 99.3)])   # H跌1.2%, C跌0.7%
    check('S2 情况2镜像：bar1立即平空（buy 10，账户1）',
          len(s) == 1 and is_special(s[0]) and '情况2' in s[0].reason
          and s[0].account == 'account1' and s[0].side == 'buy'
          and s[0].volume == 10 and s[0].open_action == 'sell', len(s))

    # ---- S3 情况3镜像 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [bar1(100.0, 97.9, 97.7, 98.7)])   # H跌2.1%, C跌1.3%
    check('S3 情况3镜像：bar1立即平空',
          len(s) == 1 and is_special(s[0]) and '情况3' in s[0].reason, len(s))

    # ---- S4 情况4镜像：C跌≥H跌/2 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [bar1(100.0, 96.8, 96.6, 98.2)])   # H跌3.2%, C跌1.8%≥1.6
    check('S4 情况4镜像：bar1立即平空',
          len(s) == 1 and is_special(s[0]) and '情况4' in s[0].reason, len(s))

    # ---- S5 情况5镜像：bar2 close 高于 bar1 → 平仓；与原策略同腿信号去重 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY_EARLY)
    s = feed(strat, POS_SHORT, [
        bar_ohlc('X', O_PRE29, 99.0, 99.0, 99.0, 99.0),           # 盘前bar：原策略建立低点(峰值1.0%)
        bar1(100.0, 98.6, 98.4, 98.9),                             # 情况5(待bar2)
        bar2(98.9, 99.3, 98.8, 99.2),                              # 99.2 > 98.9 → 平仓；原策略也触发→去重
    ])
    check('S5 情况5镜像+去重：第二分钟平空，共1单，原策略标记回滚',
          len(s) == 1 and is_special(s[0]) and '情况5' in s[0].reason
          and 'X' not in inner.closed_acc1, f'sigs={len(s)} closed={inner.closed_acc1}')

    # ---- S5b 情况5镜像未触发分支：bar2 close 低于 bar1 → 不平 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 98.6, 98.4, 98.9),
        bar2(98.9, 99.0, 98.7, 98.8),                             # 98.8 < 98.9 → 不平
    ])
    check('S5b 情况5镜像未触发分支：窗口内0信号', len(s) == 0, len(s))

    # ---- S6 情况6镜像（镜像语义：做多"bar2 close低于bar1"→做空"bar2 close高于bar1"）----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 97.6, 97.4, 97.8),                            # H跌2.4%, C跌2.2% → 情况6
        bar2(97.8, 98.9, 97.7, 98.5),                             # 98.5 > 97.8（探底回升）→ 平仓
    ])
    check('S6 情况6镜像：bar2 close高于bar1 → 第二分钟平空',
          len(s) == 1 and is_special(s[0]) and '情况6' in s[0].reason, len(s))

    # ---- S6b 情况6镜像未触发分支：bar2 close 仍低于 bar1 → 不平 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 97.6, 97.4, 97.8),
        bar2(97.8, 98.0, 97.5, 97.6),                             # 97.6 < 97.8 → 不平
    ])
    check('S6b 情况6镜像未触发分支：窗口内0信号', len(s) == 0, len(s))

    # ---- S7 情况7镜像(修订)：bar2 回涨<2% 且守住 (o,l) 中间值下方 → 平仓 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 97.0, 96.6, 97.5),                            # H跌3.0%, C跌2.5%
        bar2(97.5, 98.1, 97.9, 98.0),                             # 涨0.51%<2% 且 98.0<中间值98.3 → 平仓
    ])
    check('S7 情况7镜像(修订)：bar2回涨未破中间值 → 第二分钟平空',
          len(s) == 1 and is_special(s[0]) and '情况7' in s[0].reason, len(s))

    # ---- S7b 情况7镜像：bar2 回到中间值上方 → 不平；原策略窗口内信号被抑制，窗口后恢复 ----
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    s = feed(strat, POS_SHORT, [
        bar1(100.0, 97.0, 96.6, 97.5),
        bar2(97.5, 99.05, 97.4, 99.0),                            # 涨1.54%<2% 但 99.0>98.3 → 不平
        bar3(99.0, 99.2, 99.5, 99.7),                             # 窗口外 → 原策略触发
    ])
    check('S7b 情况7镜像未触发分支：前两分钟0信号，第三分钟原策略接管',
          len(s) == 1 and not is_special(s[0]) and s[0].account == 'account1', f'sigs={len(s)}')

    # ==================== 边界值专项（做多） ====================
    def boundary(name, h_up, c_up, expect_special, expect_case, note=''):
        """h_up/c_up 为相对开盘价的涨跌百分点；bar1(o=100, h=100+h_up, c=100+c_up)"""
        strat, inner = mk()
        strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
        b = bar1(100.0, 100.0 + h_up, 98.0, 100.0 + c_up)
        s = feed(strat, POS_LONG, [b])
        ok = (len(s) == 1 and is_special(s[0]) and f'情况{expect_case}' in s[0].reason) if expect_special \
             else (not any(is_special(x) for x in s))
        check(name, ok, f'{note} sigs={[(is_special(x), x.reason[:40]) for x in s]}')

    boundary('B1 H=1.0(下界含)→情况2', 1.0, 0.7, True, 2)
    boundary('B2 H=2.0 →情况3', 2.0, 1.1, True, 3)
    boundary('B3 H=1.99,C=2.0 →未命中情况5(C<2边界外)', 1.99, 2.0, False, None)
    boundary('B5 H=3.0,C=2.2 →情况7(待bar2，bar1无动作)', 3.0, 2.2, False, None)
    boundary('B6 H=4.0(上界不含) →未命中，走原策略', 4.0, 1.8, False, None)
    boundary('B7 C=0.5(下界含) →情况2', 1.2, 0.5, True, 2)
    boundary('B8 C=0.49 →未命中', 1.2, 0.49, False, None)
    boundary('B9 H=3.0,C=2.0 →情况4(优先于情况7)', 3.0, 2.0, True, 4)

    # ---- B4 H=2.99 →情况6（pending，需 bar2 兑现）----
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 102.99, 98.0, 102.4),            # H=+2.99%, C=+2.4% → 情况6(待bar2)
        bar2(102.4, 102.6, 101.0, 101.0),            # 101.0 < 102.4 → 平仓
    ])
    check('B4 H=2.99→情况6：bar1无动作，bar2 close低于bar1 → 第二分钟平多',
          len(s) == 1 and is_special(s[0]) and '情况6' in s[0].reason, len(s))

    # 情况7(bar1 pending) + 次日 bar2 命中（验证 pending 的 bar2 分支）
    strat, inner = mk()
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_LONG, [
        bar1(100.0, 103.0, 99.9, 102.2),            # H=+3.0%, C=+2.2% → 情况7
        bar2(102.2, 102.4, 101.5, 101.6),           # 跌0.59%<2% 且 >中间值101.5 → 平仓
    ])
    check('B10 H=3.0边界 →情况7 且 bar2 命中 → 第二分钟平仓',
          len(s) == 1 and is_special(s[0]) and '情况7' in s[0].reason, len(s))

    # ==================== 双腿独立 ====================
    strat, inner = mk()
    strat.on_entry('X', 'account1', 100.0, 10, ENTRY)
    strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
    s = feed(strat, POS_BOTH, [
        bar1(100.0, 98.8, 98.5, 99.3),              # 做空:情况2(平仓)；做多:情况1(不操作)
    ])
    check('D1 双腿独立：同bar 平空1单 + 做多腿不操作（共1单，账户1）',
          len(s) == 1 and s[0].account == 'account1' and s[0].side == 'buy'
          and s[0].volume == 10 and is_special(s[0]),
          f'sigs={[(x.account, x.side) for x in s]}')

    # ==================== Runner 层 ====================
    async def runner_cases():
        results = {}

        # R1 开门放行：bar1 dt=开盘(早于建仓 09:30:20) 被门控例外放行 → 情况2 端到端平仓
        r, strat_r, inner_r, fcm, calls = build_runner(
            snap2={'X': {'position': 10, 'avgCost': 100.0}})
        r.on_entry('X', 'account2', 100.0, 10, ENTRY)
        await r.on_bar(bar1(100.0, 101.2, 99.9, 100.7))
        c = fcm.calls[0] if fcm.calls else {}
        results['R1'] = (calls['n'] >= 1 and len(fcm.calls) == 1
                         and c.get('account') == 'D2' and c.get('action') == 'sell'
                         and c.get('volume') == 10 and c.get('open_action') == 'buy'
                         and c.get('csv') == 'buy.csv' and c.get('open_price') == 100.0)

        # R2 窗口内 bar 但该腿无持仓 → 不下单（路径已到达策略）
        r2, s2, i2, fcm2, calls2 = build_runner(snap2={})
        r2.on_entry('X', 'account2', 100.0, 10, ENTRY)
        await r2.on_bar(bar1(100.0, 101.2, 99.9, 100.7))
        results['R2'] = (calls2['n'] >= 1 and fcm2.calls == [])

        # R3 窗口内 bar + 无 on_entry 记录但有持仓 → 特殊策略仍生效（以持仓快照为准）
        r3, s3, i3, fcm3, calls3 = build_runner(
            snap2={'Y': {'position': 10, 'avgCost': 100.0}})
        await r3.on_bar(bar_ohlc('Y', O1, 100.0, 101.2, 99.9, 100.7))
        c3 = fcm3.calls[0] if fcm3.calls else {}
        results['R3'] = (len(fcm3.calls) == 1 and c3.get('account') == 'D2')

        # R4 窗口外且早于建仓的 bar → 仍按旧门控跳过（不放行）
        r4, s4, i4, fcm4, calls4 = build_runner(
            snap2={'X': {'position': 10, 'avgCost': 100.0}})
        r4.on_entry('X', 'account2', 100.0, 10, ENTRY)
        await r4.on_bar(bar_ohlc('X', EST.localize(datetime(2026, 9, 10, 8, 30, 0)),
                                 100.0, 101.2, 99.9, 100.7))
        results['R4'] = (calls4['n'] == 0 and fcm4.calls == [])

        # R5 特殊平仓执行失败 → 定案回滚 → 后续 bar 原策略接管并再次触发
        r5, s5, i5, fcm5, calls5 = build_runner(
            snap2={'X': {'position': 10, 'avgCost': 100.0}})
        fcm5.result = False                       # 所有平仓执行均失败
        r5.on_entry('X', 'account2', 100.0, 10, ENTRY)
        await r5.on_bar(bar1(100.0, 101.2, 99.9, 100.7))       # 特殊平仓 → 失败
        rolled = s5.states['X'].long.outcome == UNSET
        await r5.on_bar(bar2(100.7, 101.6, 100.5, 101.5))      # 窗口内，无动作
        await r5.on_bar(bar3(101.5, 101.7, 100.4, 100.5))      # 窗口外 → 原策略触发
        results['R5'] = (rolled and len(fcm5.calls) == 2)

        return results

    results = asyncio.run(runner_cases())
    check('R1 门控放行：开盘bar(早于建仓时刻)→情况2→账户2卖出10股', results['R1'])
    check('R2 窗口内无持仓不下单', results['R2'])
    check('R3 窗口内无on_entry但有持仓→特殊策略生效', results['R3'])
    check('R4 窗口外早于建仓的bar仍被门控跳过', results['R4'])
    check('R5 平仓失败回滚→原策略接管(共2次尝试)', results['R5'])

    if failures:
        print(f'❌ 失败用例({len(failures)}/{total[0]}):', ', '.join(failures))
        sys.exit(1)
    print(f'✅ 开盘前两分钟特殊策略全部测试通过：共 {total[0]} 项用例'
          '（做多7种情况+边界值+做空镜像+双腿独立+去重/失败回滚+Runner门控放行）')


if __name__ == '__main__':
    main()
