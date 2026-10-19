# -*- coding: utf-8 -*-
"""
DynamicTPStrategy 特殊规则（新peak + 反向bar → 即时平仓，不等下一根bar）单元测试

规则（做多）：
  1. 前一分钟bar的 high 就是 peak（建仓后最高价）；
  2. 新一分钟bar的 high 创出新peak（新peak < 4%），且新bar收阴（close<open）；
  → 用"新peak"按既有公式算平仓线，直接用本bar的close判定，跌破即立即平仓。
做空为镜像（low 是 peak / 创新低且跌幅<4% / 收阳 close>open）。

覆盖矩阵：
  L1  做多触发（同时普通规则不触发 → 证明"不等下一根bar"）
  L2  做多不触发（close 未跌破平仓线）
  L3  做多不触发（新peak >= 4%）
  L4  做多不触发（前一分钟bar的 high 不是 peak）
  L5  做多不触发（新bar 未收阴）
  L6  做多不触发（bar 间不连续，缺 1 分钟）
  L7  做多不触发（新peak < 1%，查表不触发）
  L8  做多不触发（首根bar，无前一根bar）
  L9  做多去重（平仓后同形态下一根bar 不再触发）
  L10 做多回滚（执行失败后下一根bar可再触发）
  L11 做多边界（新peak = 4.0%，严格 <4 不成立 → 不触发）
  L12 做多边界（新peak = 1.0%，查表保留 1% → 触发）
  K1  做空触发（镜像，普通规则不触发）
  K2  做空不触发（反弹未到平仓线）
  K3  做空不触发（新peak 跌幅 >= 4%）
  K4  做空不触发（前一分钟bar的 low 不是极值）
  K5  做空不触发（新bar 未收阳）
  K6  做空不触发（bar 间不连续）
  K7  做空不触发（新peak 跌幅 < 1%）
  M1  双腿独立（同形态做多腿触发、做空腿不操作）
  M2  同bar普通规则与特殊规则同腿同时命中 → 只发 1 个信号（不重复下单）
  M3  原普通规则回归（close 基触发行为不变）
  M4  展示字段回归（仍基于 close 基极值）
  M5  Runner 端到端（特殊规则A信号正确分发为 1 笔平仓单）

规则B 逐bar C/H 形态用例（做多相对本bar开盘价 C=(Close-Open)/Open, H=(High-Open)/Open）：
  L21 情况1触发 (0.4<C<=1 且 1<=H<2)            L22 情况2触发 (1<C<=2 且 2<=H<3)
  L23 情况3触发 (1.5<C<=2 且 3<=H<4)             L24 情况4 (C>2 规则停止)
  L25 H<1 不出档                               L26 H>=4 超出上限
  L27 C=0.4 边界(不含)不触发                     L28 C=1.0 边界(含)触发情况1
  L29 H=2.0 边界落入情况2档                       L30 H=3.0 边界落入情况3档
  L31 H∈[1,2) 但 C>1 不触发                      L32 H∈[2,3) 但 C<=1 不触发
  L33 H∈[3,4) 但 C<=1.5 不触发                   L34 去重（同形态2根bar只1单）
  L35 回滚（执行失败后下一bar可再触发）
  K12~K20 做空镜像（Low 代替 High，C'=(Open-Close)/Open） + K21 去重
  M6  双腿：做多形态bar → 仅做多腿1单             M7  双腿：做空形态bar → 仅做空腿1单
  M8  同bar：规则A做多触发 + 规则B做空触发 → 双腿各1单（2单）
  M9  规则B与普通规则同腿双命中 → 只发1单
  M10 规则B与成本/盈亏无关（浮亏bar仍按形态触发）
  注：K5 因新增规则B而结果变化 —— 该bar规则A不触发（未收阳），但命中规则B情况1 → 触发1单

共存语义：同一腿同一bar A优先 → B → 普通规则，最多1个信号；去重/回滚共用。

运行：python test/test_dynamic_tp_special_rule.py   （任意 cwd 均可）
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
from strategy_runner import StrategyRunner

EST = pytz.timezone('US/Eastern')
T0 = datetime(2026, 9, 10, 10, 0, 0)
ENTRY = EST.localize(T0)                      # 建仓时刻（T0）

POS_LONG = PositionView('X', 0, 0.0, 10, 100.0)     # 账户2 +10 @100
POS_SHORT = PositionView('X', -10, 100.0, 0, 0.0)   # 账户1 -10 @100
POS_BOTH = PositionView('X', -10, 100.0, 10, 100.0) # 双腿同价


def bar(minute: int, o: float, h: float, l: float, c: float, sym='X') -> Bar:
    """T0 + minute 分钟的 bar（美东 aware）"""
    dt = EST.localize(T0 + timedelta(minutes=minute))
    return Bar(sym, dt, o, h, l, c, 1000)


def feed(strat, pos, bars):
    """按顺序喂 bar，收集全部信号"""
    sigs = []
    for b in bars:
        sigs.extend(strat.on_bar(b, pos))
    return sigs


def mk(entry_long=True, entry_short=True):
    s = DynamicTPStrategy()
    if entry_long:
        s.on_entry('X', 'account2', 100.0, 10, ENTRY)
    if entry_short:
        s.on_entry('X', 'account1', 100.0, 10, ENTRY)
    return s


def is_special(sig):
    return '特殊规则' in sig.reason

def is_rule_b(sig):
    """规则B（逐bar C/H 形态）信号：reason 含「逐bar形态规则」（规则A为「特殊规则」，不互相包含）"""
    return '逐bar形态规则' in sig.reason


class FakeCloseManager:
    def __init__(self):
        self.calls = []
        self.result = True

    async def run_close_signal(self, ib, account, symbol, close_action, volume,
                               open_price, open_action, csv_path, **kwargs):
        self.calls.append({
            'account': account, 'symbol': symbol, 'action': close_action,
            'volume': volume, 'open_price': open_price,
            'open_action': open_action,
        })
        return self.result


def build_runner(strategy, snap2=None):
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

    async def fake_get_positions(ib, account=''):
        if account == 'D2':
            return dict(snap2 or {})
        return {}

    SR.get_positions = fake_get_positions
    return runner, fcm


def main():
    # Windows 控制台默认代码页无法输出中文，统一 UTF-8
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

    # ==================== L 系列：做多腿（账户2） ====================

    # L1 触发：bar1 峰值 103.0(+3%)；bar2 新高 103.5(新peak +3.5%<4%) 收阴 close 101.8
    #   特殊规则：line=100*(1+2.0%)=102.0，101.8<=102.0 → 触发
    #   普通规则：close peak=102.9(+2.9%) → line=101.0，101.8>101.0 → 不触发（关键：仅特殊规则命中）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.9),
        bar(2, 103.4, 103.5, 101.5, 101.8),
    ])
    check('L1 做多特殊规则触发（普通规则不触发，即时平仓不等下一根bar）',
          len(sigs) == 1 and is_special(sigs[0])
          and sigs[0].side == 'sell' and sigs[0].account == 'account2'
          and sigs[0].open_action == 'buy' and sigs[0].volume == 10,
          f'sigs={[(x.side, x.volume) for x in sigs]} reason={sigs[0].reason[:80] if sigs else "-"}')

    # L2 不触发（close 102.3 尚在平仓线 102.0 上方）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.9),
        bar(2, 103.4, 103.5, 101.6, 102.3),
    ])
    check('L2 做多不触发: close 未跌破平仓线(102.0)', len(sigs) == 0, detail=str(sigs))

    # L3 不触发（新peak 104.5 = +4.5% >= 4%）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.5, 103.0, 102.0, 102.6),
        bar(2, 104.2, 104.5, 103.0, 103.2),
    ])
    check('L3 做多不触发: 新peak >= 4%', len(sigs) == 0, detail=str(sigs))

    # L4 不触发（peak 105.0 在更早bar，前一根bar high=104.0 不是 peak）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 104.8, 105.0, 103.5, 104.6),
        bar(2, 104.5, 104.0, 103.8, 103.9),
    ])
    check('L4 做多不触发: 前一分钟bar的high不是peak', len(sigs) == 0, detail=str(sigs))

    # L5 不触发（新bar 收阳 close 103.3 > open 102.8）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.9, 102.4),
        bar(2, 102.8, 103.5, 102.5, 103.3),
    ])
    check('L5 做多不触发: 新bar未收阴', len(sigs) == 0, detail=str(sigs))

    # L6 不触发（bar 间缺 1 分钟：10:01 → 10:03，"前一分钟bar"不成立）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.5),
        bar(3, 103.2, 103.5, 101.9, 102.0),
    ])
    check('L6 做多不触发: bar不连续(缺1分钟)', len(sigs) == 0, detail=str(sigs))

    # L7 不触发（新peak 100.9 = +0.9% < 1%，查表 -1 不触发）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 100.3, 100.6, 100.1, 100.4),
        bar(2, 100.5, 100.9, 100.2, 100.3),
    ])
    check('L7 做多不触发: 新peak < 1%', len(sigs) == 0, detail=str(sigs))

    # L8 不触发（首根 bar 无前一根 bar；且普通规则也不触发）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 103.0, 103.5, 101.0, 101.3)])
    check('L8 做多不触发: 首根bar无前bar', len(sigs) == 0, detail=str(sigs))

    # L9 去重：触发后再来同形态 bar（新高+收阴）→ 不再触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.9),
        bar(2, 103.4, 103.5, 101.5, 101.8),
        bar(3, 103.4, 103.8, 101.4, 101.7),
    ])
    check('L9 做多去重: 平仓后同形态不再触发', len(sigs) == 1 and is_special(sigs[0]),
          detail=f'count={len(sigs)}')

    # L10 回滚：执行失败 → 下一根同形态 bar 可以再次触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.9),
        bar(2, 103.4, 103.5, 101.5, 101.8),
    ])
    assert len(sigs) == 1
    s.on_execution_result(sigs[0], success=False)        # 执行失败 → 回滚去重标记
    sigs2 = feed(s, POS_LONG, [bar(3, 103.4, 103.8, 101.4, 101.7)])
    check('L10 做多回滚: 执行失败后下一根bar可再触发',
          len(sigs2) == 1 and is_special(sigs2[0]),
          detail=f'count={len(sigs2)}')

    # L11 边界：新peak 恰好 4.0%（严格 <4 不成立）→ 不触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.5, 103.0, 102.0, 102.6),
        bar(2, 104.1, 104.0, 103.0, 103.4),
    ])
    check('L11 边界: 新peak=4.0% 不触发', len(sigs) == 0, detail=str(sigs))

    # L12 边界：新peak 恰好 1.0% → g(1.0)=1.0，line=101.0，close 100.9 → 触发；
    #       普通规则 close peak 100.9(+0.9%) 不触发 → 仅特殊规则命中
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 100.6, 100.8, 100.1, 100.7),
        bar(2, 101.2, 101.0, 100.5, 100.9),
    ])
    check('L12 边界: 新peak=1.0% 触发（仅特殊规则命中）',
          len(sigs) == 1 and is_special(sigs[0]) and sigs[0].side == 'sell',
          detail=str(sigs))

    # ==================== K 系列：做空腿（账户1，做多镜像） ====================

    # K1 触发：bar1 低点 97.0(−3%)；bar2 新低 96.5(新peak −3.5%<4%) 收阳 close 98.6
    #   特殊规则：line=100*(1−3.5%+2.0%)=98.5，98.6>=98.5 → 触发
    #   普通规则：close 极值 97.8(跌幅2.2%) → line=98.8，98.6<98.8 → 不触发（仅特殊规则命中）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 96.9, 98.2, 97.0, 97.8),
        bar(2, 96.6, 98.9, 96.5, 98.6),
    ])
    check('K1 做空特殊规则触发（镜像，普通规则不触发）',
          len(sigs) == 1 and is_special(sigs[0])
          and sigs[0].side == 'buy' and sigs[0].account == 'account1'
          and sigs[0].open_action == 'sell' and sigs[0].volume == 10,
          f'reason={sigs[0].reason[:80] if sigs else "-"}')

    # K2 不触发（反弹 close 98.2 未到平仓线 98.5）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 96.9, 98.2, 97.0, 97.8),
        bar(2, 96.6, 98.8, 96.5, 98.2),
    ])
    check('K2 做空不触发: 反弹未到平仓线(98.5)', len(sigs) == 0, detail=str(sigs))

    # K3 不触发（新低 95.5 = 跌幅 4.5% >= 4%）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 96.9, 98.0, 97.0, 97.5),
        bar(2, 95.8, 96.8, 95.5, 96.6),
    ])
    check('K3 做空不触发: 新peak跌幅 >= 4%', len(sigs) == 0, detail=str(sigs))

    # K4 不触发（极值 95.0 在更早bar，前一根bar low=96.0 不是峰值）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 95.8, 97.5, 95.0, 95.6),
        bar(2, 95.9, 97.2, 96.0, 96.8),
    ])
    check('K4 做空不触发: 前一分钟bar的low不是极值', len(sigs) == 0, detail=str(sigs))

    # K5 规则A不触发（新bar 收阴 close 97.0 < open 97.8，镜像要求收阳）；
    #    但该单bar形态（C'=0.82%∈(0.4,1], L=1.33%∈[1,2)）命中【规则B】情况1 → 规则B触发1单
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 96.9, 98.0, 97.0, 97.4),
        bar(2, 97.8, 97.9, 96.5, 97.0),
    ])
    check('K5 规则A不触发(未收阳); 规则B情况1(探底回升)命中 → 1单',
          len(sigs) == 1 and is_rule_b(sigs[0]) and sigs[0].side == 'buy',
          detail=str([(x.side, x.reason[:40]) for x in sigs]))

    # K6 不触发（bar 间缺 1 分钟）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 96.9, 98.0, 97.0, 97.5),
        bar(3, 96.7, 98.5, 96.5, 98.3),
    ])
    check('K6 做空不触发: bar不连续(缺1分钟)', len(sigs) == 0, detail=str(sigs))

    # K7 不触发（新低 99.2 = 跌幅 0.8% < 1%，查表 -1 不触发）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 99.3, 100.2, 99.5, 99.6),
        bar(2, 99.4, 100.0, 99.2, 99.8),
    ])
    check('K7 做空不触发: 新peak跌幅 < 1%', len(sigs) == 0, detail=str(sigs))

    # ==================== M 系列：混合 / 回归 / 端到端 ====================

    # M1 双腿独立：同一根 bar 收阴 → 做多腿特殊规则触发；做空腿（要求收阳）不操作
    s = mk()
    sigs = feed(s, POS_BOTH, [
        bar(1, 102.0, 103.0, 97.0, 102.9),
        bar(2, 103.4, 103.5, 96.5, 101.8),
    ])
    check('M1 双腿独立: 做多腿触发、做空腿不操作',
          len(sigs) == 1 and sigs[0].account == 'account2' and sigs[0].side == 'sell',
          detail=str([(x.account, x.side) for x in sigs]))

    # M2 同bar普通规则与特殊规则同腿均会命中 → 只发 1 个信号（不重复下单）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 102.0, 103.0, 101.8, 102.9),
        bar(2, 103.4, 103.5, 100.2, 100.5),   # 特殊规则 line 102.0 / 普通规则 line 101.0 均被 close 100.5 跌破
    ])
    check('M2 同腿双规则命中只发1个信号', len(sigs) == 1 and is_special(sigs[0]),
          detail=f'count={len(sigs)}')

    # M3 原普通规则回归（flat OHLC 使特殊规则不可能命中；close 基触发行为不变）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 100.5, 100.5, 100.5, 100.5),
        bar(2, 102.9, 102.9, 102.9, 102.9),   # close peak +2.9% → line 101.0
        bar(3, 100.9, 100.9, 100.9, 100.9),   # 100.9 <= 101.0 → 普通规则触发
    ])
    check('M3 普通规则回归: close基触发不变',
          len(sigs) == 1 and not is_special(sigs[0]) and sigs[0].side == 'sell',
          detail=str(sigs))

    # M4 展示字段回归（仍基于 close 基极值；两个百分比分别依赖做多/做空入场价）
    s4 = mk()                                            # 双腿入场 @100
    feed(s4, POS_BOTH, [
        bar(1, 100.5, 100.5, 100.5, 100.5),
        bar(2, 102.9, 102.9, 102.9, 102.9),
        bar(3, 100.9, 100.9, 100.9, 100.9),
    ])
    d = s4.get_display_state('X')
    check('M4 展示字段回归(close基极值)',
          d is not None
          and abs(d.get('day_max_since_entry', 0) - 102.9) < 1e-9
          and abs(d.get('day_min_since_entry', 0) - 100.5) < 1e-9
          and abs(d.get('day_max_since_entry_pct', 0) - 2.9) < 1e-9   # 相对做多入场价
          and abs(d.get('day_min_since_entry_pct', 0) - 0.5) < 1e-9,   # 相对做空入场价
          detail=str(d))

    # M5 Runner 端到端：特殊规则信号 → 1 笔账户2 卖出平仓单
    async def m5():
        strat = DynamicTPStrategy()
        strat.on_entry('X', 'account2', 100.0, 10, ENTRY)
        runner, fcm = build_runner(strat, snap2={'X': {'position': 10, 'avgCost': 100.0}})
        runner.entry_times['X'] = ENTRY          # 通过建仓门控
        await runner.on_bar(bar(1, 102.0, 103.0, 101.8, 102.9))
        await runner.on_bar(bar(2, 103.4, 103.5, 101.5, 101.8))
        return fcm

    fcm = asyncio.run(m5())
    # 注：Runner 分发时把逻辑账户 account2 映射为真实 IB 账户 ID（此处为 'D2'）
    check('M5 Runner端到端: 1笔平仓单(账户2卖出)',
          len(fcm.calls) == 1 and fcm.calls[0]['account'] == 'D2'
          and fcm.calls[0]['action'] == 'sell' and fcm.calls[0]['volume'] == 10
          and fcm.calls[0]['open_action'] == 'buy' and fcm.calls[0]['open_price'] == 100.0,
          detail=str(fcm.calls))

    # ==================== L21+ 系列：【规则B】做多逐bar形态（账户2） ====================
    # 规则B只依赖本bar自身 o/h/l/c（与成本、前一根bar均无关）

    # L21 情况1：C=0.8%∈(0.4,1]，H=1.5%∈[1,2) → 触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 101.5, 99.9, 100.8)])
    check('L21 规则B情况1触发 (0.4<C<=1, 1<=H<2)',
          len(sigs) == 1 and is_rule_b(sigs[0]) and sigs[0].side == 'sell'
          and sigs[0].account == 'account2' and sigs[0].open_action == 'buy',
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L22 情况2：C=1.8%∈(1,2]，H=2.5%∈[2,3) → 触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 102.5, 101.0, 101.8)])
    check('L22 规则B情况2触发 (1<C<=2, 2<=H<3)', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L23 情况3：C=1.9%∈(1.5,2]，H=3.5%∈[3,4) → 触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 103.5, 101.0, 101.9)])
    check('L23 规则B情况3触发 (1.5<C<=2, 3<=H<4)', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L24 情况4：C=2.5% > 2% → 规则停止（H=3.2% 虽在档内也不触发）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 103.2, 99.9, 102.5)])
    check('L24 规则B情况4: C>2% 停止生效', len(sigs) == 0, detail=str(sigs))

    # L25 H=0.8% < 1% → 无档位
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 100.8, 99.9, 100.6)])
    check('L25 H<1% 无档位不触发', len(sigs) == 0, detail=str(sigs))

    # L26 H=4.5% >= 4% → 超上限（C=1.8% 在档外）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 104.5, 99.9, 101.8)])
    check('L26 H>=4% 不触发', len(sigs) == 0, detail=str(sigs))

    # L27 边界：C=0.4%（严格 >0.4 不成立）→ 不触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 101.5, 99.9, 100.4)])
    check('L27 边界: C=0.4% 不触发', len(sigs) == 0, detail=str(sigs))

    # L28 边界：C=1.0%（<=1 含）且 H=1.5% → 情况1触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 101.5, 99.9, 101.0)])
    check('L28 边界: C=1.0%,H=1.5% 触发情况1', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L29 边界：H=2.0%（>=2 落情况2档），C=1.6%∈(1,2] → 触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 102.0, 100.5, 101.6)])
    check('L29 边界: H=2.0% 落情况2档触发', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L30 边界：H=3.0%（>=3 落情况3档），C=1.9%∈(1.5,2] → 触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 103.0, 100.5, 101.9)])
    check('L30 边界: H=3.0% 落情况3档触发', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # L31 H∈[1,2) 但 C=1.2% > 1% → 档内C超界不触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 101.5, 99.9, 101.2)])
    check('L31 H∈[1,2) 但 C>1 不触发', len(sigs) == 0, detail=str(sigs))

    # L32 H∈[2,3) 但 C=0.9% <= 1% → 档内C未达下界不触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 102.5, 99.9, 100.9)])
    check('L32 H∈[2,3) 但 C<=1 不触发', len(sigs) == 0, detail=str(sigs))

    # L33 H∈[3,4) 但 C=1.5%（严格 >1.5 不成立）→ 不触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 103.5, 99.9, 101.5)])
    check('L33 H∈[3,4) 但 C=1.5% 不触发', len(sigs) == 0, detail=str(sigs))

    # L34 去重：同形态两根bar → 只发1单
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 100.0, 101.5, 99.9, 100.8),
        bar(2, 100.0, 101.5, 99.9, 100.8),
    ])
    check('L34 规则B去重: 同形态2根bar只发1单', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=f'count={len(sigs)}')

    # L35 回滚：执行失败 → 下一根同形态bar可再触发
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [bar(1, 100.0, 101.5, 99.9, 100.8)])
    assert len(sigs) == 1
    s.on_execution_result(sigs[0], success=False)      # 执行失败 → 回滚去重标记
    sigs2 = feed(s, POS_LONG, [bar(2, 100.0, 101.5, 99.9, 100.8)])
    check('L35 规则B回滚: 执行失败后下一根bar可再触发',
          len(sigs2) == 1 and is_rule_b(sigs2[0]), detail=f'count={len(sigs2)}')

    # ==================== K12+ 系列：【规则B】做空逐bar形态（账户1，镜像） ====================

    # K12 情况1：C'=0.8%∈(0.4,1]，L=1.5%∈[1,2) → 触发（买入回补）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.3, 98.5, 99.2)])
    check('K12 规则B情况1触发(镜像) (0.4<C\'<=1, 1<=L<2)',
          len(sigs) == 1 and is_rule_b(sigs[0]) and sigs[0].side == 'buy'
          and sigs[0].account == 'account1' and sigs[0].open_action == 'sell',
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # K13 情况2：C'=1.8%∈(1,2]，L=2.5%∈[2,3) → 触发
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.2, 97.5, 98.2)])
    check('K13 规则B情况2触发(镜像) (1<C\'<=2, 2<=L<3)',
          len(sigs) == 1 and is_rule_b(sigs[0]), detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # K14 情况3：C'=1.9%∈(1.5,2]，L=3.5%∈[3,4) → 触发
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.2, 96.5, 98.1)])
    check('K14 规则B情况3触发(镜像) (1.5<C\'<=2, 3<=L<4)',
          len(sigs) == 1 and is_rule_b(sigs[0]), detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # K15 情况4：C'=2.5% > 2% → 规则停止
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 96.8, 97.5)])
    check('K15 规则B情况4(镜像): C\'>2% 停止生效', len(sigs) == 0, detail=str(sigs))

    # K16 L=0.8% < 1% → 无档位（C'=0.6%）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 99.2, 99.4)])
    check('K16 L<1% 无档位不触发', len(sigs) == 0, detail=str(sigs))

    # K17 L=4.5% >= 4% → 超上限（C'=1.8%）
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 95.5, 98.2)])
    check('K17 L>=4% 不触发', len(sigs) == 0, detail=str(sigs))

    # K18 边界：C'=1.0%（<=1 含），L=1.0%（>=1 含）→ 情况1触发
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 99.0, 99.0)])
    check('K18 边界: C\'=1.0%,L=1.0% 触发情况1', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # K19 边界：L=2.0%（>=2 落情况2档），C'=1.02%∈(1,2] → 触发
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 98.0, 98.98)])
    check('K19 边界: L=2.0% 落情况2档触发', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=str([(x.side, x.reason[:60]) for x in sigs]))

    # K20 L∈[1,2) 但 C'=1.2% > 1% → 档内C超界不触发
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [bar(1, 100.0, 100.5, 98.6, 98.8)])
    check('K20 L∈[1,2) 但 C\'>1 不触发', len(sigs) == 0, detail=str(sigs))

    # K21 去重：同形态两根bar → 只发1单
    s = mk(entry_long=False)
    sigs = feed(s, POS_SHORT, [
        bar(1, 100.0, 100.3, 98.5, 99.2),
        bar(2, 100.0, 100.3, 98.5, 99.2),
    ])
    check('K21 规则B去重: 同形态2根bar只发1单', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=f'count={len(sigs)}')

    # ==================== M6+ 系列：规则A/B 与既有规则共存 ====================

    # M6 双腿独立：做多形态bar（C=0.8%,H=1.5%）→ 仅做多腿规则B触发1单
    s = mk()
    sigs = feed(s, POS_BOTH, [bar(1, 100.0, 101.5, 100.2, 100.8)])
    check('M6 双腿: 做多形态bar → 仅做多腿1单',
          len(sigs) == 1 and sigs[0].account == 'account2' and sigs[0].side == 'sell'
          and is_rule_b(sigs[0]),
          detail=str([(x.account, x.side) for x in sigs]))

    # M7 双腿独立：做空形态bar（C'=0.8%,L=1.5%）→ 仅做空腿规则B触发1单
    s = mk()
    sigs = feed(s, POS_BOTH, [bar(1, 100.0, 100.3, 98.5, 99.2)])
    check('M7 双腿: 做空形态bar → 仅做空腿1单',
          len(sigs) == 1 and sigs[0].account == 'account1' and sigs[0].side == 'buy'
          and is_rule_b(sigs[0]),
          detail=str([(x.account, x.side) for x in sigs]))

    def is_rule_a(sig):
        """规则A（新peak+反向bar）信号：reason 含「特殊规则」且不含「逐bar形态规则」"""
        return ('特殊规则' in sig.reason) and not is_rule_b(sig)

    # M8 同bar双腿独立：规则A做多腿触发 + 规则B做空腿触发 → 各1单（共2单）
    #    bar2: 多head新高102.1(规则A) 且 C'=0.495%,L=1.78%（规则B情况1）
    s8 = DynamicTPStrategy()
    s8.on_entry('X', 'account2', 100.0, 10, ENTRY)      # 做多成本 100
    s8.on_entry('X', 'account1', 120.0, 10, ENTRY)      # 做空成本 120
    pos8 = PositionView('X', -10, 120.0, 10, 100.0)
    sigs8 = feed(s8, pos8, [
        bar(1, 100.0, 101.5, 99.9, 101.2),              # 建立 prev（high 101.5 为 peak）
        bar(2, 101.0, 102.1, 99.2, 100.5),              # 规则A做多 + 规则B做空 同时命中
    ])
    check('M8 同bar: 规则A(做多)+规则B(做空)双腿各1单',
          len(sigs8) == 2
          and any(x.account == 'account2' and x.side == 'sell' and is_rule_a(x) for x in sigs8)
          and any(x.account == 'account1' and x.side == 'buy' and is_rule_b(x) for x in sigs8),
          detail=str([(x.account, x.side, x.reason[:36]) for x in sigs8]))

    # M9 规则B与普通规则同腿均会命中 → 只发1单（B优先，不重复下单）
    s = mk(entry_short=False)
    sigs = feed(s, POS_LONG, [
        bar(1, 100.0, 103.0, 100.0, 103.0),       # close peak 103(+3%) → 普通规则线102
        bar(2, 100.0, 103.5, 100.5, 101.9),       # B情况3(C=1.9%,H=3.5%) 且 普通规则 101.9<=102 均命中
    ])
    check('M9 规则B与普通规则双命中只发1单', len(sigs) == 1 and is_rule_b(sigs[0]),
          detail=f'count={len(sigs)}')

    # M10 规则B与成本/盈亏无关：浮亏仓位（成本110、现价≈100）遇做多形态bar仍按形态触发
    s10 = DynamicTPStrategy()
    s10.on_entry('X', 'account2', 110.0, 10, ENTRY)    # 做多成本 110（当前价 ≈ 浮亏）
    pos10 = PositionView('X', 0, 0.0, 10, 110.0)
    sigs10 = feed(s10, pos10, [bar(1, 100.0, 101.5, 99.9, 100.8)])
    check('M10 规则B与成本无关: 浮亏bar按形态仍触发',
          len(sigs10) == 1 and is_rule_b(sigs10[0]) and sigs10[0].side == 'sell',
          detail=str([(x.side, x.reason[:60]) for x in sigs10]))

    # ---- 汇总 ----
    if failures:
        print('❌ 失败用例:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部通过：DynamicTP 特殊规则（新peak+反向bar即时平仓）做多/做空镜像、边界、去重/回滚及原规则回归均正常')


if __name__ == '__main__':
    main()
