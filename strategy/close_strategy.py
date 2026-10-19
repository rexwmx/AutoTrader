# -*- coding: utf-8 -*-
"""分段动态止盈策略

原 close.py 中的业务规则完整迁移至此，但只做决策，不做任何下单/写CSV动作。

【特殊规则A】新peak + 反向bar → 即时平仓判定（不等下一根bar）：
  做多：前一分钟bar的high就是peak；新一分钟bar的high创出新peak（新peak<4%），
        且新bar收阴（close<open）→ 用"新peak"按既有公式算平仓线，直接用本bar的
        close 判定，跌破平仓线立即平仓。
  做空：镜像（low 是 peak / 创新低且跌幅<4% / 新bar收阳 close>open）。
  动机：新peak出现且当根bar已反向时，peak 已经确定，无需再等下一根bar让
  close 基极值重新锚定后才判定。

【特殊规则B】逐bar C/H 形态"冲高回落"→ 立即平仓（每根bar独立判定，与成本无关）：
  全部相对本bar自身开盘价： C = (Close - Open) / Open * 100，H = (High - Open) / Open * 100
  做多：
    情况1: 0.4% < C <= 1% 且 1% <= H < 2%
    情况2: 1% < C <= 2%   且 2% <= H < 3%
    情况3: 1.5% < C <= 2% 且 3% <= H < 4%
    情况4: C > 2% → 本规则停止生效（等价于以上各情况均不满足）
    → 该bar冲高后收不回（"已经是1降"）→ 立即平多仓。
  做空（完整镜像，High→Low，"上涨"→"下跌"）：
    C' = (Open - Close) / Open * 100，L = (Open - Low) / Open * 100
    情况1: 0.4% < C' <= 1% 且 1% <= L < 2%
    情况2: 1% < C' <= 2%   且 2% <= L < 3%
    情况3: 1.5% < C' <= 2% 且 3% <= L < 4%
    情况4: C' > 2% → 本规则停止生效
    → 该bar探底后收不回 → 立即买入回补空头。

  两条特殊规则在同一腿同一bar最多只发一个信号（A优先，A未命中再看B）；
  与既有去重集合（closed_acc1/2）及执行失败回滚机制完全共用。
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from strategy.base import BaseStrategy, Bar, PositionView, CloseSignal
from logger import get_logger

# 特殊规则B边界容差：消除十进制边界（0.4/1/1.5/2/3/4%）的浮点表示误差，
# 使"恰好落在边界上"的bar按规格语义（含边界一侧）确定性归类。
EPS = 1e-9


def get_close_pct(peak_pct: float) -> float:
    """根据峰值涨幅/跌幅百分比，查表得到平仓线保留百分比

    峰值越大（盈利越多），保留比例越低——让利润奔跑。
    peak_pct < 1 时返回 -1（不触发）。
    """
    if peak_pct < 1:
        return -1.0
    if peak_pct < 2:
        return 1.0
    if peak_pct < 3:
        return 1.0
    if peak_pct < 4:
        return 2.0
    if peak_pct < 6:
        return peak_pct * 0.50
    return 3.0


@dataclass
class SymbolState:
    """单标的的策略状态（跨 K 线持久化于策略实例）"""
    entry_time: Optional[datetime] = None
    acc1_entry_price: Optional[float] = None
    acc2_entry_price: Optional[float] = None
    min_close_since_entry: Optional[float] = None
    max_close_since_entry: Optional[float] = None
    # ---- 【特殊规则】high/low 极值与前一根 bar 信息（新peak+反向bar 即时判定用） ----
    prev_bar_dt: Optional[datetime] = None        # 上一根已评估bar的时间戳（1分钟连续性检查）
    prev_bar_high: Optional[float] = None         # 上一根已评估bar的 high
    prev_bar_low: Optional[float] = None          # 上一根已评估bar的 low
    max_high_since_entry: Optional[float] = None  # 建仓后 high 峰值（做多 peak 判定）
    min_low_since_entry: Optional[float] = None   # 建仓后 low 峰值（做空 peak 判定）


class DynamicTPStrategy(BaseStrategy):
    """分段动态止盈策略

    账户1（做空腿）：关注"峰值跌幅"，现价反弹到平仓线以上触发买入回补
    账户2（做多腿）：关注"峰值涨幅"，现价回落到平仓线以下触发卖出清仓
    两腿独立判断、独立平仓，互不干扰。
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.states: dict = {}
        self.closed_acc1: set = set()
        self.closed_acc2: set = set()
        self.logger = get_logger()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def on_entry(self, symbol: str, account: str,
                 price: float, volume: int, dt: datetime) -> None:
        """建仓成交回调：记录最早建仓时间和两条腿的入场价"""
        st = self.states.setdefault(symbol, SymbolState())
        if st.entry_time is None or dt < st.entry_time:
            st.entry_time = dt
        if account == 'account1' and price > 0:
            st.acc1_entry_price = price
        elif account == 'account2' and price > 0:
            st.acc2_entry_price = price

    def on_bar(self, bar: Bar, pos: PositionView) -> list[CloseSignal]:
        """每根新 1 分钟 bar 调用一次"""
        st = self.states.setdefault(bar.symbol, SymbolState())

        # 建仓前的 bar（历史回放）不参与决策
        if st.entry_time is None or bar.dt <= st.entry_time:
            return []

        # ---- 【特殊规则A预检】必须在把本 bar 纳入 high/low 极值之前，
        #      基于"前一分钟bar"的状态判定（新peak + 反向bar → 即时平仓）----
        long_sig = (self._check_long_special_rule(bar, pos, st)
                    if pos.acc2_pos > 0 and bar.symbol not in self.closed_acc2
                    else None)
        short_sig = (self._check_short_special_rule(bar, pos, st)
                     if pos.acc1_pos < 0 and bar.symbol not in self.closed_acc1
                     else None)

        # ---- 【特殊规则B】逐bar C/H 形态（规则A未命中才检查；命中即共用同一去重集合）----
        if long_sig is None and pos.acc2_pos > 0 and bar.symbol not in self.closed_acc2:
            long_sig = self._check_long_pct_rule(bar, pos)
        if short_sig is None and pos.acc1_pos < 0 and bar.symbol not in self.closed_acc1:
            short_sig = self._check_short_pct_rule(bar, pos)

        signals: list[CloseSignal] = []
        if long_sig is not None:
            signals.append(long_sig)
        if short_sig is not None:
            signals.append(short_sig)

        # ---- 更新建仓后极值（close 极值与原策略一致；high/low 极值供下一根bar特殊规则使用）----
        if st.min_close_since_entry is None:
            st.min_close_since_entry = bar.close
            st.max_close_since_entry = bar.close
        else:
            st.min_close_since_entry = min(st.min_close_since_entry, bar.close)
            st.max_close_since_entry = max(st.max_close_since_entry, bar.close)

        st.prev_bar_dt = bar.dt
        st.prev_bar_high = bar.high
        st.prev_bar_low = bar.low
        st.max_high_since_entry = (bar.high if st.max_high_since_entry is None
                                   else max(st.max_high_since_entry, bar.high))
        st.min_low_since_entry = (bar.low if st.min_low_since_entry is None
                                  else min(st.min_low_since_entry, bar.low))

        # ---- 原分段动态止盈判定（特殊规则已触发平仓的腿不再重复判定/发信号）----
        if (short_sig is None
                and pos.acc1_pos < 0
                and bar.symbol not in self.closed_acc1
                and st.min_close_since_entry is not None):
            sig = self._check_short(bar, pos, st)
            if sig:
                signals.append(sig)

        if (long_sig is None
                and pos.acc2_pos > 0
                and bar.symbol not in self.closed_acc2
                and st.max_close_since_entry is not None):
            sig = self._check_long(bar, pos, st)
            if sig:
                signals.append(sig)

        return signals

    # ------------------------------------------------------------------
    # 【特殊规则】新peak + 反向bar → 用本bar的close即时判定平仓（不等下一根bar）
    # ------------------------------------------------------------------
    def _check_long_special_rule(self, bar: Bar, pos: PositionView,
                                 st: SymbolState) -> Optional[CloseSignal]:
        """做多腿特殊规则（做空镜像见 _check_short_special_rule）。

        触发形态（须同时满足，基于"前一分钟bar"的状态，本bar尚未纳入极值）：
          1. 前一分钟bar（恰好早1分钟、连续）的 high 就是建仓后 peak
             （max_high == prev.high）；
          2. 本bar的 high 创出新peak（bar.high > max_high），且新peak 距成本 < 4%；
          3. 本bar收阴（close < open）。
        满足时用"新peak"按原规则同一公式计算平仓线：
          close_line = entry * (1 + 保留比例/100)
        本bar的 close 已跌至平仓线及以下（且仍在成本上方，属止盈区）→ 立即发平仓信号，
        不等下一根 bar 让 close 基极值重新锚定后再判断。
        """
        entry = pos.acc2_avg_cost
        if entry <= 0:
            return None

        # 1) "前一分钟bar"必须恰好早 1 分钟（bar 间连续）
        if (st.prev_bar_dt is None
                or (bar.dt - st.prev_bar_dt).total_seconds() != 60):
            return None
        # 2) 前一分钟bar的 high 就是 peak（更新前的状态：max_high 尚不含本bar）
        if st.prev_bar_high is None or st.max_high_since_entry is None:
            return None
        if st.max_high_since_entry != st.prev_bar_high:
            return None
        # 3) 本bar的 high 创出新peak
        if bar.high <= st.max_high_since_entry:
            return None
        # 4) 新peak < 4%（且高于成本，仍在止盈区）
        peak_pct = (bar.high - entry) / entry * 100
        if not (0 < peak_pct < 4):
            return None
        # 5) 本bar收阴（close < open）
        if bar.close >= bar.open:
            return None
        # 6) 仍在成本上方（止盈规则前提；跌回成本以下的离场由其它逻辑/强平兜底）
        if bar.close <= entry:
            return None

        close_pct = get_close_pct(peak_pct)
        if close_pct < 0:
            return None
        close_line = entry * (1 + close_pct / 100)
        if bar.close > close_line:
            return None

        self.closed_acc2.add(bar.symbol)
        entry_dt = st.entry_time.strftime('%Y-%m-%d %H:%M:%S') if st.entry_time else '未知'
        reason = (
            f"账户2(做多)特殊规则 | 建仓: {entry_dt} | "
            f"成本{entry:.2f}, 前bar high峰值{st.prev_bar_high:.2f}(即peak), "
            f"本bar创新高{bar.high:.2f}(+{peak_pct:.1f}%<4%)且收阴, "
            f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
            f"本bar close {bar.close:.2f} → 即时平仓(不等下一根bar)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account2',
            side='sell', volume=int(pos.acc2_pos),
            open_action='buy', reason=reason,
            strategy='dynamic_tp',
        )

    def _check_short_special_rule(self, bar: Bar, pos: PositionView,
                                  st: SymbolState) -> Optional[CloseSignal]:
        """做空腿特殊规则（做多逻辑的完整镜像）。

        触发形态（做多"高"→做空"低"、"收阴"→"收阳"）：
          1. 前一分钟bar（恰好早1分钟、连续）的 low 就是建仓后 peak（min_low == prev.low）；
          2. 本bar的 low 创出新peak（bar.low < min_low），且新peak 距成本跌幅 < 4%；
          3. 本bar收阳（close > open）。
        满足时用"新peak"按原规则同一公式计算平仓线：
          close_line = entry * (1 - 峰值/100 + 保留比例/100)
        本bar的 close 已反弹至平仓线及以上（且仍在成本下方，属止盈区）→ 立即发平仓信号。
        """
        entry = pos.acc1_avg_cost
        if entry <= 0:
            return None

        # 1) "前一分钟bar"必须恰好早 1 分钟（bar 间连续）
        if (st.prev_bar_dt is None
                or (bar.dt - st.prev_bar_dt).total_seconds() != 60):
            return None
        # 2) 前一分钟bar的 low 就是 peak（更新前的状态：min_low 尚不含本bar）
        if st.prev_bar_low is None or st.min_low_since_entry is None:
            return None
        if st.min_low_since_entry != st.prev_bar_low:
            return None
        # 3) 本bar的 low 创出新peak（创新低）
        if bar.low >= st.min_low_since_entry:
            return None
        # 4) 新peak 跌幅 < 4%（且低于成本，仍在止盈区）
        peak_pct = (entry - bar.low) / entry * 100
        if not (0 < peak_pct < 4):
            return None
        # 5) 本bar收阳（close > open），多空的镜像
        if bar.close <= bar.open:
            return None
        # 6) 仍在成本下方（止盈规则前提）
        if bar.close >= entry:
            return None

        close_pct = get_close_pct(peak_pct)
        if close_pct < 0:
            return None
        close_line = entry * (1 - peak_pct / 100 + close_pct / 100)
        if bar.close < close_line:
            return None

        self.closed_acc1.add(bar.symbol)
        entry_dt = st.entry_time.strftime('%Y-%m-%d %H:%M:%S') if st.entry_time else '未知'
        reason = (
            f"账户1(做空)特殊规则 | 建仓: {entry_dt} | "
            f"成本{entry:.2f}, 前bar low峰值{st.prev_bar_low:.2f}(即peak), "
            f"本bar创新低{bar.low:.2f}(-{peak_pct:.1f}%<4%)且收阳, "
            f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
            f"本bar close {bar.close:.2f} → 即时平仓(不等下一根bar)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account1',
            side='buy', volume=abs(int(pos.acc1_pos)),
            open_action='sell', reason=reason,
            strategy='dynamic_tp',
        )

    # ------------------------------------------------------------------
    # 【特殊规则B】逐bar C/H 形态"冲高回落/探底回升"→ 立即平仓
    #   规则B只依赖本bar自身的 o/h/l/c（与建仓成本、前一根bar无关）；
    #   触发前提是该腿有持仓（pos 检查在调用方完成）。
    # ------------------------------------------------------------------
    def _long_pct_case(self, bar: Bar) -> Optional[int]:
        """做多bar形态分类。命中返回情况编号 1/2/3，未命中返回 None。

        相对本bar自身开盘价： C = (Close-Open)/Open*100，H = (High-Open)/Open*100
          情况1: 0.4 < C <= 1   且 1 <= H < 2
          情况2: 1.0 < C <= 2   且 2 <= H < 3
          情况3: 1.5 < C <= 2   且 3 <= H < 4
          情况4: C > 2 → 规则停止（等价于以上均不命中）
        """
        if bar.open <= 0:
            return None
        c = (bar.close - bar.open) / bar.open * 100
        h = (bar.high - bar.open) / bar.open * 100
        if c <= 0.4 + EPS or c > 2.0 + EPS:     # C ≤ 0.4% 无情况适用；C > 2% 情况4停止
            return None
        if 1.0 - EPS <= h < 2.0 - EPS:          # H ∈ [1%, 2%)：要求 C ∈ (0.4%, 1%]
            if c <= 1.0 + EPS:
                return 1
        elif 2.0 - EPS <= h < 3.0 - EPS:        # H ∈ [2%, 3%)：要求 C ∈ (1%, 2%]
            if c > 1.0 + EPS:
                return 2
        elif h < 4.0 - EPS:                     # H ∈ [3%, 4%)：要求 C ∈ (1.5%, 2%]
            if c > 1.5 + EPS:
                return 3
        return None

    def _short_pct_case(self, bar: Bar) -> Optional[int]:
        """做空bar形态分类（做多逻辑的完整镜像）。命中返回情况编号 1/2/3，未命中返回 None。

        相对本bar自身开盘价： C' = (Open-Close)/Open*100（close低于open为正），
                              L = (Open-Low)/Open*100（本bar下探幅度）
          情况1: 0.4 < C' <= 1  且 1 <= L < 2
          情况2: 1.0 < C' <= 2  且 2 <= L < 3
          情况3: 1.5 < C' <= 2  且 3 <= L < 4
          情况4: C' > 2 → 规则停止
        """
        if bar.open <= 0:
            return None
        c = (bar.open - bar.close) / bar.open * 100
        l = (bar.open - bar.low) / bar.open * 100
        if c <= 0.4 + EPS or c > 2.0 + EPS:
            return None
        if 1.0 - EPS <= l < 2.0 - EPS:
            if c <= 1.0 + EPS:
                return 1
        elif 2.0 - EPS <= l < 3.0 - EPS:
            if c > 1.0 + EPS:
                return 2
        elif l < 4.0 - EPS:
            if c > 1.5 + EPS:
                return 3
        return None

    def _check_long_pct_rule(self, bar: Bar, pos: PositionView) -> Optional[CloseSignal]:
        """做多腿【特殊规则B】：单bar"冲高回落"（H达1%~4%、但close低于冲高一个档）→ 立即平多仓。

        触发条件完全由本bar自身的 C/H 形态决定（按需求原文，不附加成本/盈亏条件）；
        命中任一情况即平仓（情况4 C>2% 时规则停止，不会进入本方法）。
        """
        case = self._long_pct_case(bar)
        if case is None:
            return None
        self.closed_acc2.add(bar.symbol)
        c_pct = (bar.close - bar.open) / bar.open * 100
        h_pct = (bar.high - bar.open) / bar.open * 100
        reason = (
            f"账户2(做多)逐bar形态规则 | 本bar o={bar.open:.4f} h={bar.high:.4f} c={bar.close:.4f} | "
            f"C={c_pct:+.2f}% H={h_pct:+.2f}% 情况{case}（冲高回落，已是1降）"
            f"→ 立即平多仓(sell {int(pos.acc2_pos)}股)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account2',
            side='sell', volume=int(pos.acc2_pos),
            open_action='buy', reason=reason,
            strategy='dynamic_tp',
        )

    def _check_short_pct_rule(self, bar: Bar, pos: PositionView) -> Optional[CloseSignal]:
        """做空腿【特殊规则B】（做多镜像）：单bar"探底回升"（L达1%~4%、但close仍在低档位）→ 立即买入回补。"""
        case = self._short_pct_case(bar)
        if case is None:
            return None
        self.closed_acc1.add(bar.symbol)
        c_pct = (bar.open - bar.close) / bar.open * 100
        l_pct = (bar.open - bar.low) / bar.open * 100
        reason = (
            f"账户1(做空)逐bar形态规则 | 本bar o={bar.open:.4f} l={bar.low:.4f} c={bar.close:.4f} | "
            f"C'={c_pct:+.2f}% L={l_pct:+.2f}% 情况{case}（探底回升，已是1升）"
            f"→ 立即买入回补(buy {abs(int(pos.acc1_pos))}股)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account1',
            side='buy', volume=abs(int(pos.acc1_pos)),
            open_action='sell', reason=reason,
            strategy='dynamic_tp',
        )

    def on_execution_result(self, signal: CloseSignal, success: bool) -> None:
        """执行失败时回滚去重标记，让下一根 bar 可以重试"""
        if success:
            return
        if signal.account == 'account1':
            self.closed_acc1.discard(signal.symbol)
        else:
            self.closed_acc2.discard(signal.symbol)

    # ------------------------------------------------------------------
    # 单腿决策
    # ------------------------------------------------------------------
    def _check_short(self, bar: Bar, pos: PositionView,
                     st: SymbolState) -> Optional[CloseSignal]:
        entry = pos.acc1_avg_cost
        if entry <= 0 or st.min_close_since_entry is None:
            return None

        # 条件1：现价低于成本价（处于做空盈利区间）
        if bar.close >= entry:
            return None

        # 条件2：峰值跌幅 ≥ 1%
        peak_pct = (entry - st.min_close_since_entry) / entry * 100
        close_pct = get_close_pct(peak_pct)
        if close_pct < 0 or peak_pct < 1.0:
            return None

        # 条件3：从最低点反弹到平仓线以上
        close_line = entry * (1 - peak_pct / 100 + close_pct / 100)
        if bar.close < close_line:
            return None

        self.closed_acc1.add(bar.symbol)
        rebound_pct = (bar.close - st.min_close_since_entry) / st.min_close_since_entry * 100
        entry_dt = st.entry_time.strftime('%Y-%m-%d %H:%M:%S') if st.entry_time else '未知'
        reason = (
            f"账户1(做空)触发 | 建仓: {entry_dt} | "
            f"成本{entry:.2f}, 最低{st.min_close_since_entry:.2f}(跌{peak_pct:.1f}%), "
            f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
            f"现价{bar.close:.2f}(反弹{rebound_pct:.1f}%)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account1',
            side='buy', volume=abs(int(pos.acc1_pos)),
            open_action='sell', reason=reason,
            strategy='dynamic_tp',
        )

    def _check_long(self, bar: Bar, pos: PositionView,
                    st: SymbolState) -> Optional[CloseSignal]:
        entry = pos.acc2_avg_cost
        if entry <= 0 or st.max_close_since_entry is None:
            return None

        # 条件1：现价高于成本价（处于做多盈利区间）
        if bar.close <= entry:
            return None

        # 条件2：峰值涨幅 ≥ 1%
        peak_pct = (st.max_close_since_entry - entry) / entry * 100
        close_pct = get_close_pct(peak_pct)
        if close_pct < 0 or peak_pct < 1.0:
            return None

        # 条件3：从最高点回落到平仓线以下
        close_line = entry * (1 + close_pct / 100)
        if bar.close > close_line:
            return None

        self.closed_acc2.add(bar.symbol)
        pullback_pct = (st.max_close_since_entry - bar.close) / st.max_close_since_entry * 100
        entry_dt = st.entry_time.strftime('%Y-%m-%d %H:%M:%S') if st.entry_time else '未知'
        reason = (
            f"账户2(做多)触发 | 建仓: {entry_dt} | "
            f"成本{entry:.2f}, 最高{st.max_close_since_entry:.2f}(涨{peak_pct:.1f}%), "
            f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
            f"现价{bar.close:.2f}(回撤{pullback_pct:.1f}%)"
        )
        return CloseSignal(
            symbol=bar.symbol, account='account2',
            side='sell', volume=int(pos.acc2_pos),
            open_action='buy', reason=reason,
            strategy='dynamic_tp',
        )

    # ------------------------------------------------------------------
    # 展示字段（给实时 CSV）
    # ------------------------------------------------------------------
    def get_display_state(self, symbol: str) -> dict:
        st = self.states.get(symbol)
        if st is None:
            return {
                'day_max_since_entry': '',
                'day_min_since_entry': '',
                'day_max_since_entry_pct': '',
                'day_min_since_entry_pct': '',
            }

        min_since = st.min_close_since_entry
        max_since = st.max_close_since_entry
        sell_entry = st.acc1_entry_price
        buy_entry = st.acc2_entry_price

        day_min_since_entry = round(min_since, 4) if min_since is not None else ''
        day_max_since_entry = round(max_since, 4) if max_since is not None else ''

        if min_since is not None and sell_entry and sell_entry > 0:
            day_min_since_entry_pct = round((min_since - sell_entry) / sell_entry * 100, 4)
        else:
            day_min_since_entry_pct = ''

        if max_since is not None and buy_entry and buy_entry > 0:
            day_max_since_entry_pct = round((max_since - buy_entry) / buy_entry * 100, 4)
        else:
            day_max_since_entry_pct = ''

        return {
            'day_max_since_entry': day_max_since_entry,
            'day_min_since_entry': day_min_since_entry,
            'day_max_since_entry_pct': day_max_since_entry_pct,
            'day_min_since_entry_pct': day_min_since_entry_pct,
        }
