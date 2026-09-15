# -*- coding: utf-8 -*-
"""分段动态止盈策略

原 close.py 中的业务规则完整迁移至此，但只做决策，不做任何下单/写CSV动作。
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from strategy.base import BaseStrategy, Bar, PositionView, CloseSignal
from logger import get_logger


def get_close_pct(peak_pct: float) -> float:
    """根据峰值涨幅/跌幅百分比，查表得到平仓线保留百分比

    峰值越大（盈利越多），保留比例越低——让利润奔跑。
    peak_pct < 1 时返回 -1（不触发）。
    """
    if peak_pct < 1:
        return -1.0
    if peak_pct < 2:
        return 0.65
    if peak_pct < 3:
        return 1.0
    if peak_pct < 4:
        return 1.5
    if peak_pct < 10:
        return peak_pct * 0.50
    if peak_pct < 15:
        return peak_pct * 0.45
    if peak_pct < 20:
        return peak_pct * 0.40
    if peak_pct < 25:
        return peak_pct * 0.35
    if peak_pct < 30:
        return peak_pct * 0.30
    if peak_pct < 60:
        return peak_pct * 0.25
    if peak_pct < 100:
        return peak_pct * 0.20
    return peak_pct * 0.10


@dataclass
class SymbolState:
    """单标的的策略状态（跨 K 线持久化于策略实例）"""
    entry_time: Optional[datetime] = None
    acc1_entry_price: Optional[float] = None
    acc2_entry_price: Optional[float] = None
    min_close_since_entry: Optional[float] = None
    max_close_since_entry: Optional[float] = None


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

        # ---- 更新建仓后极值（只用 close 价，与原策略一致）----
        if st.min_close_since_entry is None:
            st.min_close_since_entry = bar.close
            st.max_close_since_entry = bar.close
        else:
            st.min_close_since_entry = min(st.min_close_since_entry, bar.close)
            st.max_close_since_entry = max(st.max_close_since_entry, bar.close)

        signals: list[CloseSignal] = []

        # 账户1（做空腿）：已有持仓、未平过、有极值
        if (pos.acc1_pos < 0
                and bar.symbol not in self.closed_acc1
                and st.min_close_since_entry is not None):
            sig = self._check_short(bar, pos, st)
            if sig:
                signals.append(sig)

        # 账户2（做多腿）：已有持仓、未平过、有极值
        if (pos.acc2_pos > 0
                and bar.symbol not in self.closed_acc2
                and st.max_close_since_entry is not None):
            sig = self._check_long(bar, pos, st)
            if sig:
                signals.append(sig)

        return signals

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
