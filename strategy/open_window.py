# -*- coding: utf-8 -*-
"""开盘前两分钟特殊策略（OpenWindowStrategy）

开盘后（当日第一、第二根 1 分钟 bar）对每一个持仓股票，逐账户逐腿独立评估；
未命中任何情况的腿照常由原策略（inner 策略）接管。

记 bar1（第一分钟）的 H = (high - open) / open * 100（%），C = (close - open) / open * 100（%）：

做多（账户2）规则表：
  情况1  H < 1 且 C < 1                         → 不进行任何操作（该腿窗口内定案）
  情况2  1 <= H < 2 且 0.5 <= C < 1             → 立即平仓（卖出清多仓）
  情况3  2 <= H < 3 且 1 <= C < 2               → 立即平仓
  情况4  3 <= H < 4 且 C <= 2 且 C >= H/2       → 立即平仓
  情况5  1 <= H < 2 且 1 <= C < 2               → 若 bar2.close < bar1.close，则平仓
  情况6  2 <= H < 3 且 2 <= C < 3               → 若 bar2.close < bar1.close，则平仓
  情况7  3 <= H < 4 且 C >= 2                   → 若 bar2.close 较 bar1.close 下跌 < 2%
                                       且 bar2.close > bar1 的 (open+high)/2，则平仓

做空（账户1）为上述完整镜像："上涨"→"下跌"（H → (open-high)/open，C → (open-close)/open），
"平仓（卖出）"→"平仓（买入回补）"；情况7 中间值取 bar1 的 (open+low)/2，
bar2 条件镜像为"上涨 < 2% 且 < (open+low)/2"。

语义要点（已与用户确认）：
1. 7 种情况是"开盘前两分钟窗口"内的完整决策：某腿命中情况后，该腿在窗口内的结局由本策略
   决定（平仓 / 不操作），窗口内原策略对该腿的信号被抑制（并回滚原策略内部去重标记，
   防止其永久封死该腿）；bar2 收盘后窗口结束，原策略完全恢复（若该腿尚未平仓）。
2. 特殊信号执行失败 → 该腿定案回滚为"未定"，后续 bar 交还原策略裁决（绝不允许
   "平仓失败 + 窗口抑制"叠加导致仓位无人管理）。
3. 情况7 特别说明：原始表述"bar2.close 较 bar1.close 下跌 >= 2% 且 > (open1+high1)/2"
   在 H ∈ [3%, 4%) 区间内数学上不可行（c2 <= 0.98*c1 <= 0.98*h1，而 (o1+h1)/2 > 0.98*h1
   恰好当 H < 4.17% 时成立，两条件可行域为空集）。经用户确认修订为"下跌 < 2%（即
   c2 > 0.98*c1）且 > (open1+high1)/2"，即"冲高后温和回落、仍守住中间值"时平多
   （做空镜像："回涨 < 2% 且 < (open1+low1)/2"时平空）。
4. 窗口定位以开盘时间（open_time，美东 aware，来自 TWS 合约）为准：
   [open, open+1min) = bar1 槽位，[open+1min, open+2min) = bar2 槽位，其余 bar 不入窗口。
   open_time 缺失时退化为按 bar 序判定（首个处理的 bar = bar1，第二个 = bar2）。
5. "持仓股票"以 bar 到达时刻的权威持仓快照为准（PositionView：acc2_pos > 0 / acc1_pos < 0）。
"""
from datetime import datetime
from typing import Optional

from strategy.base import BaseStrategy, Bar, PositionView, CloseSignal
from logger import get_logger


# ---- 腿结局状态 ----
UNSET = 'unset'          # 未命中情况（或平仓失败后回滚）→ 原策略有最终裁决权
NO_ACTION = 'no_action'  # 命中情况1，或情况5/6/7 判定"不平仓" → 窗口内不操作
CLOSED = 'closed'        # 已发出平仓信号（等待执行结果；失败会被回滚）
PENDING = 'pending'      # bar1 命中情况5/6/7，等待 bar2 裁决

# 窗口内该腿由特殊策略定案时，需要抑制原策略同腿信号的状态集合
SUPPRESS_OUTCOMES = frozenset({NO_ACTION, CLOSED, PENDING})

# 边界容差：消除十进制边界值（如恰好 0.5%、C=H/2）的浮点表示误差，
# 使"恰好落在 >= / <= 边界上"的 bar 按规格语义（含边界）确定性地归类；
# 价格精度为 4 位小数，真实的非边界值与边界的距离远大于 1e-9，不会产生误判。
EPS = 1e-9


def _lt(a: float, b: float) -> bool:
    """a 严格小于 b（恰好等于 b 视为不属于"小于"一侧）"""
    return a < b - EPS


def _gt(a: float, b: float) -> bool:
    """a 严格大于 b（恰好等于 b 视为不属于"大于"一侧）"""
    return a > b + EPS


def _ge(a: float, b: float) -> bool:
    """a >= b（含边界，容忍浮点抖动）"""
    return a >= b - EPS


def _le(a: float, b: float) -> bool:
    """a <= b（含边界，容忍浮点抖动）"""
    return a <= b + EPS


class _LegState:
    """单腿（做多腿/做空腿）的窗口状态"""
    __slots__ = ('outcome', 'case_id', 'outer_closed', 'judged_bar1')

    def __init__(self):
        self.outcome: str = UNSET
        self.case_id: Optional[int] = None     # 2~7：命中的情况编号
        self.outer_closed: bool = False        # 平仓信号由本策略发出（供执行失败回滚识别）
        self.judged_bar1: bool = False         # 该腿是否已对 bar1 裁决过（双腿各自独立）


class _State:
    """单标的的窗口状态（跨 bar 持久化于策略实例）"""
    __slots__ = ('date', 'ref_bar', 'long', 'short', 'bars_seen', 'seq_slots_consumed')

    def __init__(self):
        self.date = None                        # 该状态所属交易日（跨天自动重置）
        self.ref_bar: Optional[Bar] = None      # bar1 参考 bar（情况2~7 的判定基准）
        self.long = _LegState()
        self.short = _LegState()
        self.bars_seen = 0                      # 仅 open_time 缺失的 bar 序退化模式使用
        self.seq_slots_consumed = False         # 退化模式下 bar1/bar2 槽位是否已消耗


class OpenWindowStrategy(BaseStrategy):
    """开盘前两分钟特殊策略（装饰器：inner 为原策略，窗口外/未命中情况完全透传）"""

    def __init__(self, inner: BaseStrategy, open_time: Optional[datetime] = None):
        self.inner = inner
        # 当日开盘时间（美东 aware，整分钟，如 09:30:00）；None 时退化为 bar 序判定
        self.open_time = open_time
        self.states: dict = {}
        self.logger = get_logger()

    # ------------------------------------------------------------------
    # 生命周期（透传给原策略 + 自身失败回滚）
    # ------------------------------------------------------------------
    def on_entry(self, symbol: str, account: str,
                 price: float, volume: int, dt: datetime) -> None:
        """建仓成交回调：原样透传给原策略"""
        self.inner.on_entry(symbol, account, price, volume, dt)

    def get_display_state(self, symbol: str) -> dict:
        """展示字段透传给原策略"""
        return self.inner.get_display_state(symbol)

    def on_execution_result(self, signal: CloseSignal, success: bool) -> None:
        # 原策略的去重标记回滚语义原样透传（无论信号来自哪一侧）
        self.inner.on_execution_result(signal, success)
        if success:
            return
        # 本策略发出的特殊平仓单执行失败 → 该腿定案回滚，交还原策略
        st = self.states.get(signal.symbol)
        if st is None:
            return
        leg = st.short if signal.account == 'account1' else st.long
        if leg.outer_closed:
            self.logger.warning(
                f"⚠️ [{signal.symbol}] 开盘特殊策略平仓信号执行失败 —— "
                f"回滚该腿窗口定案，后续 bar 交还原策略裁决"
            )
            leg.outcome = UNSET
            leg.case_id = None
            leg.outer_closed = False

    # ------------------------------------------------------------------
    # 每根 bar 决策
    # ------------------------------------------------------------------
    def on_bar(self, bar: Bar, pos: PositionView) -> list:
        st = self._state_for(bar)
        slot = self._window_slot(bar, st)      # 0=bar1 槽位 | 1=bar2 槽位 | None=窗口外

        outer: list = []
        if slot is not None:
            # 做多腿（账户2）
            if pos.acc2_pos > 0 and self._resolve_leg(st, slot, st.long, 'long', bar) == 'closed':
                outer.append(self._make_signal(bar, st, 'account2', int(pos.acc2_pos), st.long.case_id))
            # 做空腿（账户1）
            if pos.acc1_pos < 0 and self._resolve_leg(st, slot, st.short, 'short', bar) == 'closed':
                outer.append(self._make_signal(bar, st, 'account1', abs(int(pos.acc1_pos)), st.short.case_id))

        # 原策略照常评估（其内部自带建仓时间门控：bar1 通常早于建仓时刻而被跳过）
        inner_sigs = self.inner.on_bar(bar, pos)

        if slot is None:
            return outer + list(inner_sigs)

        # 窗口内：该腿已被特殊策略定案（平仓/不操作/待bar2裁决）→ 抑制原策略同腿信号，
        # 并回滚其内部去重标记（否则原策略该腿被永久封死，窗口结束后也无法再触发）
        kept = []
        for sig in inner_sigs:
            leg = st.short if sig.account == 'account1' else st.long
            if leg.outcome in SUPPRESS_OUTCOMES:
                self.inner.on_execution_result(sig, success=False)
                continue
            kept.append(sig)
        return outer + kept

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------
    def _state_for(self, bar: Bar) -> _State:
        """取该标的的窗口状态；跨交易日自动重置"""
        st = self.states.get(bar.symbol)
        day = bar.dt.date()
        if st is None:
            st = _State()
            st.date = day
            self.states[bar.symbol] = st
            return st
        if st.date != day:
            st.date = day
            st.ref_bar = None
            st.long = _LegState()
            st.short = _LegState()
            st.bars_seen = 0
            st.seq_slots_consumed = False
        return st

    def _window_slot(self, bar: Bar, st: _State) -> Optional[int]:
        """判定 bar 所处的窗口槽位：0=第一分钟，1=第二分钟，None=窗口外

        仅当 bar 与 open_time 同一天时才能用时间定位（open_time 只对应当日开盘）；
        跨日（策略实例跨多日运行）退化为 bar 序模式。
        """
        ot = self.open_time
        if (ot is not None and bar.dt.tzinfo is not None and ot.tzinfo is not None
                and bar.dt.date() == ot.date()):
            delta = (bar.dt - ot).total_seconds()
            if 0 <= delta < 60:
                return 0
            if 60 <= delta < 120:
                return 1
            return None
        # bar 序退化模式（open_time 缺失 / 时区不可比较 / 跨日后开盘时间未知）：
        # 每标的每日仅第一、第二根被处理的 bar 分别视为 bar1 / bar2
        if st.seq_slots_consumed:
            return None
        st.bars_seen += 1
        if st.bars_seen <= 2:
            return st.bars_seen - 1
        st.seq_slots_consumed = True
        return None

    def _resolve_leg(self, st: _State, slot: int, leg: _LegState,
                     name: str, bar: Bar) -> Optional[str]:
        """单腿窗口裁决；返回 'closed' 表示本根 bar 需发出平仓信号，None 表示无动作"""
        if leg.outcome in (NO_ACTION, CLOSED):
            # 已定案：不平仓不动作；平仓已在 bar1 发出（不会重复）；平仓失败已被回滚为 UNSET
            return None

        if slot == 0:
            if leg.judged_bar1:
                return None      # 该腿已对 bar1 裁决过（重复/乱序 bar1）→ 不重判
            st.ref_bar = bar    # 参考 bar 两腿共享（同一根 bar）
            leg.judged_bar1 = True
            case = self._judge_bar1(name, bar)
            if case == 1:
                leg.outcome = NO_ACTION
            elif case in (2, 3, 4):
                leg.outcome = CLOSED
                leg.case_id = case
                leg.outer_closed = True
                return 'closed'
            elif case in (5, 6, 7):
                leg.outcome = PENDING
                leg.case_id = case
            # None = 未命中任何情况 → 该腿保持 UNSET，由原策略裁决
            return None

        # slot == 1（bar2）：仅 bar1 命中情况5/6/7 的腿可能产生动作
        if st.ref_bar is not None and leg.outcome == PENDING and leg.case_id in (5, 6, 7):
            if self._judge_bar2(name, leg.case_id, st.ref_bar, bar):
                leg.outcome = CLOSED
                leg.outer_closed = True
                return 'closed'
            leg.outcome = NO_ACTION    # 条件不满足 → 窗口内"不操作"定案
        return None

    def _judge_bar1(self, name: str, bar: Bar) -> Optional[int]:
        """bar1（第一分钟）规则判定。返回命中的情况编号 1~7，未命中返回 None

        name: 'long'（做多腿）| 'short'（做空腿，镜像）
        """
        o = float(bar.open)
        if o <= 0:
            return None
        h_pct = (float(bar.high) - o) / o * 100.0
        c_pct = (float(bar.close) - o) / o * 100.0
        if name == 'short':
            h_pct = -h_pct            # 做空镜像：上涨 → 下跌
            c_pct = -c_pct

        if _lt(h_pct, 1.0) and _lt(c_pct, 1.0):
            return 1                                  # 情况1：不操作
        if _ge(h_pct, 1.0) and _lt(h_pct, 2.0) and _ge(c_pct, 0.5) and _lt(c_pct, 1.0):
            return 2                                  # 情况2：立即平仓
        if _ge(h_pct, 2.0) and _lt(h_pct, 3.0) and _ge(c_pct, 1.0) and _lt(c_pct, 2.0):
            return 3                                  # 情况3：立即平仓
        if _ge(h_pct, 3.0) and _lt(h_pct, 4.0) and _le(c_pct, 2.0) and _ge(c_pct, h_pct / 2.0):
            return 4                                  # 情况4：立即平仓
        if _ge(h_pct, 1.0) and _lt(h_pct, 2.0) and _ge(c_pct, 1.0) and _lt(c_pct, 2.0):
            return 5                                  # 情况5：待 bar2 裁决
        if _ge(h_pct, 2.0) and _lt(h_pct, 3.0) and _ge(c_pct, 2.0) and _lt(c_pct, 3.0):
            return 6                                  # 情况6：待 bar2 裁决
        if _ge(h_pct, 3.0) and _lt(h_pct, 4.0) and _ge(c_pct, 2.0):
            return 7                                  # 情况7：待 bar2 裁决（修订版）
        return None

    def _judge_bar2(self, name: str, case: int, ref: Bar, bar2: Bar) -> bool:
        """bar2（第二分钟）条件判定（情况5/6/7）"""
        c1 = float(ref.close)
        if c1 <= 0:
            return False
        c2 = float(bar2.close)
        if case in (5, 6):
            # 做多：bar2 close 低于 bar1 close 则平仓；做空镜像：高于则平仓
            return c2 < c1 if name == 'long' else c2 > c1
        if case == 7:
            o1 = float(ref.open)
            if name == 'long':
                # 修订版：下跌 < 2%（即 c2 > 0.98*c1）且 bar2 close 在 bar1 的 (open, high) 中间值上方
                return _gt(c2, 0.98 * c1) and _gt(c2, (o1 + float(ref.high)) / 2.0)
            # 做空镜像：回涨 < 2%（即 c2 < 1.02*c1）且 bar2 close 在 bar1 的 (open, low) 中间值下方
            return _lt(c2, 1.02 * c1) and _lt(c2, (o1 + float(ref.low)) / 2.0)
        return False

    def _make_signal(self, bar: Bar, st: _State, account: str,
                     volume: int, case_id: Optional[int]) -> CloseSignal:
        if account == 'account2':
            side, open_action, who = 'sell', 'buy', '账户2(做多)'
        else:
            side, open_action, who = 'buy', 'sell', '账户1(做空)'
        ref = st.ref_bar
        if ref is None:
            detail = f"情况{case_id}命中"
        elif case_id in (2, 3, 4):
            detail = (
                f"bar1: o={ref.open:.4f} h={ref.high:.4f} l={ref.low:.4f} c={ref.close:.4f} | "
                f"情况{case_id}（bar1 形态命中，立即平仓）"
            )
        else:
            detail = (
                f"bar1: o={ref.open:.4f} h={ref.high:.4f} l={ref.low:.4f} c={ref.close:.4f} | "
                f"bar2: close={bar.close:.4f} | 情况{case_id}（bar2 条件命中，立即平仓）"
            )
        return CloseSignal(
            symbol=bar.symbol, account=account,
            side=side, volume=volume, open_action=open_action,
            reason=f"{who} 开盘前两分钟特殊策略 | {detail}",
        )
