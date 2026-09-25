# -*- coding: utf-8 -*-
"""策略运行器（框架↔策略适配层）

职责：
1. 每个新 bar 从 TWS 拉取权威持仓快照，注入策略
2. 调用策略获取 CloseSignal 列表
3. 执行信号（下单、CSV 回写）
4. 把执行结果回传给策略（失败时策略回滚去重标记）

设计要点：
- 策略完全不感知 IB / CSV / 全局状态
- 建仓时间在此处登记，用于跳过建仓前的历史 bar（避免无谓的拉持仓）
- active / activate_at 与收市前强制平仓窗口联动：强制平仓开始时 stop()
"""
import asyncio
import datetime
from typing import Optional

import pytz
from ib_async import IB

from strategy.base import BaseStrategy, Bar, PositionView, CloseSignal
from position import get_positions
from logger import get_logger

_EST = pytz.timezone('US/Eastern')


class StrategyRunner:
    def __init__(self, strategy: BaseStrategy,
                 ib1: IB, ib2: IB, account1: str, account2: str,
                 sell_csv_path, buy_csv_path,
                 close_manager,
                 open_time: Optional[datetime.datetime] = None):
        self.strategy = strategy
        self.ib1 = ib1
        self.ib2 = ib2
        self.account1 = account1
        self.account2 = account2
        self.sell_csv_path = sell_csv_path
        self.buy_csv_path = buy_csv_path
        self.close_manager = close_manager
        self.logger = get_logger()
        # 当日开盘时间（美东 aware，整分钟）：开盘前两分钟的窗口 bar（第一分钟 bar 的
        # dt=开盘时刻，早于开盘+20s 的建仓时刻）需放行建仓前门控，供开盘特殊策略评估
        self.open_time = open_time

        # 让路开关：强制平仓窗口内/结束后不再触发运行时信号
        self.active = False
        self.activate_at: Optional[datetime.datetime] = None
        # 【R5 标的级调平暂停】取代旧的全局 bool 开关。
        # 每轮调平只把"本轮实际在调的标的"加入该集合；集合之外的标的 bar 照常评估执行。
        # 被暂停标的：信号不丢弃，先排队，本轮解除暂停时重放（走既有 dispatch 失败/回滚语义）。
        self._paused_symbols: set = set()
        self._paused_pending: list = []   # [(CloseSignal, PositionView), ...]
        # 每只标的的最早建仓时间（用于跳过建仓前的历史 bar），统一为美东 aware
        self.entry_times: dict = {}

    # ------------------------------------------------------------------
    # 时区归一化
    # ------------------------------------------------------------------
    @staticmethod
    def _to_est(dt: datetime.datetime) -> datetime.datetime:
        """统一为美东 aware datetime。

        关键：hedge.py 的建仓成交时间用的是 naive 本地时间
        （datetime.datetime.now()），而 Bar.dt 是美东 aware datetime——
        naive 与 aware 直接比较会抛 TypeError（每根 bar 都炸一次，策略永远无法触发）。
        因此在 runner 边界统一归一化：
        - aware 输入 → 转换为美东；
        - naive 输入 → astimezone 按 CPython 语义视为系统本地时间，再换算美东。
        """
        if dt.tzinfo is None:
            return dt.astimezone(_EST)
        return dt.astimezone(_EST)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def start(self, delay_seconds: int = 60,
              first_bar_boundary: Optional[datetime.datetime] = None) -> None:
        """激活运行时策略，两种生效时点设置：

        - first_bar_boundary（优先）：在第一根 1 分钟 bar 的收盘边界（开市+60s）生效——
          此时恰好收到"第一根新 bar"事件（其开盘价即第一根 bar 的最终收盘价），
          立即对第一根 bar 之后的行情做出反应，不再像旧的固定 delay 那样多等半分钟
          错过第一根 bar；
        - delay_seconds：兼容旧语义，now + delay 后生效。

        若就绪时刻晚于边界（结算慢），自动顺延为"立即生效"：
        此时第一根 bar 已过，从下一根新 bar 开始处理即可，既不误等也不提前激活。

        注意：activate_at 必须保持 naive 系统本地时间，
        因为 on_bar() 用 datetime.datetime.now()（naive）与其比较 ——
        aware/naive 混比会抛 TypeError。aware 边界先转本地 naive 再参与比较。
        """
        now = datetime.datetime.now()  # naive 系统本地时间
        if first_bar_boundary is not None:
            boundary = first_bar_boundary
            if boundary.tzinfo is not None:
                boundary = boundary.astimezone().replace(tzinfo=None)
            activate_at = boundary if boundary > now else now
            mode = (f"第一根bar边界 {boundary.strftime('%H:%M:%S')}"
                    + ("" if boundary > now else "（结算慢，顺延为立即生效）"))
        else:
            activate_at = now + datetime.timedelta(seconds=delay_seconds)
            mode = f"延迟 {delay_seconds} 秒"
        self.active = True
        self.activate_at = activate_at
        self.logger.info(
            f"🟢 运行时策略已激活 | 模式: {mode} | "
            f"生效时间: {activate_at.strftime('%H:%M:%S')}"
        )

    def stop(self) -> None:
        """让路：强制平仓窗口内不再触发运行时信号"""
        self.active = False
        self.logger.info("🔴 运行时策略已停用（强制平仓窗口接管）")

    def pause_symbols(self, symbols) -> None:
        """【R5】标的级调平暂停：只把本轮实际在调的标的加入暂停集合。

        背景（2026-09-14 OKLO 实例 + 2026-09-21 事故）：
        - 调平窗口内策略基于中间快照（0→28→84 剧烈变化过程）发出平仓单，会与调平单
          （补买/反向补卖）交叉撮合 → 超卖/裸仓，必须在"正在调的标的"上禁发平仓单；
        - 旧的全局 bool 一刀切暂停，副作用是 2026-09-21 里 RDW/MARA/TRLV/BTDR/KEEL
          这 5 只标的开盘窗口 bar1 判定被整根丢弃（特殊策略状态永久丢失）。
        标的级暂停消除了这个副作用：不在集合里的标的 bar 照常评估执行。
        """
        added = [s for s in (symbols or []) if s and s not in self._paused_symbols]
        for s in added:
            self._paused_symbols.add(s)
        if added:
            self.logger.info(f"⏸️ 标的级调平暂停: {sorted(added)}（其余标的 bar 正常评估执行）")

    def unpause_symbols(self, symbols=None) -> None:
        """【R5】解除标的级调平暂停（symbols=None → 全部解除）"""
        if symbols is None:
            cleared = sorted(self._paused_symbols)
            self._paused_symbols.clear()
        else:
            cleared = [s for s in (symbols or []) if s in self._paused_symbols]
            for s in cleared:
                self._paused_symbols.discard(s)
        if cleared:
            self.logger.info(f"▶️ 标的级调平暂停解除: {cleared}")

    async def replay_paused_signals(self) -> int:
        """【R5】轮末重放暂停期间排队的信号。

        严格走既有 dispatch 失败/回滚语义：
        - 成功 → on_execution_result(sig, True)（保留去重标记）；
        - 失败/异常 → on_execution_result(sig, False)（策略回滚去重标记，下一根 bar 可重试）。
        绝不做"评估了但不执行"：信号在生成时已按既有语义设置去重标记，
        这里要么执行、要么回滚，二者必居其一。
        """
        if not self._paused_pending:
            return 0
        pending, self._paused_pending = self._paused_pending, []
        self.logger.info(f"▶️ 轮末重放 {len(pending)} 个调平期间排队的平仓信号")
        # 按 PositionView 分组（同一根 bar 的信号共享同一快照视图）后统一分发
        groups: dict = {}
        order: list = []
        for sig, pos in pending:
            if pos not in groups:
                groups[pos] = []
                order.append(pos)
            groups[pos].append(sig)
        for pos in order:
            await self._execute_signals(groups[pos], pos)
        return len(pending)

    def _queue_paused_signals(self, signals, pos) -> None:
        for sig in signals:
            self._paused_pending.append((sig, pos))
        self.logger.info(
            f"⏸️ [{pos.symbol}] 该标的正在调平：{len(signals)} 个平仓信号已排队，"
            f"本轮解除暂停后按 dispatch 语义重放（不会静默丢弃）"
        )

    # ------------------------------------------------------------------
    # 开盘窗口判定
    # ------------------------------------------------------------------
    def _in_open_window(self, bar: Bar) -> bool:
        """bar 是否落在当日开盘前两分钟窗口内 [open, open+2min)（美东 aware 比较）"""
        if self.open_time is None:
            return False
        try:
            t = self._to_est(bar.dt)
            o = self._to_est(self.open_time)
        except Exception:
            return False
        delta = (t - o).total_seconds()
        return 0 <= delta < 120

    # ------------------------------------------------------------------
    # 事件入口
    # ------------------------------------------------------------------
    def on_entry(self, symbol: str, account: str,
                 price: float, volume: int, dt: datetime.datetime) -> None:
        """建仓成交时由 hedge 模块回调

        注意 dt 使用真实成交时间（原代码用订阅时刻近似，现在修好）。
        无论上游传入 naive（本地）还是 aware，都归一化为美东 aware 后再存储/透传，
        保证与 Bar.dt（美东 aware）的比较永远安全。
        """
        dt = self._to_est(dt)
        old = self.entry_times.get(symbol)
        if old is None or dt < old:
            self.entry_times[symbol] = dt
        self.strategy.on_entry(symbol, account, price, volume, dt)

    async def on_bar(self, bar: Bar) -> None:
        """每根新 1 分钟 bar 调用"""
        if not self.active:
            return
        if self.activate_at and datetime.datetime.now() < self.activate_at:
            return

        # 【R5 标的级暂停】不再整根跳过 bar（旧逻辑会把开盘窗口 bar1 的判定整根丢弃）；
        # 暂停只作用于"信号执行"：被暂停标的照常评估（策略状态/极值正常更新），
        # 信号排队、本轮解除暂停时重放。见 _queue_paused_signals / replay_paused_signals。

        # 建仓前的历史 bar 直接跳过（不拉持仓，省 TWS 请求）
        # 例外：开盘前两分钟窗口内的 bar —— 第一分钟 bar 的 dt（=开盘时刻）天然早于
        # 开盘+20s 的建仓时刻，若按旧门控会被丢弃，而开盘特殊策略恰恰需要这第一分钟数据
        et = self.entry_times.get(bar.symbol)
        if et is None or bar.dt <= et:
            if self._in_open_window(bar):
                self.logger.debug(
                    f"🪟 [{bar.symbol}] bar dt={bar.dt.strftime('%H:%M:%S')} 落在开盘前两分钟窗口"
                    f"（建仓前门控例外放行，供开盘特殊策略评估）"
                )
            else:
                return

        # 1. 拉取权威持仓快照
        try:
            snap1 = await get_positions(self.ib1, self.account1)
            snap2 = await get_positions(self.ib2, self.account2)
        except Exception as e:
            self.logger.warning(f"⚠️ [{bar.symbol}] 策略取持仓失败，跳过本次 bar: {e}")
            return

        p1 = snap1.get(bar.symbol, {'position': 0, 'avgCost': 0.0})
        p2 = snap2.get(bar.symbol, {'position': 0, 'avgCost': 0.0})
        pos = PositionView(
            symbol=bar.symbol,
            acc1_pos=int(p1['position']), acc1_avg_cost=float(p1['avgCost']),
            acc2_pos=int(p2['position']), acc2_avg_cost=float(p2['avgCost']),
        )

        # 2. 问策略：现在该做什么？
        try:
            signals = self.strategy.on_bar(bar, pos)
        except Exception as e:
            self.logger.error(f"❌ [{bar.symbol}] 策略执行异常: {e}", exc_info=True)
            return

        if not signals:
            return

        # 【R5】本标的正在调平 → 信号排队，轮末 replay_paused_signals 重放
        if bar.symbol in self._paused_symbols:
            self._queue_paused_signals(signals, pos)
            return

        await self._execute_signals(signals, pos)

    async def _execute_signals(self, signals, pos) -> None:
        """并发执行信号（异常隔离）+ 结果回传策略（失败回滚去重标记）

        正常 bar 路径与【R5】轮末重放路径共用同一套执行/回滚语义。"""
        results = await asyncio.gather(
            *(self._dispatch(sig, pos) for sig in signals),
            return_exceptions=True
        )
        for sig, r in zip(signals, results):
            if isinstance(r, BaseException):
                self.logger.error(
                    f"❌ 执行信号异常 {sig.symbol} {sig.account}: {r!r}"
                )
                success = False
            else:
                success = bool(r)
            # 把结果回传给策略（失败时回滚去重标记）
            try:
                self.strategy.on_execution_result(sig, success)
            except Exception as e:
                self.logger.error(f"❌ 策略回传结果异常: {e}")

    # ------------------------------------------------------------------
    # 展示字段（给实时 CSV）
    # ------------------------------------------------------------------
    def get_display_state(self, symbol: str) -> dict:
        try:
            return self.strategy.get_display_state(symbol)
        except Exception as e:
            self.logger.debug(f"⚠️ [{symbol}] 拉展示字段失败: {e}")
            return {}

    # ------------------------------------------------------------------
    # 内部分发
    # ------------------------------------------------------------------
    async def _dispatch(self, sig: CloseSignal, pos: PositionView) -> bool:
        self.logger.info(f"⚡ 策略信号: {sig.symbol} | {sig.reason}")
        if sig.account == 'account1':
            ib, acct, csv = self.ib1, self.account1, self.sell_csv_path
            open_price = pos.acc1_avg_cost
        else:
            ib, acct, csv = self.ib2, self.account2, self.buy_csv_path
            open_price = pos.acc2_avg_cost

        return await self.close_manager.run_close_signal(
            ib, acct, sig.symbol, sig.side, sig.volume,
            open_price, sig.open_action, csv,
            target_lot_id=sig.target_lot_id,
            strategy=sig.strategy,
            reason=sig.reason,
        )
