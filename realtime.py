# -*- coding: utf-8 -*-
"""
实时数据模块

职责：
- 只做1分钟K线的接收、处理、写CSV
- 每根 bar 触发一次 StrategyRunner.on_bar（策略决策和执行交给 runner）
- 建仓后极值等展示字段从 runner（→策略）查询
- 【2026-09-15 事故修复】流心跳看门狗 + 断流自动重订阅 + TWS 断连/恢复事件钩子

背景（2026-09-15 故障复盘）：
  10:18 TWS↔IBKR 断连数分钟（Error 1100/10182，12 路 keepUpToDate 流全部失效），
  1102 恢复后 TWS **不会**自动重建这些历史流。本模块此前 100% 依赖 bar 推送驱动
  策略决策与 CSV 记录 → 推送一死，策略与日志双双静默瘫痪 5.5 小时（10:18~15:50），
  持仓无人管理，直到收市前 force_close_until_flat 兜底强平。
  修复：
  1) 心跳看门狗：交易窗口内 5 分钟无任何 bar 事件 → CRITICAL 告警 + 自动重订阅全部流；
  2) TWS 错误事件钩子：1100/1102（断连/恢复）、10182/10183（流失效）实时落日志并触发修复
     （原先这些错误只打印在控制台，连日志文件都没有）；
  3) 重订阅回放安全：按 bar 时间戳去重（不重复写 CSV），且断流前已错过的历史 bar
     只补写数据、不重放入策略（防止数百根过期 bar 用旧价格冲撞决策状态）。
"""
import asyncio
import pandas as pd
import pytz
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from ib_async import IB, Stock

from models import StockInfo
from constants import REALTIME_COLUMNS, DATETIME_FORMAT
from logger import get_logger
from strategy.base import Bar

_EST = pytz.timezone('US/Eastern')

# ==================== 流心跳看门狗参数 ====================
# 1 分钟 bar 在开盘后应每根都有推送事件（含未完成 bar 的 tick 刷新）。
# 全市场 15 只流动性标的里有任意一只 tick，心跳即刷新——
# 因此「5 分钟零事件」是可靠的断流信号（比误报余量大，比 9/15 的 5.5 小时盲区短得多）。
FEED_CHECK_INTERVAL_SECONDS = 60   # 看门狗巡检周期
FEED_STALE_SECONDS = 300           # 判定断流的静默时长阈值


class RealtimeDataRecorder:
    def __init__(self, ib: IB, save_dir: Path, runner=None):
        self.ib = ib
        self.save_dir = save_dir
        self.runner = runner
        self.daily_stats: Dict[str, dict] = {}
        self.logger = get_logger()

        self.save_dir.mkdir(parents=True, exist_ok=True)

        # ---- 订阅与回放安全状态（2026-09-15 事故修复） ----
        # 当前已订阅标的（重订阅的名单源）
        self._subscribed_symbols: Dict[str, StockInfo] = {}
        # 全局心跳：任意一次 bar 事件推送即刷新（naive 本地时间，仅用于间隔比较）
        self._last_bar_event: Optional[datetime] = None
        # 每标的已处理到的最大 bar 时间戳（EST aware）——重订阅回放去重，避免重复写 CSV
        self._processed_until: Dict[str, datetime] = {}
        # 每标的策略门控边界（EST aware）：恢复后，dt < 边界的 bar 只补写数据、不触发策略
        self._strategy_resume_boundary: Dict[str, datetime] = {}
        # 回放跳过日志去重（每标的每次断流只记一条）
        self._replay_log_pending: set = set()
        # 并发修复保护（多个 10182 事件/看门狗同时触发时只跑一轮）
        self._recovery_in_flight = False
        # 看门狗后台任务
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_quiet = lambda: False

        # TWS 断连/流失效事件钩子（原先只打印在控制台，日志文件完全不可见）
        self._attach_tws_event_listeners()

    # ------------------------------------------------------------------
    # TWS 事件钩子（断连感知 + 快速修复触发）
    # ------------------------------------------------------------------
    def _attach_tws_event_listeners(self) -> None:
        """订阅 IB API 的连接级/错误级事件。失败不影响主功能（看门狗仍兜底）"""
        try:
            self.ib.errorEvent += self._on_tws_error
            self.ib.connectedEvent += self._on_tws_connected
            self.ib.disconnectedEvent += self._on_tws_disconnected
            self.logger.debug("🔁 已订阅 TWS 连接事件（errorEvent/connectedEvent/disconnectedEvent）")
        except Exception as e:
            self.logger.warning(f"⚠️ 订阅 TWS 连接事件失败（看门狗仍会兜底）: {e}")

    def _on_tws_error(self, reqId=None, errorCode=None, errorString=None, contract=None) -> None:
        """IB errorEvent 回调（同步、绝不能抛异常——eventkit 会吞掉监听器异常）"""
        try:
            code = int(errorCode) if errorCode is not None else None
            sym = getattr(contract, 'symbol', None) if contract is not None else None
            tag = f"[{sym}] " if sym else ""
            if code in (1100, 1101):
                # TWS ↔ IBKR 连通性丢失
                self.logger.critical(
                    f"🚨 TWS↔IBKR 连通性丢失 (Error {code}, reqId={reqId}): {errorString} "
                    f"——bar 流可能中断，看门狗将自动巡检修复，请留意网络/TWS 状态"
                )
                self._request_recovery(f"TWS 断连 (Error {code})")
            elif code == 1102:
                # 连通性恢复（data maintained）——但 keepUpToDate 流需主动重建
                self.logger.warning(
                    f"🔁 {tag}TWS↔IBKR 连通性已恢复 (Error 1102): {errorString} ——立即触发数据流重订阅修复"
                )
                self._request_recovery("TWS 连通性恢复 (Error 1102)")
            elif code in (10182, 10183):
                # 历史行情 live updates 失效 / 历史请求失败
                self.logger.warning(
                    f"🚨 {tag}K线流请求失效 (Error {code}, reqId={reqId}): {errorString} ——触发重订阅修复"
                )
                self._request_recovery(f"K线流失效 (Error {code})")
            # 其余错误码不处理（避免噪音）
        except Exception as e:
            # 监听器内任何异常都不允许影响 IB 事件循环
            self.logger.error(f"⚠️ TWS 错误事件处理异常: {e}")

    def _on_tws_disconnected(self, *args) -> None:
        """IB API 与 TWS 的 API 连接断开（区别于 TWS↔IBKR 的 1100）"""
        try:
            self.logger.critical(
                "🚨 IB API ↔ TWS 连接断开 ——订单/取数/行情全部不可用，"
                "等待 TWS 重连后看门狗将自动修复数据流"
            )
        except Exception:
            pass

    def _on_tws_connected(self, *args) -> None:
        """IB API 与 TWS 的 API 连接建立/重连"""
        try:
            if self._subscribed_symbols:
                self.logger.info("🔁 IB API ↔ TWS 连接已(重新)建立——触发数据流重订阅修复")
                self._request_recovery("IB API 重连 (connectedEvent)")
        except Exception:
            pass

    def _request_recovery(self, reason: str) -> None:
        """从事件回调（同步上下文）发起一次流修复（带并发保护）"""
        try:
            asyncio.ensure_future(self._guarded_recovery(reason))
        except RuntimeError as e:
            self.logger.error(f"❌ 无法调度流修复任务（事件循环不可用？）: {e}")

    async def _guarded_recovery(self, reason: str) -> None:
        if self._recovery_in_flight:
            return
        self._recovery_in_flight = True
        try:
            await self.recover_feed(reason)
        except Exception as e:
            self.logger.error(f"❌ 数据流修复异常: {e}", exc_info=True)
        finally:
            self._recovery_in_flight = False

    # ------------------------------------------------------------------
    # 断流修复：全量重订阅
    # ------------------------------------------------------------------
    async def recover_feed(self, reason: str = "心跳超时") -> bool:
        """重新订阅全部 1 分钟数据流（断流自愈入口）

        安全保证：
        - 回放去重：_processed_until 按 bar 时间戳去重，不会重复写 CSV；
        - 策略门控：断流前已错过的历史 bar 只补写数据，重放不触发策略
          （防止数百根过期 bar 用旧价格冲击策略状态）；
        - 边界取 max：多次断流时，门控只收紧、不放松。
        """
        if not self._subscribed_symbols:
            return False

        # 策略门控边界 = 本次恢复时刻的整分钟（覆盖恢复所在分钟；
        # bar dt 按开始/结束语义不同都能落在「断流前→拒策略 / 恢复后→放行」的正确一侧）
        boundary = datetime.now(_EST).replace(second=0, microsecond=0)
        for sym in self._subscribed_symbols:
            old = self._strategy_resume_boundary.get(sym)
            if old is None or boundary > old:
                self._strategy_resume_boundary[sym] = boundary

        self.logger.critical(
            f"🔁 [流修复] 触发: {reason} ——开始重订阅 {len(self._subscribed_symbols)} 路 1分钟数据流..."
        )

        async def _one(stock: StockInfo) -> Tuple[str, bool, str]:
            try:
                ok, detail = await self._renew_symbol(stock)
                return stock.code, ok, detail
            except Exception as e:
                return stock.code, False, repr(e)

        results = await asyncio.gather(*(_one(s) for s in self._subscribed_symbols.values()))

        ok_n = sum(1 for _, ok, _ in results if ok)
        for code, ok, detail in results:
            if ok:
                self.logger.info(f"✅ 重订阅成功 {code}: {detail}")
            else:
                self.logger.error(f"❌ 重订阅失败 {code}: {detail}（下一轮将自动重试）")

        all_ok = ok_n == len(results)
        self.logger.critical(
            f"🔁 [流修复] 完成: {ok_n}/{len(results)} 路恢复"
            + ("" if all_ok else " ——未恢复标的将持续自动重试，请留意 TWS/网络状态！")
        )
        return all_ok

    async def _renew_symbol(self, stock: StockInfo) -> Tuple[bool, str]:
        """重建单标的的 1 分钟 keepUpToDate 订阅（新 reqId、新 BarList、新 handler）"""
        symbol = stock.code
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                return False, "合约确认失败"
            contract = qualified[0]

            bars = await self.ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr='2 D',
                barSizeSetting='1 min', whatToShow='TRADES',
                useRTH=False, formatDate=1, keepUpToDate=True,
                timeout=30
            )
            if not bars:
                return False, "TWS 返回空数据（请求可能仍被拒/连接未就绪）"

            csv_path = self._init_csv(symbol)
            self._create_bar_handler(symbol, bars, csv_path)
            return True, f"重放 {len(bars)} 根历史 bar（已按时间戳去重/门控）"
        except Exception as e:
            return False, f"{e!r}"

    # ------------------------------------------------------------------
    # 心跳看门狗
    # ------------------------------------------------------------------
    def start_feed_watchdog(self, is_quiet=None) -> Optional[asyncio.Task]:
        """启动 1 分钟流心跳看门狗（后台任务）

        交易窗口内连续 FEED_STALE_SECONDS 秒无任何 bar 事件 →
        CRITICAL 告警 + 检查 API 连接 + 自动全量重订阅。
        is_quiet() 返回 True 时暂停巡检（如强制平仓窗口/策略已停用）。
        """
        if self._watchdog_task and not self._watchdog_task.done():
            return self._watchdog_task
        self._watchdog_quiet = is_quiet if is_quiet is not None else (lambda: False)
        self._watchdog_task = asyncio.ensure_future(self._feed_watchdog_loop())
        self.logger.info(
            f"🫀 流心跳看门狗已启动: 每 {FEED_CHECK_INTERVAL_SECONDS}s 巡检一次，"
            f"连续 {FEED_STALE_SECONDS // 60} 分钟无 bar 事件 → 自动诊断并重订阅"
        )
        return self._watchdog_task

    def stop_feed_watchdog(self) -> None:
        task, self._watchdog_task = self._watchdog_task, None
        if task and not task.done():
            task.cancel()

    async def _feed_watchdog_loop(self) -> None:
        stale_count = 0
        while True:
            try:
                await asyncio.sleep(FEED_CHECK_INTERVAL_SECONDS)

                if self._watchdog_quiet():
                    # 策略停用/强制平仓窗口：停止巡检
                    stale_count = 0
                    continue

                last = self._last_bar_event
                if last is None:
                    # 首根 bar 事件尚未到达（订阅刚建立/启动竞态），暂不告警
                    continue

                idle = (datetime.now() - last).total_seconds()
                if idle <= FEED_STALE_SECONDS:
                    if stale_count:
                        self.logger.info("✅ 流心跳恢复正常（看门狗已复位）")
                    stale_count = 0
                    continue

                # ---------- 判定断流 ----------
                stale_count += 1
                if stale_count == 1:
                    self.logger.critical(
                        f"🚨 [流心跳] 已 {idle:.0f}s 无任何 1分钟 bar 事件"
                        f"（最后事件 {last.strftime('%H:%M:%S')}）——疑似数据流断流，开始诊断修复..."
                    )
                else:
                    self.logger.error(
                        f"🚨 [流心跳] 连续第 {stale_count} 次巡检仍无 bar 事件"
                        f"（静默 {idle:.0f}s）——数据流仍未恢复，继续自动重试"
                    )

                connected: Optional[bool]
                try:
                    connected = bool(self.ib.isConnected())
                except Exception:
                    connected = None

                if connected is False:
                    self.logger.critical(
                        "🚨 [流心跳] IB API ↔ TWS 连接已断开——无法下单/取数/修流！"
                        "请立即检查 TWS 进程与网络（恢复后本看门狗会自动重订阅）"
                    )
                elif connected:
                    self.logger.info(
                        "[流心跳] IB API ↔ TWS 连接正常 ——判定为市场数据流单独失效，尝试自动重订阅..."
                    )
                else:
                    self.logger.warning(
                        "[流心跳] 连接状态未知 ——仍尝试自动重订阅（失败会留痕并下轮重试）"
                    )

                await self._guarded_recovery(
                    f"看门狗心跳超时（静默 {idle:.0f}s，连续第 {stale_count} 次）"
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 看门狗自身异常不允许杀死看门狗
                self.logger.error(f"❌ 看门狗巡检异常（继续运行）: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # CSV / bar 处理
    # ------------------------------------------------------------------
    def _init_csv(self, symbol: str) -> str:
        csv_path = self.save_dir / f"{symbol}.csv"
        if not csv_path.exists():
            df_init = pd.DataFrame(columns=REALTIME_COLUMNS)
            df_init.to_csv(csv_path, index=False)
            self.logger.debug(f"创建CSV文件: {csv_path.name}")
        return str(csv_path)

    def _mark_processed(self, symbol: str, dt_est: datetime) -> None:
        """推进该标的的已处理时间戳（单调不降）"""
        old = self._processed_until.get(symbol)
        if old is None or dt_est > old:
            self._processed_until[symbol] = dt_est

    def _create_bar_handler(self, symbol: str, bars, csv_path: str):
        """
        创建K线更新的回调处理器

        Args:
            symbol: 股票代码
            bars: BarList对象
            csv_path: CSV文件路径
        """
        now_est = datetime.now(_EST)
        today_str_est = now_est.strftime("%Y-%m-%d")

        if symbol not in self.daily_stats:
            self.daily_stats[symbol] = {
                "trade_date": None,
                "day_open": None,
                "day_max": None,
                "day_min": None,
                "day_change_pct_max": None,
                "day_change_pct_min": None,
            }

        state = {"last_written_idx": -1}

        def on_bar_update(bars, has_new_bar):
            try:
                # 全局心跳：任意 bar 事件到达 = 流还活着（看门狗依据）
                self._last_bar_event = datetime.now()

                while state["last_written_idx"] < len(bars) - 1:
                    state["last_written_idx"] += 1
                    bar = bars[state["last_written_idx"]]
                    dt = bar.date

                    # 统一转美东 aware（与策略侧 Bar.dt / 建仓时间同一时区基准）
                    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                        dt_est = dt.astimezone(_EST)
                    else:
                        try:
                            dt_est = _EST.localize(dt)
                        except Exception:
                            dt_est = dt

                    # ============ 回放去重（重订阅安全）============
                    # 重订阅会重放「2 D」全量历史：已按 bar 时间戳处理过的直接跳过，
                    # 避免重复写 CSV / 重复触发策略（仅当该标的已有已处理记录时才拦截）
                    processed = self._processed_until.get(symbol)
                    if processed is not None and dt_est <= processed:
                        continue

                    current_trade_date = dt_est.strftime("%Y-%m-%d")
                    if current_trade_date != today_str_est:
                        self._mark_processed(symbol, dt_est)
                        continue
                    if dt_est.hour < 9 or (dt_est.hour == 9 and dt_est.minute < 29):
                        self._mark_processed(symbol, dt_est)
                        continue

                    stats = self.daily_stats[symbol]
                    is_new_day = stats["trade_date"] != current_trade_date
                    if is_new_day:
                        stats["trade_date"] = current_trade_date
                        stats["day_open"] = bar.open
                        stats["day_max"] = bar.high
                        stats["day_min"] = bar.low
                    else:
                        stats["day_max"] = max(stats["day_max"], bar.high)
                        stats["day_min"] = min(stats["day_min"], bar.low)

                    day_open = stats["day_open"]
                    day_max = stats["day_max"]
                    day_min = stats["day_min"]

                    if day_open and day_open != 0:
                        day_change_pct = round(((bar.close - day_open) / day_open) * 100, 4)
                    else:
                        day_change_pct = 0.0

                    if is_new_day or stats.get("day_change_pct_max") is None:
                        stats["day_change_pct_max"] = day_change_pct
                        stats["day_change_pct_min"] = day_change_pct
                    else:
                        stats["day_change_pct_max"] = max(
                            stats["day_change_pct_max"], day_change_pct
                        )
                        stats["day_change_pct_min"] = min(
                            stats["day_change_pct_min"], day_change_pct
                        )

                    # 从 runner 拉展示字段（建仓后极值等，状态已下沉到策略）
                    # 注意：重放行也会走到这里——若该标的已平，get_display_state 无状态则返回空，安全
                    display = {
                        'day_max_since_entry': '',
                        'day_min_since_entry': '',
                        'day_max_since_entry_pct': '',
                        'day_min_since_entry_pct': '',
                    }
                    if self.runner is not None:
                        try:
                            d = self.runner.get_display_state(symbol)
                            if d:
                                display.update(d)
                        except Exception as e:
                            self.logger.debug(f"[{symbol}] 拉展示字段失败: {e}")

                    time_str = dt_est.strftime(DATETIME_FORMAT)
                    turnover = round(bar.close * bar.volume, 2)

                    row = {
                        "time": time_str,
                        "open": bar.open, "close": bar.close,
                        "high": bar.high, "low": bar.low,
                        "turnover": turnover, "volume": bar.volume,
                        "day_max": day_max, "day_min": day_min,
                        "day_change_pct": day_change_pct,
                        "day_change_pct_max": stats["day_change_pct_max"],
                        "day_change_pct_min": stats["day_change_pct_min"],
                        "day_max_since_entry": display.get('day_max_since_entry', ''),
                        "day_min_since_entry": display.get('day_min_since_entry', ''),
                        "day_max_since_entry_pct": display.get('day_max_since_entry_pct', ''),
                        "day_min_since_entry_pct": display.get('day_min_since_entry_pct', ''),
                    }
                    pd.DataFrame([row]).to_csv(
                        csv_path, mode="a", header=False, index=False
                    )
                    # 写成功后才标记已处理（写失败下轮事件可重试该 bar）
                    self._mark_processed(symbol, dt_est)

                    # ============ 触发策略（异步、异常隔离，不阻塞/不污染K线写入）============
                    # 断流门控：恢复后重放的「断流前历史 bar」只补写数据，不重放入策略——
                    # 防止数百根过期 bar 用断流期间的旧价格冲撞策略状态
                    # （2026-09-15：10:18 断流后若机械重放 10:19~15:50 共 300 余根 bar，
                    #   会基于过期行情批量触发/扭曲止盈判断）
                    if self.runner is not None:
                        boundary = self._strategy_resume_boundary.get(symbol)
                        if boundary is not None and dt_est < boundary:
                            if symbol not in self._replay_log_pending:
                                self._replay_log_pending.add(symbol)
                                self.logger.info(
                                    f"⏩ [{symbol}] bar {dt_est.strftime('%H:%M:%S')} 位于断流回放窗口"
                                    f"（门控边界 {boundary.strftime('%H:%M:%S')}）——数据已补写，跳过策略决策"
                                )
                        else:
                            self._replay_log_pending.discard(symbol)
                            try:
                                bar_obj = Bar(
                                    symbol=symbol,
                                    dt=dt_est,
                                    open=float(bar.open), high=float(bar.high),
                                    low=float(bar.low), close=float(bar.close),
                                    volume=int(bar.volume or 0),
                                )
                                asyncio.create_task(self._safe_on_bar(symbol, bar_obj))
                            except RuntimeError as e:
                                self.logger.error(f"❌ [{symbol}] 无法调度策略: {e}")

            except Exception as e:
                self.logger.error(f"❌ [{symbol}] 1分钟数据处理异常: {e}")

        bars.updateEvent += on_bar_update

    async def _safe_on_bar(self, symbol: str, bar: Bar):
        """策略执行的异常隔离包装"""
        try:
            await self.runner.on_bar(bar)
        except Exception as e:
            self.logger.error(f"❌ [{symbol}] 策略执行异常: {e}", exc_info=True)

    async def subscribe(self, stock: StockInfo) -> bool:
        symbol = stock.code
        try:
            self.logger.info(f"📡 订阅 {symbol} 1分钟数据...")
            contract = Stock(symbol, 'SMART', 'USD')
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                self.logger.error(f"❌ {symbol}: 合约确认失败")
                return False
            contract = qualified[0]

            bars = self.ib.reqHistoricalData(
                contract, endDateTime='', durationStr='2 D',
                barSizeSetting='1 min', whatToShow='TRADES',
                useRTH=False, formatDate=1, keepUpToDate=True
            )
            csv_path = self._init_csv(symbol)
            self._create_bar_handler(symbol, bars, csv_path)
            self._subscribed_symbols[symbol] = stock
            self.logger.info(f"✅ {symbol}: 1分钟数据订阅成功")
            return True
        except Exception as e:
            self.logger.error(f"❌ {symbol}: 订阅失败: {e}")
            return False

    async def subscribe_all(self, stocks: List[StockInfo]) -> int:
        """并发订阅一批股票的1分钟数据

        开盘窗口 TWS 拥堵，串行 15 次"合约确认+历史请求"可能拖出第一分钟，
        改为并发发起（同一 IB 实例内 reqId 独立分配，各 BarList 事件互不干扰）。
        """
        self.logger.info(f"\n📡 开始并发起发 {len(stocks)} 只股票的1分钟数据订阅...")
        if not stocks:
            return 0
        results = await asyncio.gather(
            *(self.subscribe(stock) for stock in stocks), return_exceptions=True
        )
        success_count = 0
        for stock, r in zip(stocks, results):
            if r is True:
                success_count += 1
            elif isinstance(r, BaseException):
                self.logger.error(f"❌ {stock.code}: 并发订阅异常: {r!r}")
            else:
                self.logger.error(f"❌ {stock.code}: 订阅失败")
        self.logger.info(f"📡 1分钟数据订阅完成: {success_count}/{len(stocks)} 成功")
        return success_count
