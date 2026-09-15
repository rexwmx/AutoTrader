# -*- coding: utf-8 -*-
"""
实时数据模块

职责已简化：
- 只做1分钟K线的接收、处理、写CSV
- 每根 bar 触发一次 StrategyRunner.on_bar（策略决策和执行交给 runner）
- 建仓后极值等展示字段从 runner（→策略）查询
"""
import asyncio
import pandas as pd
import pytz
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from ib_async import IB, Stock

from models import StockInfo
from constants import REALTIME_COLUMNS, DATETIME_FORMAT
from logger import get_logger
from strategy.base import Bar


class RealtimeDataRecorder:
    def __init__(self, ib: IB, save_dir: Path, runner=None):
        self.ib = ib
        self.save_dir = save_dir
        self.runner = runner
        self.daily_stats: Dict[str, dict] = {}
        self.logger = get_logger()

        self.save_dir.mkdir(parents=True, exist_ok=True)

    def _init_csv(self, symbol: str) -> str:
        csv_path = self.save_dir / f"{symbol}.csv"
        if not csv_path.exists():
            df_init = pd.DataFrame(columns=REALTIME_COLUMNS)
            df_init.to_csv(csv_path, index=False)
            self.logger.debug(f"创建CSV文件: {csv_path.name}")
        return str(csv_path)

    def _create_bar_handler(self, symbol: str, bars, csv_path: str):
        """
        创建K线更新的回调处理器

        Args:
            symbol: 股票代码
            bars: BarList对象
            csv_path: CSV文件路径
        """
        est = pytz.timezone('US/Eastern')
        now_est = datetime.now(est)
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
                while state["last_written_idx"] < len(bars) - 1:
                    state["last_written_idx"] += 1
                    bar = bars[state["last_written_idx"]]
                    dt = bar.date

                    # 统一转美东 aware（与策略侧 Bar.dt / 建仓时间同一时区基准）
                    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                        dt_est = dt.astimezone(est)
                    else:
                        try:
                            dt_est = est.localize(dt)
                        except Exception:
                            dt_est = dt

                    current_trade_date = dt_est.strftime("%Y-%m-%d")
                    if current_trade_date != today_str_est:
                        continue
                    if dt_est.hour < 9 or (dt_est.hour == 9 and dt_est.minute < 29):
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

                    # 触发策略（异步、异常隔离，不阻塞/不污染K线写入）
                    if self.runner is not None:
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
