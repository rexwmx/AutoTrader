# -*- coding: utf-8 -*-
"""
实时数据模块
负责订阅对冲成功股票的1分钟K线数据并持续写入CSV
支持历史K线回放补全，追踪建仓后的价格极值用于平仓判断
建仓后极值字段仅在建仓成功后的下一分钟开始才有数据
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


class RealtimeDataRecorder:
    def __init__(self, ib: IB, save_dir: Path, close_manager=None):
        self.ib = ib
        self.save_dir = save_dir
        self.close_manager = close_manager
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

    def _create_bar_handler(self, symbol: str, bars, csv_path: str, entry_time: datetime):
        """
        创建K线更新的回调处理器

        Args:
            symbol: 股票代码
            bars: BarList对象
            csv_path: CSV文件路径
            entry_time: 建仓完成时间（美东时间），只有此时间之后的K线才写入建仓后极值
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
                # 建仓后的状态追踪
                "sell_entry_price": None,
                "buy_entry_price": None,
                "min_since_entry": None,
                "max_since_entry": None
            }

        state = {"last_written_idx": -1}

        def on_bar_update(bars, has_new_bar):
            try:
                while state["last_written_idx"] < len(bars) - 1:
                    state["last_written_idx"] += 1
                    bar = bars[state["last_written_idx"]]
                    dt = bar.date

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

                    # ==================== 判断是否在建仓之后 ====================
                    # 只有K线时间严格大于建仓完成时间，才开始计算建仓后极值
                    is_after_entry = dt_est > entry_time

                    # ==================== 计算建仓后极值（仅在建仓后） ====================
                    day_min_since_entry = ''
                    day_max_since_entry = ''
                    day_min_since_entry_pct = ''
                    day_max_since_entry_pct = ''

                    if is_after_entry:
                        # 获取 entry_price（从持仓中读取）
                        if self.close_manager:
                            if stats["sell_entry_price"] is None:
                                pos1 = {p.contract.symbol: p for p in
                                        self.close_manager.ib1.positions(self.close_manager.account1)}
                                if symbol in pos1 and pos1[symbol].position < 0:
                                    stats["sell_entry_price"] = float(pos1[symbol].avgCost)

                            if stats["buy_entry_price"] is None:
                                pos2 = {p.contract.symbol: p for p in
                                        self.close_manager.ib2.positions(self.close_manager.account2)}
                                if symbol in pos2 and pos2[symbol].position > 0:
                                    stats["buy_entry_price"] = float(pos2[symbol].avgCost)

                        # 更新建仓后的极值
                        if stats["min_since_entry"] is None:
                            stats["min_since_entry"] = bar.close
                            stats["max_since_entry"] = bar.close
                        else:
                            stats["min_since_entry"] = min(stats["min_since_entry"], bar.close)
                            stats["max_since_entry"] = max(stats["max_since_entry"], bar.close)

                        # 计算 Sell 相关（做空：关注历史最低价相对建仓价的跌幅）
                        if stats["sell_entry_price"] is not None and stats["sell_entry_price"] > 0:
                            day_min_since_entry = round(stats["min_since_entry"], 4)
                            day_min_since_entry_pct = round(
                                (stats["min_since_entry"] - stats["sell_entry_price"]) / stats["sell_entry_price"] * 100, 4
                            )

                        # 计算 Buy 相关（做多：关注历史最高价相对建仓价的涨幅）
                        if stats["buy_entry_price"] is not None and stats["buy_entry_price"] > 0:
                            day_max_since_entry = round(stats["max_since_entry"], 4)
                            day_max_since_entry_pct = round(
                                (stats["max_since_entry"] - stats["buy_entry_price"]) / stats["buy_entry_price"] * 100, 4
                            )

                    # ==================== 原有 CSV 记录逻辑 ====================
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
                        stats["day_change_pct_max"] = max(stats["day_change_pct_max"], day_change_pct)
                        stats["day_change_pct_min"] = min(stats["day_change_pct_min"], day_change_pct)

                    day_change_pct_max = stats["day_change_pct_max"]
                    day_change_pct_min = stats["day_change_pct_min"]

                    time_str = dt_est.strftime(DATETIME_FORMAT)
                    turnover = round(bar.close * bar.volume, 2)

                    row = {
                        "time": time_str, "open": bar.open, "close": bar.close,
                        "high": bar.high, "low": bar.low, "turnover": turnover,
                        "volume": bar.volume, "day_max": day_max, "day_min": day_min,
                        "day_change_pct": day_change_pct,
                        "day_change_pct_max": day_change_pct_max,
                        "day_change_pct_min": day_change_pct_min,
                        "day_max_since_entry": day_max_since_entry,
                        "day_min_since_entry": day_min_since_entry,
                        "day_max_since_entry_pct": day_max_since_entry_pct,
                        "day_min_since_entry_pct": day_min_since_entry_pct
                    }

                    row_df = pd.DataFrame([row])
                    row_df.to_csv(csv_path, mode="a", header=False, index=False)

                    # ==================== 触发平仓检查 ====================
                    # check_runtime_conditions 现在是 async（内部使用权威持仓快照），
                    # 从K线回调里以任务方式调度；异常隔离，不破坏K线写入
                    if self.close_manager and is_after_entry:
                        try:
                            asyncio.create_task(self.close_manager.check_runtime_conditions(
                                symbol,
                                bar.close,
                                stats["min_since_entry"] if stats["min_since_entry"] else bar.close,
                                stats["max_since_entry"] if stats["max_since_entry"] else bar.close
                            ))
                        except RuntimeError as e:
                            self.logger.error(f"❌ [{symbol}] 无法调度运行时平仓检查: {e}")

            except Exception as e:
                self.logger.error(f"❌ [{symbol}] 1分钟数据处理异常: {e}")

        bars.updateEvent += on_bar_update

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

            # ==================== 记录建仓完成时间 ====================
            # 订阅时刻即为建仓完成后的时刻，历史K线中此时间之前的不写入建仓后极值
            est = pytz.timezone('US/Eastern')
            entry_time = datetime.now(est)

            bars = self.ib.reqHistoricalData(
                contract, endDateTime='', durationStr='2 D',
                barSizeSetting='1 min', whatToShow='TRADES',
                useRTH=False, formatDate=1, keepUpToDate=True
            )

            csv_path = self._init_csv(symbol)
            self._create_bar_handler(symbol, bars, csv_path, entry_time)
            self.logger.info(f"✅ {symbol}: 1分钟数据订阅成功 (建仓后极值从 {entry_time.strftime('%H:%M:%S')} 之后开始)")
            return True
        except Exception as e:
            self.logger.error(f"❌ {symbol}: 订阅失败: {e}")
            return False

    async def subscribe_all(self, stocks: List[StockInfo]) -> int:
        self.logger.info(f"\n📡 开始订阅 {len(stocks)} 只股票的1分钟数据...")
        success_count = 0
        for stock in stocks:
            if await self.subscribe(stock):
                success_count += 1
        self.logger.info(f"📡 1分钟数据订阅完成: {success_count}/{len(stocks)} 成功")
        return success_count