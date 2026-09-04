# -*- coding: utf-8 -*-
"""
平仓逻辑模块 (独立平仓版 + 分段式动态平仓线)
Sell持仓和Buy持仓独立判断、独立平仓，互不干扰
根据峰值涨幅/跌幅动态调整平仓回撤容忍度
"""
import asyncio
import datetime
import pandas as pd
from ib_async import IB
from order import submit_buy_order, submit_sell_order, get_fill_price, get_filled_volume
from monitor import wait_for_trade_completion
from csv_writer import update_close_data_in_csv
from logger import get_logger
from util import format_datetime

ESTIMATED_COMMISSION_RATE = 0.0035
CLOSE_RETRY_DELAY = 10
CLOSE_MAX_RETRIES = 1


def get_close_pct(peak_pct: float) -> float:
    """
    根据峰值涨幅/跌幅百分比，查表得到对应的平仓线百分比。

    峰值越大（盈利越多），允许的回撤也越大，平仓线越宽松。

    Args:
        peak_pct: 峰值涨幅/跌幅百分比（正数，如 5.0 表示 5%）

    Returns:
        float: 平仓线百分比（相对于建仓价的盈利保留比例）
               如果 peak_pct < 2 则返回 -1（不触发平仓）

    对应关系表：
        PEAK_PCT        CLOSE_PCT
        2%-3%           1%
        3%-4%           2%
        4%-10%          PEAK_PCT * 50%
        10%-15%         PEAK_PCT * 45%
        15%-20%         PEAK_PCT * 40%
        20%-25%         PEAK_PCT * 35%
        25%-30%         PEAK_PCT * 30%
        30%-60%         PEAK_PCT * 25%
        60%-100%        PEAK_PCT * 20%
        >100%           PEAK_PCT * 10%
    """
    if peak_pct < 1:
        return -1.0  # 峰值太小，不触发平仓
    elif peak_pct < 2:
        return 0.65
    elif peak_pct < 3:
        return 1.0
    elif peak_pct < 4:
        return 1.5
    elif peak_pct < 10:
        return peak_pct * 0.50
    elif peak_pct < 15:
        return peak_pct * 0.45
    elif peak_pct < 20:
        return peak_pct * 0.40
    elif peak_pct < 25:
        return peak_pct * 0.35
    elif peak_pct < 30:
        return peak_pct * 0.30
    elif peak_pct < 60:
        return peak_pct * 0.25
    elif peak_pct < 100:
        return peak_pct * 0.20
    else:
        return peak_pct * 0.10


class CloseManager:
    def __init__(self, ib1: IB, ib2: IB, account1: str, account2: str,
                 sell_csv_path, buy_csv_path):
        self.ib1 = ib1
        self.ib2 = ib2
        self.account1 = account1
        self.account2 = account2
        self.sell_csv_path = sell_csv_path
        self.buy_csv_path = buy_csv_path
        self.logger = get_logger()

        self.closed_symbols_acc1 = set()
        self.closed_symbols_acc2 = set()

        self.is_runtime_close_active = False
        self.runtime_close_start_time = None

    def start_runtime_close(self, delay_minutes: int = 2):
        self.runtime_close_start_time = datetime.datetime.now() + datetime.timedelta(minutes=delay_minutes)
        self.logger.info(f"⏱️ 运行时平仓监控将在 {self.runtime_close_start_time.strftime('%H:%M:%S')} 激活")

    def _get_entry_datetime(self, symbol: str, action: str, csv_path) -> str:
        try:
            df = pd.read_csv(csv_path)
            mask = (df['code'] == symbol) & (df['action'] == action)
            if mask.any():
                return str(df[mask].iloc[-1]['datetime'])
        except Exception:
            pass
        return "未知时间"

    def check_runtime_conditions(self, symbol: str, current_close: float,
                                 min_close_since_entry: float,
                                 max_close_since_entry: float):
        """
        独立检查账户1和账户2的平仓条件（分段式动态平仓线）
        """
        if not self.is_runtime_close_active:
            if self.runtime_close_start_time and datetime.datetime.now() >= self.runtime_close_start_time:
                self.is_runtime_close_active = True
                self.logger.info("🟢 运行时平仓监控已激活")
            else:
                return

        pos1 = {p.contract.symbol: p for p in self.ib1.positions(self.account1)}
        pos2 = {p.contract.symbol: p for p in self.ib2.positions(self.account2)}

        # ==================== 账户1 (做空) 平仓条件 ====================
        if symbol in pos1 and pos1[symbol].position < 0:
            if symbol not in self.closed_symbols_acc1:
                entry_price = float(pos1[symbol].avgCost)
                if entry_price > 0 and min_close_since_entry > 0:
                    # cond1: 当前处于下跌状态（做空盈利方向）
                    cond1 = current_close < entry_price

                    if cond1:
                        # 计算实际峰值跌幅（正数）
                        peak_pct = (entry_price - min_close_since_entry) / entry_price * 100

                        # 查表得到平仓线
                        close_pct = get_close_pct(peak_pct)

                        if close_pct >= 0:
                            # cond2: 峰值跌幅 >= 2%（已由 get_close_pct 保证）
                            cond2 = min_close_since_entry < entry_price * (1 - peak_pct / 100)

                            # cond3: 当前价从最低点反弹，回升到平仓线以上
                            # 平仓线 = entry_price * (1 - peak_pct + close_pct)
                            close_line = entry_price * (1 - peak_pct / 100 + close_pct / 100)
                            cond3 = current_close >= close_line

                            if cond2 and cond3:
                                rebound_pct = (current_close - min_close_since_entry) / min_close_since_entry * 100
                                entry_dt = self._get_entry_datetime(symbol, 'sell', self.sell_csv_path)
                                reason = (
                                    f"账户1(做空)触发 | 建仓: {entry_dt} | "
                                    f"成本{entry_price:.2f}, "
                                    f"最低{min_close_since_entry:.2f}(跌{peak_pct:.1f}%), "
                                    f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
                                    f"现价{current_close:.2f}(反弹{rebound_pct:.1f}%)"
                                )
                                self.logger.info(f"⚡ 触发独立平仓: {symbol} | {reason}")
                                self.closed_symbols_acc1.add(symbol)
                                vol = abs(int(pos1[symbol].position))
                                asyncio.create_task(self._execute_close_with_retry(
                                    self.ib1, symbol, 'buy', vol, entry_price, 'sell', self.sell_csv_path
                                ))

        # ==================== 账户2 (做多) 平仓条件 ====================
        if symbol in pos2 and pos2[symbol].position > 0:
            if symbol not in self.closed_symbols_acc2:
                entry_price = float(pos2[symbol].avgCost)
                if entry_price > 0 and max_close_since_entry > 0:
                    # cond1: 当前处于上涨状态（做多盈利方向）
                    cond1 = current_close > entry_price

                    if cond1:
                        # 计算实际峰值涨幅（正数）
                        peak_pct = (max_close_since_entry - entry_price) / entry_price * 100

                        # 查表得到平仓线
                        close_pct = get_close_pct(peak_pct)

                        if close_pct >= 0:
                            # cond2: 峰值涨幅 >= 2%（已由 get_close_pct 保证）
                            cond2 = max_close_since_entry > entry_price * (1 + peak_pct / 100)

                            # cond3: 当前价从最高点回落，跌到平仓线以下
                            # 平仓线 = entry_price * (1 + close_pct)
                            close_line = entry_price * (1 + close_pct / 100)
                            cond3 = current_close <= close_line

                            if cond2 and cond3:
                                pullback_pct = (max_close_since_entry - current_close) / max_close_since_entry * 100
                                entry_dt = self._get_entry_datetime(symbol, 'buy', self.buy_csv_path)
                                reason = (
                                    f"账户2(做多)触发 | 建仓: {entry_dt} | "
                                    f"成本{entry_price:.2f}, "
                                    f"最高{max_close_since_entry:.2f}(涨{peak_pct:.1f}%), "
                                    f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
                                    f"现价{current_close:.2f}(回撤{pullback_pct:.1f}%)"
                                )
                                self.logger.info(f"⚡ 触发独立平仓: {symbol} | {reason}")
                                self.closed_symbols_acc2.add(symbol)
                                vol = int(pos2[symbol].position)
                                asyncio.create_task(self._execute_close_with_retry(
                                    self.ib2, symbol, 'sell', vol, entry_price, 'buy', self.buy_csv_path
                                ))

    async def _execute_close_with_retry(self, ib: IB, symbol: str, close_action: str,
                                        volume: int, open_price: float, open_action: str, csv_path):
        """带重试的平仓执行"""
        for attempt in range(CLOSE_MAX_RETRIES + 1):
            if attempt > 0:
                self.logger.info(f"🔄 {symbol}: 第 {attempt} 次重试平仓...")
                await asyncio.sleep(CLOSE_RETRY_DELAY)

            success = await self._execute_close(ib, symbol, close_action, volume, open_price, open_action, csv_path)
            if success:
                return

        self.logger.error(f"❌ {symbol}: 平仓重试 {CLOSE_MAX_RETRIES} 次后仍失败")

    async def _execute_close(self, ib: IB, symbol: str, close_action: str,
                             volume: int, open_price: float, open_action: str, csv_path) -> bool:
        """执行单次平仓，返回是否成功"""
        self.logger.info(f"🔄 {symbol}: 开始执行 {close_action} 平仓 {volume}股...")

        trade = await submit_buy_order(ib, symbol, volume) if close_action == 'buy' else await submit_sell_order(ib,
                                                                                                                 symbol,
                                                                                                                 volume)
        if not trade:
            return False

        status = await wait_for_trade_completion(trade, timeout_seconds=120)

        if status != 'Filled':
            self.logger.error(f"❌ {symbol}: 平仓订单未成交 (状态: {status})")
            if status == 'Timeout':
                if trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
                    await ib.cancelOrder(trade.order)
            return False

        close_price = get_fill_price(trade)
        close_vol = get_filled_volume(trade)
        close_fund = close_price * close_vol

        open_fund = open_price * close_vol
        gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)

        commission = 0.0
        try:
            comm = trade.commission()
            if comm is not None and comm != 1.7976931348623157e+308:
                commission = comm
            else:
                commission = (volume + close_vol) * ESTIMATED_COMMISSION_RATE
        except Exception:
            commission = (volume + close_vol) * ESTIMATED_COMMISSION_RATE

        profit = gross_profit - commission

        close_data = {
            'close_datetime': format_datetime(datetime.datetime.now()),
            'close_price': round(close_price, 4),
            'close_vol': close_vol,
            'close_fund': round(close_fund, 2),
            'gross_profit': round(gross_profit, 2),
            'profit': round(profit, 2)
        }

        if update_close_data_in_csv(csv_path, symbol, open_action, close_data):
            self.logger.info(f"✅ {symbol}: 平仓完成并更新CSV | 净利: ${profit:.2f}")
        else:
            self.logger.warning(f"⚠️ {symbol}: 平仓完成但更新CSV失败")
        return True

    async def force_close_all(self):
        """收市前强制平仓：各自清理各自的持仓（逻辑不变）"""
        self.logger.info("\n" + "=" * 50)
        self.logger.info("🚨 强制平仓：清空所有持仓（独立执行）")
        self.logger.info("=" * 50)

        pos1 = {p.contract.symbol: p for p in self.ib1.positions(self.account1)}
        pos2 = {p.contract.symbol: p for p in self.ib2.positions(self.account2)}

        tasks = []

        for symbol, pos in pos1.items():
            if pos.position < 0 and symbol not in self.closed_symbols_acc1:
                vol = abs(int(pos.position))
                open_price = float(pos.avgCost)
                self.logger.info(f"🚨 强制平仓 账户1(做空): {symbol} {vol}股")
                self.closed_symbols_acc1.add(symbol)
                tasks.append(self._execute_close_with_retry(
                    self.ib1, symbol, 'buy', vol, open_price, 'sell', self.sell_csv_path
                ))

        for symbol, pos in pos2.items():
            if pos.position > 0 and symbol not in self.closed_symbols_acc2:
                vol = int(pos.position)
                open_price = float(pos.avgCost)
                self.logger.info(f"🚨 强制平仓 账户2(做多): {symbol} {vol}股")
                self.closed_symbols_acc2.add(symbol)
                tasks.append(self._execute_close_with_retry(
                    self.ib2, symbol, 'sell', vol, open_price, 'buy', self.buy_csv_path
                ))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            self.logger.info("✅ 无持仓需要强制平仓")

    async def force_close_until_flat(self, timeout_minutes: int = 10):
        """
        循环强制平仓，直到所有持仓被清空或超时。
        关键修复：使用 reqPositionsAsync 的返回值判断持仓，
        而不是依赖可能被清空的 _positions 缓存。
        """
        import time as time_module
        start_time = time_module.time()
        timeout_seconds = timeout_minutes * 60
        attempt = 0

        while time_module.time() - start_time < timeout_seconds:
            attempt += 1

            # 使用 reqPositionsAsync 获取最新持仓
            try:
                positions1 = await self.ib1.reqPositionsAsync()
                positions2 = await self.ib2.reqPositionsAsync()
            except Exception as e:
                self.logger.warning(f"⚠️ 获取持仓异常: {e}")
                await asyncio.sleep(5)
                continue

            # 过滤出有持仓的股票
            pos1 = {p.contract.symbol: p for p in positions1
                    if p.account == self.account1 and p.position != 0}
            pos2 = {p.contract.symbol: p for p in positions2
                    if p.account == self.account2 and p.position != 0}

            if not pos1 and not pos2:
                self.logger.info("✅ 所有持仓已完全平仓")
                return True

            self.logger.info(
                f"⚠️ 第{attempt}次检查: 仍有未平仓 "
                f"(账户1={len(pos1)}只, 账户2={len(pos2)}只)，"
                f"重新提交平仓订单..."
            )

            # 重置已平仓标记，允许重新平仓
            self.closed_symbols_acc1.clear()
            self.closed_symbols_acc2.clear()

            # 直接使用已获取的持仓数据提交平仓订单
            await self._force_close_positions(pos1, pos2)

            # 等待一段时间再检查
            await asyncio.sleep(240)

        # 超时
        self.logger.warning(
            f"⚠️ 平仓超时（{timeout_minutes}分钟），"
            f"可能仍有未平仓持仓"
        )
        return False

    async def _force_close_positions(self, pos1: dict, pos2: dict):
        """
        根据已获取的持仓数据提交平仓订单
        不再依赖 self.ib.positions() 的缓存
        """
        self.logger.info("\n" + "=" * 50)
        self.logger.info("🚨 强制平仓：清空所有持仓（独立执行）")
        self.logger.info("=" * 50)

        tasks = []

        # 账户1：sell持仓 → buy平仓
        for symbol, pos in pos1.items():
            if pos.position < 0 and symbol not in self.closed_symbols_acc1:
                vol = abs(int(pos.position))
                open_price = float(pos.avgCost)
                self.logger.info(f"🚨 强制平仓 账户1(做空): {symbol} {vol}股")
                self.closed_symbols_acc1.add(symbol)
                tasks.append(self._execute_close_with_retry(
                    self.ib1, symbol, 'buy', vol, open_price, 'sell', self.sell_csv_path
                ))

        # 账户2：buy持仓 → sell平仓
        for symbol, pos in pos2.items():
            if pos.position > 0 and symbol not in self.closed_symbols_acc2:
                vol = int(pos.position)
                open_price = float(pos.avgCost)
                self.logger.info(f"🚨 强制平仓 账户2(做多): {symbol} {vol}股")
                self.closed_symbols_acc2.add(symbol)
                tasks.append(self._execute_close_with_retry(
                    self.ib2, symbol, 'sell', vol, open_price, 'buy', self.buy_csv_path
                ))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            self.logger.info("✅ 无持仓需要强制平仓")

    async def reconcile_csv_with_fills(self):
        """
        退出前 CSV 补写：从 TWS 的 fills 记录中获取实际成交数据，
        补写 CSV 中缺失的平仓记录。
        解决"假 Cancelled"导致 CSV 未更新的问题。
        """
        self.logger.info("\n" + "=" * 50)
        self.logger.info("📋 退出前 CSV 补写：对比 TWS 成交记录")
        self.logger.info("=" * 50)

        # 等待几秒让 TWS 完成最后的成交回报
        await asyncio.sleep(5)

        # 刷新持仓，确认是否真的全部平仓
        await self.ib1.reqPositionsAsync()
        await self.ib2.reqPositionsAsync()
        await asyncio.sleep(2)

        remaining1 = {p.contract.symbol: p for p in self.ib1.positions(self.account1) if p.position != 0}
        remaining2 = {p.contract.symbol: p for p in self.ib2.positions(self.account2) if p.position != 0}

        if remaining1 or remaining2:
            self.logger.warning(
                f"⚠️ 仍有未平仓持仓: 账户1={len(remaining1)}只, 账户2={len(remaining2)}只"
            )
        else:
            self.logger.info("✅ TWS 确认：所有持仓已平仓")

        # 从 TWS 获取今日所有成交记录
        try:
            fills1 = self.ib1.fills()
            fills2 = self.ib2.fills()
        except Exception as e:
            self.logger.error(f"❌ 获取 fills 失败: {e}")
            return

        # 补写 sell.csv（账户1的平仓 = buy 操作）
        self._backfill_csv(fills1, 'BUY', 'sell', self.sell_csv_path)
        # 补写 buy.csv（账户2的平仓 = sell 操作）
        self._backfill_csv(fills2, 'SELL', 'buy', self.buy_csv_path)

    def _backfill_csv(self, fills, close_side: str, open_action: str, csv_path):
        """
        从 fills 中找到平仓成交，补写到 CSV

        Args:
            fills: TWS fills 列表
            close_side: 平仓方向 ('BUY' 表示做空平仓, 'SELL' 表示做多平仓)
            open_action: 开仓方向 ('sell' 或 'buy')
            csv_path: CSV 文件路径
        """
        import pandas as pd

        try:
            df = pd.read_csv(csv_path)
        except Exception:
            return

        # 找出 CSV 中未平仓的记录
        is_empty = (
            df['close_datetime'].isna() |
            (df['close_datetime'].astype(str).str.strip() == '') |
            (df['close_datetime'].astype(str).str.lower() == 'nan')
        )
        unclosed = df[(df['action'] == open_action) & is_empty]

        if unclosed.empty:
            return

        # 从 fills 中按股票分组，找到平仓方向的成交
        close_fills_by_symbol = {}
        for f in fills:
            if f.execution.side == close_side:
                sym = f.contract.symbol
                if sym not in close_fills_by_symbol:
                    close_fills_by_symbol[sym] = []
                close_fills_by_symbol[sym].append(f)

        patched_count = 0
        for idx, row in unclosed.iterrows():
            symbol = row['code']
            if symbol not in close_fills_by_symbol:
                continue

            # 汇总该股票所有平仓成交
            sym_fills = close_fills_by_symbol[symbol]
            total_vol = sum(f.execution.shares for f in sym_fills)
            if total_vol == 0:
                continue
            avg_price = sum(f.execution.shares * f.execution.price for f in sym_fills) / total_vol

            # 获取开仓数据
            entry_price = float(row['entry_price'])
            open_vol = int(row['vol'])
            open_fund = entry_price * open_vol

            close_fund = avg_price * total_vol
            gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)
            commission = (open_vol + total_vol) * ESTIMATED_COMMISSION_RATE
            profit = gross_profit - commission

            # 使用最后一笔成交的时间
            last_fill_time = max(f.execution.time for f in sym_fills)
            close_dt_str = last_fill_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(last_fill_time, 'strftime') else str(last_fill_time)

            close_data = {
                'close_datetime': close_dt_str,
                'close_price': round(avg_price, 4),
                'close_vol': total_vol,
                'close_fund': round(close_fund, 2),
                'gross_profit': round(gross_profit, 2),
                'profit': round(profit, 2)
            }

            for k, v in close_data.items():
                if k in df.columns:
                    df.loc[idx, k] = v

            patched_count += 1
            self.logger.info(
                f"📝 补写: {symbol} ({open_action}) | "
                f"平仓 {total_vol}股 @ ${avg_price:.2f} | 净利: ${profit:.2f}"
            )

        if patched_count > 0:
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"✅ CSV 补写完成: {patched_count} 条记录已更新 ({csv_path.name})")
        else:
            self.logger.info(f"✅ 无需补写 ({csv_path.name})")