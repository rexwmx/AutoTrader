# -*- coding: utf-8 -*-
"""
对冲逻辑模块
采用事件驱动 + 智能批次推进架构
对卖空和买入订单均增加持仓验证，解决Paper账户假Cancelled问题
"""
import asyncio
import datetime
import time
from typing import List, Tuple
from pathlib import Path
from ib_async import IB
from models import StockInfo, TradeRecord, HedgeOrder
from order import (
    get_current_price, submit_sell_order, submit_buy_order,
    cancel_order, get_fill_price, get_filled_volume
)
from monitor import wait_for_trade_completion
from csv_writer import append_trade_record
from util import calculate_shares, format_datetime
from logger import get_logger
from constants import (
    TARGET_HEDGED_COUNT, FUND_PER_STOCK, ORDER_TIMEOUT, BATCH_SIZE
)


async def verify_buy_position(ib2: IB, symbol: str, retries: int = 3, delay: float = 5.0) -> dict:
    """
    验证账户2是否实际持有该股票的买入持仓
    用于处理买入订单的"假Cancelled"问题

    Returns:
        dict: {'exists': bool, 'volume': int, 'avg_cost': float}
    """
    logger = get_logger()
    for attempt in range(retries):
        if attempt > 0:
            await asyncio.sleep(delay)
        try:
            await ib2.reqPositionsAsync()
            await asyncio.sleep(1)
            positions = {p.contract.symbol: p for p in ib2.positions()}
            if symbol in positions and positions[symbol].position > 0:
                return {
                    'exists': True,
                    'volume': int(positions[symbol].position),
                    'avg_cost': float(positions[symbol].avgCost)
                }
        except Exception as e:
            logger.debug(f"🔍 {symbol}: 验证买入持仓异常: {e}")
    return {'exists': False, 'volume': 0, 'avg_cost': 0.0}


async def perform_hedging(
        ib1: IB, ib2: IB, candidates: List[StockInfo], runtime_dir: Path,
        sell_csv_path: Path, buy_csv_path: Path, target_count: int = TARGET_HEDGED_COUNT
) -> Tuple[List[StockInfo], List[HedgeOrder]]:
    logger = get_logger()
    logger.info(f"{'=' * 50}\n开始对冲交易 (简化模式) | 提交: {BATCH_SIZE}个 | 等待: {ORDER_TIMEOUT}秒")

    hedged_stocks: List[StockInfo] = []
    all_orders: List[HedgeOrder] = []

    batch_stocks = candidates[:BATCH_SIZE]
    logger.info(f"\n📦 提交 {len(batch_stocks)} 个卖空订单")

    async def hedge_single_stock_task(stock: StockInfo):
        symbol = stock.code
        hedge_order = HedgeOrder(stock=stock, status='pending')
        all_orders.append(hedge_order)

        price = await get_current_price(ib1, symbol)
        if not price:
            hedge_order.status = 'failed'
            return
        vol = calculate_shares(price, FUND_PER_STOCK)
        if vol < 1:
            hedge_order.status = 'failed'
            return
        hedge_order.volume = vol

        # 1. 提交 Sell 订单
        sell_trade = await submit_sell_order(ib1, symbol, vol)
        if not sell_trade:
            hedge_order.status = 'failed'
            return
        hedge_order.sell_trade = sell_trade
        hedge_order.sell_submit_time = datetime.datetime.now()

        # 2. 等待 Sell 成交
        sell_status = await wait_for_trade_completion(sell_trade, ORDER_TIMEOUT)

        if sell_status != 'Filled':
            if sell_status == 'Timeout':
                await cancel_order(ib1, sell_trade)
            hedge_order.status = 'cancelled'
            logger.warning(f"⚠️ {symbol}: 卖空未成交 (status={sell_status}, filled=0)")
            return

        # 3. Sell 成交！记录 CSV
        hedge_order.status = 'sell_filled'
        hedge_order.sell_fill_time = datetime.datetime.now()
        hedge_order.sell_price = get_fill_price(sell_trade)
        sell_cost = hedge_order.sell_price * vol

        append_trade_record(TradeRecord(
            datetime=format_datetime(hedge_order.sell_fill_time), code=symbol,
            exchange=stock.exchange, industry=stock.industry, action='sell',
            entry_price=hedge_order.sell_price, vol=vol, total_cost=sell_cost, fund_used=sell_cost
        ), sell_csv_path)
        logger.info(
            f"💰 {symbol}: 卖空成交 {vol}股 @ ${hedge_order.sell_price:.2f} (status={sell_trade.orderStatus.status})")

        # 4. 提交对冲 Buy 订单
        buy_trade = await submit_buy_order(ib2, symbol, vol)
        if not buy_trade:
            hedge_order.status = 'sell_only'
            return
        hedge_order.buy_trade = buy_trade
        hedge_order.buy_submit_time = datetime.datetime.now()

        # 5. 等待 Buy 成交
        buy_status = await wait_for_trade_completion(buy_trade, ORDER_TIMEOUT)

        # ==================== 关键修复：买入订单持仓验证 ====================
        if buy_status != 'Filled':
            logger.debug(f"🔍 {symbol}: 买入订单状态 {buy_status}，验证账户2实际持仓...")
            pos_info = await verify_buy_position(ib2, symbol)
            if pos_info['exists']:
                buy_status = 'Filled'
                hedge_order.buy_price = pos_info['avg_cost']
                logger.info(
                    f"🔄 {symbol}: Buy Cancelled 后实际已成交！"
                    f"持仓 {pos_info['volume']}股 @ ${pos_info['avg_cost']:.2f}"
                )
            else:
                if buy_status == 'Timeout':
                    await cancel_order(ib2, buy_trade)
                hedge_order.status = 'sell_only'
                logger.error(f"❌ {symbol}: 对冲买入未成交 (status={buy_status}, filled=0)")
                return

        # 6. Buy 成交！记录 CSV
        hedge_order.status = 'hedged'
        hedge_order.buy_fill_time = datetime.datetime.now()
        if hedge_order.buy_price == 0.0:
            hedge_order.buy_price = get_fill_price(buy_trade)
        buy_cost = hedge_order.buy_price * vol

        append_trade_record(TradeRecord(
            datetime=format_datetime(hedge_order.buy_fill_time), code=symbol,
            exchange=stock.exchange, industry=stock.industry, action='buy',
            entry_price=hedge_order.buy_price, vol=vol, total_cost=buy_cost, fund_used=buy_cost
        ), buy_csv_path)

        hedged_stocks.append(stock)
        logger.info(f"✅ {symbol}: 对冲完成 | 进度: {len(hedged_stocks)}/{target_count}")

    # ==================== 并发提交所有订单 ====================
    tasks = [asyncio.create_task(hedge_single_stock_task(stock)) for stock in batch_stocks]
    await asyncio.gather(*tasks, return_exceptions=True)

    # ==================== 清理残留订单 ====================
    logger.info("\n🚫 清理所有残留的未成交订单...")
    for order in all_orders:
        if order.sell_trade and order.status not in ['hedged', 'sell_filled', 'sell_only']:
            if order.sell_trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
                await cancel_order(ib1, order.sell_trade)
        if order.buy_trade and order.status != 'hedged':
            if order.buy_trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
                await cancel_order(ib2, order.buy_trade)

    # ==================== 统计结果 ====================
    hedged_count = len(hedged_stocks)
    sell_only_count = sum(1 for o in all_orders if o.status == 'sell_only')
    cancelled_count = sum(1 for o in all_orders if o.status == 'cancelled')
    failed_count = sum(1 for o in all_orders if o.status == 'failed')

    logger.info(
        f"\n{'=' * 50}\n"
        f"对冲完成\n"
        f"  成功对冲: {hedged_count}/{len(batch_stocks)}\n"
        f"  仅卖空(未对冲): {sell_only_count}\n"
        f"  卖空未成交: {cancelled_count}\n"
        f"  提交失败: {failed_count}\n"
        f"{'=' * 50}"
    )

    return hedged_stocks, all_orders