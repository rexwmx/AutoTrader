# -*- coding: utf-8 -*-
"""
订单管理模块
"""
import asyncio
from typing import Optional, List
from ib_async import IB, Stock, MarketOrder, Trade
from logger import get_logger


async def get_current_price(ib: IB, symbol: str) -> Optional[float]:
    logger = get_logger()
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified: return None
        [ticker] = ib.reqTickers(qualified[0])
        price = ticker.marketPrice()
        if price and price > 0: return float(price)
        price = ticker.close
        if price and price > 0: return float(price)
        return None
    except Exception as e:
        logger.error(f"❌ {symbol}: 获取价格失败: {e}")
        return None


async def submit_sell_order(ib: IB, symbol: str, volume: int) -> Optional[Trade]:
    logger = get_logger()
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.error(f"❌ {symbol}: 合约确认失败")
            return None

        contract = qualified[0]
        order = MarketOrder('SELL', volume)
        trade = ib.placeOrder(contract, order)

        # 仅做短暂等待让 API 注册订单，绝不提前判断生死
        await asyncio.sleep(0.5)

        logger.info(f"📤 {symbol}: 卖空订单已提交 | {volume}股 | 初始状态: {trade.orderStatus.status}")
        return trade

    except Exception as e:
        logger.error(f"❌ {symbol}: 提交卖空订单失败: {e}")
        return None


async def submit_buy_order(ib: IB, symbol: str, volume: int) -> Optional[Trade]:
    logger = get_logger()
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.error(f"❌ {symbol}: 合约确认失败")
            return None

        contract = qualified[0]
        order = MarketOrder('BUY', volume)
        trade = ib.placeOrder(contract, order)

        await asyncio.sleep(0.5)

        logger.info(f"📤 {symbol}: 买入订单已提交 | {volume}股 | 初始状态: {trade.orderStatus.status}")
        return trade

    except Exception as e:
        logger.error(f"❌ {symbol}: 提交买入订单失败: {e}")
        return None


async def cancel_order(ib: IB, trade: Trade) -> bool:
    logger = get_logger()
    try:
        symbol = trade.contract.symbol
        if trade.orderStatus.status in ['Filled', 'Cancelled', 'Inactive']:
            return True
        ib.cancelOrder(trade.order)
        await asyncio.sleep(0.5)
        logger.info(f"🚫 {symbol}: 订单已取消")
        return True
    except Exception as e:
        logger.error(f"❌ 取消订单失败: {e}")
        return False


async def cancel_all_orders(ib: IB, trades: List[Trade]) -> int:
    logger = get_logger()
    cancelled = 0
    for trade in trades:
        if trade and trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
            if await cancel_order(ib, trade):
                cancelled += 1
    if cancelled > 0:
        logger.info(f"🚫 共取消 {cancelled} 个订单")
    return cancelled

def is_order_active(trade: Trade) -> bool:
    if trade is None: return False
    return trade.orderStatus.status in ['Submitted', 'PendingSubmit', 'PreSubmitted', 'PendingCancel']

# 在 order.py 中，更新以下函数（如果之前有的话）：

def is_order_filled(trade: Trade) -> bool:
    """
    判断订单是否有实际成交
    优先级：filled > 0 > fills列表 > status
    """
    if trade is None:
        return False
    # 最高优先级：filled > 0
    if trade.orderStatus.filled > 0:
        return True
    # 次优先级：fills 列表
    if hasattr(trade, 'fills') and len(trade.fills) > 0:
        return True
    return False


def get_fill_price(trade: Trade) -> float:
    """获取订单的成交均价（优先使用实际数据）"""
    if trade is None:
        return 0.0
    # 优先级1：avgFillPrice
    if trade.orderStatus.avgFillPrice > 0:
        return float(trade.orderStatus.avgFillPrice)
    # 优先级2：fills 列表加权平均
    if hasattr(trade, 'fills') and len(trade.fills) > 0:
        total_shares = sum(f.execution.shares for f in trade.fills)
        if total_shares > 0:
            total_cost = sum(f.execution.shares * f.execution.price for f in trade.fills)
            return float(total_cost / total_shares)
    return 0.0


def get_filled_volume(trade: Trade) -> int:
    """获取订单的成交数量（优先使用实际数据）"""
    if trade is None:
        return 0
    # 优先级1：orderStatus.filled
    if trade.orderStatus.filled > 0:
        return int(trade.orderStatus.filled)
    # 优先级2：fills 列表
    if hasattr(trade, 'fills') and len(trade.fills) > 0:
        return int(sum(f.execution.shares for f in trade.fills))
    return 0

