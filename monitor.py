# -*- coding: utf-8 -*-
"""
订单监控模块
使用事件驱动（Event-Driven），兼容 Paper 账户的各种异常状态
核心原则：filled > 0 的优先级永远高于 status
"""
import asyncio
from ib_async import IB, Trade
from logger import get_logger

# Paper 账户中，Cancelled 可能是中间状态，需要延迟确认
CANCELLED_CONFIRM_DELAY = 10  # 秒


async def wait_for_trade_completion(trade: Trade, timeout_seconds: int = 120) -> str:
    """
    等待订单完成，优先以 filled 判断成交。

    判断优先级：
    1. trade.orderStatus.filled > 0  → 有实际成交（最高优先级）
    2. trade.fills 列表非空          → 有执行记录
    3. trade.orderStatus.status      → 最终状态（最低优先级）

    返回值：
    - 'Filled': 有实际成交（filled > 0），无论 status 是什么
    - 'Cancelled': 无成交且被取消
    - 'Inactive': 无成交且不活跃
    - 'ApiCancelled': 无成交且被API取消
    - 'Timeout': 超时且无成交
    """
    logger = get_logger()
    symbol = trade.contract.symbol

    TERMINAL_STATES = {'Filled', 'Cancelled', 'Inactive', 'ApiCancelled'}

    # 如果订单已经有成交，直接返回
    if trade.orderStatus.filled > 0:
        logger.info(f"📡 {symbol}: 已有成交 filled={trade.orderStatus.filled}，直接确认")
        return 'Filled'

    # 如果订单已经结束且无成交，直接返回
    if trade.orderStatus.status in TERMINAL_STATES:
        # 再次确认 filled（防止状态更新顺序问题）
        if trade.orderStatus.filled > 0:
            return 'Filled'
        return trade.orderStatus.status

    completion_event = asyncio.Event()

    def on_status_update(trade_obj):
        """事件回调：当 TWS 推送订单状态更新时触发"""
        # 优先检查 filled
        if trade_obj.orderStatus.filled > 0:
            completion_event.set()
            return
        # 其次检查终态
        if trade_obj.orderStatus.status in TERMINAL_STATES:
            completion_event.set()

    # 绑定事件
    event_name = None
    for name in ['statusEvent', 'statusUpdateEvent', 'updateEvent']:
        if hasattr(trade, name):
            event_name = name
            break

    if event_name:
        event_obj = getattr(trade, name)
        event_obj += on_status_update

    try:
        # 绑定后再次检查（防止竞态）
        if trade.orderStatus.filled > 0:
            return 'Filled'
        if trade.orderStatus.status in TERMINAL_STATES:
            if trade.orderStatus.filled > 0:
                return 'Filled'
            return trade.orderStatus.status

        # 异步等待
        await asyncio.wait_for(completion_event.wait(), timeout=timeout_seconds)

        # ==================== 关键判断逻辑 ====================
        # 优先级1：filled > 0 → 有实际成交
        if trade.orderStatus.filled > 0:
            logger.info(
                f"📡 {symbol}: 确认成交 | "
                f"filled={trade.orderStatus.filled} | "
                f"status={trade.orderStatus.status}"
            )
            return 'Filled'

        # 优先级2：检查 fills 列表
        if hasattr(trade, 'fills') and len(trade.fills) > 0:
            total_filled = sum(f.execution.shares for f in trade.fills)
            if total_filled > 0:
                logger.info(
                    f"📡 {symbol}: 通过fills确认成交 | "
                    f"total_filled={total_filled} | "
                    f"status={trade.orderStatus.status}"
                )
                return 'Filled'

        # 优先级3：根据 status 判断（此时 filled == 0）
        final_status = trade.orderStatus.status

        # 对于 Cancelled 状态，延迟确认（Paper账户可能先报Cancelled后成交）
        if final_status == 'Cancelled':
            logger.debug(f"🔍 {symbol}: 收到 Cancelled 且 filled=0，等待 {CANCELLED_CONFIRM_DELAY}秒 确认...")
            await asyncio.sleep(CANCELLED_CONFIRM_DELAY)

            # 延迟后再次检查 filled
            if trade.orderStatus.filled > 0:
                logger.info(f"🔄 {symbol}: Cancelled 后实际已成交！filled={trade.orderStatus.filled}")
                return 'Filled'
            if hasattr(trade, 'fills') and len(trade.fills) > 0:
                logger.info(f"🔄 {symbol}: Cancelled 后通过fills确认成交！")
                return 'Filled'

            logger.debug(f"📡 {symbol}: 确认无成交，最终状态: Cancelled")
            return 'Cancelled'

        return final_status

    except asyncio.TimeoutError:
        # 超时后最终检查
        if trade.orderStatus.filled > 0:
            logger.info(f"📡 {symbol}: 超时但有成交 filled={trade.orderStatus.filled}")
            return 'Filled'
        logger.warning(f"⏰ {symbol}: 订单等待超时 ({timeout_seconds}秒)，无成交")
        return 'Timeout'

    finally:
        # 解绑事件
        if event_name:
            try:
                event_obj -= on_status_update
            except (ValueError, AttributeError):
                pass


def get_actual_filled_volume(trade: Trade) -> int:
    """
    获取实际成交数量（优先使用 filled，其次使用 fills 列表）
    """
    if trade is None:
        return 0

    # 优先级1：orderStatus.filled
    if trade.orderStatus.filled > 0:
        return int(trade.orderStatus.filled)

    # 优先级2：fills 列表
    if hasattr(trade, 'fills') and len(trade.fills) > 0:
        return int(sum(f.execution.shares for f in trade.fills))

    return 0


def get_actual_fill_price(trade: Trade) -> float:
    """
    获取实际成交均价（优先使用 avgFillPrice，其次使用 fills 列表计算）
    """
    if trade is None:
        return 0.0

    # 优先级1：orderStatus.avgFillPrice
    if trade.orderStatus.avgFillPrice > 0:
        return float(trade.orderStatus.avgFillPrice)

    # 优先级2：fills 列表加权平均
    if hasattr(trade, 'fills') and len(trade.fills) > 0:
        total_shares = sum(f.execution.shares for f in trade.fills)
        if total_shares > 0:
            total_cost = sum(f.execution.shares * f.execution.price for f in trade.fills)
            return float(total_cost / total_shares)

    return 0.0