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


def _trade_filled(t) -> int:
    try:
        return int(getattr(t.orderStatus, 'filled', 0) or 0)
    except Exception:
        return 0


def _trade_status(t):
    try:
        return t.orderStatus.status
    except Exception:
        return None


async def wait_for_order_final(trade: Trade, expected_volume: int = 0,
                               timeout_seconds: int = 120) -> str:
    """统一"等订单完成"：等 **全量成交**、订单进入**终态** 或 **超时** 才结算

    【2026-09-27 修复"部分成交竞态"】旧行为 "filled>0 → 立即返回 Filled"：
    首笔成交到达瞬间（订单仍在工作、只成交了部分）调用方就判"未全部成交"，
    立刻撤销仍在工作的剩余挂单 —— 市价单分几笔成交（间隔仅几毫秒）时，
    剩余量被这次撤销杀死。后果：多一次撤单 + ~10s 重试等待 + 一次补单
    （多付佣金/滑点）；且若"重试前复核发现仓位已归零"，旧代码直接成功返回
    却**不写平仓账**，账本缺口被迫留给退出补写兜底。

    新语义（平仓 / 收敛循环 / 对冲建仓共用）：
    1. filled >= expected_volume        → 结算 'Filled'（全量成交）；
    2. 订单终态 (Filled/Cancelled/Inactive/ApiCancelled) → 结算；
       部分成交+终态 → 仍返回 'Filled'（调用方按 get_filled_volume 判部分，
       与旧契约一致）；Cancelled 且 filled=0 → 延迟 CANCELLED_CONFIRM_DELAY
       秒再确认（Paper 假 Cancelled：成交可能随后落账）；
    3. 超时 → filled>0 返回 'Filled'（调用方自决部分/撤单），否则 'Timeout'。

    **部分成交 + 订单仍在工作 → 不结算**（继续等剩余量到达，不唤醒调用方
    去撤单）—— 这是与旧函数唯一的语义差别。

    expected_volume = 0（如无真实 order 的假对象）→ 退化为旧语义（任何成交即结算），
    保证 fake 测试对象行为不变。

    返回值（契约不变）：'Filled'(有成交) / 'Cancelled' / 'Inactive'
    / 'ApiCancelled' / 'Timeout'
    """
    logger = get_logger()
    symbol = trade.contract.symbol
    TERMINAL_STATES = {'Filled', 'Cancelled', 'Inactive', 'ApiCancelled'}
    try:
        expected = max(0, int(expected_volume))
    except Exception:
        expected = 0

    def _settle(t):
        """已结算的终态（调用方契约值）；None = 仍在工作，不提前唤醒（不杀剩余量）"""
        f = _trade_filled(t)
        if f > 0 and (expected == 0 or f >= expected):
            return 'Filled'
        st = _trade_status(t)
        if st in TERMINAL_STATES:
            return 'Filled' if f > 0 else st
        return None

    async def _confirm_cancelled() -> str:
        """Cancelled 且 filled=0：Paper 假 Cancelled 延迟确认（成交可能随后落账）"""
        logger.debug(f"🔍 {symbol}: Cancelled 且 filled=0，等待 {CANCELLED_CONFIRM_DELAY}秒 确认（Paper 延迟落账）...")
        await asyncio.sleep(CANCELLED_CONFIRM_DELAY)
        if _trade_filled(trade) > 0:
            logger.info(f"🔄 {symbol}: Cancelled 后实际已成交！filled={_trade_filled(trade)}")
            return 'Filled'
        if hasattr(trade, 'fills') and len(trade.fills) > 0:
            logger.info(f"🔄 {symbol}: Cancelled 后通过fills确认成交！")
            return 'Filled'
        logger.debug(f"📡 {symbol}: 延迟确认后仍无成交，最终状态: Cancelled")
        return 'Cancelled'

    # 绑定事件前预检（提交后的短暂等待里可能已结算）
    s = _settle(trade)
    if s == 'Cancelled':
        return await _confirm_cancelled()
    if s is not None:
        return s

    completion_event = asyncio.Event()

    def on_status_update(trade_obj):
        """事件回调：仅在「全量成交」或「终态」时唤醒 ——
        部分成交 + 仍在工作 **不唤醒**（唤醒即让调用方去撤剩余工作单 = 竞态根因）"""
        if _settle(trade_obj) is not None:
            completion_event.set()

    # 绑定事件
    event_name = None
    event_obj = None
    for name in ['statusEvent', 'statusUpdateEvent', 'updateEvent']:
        if hasattr(trade, name):
            event_name = name
            break

    if event_name:
        event_obj = getattr(trade, name)
        event_obj += on_status_update

    try:
        # 绑定后再次检查（防止回调窗口竞态）
        s = _settle(trade)
        if s == 'Cancelled':
            return await _confirm_cancelled()
        if s is not None:
            return s

        # 异步等待
        await asyncio.wait_for(completion_event.wait(), timeout=timeout_seconds)

        # 结算后再做一次 Cancelled 延迟确认
        s = _settle(trade)
        if s == 'Cancelled':
            return await _confirm_cancelled()
        if s is not None:
            f = _trade_filled(trade)
            logger.info(f"📡 {symbol}: 确认结算 | filled={f} | "
                        f"status={_trade_status(trade)}")
            return s
        # 兜底：事件已触发但状态仍未结算（理论上不可达）
        if _trade_filled(trade) > 0:
            return 'Filled'
        return _trade_status(trade) or 'Timeout'

    except asyncio.TimeoutError:
        # 超时后最终检查：有成交（可能部分）→ 'Filled'，调用方自决
        f = _trade_filled(trade)
        if f > 0:
            logger.info(f"📡 {symbol}: 超时但有成交 filled={f}（可能部分），交由调用方判部分/撤单")
            return 'Filled'
        logger.warning(f"⏰ {symbol}: 订单等待超时 ({timeout_seconds}秒)，无成交")
        return 'Timeout'

    finally:
        # 解绑事件
        if event_name:
            try:
                event_obj -= on_status_update
            except (ValueError, AttributeError, TypeError):
                pass


async def wait_for_trade_completion(trade: Trade, timeout_seconds: int = 120) -> str:
    """统一入口（历史名称，close.py / position.py / hedge.py 的打桩点，签名不变）

    【2026-09-27】语义升级为 wait_for_order_final：等 **全量成交 或 终态** 才结算，
    废除"任何成交(filled>0)立即返回 Filled"——那是"部分成交仍在工作就被撤单"
    竞态的根因（市价单分笔成交间隔仅几毫秒时，剩余量被撤销杀死：多撤单/多补单/
    多付佣金，且"重试前发现已平"分支不写平仓账）。

    expected_volume 自动从 trade.order.totalQuantity 读取（ib_async 真实订单）；
    无真实 order 的 fake 对象 → 0 → 退化为旧语义，测试打桩行为不变。

    返回值契约不变：'Filled'（有成交，全/部分由调用方按 get_filled_volume 判断）
    / 'Cancelled' / 'Inactive' / 'ApiCancelled' / 'Timeout'
    """
    expected = 0
    try:
        expected = int(getattr(getattr(trade, 'order', None), 'totalQuantity', 0) or 0)
    except Exception:
        expected = 0
    return await wait_for_order_final(
        trade, expected_volume=expected, timeout_seconds=timeout_seconds)


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