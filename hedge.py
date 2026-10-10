# -*- coding: utf-8 -*-
"""
对冲逻辑模块
采用事件驱动 + 智能批次推进架构
对卖空和买入订单均增加持仓验证，解决Paper账户假Cancelled问题

新增：建仓成功时通知 StrategyRunner（runner 可选参数），
记录真实成交时间与入场价，供策略从真实成交时刻开始追踪建仓后极值。
策略侧异常被隔离捕获，绝不影响建仓流程。
"""
import asyncio
import datetime
import math
import time
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from ib_async import IB, Stock, Trade
from models import StockInfo, TradeRecord, HedgeOrder
from order import (
    get_current_price, submit_sell_order, submit_buy_order,
    cancel_order, get_fill_price, get_filled_volume
)
from monitor import wait_for_trade_completion
from position import get_positions, get_positions_strict_with_retry
from csv_writer import append_trade_record
from util import calculate_shares, format_datetime
from logger import get_logger
from constants import (
    TARGET_HEDGED_COUNT, FUND_PER_STOCK, ORDER_TIMEOUT, BATCH_SIZE,
    SHORTABLE_TICK_ID, SHORTABLE_CHECK_TIMEOUT, BACKFILL_FILL_WAIT_SECONDS
)

# 【R2】确认阶段双快照间隔：第 1 次快照 + 5s 后第 2 次快照取并集
# （2026-09-21 INFQ 类"撤单后才落账"的空头由第 2 次快照接住）
CONFIRM_UNION_DELAY_SECONDS = 5.0


# ==================== [2026-10-09 修复] 短卖资格（SHORTABLE tick）+ 撤单防线 ====================

async def is_shortable(ib: IB, symbol: str,
                       timeout: float = SHORTABLE_CHECK_TIMEOUT) -> Optional[bool]:
    """
    用 reqMktData(genericTickList='236') 查询该标的能否做空（有无可借券）

    【2026-10-09】10/9 测试 10 只盘前排队空单未成交，可能原因之一是"无券可借"
    （can't locate）——当时日志没有 whyHeld 等诊断信息，无法区分
    Paper 撮合慢 / 延迟开盘 / 无券可借。现在用 IBKR 的 SHORTABLE 行情
    （generic tick 236 → ticker.shortable / shortableShares）在提交前过滤。

    注意：采用快照式请求（startReq 的 future 在 tickSnapshotEnd 时 resolve），
    不阻塞事件循环；超时/异常一律降级为 None（未知），绝不抛给调用方。

    Returns:
        True  — IBKR 报告可借（shortable=1）
        False — IBKR 明确报告不可借（shortable=0，通常即无券可借）→ 应剔除
        None  — 无数据 / 超时 / 异常（未知 → 调用方自行决定保留或剔除；
                本流程保留，避免盘前数据未送达时全军覆没）
    """
    logger = get_logger()
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.warning(f"⚠️ {symbol}: 合约确认失败，短卖资格未知")
            return None
        contract = qualified[0]
        reqId = ib.client.getReqId()
        future = ib.wrapper.startReq(reqId, contract)
        ticker = ib.wrapper.startTicker(reqId, contract, "snapshot")
        ib.client.reqMktData(reqId, contract, SHORTABLE_TICK_ID, True, False, [])
        await asyncio.wait_for(future, timeout=timeout)
        raw = ticker.shortable
        if raw is None or (isinstance(raw, float) and math.isnan(raw)):
            raw = ticker.shortableShares
        if raw is None or (isinstance(raw, float) and math.isnan(raw)):
            logger.info(f"🔍 {symbol}: SHORTABLE 数据未送达（未知，不过滤）")
            return None
        ok = float(raw) > 0
        if ok:
            logger.debug(f"🔍 {symbol}: shortable=1（可借）")
        else:
            logger.warning(f"🚫 {symbol}: IBKR 报告不可做空（shortable=0，无券可借）→ 剔除")
        return ok
    except Exception as e:
        logger.info(f"🔍 {symbol}: 查询短卖资格失败（{e}）→ 视为未知，不过滤")
        return None


async def _recancel(ib: IB, trade: Trade) -> bool:
    """无条件重发撤单（绕过 order.cancel_order 的本地状态守卫）

    【2026-10-09】ib_async 会把非警告错误（如 10349）按硬错误处理、本地把订单
    标记为 Cancelled（"假 Cancelled"），但交易所侧订单可能仍然挂单——
    若按本地 Cancelled 放行，迟到成交即成为没有对冲的裸空头。
    对确实已撤的单重发 cancelOrder，TWS 回 203 "already canceled"，无害。
    """
    logger = get_logger()
    try:
        ib.cancelOrder(trade.order)
        return True
    except Exception as e:
        logger.error(f"❌ {trade.contract.symbol}: 撤单调用异常: {e}")
        return False


def record_short_confirmation(
        symbol: str, vol: int, cost: float, stock: StockInfo,
        sell_csv_path: Path, reason: str,
        runner=None, trade_store=None) -> None:
    """空头确认后的账本写入 + 策略登记（首轮确认与补位共用的唯一账本入口）"""
    logger = get_logger()
    if trade_store is not None:
        trade_store.append_open(
            account='account1', symbol=symbol, price=cost, volume=vol,
            exchange=stock.exchange, industry=stock.industry,
            event_datetime=datetime.datetime.now(),
            strategy='hedge', reason=reason
        )
    else:
        append_trade_record(TradeRecord(
            datetime=format_datetime(datetime.datetime.now()), code=symbol,
            exchange=stock.exchange, industry=stock.industry, action='sell',
            entry_price=cost, vol=vol, total_cost=cost * vol, fund_used=cost * vol
        ), sell_csv_path)
    if runner is not None:
        try:
            runner.on_entry(symbol, 'account1', cost, vol, datetime.datetime.now())
        except Exception as e:
            logger.warning(f"⚠️ {symbol}: 通知策略(account1)失败: {e}")
    logger.info(f"💰 [空头已确认] {symbol}: 实际持有 {vol}股 @ ${cost:.2f}（{reason}）")


async def verify_buy_position(ib2: IB, symbol: str, expected_volume: int = 0,
                              retries: int = 3, delay: float = 5.0) -> dict:
    """
    验证账户2是否实际持有该股票的买入持仓
    用于处理买入订单的"假Cancelled"问题

    基于权威持仓快照（reqPositionsAsync 返回值）实现，
    不依赖 ib.positions() 长寿命缓存（幽灵条目会造成假阳性）。
    expected_volume>0 时同时校验成交数量达到卖空数量，
    不足视为未成交，缺口由阶段7调平补全。

    Returns:
        dict: {'exists': bool, 'volume': int, 'avg_cost': float}
    """
    logger = get_logger()
    for attempt in range(retries):
        if attempt > 0:
            await asyncio.sleep(delay)
        try:
            positions = await get_positions(ib2)
            hit = positions.get(symbol)
            if hit and hit['position'] > 0 and (not expected_volume or hit['position'] >= expected_volume):
                return {
                    'exists': True,
                    'volume': int(hit['position']),
                    'avg_cost': float(hit['avgCost'])
                }
        except Exception as e:
            logger.debug(f"🔍 {symbol}: 验证买入持仓异常: {e}")
    return {'exists': False, 'volume': 0, 'avg_cost': 0.0}


# ==================== 新版建仓三阶段（开市前一分钟预挂单 + 开盘确认 + 双通道并发） ====================
# 目标：对冲建仓 + 1分钟数据订阅全部在开市第一分钟内完成，
# 使第一根1分钟bar收盘后立即激活策略。
#
# 阶段 6.1 pre_submit_sell_orders     盘前(开盘-60s)并发预下卖空单（TWS 置 PreSubmitted，开盘自动撮合）
#                                     【2026-10-09】提交前SHORTABLE短卖资格预检 + 自动从后续候选补位
# 阶段 6.2 confirm_short_positions    开盘+45s（或不晚于提交+45s，全部订单到终态可提前）
#                                     撤残单(whyHeld诊断/防假Cancelled)、权威快照确认空头、写账本
# 阶段 6.3 execute_hedge_buys         账户2 并发买入对冲（与 1 分钟数据订阅走不同 TCP 连接，双通道并行）
#                                     【2026-10-09】实际建仓由阶段7收敛循环(reconcile_positions)负责
# 阶段 6.4 backfill_short_positions   【2026-10-09 新增】确认不足10只时，下一批候选自动补位


async def pre_submit_sell_orders(
        ib1: IB, candidates: List[StockInfo], batch_size: int = BATCH_SIZE
) -> List[Tuple[StockInfo, Trade]]:
    """
    阶段 6.1: 盘前预下空单 (开盘-60s 调用)

    并发对每只标的：短卖资格 → 取价 → 计算股数 → 提交市价空单到账户1。
    常规时段(RTH)生效的市价单在盘前提交时，TWS 会将其置为 PreSubmitted
    排队，开盘钟声一响自动送入交易所参与开盘撮合——把最重的
    合约确认/取价/报送 I/O 全部移出 09:30:00 的拥堵窗口。

    注意：盘前取价为昨收/盘前成交价，开盘跳空对"每只 $1000 上限"的
    股数误差极小（0~1~2股），残留缺口由后续调平阶段兜底，在对冲容差范围内。

    【2026-10-09 修复】10/9 测试：15 只盘前排队的空单只有 5 只成交，
    未成交的 10 只被确认点撤掉后**程序没有补位逻辑**（TARGET_HEDGED_COUNT=10
    根本没被引用，剩下 79 只候选全部闲置）。现在：
    1) 提交前并发查询 SHORTABLE（reqMktData tick 236），剔除 IBKR **明确报告
       不可借券**的标的（查不到/超时的按"未知"保留——避免盘前数据未送达时
       全部被过滤掉；未知标的仍靠确认点的成交事实与 whyHeld 日志鉴别）；
    2) 合格数不足 batch_size 时，按排名顺序自动从 candidates[batch_size:]
       （即"后面的候选"）继续取用，直到凑满或候选耗尽。

    Returns:
        List[Tuple[StockInfo, Trade]]: 提交成功的 (股票, 订单) 列表
    """
    logger = get_logger()
    initial = candidates[:batch_size]
    backup_pool = list(candidates[batch_size:])

    # ---- 短卖资格预检（并发；SHORTABLE tick 236）----
    async def check_one(stock: StockInfo):
        return (stock, await is_shortable(ib1, stock.code))

    checks = await asyncio.gather(*(check_one(s) for s in initial), return_exceptions=True)
    keep: List[StockInfo] = []
    for r in checks:
        if isinstance(r, BaseException):
            logger.warning(f"⚠️ [短卖资格] 查询异常（保守保留候选）: {r!r}")
            continue
        stock, ok = r
        if ok is False:
            continue  # IBKR 明确不可借券 → 剔除
        keep.append(stock)  # True（可借）或 None（未知）→ 保留
    if len(keep) < len(initial):
        logger.warning(
            f"⚠️ [短卖资格] 首批剔除 {len(initial) - len(keep)} 只不可借券标的，"
            f"保留 {len(keep)}/{len(initial)}"
        )

    # ---- 从后续候选补齐（同样先查资格，再按序取用）----
    need = batch_size - len(keep)
    while need > 0 and backup_pool:
        probe = backup_pool[:max(need * 2, 2)]
        backup_pool = backup_pool[len(probe):]
        probed = await asyncio.gather(
            *(check_one(s) for s in probe), return_exceptions=True)
        for r in probed:
            if need <= 0:
                break
            if isinstance(r, BaseException):
                continue
            stock, ok = r
            if ok is not False:
                keep.append(stock)
                need -= 1

    batch = keep
    if not batch:
        logger.error("❌ [盘前预报送] 所有候选均被短卖资格/查询异常剔除，无可提交标的")
        return []
    logger.info(
        f"📦 [盘前预报送] 并发提交 {len(batch)} 只股票的卖空订单"
        f"（PreSubmitted，开盘自动撮合 | 短卖资格预检已启用）..."
    )

    async def submit_one(stock: StockInfo):
        symbol = stock.code
        try:
            price = await get_current_price(ib1, symbol)
            if not price or price <= 0:
                logger.warning(f"⚠️ {symbol}: 取价失败，跳过预报送")
                return None
            vol = calculate_shares(price, FUND_PER_STOCK)
            if vol < 1:
                logger.warning(f"⚠️ {symbol}: 计算股数 {vol} < 1，跳过预报送")
                return None
            trade = await submit_sell_order(ib1, symbol, vol)
            if trade is None:
                logger.error(f"❌ {symbol}: 卖空订单预报送失败")
                return None
            return (stock, trade)
        except Exception as e:
            logger.error(f"❌ {symbol}: 卖空订单预报送异常: {e}")
            return None

    results = await asyncio.gather(*(submit_one(s) for s in batch), return_exceptions=True)
    submitted = [r for r in results if isinstance(r, tuple)]
    failed_count = sum(1 for r in results if r is None or isinstance(r, BaseException))
    logger.info(f"✅ 盘前预报送完成: 成功 {len(submitted)}/{len(batch)} 笔，等待开盘撮合 (失败/跳过 {failed_count})")
    return submitted


async def confirm_short_positions(
        ib1: IB, account1: str,
        submitted_trades: List[Tuple[StockInfo, Trade]],
        sell_csv_path: Path, runner=None,
        settle_seconds: float = 2.0,
        trade_store=None,
) -> Tuple[List[StockInfo], Dict[str, dict]]:
    """
    阶段 6.2: 确认空头持仓 (max(开盘+45s, 提交+45s) 调用，全部订单到终态可提前)

    1) 并发撤销所有"未完全成交"的卖空挂单（含本地假 Cancelled）+ 一次 settle
       （替代串行 15×0.5s 撤单；锁定账户1空头头寸，杜绝开盘后延迟成交敞口）
    2) 【R2 双快照】权威持仓快照确认空头：第 1 次快照（撤单后，现有 09:30:23）
       + 5s 后第 2 次快照取**并集**（冲突以较新的第 2 次为准）。
       2026-09-21 的 INFQ（09:30:28 才落账）即由第 2 次快照接住；MSTR 类（09:31:20 后
       才落账）接不住——接受它进入收敛循环（R1）处理。严格版+重试，失败抛
       PositionSnapshotError（避免静默空字典把"取数失败"误判成"无持仓"）
    3) 按快照写账本并登记 runner（入场价 = 账户1 avgCost，权威成交均价）
       - trade_store 可用 → 事件流 account1/{symbol}.csv 记 OPEN 事件；
       - 否则 → 兜底旧 sell.csv append_trade_record。

    Returns:
        (确认定仓股票列表, 账户1持仓快照)  —— 快照供阶段6.3买入股数直接复用，避免二次取数
    Raises:
        PositionSnapshotError: 快照连续获取失败，调用方必须终止程序并人工核查
    """
    logger = get_logger()

    # ---- 1) 并发送撤所有"未完全成交"的订单（含本地已标 Cancelled 的）----
    # 【2026-10-09 修复】旧逻辑按本地状态 (status not in Filled/Cancelled/Inactive)
    # 筛选待撤单——但 ib_async 遇到非警告错误（如修复前未设 TIF 的 10349）会**本地
    # 把订单标记为 Cancelled**（"假 Cancelled"），交易所侧订单可能仍然挂单：
    # 按旧逻辑被放行的单会在撤单后迟到成交，变成没有对冲的裸空头。
    # 现在：所有 filled < 下单总量 的订单一律重发 cancelOrder
    # （对确实已撤的单 TWS 回 203 "already canceled"，无害）。
    # 用 order.totalQuantity（下单时确定）而非 orderStatus.remaining（依赖回报，
    # 极端情况下未更新会漏撤）。
    pending_cancel = [
        t for (_stock, t) in submitted_trades
        if t is not None and t.orderStatus.filled < t.order.totalQuantity
    ]
    if pending_cancel:
        # 【2026-10-09 修复】撤单前逐笔落诊断日志：10/9 的 10 只未成交空单
        # 现有日志既没有 whyHeld（如 'locate' = 无券可借）、也没有区分
        # Paper 撮合慢 / 延迟开盘，无从定位原因；现在全部写进 log。
        fake_cancelled = []
        for t in pending_cancel:
            st = t.orderStatus
            if st.status == 'Cancelled':
                fake_cancelled.append(t.contract.symbol)
            logger.warning(
                f"🧾 [撤单前诊断] {t.contract.symbol}: status={st.status} | "
                f"whyHeld={st.whyHeld!r} | filled {int(st.filled)}/{int(st.total)}"
                + (" ⚠️ 本地已标 Cancelled（疑似 10349 假 Cancelled，交易所侧可能仍挂单，强制重撤）"
                   if st.status == 'Cancelled' else "")
            )
        if fake_cancelled:
            logger.warning(f"🚨 [撤单] 发现 {len(fake_cancelled)} 笔本地 Cancelled 单仍在待撤名单: {fake_cancelled}")
        logger.info(f"🛑 [撤单] 并发送撤 {len(pending_cancel)} 笔未完全成交卖空订单，锁定空头头寸...")
        cancel_results = await asyncio.gather(
            *(_recancel(ib1, t) for t in pending_cancel), return_exceptions=True
        )
        failed_cancels = sum(1 for r in cancel_results if r is not True)
        if failed_cancels:
            logger.warning(f"⚠️ [撤单] {failed_cancels} 笔撤单异常，以持仓快照为准（未成交不持仓则无敞口）")
        # 一次 settle，让 TWS 完成撤单与持仓落账（替代每单串行 0.5s）
        await asyncio.sleep(settle_seconds)
    else:
        logger.info("🛑 [撤单] 无未成交挂单，直接确认持仓")

    # ---- 2) 【R2 双快照】权威快照（严格版 + 重试）×2，取并集 ----
    pos1 = await get_positions_strict_with_retry(ib1, account1, retries=3, delay=2.0)
    logger.debug(f"📊 确认快照#1: {len(pos1)} 只空头")
    # 无论撤单与否都保留 5s 间隔：Paper 持仓落账本就可能晚于成交（撤单/成交均可能）
    await asyncio.sleep(CONFIRM_UNION_DELAY_SECONDS)
    pos1_b = await get_positions_strict_with_retry(ib1, account1, retries=3, delay=2.0)
    # 并集：两次快照的标的都保留；冲突以更晚（更新）的第 2 次快照为准
    merged = {**pos1, **pos1_b}
    late_caught = sorted(set(pos1_b) - set(pos1))
    if late_caught:
        logger.info(f"🕗 双快照#2 补接住延迟落账空头: {late_caught}")
    pos1 = merged
    logger.debug(f"📊 确认快照(并集): {len(pos1)} 只空头")

    # ---- 3) 确认空头 + 写 CSV + 登记 runner ----
    stock_map = {stock.code: stock for stock, _ in submitted_trades}
    confirmed_stocks: List[StockInfo] = []
    for symbol, p in pos1.items():
        if p['position'] >= 0:
            continue
        stock = stock_map.get(symbol)
        if stock is None:
            logger.warning(f"⚠️ {symbol}: 存在空头持仓 {abs(int(p['position']))} 股但不在本次预报送名单内，未处理（请人工核查）")
            continue
        vol = abs(int(p['position']))
        cost = float(p['avgCost'])
        confirmed_stocks.append(stock)
        # ==================== 账本写入 + 策略登记（共用入口） ====================
        record_short_confirmation(
            symbol, vol, cost, stock, sell_csv_path,
            reason='盘前预挂单成交确认', runner=runner, trade_store=trade_store
        )

    logger.info(f"📊 账户1实际做空成功: {len(confirmed_stocks)} 只标的")
    return confirmed_stocks, pos1


async def backfill_short_positions(
        ib1: IB, account1: str,
        candidates: List[StockInfo],
        sell_csv_path: Path,
        runner=None, trade_store=None,
        max_stocks: Optional[int] = None,
) -> Tuple[List[StockInfo], Dict[str, dict]]:
    """
    阶段 6.4（2026-10-09 新增）: 确认数量不足 TARGET_HEDGED_COUNT 时的补位

    背景（10/9 测试根因）：确认点只有 5/15 只成交，程序把剩下的 85 只候选
    整个闲置，TARGET_HEDGED_COUNT=10 从未被引用。现在按排名取下一批候选补位：

    提交（含SHORTABLE短卖资格/补位，见 pre_submit_sell_orders）
        → 常规时段市价单，能成交的秒级成交；等待全部订单进终态
           （或 BACKFILL_FILL_WAIT_SECONDS 预算上限）
        → 撤掉所有未完全成交单（撤单前逐笔落 status/whyHeld 诊断日志，
           防 ib_async "假 Cancelled" 与主确认同样处理）
        → 双快照（R2）确认新增空头 → 写账本 + 登记策略（取前 max_stocks 只，
           避免补过头）

    注意：此时市场已在常规交易时段，市价单即时成交，无需再等 PreSubmitted。

    Returns:
        (本轮新增确认的空头列表[已写账本/登记策略], 账户1全量持仓快照)
    Raises:
        PositionSnapshotError: 持仓快照连续获取失败（调用方必须终止程序并人工核查）
    """
    logger = get_logger()

    # ---- 1) 提交（pre_submit_sell_orders 内含短卖资格检查 + 从后续候选补位）----
    submitted = await pre_submit_sell_orders(
        ib1, candidates, batch_size=len(candidates)
    )
    if not submitted:
        logger.warning("⚠️ [补位] 补位空单全部提交失败（合约确认失败/全部不可借/TWS拒绝——查 [短卖资格] 与 TWS 错误日志）")
        return [], {}

    trades = [t for (_s, t) in submitted if t is not None]

    # ---- 2) 等全部订单到终态或预算上限 ----
    deadline = time.monotonic() + BACKFILL_FILL_WAIT_SECONDS
    while time.monotonic() < deadline:
        if all(t.isDone() for t in trades):
            break
        await asyncio.sleep(0.5)
    still_pending = [t.contract.symbol for t in trades if not t.isDone()]
    logger.info(
        f"⏱️ [补位] 等待结束: {len(trades) - len(still_pending)}/{len(trades)} 单到终态"
        + (f" | 仍未终态(将被撤): {still_pending}" if still_pending else "（全部到终态）")
    )

    # ---- 3) 撤单（撤单前落诊断日志，含假 Cancelled 兜底 —— 与主确认一致）----
    to_cancel = [t for t in trades if t.orderStatus.filled < t.order.totalQuantity]
    for t in to_cancel:
        st = t.orderStatus
        logger.warning(
            f"🧾 [补位-撤单前诊断] {t.contract.symbol}: status={st.status} | "
            f"whyHeld={st.whyHeld!r} | filled {int(st.filled)}/{int(st.total)}"
        )
    if to_cancel:
        await asyncio.gather(*(_recancel(ib1, t) for t in to_cancel), return_exceptions=True)
        await asyncio.sleep(2.0)  # settle：TWS 完成撤单与持仓落账

    # ---- 4) 双快照（R2）确认新增空头 ----
    pos1 = await get_positions_strict_with_retry(ib1, account1, retries=3, delay=2.0)
    await asyncio.sleep(CONFIRM_UNION_DELAY_SECONDS)
    pos1_b = await get_positions_strict_with_retry(ib1, account1, retries=3, delay=2.0)
    merged = {**pos1, **pos1_b}

    stock_map = {stock.code: stock for stock, _ in submitted}
    cap = max_stocks if max_stocks is not None else len(merged)
    new_stocks: List[StockInfo] = []
    for symbol, p in merged.items():
        if len(new_stocks) >= cap:
            break
        if p['position'] >= 0:
            continue
        stock = stock_map.get(symbol)
        if stock is None:
            # 快照中出现但不在本轮提交名单内 → 可能是首轮迟到落账或历史持仓，
            # 补位不认领（由阶段7收敛循环按实际持仓对账兜底）
            logger.warning(f"⚠️ [补位] {symbol}: 存在空头持仓但不在本轮补位名单，不认领（请人工核查）")
            continue
        vol = abs(int(p['position']))
        cost = float(p['avgCost'])
        new_stocks.append(stock)
        record_short_confirmation(
            symbol, vol, cost, stock, sell_csv_path,
            reason='补位成交确认', runner=runner, trade_store=trade_store
        )

    logger.info(f"📊 [补位] 本轮新增确认空头: {len(new_stocks)} 只标的")
    return new_stocks, merged


async def execute_hedge_buys(
        ib2: IB, account2: str, target_stocks: List[StockInfo],
        positions1: Dict[str, dict], buy_csv_path: Path, runner=None,
        trade_store=None,
) -> None:
    """
    阶段 6.3: 账户2并发多头买入对冲 (与 1 分钟数据订阅并行执行)

    按账户1确认的空头股数，并发向账户2发出市价买单（ib2/7487 与数据订阅
    ib1/7497 走不同 TCP 连接，物理级并发互不阻塞）。
    每单保留"假Cancelled持仓验证"兜底；未完全成交的缺口交由调平阶段补全。
    """
    logger = get_logger()
    logger.info(f"⚡ [并发通道1] 账户2开始对冲买入 {len(target_stocks)} 只标的...")

    async def buy_one(stock: StockInfo):
        symbol = stock.code
        pos = positions1.get(symbol)
        if pos is None or pos['position'] >= 0:
            logger.error(f"❌ {symbol}: 账户1无对应空头持仓，跳过买入对冲")
            return
        vol = abs(int(pos['position']))
        try:
            trade = await submit_buy_order(ib2, symbol, vol)
            if not trade:
                logger.error(f"❌ {symbol}: 对冲买单提交失败")
                return
            status = await wait_for_trade_completion(trade, ORDER_TIMEOUT)
            reported_status = status
            fill_price = get_fill_price(trade)
            fill_vol = get_filled_volume(trade)

            # 假 Cancelled 兜底：以账户2实际持仓为准（与原流程一致）
            if status != 'Filled' or fill_vol < vol:
                if status == 'Timeout':
                    await cancel_order(ib2, trade)
                pos_info = await verify_buy_position(ib2, symbol, vol)
                if pos_info['exists']:
                    fill_price = pos_info['avg_cost']
                    fill_vol = pos_info['volume']
                    status = 'Filled'
                    logger.info(
                        f"🔄 {symbol}: Buy 报告 {reported_status} 但实际已成交！"
                        f"持仓 {fill_vol}股 @ ${fill_price:.2f}")

            if status == 'Filled' and fill_vol > 0:
                cost = fill_price * fill_vol
                # ==================== 账本写入：事件流优先，旧 CSV 兜底 ====================
                if trade_store is not None:
                    trade_store.append_open(
                        account='account2', symbol=symbol, price=fill_price, volume=fill_vol,
                        exchange=stock.exchange, industry=stock.industry,
                        event_datetime=datetime.datetime.now(),
                        strategy='hedge', reason='对冲买入'
                    )
                else:
                    append_trade_record(TradeRecord(
                        datetime=format_datetime(datetime.datetime.now()),
                        code=symbol, exchange=stock.exchange,
                        industry=stock.industry, action='buy', entry_price=fill_price,
                        vol=fill_vol, total_cost=cost, fund_used=cost
                    ), buy_csv_path)
                if runner is not None:
                    try:
                        runner.on_entry(symbol, 'account2', fill_price, fill_vol,
                                        datetime.datetime.now())
                    except Exception as e:
                        logger.warning(f"⚠️ {symbol}: 通知策略(account2)失败: {e}")
                logger.info(f"✅ [对冲买入完成] {symbol}: {fill_vol}股 @ ${fill_price:.2f}")
            else:
                logger.error(f"⚠️ {symbol}: 对冲买入未完全成交 (成交 {fill_vol}/{vol}) —— 缺口交由调平阶段补全")
        except Exception as e:
            logger.error(f"❌ {symbol}: 对冲买入异常: {e}")

    await asyncio.gather(*(buy_one(stock) for stock in target_stocks), return_exceptions=True)
    logger.info("🏁 [并发通道1] 账户2买入对冲任务批次执行完毕")


async def perform_hedging(
        ib1: IB, ib2: IB, candidates: List[StockInfo], runtime_dir: Path,
        sell_csv_path: Path, buy_csv_path: Path, target_count: int = TARGET_HEDGED_COUNT,
        runner=None,   # 【新增】StrategyRunner（可选，None 则不通知策略）
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

        # 【新增】通知策略：账户1 sell 建仓成功（真实成交时间）
        if runner is not None:
            try:
                runner.on_entry(
                    symbol, 'account1', hedge_order.sell_price,
                    vol, hedge_order.sell_fill_time
                )
            except Exception as e:
                logger.warning(f"⚠️ {symbol}: 通知策略(account1)失败: {e}")

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
            pos_info = await verify_buy_position(ib2, symbol, vol)
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

        # 【新增】通知策略：账户2 buy 建仓成功（真实成交时间）
        if runner is not None:
            try:
                runner.on_entry(
                    symbol, 'account2', hedge_order.buy_price,
                    vol, hedge_order.buy_fill_time
                )
            except Exception as e:
                logger.warning(f"⚠️ {symbol}: 通知策略(account2)失败: {e}")

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