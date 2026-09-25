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
import time
from typing import Dict, List, Tuple
from pathlib import Path
from ib_async import IB, Trade
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
    TARGET_HEDGED_COUNT, FUND_PER_STOCK, ORDER_TIMEOUT, BATCH_SIZE
)

# 【R2】确认阶段双快照间隔：第 1 次快照 + 5s 后第 2 次快照取并集
# （2026-09-21 INFQ 类"撤单后才落账"的空头由第 2 次快照接住）
CONFIRM_UNION_DELAY_SECONDS = 5.0


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
# 阶段 6.2 confirm_short_positions    开盘+20s（或不晚于提交+20s）并发撤残单、权威快照确认空头、写 sell.csv
# 阶段 6.3 execute_hedge_buys         账户2 并发买入对冲（与 1 分钟数据订阅走不同 TCP 连接，双通道并行）


async def pre_submit_sell_orders(
        ib1: IB, candidates: List[StockInfo], batch_size: int = BATCH_SIZE
) -> List[Tuple[StockInfo, Trade]]:
    """
    阶段 6.1: 盘前预下空单 (开盘-60s 调用)

    并发对每只标的：取价 → 计算股数 → 提交市价空单到账户1。
    常规时段(RTH)生效的市价单在盘前提交时，TWS 会将其置为 PreSubmitted
    排队，开盘钟声一响自动送入交易所参与开盘撮合——把最重的
    合约确认/取价/报送 I/O 全部移出 09:30:00 的拥堵窗口。

    注意：盘前取价为昨收/盘前成交价，开盘跳空对"每只 $1000 上限"的
    股数误差极小（0~1~2股），残留缺口由后续调平阶段兜底，在对冲容差范围内。

    Returns:
        List[Tuple[StockInfo, Trade]]: 预提交成功的 (股票, 订单) 列表
    """
    logger = get_logger()
    batch = candidates[:batch_size]
    logger.info(f"📦 [盘前预报送] 并发提交 {len(batch)} 只股票的卖空订单（PreSubmitted，开盘自动撮合）...")

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
    阶段 6.2: 确认空头持仓 (max(开盘+20s, 提交+20s) 调用)

    1) 并发撤销所有未成交的卖空挂单 + 一次 settle
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

    # ---- 1) 并发撤销未成交挂单 ----
    active_trades = [
        t for (_stock, t) in submitted_trades
        if t is not None and t.orderStatus.status not in ('Filled', 'Cancelled', 'Inactive')
    ]
    if active_trades:
        logger.info(f"🛑 [撤单] 并发撤销 {len(active_trades)} 笔未成交卖空挂单，锁定空头头寸...")
        cancel_results = await asyncio.gather(
            *(cancel_order(ib1, t) for t in active_trades), return_exceptions=True
        )
        failed_cancels = sum(1 for r in cancel_results if r is not True and r is not None
                             if isinstance(r, BaseException))
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
    now_str = format_datetime(datetime.datetime.now())
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
        # ==================== 账本写入：事件流优先，旧 CSV 兜底 ====================
        if trade_store is not None:
            trade_store.append_open(
                account='account1', symbol=symbol, price=cost, volume=vol,
                exchange=stock.exchange, industry=stock.industry,
                event_datetime=datetime.datetime.now(),
                strategy='hedge', reason='盘前预挂单成交确认'
            )
        else:
            append_trade_record(TradeRecord(
                datetime=now_str, code=symbol, exchange=stock.exchange,
                industry=stock.industry, action='sell', entry_price=cost,
                vol=vol, total_cost=cost * vol, fund_used=cost * vol
            ), sell_csv_path)
        if runner is not None:
            try:
                runner.on_entry(symbol, 'account1', cost, vol, datetime.datetime.now())
            except Exception as e:
                logger.warning(f"⚠️ {symbol}: 通知策略(account1)失败: {e}")
        logger.info(f"💰 [空头已确认] {symbol}: 实际持有 {vol}股 @ ${cost:.2f}")

    logger.info(f"📊 账户1实际做空成功: {len(confirmed_stocks)} 只标的")
    return confirmed_stocks, pos1


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