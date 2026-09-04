# -*- coding: utf-8 -*-
"""
持仓检查与调平模块
增加"假Cancelled"持仓验证：无论订单状态如何，以实际持仓为准
"""
import asyncio
from typing import Dict
from ib_async import IB
from order import submit_buy_order, submit_sell_order, get_fill_price, get_filled_volume
from monitor import wait_for_trade_completion
from csv_writer import append_trade_record
from models import TradeRecord
from util import format_datetime
from logger import get_logger
import datetime


async def get_positions(ib: IB, account: str) -> Dict[str, dict]:
    """获取指定账户的所有持仓（强制刷新）"""
    logger = get_logger()
    try:
        await ib.reqPositionsAsync()
        await asyncio.sleep(2)
        positions = ib.positions(account)
        result = {}
        for pos in positions:
            symbol = pos.contract.symbol
            result[symbol] = {
                'position': int(pos.position),
                'avgCost': float(pos.avgCost),
                'contract': pos.contract
            }
        logger.info(f"📊 账户 {account}: 持仓 {len(result)} 只股票")
        return result
    except Exception as e:
        logger.error(f"❌ 获取持仓失败: {e}")
        return {}


async def verify_position_after_order(ib: IB, account: str, symbol: str,
                                      expected_side: str, retries: int = 3,
                                      delay: float = 5.0) -> dict:
    """
    订单提交后，通过实际持仓验证是否成功
    解决"假Cancelled"问题：订单报告Cancelled但实际已成交

    Args:
        ib: IB实例
        account: 账户ID
        symbol: 股票代码
        expected_side: 'buy' 或 'sell'（期望的持仓方向）
        retries: 重试次数
        delay: 每次重试间隔（秒）

    Returns:
        dict: {'exists': bool, 'volume': int, 'avg_cost': float}
    """
    logger = get_logger()

    for attempt in range(retries):
        if attempt > 0:
            await asyncio.sleep(delay)

        await ib.reqPositionsAsync()
        await asyncio.sleep(1)

        positions = {p.contract.symbol: p for p in ib.positions(account)}

        if symbol in positions:
            pos = positions[symbol]
            if expected_side == 'buy' and pos.position > 0:
                return {
                    'exists': True,
                    'volume': int(pos.position),
                    'avg_cost': float(pos.avgCost)
                }
            elif expected_side == 'sell' and pos.position < 0:
                return {
                    'exists': True,
                    'volume': abs(int(pos.position)),
                    'avg_cost': float(pos.avgCost)
                }

    return {'exists': False, 'volume': 0, 'avg_cost': 0.0}


async def reconcile_positions(
        ib1: IB, ib2: IB,
        account1: str, account2: str,
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None
) -> None:
    """
    调平两个账户的持仓
    关键改进：无论订单状态如何，通过实际持仓验证是否成功
    """
    logger = get_logger()
    logger.info(f"\n{'=' * 50}")
    logger.info("开始持仓调平检查")

    positions1 = await get_positions(ib1, account1)
    positions2 = await get_positions(ib2, account2)

    adjustments = []

    # 检查1: 账户1的sell持仓是否有对应的账户2 buy持仓
    for symbol, pos1 in positions1.items():
        if pos1['position'] < 0:
            sell_vol = abs(pos1['position'])
            pos2 = positions2.get(symbol, {'position': 0})
            buy_vol = max(0, pos2['position'])

            if buy_vol < sell_vol:
                diff = sell_vol - buy_vol
                adjustments.append({
                    'symbol': symbol,
                    'action': 'buy',
                    'ib': ib2,
                    'account': account2,
                    'volume': diff,
                    'sell_price': pos1['avgCost'],
                    'reason': f'账户1有{sell_vol}股sell，账户2仅{buy_vol}股buy，需补买{diff}股'
                })

    # 检查2: 账户2的buy持仓是否有对应的账户1 sell持仓
    for symbol, pos2 in positions2.items():
        if pos2['position'] > 0:
            buy_vol = pos2['position']
            pos1 = positions1.get(symbol, {'position': 0})
            sell_vol = abs(min(0, pos1['position']))

            if sell_vol < buy_vol:
                diff = buy_vol - sell_vol
                adjustments.append({
                    'symbol': symbol,
                    'action': 'sell',
                    'ib': ib2,
                    'account': account2,
                    'volume': diff,
                    'buy_price': pos2['avgCost'],
                    'reason': f'账户2有{buy_vol}股buy，账户1仅{sell_vol}股sell，需卖出{diff}股'
                })

    if not adjustments:
        logger.info("✅ 持仓已完全调平，无需调整")
        return

    logger.info(f"⚠️ 发现 {len(adjustments)} 个持仓不一致，开始调平...")

    success_count = 0
    for adj in adjustments:
        symbol = adj['symbol']
        action = adj['action']
        ib = adj['ib']
        account = adj['account']
        volume = adj['volume']

        logger.info(f"🔧 调平: {symbol} | {action} {volume}股 | {adj['reason']}")

        # 提交订单
        if action == 'buy':
            trade = await submit_buy_order(ib, symbol, volume)
        else:
            trade = await submit_sell_order(ib, symbol, volume)

        if not trade:
            logger.error(f"❌ 调平订单提交失败: {symbol}")
            continue

        # 等待订单完成
        status = await wait_for_trade_completion(trade, timeout_seconds=30)

        # ==================== 关键改进：持仓验证 ====================
        # 无论订单状态如何，都通过实际持仓验证
        if status == 'Filled':
            # 订单报告成功，直接使用
            fill_price = get_fill_price(trade)
            fill_vol = get_filled_volume(trade)
            verified = True
        else:
            # 订单报告失败（Cancelled/Timeout），验证实际持仓
            logger.info(f"🔍 {symbol}: 订单状态 {status}，验证实际持仓...")
            pos_info = await verify_position_after_order(
                ib, account, symbol, action, retries=3, delay=5.0
            )

            if pos_info['exists']:
                # 实际持仓存在 → 订单确实成交了（假Cancelled）
                fill_price = pos_info['avg_cost']
                fill_vol = pos_info['volume']
                verified = True
                logger.info(
                    f"🔄 {symbol}: 订单报告 {status}，但实际已成交！"
                    f"持仓 {fill_vol}股 @ ${fill_price:.2f}"
                )
            else:
                # 确实没有持仓 → 调平失败
                logger.error(f"❌ 调平失败: {symbol} {action} {volume}股 (状态: {status})")
                verified = False

        if verified:
            success_count += 1
            logger.info(f"✅ 调平成功: {symbol} {action} {fill_vol}股 @ ${fill_price:.2f}")

            # ==================== 补写CSV ====================
            now_str = format_datetime(datetime.datetime.now())
            fill_cost = fill_price * fill_vol

            # 获取股票信息
            exchange = ''
            industry = ''
            if stock_info_map and symbol in stock_info_map:
                exchange = stock_info_map[symbol].exchange
                industry = stock_info_map[symbol].industry

            if action == 'buy' and buy_csv_path:
                # 补买成功 → 写入 buy.csv
                append_trade_record(TradeRecord(
                    datetime=now_str, code=symbol,
                    exchange=exchange, industry=industry,
                    action='buy', entry_price=fill_price,
                    vol=fill_vol, total_cost=fill_cost, fund_used=fill_cost
                ), buy_csv_path)
                logger.info(f"📝 补写 buy.csv: {symbol} {fill_vol}股 @ ${fill_price:.2f}")

                # 同时检查 sell.csv 中是否有该股票的记录，如果没有则补写
                if sell_csv_path and 'sell_price' in adj:
                    import pandas as pd
                    try:
                        df_sell = pd.read_csv(sell_csv_path)
                        if symbol not in df_sell['code'].values:
                            sell_price = adj['sell_price']
                            sell_cost = sell_price * fill_vol
                            append_trade_record(TradeRecord(
                                datetime=now_str, code=symbol,
                                exchange=exchange, industry=industry,
                                action='sell', entry_price=sell_price,
                                vol=fill_vol, total_cost=sell_cost, fund_used=sell_cost
                            ), sell_csv_path)
                            logger.info(f"📝 补写 sell.csv: {symbol} {fill_vol}股 @ ${sell_price:.2f}")
                    except Exception as e:
                        logger.warning(f"⚠️ 补写 sell.csv 失败: {e}")

            elif action == 'sell' and sell_csv_path:
                append_trade_record(TradeRecord(
                    datetime=now_str, code=symbol,
                    exchange=exchange, industry=industry,
                    action='sell', entry_price=fill_price,
                    vol=fill_vol, total_cost=fill_cost, fund_used=fill_cost
                ), sell_csv_path)
                logger.info(f"📝 补写 sell.csv: {symbol} {fill_vol}股 @ ${fill_price:.2f}")

    logger.info(f"调平完成: {success_count}/{len(adjustments)} 成功")

    # 调平后持仓概览
    logger.info("\n📊 调平后持仓概览:")
    positions1_after = await get_positions(ib1, account1)
    positions2_after = await get_positions(ib2, account2)

    for symbol, pos in positions1_after.items():
        direction = "SELL" if pos['position'] < 0 else "BUY"
        logger.info(f"  账户1 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")

    for symbol, pos in positions2_after.items():
        direction = "BUY" if pos['position'] > 0 else "SELL"
        logger.info(f"  账户2 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")