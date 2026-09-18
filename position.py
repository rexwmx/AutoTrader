# -*- coding: utf-8 -*-
"""
持仓检查与调平模块
增加"假Cancelled"持仓验证：无论订单状态如何，以实际持仓为准
"""
import asyncio
from pathlib import Path
from typing import Dict, Optional
from ib_async import IB
from order import (submit_buy_order, submit_sell_order, get_fill_price,
                   get_filled_volume, cancel_order)
from monitor import wait_for_trade_completion
from csv_writer import append_trade_record
from models import TradeRecord
from util import format_datetime
from logger import get_logger
import datetime


# ==================== 持仓快照（决策用权威数据源） ====================
# 背景：ib_async 2.x 中 ib.positions() 是"长寿命事件缓存"：
#   - 已平掉的持仓，若 TWS 未显式推送 0 则不会清理 --> 残留幽灵条目；
#   - 同一 IB 实例连续两次 reqPositionsAsync 会互相覆盖 future --> 先等待的任务挂死。
# 因此一切用于决策的持仓，统一使用 reqPositionsAsync 的返回列表
# （TWS 发 END_OF_POSITIONS 时的完整集合），并按实例锁串行化请求。
_POS_LOCKS: Dict[int, asyncio.Lock] = {}


def _lock_for(ib: IB) -> asyncio.Lock:
    """获取按 IB 实例的持仓请求锁（防止 'positions' future 被并发覆盖）"""
    return _POS_LOCKS.setdefault(id(ib), asyncio.Lock())


async def get_positions(ib: IB, account: str = "") -> Dict[str, dict]:
    """
    获取指定账户的持仓快照（权威）

    使用 reqPositionsAsync 返回的持仓列表（TWS 结束推送时的全集），
    取代旧的"请求 + 固定 sleep + 读缓存"模式：
    - ib.positions() 缓存可能残留已平仓标的（幽灵条目）；
    - 半批读取（推送未收齐）会导致调平缺口误判。

    Args:
        ib: IB实例
        account: 账户ID，空字符串表示不按账户过滤

    Returns:
        Dict: {symbol: {'position': int, 'avgCost': float, 'contract': Contract}}
        失败或无持仓时返回空字典
    """
    logger = get_logger()
    try:
        async with _lock_for(ib):
            latest = await ib.reqPositionsAsync()

        result = {}
        for p in (latest or []):
            if account and getattr(p, 'account', '') != account:
                continue
            if not p.position:
                continue
            result[p.contract.symbol] = {
                'position': int(p.position),
                'avgCost': float(p.avgCost),
                'contract': p.contract
            }
        logger.info(f"📊 账户 {account}: 持仓快照 {len(result)} 只")
        return result
    except Exception as e:
        logger.error(f"❌ 获取持仓失败: {e}")
        return {}


async def get_positions_strict(ib: IB, account: str = "") -> Dict[str, dict]:
    """
    持仓快照（权威、失败严格版）

    与 get_positions 语义相同（同一实例锁串行化、同一账户过滤），
    区别：**获取失败时向上抛出异常，而不是静默返回空字典**。

    必须用于"取数失败不能被当成 0 持仓"的决策点
    （典型：收市前强制平仓的 flat 判定 —— TWS 抖动绝不能被误读为"已全部清仓"，
    否则会出现 CSV 已标记平仓而账户仍有持仓的假成功）。
    """
    async with _lock_for(ib):
        latest = await ib.reqPositionsAsync()

    result = {}
    for p in (latest or []):
        if account and getattr(p, 'account', '') != account:
            continue
        if not p.position:
            continue
        result[p.contract.symbol] = {
            'position': int(p.position),
            'avgCost': float(p.avgCost),
            'contract': p.contract
        }
    return result


class PositionSnapshotError(RuntimeError):
    """持仓快照获取失败（已重试）——绝不能被当作"无持仓"处理"""


async def get_positions_strict_with_retry(ib: IB, account: str = "",
                                          retries: int = 3, delay: float = 2.0) -> Dict[str, dict]:
    """
    权威持仓快照 + 重试（严格版语义）

    背景：get_positions 在请求失败时静默返回空字典，若被用于"确认空头持仓"
    这类决策点，会把"取数失败"误判成"无持仓"，进而误终止/误下单。
    本函数基于 get_positions_strict（失败抛异常）做 retries 次重取：
    - 成功（即便空字典 = 确实无持仓，是事实）→ 直接返回；
    - 连续失败 → 抛出 PositionSnapshotError，由调用方按"快照不可用"处置（终止并要求人工核查）。
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return await get_positions_strict(ib, account)
        except Exception as e:
            last_err = e
            get_logger().warning(f"⚠️ 持仓快照获取失败（第 {attempt}/{retries} 次）: {e}")
            if attempt < retries:
                await asyncio.sleep(delay)
    raise PositionSnapshotError(
        f"持仓快照连续 {retries} 次获取失败: {last_err} —— 无法区分'无持仓'与'取数失败'"
    )


async def verify_position_after_order(ib: IB, account: str, symbol: str,
                                      expected_side: str, retries: int = 3,
                                      delay: float = 5.0) -> dict:
    """
    订单提交后，基于权威持仓快照验证是否实际成交（解决"假Cancelled"问题）

    旧版本读取 ib.positions() 长寿命缓存，可能因幽灵条目/半批读取误判；
    现在统一以 reqPositionsAsync 的返回值为准。

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

        try:
            positions = await get_positions(ib, account)
            hit = positions.get(symbol)
            if hit:
                if expected_side == 'buy' and hit['position'] > 0:
                    return {
                        'exists': True,
                        'volume': hit['position'],
                        'avg_cost': hit['avgCost']
                    }
                elif expected_side == 'sell' and hit['position'] < 0:
                    return {
                        'exists': True,
                        'volume': abs(hit['position']),
                        'avg_cost': hit['avgCost']
                    }
        except Exception as e:
            logger.debug(f"🔍 {symbol}: 持仓验证异常: {e}")

    return {'exists': False, 'volume': 0, 'avg_cost': 0.0}


RECONCILE_MAX_ROUNDS = 3                 # 定点调平最大轮数
RECONCILE_SETTLE_LONG_SECONDS = 10       # 每轮前 settle 等待：上一轮成交"未持仓级确认"（持仓落账未保证）
RECONCILE_SETTLE_SHORT_SECONDS = 3       # 每轮前 settle 等待：上一轮全部持仓级确认（快照已反映成交）
RECONCILE_ROUND_SETTLE_SECONDS = RECONCILE_SETTLE_LONG_SECONDS  # 兼容旧名
RECONCILE_ORDER_TIMEOUT = 20             # 每笔调平单等待(秒)：20s 未成交 → 撤单 + 权威快照核实
                                         # （原30s且不撤单：残留单延迟成交是 9/14 OKLO 84股搅动的直接推手）


def backfill_missing_open_records(
        positions1: Dict[str, dict],
        positions2: Dict[str, dict],
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None,
) -> None:
    """
    CSV开仓记录对账补写（终态持仓 → CSV）

    修复 9.8 PL 事故根因：建仓时对冲单被"假 Cancelled"误判为未成交（sell_only），
    对应 openCSV 行未写入；随后 TWS 延迟成交，两账户终态恰好对称 ——
    调平只看"两账户间对称性"（_compute_adjustments 差额为 0，不发单），
    而原有补写钩子挂在"调平单成交"之后，于是**永远没有机会执行**，CSV 单边缺行。

    本函数以权威终态快照为准逐边检查：
      - 账户1 某标有空头，但 sell.csv 无该代码开仓行 → 补写（入场价 = 账户1 avgCost）
      - 账户2 某标的有多头，但 buy.csv 无该代码开仓行 → 补写（入场价 = 账户2 avgCost）
    快照获取失败（空字典）时不做任何补写，保证不误写。
    """
    logger = get_logger()

    def _open_row_codes(path: Path, action: str) -> set:
        if not path or not Path(path).exists():
            return set()
        try:
            import pandas as pd
            df = pd.read_csv(path)
            if df.empty:
                return set()
            return set(df.loc[df['action'] == action, 'code'])
        except Exception as e:
            logger.warning(f"⚠️ 读取 {Path(path).name} 开仓代码失败: {e}")
            return set()

    def _info(symbol: str):
        if stock_info_map and symbol in stock_info_map:
            s = stock_info_map[symbol]
            return s.exchange, s.industry
        return '', ''

    sell_codes = _open_row_codes(sell_csv_path, 'sell')
    buy_codes = _open_row_codes(buy_csv_path, 'buy')

    # 账户1 空头 → sell.csv 应有开仓行
    for symbol, pos1 in positions1.items():
        if pos1['position'] >= 0 or not sell_csv_path:
            continue
        if symbol in sell_codes:
            continue
        vol = abs(int(pos1['position']))
        price = float(pos1['avgCost'])
        exchange, industry = _info(symbol)
        cost = price * vol
        now_str = format_datetime(datetime.datetime.now())
        append_trade_record(TradeRecord(
            datetime=now_str, code=symbol,
            exchange=exchange, industry=industry,
            action='sell', entry_price=price,
            vol=vol, total_cost=cost, fund_used=cost
        ), sell_csv_path)
        logger.critical(
            f"🧾 开仓记录缺失补写 (sell.csv): {symbol} {vol}股 @ ${price:.2f} "
            f"（来源: 账户1终态持仓快照 avgCost；建仓阶段该单曾被误判为未成交）"
        )

    # 账户2 多头 → buy.csv 应有开仓行
    for symbol, pos2 in positions2.items():
        if pos2['position'] <= 0 or not buy_csv_path:
            continue
        if symbol in buy_codes:
            continue
        vol = int(pos2['position'])
        price = float(pos2['avgCost'])
        exchange, industry = _info(symbol)
        cost = price * vol
        now_str = format_datetime(datetime.datetime.now())
        append_trade_record(TradeRecord(
            datetime=now_str, code=symbol,
            exchange=exchange, industry=industry,
            action='buy', entry_price=price,
            vol=vol, total_cost=cost, fund_used=cost
        ), buy_csv_path)
        logger.critical(
            f"🧾 开仓记录缺失补写 (buy.csv): {symbol} {vol}股 @ ${price:.2f} "
            f"（来源: 账户2终态持仓快照 avgCost；建仓阶段该单曾被误判为未成交）"
        )


def _compute_adjustments(positions1: Dict[str, dict],
                         positions2: Dict[str, dict],
                         ib2: IB, account2: str) -> list:
    """
    依据两账户最新快照计算调平订单列表

    Returns:
        list: 调平订单 dict 列表；空列表 = 已调平
    """
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

    return adjustments


async def reconcile_positions(
        ib1: IB, ib2: IB,
        account1: str, account2: str,
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None
) -> None:
    """
    调平两个账户的持仓

    关键改进（修复"两账户有时不符"的竞态）：
    - 每轮都从权威持仓快照（reqPositionsAsync 返回值）计算缺口，
      不再依赖 ib.positions() 长寿命缓存（幽灵条目/半批读取会导致误判）；
    - 多轮定点调整：本轮执行过调平单，则下一轮重新取最新快照复核，
      直至完全对称或达到最大轮数；
    - 收尾终态确认：仍有不一致则记 CRITICAL，显式要求人工处理。
    """
    logger = get_logger()
    logger.info(f"\n{'=' * 50}")
    logger.info(f"开始持仓调平检查 (最多{RECONCILE_MAX_ROUNDS}轮定点调整)")

    success_count = 0
    total_adjustments = 0
    adjusted_any = False
    # 下一轮前的 settle 等待（每轮结束后依据"持仓级确认"更新：
    # 全部持仓级确认 → 短等待；任一缺失 → 恢复长等待）
    next_settle = RECONCILE_SETTLE_LONG_SECONDS

    for round_no in range(1, RECONCILE_MAX_ROUNDS + 1):
        if round_no > 1:
            # 让 TWS 完成上一轮调平单的持仓更新
            if next_settle < RECONCILE_SETTLE_LONG_SECONDS:
                logger.info(f"⏱️ 上一轮成交均已持仓级确认，settle 等待缩短为 {next_settle} 秒")
            await asyncio.sleep(next_settle)
            next_settle = RECONCILE_SETTLE_LONG_SECONDS

        positions1 = await get_positions(ib1, account1)
        positions2 = await get_positions(ib2, account2)

        adjustments = _compute_adjustments(positions1, positions2, ib2, account2)

        if not adjustments:
            if adjusted_any:
                logger.info(f"✅ 第{round_no}轮确认: 持仓已一致")
            else:
                logger.info("✅ 持仓已完全调平，无需调整")
            break

        logger.info(f"⚠️ 第{round_no}轮: 发现 {len(adjustments)} 个持仓不一致，开始调平...")
        total_adjustments += len(adjustments)
        adjusted_any = True

        # 本轮"持仓级确认"标记：全部调平单都在持仓快照中得到验证时，下一轮可用短 settle
        round_position_confirmed = True

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

            # 等待订单完成（20s；未成交 → 撤单 + 权威快照核实）
            status = await wait_for_trade_completion(trade, RECONCILE_ORDER_TIMEOUT)

            # ==================== 关键改进：持仓验证 ====================
            # 无论订单状态如何，都通过实际持仓验证；只有"持仓级确认"的成交
            # 才允许下一轮使用短 settle（9/14 OKLO 84股延迟成交搅动的教训）
            position_seen = False
            if status == 'Filled':
                # 订单报告成功，直接使用
                fill_price = get_fill_price(trade)
                fill_vol = get_filled_volume(trade)
                verified = True
                if action == 'buy':
                    # Paper 账户可能"先报成交、后更新持仓"：只有持仓级看到的成交
                    # 才视为落定，否则下一轮恢复长 settle，防止旧快照误判二次补买
                    try:
                        pos_chk = await verify_position_after_order(
                            ib, account, symbol, 'buy', retries=2, delay=2.0
                        )
                        position_seen = bool(pos_chk['exists'])
                    except Exception as e:
                        logger.debug(f"🔍 {symbol}: 持仓复核异常: {e}")
                    if not position_seen:
                        logger.info(f"🔍 {symbol}: 订单已成交但持仓尚未落账，下一轮恢复长 settle 等待")
            else:
                # 订单报告未成交：先撤掉残留挂单（防止其延迟成交破坏头寸），再验证实际持仓
                await cancel_order(ib, trade)
                logger.info(f"🔍 {symbol}: 订单状态 {status}（已撤单），验证实际持仓...")
                pos_info = await verify_position_after_order(
                    ib, account, symbol, action, retries=3, delay=5.0
                )

                if pos_info['exists']:
                    # 实际持仓存在 → 订单确实成交了（假Cancelled）
                    fill_price = pos_info['avg_cost']
                    fill_vol = pos_info['volume']
                    verified = True
                    position_seen = True
                    logger.info(
                        f"🔄 {symbol}: 订单报告 {status}，但实际已成交！"
                        f"持仓 {fill_vol}股 @ ${fill_price:.2f}"
                    )
                else:
                    # 确实没有持仓 → 调平失败
                    logger.error(f"❌ 调平失败: {symbol} {action} {volume}股 (状态: {status}，持仓未见)")
                    verified = False

            # 记录"持仓级确认"，供下一轮 settle 时长按安全级别选择
            round_position_confirmed = round_position_confirmed and position_seen

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


        # 本轮成交全部持仓级确认 → 下一轮可用短 settle；否则保持长 settle（安全兜底）
        if round_position_confirmed:
            next_settle = RECONCILE_SETTLE_SHORT_SECONDS

    # ==================== 终态确认（权威快照） ====================
    positions1_final = await get_positions(ib1, account1)
    positions2_final = await get_positions(ib2, account2)

    # ==================== CSV开仓记录对账补写（对称持仓盲点修复） ====================
    # 两账户对称≠CSV完整：缺行标的不会触发调平单，只能由这里兜底
    # （仅当传入 CSV 路径时生效；路径为 None 则安全跳过）
    backfill_missing_open_records(
        positions1_final, positions2_final,
        sell_csv_path, buy_csv_path, stock_info_map
    )

    remaining = _compute_adjustments(positions1_final, positions2_final, ib2, account2)

    if remaining:
        details = '; '.join(a['reason'] for a in remaining)
        logger.critical(f"🚨 调平后仍存在不一致（{len(remaining)}项）: {details} —— 请人工核对TWS持仓")
    else:
        logger.info("✅ 终态确认通过: 两账户持仓完全对称")

    logger.info(f"调平结束: 成功 {success_count}/{total_adjustments}")

    # 调平后持仓概览
    logger.info("\n📊 调平后持仓概览:")
    for symbol, pos in positions1_final.items():
        direction = "SELL" if pos['position'] < 0 else "BUY"
        logger.info(f"  账户1 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")

    for symbol, pos in positions2_final.items():
        direction = "BUY" if pos['position'] > 0 else "SELL"
        logger.info(f"  账户2 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")
