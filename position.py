# -*- coding: utf-8 -*-
"""
持仓检查与调平模块
增加"假Cancelled"持仓验证：无论订单状态如何，以实际持仓为准
"""
import asyncio
import time
from pathlib import Path
from typing import Dict, List, Optional
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


# ==================== R1 建仓收敛循环参数（取代 旧"阶段7 双通道 + 阶段7.5 三轮调平"） ====================
CONVERGE_MAX_ROUNDS = 5                  # 收敛循环最大轮数（≤5 轮）
CONVERGE_TOTAL_BUDGET_SECONDS = 90.0     # 收敛总时间预算(秒)：下一轮开始前已超 → 收尾补记（快，但不是以账实一致为代价）
CONVERGE_ROUND_SETTLE_SECONDS = 5        # 未收敛 → 隔 5s 回到取快照（计划 R1 第7步）
CONVERGE_ORDER_TIMEOUT = 20              # 每笔调平单等待(秒)：Paper 语义下是安全边界不是成本（保持 20s，R7）
                                        # （20s 未成交 → 撤单 + 权威快照核实；原30s且不撤单：残留单延迟成交
                                        #   是 9/14 OKLO 84股搅动的直接推手）
CONVERGE_VERIFY_GRACE_SECONDS = 5        # R4: 并行提交+终态判定后 grace 5s → 一次 reqPositionsAsync 按标的核验
CONVERGE_VERIFY_RETRIES = 2              # R4: 未命中（成交未落账）再单只重试
CONVERGE_VERIFY_RETRY_DELAY = 2.0        # R4: 重试间隔 2s（替代旧逐只 3×5s；复核延迟 5s→2s #5）
CONVERGE_GIVEUP_AFTER = 3                # 同一标的缺口单连续 3 轮失败 → 该标的停止重试（收尾终态 CRITICAL + 幂等补记兜底）
CANCEL_OBSERVE_SECONDS = 3               # R7: 接受"未成交"前保留 ≥3s 观察窗（今日 KEEL 通道1 的教训）
# 兼容旧引用名
RECONCILE_MAX_ROUNDS = CONVERGE_MAX_ROUNDS
RECONCILE_ORDER_TIMEOUT = CONVERGE_ORDER_TIMEOUT


def backfill_missing_open_records(
        positions1: Dict[str, dict],
        positions2: Dict[str, dict],
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None,
        trade_store=None,
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

    # ==================== 事件流路径（trade_store 可用） ====================
    # P0 统一口径：判重必须是「实际持仓 − 账上未平批次」的数量差额，而非"是否曾有开仓事件"——
    # 曾有 lot 但被超额平仓、账户里仍留着延迟成交持仓（2026-09-21 KEEL 型）→ 差额仍需补；
    # 已入账覆盖实际 → 差额为 0，绝不新增 lot。所有补记（阶段8延迟补齐/终态补平/收敛收尾）
    # 统一经由 TradeStore.ensure_cover，天然幂等、可重复调用。
    if trade_store is not None:
        _booked = lambda account, sym: sum(int(l['remaining'])
                                           for l in trade_store.get_open_lots(account, sym))

        def _cover(account: str, symbol: str, pos: dict) -> None:
            actual = abs(int(pos['position']))
            booked = _booked(account, symbol)
            gap = trade_store.ensure_cover(
                account=account, symbol=symbol, target_vol=actual,
                price=float(pos['avgCost']),
                exchange=(stock_info_map.get(symbol).exchange
                          if stock_info_map and symbol in stock_info_map else ''),
                industry=(stock_info_map.get(symbol).industry
                          if stock_info_map and symbol in stock_info_map else ''),
                event_datetime=datetime.datetime.now(),
                strategy='reconcile', reason='终态对账: 开仓记录缺失差额补写'
            )
            if gap > 0:
                logger.critical(
                    f"🧾 开仓记录缺失补写 ({account}): {symbol} {gap}股 @ ${float(pos['avgCost']):.2f} "
                    f"（来源: {account}终态持仓 {actual}股 − 账上未平批次 {booked}股；"
                    f"建仓阶段该单曾被误判为未成交或延迟成交落账）"
                )

        for symbol, pos1 in positions1.items():
            if pos1['position'] >= 0:
                continue
            _cover('account1', symbol, pos1)
        for symbol, pos2 in positions2.items():
            if pos2['position'] <= 0:
                continue
            _cover('account2', symbol, pos2)
        return

    # ==================== 旧 CSV 兜底路径（trade_store 不可用） ====================
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


# ==================== R1 收敛循环辅助函数 ====================

def _filled_volume_now(trade) -> int:
    """当前时刻重读订单 filled（filled>0 > fills 列表 > status 的优先级，见 order.get_filled_volume）"""
    try:
        return int(get_filled_volume(trade) or 0)
    except Exception:
        return 0


async def _settle_trade(ib: IB, trade, timeout: int = CONVERGE_ORDER_TIMEOUT) -> dict:
    """R7 收敛循环单订单终态判定

    判定优先级（monitor.wait_for_trade_completion 内已实现 filled>0 > fills 列表 > status）：
    - filled 在 Paper 中常早于持仓落账，甚至早于状态翻转，因此一切结论以成交事实为准；
    - 超时/Cancelled 撤单前：先读一次原单 filled，>0 直接按成交（今日 MSTR 场景，撤单动作可省）；
    - 接受"未成交"前：保留 ≥3s 观察窗（今日 KEEL 通道1 的教训：即时判负 → 下一轮双买）。

    注意：wait 返回 'Cancelled' 表示 monitor 内部已做过 10s 延迟确认且 filled=0，
    此处不再回读订单 filled（持仓级真相由批量核验步骤裁决），避免 Paper 假状态误判。

    Returns:
        {'result': 'filled'|'not_filled', 'volume': int, 'price': float, 'reported': str}
    """
    logger = get_logger()
    symbol = str(getattr(getattr(trade, 'contract', None), 'symbol', '?'))
    status = await wait_for_trade_completion(trade, timeout)

    if status == 'Filled':
        return {'result': 'filled', 'volume': max(_filled_volume_now(trade), 1),
                'price': get_fill_price(trade), 'reported': status}

    if status == 'Timeout':
        # R7(MSTR)：超时撤单前先读一次原单 filled；>0 → 直接按成交，撤单动作可省
        filled_now = _filled_volume_now(trade)
        if filled_now > 0:
            logger.info(f"🔄 {symbol}: 订单超时，但重读原单 filled={filled_now} → 按成交处理（撤单可省）")
            return {'result': 'filled', 'volume': filled_now,
                    'price': get_fill_price(trade), 'reported': status}
        logger.info(f"🚫 {symbol}: 超时且无成交，撤销残留挂单...")
        await cancel_order(ib, trade)
        # R7：接受"未成交"前的观察窗（Paper 延迟成交可能在此期间到达）
        await asyncio.sleep(CANCEL_OBSERVE_SECONDS)
        filled_obs = _filled_volume_now(trade)
        if filled_obs > 0:
            logger.info(f"🔄 {symbol}: 撤单后 {CANCEL_OBSERVE_SECONDS}s 观察窗外 filled={filled_obs} 到达 → 按成交处理")
            return {'result': 'filled', 'volume': filled_obs,
                    'price': get_fill_price(trade), 'reported': status}
        logger.info(f"🔍 {symbol}: 撤单 + {CANCEL_OBSERVE_SECONDS}s 观察窗后仍无成交 → 本轮记未成交")
        return {'result': 'not_filled', 'volume': 0, 'price': 0.0, 'reported': status}

    # 'Cancelled' / 'Inactive' / 'ApiCancelled'：monitor 已含 10s 延迟确认且 filled=0
    logger.info(f"🔍 {symbol}: 订单终态 {status}（无成交）→ 本轮记未成交（持仓真相由批量核验裁决）")
    return {'result': 'not_filled', 'volume': 0, 'price': 0.0, 'reported': status}


async def _batch_verify(ib: IB, account: str, symbols: List[str],
                        expect_booking: Optional[Dict[str, int]] = None) -> Dict[str, dict]:
    """R4 批量核验（替代旧"逐只 3×5s"）

    并行提交+终态判定后：grace 5s 等 TWS 落账 → **一次** reqPositionsAsync 按标的核验全部；
    未命中（该标的成交尚未落账）→ 按标的重试，最多 2 次 × 2s 间隔。

    Returns:
        {symbol: {'position': int, 'avgCost': float}}（缺持仓 → position=0）
    """
    logger = get_logger()
    want = expect_booking or {}

    async def _snap() -> Dict[str, dict]:
        try:
            return await get_positions(ib, account)
        except Exception as e:
            logger.debug(f"🔍 批量核验取快照异常: {e}")
            return {}

    await asyncio.sleep(CONVERGE_VERIFY_GRACE_SECONDS)
    snap = await _snap()
    for attempt in range(1, CONVERGE_VERIFY_RETRIES + 1):
        missing = [s for s, vol in want.items()
                   if vol > 0 and (snap.get(s) or {}).get('position', 0) < vol]
        if not missing:
            break
        logger.debug(f"🔍 批量核验第 {attempt} 次重试，未落账标的: {missing}")
        await asyncio.sleep(CONVERGE_VERIFY_RETRY_DELAY)
        snap = await _snap()

    out: Dict[str, dict] = {}
    for s in symbols:
        p = snap.get(s)
        if p and p.get('position'):
            out[s] = {'position': int(p['position']), 'avgCost': float(p['avgCost'])}
        else:
            out[s] = {'position': 0, 'avgCost': 0.0}
    return out


def _sym_target_short(snap1: Dict[str, dict], symbol: str) -> int:
    p = snap1.get(symbol)
    if p and p['position'] < 0:
        return abs(int(p['position']))
    return 0


def _sym_long(snap2: Dict[str, dict], symbol: str) -> int:
    p = snap2.get(symbol)
    if p and p['position'] > 0:
        return int(p['position'])
    return 0


def _book_open_leg(store, account: str, symbol: str, target_vol: int, price: float,
                   stock_info_map, reason: str) -> int:
    """P0 统一口径：账上未平批次不足时，差额幂等补记开仓（重复调用不新增 lot）"""
    if store is None or target_vol <= 0:
        return 0
    try:
        price = float(price)
    except Exception:
        return 0
    if price <= 0:
        return 0
    exchange, industry = ('', '')
    if stock_info_map and symbol in stock_info_map:
        exchange, industry = stock_info_map[symbol].exchange, stock_info_map[symbol].industry
    gap = store.ensure_cover(
        account=account, symbol=symbol, target_vol=target_vol, price=price,
        exchange=exchange, industry=industry,
        event_datetime=datetime.datetime.now(),
        strategy='reconcile', reason=reason
    )
    if gap > 0:
        get_logger().info(f"🧾 补记 {account} 开仓账: {symbol} {gap}股 @ ${price:.2f}（{reason}）")
    return gap


async def reconcile_positions(
        ib1: IB, ib2: IB,
        account1: str, account2: str,
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None,
        trade_store=None,
        runner=None, recorder=None, confirmed_codes=None
) -> dict:
    """
    R1 统一"建仓收敛循环"（取代 旧"阶段7 双通道买入 → 阶段7.5 三轮调平"）

    循环（≤5 轮，总时间预算 90s）：
      1. 取两账户最新权威快照（失败 → 本轮跳过，不判缺口）
      2. 缺口 = max(0, 账户1空头 − 账户2多头) → 买 gap；
         超额 = max(0, 账户2多头 − 账户1空头) → 卖 excess
      3. 无缺口且无在途单 → 收敛退出
      4. 并行提交全部缺口单（不同标的互不干扰）
      5. 逐单等待（保留 20s 上限）+ 终态判定（R7：filled 优先）
      6. 一次批量快照核验全部标的；
         【同轮超平】实际持仓 > 目标 → 当轮立即卖出超额（KEEL 第3轮被吸收进本轮）
      7. 未收敛 → 隔 5s 回到 1（按最新快照再算）

    安全基线：
    - R7：订单终态判定 filled>0 > fills 列表 > status；超时撤单前先读原单 filled；
      同标的再补买前必做"最新快照 + 原单 filled=0"双确认（filled 已确认但未落账的
      量以 carried 计入下一轮缺口，绝不双买）；接受未成交前有 ≥3s 观察窗。
    - P0：所有账本写入走 TradeStore.ensure_cover 差额幂等补记，重复调用不新增 lot。
    - R5：每轮只把本轮实际在调的标的加入 runner 暂停集合，轮末解除并重放排队信号。

    收尾（R10，吸收旧 阶段7.6 延迟成交补齐 + 阶段8 补记）：
    终态权威快照 → P0 差额幂等补记 → 延迟成交标的补订阅/入场登记 → 终态确认
    （仍不一致 → CRITICAL 人工核对）。
    """
    logger = get_logger()
    logger.info(f"\n{'=' * 50}")
    logger.info(f"开始建仓收敛循环 (最多{CONVERGE_MAX_ROUNDS}轮 / 总预算{CONVERGE_TOTAL_BUDGET_SECONDS:.0f}s)")

    t_start = time.monotonic()
    failed: Dict[str, int] = {}        # symbol -> 连续失败轮数（≥3 停止重试该标的）
    # symbol -> 「本轮已成交、持仓落账在途」时该标的账户2多头的**预期总量下限**（floor）。
    # 下一轮缺口计算用 max(最新快照, 预期下限)：既防"filled 早于落账"的重复补买，
    # 又不会与已落账部分重复计数（max 而非相加）。
    pending_total: Dict[str, int] = {}
    success_count = 0
    total_adjustments = 0
    converged = False

    def _long_eff(snap2, symbol):
        """缺口计算用的账户2有效多头：max(最新快照, 在途落账的预期下限)"""
        return max(_sym_long(snap2, symbol), int(pending_total.get(symbol, 0)))

    # =================================================================
    # 主循环
    # =================================================================
    for round_no in range(1, CONVERGE_MAX_ROUNDS + 1):
        if round_no > 1 and (time.monotonic() - t_start) >= CONVERGE_TOTAL_BUDGET_SECONDS:
            logger.warning(f"⏱️ 收敛循环达到总时间预算 {CONVERGE_TOTAL_BUDGET_SECONDS:.0f}s — 退出循环进入收尾补记")
            break

        # ---------- 1. 两账户最新权威快照（失败 → 本轮跳过，不判缺口） ----------
        try:
            snap1 = await get_positions_strict_with_retry(ib1, account1)
            snap2 = await get_positions_strict_with_retry(ib2, account2)
        except PositionSnapshotError as e:
            logger.warning(f"⚠️ 第{round_no}轮: 权威快照获取失败 → 本轮跳过，不判缺口: {e}")
            await asyncio.sleep(CONVERGE_ROUND_SETTLE_SECONDS)
            continue

        # 失败期间已延迟落账的标的 → 恢复（不再视为失败）
        for s in list(failed.keys()):
            if _sym_target_short(snap1, s) == _long_eff(snap2, s):
                failed.pop(s, None)
                logger.info(f"♻️ {s}: 缺口已消除（延迟成交落账）→ 移出失败名单")

        # ---------- 2. 缺口 / 超额（跳过已放弃标的；含在途落账下限修正） ----------
        adjustments = []
        all_symbols = sorted(set(snap1.keys()) | set(snap2.keys()))
        for s in all_symbols:
            if failed.get(s, 0) >= CONVERGE_GIVEUP_AFTER:
                continue  # 连续 3 轮失败 → 停止重试（收尾终态确认 + 幂等补记兜底）
            short = _sym_target_short(snap1, s)
            long_eff = _long_eff(snap2, s)
            if short > long_eff:
                adjustments.append({
                    'symbol': s, 'action': 'buy', 'ib': ib2, 'account': account2,
                    'volume': short - long_eff,
                    'sell_price': (snap1.get(s) or {}).get('avgCost', 0.0),
                    'reason': f'账户1短缺头{short}股，账户2仅{long_eff}股多头，需补买{short - long_eff}股'
                })
            elif long_eff > short:
                adjustments.append({
                    'symbol': s, 'action': 'sell', 'ib': ib2, 'account': account2,
                    'volume': long_eff - short,
                    'buy_price': (snap2.get(s) or {}).get('avgCost', 0.0),
                    'reason': f'账户2多头{long_eff}股 > 账户1空头{short}股，需卖出{long_eff - short}股'
                })

        if not adjustments:
            raw_gap = _compute_adjustments(snap1, snap2, ib2, account2)
            if not raw_gap:
                converged = True
                logger.info(f"✅ 第{round_no}轮确认: 两账户持仓完全对称 → 收敛退出")
            else:
                details = '; '.join(a['reason'] for a in raw_gap)
                logger.warning(f"⚠️ 第{round_no}轮: 无可再发单标的（失败名单未解: {sorted(failed)}）— {details}；进入收尾补记")
            break

        logger.info(f"⚠️ 第{round_no}轮: 发现 {len(adjustments)} 个持仓不一致，开始调平...")
        for a in adjustments:
            logger.info(f"🔧 调平: {a['symbol']} | {a['action']} {a['volume']}股 | {a['reason']}")
        total_adjustments += len(adjustments)

        # ---------- R5: 标的级暂停（只把本轮实际在调的标加入集合） ----------
        round_symbols = {a['symbol'] for a in adjustments}
        if runner is not None:
            try:
                runner.pause_symbols(round_symbols)
            except Exception as e:
                logger.warning(f"⚠️ 暂停标的异常（不阻断调平）: {e}")

        try:
            # ---------- 4. 并行提交全部缺口单（不同标的互不干扰） ----------
            async def _submit_one(a):
                if a['action'] == 'buy':
                    return await submit_buy_order(a['ib'], a['symbol'], a['volume'])
                return await submit_sell_order(a['ib'], a['symbol'], a['volume'])

            results = await asyncio.gather(
                *(_submit_one(a) for a in adjustments), return_exceptions=True)

            live = []  # (adj, trade)
            for a, r in zip(adjustments, results):
                if isinstance(r, BaseException):
                    logger.error(f"❌ 调平订单提交异常: {a['symbol']} — {r!r}")
                    failed[a['symbol']] = failed.get(a['symbol'], 0) + 1
                elif r is None:
                    logger.error(f"❌ 调平订单提交失败: {a['symbol']}")
                    failed[a['symbol']] = failed.get(a['symbol'], 0) + 1
                else:
                    live.append((a, r))

            # ---------- 5. 逐单等待（保留 20s 上限）+ R7 终态判定（并行） ----------
            live_results = await asyncio.gather(
                *(_settle_trade(a['ib'], t, CONVERGE_ORDER_TIMEOUT)
                  for a, t in live),
                return_exceptions=True)

            round_states = []  # (adj, trade, settle)
            for (a, t), r in zip(live, live_results):
                if isinstance(r, BaseException):
                    logger.error(f"❌ 订单终态判定异常: {a['symbol']} — {r!r}")
                    settle = {'result': 'not_filled', 'volume': 0, 'price': 0.0, 'reported': 'Exception'}
                else:
                    settle = r
                if settle['result'] == 'filled':
                    success_count += 1
                    logger.info(
                        f"✅ 调平成功: {a['symbol']} {a['action']} {settle['volume']}股 "
                        f"@ ${settle['price']:.2f}"
                    )
                else:
                    failed[a['symbol']] = failed.get(a['symbol'], 0) + 1
                    logger.error(
                        f"❌ 调平失败: {a['symbol']} {a['action']} {a['volume']}股 "
                        f"(状态: {settle['reported']}，本轮记未成交)"
                    )
                    if failed[a['symbol']] >= CONVERGE_GIVEUP_AFTER:
                        logger.error(
                            f"🛑 {a['symbol']}: 连续 {failed[a['symbol']]} 轮补单失败 → 本会话停止重试，"
                            f"收尾阶段做终态确认与账本兜底（请人工跟进）"
                        )
                round_states.append((a, t, settle))

            # ---------- 6. 批量核验（一次快照核验全部标；未落账重试 2×2s） ----------
            touched = [a['symbol'] for a, t, s in round_states]
            expect_booking = {a['symbol']: st['volume']
                              for a, t, st in round_states
                              if st['result'] == 'filled' and a['action'] == 'buy'}
            verify = await _batch_verify(ib2, account2, touched, expect_booking)

            # ---------- 账本写入（P0 差额幂等补记） ----------
            for a, t, st in round_states:
                if st['result'] != 'filled':
                    continue
                symbol = a['symbol']
                fill_vol = st['volume']
                fill_price = st['price']
                short_target = _sym_target_short(snap1, symbol)
                actual_long = int(verify.get(symbol, {}).get('position', 0) or 0)

                if trade_store is not None:
                    if a['action'] == 'buy':
                        # 账户1 空头腿：差额幂等补记（KEEL 型"无账"根治）
                        _book_open_leg(trade_store, 'account1', symbol, short_target,
                                       (snap1.get(symbol) or {}).get('avgCost', 0.0),
                                       stock_info_map, '收敛补买: 开仓记录缺失补写')
                        # 账户2 多头腿：按实际持仓补记（含前腿延迟成交的量）
                        _book_open_leg(trade_store, 'account2', symbol,
                                       max(actual_long, fill_vol),
                                       float(verify.get(symbol, {}).get('avgCost', 0) or fill_price),
                                       stock_info_map, '收敛补买: 开仓批次登记')
                        logger.info(f"📝 事件流(account2): 收敛补买 {symbol} {fill_vol}股 @ ${fill_price:.2f}")
                    else:
                        # 账户2 卖出 = 平自己的多头批次（平仓事件，绝不能记为开仓！）
                        # 无可平批次时先按差额幂等补开仓，再记平仓
                        _book_open_leg(trade_store, 'account2', symbol, fill_vol,
                                       float(verify.get(symbol, {}).get('avgCost', 0)
                                             or (snap2.get(symbol) or {}).get('avgCost', 0)
                                             or fill_price),
                                       stock_info_map, '收敛卖出: 开仓记录缺失补写')
                        trade_store.append_close(
                            account='account2', symbol=symbol, close_action='sell',
                            volume=fill_vol, price=fill_price,
                            event_datetime=datetime.datetime.now(),
                            strategy='reconcile', reason=a.get('reason', '') or '收敛卖出',
                            reconcile=True
                        )
                        logger.info(f"📝 事件流(account2): 收敛卖出 {symbol} {fill_vol}股 @ ${fill_price:.2f}")
                    continue

                # ---------- 旧 CSV 兜底路径（trade_store 不可用，保持旧语义） ----------
                now_str = format_datetime(datetime.datetime.now())
                exchange, industry = ('', '')
                if stock_info_map and symbol in stock_info_map:
                    exchange, industry = stock_info_map[symbol].exchange, stock_info_map[symbol].industry
                fill_cost = fill_price * fill_vol
                if a['action'] == 'buy' and buy_csv_path:
                    append_trade_record(TradeRecord(
                        datetime=now_str, code=symbol, exchange=exchange, industry=industry,
                        action='buy', entry_price=fill_price,
                        vol=fill_vol, total_cost=fill_cost, fund_used=fill_cost
                    ), buy_csv_path)
                    logger.info(f"📝 补写 buy.csv: {symbol} {fill_vol}股 @ ${fill_price:.2f}")
                    if sell_csv_path and a.get('sell_price'):
                        try:
                            import pandas as pd
                            df_sell = pd.read_csv(sell_csv_path)
                            if symbol not in df_sell['code'].values:
                                sell_price = float(a['sell_price'])
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
                elif a['action'] == 'sell' and sell_csv_path:
                    append_trade_record(TradeRecord(
                        datetime=now_str, code=symbol, exchange=exchange, industry=industry,
                        action='sell', entry_price=fill_price,
                        vol=fill_vol, total_cost=fill_cost, fund_used=fill_cost
                    ), sell_csv_path)
                    logger.info(f"📝 补写 sell.csv: {symbol} {fill_vol}股 @ ${fill_price:.2f}")

            # ---------- 【同轮超平】实际持仓 > 目标 → 当轮立即卖出超额 ----------
            oversell_filled: Dict[str, int] = {}  # symbol -> 同轮超平卖出成交股数（供 pending 下限计算）
            for a, t, st in list(round_states):
                if st['result'] != 'filled' or a['action'] != 'buy':
                    continue
                symbol = a['symbol']
                target = _sym_target_short(snap1, symbol)
                actual_now = int(verify.get(symbol, {}).get('position', 0) or 0)
                if target > 0 and actual_now > target:
                    excess = actual_now - target
                    logger.info(
                        f"🫧 同轮超平: {symbol} 实际持仓 {actual_now} > 目标 {target} "
                        f"→ 当轮立即卖出超额 {excess}股（无需下一轮）"
                    )
                    sell_trade = await submit_sell_order(ib2, symbol, excess)
                    if sell_trade is None:
                        logger.error(f"❌ 同轮超平卖单提交失败: {symbol}")
                        failed[symbol] = failed.get(symbol, 0) + 1
                        continue
                    sell_settle = await _settle_trade(ib2, sell_trade, CONVERGE_ORDER_TIMEOUT)
                    if sell_settle['result'] == 'filled':
                        success_count += 1
                        logger.info(
                            f"✅ 同轮超平卖出: {symbol} {sell_settle['volume']}股 "
                            f"@ ${sell_settle['price']:.2f}"
                        )
                        verify[symbol] = {
                            'position': max(0, actual_now - sell_settle['volume']),
                            'avgCost': verify.get(symbol, {}).get('avgCost', 0.0)}
                        if trade_store is not None:
                            _book_open_leg(trade_store, 'account2', symbol, sell_settle['volume'],
                                           float(verify.get(symbol, {}).get('avgCost', 0)
                                                 or sell_settle['price']),
                                           stock_info_map, '同轮超平卖出: 开仓记录缺失补写')
                            trade_store.append_close(
                                account='account2', symbol=symbol, close_action='sell',
                                volume=sell_settle['volume'], price=sell_settle['price'],
                                event_datetime=datetime.datetime.now(),
                                strategy='reconcile', reason='同轮超平: 卖出超额持仓',
                                reconcile=True
                            )
                        # 卖出腿成功 → 抵消该标的失败计数（若有）
                        if symbol in failed and failed[symbol] > 0:
                            failed[symbol] = max(0, failed[symbol] - 1)
                        # 记录同轮超平卖出量，供本轮 pending 下限计算使用
                        oversell_filled[symbol] = oversell_filled.get(symbol, 0) + sell_settle['volume']
                    else:
                        failed[symbol] = failed.get(symbol, 0) + 1
                        logger.error(
                            f"❌ 同轮超平卖出失败: {symbol} {excess}股 (状态: {sell_settle['reported']})"
                        )

            # ---------- pending 下限更新（R7 防双买）：本轮有成交的标的，其落账后的预期多头 ----------
            # pending 是"预期总量下限"（floor），下一轮用 max(最新快照, pending) 计算缺口：
            # 既防止"原单 filled 早于持仓落账"时重复补买，又不会与已落账部分重复计数。
            for a in adjustments:
                s = a['symbol']
                buy_fill = sum(st['volume'] for aa, t, st in round_states
                               if aa['symbol'] == s and aa['action'] == 'buy' and st['result'] == 'filled')
                sell_fill = sum(st['volume'] for aa, t, st in round_states
                                if aa['symbol'] == s and aa['action'] == 'sell' and st['result'] == 'filled') \
                    + oversell_filled.get(s, 0)
                expected_long = max(0, _sym_long(snap2, s) + buy_fill - sell_fill)
                if expected_long <= 0:
                    pending_total.pop(s, None)
                else:
                    pending_total[s] = max(pending_total.get(s, 0), expected_long)
                    seen = int(verify.get(s, {}).get('position', 0) or 0)
                    if buy_fill > 0 and seen < expected_long:
                        logger.info(
                            f"⏳ {s}: 本轮成交 {buy_fill}股，持仓落账中（当前已见 {seen}）"
                            f"→ 下一轮按预期下限 {expected_long}股 修正缺口，绝不重复补买"
                        )

            # ---------- 收敛判定：全部标的 空头 == 多头 ----------
            # （本轮订单均已终态判定：成交 / 已撤 / 已死，不存在在途单）
            final_long_map = {s: _sym_long(snap2, s) for s in set(snap1) | set(snap2)}
            for s, v in verify.items():
                final_long_map[s] = max(0, v['position'])
            if all(_sym_target_short(snap1, s) == final_long_map.get(s, 0)
                   for s in final_long_map):
                converged = True
                logger.info(f"✅ 第{round_no}轮: 全部持仓已核验一致 → 收敛退出")
                break

        finally:
            # ---------- R5: 解除本轮标的级暂停 + 轮末重放排队信号 ----------
            if runner is not None:
                try:
                    runner.unpause_symbols(round_symbols)
                except Exception as e:
                    logger.warning(f"⚠️ 解除标的暂停异常: {e}")
                try:
                    await runner.replay_paused_signals()
                except Exception as e:
                    logger.error(f"❌ 轮末重放排队信号异常: {e}")

        # ---------- 7. 未收敛 → 隔 5s 回到 1（按最新快照再算） ----------
        await asyncio.sleep(CONVERGE_ROUND_SETTLE_SECONDS)

    # =================================================================
    # 收尾（R10：补订阅 / 入场登记 / 差额补记 一次完成，均基于 P0 幂等逻辑）
    # =================================================================
    return await _finalize_convergence(
        ib1, ib2, account1, account2,
        sell_csv_path=sell_csv_path, buy_csv_path=buy_csv_path,
        stock_info_map=stock_info_map, trade_store=trade_store,
        runner=runner, recorder=recorder, confirmed_codes=confirmed_codes,
        converged=converged, success_count=success_count,
        total_adjustments=total_adjustments, failed_symbols=sorted(failed.keys())
    )


async def _finalize_convergence(
        ib1: IB, ib2: IB, account1: str, account2: str,
        sell_csv_path=None, buy_csv_path=None,
        stock_info_map=None, trade_store=None,
        runner=None, recorder=None, confirmed_codes=None,
        converged: bool = False, success_count: int = 0,
        total_adjustments: int = 0, failed_symbols=None,
) -> dict:
    """收敛循环收尾：终态快照 → P0 差额幂等补记 → 延迟标的补订阅/入场登记 → 终态确认"""
    logger = get_logger()
    failed_symbols = failed_symbols or []

    # 1) 终态权威快照（失败绝不当作"无持仓"）
    if converged:
        try:
            f1 = await get_positions_strict_with_retry(ib1, account1)
            f2 = await get_positions_strict_with_retry(ib2, account2)
        except PositionSnapshotError as e:
            logger.critical(f"🚨 收尾终态快照获取失败: {e} —— 账实一致性检查不完整，请人工核对两账户TWS持仓与账本")
            f1, f2 = {}, {}
    else:
        f1 = await get_positions(ib1, account1)
        f2 = await get_positions(ib2, account2)

    # 2) P0：账实一致性差额幂等补记（阶段8延迟补齐 + 终态补平统一口径，重复调用不新增 lot）
    backfill_missing_open_records(
        f1, f2, sell_csv_path, buy_csv_path, stock_info_map, trade_store
    )

    # 3) R10：确认点后延迟成交的标的 → 补订阅 + 入场登记（吸收旧 阶段7.6）
    if recorder is not None and confirmed_codes is not None and f1 and f2:
        actual_hedged = set(f1.keys()) & set(f2.keys())
        confirmed = set(confirmed_codes)
        late = sorted(actual_hedged - confirmed)
        if late:
            logger.info(
                f"🔁 检测到 {len(late)} 只标的在确认时点后延迟成交（收敛循环已补账）: {late} → 补订阅+入场登记"
            )
            try:
                from models import StockInfo as _SI
                late_stocks = []
                for code in late:
                    if stock_info_map and code in stock_info_map:
                        late_stocks.append(stock_info_map[code])
                    else:
                        late_stocks.append(_SI(code=code, exchange='SMART',
                                               industry='Unknown', open=0.0, high=0.0,
                                               low=0.0, close=0.0, volume=0, turnover_pct=0.0, y=0.0))
                await recorder.subscribe_all(late_stocks)
                if runner is not None:
                    try:
                        now_dt = datetime.datetime.now()
                        for code in late:
                            p1 = f1.get(code)
                            if p1 and p1['position'] < 0:
                                runner.on_entry(code, 'account1',
                                                float(p1['avgCost']), abs(int(p1['position'])), now_dt)
                            p2 = f2.get(code)
                            if p2 and p2['position'] > 0:
                                runner.on_entry(code, 'account2',
                                                float(p2['avgCost']), int(p2['position']), now_dt)
                    except Exception as e:
                        logger.warning(f"⚠️ 延迟成交标的入场登记失败: {e}")
            except Exception as e:
                logger.warning(f"⚠️ 延迟成交标的补订阅失败: {e}")

    # 4) 终态确认
    remaining = _compute_adjustments(f1, f2, ib2, account2)
    if remaining:
        details = '; '.join(a['reason'] for a in remaining)
        logger.critical(f"🚨 调平后仍存在不一致（{len(remaining)}项）: {details} —— 请人工核对TWS持仓")
    else:
        logger.info("✅ 终态确认通过: 两账户持仓完全对称")

    logger.info(f"调平结束: 成功 {success_count}/{total_adjustments}"
                + (f"（失败名单: {failed_symbols}）" if failed_symbols else ""))

    # 5) 收敛后持仓概览
    logger.info("\n📊 调平后持仓概览:")
    for symbol, pos in f1.items():
        direction = "SELL" if pos['position'] < 0 else "BUY"
        logger.info(f"  账户1 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")
    for symbol, pos in f2.items():
        direction = "BUY" if pos['position'] > 0 else "SELL"
        logger.info(f"  账户2 | {symbol:6s} | {direction} {abs(pos['position'])}股 @ ${pos['avgCost']:.2f}")

    return {
        'converged': converged,
        'success_count': success_count,
        'total_adjustments': total_adjustments,
        'failed_symbols': failed_symbols,
        'final_positions1': dict(f1),
        'final_positions2': dict(f2),
    }

