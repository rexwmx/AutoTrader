# -*- coding: utf-8 -*-
"""平仓执行模块 (执行层)

职责：
- 单次平仓执行（下单、等待、部分成交处理、CSV 回写）
- 带重试的执行（重试前复核实际持仓，防止双重平仓）
- 收市前强制平仓循环（收敛保证）
- 退出前 CSV 补写

策略逻辑（何时平仓）已迁移至 strategy/ 包；
本模块只负责"执行策略发出的平仓信号"以及强制平仓兜底。
"""
import asyncio
import datetime
import pytz
from pathlib import Path
from ib_async import IB
from order import (submit_buy_order, submit_sell_order,
                   get_fill_price, get_filled_volume)
from monitor import wait_for_trade_completion
from csv_writer import update_close_data_in_csv
from logger import get_logger
from util import format_datetime
from position import get_positions, get_positions_strict

ESTIMATED_COMMISSION_RATE = 0.0035
CLOSE_RETRY_DELAY = 10
CLOSE_MAX_RETRIES = 1
CLOSE_CHECK_INTERVAL = 30

# 补写时间写入 CSV 使用的时区（美东），与全系统 Bar.dt / 建仓时间同一基准
_EST_TZ = pytz.timezone('US/Eastern')


def _to_utc_instant(value):
    """
    把 IB 成交时间（各种形态）归一成 UTC 绝对时刻。

    兼容形态:
    - aware datetime   → 时刻权威，原样转 UTC 返回
    - naive datetime   → 按「UTC 墙钟」处理（现行约定：TWS 以 UTC 报表；
                         account.py 已设 TimezoneTWS='UTC' 保证 ib_async 按此解码）
    - float/int epoch  → Fill.time 的收到时刻是机器时钟 epoch，按 UTC 转换
    - 其它 / 非法      → None
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            return value.astimezone(datetime.timezone.utc)
        return value.replace(tzinfo=datetime.timezone.utc)
    if isinstance(value, (int, float)) and value > 0:
        try:
            return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    return None


def normalize_side(side: str) -> str:
    """
    归一化成交方向为 'BUY'/'SELL'（大小写不敏感）。

    关键：IBKR TWS API 的 Execution.side 协议枚举值是 **'BOT'(买) / 'SLD'(卖)**，
    ib_async 解码器把协议字符串原样透传，因此与 'BUY'/'SELL' 直接比较**永远为 False**
    （9.8 事故中 ASAN 退出前补写因此瘫痪，输出"无需补写"）。
    本函数同时兼容真实协议值与语义值，供所有 side 比较使用。
    """
    s = str(side).upper()
    return {'BOT': 'BUY', 'SLD': 'SELL'}.get(s, s)


class CloseManager:
    def __init__(self, ib1: IB, ib2: IB, account1: str, account2: str,
                 sell_csv_path, buy_csv_path,
                 trade_store=None):
        self.ib1 = ib1
        self.ib2 = ib2
        self.account1 = account1
        self.account2 = account2
        self.sell_csv_path = sell_csv_path
        self.buy_csv_path = buy_csv_path
        # 事件流存储（account1/account2/{symbol}.csv + sell.csv/buy.csv 汇总）；
        # None 时全部退化为旧 CSV 路径（兼容测试与兜底场景）
        self.trade_store = trade_store
        self.logger = get_logger()

        # 收市前强制平仓窗口标志：窗口内不再触发新的运行时平仓，
        # 避免运行时平仓任务与强制平仓循环并发对同一标的重复下单（过度平仓→反向残留）。
        # 与策略侧的让路联动（runner.stop()）由 hedge_trade 统一控制。
        self.force_close_active = False

    # ------------------------------------------------------------------
    # 公开入口（供 StrategyRunner 调用）
    # ------------------------------------------------------------------
    async def run_close_signal(self, ib, account, symbol, close_action,
                               volume, open_price, open_action, csv_path,
                               target_lot_id: str = '', strategy: str = '',
                               reason: str = '') -> bool:
        """执行一个平仓信号（带重试+持仓复核）

        Returns:
            True = 已平仓或确认目标方向仓位已归零；
            False = 重试次数用尽仍未平完。

        Args 新增（可选）：
            target_lot_id: 指定要平的批次；空 → 事件流按 FIFO 分配
            strategy/reason: 写入事件流的审计字段
        """
        return await self._execute_close_with_retry(
            ib, account, symbol, close_action, volume,
            open_price, open_action, csv_path,
            target_lot_id=target_lot_id, strategy=strategy, reason=reason
        )

    # ------------------------------------------------------------------
    # 单次执行 + 重试
    # ------------------------------------------------------------------
    async def _position_now(self, ib: IB, account: str, symbol: str) -> int:
        """
        权威快照：该账户该标的持仓（股数），失败抛异常。
        使用失败严格版快照：TWS 取数失败必须向上抛出（由调用方按"未知"处理），
        绝不能被静默当成 0 持仓——那会让重试误判"已平完"而停止补单。
        """
        snap = await get_positions_strict(ib, account)
        return int(snap.get(symbol, {'position': 0})['position'])

    async def _execute_close_with_retry(self, ib: IB, account: str, symbol: str, close_action: str,
                                        volume: int, open_price: float, open_action: str, csv_path,
                                        target_lot_id: str = '', strategy: str = '',
                                        reason: str = '', force: bool = False,
                                        reconcile: bool = False,
                                        actual_commission=None) -> bool:
        """
        带重试的平仓执行（重试前复核实际持仓，防止"已成交+重试"双重平仓）

        Returns:
            bool: True=已平仓或仓位确认已归零; False=重试次数用尽仍未平完

        Args 新增（可选）：
            target_lot_id/strategy/reason: 事件流审计字段
            force: True → 事件类型 FORCE_CLOSE（强平）
            reconcile: True → 事件类型 RECONCILE（调平/回填）
        """
        for attempt in range(CLOSE_MAX_RETRIES + 1):
            if attempt > 0:
                await asyncio.sleep(CLOSE_RETRY_DELAY)

                live = None
                try:
                    live = await self._position_now(ib, account, symbol)
                except Exception as e:
                    self.logger.warning(f"⚠️ {symbol}: 重试前持仓复核失败: {e}")

                if live is not None:
                    # 方向显式判断（修复 9.8 ASAN 事故 Bug：旧式 (close_action=='buy')==(live<0)
                    # 在 close_action='sell' 且 live==0 时 False==False→True，
                    # 把"目标仓位已归零"误判为"仍持仓"，对 0 仓盲目补单→超时失败）：
                    #   buy 平空：目标仓 = 空头 (live<0)
                    #   sell 平多：目标仓 = 多头 (live>0)
                    target_still_open = (live < 0) if close_action == 'buy' else (live > 0)
                    if not target_still_open:
                        self.logger.info(
                            f"🔍 {symbol}: 重试前目标方向持仓已不存在 (当前 {live:+d}股，"
                            f"可能已被前单延迟成交) —— 停止补单，防止双重平仓"
                        )
                        return True
                    live_vol = abs(live)
                    if live_vol != volume and live_vol > 0:
                        self.logger.info(
                            f"🔍 {symbol}: 实况复核 {live_vol}股 (原计划 {volume}股) —— 按实况数量平仓"
                        )
                        volume = live_vol

                self.logger.info(f"🔄 {symbol}: 第 {attempt} 次重试平仓 ({volume}股)...")

            success = await self._execute_close(
                ib, symbol, close_action, volume, open_price, open_action, csv_path,
                target_lot_id=target_lot_id, strategy=strategy, reason=reason,
                force=force, reconcile=reconcile,
                actual_commission=actual_commission
            )
            if success:
                return True

        self.logger.error(f"❌ {symbol}: 平仓重试 {CLOSE_MAX_RETRIES} 次后仍失败")
        return False

    async def _execute_close(self, ib: IB, symbol: str, close_action: str,
                             volume: int, open_price: float, open_action: str, csv_path,
                             target_lot_id: str = '', strategy: str = '',
                             reason: str = '', force: bool = False,
                             reconcile: bool = False,
                             actual_commission=None) -> bool:
        """执行单次平仓，返回是否成功（全部成交才返回 True）

        账本写入双路径：
        - trade_store 可用 → 事件流（按批次 FIFO / target_lot_id 分配，实际佣金按比例拆分）；
        - 否则 → 旧 CSV 回写（update_close_data_in_csv 兜底）。
        """
        self.logger.info(f"🔄 {symbol}: 开始执行 {close_action} 平仓 {volume}股...")

        trade = await submit_buy_order(ib, symbol, volume) if close_action == 'buy' else await submit_sell_order(
            ib, symbol, volume)
        if not trade:
            return False

        status = await wait_for_trade_completion(trade, timeout_seconds=120)

        if status != 'Filled':
            self.logger.error(f"❌ {symbol}: 平仓订单未成交 (状态: {status})")
            if status == 'Timeout':
                if trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
                    # 注意：本版本 ib_async 的 cancelOrder 是同步方法，
                    # 误用 await 会抛 TypeError 并炸掉整个平仓循环（导致其余股票不再处理）
                    ib.cancelOrder(trade.order)
            return False

        close_vol = get_filled_volume(trade)

        # ==================== 核心规则：部分成交 ≠ 平仓完成 ====================
        # wait_for_trade_completion 只要"有任何成交"就返回 Filled（首笔成交消息即触发），
        # 此时订单可能仍在工作（只成交了一部分）。若据此把该行 CSV 标记为已平仓，
        # 剩余部分在收市附近未被成交（被市场取消/拒绝）就会形成
        # "CSV 显示已平仓、账户仍有持仓" 的错账。
        # 因此：未全部成交一律视为"未平仓"——撤销尚在工作的工作单，
        # 交回重试/下一轮按实况数量补平，绝不提前写 CSV。
        if close_vol < volume:
            self.logger.warning(
                f"⚠️ {symbol}: 仅部分成交 {close_vol}/{volume}股 —— 视为未平仓"
                f"（收市前必须全仓平到0），撤销剩余工作单，等待后续按实况补平"
            )
            if trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']:
                try:
                    ib.cancelOrder(trade.order)
                except Exception as e:
                    self.logger.warning(f"⚠️ {symbol}: 撤销剩余工作单失败: {e}")
            return False

        close_price = get_fill_price(trade)
        close_fund = close_price * close_vol

        # ==================== 实际佣金（旧路径净利 与 事件流拆行 共用） ====================
        actual_commission = (volume + close_vol) * ESTIMATED_COMMISSION_RATE
        try:
            comm = trade.commission()
            if comm is not None and comm != 1.7976931348623157e+308:
                actual_commission = float(comm)
        except Exception:
            pass

        if csv_path is None:
            # 反向残留/历史残留仓：没有对应的开仓记录，不能回写账本
            self.logger.critical(
                f"🚨 {symbol}: 残留仓平仓完成 {close_vol}股 @ ${close_price:.2f} "
                f"(非本次对冲开仓记录，未写入账本，请人工登记核对)"
            )
            return True

        if self.trade_store is not None:
            # ==================== 事件流写入（新路径） ====================
            store_account = 'account1' if ib is self.ib1 else 'account2'
            # 账本缺口预检：平仓数量 > 账上未平批次 → 疑似延迟开仓成交或开仓记录缺失
            try:
                remaining_lot = sum(int(l['remaining'])
                                    for l in self.trade_store.get_open_lots(store_account, symbol))
                if close_vol > remaining_lot:
                    self.logger.critical(
                        f"🚨 {symbol}: 平仓成交 {close_vol}股 > 账上未平批次 {remaining_lot}股 —— "
                        f"疑似延迟开仓成交或开仓记录缺失，账本只记 {remaining_lot}股，"
                        f"超出 {close_vol - remaining_lot}股 请人工核对两账户"
                    )
            except Exception as e:
                self.logger.warning(f"⚠️ {symbol}: 未平批次预检失败: {e}")

            ok = self.trade_store.append_close(
                account=store_account, symbol=symbol,
                close_action=close_action, volume=close_vol, price=close_price,
                event_datetime=datetime.datetime.now(),
                target_lot_id=target_lot_id or None,
                strategy=strategy or 'dynamic_tp', reason=reason or 'close',
                force=force, reconcile=reconcile,
                actual_commission=actual_commission,
            )
            if ok:
                self.logger.info(
                    f"✅ {symbol}: 平仓完成，事件流已写入 | {close_vol}股 @ ${close_price:.2f} | "
                    f"strategy={strategy or 'dynamic_tp'}"
                )
            else:
                self.logger.critical(
                    f"🚨 {symbol}: 平仓完成但事件流无可扣减的开仓批次（账本缺口）—— 请人工补记账"
                )
            return True

        # ==================== 旧 CSV 回写路径（trade_store 不可用兜底） ====================
        open_fund = open_price * close_vol
        gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)
        profit = gross_profit - actual_commission

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

    # ------------------------------------------------------------------
    # 强制平仓
    # ------------------------------------------------------------------
    async def force_close_all(self):
        """收市前强制平仓：方向无关，清空两账户全部持仓（含反向残留/历史残留）"""
        self.logger.info("\n" + "=" * 50)
        self.logger.info("🚨 收市前强制平仓：清空所有持仓（方向无关）")
        self.logger.info("=" * 50)

        self.force_close_active = True
        try:
            p1 = await get_positions(self.ib1, self.account1)
            p2 = await get_positions(self.ib2, self.account2)
            pos1 = {s: v for s, v in p1.items() if v['position'] != 0}
            pos2 = {s: v for s, v in p2.items() if v['position'] != 0}
            await self._force_close_positions(pos1, pos2)
        finally:
            self.force_close_active = False

    async def force_close_until_flat(self, timeout_minutes: int = 10) -> bool:
        """
        强制平仓循环（收敛保证版）

        每轮基于最新权威持仓快照，对任意方向的非零持仓提交反向平仓：
        - 标准腿（账户1空头/账户2多头）照常回写CSV平仓字段；
        - 反向残留（账户1多头/账户2空头，通常由"已Cancel订单延迟成交 + 重试补单"
          双重成交造成，或前日遗留仓如EOSE）→ 平仓并记CRITICAL，不写CSV。
        如此循环，无论TWS延迟成交如何乱序，都会逐轮收敛到两账户全平。

        Returns:
            bool: True=全部清仓完成; False=超时仍有残留
        """
        # 进入强制平仓窗口：运行时策略让路（runner.stop() 由 hedge_trade 在调用前触发）
        self.force_close_active = True
        try:
            deadline = datetime.datetime.now() + datetime.timedelta(minutes=timeout_minutes)
            attempt = 0
            while datetime.datetime.now() < deadline:
                attempt += 1

                # 权威快照（按实例锁串行化）。
                # 关键：**取数失败必须按"未知"处理并继续重试**，
                # 绝不能回退成"0持仓"——否则 TWS 抖动会被误判为"全部清仓"，
                # 造成 CSV 显示已平仓而账户仍有持仓的假成功。
                try:
                    snap1 = await get_positions_strict(self.ib1, self.account1)
                    snap2 = await get_positions_strict(self.ib2, self.account2)
                except Exception as e:
                    self.logger.error(f"❌ 第{attempt}次检查: 获取持仓快照失败（本轮不能判定已清仓）: {e}")
                    await asyncio.sleep(min(15, CLOSE_CHECK_INTERVAL))
                    continue

                pos1 = {s: v for s, v in snap1.items() if v['position'] != 0}
                pos2 = {s: v for s, v in snap2.items() if v['position'] != 0}

                if not pos1 and not pos2:
                    self.logger.info(f"✅ 第{attempt}次检查: 所有持仓已完全平仓（两账户0持仓）")
                    return True

                def _fmt(d):
                    return ', '.join(
                        f"{sname} {'多头' if v['position'] > 0 else '空头'} {abs(int(v['position']))}股"
                        for sname, v in d.items()
                    )

                self.logger.critical(
                    f"🚨 第{attempt}次检查: 仍有未平仓 (账户1: {_fmt(pos1) or '—'}; "
                    f"账户2: {_fmt(pos2) or '—'}) —— 重新提交平仓订单..."
                )

                await self._force_close_positions(pos1, pos2)
                await asyncio.sleep(CLOSE_CHECK_INTERVAL)

            self.logger.critical(
                f"🚨 平仓超时（{timeout_minutes}分钟），未清仓 —— 可能仍有残留持仓"
                f"（含反向残留），请立即人工核对TWS两个账户"
            )
            return False
        finally:
            self.force_close_active = False

    @staticmethod
    def _pv(obj, key):
        """兼容读取：dict（get_positions快照）或 Position对象"""
        return obj[key] if isinstance(obj, dict) else getattr(obj, key)

    async def _force_close_positions(self, pos1: dict, pos2: dict):
        """
        方向无关平仓（收敛保证）：
        任意账户任意方向非零持仓一律平回零仓。
        - 标准腿（账户1空头/账户2多头）→ 回写CSV平仓字段；
        - 反向残留（账户1多头/账户2空头）→ 无开仓记录，平仓+CRITICAL，不写CSV。
        """
        jobs = []  # (标签, 协程)

        for symbol, pos in pos1.items():
            position = int(self._pv(pos, 'position'))
            standard = position < 0
            jobs.append((f"账户1 {symbol}", self._close_position(
                self.ib1, self.account1, symbol, position,
                self.sell_csv_path if standard else None, 'sell', standard,
                float(self._pv(pos, 'avgCost')),
            )))

        for symbol, pos in pos2.items():
            position = int(self._pv(pos, 'position'))
            standard = position > 0
            jobs.append((f"账户2 {symbol}", self._close_position(
                self.ib2, self.account2, symbol, position,
                self.buy_csv_path if standard else None, 'buy', standard,
                float(self._pv(pos, 'avgCost')),
            )))

        if jobs:
            # 隔离单个标的的异常：任一只股票平仓任务抛错（TWS抖动/接口错误）
            # 不得炸掉整轮平仓、更不得炸掉整个程序——其余股票必须继续平
            results = await asyncio.gather(*(coro for _, coro in jobs), return_exceptions=True)
            for (label, _), r in zip(jobs, results):
                if isinstance(r, BaseException):
                    self.logger.error(
                        f"❌ 强制平仓 {label}: 任务异常（其他股票不受影响，本轮结束后会自动补平）: {r!r}"
                    )
        else:
            self.logger.info("✅ 无持仓需要强制平仓")

    async def _close_position(self, ib: IB, account: str, symbol: str, position: int,
                              csv_path, open_action: str, standard: bool, avg_cost: float = 0.0):
        """平单个持仓：空头→买入回补，多头→卖出清仓"""
        side = 'buy' if position < 0 else 'sell'
        vol = abs(int(position))
        if standard:
            acc_name = "账户1(做空)" if ib is self.ib1 else "账户2(做多)"
            self.logger.info(f"🚨 强制平仓 {acc_name}: {symbol} {vol}股 -> {side} 平仓")
        else:
            acc_name = "账户1" if ib is self.ib1 else "账户2"
            self.logger.critical(
                f"🚨 检测到反向残留持仓: {acc_name} {symbol} "
                f"{'多头' if position > 0 else '空头'} {vol}股 "
                f"(非本次对冲仓位 —— 疑似延迟成交超平或前日遗留仓) "
                f"—— 执行 {side} 平仓（不写CSV，请人工核对）"
            )
        await self._execute_close_with_retry(
            ib, account, symbol, side, vol, avg_cost,
            open_action if standard else None, csv_path,
            strategy='force_close', reason='收市前强制平仓', force=True
        )

    # ------------------------------------------------------------------
    # 退出前 CSV 补写
    # ------------------------------------------------------------------
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

        # 刷新持仓，确认是否真的全部平仓（用权威快照，不用长寿命缓存）
        # 关键：取数失败必须如实报错，绝不能静默当成"0持仓"而宣称"已全部平仓"
        try:
            snap1 = await get_positions_strict(self.ib1, self.account1)
            snap2 = await get_positions_strict(self.ib2, self.account2)
            remaining1 = {s: v['position'] for s, v in snap1.items() if v['position'] != 0}
            remaining2 = {s: v['position'] for s, v in snap2.items() if v['position'] != 0}
        except Exception as e:
            self.logger.critical(
                f"🚨 退出前持仓终态确认失败: {e} —— 无法确认两账户是否真正清仓，"
                f"请立即人工核对TWS两个账户的持仓!"
            )
            remaining1, remaining2 = {}, {}

        if remaining1 or remaining2:
            d1 = ', '.join(f"{sname} {'+' if v > 0 else ''}{v}股" for sname, v in remaining1.items())
            d2 = ', '.join(f"{sname} {'+' if v > 0 else ''}{v}股" for sname, v in remaining2.items())
            self.logger.critical(
                f"🚨 TWS仍有残留持仓 (账户1: {d1}; 账户2: {d2}) —— "
                f"CSV显示已全部平仓但账户仍有持仓，请立即人工对账处理!"
            )
        else:
            self.logger.info("✅ TWS 确认：所有持仓已平仓，两账户0持仓")

        # 从 TWS 获取今日所有成交记录
        try:
            fills1 = self.ib1.fills()
            fills2 = self.ib2.fills()
        except Exception as e:
            self.logger.error(f"❌ 获取 fills 失败: {e}")
            return

        if self.trade_store is not None:
            # ==================== 事件流回填（新路径） ====================
            # 账户1 平仓 = BUY 方向成交；账户2 平仓 = SELL 方向成交
            p1 = self._backfill_store_account('account1', fills1, 'BUY')
            p2 = self._backfill_store_account('account2', fills2, 'SELL')
            self.logger.info(f"✅ 事件流补写完成: account1 {p1} 行, account2 {p2} 行")
            return

        # ==================== 旧 CSV 回填（trade_store 不可用兜底） ====================
        # 补写 sell.csv（账户1的平仓 = buy 操作）
        self._backfill_csv(fills1, 'BUY', 'sell', self.sell_csv_path)
        # 补写 buy.csv（账户2的平仓 = sell 操作）
        self._backfill_csv(fills2, 'SELL', 'buy', self.buy_csv_path)

    def _resolve_close_time(self, sym_fills, symbol: str) -> str:
        """解析平仓入账时间（美东时间字符串）——时区加固，新旧路径共用

        9.14 SBET/ASST、9.16 EOSE、9.18 GNRC/RXRX/ABSI 事故加固（详见模块头注释）：
        - execution.time → UTC 时刻（aware 原样 / naive 按 UTC 墙钟 / epoch 换算）；
        - 收到时刻交叉校验（fill.time，机器时钟、不受 TWS 时区设置影响）：
          偏差 > 10 分钟 → 判定时区约定已失效，改用收到时刻 + CRITICAL；
        - 无法解析 → 回退收到时刻/机器时钟，绝不把坏时间写入账本。
        """
        def _utc_key(f):
            t = _to_utc_instant(getattr(f.execution, 'time', None))
            return t if t is not None else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        last_fill = max(sym_fills, key=_utc_key)
        exec_utc = _utc_key(last_fill)
        recv_utc = _to_utc_instant(getattr(last_fill, 'time', None))

        if recv_utc is not None:
            drift = abs((exec_utc - recv_utc).total_seconds())
            if drift > 600:
                self.logger.critical(
                    f"🚨 {symbol}: IB 成交回报时间 {exec_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} 与"
                    f"收到时刻 {recv_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} 偏差 {drift/60:.0f} 分钟 —— "
                    f"时区约定很可能已失效（TWS 时区改动 / ib_async 解码异常）。"
                    f"已改用收到时刻写入账本；请立即核对 Client Portal 成交明细!"
                )
                exec_utc = recv_utc
        else:
            if exec_utc.year == datetime.datetime.min.year:
                self.logger.error(f"⚠️ {symbol}: 成交时间无法解析 —— 回退为当前机器时钟")
                exec_utc = datetime.datetime.now(datetime.timezone.utc)
            else:
                self.logger.warning(
                    f"⚠️ {symbol}: 无收到时刻可用于交叉校验，直接采用 execution.time"
                    f"（={exec_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}）"
                )
        return exec_utc.astimezone(_EST_TZ).strftime('%Y-%m-%d %H:%M:%S')

    def _backfill_store_account(self, account: str, fills, close_side: str) -> int:
        """退出前事件流回填：按该账户每只股票的未平批次，把平仓成交分配写入平仓事件

        - 每批一行（related_lot_id 关联），event_type=RECONCILE，strategy=reconcile；
        - 平仓成交 > 未平批次（延迟开仓成交场景）→ 只记未平部分并 CRITICAL 告警；
        - 入账时间沿用 _resolve_close_time 的时区加固逻辑（与旧路径一致）。
        """
        store = self.trade_store
        close_action = 'buy' if account == 'account1' else 'sell'
        account_dir = Path(store.account_dirs[account])
        patched = 0
        if not Path(account_dir).exists():
            return 0

        for stock_csv in sorted(Path(account_dir).glob('*.csv')):
            symbol = stock_csv.stem
            try:
                lots = store.get_open_lots(account, symbol)
            except Exception as e:
                self.logger.warning(f"⚠️ {symbol}: 读取未平批次失败: {e}")
                continue
            if not lots:
                continue

            # 汇总该股票平仓方向成交
            close_fills = []
            total_fill = 0
            fill_cost = 0.0
            for f in fills:
                if normalize_side(f.execution.side) != normalize_side(close_side):
                    continue
                if f.contract.symbol != symbol:
                    continue
                try:
                    sh = abs(int(float(f.execution.shares)))
                except Exception:
                    continue
                if sh <= 0:
                    continue
                close_fills.append(f)
                total_fill += sh
                try:
                    fill_cost += sh * float(f.execution.price)
                except Exception:
                    pass
            if total_fill <= 0:
                continue

            avg_price = (fill_cost / total_fill) if total_fill else 0.0
            remaining = sum(int(l['remaining']) for l in lots)
            if total_fill > remaining:
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 退出回填平仓成交 {total_fill}股 > 未平批次 {remaining}股 —— "
                    f"可能有延迟开仓成交，账本只记 {remaining}股，请立即核对账户实际持仓!"
                )

            dt_str = self._resolve_close_time(close_fills, symbol)

            remaining_to_close = remaining
            for lot in lots:
                if remaining_to_close <= 0:
                    break
                alloc = min(remaining_to_close, int(lot['remaining']))
                ok = store.append_close(
                    account=account, symbol=symbol, close_action=close_action,
                    volume=alloc, price=avg_price, event_datetime=dt_str,
                    target_lot_id=lot['lot_id'],
                    strategy='reconcile', reason='退出前补写',
                    reconcile=True,
                )
                if ok:
                    patched += 1
                    self.logger.info(
                        f"📝 补写: {symbol} ({account}) {lot['lot_id']} | "
                        f"平仓 {alloc}股 @ ${avg_price:.4f}"
                    )
                remaining_to_close -= alloc
        return patched

    def _backfill_csv(self, fills, close_side: str, open_action: str, csv_path):
        """
        从 fills 中找到平仓成交，补写到 CSV

        Args:
            fills: TWS fills 列表
            close_side: 平仓方向 ('BUY' 表示做空平仓, 'SELL' 表示做多平仓)
                        注意：fills 内的 f.execution.side 是 TWS 协议值 'BOT'/'SLD'，
                        必须经 normalize_side() 归一化后再比较（直接 == 恒为 False）
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

        # 从 fills 中按股票分组，汇总平仓方向的成交（数量取 abs：方向以 side 为准，数量恒为正）
        close_fills_by_symbol = {}
        fill_vol_by_symbol = {}
        fill_cost_by_symbol = {}
        for f in fills:
            # 必须归一化：TWS 协议 side 值为 BOT/SLD，直接 == 'BUY'/'SELL' 恒为 False
            if normalize_side(f.execution.side) == normalize_side(close_side):
                try:
                    sh = abs(int(float(f.execution.shares)))
                except Exception:
                    continue
                if sh <= 0:
                    continue
                sym = f.contract.symbol
                close_fills_by_symbol.setdefault(sym, []).append(f)
                fill_vol_by_symbol[sym] = fill_vol_by_symbol.get(sym, 0) + sh
                try:
                    fill_cost_by_symbol[sym] = fill_cost_by_symbol.get(sym, 0.0) + sh * float(f.execution.price)
                except Exception:
                    pass

        patched_count = 0
        for idx, row in unclosed.iterrows():
            symbol = row['code']
            total_fill = fill_vol_by_symbol.get(symbol, 0)
            if total_fill <= 0:
                continue

            try:
                row_vol = int(float(row['vol']))
            except Exception:
                row_vol = 0

            # ==================== 核心规则：成交必须完整覆盖开仓记录才允许标记已平仓 ====================
            # 旧代码只要有任何一笔平仓成交就补写整行，部分成交也会被标记"已平仓"，
            # 直接造成 "CSV 显示已全部平仓、账户仍有持仓" 的错账。未覆盖的行必须保留未平仓标记。
            if row_vol > 0 and total_fill < row_vol:
                fill_vol_by_symbol[symbol] = 0
                self.logger.critical(
                    f"🚨 {symbol} ({open_action}): 平仓成交仅 {total_fill}股 < 开仓记录 {row_vol}股 —— "
                    f"其中 {row_vol - total_fill}股 可能真正未平仓！该行保留未平仓标记（不写平仓字段），"
                    f"请立即核对两个账户的实际残留持仓并手动处理"
                )
                continue

            close_vol = row_vol if row_vol > 0 else total_fill
            fill_vol_by_symbol[symbol] = total_fill - close_vol

            # 该股票全部平仓成交的加权均价（多行 FIFO 覆盖时按同一均价近似）
            avg_price = (fill_cost_by_symbol.get(symbol, 0.0) / total_fill) if total_fill else 0.0

            # 获取开仓数据
            entry_price = float(row['entry_price'])
            open_fund = entry_price * close_vol

            close_fund = avg_price * close_vol
            gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)
            commission = (row_vol + close_vol) * ESTIMATED_COMMISSION_RATE
            profit = gross_profit - commission

            # 使用最后一笔成交的时间
            # ==================== 时区加固（9.14 SBET/ASST、9.16 EOSE、9.18 GNRC/RXRX/ABSI 事故） ====================
            # 历史问题：TWS(paper) 以「UTC 墙钟字符串」回报成交时间，ib_async 默认(TimezoneTWS='')
            # 把 naive 时间按**机器本地时区(美东)**解释后再转 UTC —— 解码出的时刻整体偏移 4h(EDT)；
            # 旧代码直接 strftime 写入 CSV，最终记录比真实美东时间偏 **8 小时**（如 15:50:09 → 23:50:09）。
            # 修复：
            #   1) account.py 建连时设 ib.TimezoneTWS='UTC'（告知 ib_async TWS 回报用 UTC），
            #      使 execution.time 成为正确时刻；
            #   2) 此处统一转美东(_EST_TZ)再写账本；
            #   3) 兜底：用「成交回报收到时刻」(fill.time，机器时钟、不受 TWS 时区设置影响) 交叉校验，
            #      偏差 >10 分钟说明时区约定已失效 —— 改用收到时刻并记 CRITICAL，绝不把坏时间写进账本。
            # （加固逻辑已抽取为 _resolve_close_time，与事件流回填路共用，保证两条路径行为一致）
            close_dt_str = self._resolve_close_time(close_fills_by_symbol[symbol], symbol)

            close_data = {
                'close_datetime': close_dt_str,
                'close_price': round(avg_price, 4),
                'close_vol': close_vol,
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
                f"平仓 {close_vol}股 @ ${avg_price:.2f} | 净利: ${profit:.2f}"
            )

        if patched_count > 0:
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"✅ CSV 补写完成: {patched_count} 条记录已更新 ({csv_path.name})")
        else:
            self.logger.info(f"✅ 无需补写 ({csv_path.name})")
