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
from position import (
    get_positions, get_positions_strict,
    get_positions_strict_with_retry, PositionSnapshotError,
)

ESTIMATED_COMMISSION_RATE = 0.0035
CLOSE_RETRY_DELAY = 10
CLOSE_MAX_RETRIES = 1
CLOSE_CHECK_INTERVAL = 30
# 【R9】退出回填前 fills 覆盖校验：收盘方向 fills 总量 < 计划平仓量 → 等待秒数后重取一次
FILLS_COVERAGE_RETRY_SECONDS = 5

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
                        # 【2026-09-27 修复】重试前复核发现目标方向仓位已不存在
                        # （前单延迟成交/fake-Cancelled 落账）→ 停止补单防双重平仓，
                        # 同时**按实际成交补记平仓账**：账户已无该方向持仓 =
                        # 账上未平开仓批次必然已全部成交。
                        # 旧行为：直接 return True 不写账，缺口留给退出补写
                        # （而退出补写当时还有第2条缺陷，事件流路径无测试覆盖）。
                        booked = await self._book_close_confirmed_flat(
                            ib, account, symbol, close_action, volume, open_price,
                            open_action, csv_path,
                            target_lot_id=target_lot_id, strategy=strategy,
                            reason=reason or '重试前复核已平：按实际成交补记')
                        self.logger.info(
                            f"🔍 {symbol}: 重试前目标方向持仓已不存在 (当前 {live:+d}股，"
                            f"可能已被前单延迟成交) —— 停止补单，防止双重平仓"
                            f"（平仓账已按实际成交补记 {booked}股）"
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

    async def _book_close_confirmed_flat(self, ib: IB, account: str, symbol: str,
                                         close_action: str, volume: int, open_price: float,
                                         open_action: str, csv_path,
                                         target_lot_id: str = '', strategy: str = '',
                                         reason: str = '') -> int:
        """重试前复核"已平"的平仓账补记（2026-09-27）

        背景：前单部分成交后重试，重试前持仓复核发现目标方向仓位已不存在
        （前单延迟成交 / fake-Cancelled 落账）→ 停止补单防双重平仓。
        旧行为到此直接成功返回**不写平仓账**，账本缺口只能等退出补写兜底。
        本方法把补账前置到"已平确认"的当下：

        - 补记量 = 该账户该标的**账上未平开仓批次全部余量**（账户已无该方向
          持仓 = 这批未平必然已全部成交；不按原计划量、也不按已见 fills 量）；
        - 价格 = 最近一单的实际成交均价（_last_trade 的 avgFillPrice/fills），
          无实际成交信息时退回开仓价并 WARNING（延迟落账部分的价格以
          Client Portal 为准，人工核对）；
        - 账上无未平批次（已入账过）→ 幂等返回 0，绝不重复补记。

        Returns:
            int: 实际补记股数（无未平批次/补记失败 → 0）
        """
        price = 0.0
        trade = getattr(self, '_last_trade', None)
        if trade is not None:
            try:
                price = float(get_fill_price(trade) or 0.0)
            except Exception:
                price = 0.0
        if price <= 0:
            price = float(open_price)
            self.logger.warning(
                f"⚠️ {symbol}: 补记平仓价无实际成交信息（无 trade / avgFillPrice）—— "
                f"暂按开仓价 ${price:.4f} 补记，请以 Client Portal 实际成交价人工核对")

        # ==================== 事件流路径 ====================
        if self.trade_store is not None:
            store_account = 'account1' if ib is self.ib1 else 'account2'
            try:
                remaining_lot = sum(int(l['remaining'])
                                    for l in self.trade_store.get_open_lots(store_account, symbol))
            except Exception as e:
                self.logger.critical(f"🚨 {symbol}: 已平补账读取未平批次失败: {e} —— 请人工补记账")
                return 0
            if remaining_lot <= 0:
                self.logger.info(f"🔍 {symbol}: 账上已无未平批次（应已入账）—— 不重复补记")
                return 0
            try:
                ok = self.trade_store.append_close(
                    account=store_account, symbol=symbol,
                    close_action=close_action, volume=remaining_lot, price=price,
                    event_datetime=datetime.datetime.now(),
                    target_lot_id=target_lot_id or None,
                    strategy=strategy or 'reconcile',
                    reason=reason or '重试前复核已平：按实际成交补记',
                    reconcile=True)
            except Exception as e:
                self.logger.critical(f"🚨 {symbol}: 已平补账写事件流失败: {e} —— 请人工补记账")
                return 0
            if ok:
                self.logger.info(
                    f"✅ {symbol}: 重试前复核已平 → 平仓账已补记 {remaining_lot}股 @ ${price:.4f}"
                    f"（账户已无该方向持仓 = 未平批次必然全部成交）")
            else:
                self.logger.critical(f"🚨 {symbol}: 已平补账无可扣减的开仓批次（账本缺口）—— 请人工补记账")
                return 0
            return remaining_lot

        # ==================== 旧 CSV 路径 ====================
        if csv_path is None:
            self.logger.critical(
                f"🚨 {symbol}: 确认已平但该标的无开仓记录可回写（残留仓场景）—— 请人工登记核对")
            return 0
        close_vol = max(1, int(volume))
        close_fund = price * close_vol
        open_fund = float(open_price) * close_vol
        gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)
        profit = gross_profit - (volume + close_vol) * ESTIMATED_COMMISSION_RATE
        close_data = {
            'close_datetime': format_datetime(datetime.datetime.now()),
            'close_price': round(price, 4),
            'close_vol': close_vol,
            'close_fund': round(close_fund, 2),
            'gross_profit': round(gross_profit, 2),
            'profit': round(profit, 2),
        }
        if update_close_data_in_csv(csv_path, symbol, open_action, close_data):
            self.logger.info(f"✅ {symbol}: 重试前复核已平 → 平仓账已补记 {close_vol}股 @ ${price:.4f}（旧CSV）")
            return close_vol
        self.logger.critical(f"🚨 {symbol}: 已平补账更新CSV失败 —— 请人工补记账")
        return 0

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
            self._last_trade = None
            return False

        # 供"重试前复核已平"分支按实际成交补记平仓账（价格取该单实际成交均价）
        self._last_trade = trade

        # 【统一"等订单完成"】wait_for_trade_completion 已于 2026-09-27 升级为
        # monitor.wait_for_order_final 语义：等全量成交或订单终态才结算，
        # 部分成交+仍在工作不再提前返回（旧行为会让下面的"部分成交"分支
        # 误撤仍在工作的剩余挂单 —— 市价单分笔成交时的竞态根因）
        # 【2026-09-28 强平提速】强平模式每笔等待 120s→60s：
        # 单标的从最坏 ~4.2 分钟（120+10+120）降到 ~2.2 分钟（60+10+60），
        # 第二/三轮才能在收盘前落进可成交窗口（2026-09-21 15:55 起强平
        # 拖过 16:00、收盘后 40 分钟空转的审查发现）。
        status = await wait_for_trade_completion(trade, timeout_seconds=60 if force else 120)

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
        # 【2026-09-27】wait 已升级：到达这里时订单要么**终态**（部分成交+Cancelled，
        # 剩余量已死，下面 cancel 为无操作幂等），要么**超时**（仍在工作 → 撤剩余挂单）。
        # 旧竞态（部分成交+仍工作即被判"未平"并撤杀剩余量）已随统一 wait 消除。
        # 规则不变：未全部成交一律视为"未平仓"——绝不提前写 CSV，
        # 交回重试/下一轮按实况数量补平（重试前复核已平 → 按实际成交补记账）。
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

    async def force_close_until_flat(self, timeout_minutes: int = 10,
                                     close_deadline: datetime.datetime = None) -> bool:
        """
        强制平仓循环（收敛保证版）

        每轮基于最新权威持仓快照，对任意方向的非零持仓提交反向平仓：
        - 标准腿（账户1空头/账户2多头）照常回写CSV平仓字段；
        - 反向残留（账户1多头/账户2空头，通常由"已Cancel订单延迟成交 + 重试补单"
          双重成交造成，或前日遗留仓如EOSE）→ 平仓并记CRITICAL，不写CSV。
        如此循环，无论TWS延迟成交如何乱序，都会逐轮收敛到两账户全平。

        Args:
            timeout_minutes: 本方法的总时限上限（秒级安全边界，默认 10 分钟）；
            close_deadline: **绝对**收盘时刻（naive 本地时区，须与 datetime.now() 同基准）。
                到达即停止开下一轮并返回 False——收盘后普通市价单无法再成交，
                继续重试只会空转。传 None 时退化为仅 timeout_minutes 上限（测试/兜底）。

        Returns:
            bool: True=全部清仓完成; False=到达时限/收盘仍有残留
        """
        # 进入强制平仓窗口：运行时策略让路（runner.stop() 由 hedge_trade 在调用前触发）
        self.force_close_active = True
        try:
            deadline = datetime.datetime.now() + datetime.timedelta(minutes=timeout_minutes)
            if close_deadline is not None:
                deadline = min(deadline, close_deadline)
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

                # 【2026-09-28 收盘即停】每轮结束先查是否已到收盘：
                # 已到收盘则不再开下一轮（收盘后普通市价单无法再成交，继续只会空转）
                now = datetime.datetime.now()
                if close_deadline is not None and now >= close_deadline:
                    self.logger.critical(
                        "🚨 已到收盘（收盘后普通市价单无法再成交）—— 停止重试，转人工"
                    )
                    break
                # 收尾睡眠不越过 deadline，避免把强平窗口拖出收盘/时限
                remaining = (deadline - now).total_seconds()
                await asyncio.sleep(max(0.0, min(CLOSE_CHECK_INTERVAL, remaining)))

            if close_deadline is not None and \
                    datetime.datetime.now() >= close_deadline - datetime.timedelta(seconds=1):
                self.logger.critical(
                    "🚨 收盘仍未清仓（可能停牌/流动性差）—— 收盘后不再重试，请立即人工核对"
                    "TWS两个账户并手动平仓，勿留隔夜仓位！"
                )
            else:
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
        # 【2026-09-22 硬化】snap_failed=True 时下面"空 remaining"不可信——
        # 下游补写据此【整账户禁用】"记已平"（旧版失败后 remaining={} 仍照常补写，
        # 强平失败的残留仓位会被补记成已平）。重试版降低退出时点抖动误判。
        snap_failed = False
        try:
            snap1 = await get_positions_strict_with_retry(self.ib1, self.account1)
            snap2 = await get_positions_strict_with_retry(self.ib2, self.account2)
            remaining1 = {s: v['position'] for s, v in snap1.items() if v['position'] != 0}
            remaining2 = {s: v['position'] for s, v in snap2.items() if v['position'] != 0}
        except Exception as e:
            snap_failed = True
            self.logger.critical(
                f"🚨 退出前持仓终态确认失败: {e} —— 无法确认两账户是否真正清仓，"
                f"本次退出不补记任何'已平'，请立即人工核对TWS两个账户的持仓!"
            )
            remaining1, remaining2 = {}, {}

        if snap_failed:
            # CRITICAL 已在取数失败处给出；"已清仓"判定不成立，禁止宣称 ✅
            # 【2026-09-27 硬化】终态持仓未知时，**任何路径**都不得补记"已平"：
            # 事件流路径已由 holding_symbols=None 整账户禁用，但旧 CSV 兜底路径
            # (_backfill_csv) 此前仍会仅凭 fills 把账上全部未平行标记为已平——
            # 即"取数失败后仍把账本强制成两账户0持仓"（实际可能仍有残留仓）。
            # 统一口径：到此直接结束，留给"退出前持仓终态确认失败"的人工核查。
            self.logger.critical(
                "🚨 退出补写整体取消（事件流 + 旧CSV 双路径）—— 终态持仓快照不可用，"
                "无法确认任何标的已清仓，禁止把账本补记成'两账户0持仓'；"
                "请立即人工核对TWS两账户实际持仓后再入账!"
            )
            return
        elif remaining1 or remaining2:
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

        # 【R9】退出回填前核对：收盘方向 fills 总量 ≥ 计划平仓量；不足则等 5s 重取一次
        # （2026-09-21 15:55 MSTR 卖单 fills 未全部到达时，旧成交被错配到 fake lot 的根因）
        fills1, fills2 = await self._ensure_fills_coverage(fills1, fills2)

        if self.trade_store is not None:
            # ==================== 事件流回填（新路径） ====================
            # 账户1 平仓 = BUY 方向成交；账户2 平仓 = SELL 方向成交
            # 【守卫】holding = 严格快照确认仍持仓的标的集合（事实）；
            #         snap_failed → None = 该账户无法确认任何标的已平 → 全部禁止"记已平"
            hold1 = None if snap_failed else set(remaining1.keys())
            hold2 = None if snap_failed else set(remaining2.keys())
            p1 = self._backfill_store_account('account1', fills1, 'BUY', holding_symbols=hold1)
            p2 = self._backfill_store_account('account2', fills2, 'SELL', holding_symbols=hold2)
            self.logger.info(f"✅ 事件流补写完成: account1 {p1} 行, account2 {p2} 行")
            return

        # ==================== 旧 CSV 回填（trade_store 不可用兜底） ====================
        # 补写 sell.csv（账户1的平仓 = buy 操作）
        self._backfill_csv(fills1, 'BUY', 'sell', self.sell_csv_path)
        # 补写 buy.csv（账户2的平仓 = sell 操作）
        self._backfill_csv(fills2, 'SELL', 'buy', self.buy_csv_path)

    # ------------------------------------------------------------------
    # 【R9】退出回填前 fills 覆盖校验
    # ------------------------------------------------------------------
    def _planned_close_by_symbol(self, account: str) -> dict:
        """该账户的计划平仓量 = 未完成开仓

        - 事件流：每标的未平批次之和（get_open_lots remaining 求和）；
        - 旧 CSV：该账户 CSV 中未平（无平仓行）的 vol 合计。
        """
        planned = {}
        if self.trade_store is not None:
            for code in self.trade_store.open_codes(account):
                vol = sum(int(l['remaining']) for l in self.trade_store.get_open_lots(account, code))
                if vol > 0:
                    planned[code] = vol
            return planned

        path = self.sell_csv_path if account == 'account1' else self.buy_csv_path
        if not path:
            return planned
        try:
            import pandas as pd
            df = pd.read_csv(path)
            if df.empty or 'close_vol' not in df.columns:
                return planned

            def _blank(v) -> bool:
                s = str(v).strip().lower()
                return s in ('', 'nan', 'none', 'nat')

            for _, row in df.iterrows():
                if _blank(row.get('close_vol')) or _blank(row.get('close_datetime')):
                    try:
                        v = int(float(row['vol']))
                    except Exception:
                        continue
                    if v > 0:
                        code = str(row['code'])
                        planned[code] = planned.get(code, 0) + v
        except Exception as e:
            self.logger.warning(f"⚠️ [R9] 读取计划平仓量(旧CSV)失败: {e}")
        return planned

    @staticmethod
    def _fills_total_by_symbol(fills, close_side: str) -> dict:
        """收盘方向成交按标的合并股数（normalize_side 兼容 BOT/SLD 协议值）"""
        totals = {}
        for f in fills or []:
            try:
                if normalize_side(f.execution.side) != normalize_side(close_side):
                    continue
                # 【只算股票】账户里同名期权/期货的成交不得计入该股票的平仓量
                # （fills 仅按 symbol 匹配时，期权成交会污染 R9 覆盖校验与均价）
                c = getattr(f, 'contract', None)
                if (getattr(c, 'secType', None) or 'STK') != 'STK':
                    continue
                shares = abs(int(float(f.execution.shares)))
                sym = getattr(c, 'symbol', None)
            except Exception:
                continue
            if not sym or shares <= 0:
                continue
            totals[sym] = totals.get(sym, 0) + shares
        return totals

    async def _ensure_fills_coverage(self, fills1, fills2) -> tuple:
        """【R9】退出回填前核对：fills() 取数后校验各账户
        「收盘方向 fills 总量 ≥ 计划平仓量」，不足则等 5s 重取一次。

        2026-09-21 15:55 MSTR 事故：退出时部分卖单 fills 尚未全部到达，
        回填逻辑拿更早的无关成交顶替、错配到 fake lot；本前置校验让
        "fills 不足"被显式发现并等待到齐。
        """
        checks = [
            (self.ib1, 'account1', 'BUY', fills1),
            (self.ib2, 'account2', 'SELL', fills2),
        ]
        short = []
        for _ib, account, side, fills in checks:
            planned = self._planned_close_by_symbol(account)
            if not planned:
                continue
            got = self._unbooked_have_by_symbol(account, fills, side)
            for sym, need in sorted(planned.items()):
                have = got.get(sym, 0)
                if have < need:
                    short.append(f"{account}[{sym}] 未入账fills {have} < 计划平仓 {need}")
        if not short:
            return fills1, fills2

        self.logger.warning(
            f"⚠️ [R9] 收盘方向未入账 fills 不足: {'; '.join(short)} —— "
            f"等待 {FILLS_COVERAGE_RETRY_SECONDS} 秒后重取一次"
        )
        await asyncio.sleep(FILLS_COVERAGE_RETRY_SECONDS)
        orig1, orig2 = fills1, fills2
        try:
            fills1 = self.ib1.fills()
            fills2 = self.ib2.fills()
        except Exception as e:
            self.logger.error(f"❌ [R9] 重取 fills 失败: {e} —— 沿用首次取数结果")
            fills1, fills2 = orig1, orig2
        # 重取后审计记录
        for _ib, account, side, _fills in checks:
            planned = self._planned_close_by_symbol(account)
            if not planned:
                continue
            src = fills1 if account == 'account1' else fills2
            got = self._unbooked_have_by_symbol(account, src, side)
            still = [f"{sym} {got.get(sym, 0)}/{need}"
                     for sym, need in sorted(planned.items()) if got.get(sym, 0) < need]
            if still:
                self.logger.error(
                    f"🚨 [R9] 重取后收盘方向未入账 fills 仍不足: {'; '.join(still)} —— "
                    f"回填将按实际到手的 fills 执行（并受'仍持仓不记已平'守卫），请人工核对账本"
                )
            else:
                self.logger.info(f"✅ [R9] {account} 收盘方向未入账 fills 已充足")
        return fills1, fills2

    def _unbooked_have_by_symbol(self, account: str, fills, close_side: str) -> dict:
        """【2026-09-22】按标的的【未入账】收盘方向成交股数

        = max(0, 会话同方向 fills 总量 − 当日已入账平仓量)。
        旧版用"全天同方向 fills"当 have，已被入账的旧成交（如早上收敛循环
        卖出的超额部分）会把"未到货的新 fills"凑够数 → R9 覆盖校验形同虚设。
        """
        totals = self._fills_total_by_symbol(fills, close_side)
        if not totals or self.trade_store is None:
            return {k: int(v) for k, v in totals.items()}
        today = datetime.date.today()
        out = {}
        for sym, vol in totals.items():
            try:
                booked = int(self.trade_store.close_volume_since(account, sym, today))
            except Exception:
                booked = 0
            out[sym] = max(0, int(vol) - booked)
        return out

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

    def _backfill_store_account(self, account: str, fills, close_side: str,
                                holding_symbols=None) -> int:
        """退出前事件流回填：只把【未入账的平仓成交】写入平仓事件（2026-09-22 硬化）

        旧版三个"还拿着仓位却记成已平"的缺陷（已修）：
        1) 补记数量恒为全部未平批次（remaining_to_close = remaining），不按实际成交封顶
           —— 强平失败时，只要当天有过同方向成交，残留仓位就会被补记成"已平"；
        2) 成交总量/均价按"全天同方向的所有成交"计算，包含当日已入账的平仓
           （如收敛循环同轮超平卖出）→ 数量与均价双污染、"成交>未平批次"误报；
        3) 账户仍持仓（强平失败/残留）也照常补记"已平"。

        新版规则（每账户每标的）：
        - 【守卫】holding_symbols 为严格快照确认"仍持仓"的标的集合：
          * None = 快照取数失败，无法确认任何标的已平 → **整账户禁止补记"已平"**；
          * 集合内标的 → 不补记、只 CRITICAL（残留仓位，人工处理）。
        - 【去重】未入账成交 = max(0, 会话同方向 fills − 当日已入账平仓量)
          （execId 去重的数量等价：账本平仓行即"已入账"证据，不扩 CSV 列）。
        - 【封顶】补记量 = min(未入账成交, 未平批次)；
          未入账 > 未平（重复成交/延迟开仓）→ 只记未平部分并 CRITICAL；
          未入账 < 未平（跨日延迟入账等）→ 只记可证明部分并 CRITICAL。
        - 【价格】只用"未入账部分"的成交定价（按成交时间取最近 unbooked 股，
          早前的成交视为已入账），均价不再混新旧。
        - 每批一行（related_lot_id 关联），event_type=RECONCILE，strategy=reconcile；
          入账时间沿用 _resolve_close_time 的时区加固逻辑（与旧路径一致）。
        """
        store = self.trade_store
        close_action = 'buy' if account == 'account1' else 'sell'
        account_dir = Path(store.account_dirs[account])
        patched = 0
        if not Path(account_dir).exists():
            return 0

        if holding_symbols is None:
            # 严格快照失败 → 无法确认任何标的已清仓 → 本账户一律不补记"已平"
            self.logger.critical(
                f"🚨 {account}: 退出补写整体禁用 —— 终态持仓快照失败，"
                f"无法确认任何标的已清仓；请人工核对 TWS 持仓后再手工入账（避免把残留仓位记成已平）"
            )
            return 0

        today = datetime.date.today()

        for stock_csv in sorted(Path(account_dir).glob('*.csv')):
            symbol = stock_csv.stem
            try:
                lots = store.get_open_lots(account, symbol)
            except Exception as e:
                self.logger.warning(f"⚠️ {symbol}: 读取未平批次失败: {e}")
                continue
            if not lots:
                continue
            remaining = sum(int(l['remaining']) for l in lots)
            if remaining <= 0:
                continue

            # 【守卫】该标的账户仍持仓（强平失败/残留）→ 绝不能记"已平"
            if symbol in holding_symbols:
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 账户仍持有实际持仓（严格快照确认），"
                    f"账本尚有未平批次 {remaining}股 —— 平仓并未真正完成，"
                    f"本次不补记'已平'，请立即人工处理该仓位!"
                )
                continue

            # 汇总该标的会话内收盘方向成交（含当日已入账的，稍后扣除）
            day_fills = []
            total_fill = 0
            for f in fills:
                if normalize_side(f.execution.side) != normalize_side(close_side):
                    continue
                if (getattr(f.contract, 'secType', None) or 'STK') != 'STK':
                    continue  # 【只算股票】同名期权/期货成交不得计入该股票平仓
                if f.contract.symbol != symbol:
                    continue
                try:
                    sh = abs(int(float(f.execution.shares)))
                except Exception:
                    continue
                if sh <= 0:
                    continue
                day_fills.append(f)
                total_fill += sh
            if total_fill <= 0:
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 账户已确认清仓、账本还有未平批次 {remaining}股，"
                    f"但会话内无同方向成交 —— 该批平仓可能跨日延迟入账（成交不在当日会话），"
                    f"未自动补记，请人工核对 Client Portal 成交明细后手工入账!"
                )
                continue

            # 【去重】未入账成交 = 会话同方向 fills − 当日已入账平仓量
            try:
                booked_today = int(store.close_volume_since(account, symbol, today))
            except Exception as e:
                self.logger.warning(
                    f"⚠️ {symbol}: 统计当日已入账平仓量失败: {e} —— "
                    f"去重按 0 处理（'仍持仓不记已平'守卫兜底）"
                )
                booked_today = 0
            unbooked = max(0, total_fill - booked_today)

            bookable = min(unbooked, remaining)
            if bookable <= 0:
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 会话同方向成交 {total_fill}股 已全部入账（当日已入账 {booked_today}股），"
                    f"未平批次 {remaining}股 无可补记成交 —— 请人工核对账本!"
                )
                continue
            if unbooked > remaining:
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 未入账平仓成交 {unbooked}股 > 未平批次 {remaining}股（会话成交 {total_fill} − 已入账 {booked_today}）—— "
                    f"可能有重复成交/延迟开仓，账本只记 {remaining}股，请立即核对账户实际持仓!"
                )
            elif bookable < remaining:
                # 账户已确认清仓但未入账 fills 不足以覆盖未平批次 → 缺口必须显式升级
                self.logger.critical(
                    f"🚨 {symbol} ({account}): 账户已确认清仓，但未入账 fills 仅 {unbooked}股 < 未平批次 {remaining}股"
                    f"（会话成交 {total_fill} − 已入账 {booked_today}）—— 账本只记 {bookable}股，"
                    f"缺口 {remaining - bookable}股 需人工核对 Client Portal 成交明细后入账!"
                )

            # 【价格纯净】按成交时间取最近的 unbooked 股（更早的视为已入账）
            def _t_utc(f):
                u = _to_utc_instant(getattr(getattr(f, 'execution', None), 'time', None))
                return u if u is not None else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

            ordered = sorted(day_fills, key=_t_utc)
            selected = []
            left = bookable
            for f in reversed(ordered):
                if left <= 0:
                    break
                sh = abs(int(float(f.execution.shares)))
                if sh <= 0:
                    continue
                selected.append(min(left, sh))
                left -= min(left, sh)
            sel_shares = sum(selected)
            # 与 selected 对应的成交（同序）定价
            sel_fills = [f for f in reversed(ordered)]
            sel_cost = 0.0
            for i, f in enumerate(sel_fills):
                if i >= len(selected):
                    break
                try:
                    sel_cost += selected[i] * float(f.execution.price)
                except Exception:
                    pass
            avg_price = (sel_cost / sel_shares) if sel_shares else 0.0

            # 入账时点 = 所选"未入账成交"中最新一笔（_resolve_close_time 内部取 max）
            dt_str = self._resolve_close_time(ordered[-len(selected):], symbol)

            remaining_to_close = bookable
            for lot in lots:
                if remaining_to_close <= 0:
                    break
                alloc = min(remaining_to_close, int(lot['remaining']))
                if alloc <= 0:
                    continue
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
                        f"平仓 {alloc}股 @ ${avg_price:.4f}（未入账成交部分，去重: 会话{total_fill} − 已入账{booked_today}）"
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
