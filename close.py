# -*- coding: utf-8 -*-
"""
平仓逻辑模块 (独立平仓版 + 分段式动态平仓线)
Sell持仓和Buy持仓独立判断、独立平仓，互不干扰
根据峰值涨幅/跌幅动态调整平仓回撤容忍度
"""
import asyncio
import datetime
import pandas as pd
from ib_async import IB
from order import submit_buy_order, submit_sell_order, get_fill_price, get_filled_volume
from monitor import wait_for_trade_completion
from csv_writer import update_close_data_in_csv
from logger import get_logger
from util import format_datetime
# 权威持仓快照（按实例锁串行化，基于 reqPositionsAsync 返回值）
from position import get_positions, get_positions_strict

ESTIMATED_COMMISSION_RATE = 0.0035
CLOSE_RETRY_DELAY = 10
CLOSE_MAX_RETRIES = 1
CLOSE_CHECK_INTERVAL = 30     # 强平循环轮间检查间隔（秒）


def normalize_side(side: str) -> str:
    """
    归一化成交方向为 'BUY'/'SELL'（大小写不敏感）。

    关键：IBKR TWS API 的 Execution.side 协议枚举值是 **'BOT'(买) / 'SLD'(卖)**，
    官方文档："Specifies if the transaction was buy or sale, BOT for bought, SLD for sold."
    ib_async 解码器把协议字符串原样透传，因此与 'BUY'/'SELL' 直接比较**永远为 False**
    （9.8 事故中 ASAN 退出前补写因此瘫痪，输出"无需补写"）。
    本函数同时兼容真实协议值与语义值，供所有 side 比较使用。
    """
    s = str(side).upper()
    return {'BOT': 'BUY', 'SLD': 'SELL'}.get(s, s)


def get_close_pct(peak_pct: float) -> float:
    """
    根据峰值涨幅/跌幅百分比，查表得到对应的平仓线百分比。

    峰值越大（盈利越多），允许的回撤也越大，平仓线越宽松。

    Args:
        peak_pct: 峰值涨幅/跌幅百分比（正数，如 5.0 表示 5%）

    Returns:
        float: 平仓线百分比（相对于建仓价的盈利保留比例）
               如果 peak_pct < 1 则返回 -1（不触发平仓）

    对应关系表：
        PEAK_PCT        CLOSE_PCT
        2%-3%           1%
        3%-4%           2%
        4%-10%          PEAK_PCT * 50%
        10%-15%         PEAK_PCT * 45%
        15%-20%         PEAK_PCT * 40%
        20%-25%         PEAK_PCT * 35%
        25%-30%         PEAK_PCT * 30%
        30%-60%         PEAK_PCT * 25%
        60%-100%        PEAK_PCT * 20%
        >100%           PEAK_PCT * 10%
    """
    if peak_pct < 1:
        return -1.0  # 峰值太小，不触发平仓
    elif peak_pct < 2:
        return 0.65
    elif peak_pct < 3:
        return 1.0
    elif peak_pct < 4:
        return 1.5
    elif peak_pct < 10:
        return peak_pct * 0.50
    elif peak_pct < 15:
        return peak_pct * 0.45
    elif peak_pct < 20:
        return peak_pct * 0.40
    elif peak_pct < 25:
        return peak_pct * 0.35
    elif peak_pct < 30:
        return peak_pct * 0.30
    elif peak_pct < 60:
        return peak_pct * 0.25
    elif peak_pct < 100:
        return peak_pct * 0.20
    else:
        return peak_pct * 0.10


class CloseManager:
    def __init__(self, ib1: IB, ib2: IB, account1: str, account2: str,
                 sell_csv_path, buy_csv_path):
        self.ib1 = ib1
        self.ib2 = ib2
        self.account1 = account1
        self.account2 = account2
        self.sell_csv_path = sell_csv_path
        self.buy_csv_path = buy_csv_path
        self.logger = get_logger()

        self.closed_symbols_acc1 = set()
        self.closed_symbols_acc2 = set()

        self.is_runtime_close_active = False
        self.runtime_close_start_time = None
        # 收市前强制平仓窗口标志：窗口内不再触发新的运行时平仓，
        # 避免运行时平仓任务与强制平仓循环并发对同一标的重复下单（过度平仓→反向残留）
        self.force_close_active = False

    def start_runtime_close(self, delay_minutes: int = 2):
        self.runtime_close_start_time = datetime.datetime.now() + datetime.timedelta(minutes=delay_minutes)
        self.logger.info(f"⏱️ 运行时平仓监控将在 {self.runtime_close_start_time.strftime('%H:%M:%S')} 激活")

    def _get_entry_datetime(self, symbol: str, action: str, csv_path) -> str:
        try:
            df = pd.read_csv(csv_path)
            mask = (df['code'] == symbol) & (df['action'] == action)
            if mask.any():
                return str(df[mask].iloc[-1]['datetime'])
        except Exception:
            pass
        return "未知时间"

    async def check_runtime_conditions(self, symbol: str, current_close: float,
                                       min_close_since_entry: float,
                                       max_close_since_entry: float):
        """
        独立检查账户1和账户2的平仓条件（分段式动态平仓线）

        - 使用权威持仓快照（reqPositionsAsync 返回值，按实例锁串行化），
          不再使用 ib.positions() 长寿命缓存：
          缓存的幽灵条目会对"不存在的持仓"触发平仓（真实后果是反向开仓→残留），
          半批读取会漏掉真实持仓。
        - 收市前强制平仓窗口内（force_close_active=True）不再触发新的运行时平仓，
          避免与强制平仓循环并发对同一标的重复下单。
        """
        if self.force_close_active:
            return

        if not self.is_runtime_close_active:
            if self.runtime_close_start_time and datetime.datetime.now() >= self.runtime_close_start_time:
                self.is_runtime_close_active = True
                self.logger.info("🟢 运行时平仓监控已激活")
            else:
                return

        try:
            snap1 = await get_positions(self.ib1, self.account1)
            snap2 = await get_positions(self.ib2, self.account2)
        except Exception as e:
            self.logger.warning(f"⚠️ {symbol}: 获取持仓快照失败，跳过本次运行时平仓检查: {e}")
            return
        pos1 = snap1.get(symbol)
        pos2 = snap2.get(symbol)

        # ==================== 账户1 (做空) 平仓条件 ====================
        if pos1 is not None and pos1['position'] < 0:
            if symbol not in self.closed_symbols_acc1:
                entry_price = float(pos1['avgCost'])
                if entry_price > 0 and min_close_since_entry > 0:
                    # cond1: 当前处于下跌状态（做空盈利方向）
                    cond1 = current_close < entry_price

                    if cond1:
                        # 计算实际峰值跌幅（正数）
                        peak_pct = (entry_price - min_close_since_entry) / entry_price * 100

                        # 查表得到平仓线
                        close_pct = get_close_pct(peak_pct)

                        if close_pct >= 0:
                            # cond2: 峰值跌幅达到 get_close_pct 的最低门槛（1%）
                            # 修复：旧式 min < entry*(1-peak_pct/100) 中右边恒等于 min，
                            # 化简为 min < min，永远为 False，导致运行时平仓从不触发
                            cond2 = peak_pct >= 1.0

                            # cond3: 当前价从最低点反弹，回升到平仓线以上
                            # 平仓线 = entry_price * (1 - peak_pct + close_pct)
                            close_line = entry_price * (1 - peak_pct / 100 + close_pct / 100)
                            cond3 = current_close >= close_line

                            if cond2 and cond3:
                                rebound_pct = (current_close - min_close_since_entry) / min_close_since_entry * 100
                                entry_dt = self._get_entry_datetime(symbol, 'sell', self.sell_csv_path)
                                reason = (
                                    f"账户1(做空)触发 | 建仓: {entry_dt} | "
                                    f"成本{entry_price:.2f}, "
                                    f"最低{min_close_since_entry:.2f}(跌{peak_pct:.1f}%), "
                                    f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
                                    f"现价{current_close:.2f}(反弹{rebound_pct:.1f}%)"
                                )
                                self.logger.info(f"⚡ 触发独立平仓: {symbol} | {reason}")
                                self.closed_symbols_acc1.add(symbol)
                                vol = abs(int(pos1['position']))
                                asyncio.create_task(self._execute_close_with_retry(
                                    self.ib1, self.account1, symbol, 'buy', vol, entry_price, 'sell', self.sell_csv_path
                                ))

        # ==================== 账户2 (做多) 平仓条件 ====================
        if pos2 is not None and pos2['position'] > 0:
            if symbol not in self.closed_symbols_acc2:
                entry_price = float(pos2['avgCost'])
                if entry_price > 0 and max_close_since_entry > 0:
                    # cond1: 当前处于上涨状态（做多盈利方向）
                    cond1 = current_close > entry_price

                    if cond1:
                        # 计算实际峰值涨幅（正数）
                        peak_pct = (max_close_since_entry - entry_price) / entry_price * 100

                        # 查表得到平仓线
                        close_pct = get_close_pct(peak_pct)

                        if close_pct >= 0:
                            # cond2: 峰值涨幅达到 get_close_pct 的最低门槛（1%）
                            # 修复：旧式 max > entry*(1+peak_pct/100) 中右边恒等于 max，
                            # 化简为 max > max，永远为 False，导致运行时平仓从不触发
                            cond2 = peak_pct >= 1.0

                            # cond3: 当前价从最高点回落，跌到平仓线以下
                            # 平仓线 = entry_price * (1 + close_pct)
                            close_line = entry_price * (1 + close_pct / 100)
                            cond3 = current_close <= close_line

                            if cond2 and cond3:
                                pullback_pct = (max_close_since_entry - current_close) / max_close_since_entry * 100
                                entry_dt = self._get_entry_datetime(symbol, 'buy', self.buy_csv_path)
                                reason = (
                                    f"账户2(做多)触发 | 建仓: {entry_dt} | "
                                    f"成本{entry_price:.2f}, "
                                    f"最高{max_close_since_entry:.2f}(涨{peak_pct:.1f}%), "
                                    f"平仓线{close_line:.2f}(保留{close_pct:.1f}%), "
                                    f"现价{current_close:.2f}(回撤{pullback_pct:.1f}%)"
                                )
                                self.logger.info(f"⚡ 触发独立平仓: {symbol} | {reason}")
                                self.closed_symbols_acc2.add(symbol)
                                vol = int(pos2['position'])
                                asyncio.create_task(self._execute_close_with_retry(
                                    self.ib2, self.account2, symbol, 'sell', vol, entry_price, 'buy', self.buy_csv_path
                                ))

    async def _position_now(self, ib: IB, account: str, symbol: str) -> int:
        """
        权威快照：该账户当前该标的持仓（股数，无持仓为0），禁止用长寿命缓存判断。
        使用失败严格版快照：TWS 取数失败必须向上抛出（由调用方按"未知"处理），
        绝不能被静默当成 0 持仓——那会让重试误判"已平完"而停止补单。
        """
        snap = await get_positions_strict(ib, account)
        return int(snap.get(symbol, {'position': 0})['position'])

    async def _execute_close_with_retry(self, ib: IB, account: str, symbol: str, close_action: str,
                                        volume: int, open_price: float, open_action: str, csv_path):
        """
        带重试的平仓执行（重试前复核实际持仓，防止“已成交+重试”双重平仓）

        Returns:
            bool: True=已平仓或仓位确认已归零; False=重试次数用尽仍未平完
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

            success = await self._execute_close(ib, symbol, close_action, volume, open_price, open_action, csv_path)
            if success:
                return True

        self.logger.error(f"❌ {symbol}: 平仓重试 {CLOSE_MAX_RETRIES} 次后仍失败")

    async def _execute_close(self, ib: IB, symbol: str, close_action: str,
                             volume: int, open_price: float, open_action: str, csv_path) -> bool:
        """执行单次平仓，返回是否成功"""
        self.logger.info(f"🔄 {symbol}: 开始执行 {close_action} 平仓 {volume}股...")

        trade = await submit_buy_order(ib, symbol, volume) if close_action == 'buy' else await submit_sell_order(ib,
                                                                                                                 symbol,
                                                                                                                 volume)
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

        open_fund = open_price * close_vol
        gross_profit = (open_fund - close_fund) if open_action == 'sell' else (close_fund - open_fund)

        commission = 0.0
        try:
            comm = trade.commission()
            if comm is not None and comm != 1.7976931348623157e+308:
                commission = comm
            else:
                commission = (volume + close_vol) * ESTIMATED_COMMISSION_RATE
        except Exception:
            commission = (volume + close_vol) * ESTIMATED_COMMISSION_RATE

        profit = gross_profit - commission

        close_data = {
            'close_datetime': format_datetime(datetime.datetime.now()),
            'close_price': round(close_price, 4),
            'close_vol': close_vol,
            'close_fund': round(close_fund, 2),
            'gross_profit': round(gross_profit, 2),
            'profit': round(profit, 2)
        }

        if csv_path is None:
            # 反向残留/历史残留仓：没有对应的开仓记录，不能回写CSV
            self.logger.critical(
                f"🚨 {symbol}: 残留仓平仓完成 {close_vol}股 @ ${close_price:.2f} "
                f"(非本次对冲开仓记录，未写入CSV，请人工登记核对)"
            )
        elif update_close_data_in_csv(csv_path, symbol, open_action, close_data):
            self.logger.info(f"✅ {symbol}: 平仓完成并更新CSV | 净利: ${profit:.2f}")
        else:
            self.logger.warning(f"⚠️ {symbol}: 平仓完成但更新CSV失败")
        return True

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
        - 反向残留（账户1多头/账户2空头，通常由“已Cancel订单延迟成交 + 重试补单”
          双重成交造成，或前日遗留仓如EOSE）→ 平仓并记CRITICAL，不写CSV。
        如此循环，无论TWS延迟成交如何乱序，都会逐轮收敛到两账户全平。

        Returns:
            bool: True=全部清仓完成; False=超时仍有残留
        """
        # 进入强制平仓窗口：运行时平仓让路，避免并发对同一标的重复下单
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

                # 重置已平仓标记（仓位仍在 = 必须可再平）
                self.closed_symbols_acc1.clear()
                self.closed_symbols_acc2.clear()
                await self._force_close_positions(pos1, pos2)
                await asyncio.sleep(CLOSE_CHECK_INTERVAL)

            self.logger.critical(
                f"🚨 平仓超时（{timeout_minutes}分钟），未清仓 —— 可能仍有残留持仓"
                f"（含反向残留），请立即人工核对TWS两个账户"
            )
            return False
        finally:
            self.force_close_active = False
            # 强制平仓窗口结束后，运行时平仓不再触发（临近/已过收市，只允许人工处理）
            self.is_runtime_close_active = False

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
            self.closed_symbols_acc1.add(symbol)
            jobs.append((f"账户1 {symbol}", self._close_position(
                self.ib1, self.account1, symbol, position,
                self.sell_csv_path if standard else None, 'sell', standard,
                float(self._pv(pos, 'avgCost')),
            )))

        for symbol, pos in pos2.items():
            position = int(self._pv(pos, 'position'))
            standard = position > 0
            self.closed_symbols_acc2.add(symbol)
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
            open_action if standard else None, csv_path
        )

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

        # 补写 sell.csv（账户1的平仓 = buy 操作）
        self._backfill_csv(fills1, 'BUY', 'sell', self.sell_csv_path)
        # 补写 buy.csv（账户2的平仓 = sell 操作）
        self._backfill_csv(fills2, 'SELL', 'buy', self.buy_csv_path)

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
            # 旧代码只要有任何一笔平仓成交就补写整行，部分成交也会被标记“已平仓”，
            # 直接造成 “CSV 显示已全部平仓、账户仍有持仓” 的错账。未覆盖的行必须保留未平仓标记。
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
            sym_fills = close_fills_by_symbol[symbol]
            last_fill_time = max(f.execution.time for f in sym_fills)
            close_dt_str = last_fill_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(last_fill_time, 'strftime') else str(last_fill_time)

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