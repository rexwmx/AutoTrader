# -*- coding: utf-8 -*-
"""
美股对冲交易主程序
"""
import sys
import asyncio
import datetime
from pathlib import Path

from models import StockInfo

sys.path.insert(0, str(Path(__file__).parent))

from ib_async import util

from config import (
    TWS_HOST, ACCOUNT1_PORT, ACCOUNT1_CLIENT_ID,
    ACCOUNT2_PORT, ACCOUNT2_CLIENT_ID, DB_URL, BASE_RUNTIME_DIR,
    MINUTE_DATA_DIR, LOG_FILE_NAME, SELECTED_STOCKS_FILE,
    SELL_RECORDS_FILE, BUY_RECORDS_FILE,
    ACCOUNT1_DIR, ACCOUNT2_DIR
)
from constants import SELECTION_COUNT, BATCH_SIZE

from logger import setup_logging, get_logger
from runtime import create_runtime_dir, create_subdirs
from account import connect_both_accounts
from trading_calendar import check_trading_day, wait_until_time, get_current_time_est
from database import (
    get_db_engine, cleanup_locked_stocks, get_locked_stocks,
    get_previous_trade_date, fetch_stock_data, check_data_availability
)
from selector import select_stocks
from csv_writer import write_selected_stocks
from trade_store import TradeStore
from hedge import (
    pre_submit_sell_orders, confirm_short_positions,
)
from position import reconcile_positions, get_positions, PositionSnapshotError
from realtime import RealtimeDataRecorder
from close import CloseManager  # 新增导入

# 【新增】策略系统
from strategy import DynamicTPStrategy, OpenWindowStrategy
from strategy_runner import StrategyRunner


async def main():
    """主程序流程"""
    print("=" * 70)
    print("  美股对冲交易程序 v1.1")
    print("=" * 70)

    # ==================== 1. 极早期初始化 ====================
    # 1.1 先创建基础目录 (此时不使用 logger)
    try:
        runtime_dir = create_runtime_dir(BASE_RUNTIME_DIR)
        print(f"\n📁 运行时目录: {runtime_dir}")
    except Exception as e:
        print(f"❌ 无法创建运行目录: {e}")
        sys.exit(1)

    # 1.2 立即初始化日志系统 (确保后续所有错误都能写入文件)
    logger = setup_logging(runtime_dir, LOG_FILE_NAME)

    # 1.3 创建子目录（含事件流明细目录 account1/account2）
    subdirs = create_subdirs(runtime_dir, [MINUTE_DATA_DIR, ACCOUNT1_DIR, ACCOUNT2_DIR])
    minute_dir = subdirs[MINUTE_DATA_DIR]

    logger.info("=" * 70)
    logger.info("  美股对冲交易程序启动")
    logger.info(f"  运行时目录: {runtime_dir}")
    logger.info("=" * 70)

    # 1.4 定义输出文件路径
    selected_stocks_path = runtime_dir / SELECTED_STOCKS_FILE
    sell_csv_path = runtime_dir / SELL_RECORDS_FILE
    buy_csv_path = runtime_dir / BUY_RECORDS_FILE

    # 1.5 初始化交易数据存储（事件流明细 account1/account2 + 日汇总 sell.csv/buy.csv 表头）
    trade_store = TradeStore(runtime_dir)
    trade_store.init_summary_files()
    logger.info(f"📁 交易数据存储已初始化 | 明细: {runtime_dir / ACCOUNT1_DIR}, {runtime_dir / ACCOUNT2_DIR} | "
                f"汇总: sell.csv (account1) / buy.csv (account2)")

    # ============================================================
    # 阶段2: 连接IBKR账户
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("📡 阶段2: 连接IBKR账户")
    logger.info("=" * 50)

    ib1, ib2, account1, account2 = await connect_both_accounts(
        TWS_HOST,
        ACCOUNT1_PORT, ACCOUNT1_CLIENT_ID,
        ACCOUNT2_PORT, ACCOUNT2_CLIENT_ID
    )

    if not ib1 or not ib2:
        logger.error("❌ 账户连接失败，程序终止")
        return

    # ============================================================
    # 阶段3: 检查交易日
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("📅 阶段3: 检查交易日")
    logger.info("=" * 50)

    is_open, open_time, close_time, today_str = await check_trading_day(ib1)

    if not is_open:
        logger.error("❌ 今日非交易日，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # ---- 从 TWS 返回的开盘时间推导 open_dt（美东 aware），供阶段5.5（开盘前两分钟特殊策略）
    #      与阶段6（预挂单/确认/激活时序）共用 ----
    open_dt = None
    if open_time:
        try:
            open_dt = get_current_time_est().replace(
                hour=int(open_time.split(':')[0]),
                minute=int(open_time.split(':')[1]),
                second=0, microsecond=0
            )
            logger.info(
                f"🕗 开盘时间(TWS): {open_time} 美东 | "
                f"预报送: {open_dt - datetime.timedelta(seconds=60)} | "
                f"确认: {open_dt + datetime.timedelta(seconds=20)} | "
                f"开盘前两分钟特殊策略窗口: {open_dt.strftime('%H:%M:%S')} ~ "
                f"{(open_dt + datetime.timedelta(seconds=120)).strftime('%H:%M:%S')} | "
                f"策略激活边界: {open_dt + datetime.timedelta(seconds=60)}"
            )
        except Exception as e:
            logger.warning(
                f"⚠️ 解析开盘时间失败: {e} —— 按「立即执行 + 提交+20s确认」兜底；"
                f"开盘前两分钟特殊策略退化为 bar 序判定模式"
            )

    # ============================================================
    # 阶段4: 数据库操作
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("🗄️ 阶段4: 数据库操作")
    logger.info("=" * 50)

    engine = get_db_engine(DB_URL)

    # 获取当前美东日期
    now_est = get_current_time_est()
    current_date = now_est.strftime('%Y-%m-%d')

    # 4.1 清理过期的锁定股票
    cleanup_locked_stocks(engine, current_date)

    # 4.2 获取上一交易日
    prev_date = get_previous_trade_date(engine, current_date)
    if not prev_date:
        logger.error("❌ 无法获取上一交易日，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # 4.3 检查数据可用性
    if not check_data_availability(engine, prev_date):
        logger.error(f"❌ 上一交易日({prev_date})无数据，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # 4.4 获取锁定股票列表
    locked_codes = get_locked_stocks(engine)

    # ============================================================
    # 阶段5: 股票筛选
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("🔍 阶段5: 股票筛选")
    logger.info("=" * 50)

    # 5.1 获取上一交易日数据
    df_data = fetch_stock_data(engine, prev_date)
    if df_data.empty:
        logger.error("❌ 获取股票数据失败，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # 5.2 执行筛选
    selected_stocks = await select_stocks(
        ib1, df_data, locked_codes,
        prev_date, SELECTION_COUNT
    )

    if not selected_stocks:
        logger.error("❌ 未筛选到任何股票，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # 5.3 写入选股结果
    write_selected_stocks(selected_stocks, selected_stocks_path)

    # ============================================================
    # 阶段5.5: 组装策略系统
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("🧠 阶段5.5: 组装策略系统")
    logger.info("=" * 50)

    # 策略 = 决策者（输入 bar + 持仓，输出信号，不碰 IB/CSV/全局变量）
    # Runner = 翻译官（拉持仓、调策略、执行信号、回传结果）
    # CloseManager = 执行者（下单、重试、CSV 回写、强制平仓）
    # inner = 原有分段动态止盈策略；outer = 开盘前两分钟特殊策略
    # （命中 7 种情况之一 → 由 outer 定案：立即平仓 / 不操作；未命中 → inner 照常接管）
    open_window_inner = DynamicTPStrategy()
    strategy = OpenWindowStrategy(open_window_inner, open_time=open_dt)
    # trade_store：事件流账本（account1/account2/{symbol}.csv + sell.csv/buy.csv 汇总）；
    # None 时 CloseManager 自动退化为旧 CSV 回写路径（兜底）
    close_manager = CloseManager(
        ib1, ib2, account1, account2,
        sell_csv_path, buy_csv_path,
        trade_store=trade_store
    )
    runner = StrategyRunner(
        strategy=strategy,
        ib1=ib1, ib2=ib2, account1=account1, account2=account2,
        sell_csv_path=sell_csv_path, buy_csv_path=buy_csv_path,
        close_manager=close_manager,
        open_time=open_dt,
    )
    logger.info(f"🧠 策略: {strategy.__class__.__name__} ⊃ {open_window_inner.__class__.__name__}")

    # ============================================================
    # 阶段6: 执行对冲交易（优化时序：盘前预挂单 + 开盘确认 + 双通道并发）
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("💱 阶段6: 执行对冲交易 (开盘-60s 预挂单 / 开盘+20s 确认 / 双通道并发)")
    logger.info("=" * 50)

    # ---- 三个关键时点（不硬编码 09:29 / 09:30:20；open_dt 已在阶段3后推导）----
    # 预报送 = 开盘-60s（盘前空闲窗口，TWS 置 PreSubmitted 排队）
    # 确认定仓 = max(开盘+20s, 提交+20s)（程序晚启动时按提交时间顺延）
    # 策略激活 = 开盘+60s（第一根 1 分钟 bar 的收盘边界）

    # ---- 6.1 在 开盘-60s 预报送卖空订单（开盘钟声自动送入交易所撮合）----
    if open_dt:
        pre_submit_dt = open_dt - datetime.timedelta(seconds=60)
        logger.info(f"⏳ 等待至 {pre_submit_dt.strftime('%H:%M:%S')} (开盘-60s) 美东时间预挂单...")
        wait_until_time(pre_submit_dt.hour, pre_submit_dt.minute, pre_submit_dt.second)
    else:
        logger.info("⚠️ 无有效开盘时间，立即执行预报送（兜底路径）")

    submitted_trades = await pre_submit_sell_orders(
        ib1, selected_stocks, batch_size=BATCH_SIZE
    )
    if not submitted_trades:
        logger.critical("❌ 卖空订单全部预报送失败，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return
    submit_done_dt = get_current_time_est()  # 全部提交完成的时刻

    # ---- 6.2 在 max(开盘+20s, 提交+20s) 确认空头持仓并冻结 ----
    if open_dt:
        confirm_dt = max(open_dt + datetime.timedelta(seconds=20),
                         submit_done_dt + datetime.timedelta(seconds=20))
    else:
        confirm_dt = submit_done_dt + datetime.timedelta(seconds=20)
    logger.info(f"⏳ 等待至 {confirm_dt.strftime('%H:%M:%S')} 美东时间确认空头成交并撤残单...")
    wait_until_time(confirm_dt.hour, confirm_dt.minute, confirm_dt.second)

    try:
        confirmed_stocks, pos1_snap = await confirm_short_positions(
            ib1, account1, submitted_trades, sell_csv_path, runner=runner,
            trade_store=trade_store
        )
    except PositionSnapshotError as e:
        logger.critical(f"❌ 权威持仓快照获取失败: {e} —— 拒绝把取数失败误判为'无持仓'，程序终止，请人工核查两账户")
        ib1.disconnect()
        ib2.disconnect()
        return

    if not confirmed_stocks:
        logger.critical("❌ 确认时点无任何卖空订单成交，程序终止")
        ib1.disconnect()
        ib2.disconnect()
        return

    # ============================================================
    # 阶段7: 建仓收敛循环（R1）∥ 1分钟数据订阅（双通道保持）
    # 取代旧"通道1 买入补平(逐只3×5s核验) + 阶段7.5 三轮调平"：
    #   ≤5轮 / 总预算90s，每轮: 权威双快照 → 缺口/超额 → 并行发单 →
    #   逐单等待(20s上限)+filled优先终态判定 → 一次批量快照核验 →
    #   【同轮超平】实际>目标 → 当轮立即卖出超额 → 未收敛隔5s再来。
    # 收尾（循环内完成）: 终态快照 → P0 差额幂等补记 → 延迟标的补订阅/
    # 入场登记（R10 吸收旧阶段7.6）→ 终态确认（不一致 CRITICAL）。
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("🚀 阶段7: 建仓收敛循环 (缺口补买 ∥ 1分钟数据订阅 同时发起) + 同轮超平兜底")
    logger.info("=" * 50)

    recorder = RealtimeDataRecorder(ib1, minute_dir, runner)

    # 流心跳看门狗（2026-09-15 事故修复）
    # TWS↔IBKR 断连会杀死全部 1 分钟 keepUpToDate 流且恢复后不会自动重建；
    # 看门狗在 5 分钟无 bar 事件时自动诊断（API 连接 + 全量重订阅），
    # 回放数据只补写 CSV、不重放入策略。策略停用后（强制平仓窗口）自动静默。
    recorder.start_feed_watchdog(is_quiet=lambda: not runner.active)
    logger.info("🫀 数据流心跳看门狗已启动（断流后自动巡检修复策略已就绪）")

    # 【方案B】策略激活前移到收敛循环之前（生效时点 = 第一根bar边界 开盘+60s）。
    # 收敛循环只对本轮在调的标的做"标的级暂停"（R5），其余标的 bar 照常评估执行；
    # 被暂停标的的信号排队、轮末重放。循环若拖过 bar 边界，runner 自动顺延为立即生效。
    if open_dt:
        runner.start(first_bar_boundary=open_dt + datetime.timedelta(seconds=60))
    else:
        runner.start(delay_seconds=60)

    stock_info_map = {s.code: s for s in selected_stocks}
    confirmed_codes = {s.code for s in confirmed_stocks}

    # 收敛循环（通道1，ib2 下单/ib1+ib2 快照）与 1 分钟订阅（通道2，ib1 数据流）
    # 走不同 TCP 连接，并行发起——开盘一分钟预算靠这个重叠保住。
    converge_task = asyncio.ensure_future(
        reconcile_positions(
            ib1, ib2, account1, account2,
            sell_csv_path=sell_csv_path,
            buy_csv_path=buy_csv_path,
            stock_info_map=stock_info_map,
            trade_store=trade_store,
            runner=runner,
            recorder=recorder,
            confirmed_codes=confirmed_codes,
        )
    )
    sub_ok = await recorder.subscribe_all(confirmed_stocks)
    convergence = await converge_task
    logger.info(
        f"🎯 阶段7完成: 收敛={convergence['converged']} | "
        f"调平单 {convergence['success_count']}/{convergence['total_adjustments']} 成功 | "
        f"实际对冲 账户1={len(convergence['final_positions1'])}只, "
        f"账户2={len(convergence['final_positions2'])}只 | 1分钟订阅 {sub_ok}/{len(confirmed_stocks)}"
    )

    # ============================================================
    # 阶段8: 账实审计（只读断言）
    # 差额补记 / 延迟标的补订阅 / 入场登记 均已在阶段7收敛收尾（R10/P0）完成，
    # 这里只做"账 vs 实"的三向独立核对：出现缺口即 CRITICAL 人工跟进。
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("📡 阶段8: 账实审计 (三向核对：账本双边一致 / 账本↔TWS 双向吻合)")
    logger.info("=" * 50)

    # ==================== 关键修复：基于TWS实际持仓判断 ====================
    # 不依赖确认列表，而是检查TWS实际持仓
    # 使用权威快照（reqPositionsAsync 返回值），修复
    # "请求 + 固定sleep + 读长寿命缓存"的竞态（幽灵条目/半批读取会漏算对冲名单）
    await asyncio.sleep(2)  # 等待TWS完成收敛循环末批订单的持仓更新
    pos1_all = await get_positions(ib1, account1)
    pos2_all = await get_positions(ib2, account2)

    # 找出两个账户都有持仓的股票（即实际成功对冲的）
    actual_hedged_symbols = set(pos1_all.keys()) & set(pos2_all.keys())

    # 构建实际对冲股票列表（用于订阅1分钟数据）
    actual_hedged_stocks = []
    stock_info_map = {s.code: s for s in selected_stocks}
    for sym in actual_hedged_symbols:
        if sym in stock_info_map:
            actual_hedged_stocks.append(stock_info_map[sym])
        else:
            # 如果不在选股列表中，创建一个基本的StockInfo
            actual_hedged_stocks.append(StockInfo(
                code=sym, exchange='', industry='',
                open=0, high=0, low=0, close=0,
                volume=0, turnover_pct=0, y=0
            ))

    logger.info(
        f"📊 TWS实际持仓: 账户1={len(pos1_all)}只, 账户2={len(pos2_all)}只, "
        f"实际对冲={len(actual_hedged_symbols)}只"
    )

    # ==================== 对账审计：账本开仓记录 vs 实际对冲名单（双向） ====================
    # 修复 9.8 PL 事故根因：旧版只用"两侧并集 - 实际对冲名单"的子集检查，
    # "单边缺行"会被并集掩盖 → 假通过。三向核对：单边缺失 / 账本有而TWS无 / TWS有而账本两边都无。
    # （事件流账本下：开仓记录 = account1/account2 明细中存在未平完批次 remaining>0 的股票）
    try:
        sell_open = trade_store.open_codes('account1')
        buy_open = trade_store.open_codes('account2')

        # 1) 单边缺失：一边有开仓、另一边没有（对冲开仓本应双边对称，出现即记账错误）
        only_sell = sorted(sell_open - buy_open)
        only_buy = sorted(buy_open - sell_open)
        # 2) 账本有开仓但两账户无此对冲持仓（裸露风险，且不会进入1分钟监控名单）
        missing_hedge = sorted((sell_open | buy_open) - actual_hedged_symbols)
        # 3) TWS 有对冲持仓但账本两边都无开仓记录（正常应已被调平阶段补写，出现即补写失败）
        no_csv = sorted(actual_hedged_symbols - (sell_open | buy_open))

        if only_sell or only_buy or missing_hedge or no_csv:
            _parts = []
            if only_sell:
                _parts.append(f"sell侧有开仓但buy侧缺失: {only_sell}")
            if only_buy:
                _parts.append(f"buy侧有开仓但sell侧缺失: {only_buy}")
            if missing_hedge:
                _parts.append(f"账本有开仓但不在两账户实际对冲名单: {missing_hedge}")
            if no_csv:
                _parts.append(f"TWS有对冲持仓但两侧账本均无开仓记录: {no_csv}")
            logger.critical(
                "🚨 开仓对账发现缺口: " + "; ".join(_parts)
                + " —— 账本记录不完整，请人工核对TWS持仓与两账户事件流开仓记录"
            )
        else:
            logger.info(
                "✅ 开仓对账通过: account1/account2 两侧开仓清单相互一致，"
                "且与两账户实际对冲名单双向吻合"
            )
    except Exception as e:
        logger.warning(f"⚠️ 开仓对账检查失败: {e}")

    # ==================== 延迟成交标的补齐（R10：并入收敛循环收尾，一处完成） ====================
    # 历史日志证据（2026-09-10/09-11、2026-09-21 INFQ/MSTR）：多数标的的卖空实际是在
    # "确认时点之后"才落账的。旧流程在这里显式补齐（补订阅/on_entry/差额补记）；
    # 现已并入收敛循环收尾（position._finalize_convergence：终态快照 → P0 差额幂等补记
    # → 补订阅 → 入场登记 → 终态确认），此处只保留"账 vs 实"的只读审计。
    late_symbols = sorted(sym for sym in actual_hedged_symbols if sym not in confirmed_codes)
    if late_symbols:
        logger.info(
            f"🔁 延迟成交标的（确认时点后落账）: {late_symbols} —— "
            f"补订阅/入场登记/差额补记已由阶段7收敛收尾统一完成"
        )

    if actual_hedged_stocks:
        # （策略激活已前移到阶段7.5之前；生效时点 = 第一根bar边界 开盘+60s，
        #  若调平拖过边界则 runner 已自动顺延为立即生效）
        logger.info(
            f"\n✅ 所有阶段完成！\n"
            f"  实际对冲: {len(actual_hedged_stocks)} 只\n"
            f"  股票: {[s.code for s in actual_hedged_stocks]}\n"
            f"  1分钟实时数据流已在开盘第一分钟内接入；\n"
            f"  程序将持续运行，监控运行时平仓条件...\n"
            f"  收市前5分钟将自动触发强制平仓。"
        )

        # 计算强制平仓时间（收市前5分钟；与"✅ 所有阶段完成"日志的"收市前5分钟"一致）
        try:
            close_hour, close_minute = map(int, close_time.split(':'))
            now_est = get_current_time_est()
            close_dt = now_est.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
            force_close_dt = close_dt - datetime.timedelta(minutes=5)
            logger.info(f"⏰ 强制平仓触发时间设定为: {force_close_dt.strftime('%H:%M:%S')} 美东时间")
        except Exception as e:
            logger.error(f"❌ 解析收市时间失败: {e}")
            force_close_dt = None

        # 持续运行循环
        try:
            while True:
                now_est = get_current_time_est()
                if force_close_dt and now_est >= force_close_dt:
                    logger.info("⏰ 到达收市前5分钟，触发强制平仓...")
                    # 【新增】先让运行时策略让路，防止并发对同一标的重复下单
                    runner.stop()
                    # 强制平仓窗口不再需要行情驱动，停止流看门狗巡检（is_quiet 也会静默）
                    recorder.stop_feed_watchdog()

                    # 多轮强制平仓：单轮不收敛（超时）立即再开下一轮，
                    # 直到清仓或达到总时限 —— 绝不带着残留仓位静默退出
                    flat = False
                    deadline = datetime.datetime.now() + datetime.timedelta(minutes=45)
                    while not flat and datetime.datetime.now() < deadline:
                        flat = await close_manager.force_close_until_flat(timeout_minutes=10)
                        if not flat:
                            logger.critical("⚠️ 本轮强制平仓后仍有残留，30秒后继续下一轮...")
                            await asyncio.sleep(30)
                    if not flat:
                        logger.critical(
                            "🚨 45分钟内多轮强制平仓仍未清仓 —— 请立即人工核查两个账户的TWS持仓并手动平仓！"
                        )
                    break
                await asyncio.sleep(10)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("🛑 收到终止信号")

        # 退出前 CSV 补写
        logger.info("\n📋 执行退出前 CSV 补写...")
        await close_manager.reconcile_csv_with_fills()
    else:
        logger.warning("⚠️ TWS中无实际对冲持仓，跳过数据订阅和平仓管理")

    # ============================================================
    # 清理
    # ============================================================
    logger.info("\n🔌 程序结束，断开连接...")
    try:
        ib1.disconnect()
        ib2.disconnect()
    except Exception:
        pass
    logger.info("✅ 已断开所有连接，程序退出")


if __name__ == "__main__":
    try:
        # ib_async 需要修补 asyncio 以兼容
        util.patchAsyncio()
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
        sys.exit(0)
    except Exception as e:
        # ==================== 兜底崩溃日志 ====================
        # 如果程序在日志系统初始化前就崩溃，将错误写入固定的 crash.log
        crash_log_path = BASE_RUNTIME_DIR / 'crash.log'
        try:
            crash_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(crash_log_path, 'a', encoding='utf-8') as f:
                f.write(f"\n{'=' * 50}\n")
                f.write(f"崩溃时间: {datetime.datetime.now()}\n")
                f.write(f"错误信息: {e}\n")
                import traceback

                f.write(traceback.format_exc())
            print(f"\n❌ 程序异常终止，崩溃日志已写入: {crash_log_path}")
        except Exception:
            pass  # 如果连 crash.log 都写不进去，彻底放弃

        print(f"\n❌ 程序异常终止: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)