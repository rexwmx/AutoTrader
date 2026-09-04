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
    SELL_RECORDS_FILE, BUY_RECORDS_FILE
)
from constants import TARGET_HEDGED_COUNT, SELECTION_COUNT

from logger import setup_logging, get_logger
from runtime import create_runtime_dir, create_subdirs
from account import connect_both_accounts
from trading_calendar import check_trading_day, wait_until_time, get_current_time_est
from database import (
    get_db_engine, cleanup_locked_stocks, get_locked_stocks,
    get_previous_trade_date, fetch_stock_data, check_data_availability
)
from selector import select_stocks
from csv_writer import write_selected_stocks, init_trade_csv
from hedge import perform_hedging
from position import reconcile_positions
from realtime import RealtimeDataRecorder
from close import CloseManager  # 新增导入


async def main():
    """主程序流程"""
    print("=" * 70)
    print("  美股对冲交易程序 v1.0")
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

    # 1.3 创建子目录
    subdirs = create_subdirs(runtime_dir, [MINUTE_DATA_DIR])
    minute_dir = subdirs[MINUTE_DATA_DIR]

    logger.info("=" * 70)
    logger.info("  美股对冲交易程序启动")
    logger.info(f"  运行时目录: {runtime_dir}")
    logger.info("=" * 70)

    # 1.4 定义输出文件路径
    selected_stocks_path = runtime_dir / SELECTED_STOCKS_FILE
    sell_csv_path = runtime_dir / SELL_RECORDS_FILE
    buy_csv_path = runtime_dir / BUY_RECORDS_FILE

    # 1.5 初始化交易记录CSV文件
    init_trade_csv(sell_csv_path)
    init_trade_csv(buy_csv_path)

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
    # 阶段6: 执行对冲交易
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("💱 阶段6: 执行对冲交易")
    logger.info("=" * 50)

    # 6.1 等待开市时间
    if open_time:
        try:
            open_hour = int(open_time.split(':')[0])
            open_minute = int(open_time.split(':')[1])
            logger.info(f"⏳ 等待开市时间 {open_time} 美东...")
            wait_until_time(open_hour, open_minute)
            logger.info("✅ 已到开市时间")
        except Exception as e:
            logger.warning(f"⚠️ 等待开市时间异常: {e}")

    # 6.2 执行对冲
    hedged_stocks, all_orders = await perform_hedging(
        ib1, ib2, selected_stocks,
        runtime_dir, sell_csv_path, buy_csv_path,
        TARGET_HEDGED_COUNT
    )

    # ============================================================
    # 阶段6.5: 等待TWS完成订单执行（关键！）
    # ============================================================
    # Paper账户中，订单可能在程序判定Cancelled后仍被TWS执行
    # 必须等待足够时间让TWS完成所有订单处理
    logger.info("\n" + "=" * 50)
    logger.info("⏳ 阶段6.5: 等待TWS完成订单处理 (15秒)")
    logger.info("=" * 50)
    await asyncio.sleep(15)

    # ============================================================
    # 阶段7: 持仓调平
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("⚖️ 阶段7: 持仓调平")
    logger.info("=" * 50)

    # 构建股票信息映射（从 selected_stocks 中获取）
    stock_info_map = {s.code: s for s in selected_stocks}

    await reconcile_positions(
        ib1, ib2, account1, account2,
        sell_csv_path=sell_csv_path,
        buy_csv_path=buy_csv_path,
        stock_info_map=stock_info_map
    )

    # ============================================================
    # 阶段8: 订阅数据与平仓管理
    # ============================================================
    logger.info("\n" + "=" * 50)
    logger.info("📡 阶段8: 订阅数据与平仓管理")
    logger.info("=" * 50)

    # ==================== 关键修复：基于TWS实际持仓判断 ====================
    # 不依赖 hedged_stocks 列表，而是检查TWS实际持仓
    await asyncio.sleep(5)  # 等待TWS持仓数据稳定
    try:
        await ib1.reqPositionsAsync()
        await ib2.reqPositionsAsync()
        await asyncio.sleep(2)
    except Exception:
        pass

    pos1_all = {p.contract.symbol: p for p in ib1.positions() if p.position != 0}
    pos2_all = {p.contract.symbol: p for p in ib2.positions() if p.position != 0}

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

    if actual_hedged_stocks:
        # 初始化平仓管理器
        close_manager = CloseManager(
            ib1, ib2, account1, account2,
            sell_csv_path, buy_csv_path
        )
        close_manager.start_runtime_close(delay_minutes=2)

        # 订阅1分钟数据
        recorder = RealtimeDataRecorder(ib1, minute_dir, close_manager)
        await recorder.subscribe_all(actual_hedged_stocks)

        logger.info(
            f"\n✅ 所有阶段完成！\n"
            f"  实际对冲: {len(actual_hedged_stocks)} 只\n"
            f"  股票: {[s.code for s in actual_hedged_stocks]}\n"
            f"  程序将持续运行，监控运行时平仓条件...\n"
            f"  收市前10分钟将自动触发强制平仓。"
        )

        # 计算强制平仓时间
        try:
            close_hour, close_minute = map(int, close_time.split(':'))
            now_est = get_current_time_est()
            close_dt = now_est.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
            force_close_dt = close_dt - datetime.timedelta(minutes=10)
            logger.info(f"⏰ 强制平仓触发时间设定为: {force_close_dt.strftime('%H:%M:%S')} 美东时间")
        except Exception as e:
            logger.error(f"❌ 解析收市时间失败: {e}")
            force_close_dt = None

        # 持续运行循环
        try:
            while True:
                now_est = get_current_time_est()
                if force_close_dt and now_est >= force_close_dt:
                    logger.info("⏰ 到达收市前10分钟，触发强制平仓...")
                    await close_manager.force_close_until_flat(timeout_minutes=10)
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
        crash_log_path = Path(r'C:\trader\selection\hedge_trade\crash.log')
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