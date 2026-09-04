# -*- coding: utf-8 -*-
"""
交易日历模块
负责判断美股交易日和获取市场时间
"""
import datetime
import time as time_module
import pytz
from typing import Tuple
from ib_async import IB, Stock
from logger import get_logger


def get_current_time_est() -> datetime.datetime:
    """获取当前美东时间"""
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est)


async def check_trading_day(ib: IB) -> Tuple[bool, str, str, str]:
    """
    检查今日是否为美股交易日

    通过查询SPY合约的交易时间来判断

    Args:
        ib: 已连接的IB实例

    Returns:
        Tuple: (is_open, open_time, close_time, today_str)
               is_open: 是否交易日
               open_time: 开盘时间 (HH:MM)
               close_time: 收盘时间 (HH:MM)
               today_str: 今日日期 (YYYYMMDD)
    """
    logger = get_logger()

    try:
        # 使用SPY作为市场基准
        contract = Stock('SPY', 'SMART', 'USD')
        details = await ib.reqContractDetailsAsync(contract)

        if not details:
            logger.error("❌ 无法获取SPY合约详情，请检查TWS连接")
            return False, "", "", ""

        # 获取美东当前日期
        now_est = get_current_time_est()
        today_str = now_est.strftime('%Y%m%d')

        # 解析liquidHours（常规交易时段）
        liquid_hours = details[0].liquidHours
        schedules = liquid_hours.split(';')

        # 查找今日的交易安排
        today_schedule = None
        for s in schedules:
            if s.startswith(today_str):
                today_schedule = s
                break

        # 判断是否休市
        if not today_schedule or "CLOSED" in today_schedule.upper():
            logger.info(f"📅 今日({today_str})非交易日（休市）")
            return False, "", "", today_str

        # 解析交易时间 格式如: "20260715:0930-1600"
        parts = today_schedule.split('-')
        if len(parts) < 2:
            logger.error(f"❌ 无法解析交易时间: {today_schedule}")
            return False, "", "", today_str

        # 提取时间部分
        open_part = parts[0]
        close_part = parts[1]

        open_time_raw = open_part.split(':')[1] if ':' in open_part else open_part[-4:]
        close_time_raw = close_part.split(':')[1] if ':' in close_part else close_part[-4:]

        market_open = f"{open_time_raw[:2]}:{open_time_raw[2:]}"
        market_close = f"{close_time_raw[:2]}:{close_time_raw[2:]}"

        logger.info(
            f"📅 今日({today_str})为交易日 | "
            f"开盘: {market_open} | 收盘: {market_close} 美东时间"
        )
        return True, market_open, market_close, today_str

    except Exception as e:
        logger.error(f"❌ 检查交易日历出错: {e}")
        return False, "", "", ""


def wait_until_time(target_hour: int, target_minute: int) -> None:
    """
    阻塞等待至指定的美东时间

    Args:
        target_hour: 目标小时 (0-23)
        target_minute: 目标分钟 (0-59)
    """
    logger = get_logger()
    last_logged_minutes = -1  # 用于防止日志刷屏

    while True:
        now_est = get_current_time_est()
        current_time = now_est.time()
        target_time = datetime.time(target_hour, target_minute)

        # 已到达或超过目标时间
        if current_time >= target_time:
            return

        # 计算需要等待的秒数
        wait_seconds = (
                (target_time.hour - current_time.hour) * 3600 +
                (target_time.minute - current_time.minute) * 60 -
                current_time.second
        )

        if wait_seconds > 0:
            remaining_minutes = wait_seconds // 60

            # 只在剩余分钟数发生变化时打印日志，避免每5秒刷屏
            if remaining_minutes != last_logged_minutes:
                logger.info(
                    f"⏳ 等待至 {target_hour:02d}:{target_minute:02d} 美东时间... "
                    f"(剩余 {remaining_minutes}分{wait_seconds % 60}秒)"
                )
                last_logged_minutes = remaining_minutes

            # ==================== 修改点：每5秒检查一次 ====================
            time_module.sleep(min(5, wait_seconds))
        else:
            return