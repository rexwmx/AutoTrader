# -*- coding: utf-8 -*-
"""
工具函数模块
"""
import datetime
import pytz
from typing import Optional
from constants import DATETIME_FORMAT


def get_current_time_est() -> datetime.datetime:
    """获取当前美东时间"""
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est)


def get_today_str_est(format_str: str = '%Y-%m-%d') -> str:
    """获取美东时间的今日日期字符串"""
    return get_current_time_est().strftime(format_str)


def format_datetime(dt: Optional[datetime.datetime],
                    format_str: str = DATETIME_FORMAT) -> str:
    """
    格式化日期时间对象为字符串，默认精确到秒

    Args:
        dt: 日期时间对象
        format_str: 格式字符串，默认使用全局常量

    Returns:
        str: 格式化的字符串，None返回空字符串
    """
    if dt is None:
        return ''
    return dt.strftime(format_str)


def calculate_shares(price: float, fund_limit: float = 1000.0) -> int:
    """根据价格和资金限制计算可交易股数"""
    if price <= 0:
        return 0
    return int(fund_limit // price)