# -*- coding: utf-8 -*-
"""
配置模块
定义程序运行所需的各种配置参数
"""
from pathlib import Path

# ==================== TWS连接配置 ====================
TWS_HOST = '127.0.0.1'

# 账户1配置（做空账户）
ACCOUNT1_PORT = 7497
ACCOUNT1_CLIENT_ID = 1001

# 账户2配置（做多对冲账户）
ACCOUNT2_PORT = 7487
ACCOUNT2_CLIENT_ID = 1002

# ==================== 数据库配置 ====================
DB_URL = "postgresql://postgres:wangyang@localhost:5432/postgres"

# ==================== 路径配置 ====================
# 基础运行目录（基于当前项目根目录的相对路径，输出到 AutoTrader/trade_log）
BASE_RUNTIME_DIR = Path(__file__).resolve().parent / 'trade_log'

# 1分钟数据子目录名
MINUTE_DATA_DIR = '1min'

# 日志文件名
LOG_FILE_NAME = 'hedge_trade.log'

# 输出文件名
SELECTED_STOCKS_FILE = 'selected_stocks.csv'
SELL_RECORDS_FILE = 'sell.csv'
BUY_RECORDS_FILE = 'buy.csv'

# ==================== 时区配置 ====================
TIMEZONE_EST = 'US/Eastern'