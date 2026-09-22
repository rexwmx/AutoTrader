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

# ==================== 交易数据存储（事件流）目录配置 ====================
ACCOUNT1_DIR = 'account1'          # 账户1（做空账户）事件流目录
ACCOUNT2_DIR = 'account2'          # 账户2（做多对冲账户）事件流目录
SELL_SUMMARY_FILE = 'sell.csv'     # 账户1 日汇总（按股票一行，由事件流重建）
BUY_SUMMARY_FILE = 'buy.csv'       # 账户2 日汇总（按股票一行，由事件流重建）

# ==================== 时区配置 ====================
TIMEZONE_EST = 'US/Eastern'