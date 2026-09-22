# -*- coding: utf-8 -*-
"""
常量定义模块
"""

# ==================== 时间格式配置 ====================
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# ==================== 交易所配置 ====================
ALLOWED_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']
EXCLUDED_EXCHANGES = ['PINK', 'OTC', 'OTCBB']

# ==================== 股票过滤关键词 ====================
LEVERAGED_KEYWORDS = [
    '2X', '3X', 'LEVERAGED', 'ULTRA', 'SHORT',
    'INVERSE', 'DAILY', 'BULL', 'BEAR', 'DIR'
]
ETF_INDUSTRIES = ['Funds', 'ETF', 'Trust', 'Index', 'SPDR']

# ==================== 筛选阈值 ====================
MIN_LISTING_DAYS = 30
MIN_MARKET_CAP = 1_000_000_000
MIN_PRICE = 2.0
MAX_PRICE = 1000.0
MIN_TURNOVER_PCT = 5

# ==================== 对冲参数 ====================
TARGET_HEDGED_COUNT = 10
SELECTION_COUNT = 100
FUND_PER_STOCK = 1000
BATCH_SIZE = 15
ORDER_TIMEOUT = 25

# ==================== CSV字段定义 ====================
SELECTED_STOCKS_COLUMNS = [
    'date', 'code', 'exchange', 'industry', 'open', 'high', 'low',
    'close', 'volume', 'turnover_pct', 'Y'
]

TRADE_RECORD_COLUMNS = [
    'datetime', 'code', 'exchange', 'industry', 'action',
    'entry_price', 'vol', 'total_cost', 'fund_used',
    'close_datetime', 'close_price', 'close_vol', 'close_fund',
    'gross_profit', 'profit'
]

# 增加了建仓后极值及百分比字段
REALTIME_COLUMNS = [
    'time', 'open', 'close', 'high', 'low', 'turnover', 'volume',
    'day_max', 'day_min', 'day_change_pct', 'day_change_pct_max', 'day_change_pct_min',
    'day_max_since_entry', 'day_min_since_entry',
    'day_max_since_entry_pct', 'day_min_since_entry_pct'
]

# ==================== 交易事件流字段（account1/account2 每只股票明细） ====================
# 事件类型：OPEN/ADD（开仓/加仓，生成 lot_id）
#          REDUCE/CLOSE/FORCE_CLOSE/RECONCILE（平仓事件，写 related_lot_id 按批次分配）
# 佣金口径：开仓行 commission = 单向往来（仅审计参考，不计入汇总）；
#          平仓行 commission = 该分配量的双向佣金（实际值按比例拆分优先，否则 2×估算）。
ACCOUNT_EVENT_COLUMNS = [
    'event_id', 'account', 'code', 'exchange', 'industry',
    'event_type', 'action', 'event_datetime',
    'price', 'volume', 'amount',
    'lot_id', 'related_lot_id', 'allocated_vol', 'remaining_vol_after',
    'gross_profit', 'commission', 'profit',
    'strategy', 'reason'
]

# ==================== 账户日汇总字段（sell.csv / buy.csv） ====================
ACCOUNT_SUMMARY_COLUMNS = [
    'date', 'account', 'code', 'exchange', 'industry', 'action',
    'lot_count', 'event_count',
    'open_vol', 'close_vol', 'remaining_vol',
    'open_fund', 'close_fund',
    'gross_profit', 'commission', 'profit',
    'first_event_time', 'last_event_time', 'status'
]