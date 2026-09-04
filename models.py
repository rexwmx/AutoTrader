# -*- coding: utf-8 -*-
"""
数据模型模块
定义程序中使用的数据类（Dataclasses）
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class StockInfo:
    """
    股票信息数据类
    存储筛选后的股票详细信息
    """
    code: str  # 股票代码
    exchange: str  # 交易所
    industry: str  # 行业分类
    open: float  # 开盘价
    high: float  # 最高价
    low: float  # 最低价
    close: float  # 收盘价
    volume: int  # 成交量
    turnover_pct: float  # 换手率(%)
    y: float  # 股价波动幅度百分比 Y=((High-Low)/Low)*100
    date: str = ''  # 数据日期
    market_cap: float = 0.0  # 市值


@dataclass
class TradeRecord:
    """
    交易记录数据类
    存储成交订单的详细信息
    """
    datetime: str  # 成交时间
    code: str  # 股票代码
    exchange: str  # 交易所
    industry: str  # 行业
    action: str  # 交易方向: 'buy' 或 'sell'
    entry_price: float  # 成交价格
    vol: int  # 成交数量
    total_cost: float  # 总成本
    fund_used: float  # 使用资金

    # ==================== 平仓数据字段 ====================
    # 建仓时为空，平仓后根据实际数据填入
    close_datetime: str = ''  # 平仓订单完成时间
    close_price: str = ''  # 平仓订单每股价格
    close_vol: str = ''  # 平仓订单股数
    close_fund: str = ''  # 平仓价格*股数
    gross_profit: str = ''  # 毛利润 (做空: fund_used - close_fund; 做多: close_fund - fund_used)
    profit: str = ''  # 净利润 (IB Realized PnL)


@dataclass
class HedgeOrder:
    """
    对冲订单数据类
    跟踪单个股票的完整对冲流程
    """
    stock: StockInfo  # 股票信息
    sell_trade: object = None  # 账户1卖空Trade对象
    buy_trade: object = None  # 账户2买入Trade对象
    sell_submit_time: Optional[datetime] = None  # 卖空提交时间
    buy_submit_time: Optional[datetime] = None  # 买入提交时间
    sell_fill_time: Optional[datetime] = None  # 卖空成交时间
    buy_fill_time: Optional[datetime] = None  # 买入成交时间
    sell_price: float = 0.0  # 卖空成交价
    buy_price: float = 0.0  # 买入成交价
    volume: int = 0  # 交易数量
    status: str = 'pending'  # 状态: pending/sell_filled/hedged/cancelled/failed/sell_only