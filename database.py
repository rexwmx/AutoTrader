# -*- coding: utf-8 -*-
"""
数据库操作模块
负责与PostgreSQL数据库的交互
包括：清理锁定股票、获取交易日、查询股票数据
"""
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List
from logger import get_logger


def get_db_engine(db_url: str):
    """
    创建SQLAlchemy数据库连接引擎

    Args:
        db_url: 数据库连接URL

    Returns:
        Engine: SQLAlchemy引擎对象
    """
    return create_engine(db_url)


def cleanup_locked_stocks(engine, current_date: str) -> None:
    """
    清理超过30天的锁定股票记录

    Args:
        engine: 数据库引擎
        current_date: 当前日期 (YYYY-MM-DD格式)
    """
    logger = get_logger()

    try:
        with engine.begin() as conn:
            delete_query = text("""
                DELETE FROM public.locked_stock_p
                WHERE start_date < (CAST(:current_date AS DATE) - INTERVAL '30 DAYS')
            """)
            result = conn.execute(delete_query, {"current_date": current_date})
            logger.info(f"🗑️ 已清理过期锁定股票: {result.rowcount} 条记录")
    except Exception as e:
        logger.error(f"❌ 清理锁定股票失败: {e}")


def get_locked_stocks(engine) -> List[str]:
    """
    获取当前有效的锁定股票代码列表

    Args:
        engine: 数据库引擎

    Returns:
        List[str]: 锁定股票代码列表
    """
    logger = get_logger()

    try:
        query = """
            SELECT DISTINCT code
            FROM public.locked_stock_p
            WHERE end_date >= CURRENT_DATE
        """
        df = pd.read_sql(query, engine)
        locked = df['code'].tolist() if not df.empty else []
        logger.info(f"🔒 当前锁定股票数量: {len(locked)}")
        return locked
    except Exception as e:
        logger.error(f"❌ 获取锁定股票失败: {e}")
        return []


def get_previous_trade_date(engine, current_date: str) -> Optional[str]:
    """
    获取上一个交易日的日期

    Args:
        engine: 数据库引擎
        current_date: 当前日期 (YYYY-MM-DD格式)

    Returns:
        Optional[str]: 上一交易日日期字符串，失败返回None
    """
    logger = get_logger()

    try:
        query = f"""
            SELECT cal_date
            FROM trade_calendar
            WHERE is_open = 1
            AND cal_date < '{current_date}'
            ORDER BY cal_date DESC
            LIMIT 1
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            logger.error("❌ 未找到上一交易日")
            return None

        prev_date = str(df.iloc[0]['cal_date'])
        logger.info(f"📅 上一交易日: {prev_date}")
        return prev_date
    except Exception as e:
        logger.error(f"❌ 获取上一交易日失败: {e}")
        return None


def fetch_stock_data(engine, target_date: str) -> pd.DataFrame:
    """
    从数据库获取指定日期的完整股票数据

    尝试获取包含 high/low 的完整数据，
    如果字段不存在则降级查询基础数据

    Args:
        engine: 数据库引擎
        target_date: 目标日期 (YYYY-MM-DD格式)

    Returns:
        pd.DataFrame: 股票数据，失败返回空DataFrame
    """
    logger = get_logger()

    # 完整查询：包含 high/low 价格
    try:
        query = f"""
            SELECT
                date,
                code,
                open_price::numeric AS open,
                high_price::numeric AS high,
                low_price::numeric AS low,
                close_price::numeric AS close,
                volume::numeric AS volume,
                turnover::numeric AS turnover,
                market_cap::numeric AS market_cap,
                pct_change::numeric AS pct_change
            FROM daily_price
            WHERE date = '{target_date}'
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.error(f"❌ 未找到 {target_date} 的股票数据")
            return pd.DataFrame()

        logger.info(f"📊 获取到 {len(df)} 条股票数据 (日期: {target_date})")
        return df

    except Exception as e:
        logger.warning(f"⚠️ 完整查询失败 (可能缺少high/low字段): {e}")

        # 降级查询：不包含 high/low
        try:
            query = f"""
                SELECT
                    date,
                    code,
                    open_price::numeric AS open,
                    close_price::numeric AS close,
                    volume::numeric AS volume,
                    turnover::numeric AS turnover,
                    market_cap::numeric AS market_cap,
                    pct_change::numeric AS pct_change
                FROM daily_price
                WHERE date = '{target_date}'
            """
            df = pd.read_sql(query, engine)

            if not df.empty:
                # 用 open/close 估算 high/low
                df['high'] = df[['open', 'close']].max(axis=1)
                df['low'] = df[['open', 'close']].min(axis=1)
                logger.warning(
                    f"⚠️ 使用降级查询，high/low 为估算值 | "
                    f"数据量: {len(df)}"
                )
            return df

        except Exception as e2:
            logger.error(f"❌ 降级查询也失败: {e2}")
            return pd.DataFrame()


def check_data_availability(engine, target_date: str) -> bool:
    """
    检查指定日期是否有可用的股票数据

    Args:
        engine: 数据库引擎
        target_date: 目标日期

    Returns:
        bool: True=有数据, False=无数据
    """
    logger = get_logger()

    try:
        query = f"""
            SELECT COUNT(*) as cnt
            FROM daily_price
            WHERE date = '{target_date}'
        """
        df = pd.read_sql(query, engine)
        count = int(df.iloc[0]['cnt'])

        if count > 0:
            logger.info(f"✅ 日期 {target_date} 有 {count} 条数据")
            return True
        else:
            logger.error(f"❌ 日期 {target_date} 无任何数据")
            return False

    except Exception as e:
        logger.error(f"❌ 检查数据可用性失败: {e}")
        return False