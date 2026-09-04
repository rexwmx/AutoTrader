# -*- coding: utf-8 -*-
"""
股票筛选模块
负责根据策略筛选适合对冲的股票

筛选流程：
1. 基础过滤：市值、价格、换手率、排除锁定
2. 计算波动幅度Y并排序
3. 逐个验证资质（排除PINK/OTC/ETF/杠杆股/新股）
4. 选出目标数量的股票
"""
import pandas as pd
import numpy as np
from typing import List
from ib_async import IB, Stock
from models import StockInfo
from logger import get_logger
from constants import (
    ALLOWED_EXCHANGES, LEVERAGED_KEYWORDS, ETF_INDUSTRIES,
    MIN_LISTING_DAYS, MIN_MARKET_CAP, MIN_PRICE, MAX_PRICE,
    MIN_TURNOVER_PCT
)


def filter_basic_criteria(df: pd.DataFrame,
                          locked_codes: List[str]) -> pd.DataFrame:
    """
    应用基础筛选条件

    条件：
    1) market_cap >= 5百万
    2) 2 <= close <= 1000
    3) turnover_pct >= 0.2%
    4) 不在锁定清单中

    Args:
        df: 原始股票数据
        locked_codes: 锁定股票代码列表

    Returns:
        pd.DataFrame: 筛选后的数据
    """
    logger = get_logger()
    initial_count = len(df)
    logger.info(f"开始基础筛选，初始数量: {initial_count}")

    # 1) 市值筛选
    if 'market_cap' in df.columns:
        before = len(df)
        df = df[df['market_cap'] >= MIN_MARKET_CAP].copy()
        logger.info(f"  市值>={MIN_MARKET_CAP / 1e6}M: {before} → {len(df)}")

    # 2) 价格范围筛选
    if 'close' in df.columns:
        before = len(df)
        df = df[(df['close'] >= MIN_PRICE) & (df['close'] <= MAX_PRICE)].copy()
        logger.info(f"  价格{MIN_PRICE}-{MAX_PRICE}: {before} → {len(df)}")

    # 3) 换手率筛选
    if 'turnover' in df.columns and 'market_cap' in df.columns:
        # 计算换手率
        df['turnover_pct'] = np.where(
            df['market_cap'] > 0,
            df['turnover'] / df['market_cap'] * 100,
            0
        )
    if 'turnover_pct' in df.columns:
        before = len(df)
        df = df[df['turnover_pct'] >= MIN_TURNOVER_PCT].copy()
        logger.info(f"  换手率>={MIN_TURNOVER_PCT}%: {before} → {len(df)}")

    # 4) 排除锁定股票
    if locked_codes:
        before = len(df)
        df = df[~df['code'].isin(locked_codes)].copy()
        logger.info(f"  排除锁定股: {before} → {len(df)}")

    logger.info(f"基础筛选完成: {initial_count} → {len(df)}")
    return df


def calculate_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算股价波动幅度百分比 Y = ((High - Low) / Low) * 100
    按Y值降序排序，并过滤掉 Y <= 6% 的股票

    Args:
        df: 股票数据

    Returns:
        pd.DataFrame: 包含Y列、排序并过滤后的数据
    """
    logger = get_logger()

    if 'high' not in df.columns or 'low' not in df.columns:
        logger.warning("⚠️ 缺少 high/low 数据，波动幅度设为0")
        df = df.copy()
        df['Y'] = 0.0
        return df

    df = df.copy()
    df['Y'] = np.where(
        df['low'] > 0,
        ((df['high'] - df['low']) / df['low']) * 100,
        0.0
    )

    # 按Y降序排序
    df = df.sort_values('Y', ascending=False).reset_index(drop=True)

    before_count = len(df)

    # ==================== 过滤 Y <= 6% 的股票 ====================
    df = df[df['Y'] > 6.0].reset_index(drop=True)

    logger.info(
        f"波动幅度计算完成 | "
        f"最大Y: {df['Y'].max():.2f}% | "
        f"平均Y: {df['Y'].mean():.2f}% | "
        f"Y>6%过滤: {before_count} → {len(df)}"
    )

    return df


async def verify_stock_qualification(ib: IB, symbol: str) -> dict:
    """
    验证单个股票的资质

    排除：
    - PINK/OTC 交易所
    - ETF/基金
    - 杠杆/反向产品
    - 新股（上市不足30天）
    - 无行业分类的股票

    Args:
        ib: IB实例
        symbol: 股票代码

    Returns:
        dict: 验证通过的股票信息，不符合返回None
    """
    logger = get_logger()

    try:
        contract = Stock(symbol, 'SMART', 'USD')
        details = await ib.reqContractDetailsAsync(contract)

        if not details:
            return None

        d = details[0]
        primary_exch = d.contract.primaryExchange or ''
        industry = d.industry or ''
        long_name = (d.longName or '').upper()

        # 1. 交易所过滤
        if primary_exch not in ALLOWED_EXCHANGES:
            logger.debug(f"  ❌ {symbol}: 交易所不符 {primary_exch}")
            return None

        # 2. 行业分类检查
        if not industry.strip():
            logger.debug(f"  ❌ {symbol}: 无行业分类")
            return None

        if any(kw in industry for kw in ETF_INDUSTRIES):
            logger.debug(f"  ❌ {symbol}: ETF/基金")
            return None

        # 3. 杠杆/反向产品检查
        if any(kw in long_name for kw in LEVERAGED_KEYWORDS):
            logger.debug(f"  ❌ {symbol}: 杠杆/反向产品")
            return None

        # 4. 新股检查（历史数据天数）
        bars = None
        try:
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime='',
                durationStr=f'{MIN_LISTING_DAYS * 2} D',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True
            )
            if len(bars) < MIN_LISTING_DAYS:
                logger.debug(
                    f"  ❌ {symbol}: 新股 (历史{len(bars)}天 < {MIN_LISTING_DAYS}天)"
                )
                return None
        except Exception:
            logger.debug(f"  ❌ {symbol}: 无法获取历史数据")
            return None

        return {
            'symbol': symbol,
            'exchange': primary_exch,
            'industry': industry,
            'longName': d.longName,
            'history_days': len(bars) if bars else 0
        }

    except Exception as e:
        logger.debug(f"  ❌ {symbol}: 验证异常 - {e}")
        return None


async def select_stocks(
        ib: IB,
        df_data: pd.DataFrame,
        locked_codes: List[str],
        target_date: str,
        target_count: int = 100
) -> List[StockInfo]:
    """
    完整的股票筛选流程

    Args:
        ib: IB实例（用于验证资质）
        df_data: 原始股票数据
        locked_codes: 锁定股票代码列表
        target_date: 数据日期
        target_count: 目标筛选数量

    Returns:
        List[StockInfo]: 筛选出的股票列表
    """
    logger = get_logger()
    logger.info(f"{'=' * 50}")
    logger.info(f"开始股票筛选 | 目标数量: {target_count}")

    # 第一步：基础筛选
    df_filtered = filter_basic_criteria(df_data, locked_codes)

    if df_filtered.empty:
        logger.error("❌ 基础筛选后无股票")
        return []

    # 第二步：计算波动幅度并排序
    df_filtered = calculate_volatility(df_filtered)

    # 第三步：逐个验证资质
    # 取较多的候选进行验证（因为部分会被淘汰）
    candidate_count = min(target_count * 3, len(df_filtered))
    candidates = df_filtered.head(candidate_count).to_dict('records')

    logger.info(f"开始验证股票资质，候选数量: {len(candidates)}")

    selected = []
    verified_count = 0

    for row in candidates:
        if len(selected) >= target_count:
            break

        symbol = row['code']
        verified_count += 1

        verified = await verify_stock_qualification(ib, symbol)

        if verified:
            # 处理可能的NaN值
            open_price = float(row.get('open', 0)) if pd.notna(row.get('open')) else 0
            high_price = float(row.get('high', 0)) if pd.notna(row.get('high')) else 0
            low_price = float(row.get('low', 0)) if pd.notna(row.get('low')) else 0
            close_price = float(row.get('close', 0)) if pd.notna(row.get('close')) else 0
            volume = int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0
            turnover_pct = float(row.get('turnover_pct', 0)) if pd.notna(row.get('turnover_pct')) else 0
            y_value = float(row.get('Y', 0)) if pd.notna(row.get('Y')) else 0
            market_cap = float(row.get('market_cap', 0)) if pd.notna(row.get('market_cap')) else 0

            stock_info = StockInfo(
                code=symbol,
                exchange=verified['exchange'],
                industry=verified['industry'],
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                turnover_pct=turnover_pct,
                y=y_value,
                date=target_date,
                market_cap=market_cap
            )
            selected.append(stock_info)

            logger.info(
                f"  ✅ [{len(selected):3d}/{target_count}] "
                f"{symbol:6s} | {verified['exchange']:6s} | "
                f"Y={y_value:6.2f}% | {verified['industry'][:25]}"
            )

    logger.info(
        f"股票筛选完成 | 验证: {verified_count} | "
        f"选中: {len(selected)}/{target_count}"
    )

    return selected