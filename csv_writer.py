# -*- coding: utf-8 -*-
"""
CSV写入模块
负责将选股结果和交易记录写入CSV文件
包含并发写入重试机制，解决 Windows 下的 Permission denied 文件占用冲突
"""
import pandas as pd
import time
from pathlib import Path
from typing import List, Dict
from models import StockInfo, TradeRecord
from constants import SELECTED_STOCKS_COLUMNS, TRADE_RECORD_COLUMNS
from logger import get_logger


def write_selected_stocks(stocks: List[StockInfo], filepath: Path) -> bool:
    """
    写入筛选出的股票列表到CSV文件

    Args:
        stocks: 股票信息列表
        filepath: 输出文件路径

    Returns:
        bool: 是否成功写入
    """
    logger = get_logger()

    try:
        data = []
        for s in stocks:
            data.append({
                'date': s.date,
                'code': s.code,
                'exchange': s.exchange,
                'industry': s.industry,
                'open': s.open,
                'high': s.high,
                'low': s.low,
                'close': s.close,
                'volume': s.volume,
                'turnover_pct': s.turnover_pct,
                'Y': s.y
            })

        df = pd.DataFrame(data, columns=SELECTED_STOCKS_COLUMNS)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')

        logger.info(f"📝 已写入选股结果: {filepath.name} ({len(stocks)} 只)")
        return True

    except Exception as e:
        logger.error(f"❌ 写入选股结果失败: {e}")
        return False


def init_trade_csv(filepath: Path) -> bool:
    """
    初始化交易记录CSV文件（创建文件并写入表头）

    Args:
        filepath: 文件路径

    Returns:
        bool: 是否成功
    """
    logger = get_logger()

    try:
        df = pd.DataFrame(columns=TRADE_RECORD_COLUMNS)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.debug(f"初始化交易记录文件: {filepath.name}")
        return True
    except Exception as e:
        logger.error(f"❌ 初始化交易记录文件失败: {e}")
        return False


def append_trade_record(record: TradeRecord, filepath: Path) -> bool:
    """
    追加单条交易记录到CSV文件

    Args:
        record: 交易记录对象
        filepath: 输出文件路径

    Returns:
        bool: 是否成功
    """
    logger = get_logger()

    try:
        data = {
            'datetime': record.datetime,
            'code': record.code,
            'exchange': record.exchange,
            'industry': record.industry,
            'action': record.action,
            'entry_price': record.entry_price,
            'vol': record.vol,
            'total_cost': record.total_cost,
            'fund_used': record.fund_used,
            # 新增的平仓字段（建仓时默认为空字符串）
            'close_datetime': record.close_datetime,
            'close_price': record.close_price,
            'close_vol': record.close_vol,
            'close_fund': record.close_fund,
            'gross_profit': record.gross_profit,
            'profit': record.profit
        }

        df = pd.DataFrame([data], columns=TRADE_RECORD_COLUMNS)

        if not filepath.exists():
            # 文件不存在，创建并写入表头
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
        else:
            # 文件存在，追加写入（不写表头）
            df.to_csv(
                filepath, mode='a', header=False,
                index=False, encoding='utf-8-sig'
            )

        logger.debug(
            f"📝 交易记录: {record.code} {record.action} "
            f"{record.vol}股 @ ${record.entry_price:.2f}"
        )
        return True

    except Exception as e:
        logger.error(f"❌ 追加交易记录失败: {e}")
        return False


def update_close_data_in_csv(filepath: Path, code: str, open_action: str, close_data: Dict) -> bool:
    """
    更新CSV中对应开仓记录的平仓数据
    包含重试机制，解决并发平仓时 Windows 下的 Permission denied 文件占用冲突
    """
    logger = get_logger()

    if not filepath.exists():
        logger.error(f"❌ 文件不存在，无法更新: {filepath}")
        return False

    max_retries = 5
    for attempt in range(max_retries):
        try:
            df = pd.read_csv(filepath)

            # 兼容 NaN、空字符串、以及字符串 'nan'
            is_empty = (
                    df['close_datetime'].isna() |
                    (df['close_datetime'].astype(str).str.strip() == '') |
                    (df['close_datetime'].astype(str).str.lower() == 'nan')
            )

            mask = (
                    (df['code'] == code) &
                    (df['action'] == open_action) &
                    is_empty
            )

            if not mask.any():
                logger.warning(f"⚠️ 未找到 {code} ({open_action}) 的未平仓记录")
                return False

            idx = df[mask].index[-1]

            for k, v in close_data.items():
                if k in df.columns:
                    df.loc[idx, k] = v

            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.debug(f"📝 已更新 {code} 的平仓数据到 {filepath.name}")
            return True

        except PermissionError:
            # Windows 文件被其他异步任务占用，等待 200ms 后重试
            time.sleep(0.2)
        except Exception as e:
            logger.error(f"❌ 更新CSV平仓数据失败: {e}")
            return False

    logger.error(f"❌ 更新CSV平仓数据失败: 达到最大重试次数 (文件持续被占用)")
    return False