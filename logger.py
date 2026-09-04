# -*- coding: utf-8 -*-
"""
日志模块
"""
import logging
import sys
from pathlib import Path
from typing import Optional

_logger: Optional[logging.Logger] = None


def setup_logging(log_dir: Path, log_file_name: str = 'hedge_trade.log') -> logging.Logger:
    """配置日志系统（文件+控制台）"""
    global _logger

    if _logger is not None and any(isinstance(h, logging.FileHandler) for h in _logger.handlers):
        return _logger

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / log_file_name

        logger = logging.getLogger('hedge_trade')
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        _logger = logger
        logger.info(f"日志系统初始化完成 | 日志文件: {log_file}")
        return logger

    except Exception as e:
        # 如果连日志文件都创建失败（如权限问题），直接在控制台报错并退出
        print(f"❌ 致命错误：无法初始化日志系统 ({log_dir / log_file_name})。原因: {e}")
        sys.exit(1)


def get_logger() -> logging.Logger:
    """获取全局日志记录器"""
    global _logger
    if _logger is None:
        # 未初始化时，返回一个基本的控制台 logger，避免报错
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(message)s'
        )
        _logger = logging.getLogger('hedge_trade')
    return _logger