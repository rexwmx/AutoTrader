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

        # ---------------- ib_async 的日志也写入同一日志文件 ----------------
        # ib_async 用独立的 logger（ib_async.client，父级 ib_async）。
        # 此前这里只配置了本项目的 'hedge_trade' logger，导致 ib_async 自身的日志
        # （限流、连接/断开、同步阶段错误、内部 error）没有任何文件 handler，
        # 只能落到 logging.lastResort → 打印到标准错误（控制台），日志文件里看不到。
        # 这里复用同一个文件 handler，让这部分信息也落进 hedge_trade.log。
        #   * ib_async.client 的 DEBUG 级协议报文（">>> send msg..."）量极大，
        #     因此 ib_async 侧级别取 WARNING（只收 error/warning，避免刷屏）；
        #   * propagate=False + 自带一个 stderr 控制台 handler，避免重复输出，
        #     同时保持与原先 lastResort 一致的控制台可见性。
        ib_logger = logging.getLogger('ib_async')
        if not any(isinstance(h, logging.FileHandler) for h in ib_logger.handlers):
            ib_logger.setLevel(logging.WARNING)
            ib_logger.addHandler(file_handler)
            ib_console = logging.StreamHandler(sys.stderr)
            ib_console.setLevel(logging.WARNING)
            ib_console.setFormatter(formatter)
            ib_logger.addHandler(ib_console)
            ib_logger.propagate = False

        _logger = logger
        logger.info(f"日志系统初始化完成 | 日志文件: {log_file}")
        logger.info("🔁 ib_async 内部日志（限流/连接/同步）已并入该日志文件")
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