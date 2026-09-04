# -*- coding: utf-8 -*-
"""
账户连接模块
负责连接和管理两个IBKR Paper账户
"""
import asyncio
from typing import Tuple, Optional
from ib_async import IB
from logger import get_logger


async def connect_account(host: str, port: int,
                          client_id: int, name: str) -> Optional[IB]:
    """
    连接到单个IBKR账户

    Args:
        host: TWS/Gateway主机地址
        port: TWS/Gateway端口
        client_id: 客户端ID（不同账户必须不同）
        name: 账户名称（用于日志标识）

    Returns:
        IB: 连接成功的IB实例，失败返回None
    """
    logger = get_logger()
    ib = IB()

    try:
        logger.info(f"正在连接 {name} ({host}:{port}, clientId={client_id})...")
        await ib.connectAsync(host, port, clientId=client_id)
        logger.info(f"✅ {name} 连接成功")
        return ib
    except Exception as e:
        logger.error(f"❌ {name} 连接失败: {e}")
        return None


def verify_paper_account(ib: IB, name: str) -> bool:
    """
    验证账户是否为Paper（模拟）账户

    判断规则：账户ID以字母'D'开头（如 DU123456）

    Args:
        ib: 已连接的IB实例
        name: 账户名称（用于日志）

    Returns:
        bool: True=Paper账户, False=非Paper账户或验证失败
    """
    logger = get_logger()

    try:
        accounts = ib.managedAccounts()
        if not accounts:
            logger.error(f"❌ {name}: 未找到任何账户")
            return False

        account_id = accounts[0]
        logger.info(f"{name}: 账户ID = {account_id}")

        if account_id.startswith('D'):
            logger.info(f"✅ {name}: 确认为Paper账户")
            return True
        else:
            logger.error(
                f"❌ {name}: 账户ID '{account_id}' 不以'D'开头，"
                f"不是Paper账户！程序终止"
            )
            return False

    except Exception as e:
        logger.error(f"❌ {name}: 验证账户失败: {e}")
        return False


async def connect_both_accounts(
        host: str,
        port1: int, client_id1: int,
        port2: int, client_id2: int
) -> Tuple[Optional[IB], Optional[IB], str, str]:
    """
    连接两个IBKR账户并验证均为Paper账户

    Args:
        host: TWS主机地址
        port1: 账户1端口
        client_id1: 账户1客户端ID
        port2: 账户2端口
        client_id2: 账户2客户端ID

    Returns:
        Tuple: (ib1, ib2, account1_id, account2_id)
               连接失败时返回 (None, None, '', '')
    """
    logger = get_logger()

    # 连接账户1（做空账户）
    ib1 = await connect_account(host, port1, client_id1, "账户1(做空)")
    if not ib1:
        return None, None, '', ''

    # 连接账户2（对冲账户）
    ib2 = await connect_account(host, port2, client_id2, "账户2(对冲)")
    if not ib2:
        ib1.disconnect()
        return None, None, '', ''

    # 验证账户1
    if not verify_paper_account(ib1, "账户1"):
        ib1.disconnect()
        ib2.disconnect()
        return None, None, '', ''

    # 验证账户2
    if not verify_paper_account(ib2, "账户2"):
        ib1.disconnect()
        ib2.disconnect()
        return None, None, '', ''

    account1 = ib1.managedAccounts()[0]
    account2 = ib2.managedAccounts()[0]

    logger.info(f"✅ 两个Paper账户连接成功: {account1} / {account2}")
    return ib1, ib2, account1, account2