# -*- coding: utf-8 -*-
"""
账户连接模块
负责连接和管理两个IBKR Paper账户
"""
import asyncio
from typing import Tuple, Optional
from ib_async import IB
from logger import get_logger

# ==================== TWS 错误码语义分级 ====================
# ib_async 把 TWS 下发的所有 error（订单被拒 201、历史数据错误 162、限流、
# 断连 1100/1101 等）统一通过 errorEvent(reqId, errorCode, errorString, contract)
# 转发（见 ib_async/wrapper.py）。这里按错误码选择日志级别，便于告警区分。
# 具体含义参照 IBKR API Reference 的错误码表。
_TWS_ERROR_CODES = {
    # 行情 / 历史数据请求错误
    162, 167, 169,
    # 订单被拒 / 订单错误
    200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211,
    217, 221, 228, 275,
    # 限流 / 请求过快
    10191, 10197, 10199,
    # 连接 / 同步
    1100, 1101, 1103, 1110,
}
# "完成/状态"类，非真正错误（避免每次历史数据批处理完成就刷一条 error 的噪音）
_TWS_INFO_CODES = {161, 2158, 2159}


def install_tws_error_logger(ib: IB, tag: str) -> None:
    """为一个 IB 连接安装「全量 TWS 错误」日志监听器。

    - 记录每一条 TWS 错误事件：错误码、对应股票（若带合约）、错误信息；
    - 按错误码语义选择级别：info / warning / error，便于告警筛选；
    - 监听器内任何异常都不抛出——eventkit 会吞掉监听器异常，绝不允许影响 IB 事件循环。

    关键修复点：
      此前错误监听只挂在账户1(ib1)上，且只处理少数错误码（其余"为避免噪音"直接丢弃），
      账户2(ib2) 的下单报错（如 201 订单被拒 / 不能卖空）完全没有记录。
      现在每个 IB 实例都装上这一处全量监听器 —— 两个账户、全部错误码都能落进日志文件。
      且监听器绑在 IB 实例上，断线重连(reconnect 复用同一实例)后不会丢失。
    """
    logger = get_logger()

    def _on_tws_error(reqId=None, errorCode=None, errorString=None, contract=None) -> None:
        try:
            code = int(errorCode) if errorCode is not None else None
            sym = getattr(contract, 'symbol', None) if contract is not None else None
            sym_tag = f"[{sym}] " if sym else ""
            body = (f"{tag} {sym_tag}TWS 错误 {code}: {errorString}"
                    + (f" (reqId={reqId})" if reqId not in (None, 0) else ""))
            if code in _TWS_INFO_CODES:
                logger.info("ℹ️  " + body)
            elif code in _TWS_ERROR_CODES:
                logger.error("🚨  " + body)
            else:
                logger.warning("⚠️  " + body)
        except Exception as e:  # 兜底：监听器绝不允许把异常抛回 IB 事件循环
            try:
                logger.error(f"⚠️ {tag} TWS 错误监听器处理异常: {e}")
            except Exception:
                pass

    try:
        ib.errorEvent += _on_tws_error
    except Exception as e:
        logger.warning(f"⚠️ {tag} 安装 TWS 错误日志监听器失败: {e}")


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
    # ==================== 必须声明：TWS 以 UTC 回报时间 ====================
    # 背景（9.14 SBET/ASST、9.16 EOSE、9.18 GNRC/RXRX/ABSI 平仓时间异常事故）：
    # TWS/Paper 的成交回报时间是「UTC 墙钟」；ib_async 默认(TimezoneTWS='')会把 naive
    # 时间按机器本地时区(本机=美东)解释，解码出的时刻整体偏移 4 小时(EDT)，
    # 退出前 CSV 补写(close.py)若直接采用该时间会把平仓时间多写 8 小时。
    # 此处显式告知 ib_async「TWS 用的是 UTC」→ execution.time 即为正确 UTC 时刻；
    # close.py 补写时再统一转美东时间并做收到时刻交叉校验。
    # ⚠️ 请勿删除；若日后 TWS 时区设置(TWS: 文件→设置→时间和日历)改动，须同步更新此值。
    ib.TimezoneTWS = 'UTC'

    try:
        logger.info(f"正在连接 {name} ({host}:{port}, clientId={client_id})...")
        await ib.connectAsync(host, port, clientId=client_id)
        logger.info(f"✅ {name} 连接成功")
        # 挂全量 TWS 错误日志监听器（两个账户都要，账户2 的下单报错此前完全无记录）
        install_tws_error_logger(ib, name)
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