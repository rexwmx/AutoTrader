# -*- coding: utf-8 -*-
"""策略抽象接口与数据契约

设计原则：
1. 所有策略输入/输出都是不可变数据类（frozen dataclass），避免框架与策略共享可变状态。
2. 策略不接触 IB、不接触 CSV、不接触全局变量——只处理 Bar + PositionView。
3. 状态跨 K 线持久化于策略实例（self.states），不使用模块级全局。
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Bar:
    """1分钟K线——策略唯一的时间驱动源

    注意：dt 是美东时区的 aware datetime（与 hedge.py 里建仓成交时间一致），
    便于跨模块比较而不会有时区陷阱。
    """
    symbol: str
    dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class PositionView:
    """框架注入的权威持仓视图

    策略通过这些字段判读当前持仓状态，无需自己调 IB。
    acc1 = 账户1（做空腿），acc2 = 账户2（做多腿）。
    pos 为负数表示空头，正数表示多头，0 表示无持仓。
    """
    symbol: str
    acc1_pos: int
    acc1_avg_cost: float
    acc2_pos: int
    acc2_avg_cost: float


@dataclass(frozen=True)
class CloseSignal:
    """策略的输出：平仓意图

    只描述"做什么"，不管"怎么做"。
    - side：'buy' 平空，'sell' 平多
    - open_action：用于账本匹配（做空腿对应 'sell'，做多腿对应 'buy'）
    - reason：审计日志，框架直接打日志
    - strategy：来源策略标识（dynamic_tp / open_window 等），纯审计字段，
      写入事件流 strategy 列；为空时执行层按平仓上下文补默认值
    - target_lot_id：指定要平的批次（lot）；为空 → 存储层按 FIFO 自动分配
    """
    symbol: str
    account: str        # 'account1' | 'account2'
    side: str           # 'buy' | 'sell'
    volume: int
    open_action: str    # 'sell' | 'buy'
    reason: str
    strategy: str = ''
    target_lot_id: str = ''


class BaseStrategy(ABC):
    """策略抽象基类

    生命周期（由 StrategyRunner 驱动）：
        on_entry(...)           建仓成交时（框架回调，可多次：sell腿、buy腿）
        on_bar(bar, pos)        每根新 1 分钟 bar（返回信号列表）
        on_execution_result()   框架执行完信号后（失败时策略可回滚去重标记）
        get_display_state()     给实时 CSV 提供展示字段（可选）
    """

    def on_entry(self, symbol: str, account: str,
                 price: float, volume: int, dt: datetime) -> None:
        """框架在建仓成交时调用"""
        pass

    @abstractmethod
    def on_bar(self, bar: Bar, pos: PositionView) -> list[CloseSignal]:
        """每根新 1 分钟 bar 调用一次，返回待执行的平仓信号列表"""
        raise NotImplementedError

    def on_execution_result(self, signal: CloseSignal, success: bool) -> None:
        """框架执行完信号后回调

        成功：策略保持去重标记（同一标的本次会话不再触发）
        失败：策略**应回滚去重标记**，让下一根 bar 有机会重试
        """
        pass

    def get_display_state(self, symbol: str) -> dict:
        """给实时 CSV 写行用的额外展示字段（默认为空）"""
        return {}
