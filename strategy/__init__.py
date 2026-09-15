# -*- coding: utf-8 -*-
"""交易策略包"""
from strategy.base import Bar, PositionView, CloseSignal, BaseStrategy
from strategy.close_strategy import DynamicTPStrategy

__all__ = [
    'Bar', 'PositionView', 'CloseSignal', 'BaseStrategy',
    'DynamicTPStrategy',
]
