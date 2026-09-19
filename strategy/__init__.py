# -*- coding: utf-8 -*-
"""交易策略包"""
from strategy.base import Bar, PositionView, CloseSignal, BaseStrategy
from strategy.close_strategy import DynamicTPStrategy
from strategy.open_window import OpenWindowStrategy

__all__ = [
    'Bar', 'PositionView', 'CloseSignal', 'BaseStrategy',
    'DynamicTPStrategy', 'OpenWindowStrategy',
]
