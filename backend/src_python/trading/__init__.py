"""
Trident Trading Engine

Modular trading system with pluggable signal sources.

Architecture:
    Signal Sources → Signal Aggregator → Decision Engine → Risk Manager → Execution Engine
"""

from .signals import Signal, SignalSource, Direction
from .aggregator import SignalAggregator
from .decision import DecisionEngine, TradeOrder, ActionType, Position
from .risk import RiskManager
from .execution import ExecutionEngine, ExecutionMode
from .binance_execution import (
    BinanceExecutionEngine,
    ExecutionResult,
    create_binance_engine,
)
from .engine import TradingEngine

__all__ = [
    "Signal",
    "SignalSource",
    "Direction",
    "SignalAggregator",
    "DecisionEngine",
    "TradeOrder",
    "ActionType",
    "Position",
    "RiskManager",
    "ExecutionEngine",
    "ExecutionMode",
    "BinanceExecutionEngine",
    "ExecutionResult",
    "create_binance_engine",
    "TradingEngine",
]
