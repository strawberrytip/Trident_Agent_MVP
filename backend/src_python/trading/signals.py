"""
Signal Module - 标准化信号接口

所有信号源（新闻、CTA、自定义策略）都实现这个接口
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod


class Direction(Enum):
    """交易方向"""
    LONG = 1      # 做多
    SHORT = -1     # 做空
    FLAT = 0       # 平仓/观望


@dataclass
class Signal:
    """
    标准化信号数据结构

    Attributes:
        timestamp: 信号生成时间
        source: 信号源标识 (如 "Kimi K3", "PullbackV2", "Manual")
        direction: 交易方向
        strength: 信号强度 [0.0, 1.0]，1.0为最强
        symbol: 交易品种 (如 "XAUUSD", "BTCUSDT")

        price_data: 价格相关信息
        reason: 信号理由/逻辑
        confidence: 信号置信度 [0.0, 1.0]

        metadata: 扩展元数据，用于存储策略特定信息
        stop_loss: 建议止损价
        take_profit: 建议止盈价
        time_in_force: 订单有效期
        tags: 标签 (如 ["VIP", "Trend", "Breakout"])
    """
    timestamp: datetime
    source: str
    direction: Direction
    strength: float
    symbol: str = "XAUUSD"

    # 价格信息
    price_data: Dict[str, Any] = field(default_factory=dict)

    # 理由和置信度
    reason: str = ""
    confidence: float = 0.8

    # 扩展信息
    metadata: Dict[str, Any] = field(default_factory=dict)

    # 止损止盈
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    time_in_force: str = "GTC"  # GTC=成交取消, IOC=立即成交否则取消

    # 标签
    tags: list = field(default_factory=list)

    def __post_init__(self):
        """数据验证"""
        # 确保 strength 在 [0, 1] 范围内
        self.strength = max(0.0, min(1.0, self.strength))
        # 确保 confidence 在 [0, 1] 范围内
        self.confidence = max(0.0, min(1.0, self.confidence))

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于数据库存储）"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "direction": self.direction.name,
            "strength": round(self.strength, 4),
            "symbol": self.symbol,
            "price_data": self.price_data,
            "reason": self.reason[:200],
            "confidence": round(self.confidence, 4),
            "metadata": self.metadata,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "time_in_force": self.time_in_force,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Signal":
        """从字典创建信号（用于数据库读取）"""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            source=data["source"],
            direction=Direction[data["direction"]],
            strength=data["strength"],
            symbol=data.get("symbol", "XAUUSD"),
            price_data=data.get("price_data", {}),
            reason=data.get("reason", ""),
            confidence=data.get("confidence", 0.8),
            metadata=data.get("metadata", {}),
            stop_loss=data.get("stop_loss"),
            take_profit=data.get("take_profit"),
            time_in_force=data.get("time_in_force", "GTC"),
            tags=data.get("tags", []),
        )


class SignalSource(ABC):
    """
    信号源抽象基类

    所有信号源（新闻、CTA策略等）都需要继承这个类并实现 generate_signals()
    """

    def __init__(self, name: str, symbols: list = None):
        self.name = name
        self.symbols = symbols or ["XAUUSD"]
        self.enabled = True
        self.last_update: Optional[datetime] = None

    @abstractmethod
    async def generate_signals(self, context: Dict[str, Any]) -> list[Signal]:
        """
        生成信号

        Args:
            context: 上下文信息，包含：
                - current_price: 当前价格
                - position: 当前持仓
                - historical_data: 历史K线数据
                - market_state: 市场状态
                - 其他策略需要的特定数据

        Returns:
            信号列表
        """
        pass

    def validate_signal(self, signal: Signal) -> bool:
        """
        验证信号有效性

        Args:
            signal: 待验证的信号

        Returns:
            True if signal is valid
        """
        # 基础验证
        if signal.strength < 0.1:  # 信号太弱
            return False

        if signal.confidence < 0.3:  # 置信度太低
            return False

        # 方向验证
        if signal.direction == Direction.FLAT and signal.strength > 0.5:
            # FLAT 信号强度不应该太高
            return False

        return True

    def get_status(self) -> Dict[str, Any]:
        """获取信号源状态"""
        return {
            "name": self.name,
            "enabled": self.enabled,
            "symbols": self.symbols,
            "last_update": self.last_update.isoformat() if self.last_update else None,
        }
