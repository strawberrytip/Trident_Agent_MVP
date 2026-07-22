"""
Decision Engine - 决策引擎

负责：
1. 接收聚合信号
2. 根据当前持仓状态决定交易动作
3. 计算仓位大小
4. 生成交易指令
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

from .signals import Signal, Direction
from .aggregator import AggregatedSignal


class ActionType(Enum):
    """交易动作类型"""
    OPEN_LONG = "OPEN_LONG"       # 开多
    OPEN_SHORT = "OPEN_SHORT"     # 开空
    ADD_LONG = "ADD_LONG"         # 加多
    ADD_SHORT = "ADD_SHORT"       # 加空
    CLOSE_LONG = "CLOSE_LONG"     # 平多
    CLOSE_SHORT = "CLOSE_SHORT"   # 平空
    REVERSE_LONG = "REVERSE_LONG" # 反手做多（空平多）
    REVERSE_SHORT = "REVERSE_SHORT"  # 反手做空（多平空）
    HOLD = "HOLD"                # 持仓观望
    FLAT = "FLAT"                # 强制平仓


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    direction: Direction         # 持仓方向
    size: float                 # 持仓数量（正向数）
    entry_price: float          # 开仓均价
    entry_time: datetime        # 开仓时间

    stop_loss: Optional[float] = None        # 当前止损价
    take_profit: Optional[float] = None      # 当前止盈价

    unrealized_pnl: float = 0.0             # 浮动盈亏
    realized_pnl: float = 0.0               # 已实现盈亏

    max_profit: float = 0.0                 # 最大浮盈
    max_loss: float = 0.0                   # 最大浮亏

    hold_time: Optional[timedelta] = None    # 持仓时长

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_long(self) -> bool:
        return self.direction == Direction.LONG

    @property
    def is_short(self) -> bool:
        return self.direction == Direction.SHORT

    @property
    def is_flat(self) -> bool:
        return self.direction == Direction.FLAT or self.size == 0

    def update_pnl(self, current_price: float):
        """更新盈亏"""
        if self.size == 0:
            return

        if self.is_long:
            self.unrealized_pnl = (current_price - self.entry_price) * self.size
        elif self.is_short:
            self.unrealized_pnl = (self.entry_price - current_price) * self.size

        # 更新最大盈亏
        self.max_profit = max(self.max_profit, self.unrealized_pnl)
        self.max_loss = min(self.max_loss, self.unrealized_pnl)

        # 更新持仓时长
        if self.entry_time:
            self.hold_time = datetime.now() - self.entry_time

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "symbol": self.symbol,
            "direction": self.direction.name,
            "size": round(self.size, 4),
            "entry_price": round(self.entry_price, 2),
            "entry_time": self.entry_time.isoformat(),
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "max_profit": round(self.max_profit, 2),
            "max_loss": round(self.max_loss, 2),
            "hold_time": str(self.hold_time) if self.hold_time else None,
        }


@dataclass
class TradeOrder:
    """交易指令"""
    order_id: str
    timestamp: datetime
    action: ActionType
    symbol: str
    size: float                # 交易数量（正数）
    price: Optional[float] = None  # 限价单价格，None=市价单

    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    reason: str = ""
    signal_source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    # 订单状态
    status: str = "PENDING"     # PENDING, FILLED, PARTIAL, CANCELLED, REJECTED
    filled_price: Optional[float] = None
    filled_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "order_id": self.order_id,
            "timestamp": self.timestamp.isoformat(),
            "action": self.action.value,
            "symbol": self.symbol,
            "size": round(self.size, 4),
            "price": self.price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "reason": self.reason[:200],
            "signal_source": self.signal_source,
            "status": self.status,
            "filled_price": self.filled_price,
            "filled_time": self.filled_time.isoformat() if self.filled_time else None,
        }


class PositionSizer(ABC):
    """仓位大小计算器抽象基类"""

    @abstractmethod
    def calculate_size(
        self,
        signal_strength: float,
        current_price: float,
        account_value: float,
        current_position: Optional[Position] = None
    ) -> float:
        """计算仓位大小"""
        pass


class FixedPercentageSizer(PositionSizer):
    """固定百分比仓位计算器"""

    def __init__(
        self,
        base_size_pct: float = 0.1,     # 基础仓位 10%
        max_size_pct: float = 0.3,      # 最大仓位 30%
        strength_multiplier: bool = True  # 是否根据信号强度调整
    ):
        self.base_size_pct = base_size_pct
        self.max_size_pct = max_size_pct
        self.strength_multiplier = strength_multiplier

    def calculate_size(
        self,
        signal_strength: float,
        current_price: float,
        account_value: float,
        current_position: Optional[Position] = None
    ) -> float:
        """
        计算仓位大小（以金额计）

        公式：
        - strength_multiplier=True: size = base_size * (1 + strength) * account_value
        - strength_multiplier=False: size = base_size * account_value
        """
        base_size = self.base_size_pct * account_value

        if self.strength_multiplier:
            # 信号强度越大，仓位越大
            multiplier = 1.0 + signal_strength
            size = base_size * multiplier
        else:
            size = base_size

        # 限制最大仓位
        max_size = self.max_size_pct * account_value
        size = min(size, max_size)

        return size


class ATRBasedSizer(PositionSizer):
    """基于ATR的仓位计算器（风险平价）"""

    def __init__(
        self,
        risk_per_trade: float = 0.02,  # 每笔交易风险 2%
        atr_period: int = 14,
        atr_multiplier: float = 2.0   # 止损 = ATR * multiplier
    ):
        self.risk_per_trade = risk_per_trade
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier

    def calculate_size(
        self,
        signal_strength: float,
        current_price: float,
        account_value: float,
        current_position: Optional[Position] = None,
        atr_value: Optional[float] = None
    ) -> float:
        """
        基于ATR计算仓位

        公式：
        size = (account_value * risk_per_trade) / (ATR * multiplier)
        """
        if atr_value is None or atr_value == 0:
            # ATR不可用，使用固定百分比
            return 0.1 * account_value

        risk_amount = account_value * self.risk_per_trade
        stop_distance = atr_value * self.atr_multiplier

        # 计算合约数量
        size = risk_amount / stop_distance

        # 根据信号强度调整
        size *= (0.5 + signal_strength * 0.5)  # 范围 [0.5, 1.0]

        return size


class DecisionEngine:
    """
    决策引擎

    职责：
    1. 根据聚合信号和当前持仓状态决定交易动作
    2. 计算仓位大小
    3. 生成交易指令
    """

    def __init__(
        self,
        symbol: str = "XAUUSD",
        sizer: Optional[PositionSizer] = None
    ):
        self.symbol = symbol
        self.sizer = sizer or FixedPercentageSizer()

        # 决策参数
        self.min_signal_strength = 0.4      # 最小开仓信号强度
        self.add_position_threshold = 0.6    # 加仓信号强度阈值
        self.reverse_threshold = 0.7        # 反手信号强度阈值

        # 状态
        self.last_signal: Optional[AggregatedSignal] = None
        self.last_decision: Optional[TradeOrder] = None

    def decide(
        self,
        aggregated_signal: AggregatedSignal,
        current_position: Optional[Position],
        current_price: float,
        account_value: float = 100000,
        atr_value: Optional[float] = None
    ) -> Optional[TradeOrder]:
        """
        做交易决策

        决策逻辑：
        1. 无持仓 + 强信号 → 开仓
        2. 有持仓 + 同向信号 → 加仓
        3. 有持仓 + 反向信号 → 平仓/反手
        4. 有持仓 + 弱反向 → 减仓
        5. 无信号 → 观望
        """
        self.last_signal = aggregated_signal

        direction = aggregated_signal.final_direction
        strength = aggregated_signal.final_strength

        # 信号太弱 → 不交易
        if strength < self.min_signal_strength:
            return None

        # ===== 无持仓 =====
        if current_position is None or current_position.is_flat:
            if direction == Direction.LONG:
                return self._create_open_long_order(
                    aggregated_signal, current_price, account_value, atr_value
                )
            elif direction == Direction.SHORT:
                return self._create_open_short_order(
                    aggregated_signal, current_price, account_value, atr_value
                )

        # ===== 有多头持仓 =====
        elif current_position.is_long:
            if direction == Direction.SHORT and strength >= self.reverse_threshold:
                # 强空头信号 → 反手
                return self._create_reverse_short_order(
                    current_position, aggregated_signal, current_price, account_value
                )
            elif direction == Direction.SHORT:
                # 弱空头信号 → 平仓
                return self._create_close_long_order(
                    current_position, "信号转弱"
                )
            elif direction == Direction.LONG and strength >= self.add_position_threshold:
                # 同向强信号 → 加仓
                return self._create_add_long_order(
                    current_position, aggregated_signal, current_price, account_value
                )

        # ===== 有空头持仓 =====
        elif current_position.is_short:
            if direction == Direction.LONG and strength >= self.reverse_threshold:
                # 强多头信号 → 反手
                return self._create_reverse_long_order(
                    current_position, aggregated_signal, current_price, account_value
                )
            elif direction == Direction.LONG:
                # 弱多头信号 → 平仓
                return self._create_close_short_order(
                    current_position, "信号转弱"
                )
            elif direction == Direction.SHORT and strength >= self.add_position_threshold:
                # 同向强信号 → 加仓
                return self._create_add_short_order(
                    current_position, aggregated_signal, current_price, account_value
                )

        # 无明确动作
        return None

    def _create_open_long_order(
        self,
        signal: AggregatedSignal,
        price: float,
        account_value: float,
        atr_value: Optional[float]
    ) -> TradeOrder:
        """创建开多订单"""
        size = self._calculate_size_amount(
            signal.final_strength, price, account_value, atr_value
        )

        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.OPEN_LONG,
            symbol=self.symbol,
            size=size,
            reason=f"开多: 信号强度{signal.final_strength:.2f}, 来源{signal.sources}",
            signal_source=",".join(signal.sources),
            metadata=signal.to_dict()
        )

    def _create_open_short_order(
        self,
        signal: AggregatedSignal,
        price: float,
        account_value: float,
        atr_value: Optional[float]
    ) -> TradeOrder:
        """创建开空订单"""
        size = self._calculate_size_amount(
            signal.final_strength, price, account_value, atr_value
        )

        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.OPEN_SHORT,
            symbol=self.symbol,
            size=size,
            reason=f"开空: 信号强度{signal.final_strength:.2f}, 来源{signal.sources}",
            signal_source=",".join(signal.sources),
            metadata=signal.to_dict()
        )

    def _create_add_long_order(
        self,
        position: Position,
        signal: AggregatedSignal,
        price: float,
        account_value: float
    ) -> TradeOrder:
        """创建加多订单"""
        base_size = self._calculate_size_amount(
            signal.final_strength * 0.5, price, account_value, None  # 加仓用一半强度
        )

        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.ADD_LONG,
            symbol=self.symbol,
            size=base_size,
            reason=f"加多: 当前持仓{position.size}, 信号强度{signal.final_strength:.2f}",
            signal_source=",".join(signal.sources),
            metadata={"current_position": position.to_dict(), "signal": signal.to_dict()}
        )

    def _create_add_short_order(
        self,
        position: Position,
        signal: AggregatedSignal,
        price: float,
        account_value: float
    ) -> TradeOrder:
        """创建加空订单"""
        base_size = self._calculate_size_amount(
            signal.final_strength * 0.5, price, account_value, None
        )

        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.ADD_SHORT,
            symbol=self.symbol,
            size=base_size,
            reason=f"加空: 当前持仓{position.size}, 信号强度{signal.final_strength:.2f}",
            signal_source=",".join(signal.sources),
            metadata={"current_position": position.to_dict(), "signal": signal.to_dict()}
        )

    def _create_close_long_order(
        self,
        position: Position,
        reason: str
    ) -> TradeOrder:
        """创建平多订单"""
        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.CLOSE_LONG,
            symbol=self.symbol,
            size=position.size,
            reason=f"平多: {reason}",
            metadata={"closed_position": position.to_dict()}
        )

    def _create_close_short_order(
        self,
        position: Position,
        reason: str
    ) -> TradeOrder:
        """创建平空订单"""
        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.CLOSE_SHORT,
            symbol=self.symbol,
            size=position.size,
            reason=f"平空: {reason}",
            metadata={"closed_position": position.to_dict()}
        )

    def _create_reverse_long_order(
        self,
        position: Position,
        signal: AggregatedSignal,
        price: float,
        account_value: float
    ) -> TradeOrder:
        """创建反手做多订单"""
        # 先平空，再开多
        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.REVERSE_LONG,
            symbol=self.symbol,
            size=position.size,  # 平仓数量
            reason=f"反手做多: 原持仓{position.size}, 信号强度{signal.final_strength:.2f}",
            signal_source=",".join(signal.sources),
            metadata={
                "closed_position": position.to_dict(),
                "signal": signal.to_dict()
            }
        )

    def _create_reverse_short_order(
        self,
        position: Position,
        signal: AggregatedSignal,
        price: float,
        account_value: float
    ) -> TradeOrder:
        """创建反手做空订单"""
        return TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=ActionType.REVERSE_SHORT,
            symbol=self.symbol,
            size=position.size,
            reason=f"反手做空: 原持仓{position.size}, 信号强度{signal.final_strength:.2f}",
            signal_source=",".join(signal.sources),
            metadata={
                "closed_position": position.to_dict(),
                "signal": signal.to_dict()
            }
        )

    def _calculate_size_amount(
        self,
        strength: float,
        price: float,
        account_value: float,
        atr_value: Optional[float]
    ) -> float:
        """计算交易数量（以金额计）"""
        if atr_value is not None and isinstance(self.sizer, ATRBasedSizer):
            return self.sizer.calculate_size(strength, price, account_value, None, atr_value)
        else:
            return self.sizer.calculate_size(strength, price, account_value)

    def _generate_order_id(self) -> str:
        """生成订单ID"""
        import uuid
        return f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    def get_status(self) -> Dict[str, Any]:
        """获取决策引擎状态"""
        return {
            "symbol": self.symbol,
            "sizer_type": type(self.sizer).__name__,
            "min_signal_strength": self.min_signal_strength,
            "add_position_threshold": self.add_position_threshold,
            "reverse_threshold": self.reverse_threshold,
            "last_signal": self.last_signal.to_dict() if self.last_signal else None,
        }
