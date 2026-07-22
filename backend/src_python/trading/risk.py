"""
Risk Manager - 风控模块

负责：
1. 订单风险评估
2. 止损止盈计算
3. 持仓风险监控
4. 爆仓保护
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any
import asyncio

from .signals import Direction
from .decision import Position, TradeOrder


class RiskLevel(Enum):
    """风险等级"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class RiskCheck:
    """风险检查结果"""
    passed: bool
    level: RiskLevel
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class RiskManager:
    """
    风控管理器

    功能：
    1. 检查订单是否符合风控规则
    2. 计算动态止损止盈
    3. 监控持仓风险
    4. 强制平仓保护
    """

    def __init__(
        self,
        symbol: str = "XAUUSD",
        account_value: float = 100000
    ):
        self.symbol = symbol
        self.account_value = account_value

        # 仓位限制
        self.max_position_size = 0.3         # 最大单品种仓位 30%
        self.max_total_exposure = 0.5        # 最大总敞口 50%

        # 止损参数
        self.default_stop_loss_pct = 0.02    # 默认止损 2%
        self.default_take_profit_pct = 0.05  # 默认止盈 5%
        self.trailing_stop_pct = 0.03        # 移动止损 3%

        # 风险参数
        self.max_drawdown_pct = 0.15         # 最大回撤 15%
        self.max_loss_per_trade = 0.02        # 单笔最大亏损 2%
        self.daily_loss_limit = 0.05          # 日内最大亏损 5%

        # 状态
        self.daily_pnl = 0.0
        self.max_drawdown = 0.0
        self.peak_account_value = account_value
        self.last_check_time: Optional[datetime] = None

    def check_order(self, order: TradeOrder, current_price: float) -> RiskCheck:
        """
        检查订单是否符合风控规则

        检查项：
        1. 仓位大小是否超限
        2. 单笔风险是否过大
        3. 日内亏损是否超限
        """
        checks = []

        # 检查1：仓位大小
        position_value = order.size * current_price
        position_pct = position_value / self.account_value

        if position_pct > self.max_position_size:
            checks.append(RiskCheck(
                passed=False,
                level=RiskLevel.HIGH,
                message=f"仓位过大: {position_pct:.1%} > {self.max_position_size:.1%}",
                metadata={"position_pct": position_pct, "limit": self.max_position_size}
            ))
        else:
            checks.append(RiskCheck(
                passed=True,
                level=RiskLevel.LOW,
                message=f"仓位正常: {position_pct:.1%}",
                metadata={"position_pct": position_pct}
            ))

        # 检查2：日内亏损限制
        if self.daily_pnl < -self.account_value * self.daily_loss_limit:
            checks.append(RiskCheck(
                passed=False,
                level=RiskLevel.CRITICAL,
                message=f"日内亏损超限: {self.daily_pnl:.2f} < {-self.account_value * self.daily_loss_limit:.2f}",
                metadata={"daily_pnl": self.daily_pnl, "limit": -self.account_value * self.daily_loss_limit}
            ))
        else:
            checks.append(RiskCheck(
                passed=True,
                level=RiskLevel.LOW,
                message="日内亏损正常",
                metadata={"daily_pnl": self.daily_pnl}
            ))

        # 汇总结果
        failed_checks = [c for c in checks if not c.passed]

        if failed_checks:
            # 找到最高风险等级
            max_level = max((c.level for c in failed_checks), default=RiskLevel.MEDIUM)
            return RiskCheck(
                passed=False,
                level=max_level,
                message=f"订单被拒绝: {failed_checks[0].message}",
                metadata={"failed_checks": [c.__dict__ for c in failed_checks]}
            )

        return RiskCheck(
            passed=True,
            level=RiskLevel.LOW,
            message="订单通过风控检查",
            metadata={"passed_checks": [c.__dict__ for c in checks]}
        )

    def calculate_stop_loss(
        self,
        direction: Direction,
        entry_price: float,
        atr_value: Optional[float] = None
    ) -> float:
        """
        计算止损价格

        优先级：ATR > 固定百分比
        """
        if atr_value is not None and atr_value > 0:
            # 使用ATR计算止损（2倍ATR）
            stop_distance = atr_value * 2.0
        else:
            # 使用固定百分比
            stop_distance = entry_price * self.default_stop_loss_pct

        if direction == Direction.LONG:
            return entry_price - stop_distance
        else:  # SHORT
            return entry_price + stop_distance

    def calculate_take_profit(
        self,
        direction: Direction,
        entry_price: float,
        stop_loss: Optional[float] = None,
        risk_reward_ratio: float = 2.0
    ) -> float:
        """
        计算止盈价格

        默认风险收益比 1:2
        """
        if stop_loss is not None:
            risk_distance = abs(entry_price - stop_loss)
            profit_distance = risk_distance * risk_reward_ratio
        else:
            profit_distance = entry_price * self.default_take_profit_pct

        if direction == Direction.LONG:
            return entry_price + profit_distance
        else:  # SHORT
            return entry_price - profit_distance

    def calculate_trailing_stop(
        self,
        position: Position,
        current_price: float
    ) -> Optional[float]:
        """
        计算移动止损

        规则：
        - 多头：如果价格有利，止损上移到（当前价 - 移动止损距离）
        - 空头：如果价格有利，止损下移到（当前价 + 移动止损距离）
        - 只向有利方向移动，从不回调
        """
        if position.is_long:
            # 多头移动止损
            trailing_distance = current_price * self.trailing_stop_pct
            new_stop = current_price - trailing_distance

            # 只有新高时才上移止损
            if position.stop_loss is None or new_stop > position.stop_loss:
                return new_stop

        elif position.is_short:
            # 空头移动止损
            trailing_distance = current_price * self.trailing_stop_pct
            new_stop = current_price + trailing_distance

            # 只有新低时才下移止损
            if position.stop_loss is None or new_stop < position.stop_loss:
                return new_stop

        return None

    def check_position_risk(
        self,
        position: Position,
        current_price: float
    ) -> List[RiskCheck]:
        """
        检查持仓风险

        检查项：
        1. 是否触发止损
        2. 是否触发止盈
        3. 浮亏是否超限
        4. 移动止损是否需要更新
        """
        checks = []

        # 更新盈亏
        position.update_pnl(current_price)

        # 检查1：止损检查
        if position.stop_loss is not None:
            if position.is_long and current_price <= position.stop_loss:
                checks.append(RiskCheck(
                    passed=False,
                    level=RiskLevel.HIGH,
                    message="触发止损",
                    metadata={"current_price": current_price, "stop_loss": position.stop_loss}
                ))
            elif position.is_short and current_price >= position.stop_loss:
                checks.append(RiskCheck(
                    passed=False,
                    level=RiskLevel.HIGH,
                    message="触发止损",
                    metadata={"current_price": current_price, "stop_loss": position.stop_loss}
                ))

        # 检查2：止盈检查
        if position.take_profit is not None:
            if position.is_long and current_price >= position.take_profit:
                checks.append(RiskCheck(
                    passed=True,
                    level=RiskLevel.LOW,
                    message="触发止盈（建议平仓）",
                    metadata={"current_price": current_price, "take_profit": position.take_profit}
                ))
            elif position.is_short and current_price <= position.take_profit:
                checks.append(RiskCheck(
                    passed=True,
                    level=RiskLevel.LOW,
                    message="触发止盈（建议平仓）",
                    metadata={"current_price": current_price, "take_profit": position.take_profit}
                ))

        # 检查3：浮亏超限
        if position.unrealized_pnl < 0:
            loss_pct = abs(position.unrealized_pnl) / self.account_value
            if loss_pct > self.max_loss_per_trade:
                checks.append(RiskCheck(
                    passed=False,
                    level=RiskLevel.CRITICAL,
                    message=f"浮亏超限: {loss_pct:.1%} > {self.max_loss_per_trade:.1%}",
                    metadata={"unrealized_pnl": position.unrealized_pnl, "limit_pct": self.max_loss_per_trade}
                ))

        # 检查4：最大回撤检查
        total_value = self.account_value + position.unrealized_pnl
        if total_value > self.peak_account_value:
            self.peak_account_value = total_value

        drawdown = (self.peak_account_value - total_value) / self.peak_account_value
        self.max_drawdown = max(self.max_drawdown, drawdown)

        if drawdown > self.max_drawdown_pct:
            checks.append(RiskCheck(
                passed=False,
                level=RiskLevel.CRITICAL,
                message=f"回撤超限: {drawdown:.1%} > {self.max_drawdown_pct:.1%}",
                metadata={"drawdown": drawdown, "limit": self.max_drawdown_pct}
            ))

        return checks

    def update_daily_pnl(self, realized_pnl: float):
        """更新日内盈亏"""
        self.daily_pnl += realized_pnl

    def reset_daily_stats(self):
        """重置日内统计（每日开盘时调用）"""
        self.daily_pnl = 0.0

    def get_status(self) -> Dict[str, Any]:
        """获取风控状态"""
        return {
            "account_value": self.account_value,
            "daily_pnl": round(self.daily_pnl, 2),
            "max_drawdown": round(self.max_drawdown, 4),
            "peak_account_value": round(self.peak_account_value, 2),
            "max_position_size": self.max_position_size,
            "daily_loss_limit": self.daily_loss_limit,
        }
