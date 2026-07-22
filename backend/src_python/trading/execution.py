"""
Execution Engine - 执行引擎

负责：
1. 订单执行（模拟/实盘）
2. 持仓管理
3. 订单状态跟踪
4. 数据库同步
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import asyncio
import sqlite3
import uuid

from .signals import Signal, Direction
from .decision import Position, TradeOrder, ActionType
from .risk import RiskManager, RiskCheck


class ExecutionMode(Enum):
    """执行模式"""
    SIMULATION = "SIMULATION"   # 模拟盘
    LIVE = "LIVE"               # 实盘
    PAPER = "PAPER"             # 纸上交易


@dataclass
class ExecutionResult:
    """执行结果"""
    order_id: str
    success: bool
    message: str
    filled_price: Optional[float] = None
    filled_size: Optional[float] = None
    filled_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ExecutionEngine:
    """
    执行引擎

    职责：
    1. 接收交易指令
    2. 风控检查
    3. 执行订单（模拟/实盘）
    4. 更新持仓
    5. 持续风控监控
    """

    def __init__(
        self,
        symbol: str = "XAUUSD",
        mode: ExecutionMode = ExecutionMode.SIMULATION,
        db_path: Optional[str] = None
    ):
        self.symbol = symbol
        self.mode = mode

        # 持仓管理
        self.position: Optional[Position] = None

        # 订单历史
        self.orders: Dict[str, TradeOrder] = {}
        self.executions: Dict[str, ExecutionResult] = {}

        # 风控
        self.risk_manager = RiskManager(symbol=symbol)

        # 数据库
        self.db_path = db_path

        # 实盘引擎 (懒初始化)
        self._live_engine: Any = None  # BinanceExecutionEngine instance
        self._live_config: Optional[Dict[str, Any]] = None

        # 执行统计
        self.stats = {
            "total_orders": 0,
            "filled_orders": 0,
            "rejected_orders": 0,
            "total_pnl": 0.0,
        }

    async def execute_order(
        self,
        order: TradeOrder,
        current_price: float
    ) -> ExecutionResult:
        """
        执行交易指令

        流程：
        1. 风控检查
        2. 执行订单
        3. 更新持仓
        4. 记录到数据库
        """
        # 记录订单
        self.orders[order.order_id] = order
        self.stats["total_orders"] += 1

        # 风控检查
        risk_check = self.risk_manager.check_order(order, current_price)
        if not risk_check.passed:
            self.stats["rejected_orders"] += 1
            order.status = "REJECTED"
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message=f"风控拒绝: {risk_check.message}",
                metadata={"risk_check": risk_check.__dict__}
            )

        # 执行订单
        result = await self._do_execute(order, current_price)

        # 更新持仓
        if result.success:
            self._update_position(order, result, current_price)
            self.stats["filled_orders"] += 1
            order.status = "FILLED"
        else:
            order.status = "REJECTED"

        # 记录执行结果
        self.executions[order.order_id] = result

        # 同步到数据库
        if self.db_path:
            await self._save_to_db(order, result)

        return result

    async def _do_execute(
        self,
        order: TradeOrder,
        current_price: float
    ) -> ExecutionResult:
        """
        实际执行订单（模拟或实盘）
        """
        if self.mode == ExecutionMode.SIMULATION:
            return await self._execute_simulation(order, current_price)
        elif self.mode == ExecutionMode.LIVE:
            return await self._execute_live(order, current_price)
        else:
            return await self._execute_paper(order, current_price)

    async def _execute_simulation(
        self,
        order: TradeOrder,
        current_price: float
    ) -> ExecutionResult:
        """
        模拟执行

        规则：
        - 市价单：以当前价成交
        - 限价单：检查限价是否满足
        - 滑点：添加0.01%的随机滑点
        """
        import random

        # 模拟滑点
        slippage_pct = random.uniform(-0.0001, 0.0001)  # ±0.01%
        filled_price = current_price * (1 + slippage_pct)

        # 检查限价单
        if order.price is not None:
            if order.action in [ActionType.OPEN_LONG, ActionType.ADD_LONG]:
                if order.price < filled_price:
                    return ExecutionResult(
                        order_id=order.order_id,
                        success=False,
                        message=f"限价未满足: 限价{order.price} < 市价{filled_price}"
                    )
            elif order.action in [ActionType.OPEN_SHORT, ActionType.ADD_SHORT]:
                if order.price > filled_price:
                    return ExecutionResult(
                        order_id=order.order_id,
                        success=False,
                        message=f"限价未满足: 限价{order.price} > 市价{filled_price}"
                    )

        # 计算成交数量
        if isinstance(order.size, (int, float)):
            filled_size = abs(order.size)
        else:
            filled_size = order.size

        return ExecutionResult(
            order_id=order.order_id,
            success=True,
            message="模拟成交成功",
            filled_price=round(filled_price, 2),
            filled_size=filled_size,
            filled_time=datetime.now(),
            metadata={"slippage_pct": slippage_pct}
        )

    async def _execute_live(
        self,
        order: TradeOrder,
        current_price: float
    ) -> ExecutionResult:
        """
        实盘执行 — 委托给 BinanceExecutionEngine

        如果 BinanceExecutionEngine 尚未初始化，则:
          1. 尝试懒初始化 (需要 api_key / api_secret 已通过 configure_live() 设置)
          2. 如果初始化失败, 降级为 Simulation
        """
        if self._live_engine is not None:
            result = await self._live_engine.execute_order(order, current_price)
            return ExecutionResult(
                order_id=result.order_id,
                success=result.success,
                message=result.message,
                filled_price=result.filled_price,
                filled_size=result.filled_size,
                filled_time=result.filled_time,
                metadata=result.metadata,
            )

        # 尝试懒初始化
        if self._live_config:
            try:
                from .binance_execution import BinanceExecutionEngine
                self._live_engine = BinanceExecutionEngine(
                    symbol=self.symbol,
                    api_key=self._live_config.get("api_key", ""),
                    api_secret=self._live_config.get("api_secret", ""),
                    proxy_url=self._live_config.get("proxy_url", "http://127.0.0.1:10808"),
                    testnet=self._live_config.get("testnet", True),
                    db_path=self.db_path,
                    risk_manager=self.risk_manager,
                )
                ok = await self._live_engine.initialize()
                if ok:
                    result = await self._live_engine.execute_order(order, current_price)
                    return ExecutionResult(
                        order_id=result.order_id,
                        success=result.success,
                        message=result.message,
                        filled_price=result.filled_price,
                        filled_size=result.filled_size,
                        filled_time=result.filled_time,
                        metadata=result.metadata,
                    )
                else:
                    print("[ExecutionEngine] Binance 初始化失败 — 降级为 Simulation")
            except Exception as e:
                print(f"[ExecutionEngine] Binance 不可用: {e} — 降级为 Simulation")

        # 降级
        return await self._execute_simulation(order, current_price)

    def configure_live(
        self,
        api_key: str = "",
        api_secret: str = "",
        proxy_url: str = "http://127.0.0.1:10808",
        testnet: bool = True,
    ) -> None:
        """
        配置实盘参数 (未提供时尝试懒初始化)。
        """
        self._live_config = {
            "api_key": api_key,
            "api_secret": api_secret,
            "proxy_url": proxy_url,
            "testnet": testnet,
        }

    async def _execute_paper(
        self,
        order: TradeOrder,
        current_price: float
    ) -> ExecutionResult:
        """
        纸上交易（不考虑滑点）
        """
        return ExecutionResult(
            order_id=order.order_id,
            success=True,
            message="纸上成交",
            filled_price=current_price,
            filled_size=abs(order.size) if isinstance(order.size, (int, float)) else order.size,
            filled_time=datetime.now()
        )

    def _update_position(
        self,
        order: TradeOrder,
        result: ExecutionResult,
        current_price: float
    ):
        """
        根据执行结果更新持仓

        持仓状态机：
        - FLAT → LONG/SHORT: 开仓
        - LONG → LONG: 加多
        - LONG → FLAT: 平多
        - LONG → SHORT: 反手做空
        - SHORT → SHORT: 加空
        - SHORT → FLAT: 平空
        - SHORT → LONG: 反手做多
        """
        filled_price = result.filled_price or current_price
        filled_size = result.filled_size

        if order.action in [ActionType.OPEN_LONG, ActionType.ADD_LONG]:
            # 多头操作
            if self.position is None or self.position.is_flat:
                # 新开多头
                self.position = Position(
                    symbol=self.symbol,
                    direction=Direction.LONG,
                    size=filled_size,
                    entry_price=filled_price,
                    entry_time=datetime.now()
                )
            elif self.position.is_long:
                # 加多
                total_size = self.position.size + filled_size
                avg_price = (
                    self.position.entry_price * self.position.size +
                    filled_price * filled_size
                ) / total_size

                self.position.size = total_size
                self.position.entry_price = avg_price

            # 设置止损止盈
            if order.stop_loss:
                self.position.stop_loss = order.stop_loss
            else:
                self.position.stop_loss = self.risk_manager.calculate_stop_loss(
                    Direction.LONG, filled_price
                )

            if order.take_profit:
                self.position.take_profit = order.take_profit
            else:
                self.position.take_profit = self.risk_manager.calculate_take_profit(
                    Direction.LONG, filled_price, self.position.stop_loss
                )

        elif order.action in [ActionType.OPEN_SHORT, ActionType.ADD_SHORT]:
            # 空头操作
            if self.position is None or self.position.is_flat:
                # 新开空头
                self.position = Position(
                    symbol=self.symbol,
                    direction=Direction.SHORT,
                    size=filled_size,
                    entry_price=filled_price,
                    entry_time=datetime.now()
                )
            elif self.position.is_short:
                # 加空
                total_size = self.position.size + filled_size
                avg_price = (
                    self.position.entry_price * self.position.size +
                    filled_price * filled_size
                ) / total_size

                self.position.size = total_size
                self.position.entry_price = avg_price

            # 设置止损止盈
            if order.stop_loss:
                self.position.stop_loss = order.stop_loss
            else:
                self.position.stop_loss = self.risk_manager.calculate_stop_loss(
                    Direction.SHORT, filled_price
                )

            if order.take_profit:
                self.position.take_profit = order.take_profit
            else:
                self.position.take_profit = self.risk_manager.calculate_take_profit(
                    Direction.SHORT, filled_price, self.position.stop_loss
                )

        elif order.action == ActionType.CLOSE_LONG:
            # 平多
            if self.position and self.position.is_long:
                realized_pnl = self.position.unrealized_pnl
                self.position.realized_pnl = realized_pnl
                self.risk_manager.update_daily_pnl(realized_pnl)
                self.stats["total_pnl"] += realized_pnl

                # 清空持仓
                self.position = None

        elif order.action == ActionType.CLOSE_SHORT:
            # 平空
            if self.position and self.position.is_short:
                realized_pnl = self.position.unrealized_pnl
                self.position.realized_pnl = realized_pnl
                self.risk_manager.update_daily_pnl(realized_pnl)
                self.stats["total_pnl"] += realized_pnl

                # 清空持仓
                self.position = None

        elif order.action == ActionType.REVERSE_LONG:
            # 反手做多（先平空，再开多）
            realized_pnl = 0
            if self.position and self.position.is_short:
                realized_pnl = self.position.unrealized_pnl
                self.position.realized_pnl = realized_pnl
                self.risk_manager.update_daily_pnl(realized_pnl)
                self.stats["total_pnl"] += realized_pnl

            # 开多
            self.position = Position(
                symbol=self.symbol,
                direction=Direction.LONG,
                size=filled_size,
                entry_price=filled_price,
                entry_time=datetime.now()
            )

        elif order.action == ActionType.REVERSE_SHORT:
            # 反手做空（先平多，再开空）
            realized_pnl = 0
            if self.position and self.position.is_long:
                realized_pnl = self.position.unrealized_pnl
                self.position.realized_pnl = realized_pnl
                self.risk_manager.update_daily_pnl(realized_pnl)
                self.stats["total_pnl"] += realized_pnl

            # 开空
            self.position = Position(
                symbol=self.symbol,
                direction=Direction.SHORT,
                size=filled_size,
                entry_price=filled_price,
                entry_time=datetime.now()
            )

    async def monitor_position(
        self,
        current_price: float
    ) -> List[RiskCheck]:
        """
        监控持仓风险

        检查：
        1. 止损止盈
        2. 移动止损更新
        3. 浮亏超限
        """
        if self.position is None or self.position.is_flat:
            return []

        # 风险检查
        checks = self.risk_manager.check_position_risk(self.position, current_price)

        # 更新移动止损
        new_stop = self.risk_manager.calculate_trailing_stop(self.position, current_price)
        if new_stop is not None:
            self.position.stop_loss = new_stop

        # 如果有止损/止盈/风控触发，自动平仓
        for check in checks:
            if not check.passed and check.level.value >= 3:  # HIGH或CRITICAL
                # 自动平仓
                await self._emergency_close(check.message, current_price)
                break

        return checks

    async def _emergency_close(self, reason: str, current_price: float):
        """紧急平仓"""
        if self.position is None:
            return

        # 创建平仓订单
        if self.position.is_long:
            action = ActionType.CLOSE_LONG
        else:
            action = ActionType.CLOSE_SHORT

        order = TradeOrder(
            order_id=self._generate_order_id(),
            timestamp=datetime.now(),
            action=action,
            symbol=self.symbol,
            size=self.position.size,
            reason=f"紧急平仓: {reason}",
            metadata={"emergency": True}
        )

        # 执行平仓
        result = await self._do_execute(order, current_price)
        if result.success:
            self._update_position(order, result, current_price)
            self.orders[order.order_id] = order
            self.executions[order.order_id] = result

    def _generate_order_id(self) -> str:
        """生成订单ID"""
        return f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    async def _save_to_db(self, order: TradeOrder, result: ExecutionResult):
        """保存订单到数据库"""
        if not self.db_path:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")

            # 创建订单表（如果不存在）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_orders (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    size REAL NOT NULL,
                    price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    reason TEXT,
                    signal_source TEXT,
                    status TEXT NOT NULL,
                    filled_price REAL,
                    filled_time TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );
            """)

            # 插入订单
            conn.execute("""
                INSERT INTO trade_orders (
                    id, timestamp, action, symbol, size, price,
                    stop_loss, take_profit, reason, signal_source,
                    status, filled_price, filled_time, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                order.order_id,
                order.timestamp.isoformat(),
                order.action.value,
                order.symbol,
                order.size,
                order.price,
                order.stop_loss,
                order.take_profit,
                order.reason,
                order.signal_source,
                order.status,
                result.filled_price,
                result.filled_time.isoformat() if result.filled_time else None,
                str(result.metadata)
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            print(f"[ExecutionEngine] DB保存失败: {e}")

    def get_position(self) -> Optional[Position]:
        """获取当前持仓"""
        return self.position

    def get_orders_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取订单历史"""
        orders_list = list(self.orders.values())
        orders_list.sort(key=lambda o: o.timestamp, reverse=True)
        return [o.to_dict() for o in orders_list[:limit]]

    def get_status(self) -> Dict[str, Any]:
        """获取执行引擎状态"""
        return {
            "symbol": self.symbol,
            "mode": self.mode.value,
            "position": self.position.to_dict() if self.position else None,
            "stats": self.stats,
            "risk_manager": self.risk_manager.get_status(),
            "orders_count": len(self.orders),
        }


class PositionTracker:
    """
    持仓追踪器

    独立于执行引擎，用于：
    1. 从数据库读取持仓状态
    2. 追踪历史表现
    3. 生成统计报告
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_all_positions(self) -> List[Dict[str, Any]]:
        """获取所有持仓记录"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # 从订单记录推断持仓
        cursor = conn.execute("""
            SELECT * FROM trade_orders
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def calculate_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        计算交易统计

        Returns:
            {
                "total_trades": int,
                "win_rate": float,
                "total_pnl": float,
                "sharpe_ratio": float,
                "max_drawdown": float,
            }
        """
        # TODO: 实现统计计算
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
        }
