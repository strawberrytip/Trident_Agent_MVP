"""
Trading Engine - 主交易引擎

整合所有模块，提供统一的交易接口
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
import sqlite3

from .signals import Signal, SignalSource, Direction
from .aggregator import SignalAggregator, AggregatedSignal, SourceConfig
from .decision import DecisionEngine, Position, TradeOrder, ActionType, FixedPercentageSizer
from .risk import RiskManager
from .execution import ExecutionEngine, ExecutionMode, ExecutionResult


class TradingEngine:
    """
    主交易引擎

    整合：
    - 信号源管理
    - 信号聚合
    - 决策引擎
    - 风控管理
    - 订单执行
    """

    def __init__(
        self,
        symbol: str = "XAUUSD",
        account_value: float = 100000,
        mode: ExecutionMode = ExecutionMode.SIMULATION,
        db_path: Optional[str] = None
    ):
        self.symbol = symbol
        self.account_value = account_value
        self.db_path = db_path or "trident_trading.db"

        # 初始化各个模块
        self.aggregator = SignalAggregator()
        self.decision_engine = DecisionEngine(symbol=symbol)
        self.execution_engine = ExecutionEngine(
            symbol=symbol,
            mode=mode,
            db_path=self.db_path
        )

        # 共享风控管理器
        self.risk_manager = self.execution_engine.risk_manager
        self.risk_manager.account_value = account_value

        # 信号源注册表
        self.signal_sources: Dict[str, SignalSource] = {}

        # 状态
        self.is_running = False
        self.last_process_time: Optional[datetime] = None

        # 初始化数据库
        self._init_db()

    def _init_db(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")

        # 创建必要的表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                direction TEXT NOT NULL,
                strength REAL NOT NULL,
                symbol TEXT NOT NULL,
                reason TEXT,
                confidence REAL,
                metadata TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                votes_long INTEGER,
                votes_short INTEGER,
                votes_flat INTEGER,
                weighted_long REAL,
                weighted_short REAL,
                final_direction TEXT NOT NULL,
                final_strength REAL NOT NULL,
                sources TEXT,
                has_conflict INTEGER,
                conflict_reason TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                size REAL NOT NULL,
                entry_price REAL NOT NULL,
                entry_time TEXT NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                unrealized_pnl REAL DEFAULT 0,
                realized_pnl REAL DEFAULT 0,
                max_profit REAL DEFAULT 0,
                max_loss REAL DEFAULT 0,
                exit_time TEXT,
                exit_price REAL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)

        conn.commit()
        conn.close()

    def add_signal_source(self, source: SignalSource, config: Optional[SourceConfig] = None):
        """
        添加信号源

        Args:
            source: 信号源实例
            config: 信号源配置（可选）
        """
        self.signal_sources[source.name] = source

        if config is None:
            config = SourceConfig(name=source.name)

        self.aggregator.add_source(config)

    def remove_signal_source(self, name: str):
        """移除信号源"""
        if name in self.signal_sources:
            del self.signal_sources[name]
        self.aggregator.remove_source(name)

    def get_signal_source_config(self, name: str) -> Optional[SourceConfig]:
        """获取信号源配置"""
        return self.aggregator.get_source_config(name)

    async def process_signals(
        self,
        context: Optional[Dict[str, Any]] = None
    ) -> List[TradeOrder]:
        """
        处理信号并生成交易指令

        流程：
        1. 从所有信号源收集信号
        2. 聚合信号
        3. 决策引擎判断
        4. 风控检查
        5. 执行订单
        """
        context = context or {}
        context["current_price"] = context.get("current_price", 0)
        context["current_time"] = datetime.now()

        # 1. 收集信号
        all_signals = []
        for source in self.signal_sources.values():
            if not hasattr(source, 'enabled') or source.enabled:
                try:
                    signals = await source.generate_signals(context)
                    all_signals.extend(signals)
                except Exception as e:
                    print(f"[TradingEngine] 信号源 {source.name} 失败: {e}")

        if not all_signals:
            return []

        # 2. 聚合信号
        aggregated = await self.aggregator.aggregate(all_signals, self.symbol)

        # 3. 决策
        current_price = context.get("current_price", 0)
        current_position = self.execution_engine.get_position()
        atr_value = context.get("atr_value", None)

        decision = self.decision_engine.decide(
            aggregated,
            current_position,
            current_price,
            self.account_value,
            atr_value
        )

        if decision is None:
            return []

        # 4. 风控检查
        risk_check = self.risk_manager.check_order(decision, current_price)

        if not risk_check.passed:
            print(f"[TradingEngine] 订单被风控拒绝: {risk_check.message}")
            return []

        # 5. 执行订单
        result = await self.execution_engine.execute_order(decision, current_price)

        # 6. 记录到数据库
        self._save_signals_to_db(all_signals, aggregated)

        self.last_process_time = datetime.now()

        return [decision] if result.success else []

    async def monitor_and_execute(self, current_price: float):
        """
        监控并执行（持续运行的任务）

        功能：
        1. 监控持仓风险
        2. 自动止损止盈
        3. 更新移动止损
        """
        risk_checks = await self.execution_engine.monitor_position(current_price)

        for check in risk_checks:
            if not check.passed:
                print(f"[TradingEngine] 风控触发: {check.message}")

    def _save_signals_to_db(
        self,
        signals: List[Signal],
        aggregated: AggregatedSignal
    ):
        """保存信号到数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL;")

            # 保存原始信号
            for signal in signals:
                conn.execute("""
                    INSERT INTO trading_signals (
                        timestamp, source, direction, strength, symbol,
                        reason, confidence, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    signal.timestamp.isoformat(),
                    signal.source,
                    signal.direction.name,
                    signal.strength,
                    signal.symbol,
                    signal.reason,
                    signal.confidence,
                    str(signal.metadata)
                ))

            # 保存聚合信号
            conn.execute("""
                INSERT INTO aggregated_signals (
                    timestamp, symbol, votes_long, votes_short, votes_flat,
                    weighted_long, weighted_short, final_direction, final_strength,
                    sources, has_conflict, conflict_reason, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                aggregated.timestamp.isoformat(),
                aggregated.symbol,
                aggregated.votes_long,
                aggregated.votes_short,
                aggregated.votes_flat,
                aggregated.weighted_long,
                aggregated.weighted_short,
                aggregated.final_direction.name,
                aggregated.final_strength,
                ",".join(aggregated.sources),
                1 if aggregated.has_conflict else 0,
                aggregated.conflict_reason,
                str(aggregated.metadata)
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            print(f"[TradingEngine] 信号保存失败: {e}")

    async def run_loop(self, interval: float = 5.0):
        """
        运行主循环

        Args:
            interval: 检查间隔（秒）
        """
        self.is_running = True
        print(f"[TradingEngine] 启动交易引擎，检查间隔 {interval}秒")

        while self.is_running:
            try:
                # 获取当前价格
                current_price = await self._get_current_price()

                context = {
                    "current_price": current_price,
                    "atr_value": await self._get_current_atr()
                }

                # 处理信号
                orders = await self.process_signals(context)

                # 监控持仓
                await self.monitor_and_execute(current_price)

            except Exception as e:
                print(f"[TradingEngine] 循环错误: {e}")

            await asyncio.sleep(interval)

    async def _get_current_price(self) -> float:
        """获取当前价格（示例）"""
        # TODO: 从真实数据源获取
        return 2500.0

    async def _get_current_atr(self) -> Optional[float]:
        """获取当前ATR（示例）"""
        # TODO: 从真实数据源获取
        return 10.0

    def stop(self):
        """停止引擎"""
        self.is_running = False
        print("[TradingEngine] 交易引擎已停止")

    def get_status(self) -> Dict[str, Any]:
        """获取引擎状态"""
        return {
            "symbol": self.symbol,
            "account_value": self.account_value,
            "is_running": self.is_running,
            "last_process_time": self.last_process_time.isoformat() if self.last_process_time else None,
            "signal_sources": [name for name in self.signal_sources.keys()],
            "aggregator": self.aggregator.get_status(),
            "decision_engine": self.decision_engine.get_status(),
            "execution_engine": self.execution_engine.get_status(),
        }
