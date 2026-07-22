"""
Binance Execution Engine — 币安 U本位永续合约 实盘执行引擎

核心特性:
1. ccxt.async_support 异步对接币安 Futures API
2. 严格的价格 / 数量精度控制 (tick size / lot step)
3. 单向持仓模式 (One-way Mode) 安全开平仓 + reduceOnly
4. 防滑点"超价限价单 + 强制止损单"双挂机制
5. OCO 止盈止损管理 / 移动止损更新 / 紧急平仓

Architecture:
    TradeOrder → BinanceExecutionEngine.execute_order()
        ├─ _resolve_order_params()     side / reduceOnly / closePosition
        ├─ _calculate_quantity()       USD notional → contract qty
        ├─ _calculate_limit_price()    超价限价 (marketable limit)
        ├─ exchange.create_order()     下单
        ├─ _place_stop_loss_after_entry()  止损双挂
        └─ _update_position()         本地持仓簿记
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple

from .signals import Signal, Direction
from .decision import Position, TradeOrder, ActionType
from .risk import RiskManager, RiskCheck

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# Data Types
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ExecutionResult:
    """统一执行结果 — 兼容原 ExecutionEngine 接口"""
    order_id: str
    success: bool
    message: str = ""
    filled_price: Optional[float] = None
    filled_size: Optional[float] = None
    filled_time: Optional[datetime] = None
    exchange_order_id: Optional[str] = None
    exchange_status: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "success": self.success,
            "message": self.message,
            "filled_price": self.filled_price,
            "filled_size": self.filled_size,
            "filled_time": self.filled_time.isoformat() if self.filled_time else None,
            "exchange_order_id": self.exchange_order_id,
            "exchange_status": self.exchange_status,
            "metadata": self.metadata,
        }


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


# ══════════════════════════════════════════════════════════════════════
# BinanceExecutionEngine
# ══════════════════════════════════════════════════════════════════════

class BinanceExecutionEngine:
    """
    币安 U本位永续合约 执行引擎

    使用方式:
        engine = BinanceExecutionEngine(
            symbol="XAUUSD",
            api_key="YOUR_API_KEY",
            api_secret="YOUR_SECRET",
            proxy_url="http://127.0.0.1:10808",
            testnet=True,
        )
        ok = await engine.initialize()
        if not ok:
            raise RuntimeError("Binance 初始化失败")

        result = await engine.execute_order(trade_order, current_price=2500.0)

    ── 交易流程 ──
    开仓:
        1. 计算超价限价 (买单略高于市价, 卖单略低于市价)
        2. 提交 GTC 限价单 → 立即以 taker 成交
        3. 成交后立即挂 STOP_MARKET 止损单
        4. (可选) 挂 TAKE_PROFIT_MARKET 止盈单

    平仓:
        1. 计算超价限价 + reduceOnly=True
        2. 成交前先撤销该仓位的所有止损/止盈单
        3. 提交平仓限价单

    反手:
        1. 先平仓 (reduceOnly=True)
        2. 等平仓成交后，再开反向仓位
    """

    # ── 品种映射: 内部符号 → Binance ccxt 符号 ──
    SYMBOL_MAP: Dict[str, str] = {
        "XAUUSD":  "XAU/USDT:USDT",
        "BTCUSDT": "BTC/USDT",
        "ETHUSDT": "ETH/USDT",
        "BNBUSDT": "BNB/USDT",
        "SOLUSDT": "SOL/USDT",
    }

    # ── 测试网 REST 端点 ──
    TESTNET_URLS = {
        'api': {
            'public':  'https://testnet.binancefuture.com/fapi/v1',
            'private': 'https://testnet.binancefuture.com/fapi/v1',
        },
        'www':  'https://testnet.binancefuture.com',
        'doc':  'https://binance-docs.github.io/apidocs/testnet/',
        'fapiPublic':  'https://testnet.binancefuture.com/fapi/v1',
        'fapiPrivate': 'https://testnet.binancefuture.com/fapi/v1',
    }

    def __init__(
        self,
        symbol: str = "XAUUSD",
        api_key: str = "",
        api_secret: str = "",
        proxy_url: Optional[str] = "http://127.0.0.1:10808",
        testnet: bool = True,
        db_path: Optional[str] = None,
        # ── 滑点 / 止损参数 ──
        slippage_pct: float = 0.0005,           # 超价幅度 0.05%
        close_slippage_mult: float = 1.5,        # 平仓超价倍数
        default_stop_offset_pct: float = 0.005,   # 默认止损距离 0.5%
        # ── RiskManager (共享实例 / 自动创建) ──
        risk_manager: Optional[RiskManager] = None,
        account_value: float = 100_000,
    ):
        # ── 符号 ──
        self.symbol = symbol
        self.binance_symbol = self.SYMBOL_MAP.get(symbol, symbol)

        # ── ccxt 配置 ──
        self._exchange_cfg: Dict[str, Any] = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',             # USDⓈ-M
                'adjustForTimeDifference': True,
            },
        }

        if testnet:
            self._exchange_cfg['urls'] = self.TESTNET_URLS
            # 测试网也需关闭对冲模式里的一些重定向
            self._exchange_cfg['options']['warnOnFetchOpenOrdersWithoutSymbol'] = False

        # Proxy
        if proxy_url:
            self._exchange_cfg['proxies'] = {
                'http':  proxy_url,
                'https': proxy_url,
            }
            # ccxt 的 aiohttp session 需要单独设置
            self._exchange_cfg['aiohttp_proxy'] = proxy_url

        # 延迟初始化 — 只有调用 initialize() 时才创建 exchange 实例
        self.exchange: Any = None
        self.testnet = testnet
        self.proxy_url = proxy_url

        # ── 市场属性 (initialize() 后填充) ──
        self.market: Optional[Dict[str, Any]] = None
        self._price_tick: float = 0.0
        self._amount_step: float = 0.0
        self._price_precision: int = 0
        self._amount_precision: int = 0
        self._min_notional: float = 0.0
        self._min_amount: float = 0.0
        self._contract_size: float = 1.0
        self._maker_fee: float = 0.0002
        self._taker_fee: float = 0.0004

        # ── 持仓模式 ──
        self._hedge_mode: bool = False   # 单向持仓

        # ── 滑点参数 ──
        self.slippage_pct = slippage_pct
        self.close_slippage_mult = close_slippage_mult
        self.default_stop_offset_pct = default_stop_offset_pct

        # ── 风控 ──
        self.risk_manager = risk_manager or RiskManager(
            symbol=symbol, account_value=account_value
        )

        # ── 本地状态 ──
        self.position: Optional[Position] = None
        self.orders: Dict[str, TradeOrder] = {}
        self.executions: Dict[str, ExecutionResult] = {}
        self._open_stop_order_ids: Dict[str, str] = {}   # entry_order_id → stop_order_id
        self._open_tp_order_ids: Dict[str, str] = {}     # entry_order_id → tp_order_id

        # ── DB ──
        self.db_path = db_path
        self._db_ready = False

        # ── 统计 ──
        self.stats: Dict[str, Any] = {
            "total_orders": 0,
            "filled_orders": 0,
            "rejected_orders": 0,
            "total_pnl": 0.0,
            "api_errors": 0,
            "retry_count": 0,
        }

        # ── 初始化标记 ──
        self._initialized = False
        self._closing = False

    # ═══════════════════════════════════════════════════════════════
    # 初始化
    # ═══════════════════════════════════════════════════════════════

    async def initialize(self) -> bool:
        """
        初始化交易所连接 & 加载市场属性。
        所有交易操作前必须调用，成功返回 True。
        """
        if self._initialized:
            return True

        try:
            # 0. 延迟创建 ccxt exchange 实例
            if self.exchange is None:
                import ccxt.async_support as ccxt_async
                self.exchange = ccxt_async.binance(self._exchange_cfg)

            # 1. 加载市场信息
            await self.exchange.load_markets()

            if self.binance_symbol not in self.exchange.markets:
                # 模糊搜索 — 列出可能匹配的 symbol
                candidates = [
                    k for k in self.exchange.markets
                    if any(tag in k.upper() for tag in ["XAU", "PAXG", "GOLD"])
                ]
                logger.error(
                    "Symbol '%s' not found on exchange. "
                    "Candidates (gold-related): %s",
                    self.binance_symbol, candidates or "None"
                )
                return False

            self.market = self.exchange.markets[self.binance_symbol]

            # 2. 提取精度 & 限制
            mp = self.market['precision']
            ml = self.market['limits']

            self._price_precision = mp.get('price', 2)
            self._amount_precision = mp.get('amount', 0)
            self._price_tick = float(10 ** (-self._price_precision))
            self._amount_step = float(10 ** (-self._amount_precision))
            self._min_notional = ml.get('cost', {}).get('min', 5.0) or 5.0
            self._min_amount = ml.get('amount', {}).get('min', self._amount_step) or self._amount_step
            self._contract_size = self.market.get('contractSize', 1.0)

            # 费率
            self._maker_fee = self.market.get('maker', 0.0002)
            self._taker_fee = self.market.get('taker', 0.0004)

            # 3. 检测持仓模式
            await self._detect_position_mode()

            # 4. DB 初始化
            if self.db_path:
                await self._init_db()

            self._initialized = True

            logger.info(
                "✓ BinanceExecutionEngine initialized\n"
                "  Symbol:       %s\n"
                "  Price Tick:   %.6f  (precision=%d)\n"
                "  Amount Step:  %.6f  (precision=%d)\n"
                "  Min Notional: %.2f USDT\n"
                "  Contract Sz:  %.4f\n"
                "  Hedge Mode:   %s\n"
                "  Testnet:      %s\n"
                "  Proxy:        %s",
                self.binance_symbol,
                self._price_tick, self._price_precision,
                self._amount_step, self._amount_precision,
                self._min_notional,
                self._contract_size,
                'ON' if self._hedge_mode else 'OFF (One-way)',
                self.testnet,
                self.proxy_url or 'Direct',
            )
            return True

        except Exception as exc:
            logger.error("BinanceExecutionEngine initialize failed: %s", exc, exc_info=True)
            return False

    async def _detect_position_mode(self) -> None:
        """检测当前持仓模式 (单向 vs 双向对冲)"""
        try:
            resp = await self.exchange.fapiPrivate_get_positionside_dual()
            self._hedge_mode = resp.get('dualSidePosition', False)
        except Exception:
            # API 不可用时默认单向 (更安全)
            self._hedge_mode = False
            logger.warning("Cannot detect position mode, assuming one-way")

    async def set_position_mode(self, hedge: bool = False) -> None:
        """
        设置持仓模式。

        Args:
            hedge: True=双向对冲, False=单向持仓 (推荐)
        """
        try:
            await self.exchange.fapiPrivate_post_positionside_dual({
                'dualSidePosition': 'true' if hedge else 'false'
            })
            self._hedge_mode = hedge
            logger.info("Position mode → %s", 'Hedge' if hedge else 'One-way')
        except Exception as exc:
            err_msg = str(exc)
            if 'No need to change' in err_msg:
                self._hedge_mode = hedge
                return
            logger.error("set_position_mode failed: %s", exc)
            raise

    # ═══════════════════════════════════════════════════════════════
    # 精度控制 (核心 — 任何价格/数量进场前必须过这层)
    # ═══════════════════════════════════════════════════════════════

    def _round_price(self, price: float) -> float:
        """将价格按 tick size 四舍五入。"""
        if self._price_tick <= 0:
            return price
        return round(price / self._price_tick) * self._price_tick

    def _round_amount(self, amount: float) -> float:
        """将数量按 lot step 四舍五入。"""
        if self._amount_step <= 0:
            return amount
        return round(amount / self._amount_step) * self._amount_step

    def _truncate_price(self, price: float) -> float:
        """向下截断到 tick precision (保守 — 买单价不偏高, 卖单价不偏低)"""
        if self._price_precision <= 0:
            return price
        factor = 10 ** self._price_precision
        return float(int(price * factor) / factor)

    def _truncate_amount(self, amount: float) -> float:
        """向下截断到 lot precision"""
        if self._amount_precision <= 0:
            return amount
        factor = 10 ** self._amount_precision
        return float(int(amount * factor) / factor)

    # ═══════════════════════════════════════════════════════════════
    # 核心: 订单执行
    # ═══════════════════════════════════════════════════════════════

    async def execute_order(
        self,
        order: TradeOrder,
        current_price: float,
    ) -> ExecutionResult:
        """
        执行一笔 TradeOrder。

        完整流水线:
        1. 解析 side / reduceOnly / closePosition
        2. 计算合约张数 (USD notional → contracts)
        3. 计算超价限价 (防滑点)
        4. 风控检查 (RiskManager)
        5. 下单到 Binance
        6. 如果填满 → 立即挂止损单
        7. 更新本地持仓 & DB
        """
        if not self._initialized:
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message="Engine not initialized — call initialize() first",
            )

        # 记录
        self.orders[order.order_id] = order
        self.stats["total_orders"] += 1

        try:
            # ── Step 1: 解析方向 & reduceOnly ──
            side, reduce_only, close_position = self._resolve_order_params(order)

            # ── Step 2: 计算数量 ──
            quantity = self._calculate_contract_quantity(order, current_price)
            quantity = self._truncate_amount(quantity)

            # ── 最小名义价值检查 ──
            notional = quantity * current_price
            if notional < self._min_notional:
                quantity = self._truncate_amount(self._min_notional / current_price)
                notional = quantity * current_price
                if quantity < self._min_amount or notional < self._min_notional:
                    return ExecutionResult(
                        order_id=order.order_id,
                        success=False,
                        message=(
                            f"Order too small: notional={notional:.2f} USDT "
                            f"< min={self._min_notional} USDT"
                        ),
                        metadata={"notional": notional, "min_notional": self._min_notional},
                    )

            if quantity < self._min_amount:
                return ExecutionResult(
                    order_id=order.order_id,
                    success=False,
                    message=f"Quantity {quantity} < min amount {self._min_amount}",
                )

            # ── Step 3: 超价限价 ──
            limit_price = self._calculate_limit_price(side, current_price, reduce_only)
            limit_price = self._round_price(limit_price)

            # ── Step 4: 风控检查 ──
            risk_check = self.risk_manager.check_order(order, current_price)
            if not risk_check.passed:
                self.stats["rejected_orders"] += 1
                order.status = "REJECTED"
                return ExecutionResult(
                    order_id=order.order_id,
                    success=False,
                    message=f"Risk check failed: {risk_check.message}",
                    metadata={"risk_check": risk_check.__dict__},
                )

            # ── Step 5: 特殊处理 — REVERSE (反手) ──
            if order.action in (ActionType.REVERSE_LONG, ActionType.REVERSE_SHORT):
                return await self._execute_reverse(order, current_price, quantity)

            # ── Step 6: 如果是减仓/平仓, 先撤销关联止损止盈单 ──
            if reduce_only:
                await self._cancel_attached_orders()

            # ── Step 7: 下单 ──
            logger.info(
                "→ Placing: %s | side=%s qty=%.4f limit=%.2f reduceOnly=%s closePosition=%s",
                order.action.value, side, quantity, limit_price, reduce_only, close_position,
            )

            params: Dict[str, Any] = {}
            if reduce_only:
                params['reduceOnly'] = True
            if close_position:
                params['reduceOnly'] = True
                # Binance 支持 closePosition — 一键全平
                # 但我们仍传 quantity 作为上限
                params['closePosition'] = False   # 自己算好量, 不完全依赖 closePosition

            ccxt_order = await self._place_order_with_retry(
                side=side,
                quantity=quantity,
                price=limit_price,
                params=params,
            )

            if ccxt_order is None:
                self.stats["api_errors"] += 1
                self.stats["rejected_orders"] += 1
                order.status = "REJECTED"
                return ExecutionResult(
                    order_id=order.order_id,
                    success=False,
                    message="Order placement failed after retries",
                )

            exchange_id   = ccxt_order.get('id', '')
            filled_qty    = float(ccxt_order.get('filled', 0) or 0)
            filled_avg    = float(ccxt_order.get('average', 0) or 0)
            status        = ccxt_order.get('status', 'UNKNOWN')

            # ── Step 8: 短暂等待后检查成交 ──
            await asyncio.sleep(0.3)

            try:
                updated = await self.exchange.fetch_order(exchange_id, self.binance_symbol)
                filled_qty = float(updated.get('filled', filled_qty) or 0)
                filled_avg = float(updated.get('average', filled_avg) or 0) or limit_price
                status     = updated.get('status', status)
            except Exception:
                pass  # 拉不到更新也不致命

            is_filled = status in ('closed', 'FILLED') or filled_qty >= quantity * 0.99

            # ── Step 9: 如果填满 & 是开仓方向 → 挂止损 ──
            if is_filled and filled_qty > 0:
                order.status = "FILLED"
                order.filled_price = filled_avg or limit_price
                order.filled_time = datetime.now()
                self.stats["filled_orders"] += 1

                if not reduce_only:
                    # 开仓 → 双挂: 止损 + (可选) 止盈
                    await self._attach_stop_loss(order, filled_avg or limit_price, quantity)
                    if order.take_profit:
                        await self._attach_take_profit(order, filled_avg or limit_price, quantity, order.take_profit)

                # 更新本地持仓
                self._update_position(order, filled_avg or limit_price, quantity)

                result = ExecutionResult(
                    order_id=order.order_id,
                    success=True,
                    message="Filled",
                    filled_price=filled_avg or limit_price,
                    filled_size=filled_qty,
                    filled_time=datetime.now(),
                    exchange_order_id=exchange_id,
                    exchange_status=status,
                    metadata={"limit_price": limit_price, "side": side, "reduce_only": reduce_only},
                )

            elif is_filled and filled_qty <= 0:
                # 订单关闭但无成交 (可能是 POST_ONLY 被取消等)
                order.status = "CANCELLED"
                self.stats["rejected_orders"] += 1
                result = ExecutionResult(
                    order_id=order.order_id,
                    success=False,
                    message=f"Order closed without fill (status={status})",
                    exchange_order_id=exchange_id,
                    exchange_status=status,
                )

            else:
                # 未完全成交 → 仍标记为 PENDING (回测或监控可后续处理)
                order.status = "PENDING"
                result = ExecutionResult(
                    order_id=order.order_id,
                    success=True,
                    message=f"Placed but not filled (status={status})",
                    exchange_order_id=exchange_id,
                    exchange_status=status,
                    metadata={"limit_price": limit_price, "side": side, "reduce_only": reduce_only},
                )

            # ── Step 10: 持久化 ──
            self.executions[order.order_id] = result
            if self.db_path and self._db_ready:
                await self._save_to_db(order, result)

            return result

        except Exception as exc:
            self.stats["api_errors"] += 1
            self.stats["rejected_orders"] += 1
            order.status = "REJECTED"
            logger.error("execute_order unhandled: %s", exc, exc_info=True)
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message=f"Unexpected error: {exc}",
                metadata={"error": str(exc)},
            )

    # ─────────────────────────────────────────────────────────────
    # 订单参数解析
    # ─────────────────────────────────────────────────────────────

    def _resolve_order_params(self, order: TradeOrder) -> Tuple[str, bool, bool]:
        """
        将 TradeOrder.action 映射为 (side, reduce_only, close_position)

        单向持仓模式规则:
        ┌────────────────────┬────────┬─────────────┬───────────────┐
        │ Action             │ side   │ reduceOnly  │ closePosition │
        ├────────────────────┼────────┼─────────────┼───────────────┤
        │ OPEN_LONG          │ buy    │ False       │ False         │
        │ OPEN_SHORT         │ sell   │ False       │ False         │
        │ ADD_LONG           │ buy    │ False       │ False         │
        │ ADD_SHORT          │ sell   │ False       │ False         │
        │ CLOSE_LONG         │ sell   │ True        │ False         │
        │ CLOSE_SHORT        │ buy    │ True        │ False         │
        │ REVERSE_LONG       │ buy    │ False       │ True          │ ← 特殊
        │ REVERSE_SHORT      │ sell   │ False       │ True          │ ← 特殊
        │ FLAT               │ *      │ True        │ True          │ ← 全平
        └────────────────────┴────────┴─────────────┴───────────────┘
        """
        if order.action == ActionType.OPEN_LONG:
            return ('buy', False, False)
        elif order.action == ActionType.OPEN_SHORT:
            return ('sell', False, False)
        elif order.action == ActionType.ADD_LONG:
            return ('buy', False, False)
        elif order.action == ActionType.ADD_SHORT:
            return ('sell', False, False)
        elif order.action == ActionType.CLOSE_LONG:
            return ('sell', True, False)
        elif order.action == ActionType.CLOSE_SHORT:
            return ('buy', True, False)
        elif order.action == ActionType.REVERSE_LONG:
            return ('buy', False, True)   # closePosition 平空 → 开多
        elif order.action == ActionType.REVERSE_SHORT:
            return ('sell', False, True)  # closePosition 平多 → 开空
        elif order.action == ActionType.FLAT:
            # 根据当前持仓决定 side
            if self.position and self.position.is_long:
                return ('sell', True, True)
            elif self.position and self.position.is_short:
                return ('buy', True, True)
            else:
                return ('sell', True, False)
        elif order.action == ActionType.HOLD:
            return ('buy', False, False)  # 不会真正执行
        else:
            raise ValueError(f"Unknown action: {order.action}")

    def _calculate_contract_quantity(self, order: TradeOrder, current_price: float) -> float:
        """
        将订单中的 USD 名义金额转换为合约张数。

        order.size 是决策引擎输出的美元金额 (e.g. 10000 表示 $10,000)。
        对于 USDⓈ-M 永续: quantity_contracts = notional / price
        """
        notional = abs(order.size)
        if notional <= 0:
            return 0.0
        return notional / current_price

    def _calculate_limit_price(
        self,
        side: str,
        current_price: float,
        reduce_only: bool,
    ) -> float:
        """
        计算 "超价限价" (marketable limit order)。

        核心思想:
        - 买家限价略高于市价 → 立即吃掉卖单墙 (充当 taker)
        - 卖家限价略低于市价 → 立即吃掉买单墙 (充当 taker)
        - 但限价本身保护我们不致于滑出离谱价格

        Args:
            side: 'buy' or 'sell'
            current_price: 当前中间价 / 最新成交价
            reduce_only: 是否平仓 (平仓时更激进)
        """
        slippage = self.slippage_pct
        if reduce_only:
            slippage *= self.close_slippage_mult

        if side == 'buy':
            return current_price * (1.0 + slippage)
        else:  # sell
            return current_price * (1.0 - slippage)

    # ─────────────────────────────────────────────────────────────
    # 下单 (含重试)
    # ─────────────────────────────────────────────────────────────

    async def _place_order_with_retry(
        self,
        side: str,
        quantity: float,
        price: float,
        params: Dict[str, Any],
        max_retries: int = 2,
    ) -> Optional[Dict[str, Any]]:
        """
        下单 + 有限重试。

        重试场景:
        - 网络超时
        - 速率限制 (ccxt 自带处理)
        - 交易所临时不可用

        不重试场景:
        - 资金不足
        - 精度错误 (调整精度后重试)
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                return await self.exchange.create_order(
                    symbol=self.binance_symbol,
                    type='limit',
                    side=side,
                    amount=quantity,
                    price=price,
                    params={
                        **params,
                        'timeInForce': 'GTC',
                    },
                )
            except Exception as exc:
                last_error = exc
                err_msg = str(exc).lower()

                # 精度问题 → 调整后重试
                if 'filter failure: price' in err_msg or 'price' in err_msg and 'precision' in err_msg:
                    price = self._truncate_price(price)
                    logger.warning("Price precision error, truncated → %.6f (retry %d)", price, attempt + 1)
                    continue

                if 'filter failure: lot_size' in err_msg or 'quantity' in err_msg:
                    quantity = self._truncate_amount(quantity)
                    logger.warning("Quantity precision error, truncated → %.6f (retry %d)", quantity, attempt + 1)
                    continue

                if 'filter failure: notional' in err_msg or 'min_notional' in err_msg:
                    logger.error("Min notional violation — cannot recover")
                    return None

                if 'insufficient balance' in err_msg or 'margin' in err_msg:
                    logger.error("Insufficient balance / margin")
                    return None

                # 网络/临时错误 → 重试
                if any(kw in err_msg for kw in ('timeout', 'connection', 'dns', 'reset', '502', '503')):
                    wait = (attempt + 1) * 1.0
                    logger.warning("Network error, retry %d after %.1fs: %s", attempt + 1, wait, exc)
                    await asyncio.sleep(wait)
                    self.stats["retry_count"] += 1
                    continue

                # 不可恢复的错误
                logger.error("Unrecoverable order error: %s", exc)
                return None

        logger.error("All %d retries exhausted for %s order", max_retries + 1, side)
        return None

    # ─────────────────────────────────────────────────────────────
    # 反手 (REVERSE)
    # ─────────────────────────────────────────────────────────────

    async def _execute_reverse(
        self,
        order: TradeOrder,
        current_price: float,
        new_qty: float,
    ) -> ExecutionResult:
        """
        反手执行: 先平仓 → 等确认 → 再开新仓。

        在单向持仓模式下, REVERSE 不能原子化完成,
        必须拆成两步 (先 close, 后 open)。
        """
        logger.info("↻ Reverse: closing existing position first...")

        # Step 1: 平掉现有仓位
        close_side = 'sell' if (self.position and self.position.is_long) else 'buy'
        close_qty = self.position.size if self.position else new_qty

        close_params = {'reduceOnly': True}
        close_result = await self._place_order_with_retry(
            side=close_side,
            quantity=close_qty,
            price=self._calculate_limit_price(close_side, current_price, reduce_only=True),
            params=close_params,
        )

        if close_result is None:
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message="Reverse failed: could not close existing position",
            )

        # Step 2: 等待平仓成交
        await asyncio.sleep(0.5)
        close_id = close_result.get('id', '')
        close_filled = float(close_result.get('filled', 0) or 0)

        try:
            updated = await self.exchange.fetch_order(close_id, self.binance_symbol)
            close_filled = float(updated.get('filled', close_filled) or 0)
        except Exception:
            pass

        if close_filled <= 0:
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message="Reverse failed: close order did not fill",
            )

        # 记录平仓盈亏
        if self.position:
            realized = self.position.unrealized_pnl
            self.position.realized_pnl = realized
            self.risk_manager.update_daily_pnl(realized)
            self.stats["total_pnl"] += realized
            self.position = None

        # 清除旧止损/止盈
        await self._cancel_attached_orders()

        # Step 3: 开新仓
        new_side, _, _ = self._resolve_order_params(order)
        new_price = self._calculate_limit_price(new_side, current_price, reduce_only=False)
        new_price = self._round_price(new_price)

        new_result = await self._place_order_with_retry(
            side=new_side,
            quantity=new_qty,
            price=new_price,
            params={},
        )

        if new_result is None:
            return ExecutionResult(
                order_id=order.order_id,
                success=False,
                message="Reverse: new position order failed after closing",
            )

        filled_avg = float(new_result.get('average', 0) or 0) or new_price
        filled_qty = float(new_result.get('filled', 0) or 0)
        exchange_id = new_result.get('id', '')

        if filled_qty > 0:
            order.status = "FILLED"
            order.filled_price = filled_avg
            order.filled_time = datetime.now()
            self.stats["filled_orders"] += 1

            # 挂新止损
            await self._attach_stop_loss(order, filled_avg, new_qty)

            # 更新持仓
            self._update_position(order, filled_avg, new_qty)

        result = ExecutionResult(
            order_id=order.order_id,
            success=filled_qty > 0,
            message="Reverse completed" if filled_qty > 0 else "Reverse partially filled",
            filled_price=filled_avg,
            filled_size=filled_qty,
            filled_time=datetime.now(),
            exchange_order_id=exchange_id,
            metadata={"close_order_id": close_id},
        )

        self.executions[order.order_id] = result
        return result

    # ═══════════════════════════════════════════════════════════════
    # 双挂机制: 止损 & 止盈
    # ═══════════════════════════════════════════════════════════════

    async def _attach_stop_loss(
        self,
        order: TradeOrder,
        entry_price: float,
        quantity: float,
    ) -> Optional[str]:
        """
        开仓成交后立即挂 STOP_MARKET 止损单。

        这是 "强制止损单" — 每笔开仓必须带止损,
        确保极端行情下自动截断亏损。

        Returns:
            stop_order_id or None
        """
        try:
            # 判断止损方向 (与开仓相反)
            if order.action in (ActionType.OPEN_LONG, ActionType.ADD_LONG, ActionType.REVERSE_LONG):
                stop_side = 'sell'
                trigger = entry_price * (1.0 - self.default_stop_offset_pct)
            else:
                stop_side = 'buy'
                trigger = entry_price * (1.0 + self.default_stop_offset_pct)

            # 如果 TradeOrder 包含自定义止损, 优先使用
            if order.stop_loss is not None:
                trigger = order.stop_loss
                # 校验止损方向正确
                if stop_side == 'sell' and trigger >= entry_price:
                    logger.warning("Stop-loss above entry for LONG — ignoring custom, using default")
                    trigger = entry_price * (1.0 - self.default_stop_offset_pct)
                elif stop_side == 'buy' and trigger <= entry_price:
                    logger.warning("Stop-loss below entry for SHORT — ignoring custom, using default")
                    trigger = entry_price * (1.0 + self.default_stop_offset_pct)

            trigger = self._round_price(trigger)

            # 撤销该仓位的旧止损
            if order.order_id in self._open_stop_order_ids:
                old_id = self._open_stop_order_ids.pop(order.order_id)
                try:
                    await self.exchange.cancel_order(old_id, self.binance_symbol)
                except Exception:
                    pass

            stop_qty = self._truncate_amount(quantity)

            stop_order = await self.exchange.create_order(
                symbol=self.binance_symbol,
                type='STOP_MARKET',
                side=stop_side,
                amount=stop_qty,
                params={
                    'stopPrice': trigger,
                    'reduceOnly': True,
                },
            )

            stop_id = stop_order.get('id', '')
            self._open_stop_order_ids[order.order_id] = stop_id

            logger.info(
                "  🛡 Stop-loss placed: id=%s side=%s trigger=%.2f qty=%.4f",
                stop_id, stop_side, trigger, stop_qty,
            )
            return stop_id

        except Exception as exc:
            logger.error("Failed to place stop-loss: %s", exc, exc_info=True)
            return None

    async def _attach_take_profit(
        self,
        order: TradeOrder,
        entry_price: float,
        quantity: float,
        tp_price: float,
    ) -> Optional[str]:
        """
        开仓成交后挂 TAKE_PROFIT_MARKET 止盈单。
        """
        try:
            if order.action in (ActionType.OPEN_LONG, ActionType.ADD_LONG, ActionType.REVERSE_LONG):
                tp_side = 'sell'
            else:
                tp_side = 'buy'

            tp_price = self._round_price(tp_price)
            tp_qty = self._truncate_amount(quantity)

            # 撤销旧止盈
            if order.order_id in self._open_tp_order_ids:
                old_id = self._open_tp_order_ids.pop(order.order_id)
                try:
                    await self.exchange.cancel_order(old_id, self.binance_symbol)
                except Exception:
                    pass

            tp_order = await self.exchange.create_order(
                symbol=self.binance_symbol,
                type='TAKE_PROFIT_MARKET',
                side=tp_side,
                amount=tp_qty,
                params={
                    'stopPrice': tp_price,
                    'reduceOnly': True,
                },
            )

            tp_id = tp_order.get('id', '')
            self._open_tp_order_ids[order.order_id] = tp_id

            logger.info(
                "  🎯 Take-profit placed: id=%s side=%s trigger=%.2f qty=%.4f",
                tp_id, tp_side, tp_price, tp_qty,
            )
            return tp_id

        except Exception as exc:
            logger.error("Failed to place take-profit: %s", exc, exc_info=True)
            return None

    async def _cancel_attached_orders(self) -> None:
        """撤销当前仓位关联的所有止损/止盈单。"""
        all_ids = list(self._open_stop_order_ids.values()) + list(self._open_tp_order_ids.values())
        for oid in all_ids:
            try:
                await self.exchange.cancel_order(oid, self.binance_symbol)
                logger.info("Cancelled attached order: %s", oid)
            except Exception:
                pass
        self._open_stop_order_ids.clear()
        self._open_tp_order_ids.clear()

    # ═══════════════════════════════════════════════════════════════
    # 持仓管理
    # ═══════════════════════════════════════════════════════════════

    def _update_position(
        self,
        order: TradeOrder,
        filled_price: float,
        filled_qty: float,
    ) -> None:
        """
        根据成交更新本地持仓簿记。

        状态机:
          FLAT → LONG/SHORT      开仓
          LONG → LONG            加仓 (均价合并)
          LONG → FLAT            平仓
          SHORT → SHORT          加仓
          SHORT → FLAT           平仓
        """
        if order.action in (ActionType.OPEN_LONG, ActionType.ADD_LONG):
            if self.position is None or self.position.is_flat:
                self.position = Position(
                    symbol=self.symbol,
                    direction=Direction.LONG,
                    size=filled_qty,
                    entry_price=filled_price,
                    entry_time=datetime.now(),
                )
            elif self.position.is_long:
                total = self.position.size + filled_qty
                avg = (
                    self.position.entry_price * self.position.size
                    + filled_price * filled_qty
                ) / total
                self.position.size = total
                self.position.entry_price = avg

            # 设置止损止盈引用
            self.position.stop_loss = order.stop_loss or (
                filled_price * (1 - self.default_stop_offset_pct)
            )
            self.position.take_profit = order.take_profit

        elif order.action in (ActionType.OPEN_SHORT, ActionType.ADD_SHORT):
            if self.position is None or self.position.is_flat:
                self.position = Position(
                    symbol=self.symbol,
                    direction=Direction.SHORT,
                    size=filled_qty,
                    entry_price=filled_price,
                    entry_time=datetime.now(),
                )
            elif self.position.is_short:
                total = self.position.size + filled_qty
                avg = (
                    self.position.entry_price * self.position.size
                    + filled_price * filled_qty
                ) / total
                self.position.size = total
                self.position.entry_price = avg

            self.position.stop_loss = order.stop_loss or (
                filled_price * (1 + self.default_stop_offset_pct)
            )
            self.position.take_profit = order.take_profit

        elif order.action == ActionType.CLOSE_LONG:
            if self.position and self.position.is_long:
                self.position.unrealized_pnl = (
                    (filled_price - self.position.entry_price) * self.position.size
                )
                self.position.realized_pnl = self.position.unrealized_pnl
                self.risk_manager.update_daily_pnl(self.position.realized_pnl)
                self.stats["total_pnl"] += self.position.realized_pnl
                self.position = None

        elif order.action == ActionType.CLOSE_SHORT:
            if self.position and self.position.is_short:
                self.position.unrealized_pnl = (
                    (self.position.entry_price - filled_price) * self.position.size
                )
                self.position.realized_pnl = self.position.unrealized_pnl
                self.risk_manager.update_daily_pnl(self.position.realized_pnl)
                self.stats["total_pnl"] += self.position.realized_pnl
                self.position = None

        # REVERSE 已在 _execute_reverse 中处理

    async def fetch_position_from_exchange(self) -> Optional[Dict[str, Any]]:
        """从交易所获取实时持仓（用于对账）"""
        try:
            positions = await self.exchange.fetch_positions(symbols=[self.binance_symbol])
            for pos in positions:
                if pos.get('symbol') == self.binance_symbol:
                    contracts = float(pos.get('contracts', 0) or 0)
                    if abs(contracts) > 0:
                        return pos
            return None
        except Exception as exc:
            logger.error("fetch_position_from_exchange: %s", exc)
            return None

    async def sync_position(self) -> None:
        """
        从交易所同步持仓到本地簿记 (启动时调用)。
        """
        raw = await self.fetch_position_from_exchange()
        if raw is None:
            self.position = None
            return

        contracts = abs(float(raw.get('contracts', 0) or 0))
        direction = Direction.LONG if float(raw.get('contracts', 0) or 0) > 0 else Direction.SHORT
        entry_price = float(raw.get('entryPrice', 0) or 0)
        unrealized = float(raw.get('unrealizedPnl', 0) or 0)

        self.position = Position(
            symbol=self.symbol,
            direction=direction,
            size=contracts,
            entry_price=entry_price,
            entry_time=datetime.now(),
            unrealized_pnl=unrealized,
        )
        logger.info("Synced position: %s %.4f @ %.2f", direction.name, contracts, entry_price)

    # ═══════════════════════════════════════════════════════════════
    # 持仓监控 & 紧急平仓
    # ═══════════════════════════════════════════════════════════════

    async def monitor_position(self, current_price: float) -> List[RiskCheck]:
        """
        监控持仓风险 (每个 tick 调用):
        1. 检查止损/止盈是否应被触发
        2. 更新移动止损
        3. 风控超限自动平仓
        """
        if self.position is None or self.position.is_flat:
            return []

        checks = self.risk_manager.check_position_risk(self.position, current_price)

        # 移动止损更新
        new_stop = self.risk_manager.calculate_trailing_stop(self.position, current_price)
        if new_stop is not None:
            self.position.stop_loss = new_stop
            # 可选: 同步到交易所 (撤销旧止损, 挂新止损)
            # await self._update_stop_loss_on_exchange(new_stop)

        # 高风险 → 自动紧急平仓
        for check in checks:
            if not check.passed and check.level.value >= 3:  # HIGH or CRITICAL
                await self._emergency_close(check.message, current_price)
                break

        return checks

    async def _emergency_close(self, reason: str, current_price: float) -> None:
        """紧急平仓 — 市价全平"""
        if self.position is None or self.position.is_flat:
            return

        logger.warning("!! EMERGENCY CLOSE: %s", reason)

        side = 'sell' if self.position.is_long else 'buy'

        try:
            # 先撤止损止盈
            await self._cancel_attached_orders()

            # 市价平仓 (用超价限价模拟)
            price = self._calculate_limit_price(side, current_price, reduce_only=True)
            price = self._round_price(price)
            qty = self._truncate_amount(self.position.size)

            result = await self.exchange.create_order(
                symbol=self.binance_symbol,
                type='limit',
                side=side,
                amount=qty,
                price=price,
                params={'reduceOnly': True, 'timeInForce': 'GTC'},
            )

            logger.info("Emergency close order placed: %s", result.get('id', '?'))

        except Exception as exc:
            logger.critical("Emergency close FAILED: %s", exc, exc_info=True)

    # ═══════════════════════════════════════════════════════════════
    # 订单管理
    # ═══════════════════════════════════════════════════════════════

    async def cancel_all_orders(self) -> None:
        """撤销所有挂单（紧急使用）"""
        try:
            await self.exchange.cancel_all_orders(symbol=self.binance_symbol)
            self._open_stop_order_ids.clear()
            self._open_tp_order_ids.clear()
            logger.warning("All orders cancelled for %s", self.binance_symbol)
        except Exception as exc:
            logger.error("cancel_all_orders: %s", exc)

    async def get_open_orders(self) -> List[Dict[str, Any]]:
        """获取当前所有挂单"""
        try:
            return await self.exchange.fetch_open_orders(symbol=self.binance_symbol) or []
        except Exception:
            return []

    # ═══════════════════════════════════════════════════════════════
    # 状态 & 统计
    # ═══════════════════════════════════════════════════════════════

    def get_position(self) -> Optional[Position]:
        return self.position

    def get_orders_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        orders_list = list(self.orders.values())
        orders_list.sort(key=lambda o: o.timestamp, reverse=True)
        return [o.to_dict() for o in orders_list[:limit]]

    def get_status(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "binance_symbol": self.binance_symbol,
            "mode": "LIVE" if not self.testnet else "TESTNET",
            "initialized": self._initialized,
            "hedge_mode": self._hedge_mode,
            "position": self.position.to_dict() if self.position else None,
            "stats": self.stats,
            "risk_manager": self.risk_manager.get_status(),
            "orders_count": len(self.orders),
            "market": {
                "price_tick": self._price_tick,
                "amount_step": self._amount_step,
                "min_notional": self._min_notional,
                "contract_size": self._contract_size,
                "maker_fee": self._maker_fee,
                "taker_fee": self._taker_fee,
            } if self.market else None,
        }

    # ═══════════════════════════════════════════════════════════════
    # 生命周期
    # ═══════════════════════════════════════════════════════════════

    async def close(self) -> None:
        """关闭交易所连接"""
        self._closing = True
        if hasattr(self, 'exchange') and self.exchange:
            await self.exchange.close()
            logger.info("Exchange connection closed")

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ═══════════════════════════════════════════════════════════════
    # Database
    # ═══════════════════════════════════════════════════════════════

    async def _init_db(self) -> None:
        if not self.db_path:
            return
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS binance_orders (
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
                    exchange_order_id TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );
            """)
            conn.commit()
            conn.close()
            self._db_ready = True
        except Exception as exc:
            logger.error("DB init failed: %s", exc)

    async def _save_to_db(self, order: TradeOrder, result: ExecutionResult) -> None:
        if not self.db_path or not self._db_ready:
            return
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("""
                INSERT INTO binance_orders (
                    id, timestamp, action, symbol, size, price,
                    stop_loss, take_profit, reason, signal_source,
                    status, filled_price, filled_time, exchange_order_id, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                result.exchange_order_id,
                str(result.metadata),
            ))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.error("DB save failed: %s", exc)


# ══════════════════════════════════════════════════════════════════
# Factory — 兼容现有 TradingEngine
# ══════════════════════════════════════════════════════════════════

def create_binance_engine(
    symbol: str = "XAUUSD",
    api_key: str = "",
    api_secret: str = "",
    proxy_url: str = "http://127.0.0.1:10808",
    testnet: bool = True,
    db_path: Optional[str] = None,
    risk_manager: Optional[RiskManager] = None,
    account_value: float = 100_000,
    slippage_pct: float = 0.0005,
) -> BinanceExecutionEngine:
    """
    工厂函数 — 创建预配置的 BinanceExecutionEngine。

    Usage:
        engine = create_binance_engine(
            api_key="xxx",
            api_secret="yyy",
        )
        await engine.initialize()
    """
    return BinanceExecutionEngine(
        symbol=symbol,
        api_key=api_key,
        api_secret=api_secret,
        proxy_url=proxy_url,
        testnet=testnet,
        db_path=db_path,
        risk_manager=risk_manager,
        account_value=account_value,
        slippage_pct=slippage_pct,
    )
