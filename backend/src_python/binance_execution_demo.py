#!/usr/bin/env python3
"""
BinanceExecutionEngine Integration Demo

演示币安实盘执行引擎的完整生命周期:
1. 引擎初始化 & 市场属性加载
2. 价格/数量精度控制验证
3. 完整交易流程: 开仓 → 加仓 → 平仓 → 反手
4. 止损双挂机制
5. 持仓风控监控
6. 错误处理 & 边界情况

Usage:
    # 模拟模式 (无需 API Key)
    python binance_execution_demo.py

    # 测试网模式 (需要 Binance Testnet API Key)
    python binance_execution_demo.py --live \\
        --api-key YOUR_KEY --api-secret YOUR_SECRET

    # 自定义品种
    python binance_execution_demo.py --symbol BTCUSDT
"""

import asyncio
import os
import sys
import argparse
from datetime import datetime

# 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'trading'))

from trading import (
    Signal, Direction, SignalAggregator, DecisionEngine,
    RiskManager, ExecutionEngine, BinanceExecutionEngine,
    create_binance_engine, TradeOrder, ActionType, Position,
)


# ══════════════════════════════════════════════════════════════════
# Demo 1: Precision Control
# ══════════════════════════════════════════════════════════════════

async def demo_precision():
    """演示精度控制逻辑 (无需连接交易所)"""
    print("=" * 60)
    print("Demo 1: Price / Quantity Precision Control")
    print("=" * 60)

    # 手动设置市场属性来演示精度 (不需要真实连接)
    engine = BinanceExecutionEngine(
        symbol="XAUUSD",
        api_key="demo",
        api_secret="demo",
        testnet=True,
    )

    # 模拟 set 市场属性 (通常由 initialize() 填充)
    engine._price_tick = 0.1
    engine._price_precision = 1
    engine._amount_step = 0.01
    engine._amount_precision = 2
    engine._min_notional = 5.0
    engine._contract_size = 1.0
    engine._initialized = True

    print("\nMarket Properties (simulated XAU/USDT:USDT):")
    print(f"  Price Tick:    {engine._price_tick}")
    print(f"  Amount Step:   {engine._amount_step}")
    print(f"  Min Notional:  {engine._min_notional} USDT")

    print("\n--- Round Price ---")
    test_prices = [2657.83, 2657.87, 2657.91, 2657.99]
    for p in test_prices:
        print(f"  {p:>8.2f} → {engine._round_price(p):>8.2f}")

    print("\n--- Truncate Amount ---")
    test_amounts = [0.378, 0.372, 0.3799, 0.3701]
    for a in test_amounts:
        print(f"  {a:>8.4f} → {engine._truncate_amount(a):>8.4f}")

    print("\n--- Round Amount ---")
    for a in test_amounts:
        print(f"  {a:>8.4f} → {engine._round_amount(a):>8.4f}")


# ══════════════════════════════════════════════════════════════════
# Demo 2: Order Resolution (side / reduceOnly)
# ══════════════════════════════════════════════════════════════════

async def demo_order_resolution():
    """演示 ActionType → (side, reduceOnly, closePosition) 映射"""
    print("\n" + "=" * 60)
    print("Demo 2: Action → Order Params Resolution")
    print("=" * 60)

    engine = BinanceExecutionEngine(testnet=True, api_key="d", api_secret="d")
    engine._initialized = True

    test_cases = [
        (ActionType.OPEN_LONG,   "Open Long"),
        (ActionType.OPEN_SHORT,  "Open Short"),
        (ActionType.ADD_LONG,    "Add Long"),
        (ActionType.ADD_SHORT,   "Add Short"),
        (ActionType.CLOSE_LONG,  "Close Long"),
        (ActionType.CLOSE_SHORT, "Close Short"),
        (ActionType.REVERSE_LONG,"Reverse → Long"),
        (ActionType.REVERSE_SHORT,"Reverse → Short"),
    ]

    print(f"\n{'Action':<18} {'Side':<6} {'reduceOnly':<12} {'closePosition'}")
    print("-" * 56)
    for action, label in test_cases:
        order = TradeOrder(
            order_id=f"test-{label}",
            timestamp=datetime.now(),
            action=action,
            symbol="XAUUSD",
            size=1000,
            reason=label,
        )
        side, ro, cp = engine._resolve_order_params(order)
        print(f"  {label:<16} {side:<6} {str(ro):<12} {cp}")


# ══════════════════════════════════════════════════════════════════
# Demo 3: Full Trade Flow (Simulated)
# ══════════════════════════════════════════════════════════════════

async def demo_trade_flow():
    """演示完整交易流程 (使用现有 ExecutionEngine 的 Simulation 模式)"""
    print("\n" + "=" * 60)
    print("Demo 3: Full Trade Lifecycle (Simulation)")
    print("=" * 60)

    from trading import TradingEngine, ExecutionMode
    from trading.signal_sources import ManualSignalSource
    from trading.aggregator import SourceConfig

    engine = TradingEngine(
        symbol="XAUUSD",
        account_value=100_000,
        mode=ExecutionMode.SIMULATION,
        db_path="/tmp/demo_trading.db",
    )

    # 添加信号源 & 设置简单多数决 (演示用)
    manual = ManualSignalSource()
    engine.add_signal_source(manual)
    engine.aggregator.add_source(SourceConfig(
        name="Manual", enabled=True, weight=1.0,
    ))
    engine.aggregator.set_conflict_strategy("majority")

    price = 2660.0

    # ── Step 1: 开多 ──
    print("\n--- Step 1: Open Long ---")
    manual.add_manual_signal("LONG", 0.7, "XAUUSD", "黄金突破前高 + AI看多")
    orders = await engine.process_signals({"current_price": price})
    for o in orders:
        print(f"  → {o.action.value}: size={o.size:.0f} status={o.status}")

    # ── Step 2: 价格上涨, 加仓 ──
    print("\n--- Step 2: Add Long (price up to 2680) ---")
    price = 2680.0
    manual.add_manual_signal("LONG", 0.65, "XAUUSD", "趋势延续, 加仓")
    orders = await engine.process_signals({"current_price": price})
    for o in orders:
        print(f"  → {o.action.value}: size={o.size:.0f} status={o.status}")

    # ── Step 3: 信号转空, 平仓 ──
    print("\n--- Step 3: Close Long (signal flipped) ---")
    price = 2675.0
    manual.add_manual_signal("SHORT", 0.55, "XAUUSD", "动能衰竭, 减仓")
    orders = await engine.process_signals({"current_price": price})
    for o in orders:
        print(f"  → {o.action.value}: size={o.size:.0f} status={o.status}")

    # ── Step 4: 强空信号, 反手做空 ──
    print("\n--- Step 4: Reverse Short (strong bearish) ---")
    price = 2670.0
    manual.add_manual_signal("SHORT", 0.75, "XAUUSD", "美联储转鹰 + 美元走强")
    orders = await engine.process_signals({"current_price": price})
    for o in orders:
        print(f"  → {o.action.value}: size={o.size:.0f} status={o.status}")

    # ── 最终状态 ──
    print("\n--- Final State ---")
    status = engine.get_status()
    pos = status['execution_engine']['position']
    print(f"  Position: {pos}")
    stats = status['execution_engine']['stats']
    print(f"  Stats: orders={stats['total_orders']} filled={stats['filled_orders']}")


# ══════════════════════════════════════════════════════════════════
# Demo 4: Risk & Stop-Loss Mechanics
# ══════════════════════════════════════════════════════════════════

async def demo_risk_mechanics():
    """演示止损止盈 & 风控触发"""
    print("\n" + "=" * 60)
    print("Demo 4: Risk Management & Stop-Loss Mechanics")
    print("=" * 60)

    rm = RiskManager(symbol="XAUUSD", account_value=100_000)

    entry = 2660.0
    atr = 15.0

    # 止损止盈计算
    sl = rm.calculate_stop_loss(Direction.LONG, entry, atr)
    tp = rm.calculate_take_profit(Direction.LONG, entry, sl)
    print(f"\n  Entry:      ${entry:.2f}")
    print(f"  ATR:        ${atr:.2f}")
    print(f"  Stop Loss:  ${sl:.2f}  ({(entry-sl):.2f} risk)")
    print(f"  Take Profit:${tp:.2f}  (risk:reward = 1:{((tp-entry)/(entry-sl)):.1f})")

    # 移动止损
    pos = Position(
        symbol="XAUUSD", direction=Direction.LONG,
        size=0.5, entry_price=entry, entry_time=datetime.now(),
        stop_loss=sl,
    )

    print("\n--- Trailing Stop (price moves up) ---")
    for price in [2680, 2695, 2710, 2705, 2720]:
        new_stop = rm.calculate_trailing_stop(pos, price)
        if new_stop and (pos.stop_loss is None or new_stop > pos.stop_loss):
            pos.stop_loss = new_stop
            print(f"  Price={price:.1f}: stop raised → {pos.stop_loss:.1f}")

    # 触发止损
    print("\n--- Stop-Loss Trigger Check ---")
    risk_checks = rm.check_position_risk(pos, current_price=pos.stop_loss - 1)
    for c in risk_checks:
        if not c.passed:
            print(f"  !! {c.message} (level={c.level.name})")


# ══════════════════════════════════════════════════════════════════
# Demo 5: Live Engine Architecture (read-only inspection)
# ══════════════════════════════════════════════════════════════════

async def demo_architecture():
    """展示 BinanceExecutionEngine 架构 & 类图"""
    print("\n" + "=" * 60)
    print("Demo 5: BinanceExecutionEngine Architecture")
    print("=" * 60)

    print("""
┌─────────────────────────────────────────────────────┐
│                BinanceExecutionEngine               │
│─────────────────────────────────────────────────────│
│  市场属性:                                           │
│    _price_tick, _amount_step                         │
│    _min_notional, _contract_size                     │
│─────────────────────────────────────────────────────│
│  核心方法:                                           │
│    initialize()          加载市场 & 检测持仓模式      │
│    execute_order()       主入口 — 下单流水线          │
│─────────────────────────────────────────────────────│
│  内部流水线:                                         │
│    1. _resolve_order_params()   side / reduceOnly    │
│    2. _calculate_contract_quantity()  USD→contracts  │
│    3. _calculate_limit_price()  超价限价 (防滑点)     │
│    4. _place_order_with_retry()  下单 + 精度修正重试  │
│    5. _attach_stop_loss()    止损双挂                │
│    6. _update_position()     本地簿记                │
│─────────────────────────────────────────────────────│
│  双挂机制:                                           │
│    开仓成交 → STOP_MARKET (止损)                     │
│            → TAKE_PROFIT_MARKET (止盈, 可选)         │
│─────────────────────────────────────────────────────│
│  风控集成:                                           │
│    RiskManager.check_order()         下单前风控       │
│    monitor_position()               持仓中监控        │
│    _emergency_close()               风控触发平仓      │
└─────────────────────────────────────────────────────┘
    """)

    print("Order Flow (One-way Mode):")
    print("""
  DecisionEngine
       │
       ▼
  TradeOrder (action, size, price, stop_loss)
       │
       ▼
  execute_order()
       │
       ├── OPEN / ADD ──→ reduceOnly=False ──→ fill? ──→ attach_stop()
       │                                                      └── attach_tp()
       ├── CLOSE ──────→ reduceOnly=True  ──→ cancel_attached_orders()
       │                                       └── place limit order
       └── REVERSE ────→ close_position=True ──→ close old ──→ open new
                                                       └── wait fill ──→ attach_stop()
    """)


# ══════════════════════════════════════════════════════════════════
# Demo 6: Edge Cases & Error Handling
# ══════════════════════════════════════════════════════════════════

async def demo_edge_cases():
    """覆盖边界情况"""
    print("\n" + "=" * 60)
    print("Demo 6: Edge Cases & Error Handling")
    print("=" * 60)

    # Case 1: Non-initialized engine → execute_order() returns error
    print("\n--- Case 1: Engine Not Initialized ---")
    engine = BinanceExecutionEngine(api_key="d", api_secret="d")
    order = TradeOrder(
        order_id="edge-1", timestamp=datetime.now(),
        action=ActionType.OPEN_LONG, symbol="XAUUSD", size=1000,
    )
    result = await engine.execute_order(order, current_price=2660.0)
    print(f"  Result: success={result.success}  msg={result.message}")

    # Case 2: Min notional calculation
    print("\n--- Case 2: Min Notional Edge ---")
    engine2 = BinanceExecutionEngine(api_key="d", api_secret="d")
    engine2._price_tick = 0.1
    engine2._amount_step = 0.01
    engine2._min_notional = 5.0
    engine2._min_amount = 0.01
    qty = engine2._calculate_contract_quantity(
        TradeOrder("test", datetime.now(), ActionType.OPEN_LONG, "XAUUSD", 1.0),
        2660.0
    )
    notional = qty * 2660.0
    print(f"  Size=$1 → qty={qty:.6f} contracts, notional=${notional:.2f}")
    print(f"  Would be rejected: {notional < engine2._min_notional}")

    # Case 3: REVERSE & FLAT param resolution
    print("\n--- Case 3: Reverse & FLAT Resolution ---")
    for action in [ActionType.REVERSE_LONG, ActionType.REVERSE_SHORT, ActionType.FLAT]:
        order = TradeOrder(
            order_id=f"test-{action.value}", timestamp=datetime.now(),
            action=action, symbol="XAUUSD", size=5000,
        )
        side, ro, cp = engine2._resolve_order_params(order)
        print(f"  {action.value:<16} → side={side:<5} reduceOnly={ro:<6} closePosition={cp}")

    # Case 4: FLAT with position
    print("\n--- Case 4: FLAT Resolution (LONG pos → sell) ---")
    engine2.position = Position(
        symbol="XAUUSD", direction=Direction.LONG,
        size=0.5, entry_price=2660, entry_time=datetime.now(),
    )
    order_f = TradeOrder(
        order_id="flat-test", timestamp=datetime.now(),
        action=ActionType.FLAT, symbol="XAUUSD", size=5000,
    )
    side, ro, cp = engine2._resolve_order_params(order_f)
    print(f"  FLAT+LONG → side={side} reduceOnly={ro} closePosition={cp}")


# ══════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="BinanceExecutionEngine Demo")
    parser.add_argument("--live", action="store_true", help="Connect to Binance testnet")
    parser.add_argument("--api-key", default="", help="Binance API Key")
    parser.add_argument("--api-secret", default="", help="Binance API Secret")
    parser.add_argument("--symbol", default="XAUUSD", help="Trading symbol")
    args = parser.parse_args()

    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Trident Agent MVP — BinanceExecutionEngine Demo       ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 基础演示 (无需联网)
    await demo_precision()
    await demo_order_resolution()
    await demo_trade_flow()
    await demo_risk_mechanics()
    await demo_architecture()
    await demo_edge_cases()

    # 实盘连接 (如果提供)
    if args.live:
        print("\n" + "=" * 60)
        print("LIVE Connection Demo")
        print("=" * 60)

        engine = BinanceExecutionEngine(
            symbol=args.symbol,
            api_key=args.api_key,
            api_secret=args.api_secret,
            proxy_url="http://127.0.0.1:10808",
            testnet=True,
        )

        print(f"\nConnecting to Binance Testnet ({args.symbol})...")
        ok = await engine.initialize()

        if ok:
            print("✓ Connected!")

            # 同步持仓
            await engine.sync_position()
            pos = engine.get_position()
            if pos:
                print(f"  Current position: {pos.direction.name} {pos.size} @ {pos.entry_price}")
            else:
                print("  No current position")

            # 获取挂单
            open_orders = await engine.get_open_orders()
            print(f"  Open orders: {len(open_orders)}")

            await engine.close()
        else:
            print("✗ Connection failed — check API key / network")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)

    print("\nNext steps for production:")
    print("  1. Set BINANCE_API_KEY and BINANCE_API_SECRET env vars")
    print("  2. Start with testnet: engine = create_binance_engine(testnet=True)")
    print("  3. Run: await engine.initialize() → verify market props")
    print("  4. Test with tiny position: size=10 (≈$10 notional)")
    print("  5. Monitor: await engine.monitor_position(price) each tick")
    print("  6. Go live: testnet=False after validation")


if __name__ == "__main__":
    asyncio.run(main())
