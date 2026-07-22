#!/usr/bin/env python3
"""
Trading Engine Demo - 交易引擎演示

展示如何使用模块化交易系统
"""

import asyncio
import sys
import os

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading import TradingEngine, Signal, Direction
from trading.signal_sources import NewsSignalSource, CTASignalSource, ManualSignalSource
from trading.aggregator import SourceConfig


async def demo_basic_usage():
    """基础使用演示"""
    print("=" * 60)
    print("Trident 交易引擎演示")
    print("=" * 60)

    # 1. 创建交易引擎
    engine = TradingEngine(
        symbol="XAUUSD",
        account_value=100000,
        mode="SIMULATION"
    )

    # 2. 配置信号源权重
    engine.aggregator.add_source(SourceConfig(
        name="Kimi K3",
        enabled=True,
        weight=1.2,      # Kimi K3权重更高
        min_strength=0.3,
        vip_boost=1.25   # VIP新闻加成
    ))

    engine.aggregator.add_source(SourceConfig(
        name="DeepSeek",
        enabled=True,
        weight=1.0,
        min_strength=0.2
    ))

    engine.aggregator.add_source(SourceConfig(
        name="Grok",
        enabled=True,
        weight=0.8,      # Grok权重稍低
        min_strength=0.2
    ))

    # 3. 添加手动信号源（用于测试）
    manual_source = ManualSignalSource()
    engine.add_signal_source(manual_source)

    # 模拟：添加一些手动信号
    print("\n--- 测试1: 添加手动多头信号 ---")
    manual_source.add_manual_signal(
        direction="LONG",
        strength=0.7,
        symbol="XAUUSD",
        reason="美联储放鸽预期"
    )

    context = {"current_price": 2500.0}
    orders = await engine.process_signals(context)

    if orders:
        for order in orders:
            print(f"生成订单: {order.action.value} {order.size} @ {order.reason}")
    else:
        print("无订单生成")

    # 4. 查看状态
    print("\n--- 引擎状态 ---")
    status = engine.get_status()
    print(f"信号源数量: {status['signal_sources']}")
    print(f"当前持仓: {status['execution_engine']['position']}")


async def demo_news_integration():
    """新闻信号源集成演示"""
    print("\n" + "=" * 60)
    print("新闻信号源集成演示")
    print("=" * 60)

    # 创建引擎
    engine = TradingEngine(
        symbol="XAUUSD",
        account_value=100000
    )

    # 添加新闻信号源
    news_source = NewsSignalSource(
        db_path="../trident_event_bus.db"
    )
    engine.add_signal_source(news_source)

    # 配置新闻信号源
    engine.aggregator.add_source(SourceConfig(
        name="Kimi K3",
        enabled=True,
        weight=1.0,
        vip_boost=1.5,  # 新闻VIP加成更高
        allow_short=True
    ))

    print("\n--- 从数据库读取最新新闻决策 ---")
    context = {"current_price": 2500.0}
    orders = await engine.process_signals(context)

    if orders:
        print(f"生成 {len(orders)} 个订单:")
        for order in orders:
            print(f"  - {order.action.value}: {order.reason}")
    else:
        print("当前无交易信号")


async def demo_cta_integration():
    """CTA策略集成演示"""
    print("\n" + "=" * 60)
    print("CTA策略集成演示")
    print("=" * 60)

    # TODO: 导入CTA策略
    # from strategies.xauusdt_trend_pullback_v2 import XAUUSDTTrendPullbackV2
    # from strategies.xauusdt_multitimeframe_v4 import XAUUSDTMultiTimeframeV4

    # 创建CTA信号源
    cta_source = CTASignalSource(symbol="XAUUSD")

    # TODO: 添加CTA策略实例
    # cta_source.add_strategy(XAUUSDTTrendPullbackV2(price_data))
    # cta_source.add_strategy(XAUUSDTMultiTimeframeV4(price_data_4h, price_data_daily))

    print("\n--- CTA信号源 ---")
    print(f"策略数量: {len(cta_source.strategies)}")
    print("提示: 实际使用时需要导入并添加CTA策略实例")


async def demo_hybrid_mode():
    """混合模式演示"""
    print("\n" + "=" * 60)
    print("混合模式演示（新闻 + CTA）")
    print("=" * 60)

    # 创建混合场景
    manual_source = ManualSignalSource()

    # 模拟新闻看多
    manual_source.add_manual_signal("LONG", 0.6, "XAUUSD", "美联储暗示降息")

    # 模拟CTA也看多
    manual_source.add_manual_signal("LONG", 0.5, "XAUUSD", "EMA趋势向上")

    # TODO: 实际使用时用 HybridSignalSource
    print("\n--- 混合模式 ---")
    print("当新闻和技术面都看多时，信号强度增强")
    print("当新闻和技术面方向冲突时，观望或平仓")


async def demo_risk_controls():
    """风控演示"""
    print("\n" + "=" * 60)
    print("风控功能演示")
    print("=" * 60)

    engine = TradingEngine(
        symbol="XAUUSD",
        account_value=100000
    )

    # 查看风控参数
    risk = engine.risk_manager
    print(f"\n最大单品种仓位: {risk.max_position_size:.0%}")
    print(f"最大总敞口: {risk.max_total_exposure:.0%}")
    print(f"单笔最大亏损: {risk.max_loss_per_trade:.0%}")
    print(f"日内亏损限制: {risk.daily_loss_limit:.0%}")

    # 计算止损止盈示例
    entry_price = 2500.0
    print(f"\n开仓价: ${entry_price:.2f}")
    print(f"止损价: ${risk.calculate_stop_loss(Direction.LONG, entry_price):.2f}")
    print(f"止盈价: ${risk.calculate_take_profit(Direction.LONG, entry_price):.2f}")


async def main():
    """运行所有演示"""
    await demo_basic_usage()
    await demo_news_integration()
    await demo_cta_integration()
    await demo_hybrid_mode()
    await demo_risk_controls()

    print("\n" + "=" * 60)
    print("演示完成！")
    print("=" * 60)
    print("\n下一步：")
    print("1. 集成你的CTA策略到 CTASignalSource")
    print("2. 配置新闻信号源的数据源路径")
    print("3. 设置实际价格数据源")
    print("4. 运行回测验证策略")


if __name__ == "__main__":
    asyncio.run(main())
