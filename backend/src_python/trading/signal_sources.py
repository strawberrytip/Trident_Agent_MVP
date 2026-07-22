"""
Signal Source Implementations - 信号源实现示例

包含：
1. NewsSignalSource - 新闻信号源（对接现有新闻系统）
2. CTASignalSource - CTA策略信号源（对接用户CTA策略）
3. ManualSignalSource - 手动信号源（用于测试）
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
import sqlite3

from .signals import SignalSource, Signal, Direction


class NewsSignalSource(SignalSource):
    """
    新闻信号源

    从 trident_event_bus.db 的 ai_decisions 表读取AI决策
    """

    def __init__(self, db_path: str = "../trident_event_bus.db"):
        super().__init__(name="NewsAI", symbols=["XAUUSD", "BTCUSDT", "WTIUSD"])
        self.db_path = db_path

    async def generate_signals(self, context: Dict[str, Any]) -> List[Signal]:
        """
        从数据库读取最新的AI决策并转换为信号

        Args:
            context: 包含 current_price 等信息
        """
        signals = []

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # 读取最新的未处理决策
            cursor = conn.execute("""
                SELECT
                    ad.id,
                    ad.news_id,
                    rn.content as news_content,
                    ad.suggested_action,
                    ad.sentiment_score,
                    ad.reasoning,
                    ad.target_asset,
                    ad.cluster_size,
                    GROUP_CONCAT(DISTINCT ad.vip_tag) as vip_tags
                FROM ai_decisions ad
                INNER JOIN raw_news rn ON ad.news_id = rn.id
                WHERE ad.created_at > datetime('now', '-10 minutes')
                  AND ad.suggested_action IN ('BUY', 'SELL')
                GROUP BY ad.id
                ORDER BY ad.created_at DESC
                LIMIT 10
            """)
            rows = cursor.fetchall()
            conn.close()

            for row in rows:
                # 映射到交易方向
                action_map = {"BUY": Direction.LONG, "SELL": Direction.SHORT}
                direction = action_map.get(row["suggested_action"], Direction.FLAT)

                if direction == Direction.FLAT:
                    continue

                # 计算信号强度
                # 基础强度 = |sentiment_score|
                strength = abs(row["sentiment_score"])

                # 聚合加成
                cluster_size = row["cluster_size"] or 1
                if cluster_size > 1:
                    strength *= (1 + cluster_size * 0.1)

                # VIP加成
                vip_tags = row["vip_tags"] or ""
                if "VIP" in vip_tags:
                    strength *= 1.25

                # 限制在 [0, 1]
                strength = min(max(strength, 0.0), 1.0)

                # 映射品种
                asset_map = {
                    "XAU": "XAUUSD",
                    "GOLD": "XAUUSD",
                    "BTC": "BTCUSDT",
                    "WTI": "WTIUSD"
                }
                symbol = asset_map.get(row["target_asset"], "XAUUSD")

                # 创建信号
                signal = Signal(
                    timestamp=datetime.now(),
                    source=f"Kimi K3-{row['id']}",
                    direction=direction,
                    strength=strength,
                    symbol=symbol,
                    reason=row["reasoning"][:200],
                    confidence=min(abs(row["sentiment_score"]) + 0.5, 1.0),
                    metadata={
                        "news_id": row["news_id"],
                        "decision_id": row["id"],
                        "cluster_size": cluster_size,
                        "vip_tags": vip_tags,
                        "original_action": row["suggested_action"]
                    },
                    tags=["News", "AI"] + ([tag for tag in vip_tags.split(",") if tag] if vip_tags else [])
                )

                if self.validate_signal(signal):
                    signals.append(signal)

        except Exception as e:
            print(f"[NewsSignalSource] 读取失败: {e}")

        return signals


class CTASignalSource(SignalSource):
    """
    CTA策略信号源

    适配用户的CTA策略：
    - XAUUSDTTrendPullbackV2
    - XAUUSDTMultiTimeframeV4
    - XAUUSDTATRCompressionV3
    """

    def __init__(
        self,
        strategies: Optional[List[Any]] = None,
        symbol: str = "XAUUSD"
    ):
        super().__init__(name="CTA", symbols=[symbol])
        self.strategies = strategies or []
        self.symbol = symbol

    def add_strategy(self, strategy):
        """添加CTA策略"""
        self.strategies.append(strategy)

    async def generate_signals(self, context: Dict[str, Any]) -> List[Signal]:
        """
        从CTA策略生成信号

        Args:
            context: 应包含 historical_data (DataFrame)
        """
        signals = []

        price_data = context.get("historical_data")
        if price_data is None:
            return signals

        for strategy in self.strategies:
            try:
                # 调用CTA策略生成信号
                cta_signals = strategy.generate_signals()

                for cta_sig in cta_signals:
                    # 映射方向
                    direction_map = {
                        1: Direction.LONG,
                        -1: Direction.SHORT,
                        0: Direction.FLAT
                    }
                    direction = direction_map.get(cta_sig.signal.value, Direction.FLAT)

                    if direction == Direction.FLAT:
                        continue

                    # 计算强度（基于ATR）
                    strength = 0.5  # CTA信号基础强度
                    if hasattr(cta_sig, 'atr_value') and cta_sig.atr_value:
                        # ATR越大，信心越强
                        strength = min(0.3 + (cta_sig.atr_value / 50) * 0.7, 1.0)

                    signal = Signal(
                        timestamp=cta_sig.timestamp,
                        source=strategy.NAME,
                        direction=direction,
                        strength=strength,
                        symbol=self.symbol,
                        reason=f"{strategy.NAME} 信号",
                        confidence=0.7,
                        metadata={
                            "stop_loss": cta_sig.stop_loss,
                            "atr_value": cta_sig.atr_value,
                            "strategy_name": strategy.NAME
                        },
                        tags=["CTA", "Trend"]
                    )

                    if self.validate_signal(signal):
                        signals.append(signal)

            except Exception as e:
                print(f"[CTASignalSource] 策略 {strategy.NAME} 失败: {e}")

        return signals


class ManualSignalSource(SignalSource):
    """
    手动信号源

    用于测试或手动干预
    """

    def __init__(self):
        super().__init__(name="Manual", symbols=["XAUUSD"])
        self.pending_signals: List[Dict] = []

    def add_manual_signal(
        self,
        direction: str,
        strength: float,
        symbol: str = "XAUUSD",
        reason: str = ""
    ):
        """添加手动信号"""
        direction_map = {
            "LONG": Direction.LONG,
            "SHORT": Direction.SHORT,
            "FLAT": Direction.FLAT
        }

        self.pending_signals.append({
            "direction": direction_map.get(direction.upper(), Direction.FLAT),
            "strength": strength,
            "symbol": symbol,
            "reason": reason,
            "timestamp": datetime.now()
        })

    async def generate_signals(self, context: Dict[str, Any]) -> List[Signal]:
        """返回待处理的手动信号"""
        signals = []

        for sig_data in self.pending_signals:
            signal = Signal(
                timestamp=sig_data["timestamp"],
                source="Manual",
                direction=sig_data["direction"],
                strength=sig_data["strength"],
                symbol=sig_data["symbol"],
                reason=sig_data["reason"] or "手动信号",
                confidence=1.0,  # 手动信号置信度最高
                tags=["Manual"]
            )

            if self.validate_signal(signal):
                signals.append(signal)

        # 清空已处理的信号
        self.pending_signals.clear()

        return signals


class HybridSignalSource(SignalSource):
    """
    混合信号源

    同时考虑新闻和技术面，只在两者共振时输出信号
    """

    def __init__(
        self,
        news_source: NewsSignalSource,
        cta_source: CTASignalSource
    ):
        super().__init__(name="Hybrid", symbols=["XAUUSD"])
        self.news_source = news_source
        self.cta_source = cta_source

    async def generate_signals(self, context: Dict[str, Any]) -> List[Signal]:
        """
        生成混合信号

        逻辑：
        1. 获取新闻信号和CTA信号
        2. 检查方向是否一致
        3. 一致时输出增强信号，不一致时输出FLAT
        """
        # 获取各类信号
        news_signals = await self.news_source.generate_signals(context)
        cta_signals = await self.cta_source.generate_signals(context)

        # 按方向分组
        news_directions = set(s.direction for s in news_signals if s.direction != Direction.FLAT)
        cta_directions = set(s.direction for s in cta_signals if s.direction != Direction.FLAT)

        # 检查共振
        if not news_directions or not cta_directions:
            return []

        common_directions = news_directions & cta_directions
        if not common_directions:
            return []

        # 有共振，生成混合信号
        direction = common_directions.pop()

        # 合并强度
        news_strength = sum(s.strength for s in news_signals if s.direction == direction)
        cta_strength = sum(s.strength for s in cta_signals if s.direction == direction)
        combined_strength = min((news_strength + cta_strength) / 2, 1.0)

        signal = Signal(
            timestamp=datetime.now(),
            source="Hybrid",
            direction=direction,
            strength=combined_strength,
            symbol="XAUUSD",
            reason=f"新闻+CTA共振: {len(news_signals)}个新闻信号 + {len(cta_signals)}个CTA信号",
            confidence=0.9,  # 共振信号置信度高
            metadata={
                "news_count": len(news_signals),
                "cta_count": len(cta_signals),
                "news_strength": news_strength,
                "cta_strength": cta_strength
            },
            tags=["Hybrid", "Resonance"]
        )

        if self.validate_signal(signal):
            return [signal]

        return []
