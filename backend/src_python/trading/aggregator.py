"""
Signal Aggregator - 信号聚合器

负责：
1. 收集多个信号源的信号
2. 处理信号冲突
3. 计算聚合信号
4. 管理信号源权重
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict

from .signals import Signal, SignalSource, Direction


@dataclass
class SourceConfig:
    """信号源配置"""
    name: str
    enabled: bool = True
    weight: float = 1.0           # 投票权重
    min_strength: float = 0.3     # 最小信号强度阈值
    max_strength: float = 1.0     # 最大信号强度
    vip_boost: float = 1.0         # VIP信号加成
    allow_short: bool = True      # 是否允许做空信号


@dataclass
class AggregatedSignal:
    """聚合信号"""
    timestamp: datetime
    symbol: str

    # 投票结果
    votes_long: int = 0
    votes_short: int = 0
    votes_flat: int = 0

    # 加权投票
    weighted_long: float = 0.0
    weighted_short: float = 0.0

    # 最终决策
    final_direction: Direction = Direction.FLAT
    final_strength: float = 0.0

    # 参与信号源
    sources: List[str] = field(default_factory=list)

    # 冲突标记
    has_conflict: bool = False
    conflict_reason: str = ""

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "votes_long": self.votes_long,
            "votes_short": self.votes_short,
            "votes_flat": self.votes_flat,
            "weighted_long": round(self.weighted_long, 4),
            "weighted_short": round(self.weighted_short, 4),
            "final_direction": self.final_direction.name,
            "final_strength": round(self.final_strength, 4),
            "sources": self.sources,
            "has_conflict": self.has_conflict,
            "conflict_reason": self.conflict_reason,
        }


class SignalAggregator:
    """
    信号聚合器

    功能：
    1. 管理多个信号源配置
    2. 收集并验证信号
    3. 处理冲突（做多/做空冲突）
    4. 计算加权投票结果
    """

    def __init__(self):
        # 信号源配置
        self.sources: Dict[str, SourceConfig] = {}

        # 历史信号缓存（用于去重）
        self._signal_cache: Dict[str, set] = defaultdict(set)

        # 冲突处理策略
        self.conflict_strategy = "consensus"  # consensus|majority|weighted|priority

    def add_source(self, config: SourceConfig):
        """添加信号源配置"""
        self.sources[config.name] = config

    def remove_source(self, name: str):
        """移除信号源"""
        if name in self.sources:
            del self.sources[name]

    def get_source_config(self, name: str) -> Optional[SourceConfig]:
        """获取信号源配置"""
        return self.sources.get(name)

    def set_conflict_strategy(self, strategy: str):
        """
        设置冲突处理策略

        Args:
            strategy: 冲突处理策略
                - consensus: 需要一致共识（默认）
                - majority: 简单多数决
                - weighted: 加权投票
                - priority: 按优先级取第一个
        """
        valid_strategies = ["consensus", "majority", "weighted", "priority"]
        if strategy in valid_strategies:
            self.conflict_strategy = strategy
        else:
            raise ValueError(f"Invalid strategy: {strategy}")

    async def aggregate(
        self,
        signals: List[Signal],
        symbol: str = "XAUUSD"
    ) -> AggregatedSignal:
        """
        聚合多个信号

        Args:
            signals: 信号列表
            symbol: 交易品种

        Returns:
            AggregatedSignal: 聚合后的信号
        """
        agg = AggregatedSignal(
            timestamp=datetime.now(),
            symbol=symbol
        )

        # 按方向分组
        long_signals = []
        short_signals = []
        flat_signals = []

        for signal in signals:
            # 获取信号源配置
            config = self.sources.get(signal.source)
            if not config or not config.enabled:
                continue  # 信号源未启用

            # 强度过滤
            if signal.strength < config.min_strength:
                continue

            # 做空权限检查
            if signal.direction == Direction.SHORT and not config.allow_short:
                continue

            # VIP加成
            strength = signal.strength
            if "VIP" in signal.tags:
                strength *= config.vip_boost

            # 权重应用
            weighted_strength = strength * config.weight

            # 分组
            if signal.direction == Direction.LONG:
                long_signals.append((signal, weighted_strength))
            elif signal.direction == Direction.SHORT:
                short_signals.append((signal, weighted_strength))
            else:
                flat_signals.append((signal, weighted_strength))

            # 记录来源
            agg.sources.append(signal.source)

        # 统计投票
        agg.votes_long = len(long_signals)
        agg.votes_short = len(short_signals)
        agg.votes_flat = len(flat_signals)

        # 计算加权投票
        agg.weighted_long = sum(s[1] for s in long_signals)
        agg.weighted_short = sum(s[1] for s in short_signals)

        # 检测冲突
        total_sources = len(signals)
        if agg.votes_long > 0 and agg.votes_short > 0:
            conflict_ratio = min(agg.votes_long, agg.votes_short) / total_sources
            if conflict_ratio > 0.3:  # 超过30%的冲突
                agg.has_conflict = True
                agg.conflict_reason = f"多头{agg.votes_long} vs 空头{agg.votes_short}"

        # 根据策略做最终决策
        agg.final_direction, agg.final_strength = self._make_decision(
            long_signals, short_signals, flat_signals, agg
        )

        # 存储元数据
        agg.metadata = {
            "total_sources": total_sources,
            "active_sources": len(agg.sources),
            "strategy": self.conflict_strategy,
            "long_signals": [{"source": s.source, "strength": s.strength}
                             for s, _ in long_signals],
            "short_signals": [{"source": s.source, "strength": s.strength}
                              for s, _ in short_signals],
        }

        return agg

    def _make_decision(
        self,
        long_signals: List,
        short_signals: List,
        flat_signals: List,
        agg: AggregatedSignal
    ) -> tuple[Direction, float]:
        """
        根据配置的策略做最终决策
        """
        total_long = agg.weighted_long
        total_short = agg.weighted_short

        if self.conflict_strategy == "consensus":
            return self._decision_consensus(long_signals, short_signals, flat_signals, agg)
        elif self.conflict_strategy == "majority":
            return self._decision_majority(agg.votes_long, agg.votes_short, agg.votes_flat)
        elif self.conflict_strategy == "weighted":
            return self._decision_weighted(total_long, total_short)
        elif self.conflict_strategy == "priority":
            return self._decision_priority(long_signals, short_signals)
        else:
            return Direction.FLAT, 0.0

    def _decision_consensus(self, long_signals, short_signals, flat_signals, agg):
        """
        共识策略：需要大多数一致

        规则：
        - 如果有多头也有空头 → FLAT（冲突）
        - 如果信号总数 < 3 → FLAT（样本不足）
        - 需要至少 60% 同向
        """
        total = len(long_signals) + len(short_signals) + len(flat_signals)

        if total < 3:
            return Direction.FLAT, 0.0

        if len(long_signals) > 0 and len(short_signals) > 0:
            # 有冲突 → 平仓
            return Direction.FLAT, 0.0

        if len(long_signals) >= len(short_signals) and len(long_signals) > 0:
            # 多头占优
            ratio = len(long_signals) / total
            strength = min(agg.weighted_long / max(len(long_signals), 1), 1.0)
            return Direction.LONG, strength * ratio
        elif len(short_signals) > 0:
            # 空头占优
            ratio = len(short_signals) / total
            strength = min(agg.weighted_short / max(len(short_signals), 1), 1.0)
            return Direction.SHORT, strength * ratio

        return Direction.FLAT, 0.0

    def _decision_majority(self, long_votes, short_votes, flat_votes):
        """简单多数决"""
        total = long_votes + short_votes + flat_votes
        if total == 0:
            return Direction.FLAT, 0.0

        if long_votes > short_votes and long_votes > flat_votes:
            strength = long_votes / total
            return Direction.LONG, strength
        elif short_votes > long_votes and short_votes > flat_votes:
            strength = short_votes / total
            return Direction.SHORT, strength

        return Direction.FLAT, 0.0

    def _decision_weighted(self, total_long, total_short):
        """加权投票"""
        if total_long == total_short:
            return Direction.FLAT, 0.0

        if total_long > total_short:
            total = total_long + total_short
            strength = total_long / total
            return Direction.LONG, min(strength, 1.0)
        else:
            total = total_long + total_short
            strength = total_short / total
            return Direction.SHORT, min(strength, 1.0)

    def _decision_priority(self, long_signals, short_signals):
        """优先级策略：取权重最高的信号"""
        all_signals = []

        for signal, weight in long_signals:
            all_signals.append((Direction.LONG, signal.strength, signal.source))

        for signal, weight in short_signals:
            all_signals.append((Direction.SHORT, signal.strength, signal.source))

        if not all_signals:
            return Direction.FLAT, 0.0

        # 按强度排序，取最强
        all_signals.sort(key=lambda x: x[1], reverse=True)
        direction, strength, source = all_signals[0]

        return direction, strength

    def get_status(self) -> Dict[str, Any]:
        """获取聚合器状态"""
        return {
            "sources_count": len(self.sources),
            "enabled_sources": [name for name, cfg in self.sources.items() if cfg.enabled],
            "conflict_strategy": self.conflict_strategy,
            "sources": {
                name: cfg.__dict__
                for name, cfg in self.sources.items()
            }
        }
