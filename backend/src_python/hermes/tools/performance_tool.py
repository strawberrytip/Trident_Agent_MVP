"""
Phase 1 — Performance Intelligence Layer
==========================================

Read-only analytics engine.  Answers three families of questions:

  1. 某类新闻历史是否有效？
     ``evaluate_news_category("CRYPTO", asset="BTC")``

  2. 某 prediction_type 在不同 regime 下胜率？
     ``evaluate_prediction_by_regime("continuation", regime="Bull")``

  3. 当前信号是否处于历史高胜率环境？
     ``evaluate_signal_context(signal_id=1234)``

Every output includes:
  - sample_size       → n (settled and verdict-assigned)
  - win_rate          → wins / n
  - avg_pnl           → mean forward_pnl %
  - avg_mfe           → mean MFE %
  - avg_mae           → mean MAE %
  - confidence_interval → Wilson 95% CI for win_rate (binomial)
  - small_sample_warning → True when n < 20

Architecture:
  tools/performance_tool.py  ←  pure-Python stats + SQL reads
  repositories/signal_repo   ←  raw row retrieval (optional; tool can read directly)

Never writes to DB.  Never touches trade execution.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TZ_SHANGHAI = timezone(timedelta(hours=8))
_DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "trident_event_bus.db",
)

# Minimum sample size before we trust the statistics
_MIN_SAMPLE = 20
# Confidence level for Wilson score interval
_Z_95 = 1.96

# Recognised values (must match engine.py canonical values)
_VALID_PREDICTION_TYPES = {"reversal", "continuation", "breakout"}
_VALID_EVENT_PHASES = {"early", "mid", "late"}
_VALID_MARKET_CONFIRMATIONS = {"positive", "negative", "unknown"}
_VALID_ACTIONS = {"BUY", "SELL", "HOLD"}
_VALID_VERDICTS = {"WIN", "LOSS", "HOLD"}

# Asset name resolution
_ASSET_MAP: Dict[str, str] = {
    "BTC": "BTC", "ETH": "ETH",
    "GOLD": "XAU", "XAU": "XAU", "XAUUSD": "XAU",
    "WTI": "WTI", "OIL": "WTI",
}
# Reverse lookup: DB value → canonical display name
_ASSET_DISPLAY: Dict[str, str] = {v: k for k, v in _ASSET_MAP.items()}
_ASSET_DISPLAY.update({"BTC": "BTC", "XAU": "XAU", "WTI": "WTI"})


def _resolve_asset(asset: Optional[str]) -> Optional[str]:
    """Map user-facing asset names to DB target_asset values."""
    if not asset:
        return None
    return _ASSET_MAP.get(asset.upper(), asset.upper())


# ---------------------------------------------------------------------------
# Statistics helpers (pure functions, no DB)
# ---------------------------------------------------------------------------

def wilson_ci(wins: int, n: int, z: float = _Z_95) -> Tuple[float, float]:
    """Wilson score confidence interval for a binomial proportion.

    More accurate than normal approximation for small samples and
    proportions near 0 or 1.  Returns (lower, upper) as decimals (0-1).

    Args:
        wins: Number of successes (WIN verdicts).
        n:    Total sample size (must be > 0).
        z:    Z-score for desired confidence level (1.96 = 95%).
    """
    if n == 0:
        return (0.0, 0.0)
    p_hat = wins / n
    z2 = z * z
    denominator = 1.0 + z2 / n
    centre = (p_hat + z2 / (2.0 * n)) / denominator
    margin = z * math.sqrt(
        (p_hat * (1.0 - p_hat) + z2 / (4.0 * n)) / n
    ) / denominator
    lower = max(0.0, centre - margin)
    upper = min(1.0, centre + margin)
    return (round(lower, 4), round(upper, 4))


def mean_ci(values: List[float], z: float = _Z_95) -> Tuple[float, float]:
    """Confidence interval for a mean (normal approximation).

    Returns (lower, upper) in the same unit as the input values.
    Returns (0.0, 0.0) when n < 2.
    """
    n = len(values)
    if n < 2:
        return (0.0, 0.0)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    se = math.sqrt(variance) / math.sqrt(n)
    margin = z * se
    return (round(mean - margin, 6), round(mean + margin, 6))


# ---------------------------------------------------------------------------
# Regime extraction from decision_context JSON
# ---------------------------------------------------------------------------

def _extract_trend(decision_context: str, target_asset: str) -> Optional[str]:
    """Parse the trend regime from a decision_context JSON string.

    decision_context format (engine.py _build_summary output):
        {"assets": {"BTC/USDT": {"trend": "Bull", ...}, ...}, "macro": {...}}

    Returns one of: "Strong Bull", "Bull", "Mild Bull", "Ranging",
                     "Mild Bear", "Bear", "Strong Bear", or None.
    """
    if not decision_context or not decision_context.strip():
        return None
    try:
        ctx = json.loads(decision_context)
    except (json.JSONDecodeError, TypeError):
        return None

    assets = ctx.get("assets", {})
    if not isinstance(assets, dict):
        return None

    # Map target_asset → CCXT symbol (e.g. "BTC" → "BTC/USDT", "XAU" → "XAU/USDT")
    resolved = _resolve_asset(target_asset)
    symbol = f"{resolved}/USDT" if resolved else None

    for key, value in assets.items():
        if not isinstance(value, dict):
            continue
        trend = value.get("trend")
        if trend is None:
            continue
        # Match: "BTC/USDT" ↔ target_asset "BTC"
        if symbol and key == symbol:
            return str(trend)
        # Fallback: fuzzy match asset name in key
        if resolved and resolved.upper() in key.upper().replace("/", ""):
            return str(trend)

    return None


def _classify_regime(trend: Optional[str]) -> str:
    """Collapse fine-grained trend labels into coarse regime buckets.

    Mapping:
        Strong Bull / Bull / Mild Bull  → "Bull"
        Strong Bear / Bear / Mild Bear  → "Bear"
        Ranging                         → "Ranging"
        None / empty / unknown          → "unknown"
    """
    if not trend:
        return "unknown"
    t = trend.strip().lower()
    if "bull" in t:
        return "Bull"
    if "bear" in t:
        return "Bear"
    if "ranging" in t or "range" in t:
        return "Ranging"
    return "unknown"


# ---------------------------------------------------------------------------
# StatsResult — canonical output format
# ---------------------------------------------------------------------------

@dataclass
class StatsResult:
    """Canonical output of every PerformanceQuery method."""

    # ── Query identity ──
    query_description: str = ""              # Human-readable description of the slice
    filters_applied: Dict[str, Any] = field(default_factory=dict)

    # ── Core metrics ──
    sample_size: int = 0
    win_count: int = 0
    loss_count: int = 0
    hold_count: int = 0
    win_rate: float = 0.0                    # decimal (0.0–1.0), only among WIN/LOSS

    # ── PnL metrics (%, among settled with forward_pnl available) ──
    avg_pnl: Optional[float] = None          # mean forward_pnl %
    avg_mfe: Optional[float] = None          # mean MFE %
    avg_mae: Optional[float] = None          # mean MAE %
    std_pnl: Optional[float] = None          # std of forward_pnl %
    total_pnl: Optional[float] = None        # sum of all forward_pnl %

    # ── Confidence intervals ──
    win_rate_ci_lower: float = 0.0
    win_rate_ci_upper: float = 0.0
    pnl_ci_lower: Optional[float] = None
    pnl_ci_upper: Optional[float] = None

    # ── Data quality ──
    small_sample_warning: bool = False
    min_sample_for_reliability: int = _MIN_SAMPLE

    # ── Breakdowns (optional, populated by cross-section methods) ──
    by_asset: Dict[str, "StatsResult"] = field(default_factory=dict)
    by_regime: Dict[str, "StatsResult"] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-safe dict for API / tool output."""
        d: Dict[str, Any] = {
            "query_description": self.query_description,
            "filters_applied": self.filters_applied,
            "sample_size": self.sample_size,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "hold_count": self.hold_count,
            "win_rate": round(self.win_rate, 4),
            "avg_pnl": round(self.avg_pnl, 6) if self.avg_pnl is not None else None,
            "avg_mfe": round(self.avg_mfe, 6) if self.avg_mfe is not None else None,
            "avg_mae": round(self.avg_mae, 6) if self.avg_mae is not None else None,
            "std_pnl": round(self.std_pnl, 6) if self.std_pnl is not None else None,
            "total_pnl": round(self.total_pnl, 6) if self.total_pnl is not None else None,
            "win_rate_ci_95": [self.win_rate_ci_lower, self.win_rate_ci_upper],
            "pnl_ci_95": (
                [self.pnl_ci_lower, self.pnl_ci_upper]
                if self.pnl_ci_lower is not None and self.pnl_ci_upper is not None
                else None
            ),
            "small_sample_warning": self.small_sample_warning,
            "reliable": not self.small_sample_warning,
        }
        if self.by_asset:
            d["by_asset"] = {k: v.to_dict() for k, v in self.by_asset.items()}
        if self.by_regime:
            d["by_regime"] = {k: v.to_dict() for k, v in self.by_regime.items()}
        return d

    def summary(self) -> str:
        """One-line human-readable summary."""
        reliable = "" if not self.small_sample_warning else " [小样本⚠]"
        pnl_str = f"PnL={self.avg_pnl:+.2f}%" if self.avg_pnl is not None else "PnL=N/A"
        ci_str = f"CI=[{self.win_rate_ci_lower:.0%}–{self.win_rate_ci_upper:.0%}]"
        return (
            f"n={self.sample_size} | WR={self.win_rate:.0%} {ci_str}{reliable}"
            f" | {pnl_str} | MFE={self.avg_mfe:+.2f}% MAE={self.avg_mae:+.2f}%"
            if self.avg_mfe is not None and self.avg_mae is not None
            else f"n={self.sample_size} | WR={self.win_rate:.0%} {ci_str}{reliable}"
            f" | {pnl_str}"
        )


# ---------------------------------------------------------------------------
# PerformanceQuery — main engine
# ---------------------------------------------------------------------------

class PerformanceQuery:
    """Read-only performance analytics over the ai_decisions table.

    Usage::

        pq = PerformanceQuery()
        result = pq.evaluate_prediction_by_regime("continuation", regime="Bull")
        print(result.summary())
        # n=45 | WR=67% CI=[52%–79%] | PnL=+0.82% | MFE=+1.21% MAE=0.34%
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or _DEFAULT_DB
        # Lazily discovered column availability
        self._has_outcome_cols: Optional[bool] = None

    # ------------------------------------------------------------------
    # Internal — DB connection
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

    def _column_exists(self, column: str) -> bool:
        conn = self._connect()
        try:
            rows = conn.execute("PRAGMA table_info(ai_decisions)").fetchall()
        finally:
            conn.close()
        return any(r["name"] == column for r in rows)

    def _has_outcome_metrics(self) -> bool:
        """Check whether mfe_pct / mae_pct / forward_pnl columns exist."""
        if self._has_outcome_cols is None:
            self._has_outcome_cols = self._column_exists("mfe_pct")
        return self._has_outcome_cols

    # ------------------------------------------------------------------
    # Internal — data retrieval
    # ------------------------------------------------------------------

    def _fetch_settled(
        self,
        days: int = 90,
        target_asset: Optional[str] = None,
        suggested_action: Optional[str] = None,
        market_category: Optional[str] = None,
        prediction_type: Optional[str] = None,
        event_phase: Optional[str] = None,
        market_confirmation: Optional[str] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Fetch settled signals with all Phase 0/0.5 metadata columns.

        Returns raw rows as dicts.  Caller does Python-side filtering for
        dimensions that can't be expressed in SQL (regime from JSON).
        """
        has_outcome = self._has_outcome_metrics()
        outcome_cols = (
            "mfe_pct, mae_pct, forward_pnl, mfe_time_mins"
            if has_outcome
            else "NULL AS mfe_pct, NULL AS mae_pct, NULL AS forward_pnl, NULL AS mfe_time_mins"
        )

        columns = (
            "id, news_id, suggested_action, sentiment_score, reasoning, "
            "market_category, target_asset, created_at, "
            "entry_price, exit_price, max_price, min_price, "
            "is_correct, settled, entry_time, "
            "prediction_type, event_phase, market_confirmation, "
            "expected_horizon, invalidation_condition, "
            "decision_context, "
            f"{outcome_cols}"
        )

        params: List[Any] = [f"-{days} days"]
        sql = (
            f"SELECT {columns} FROM ai_decisions "
            "WHERE settled = 1 "
            "  AND is_correct IN ('WIN', 'LOSS', 'HOLD') "
            "  AND created_at >= datetime('now', 'localtime', ?)"
        )

        if target_asset:
            resolved = _resolve_asset(target_asset)
            sql += " AND target_asset = ?"
            params.append(resolved)

        if suggested_action:
            action_upper = suggested_action.upper()
            if action_upper in _VALID_ACTIONS:
                sql += " AND suggested_action = ?"
                params.append(action_upper)

        if market_category:
            sql += " AND market_category = ?"
            params.append(market_category.upper())

        if prediction_type:
            sql += " AND prediction_type = ?"
            params.append(prediction_type.lower())

        if event_phase:
            sql += " AND event_phase = ?"
            params.append(event_phase.lower())

        if market_confirmation:
            sql += " AND market_confirmation = ?"
            params.append(market_confirmation.lower())

        sql += " ORDER BY id DESC"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)

        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        return [dict(r) for r in rows]

    def _get_signal_by_id(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single signal by its ID (for evaluate_signal_context)."""
        has_outcome = self._has_outcome_metrics()
        outcome_cols = (
            "mfe_pct, mae_pct, forward_pnl, mfe_time_mins"
            if has_outcome
            else "NULL AS mfe_pct, NULL AS mae_pct, NULL AS forward_pnl, NULL AS mfe_time_mins"
        )
        columns = (
            "id, news_id, suggested_action, sentiment_score, reasoning, "
            "market_category, target_asset, created_at, "
            "entry_price, exit_price, max_price, min_price, "
            "is_correct, settled, entry_time, "
            "prediction_type, event_phase, market_confirmation, "
            "expected_horizon, invalidation_condition, "
            "decision_context, "
            f"{outcome_cols}"
        )
        conn = self._connect()
        try:
            row = conn.execute(
                f"SELECT {columns} FROM ai_decisions WHERE id = ?", (signal_id,)
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Internal — filtering by regime (Python-side, JSON parsing)
    # ------------------------------------------------------------------

    def _filter_by_regime(
        self,
        rows: List[Dict[str, Any]],
        target_regime: str,
        target_asset: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Keep only rows whose decision_context trend matches target_regime.

        Args:
            rows:          Settled signal dicts.
            target_regime: "Bull", "Bear", "Ranging", or "unknown".
            target_asset:  If set, only check regime for this asset.
        """
        kept: List[Dict[str, Any]] = []
        for r in rows:
            ctx = r.get("decision_context", "") or ""
            asset = target_asset or r.get("target_asset", "")
            trend = _extract_trend(ctx, asset)
            regime = _classify_regime(trend)
            if regime == target_regime:
                kept.append(r)
        return kept

    def _tag_regimes(
        self,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Attach a 'regime' key to each row by parsing its decision_context."""
        for r in rows:
            ctx = r.get("decision_context", "") or ""
            asset = r.get("target_asset", "")
            trend = _extract_trend(ctx, asset)
            r["_regime"] = _classify_regime(trend)
        return rows

    # ------------------------------------------------------------------
    # Internal — statistics computation
    # ------------------------------------------------------------------

    def _compute_stats(
        self,
        rows: List[Dict[str, Any]],
        description: str = "",
        filters: Optional[Dict[str, Any]] = None,
    ) -> StatsResult:
        """Compute aggregate statistics from a list of settled signal dicts.

        This is the core stats engine.  All high-level methods delegate here.
        """
        result = StatsResult(
            query_description=description,
            filters_applied=filters or {},
        )

        if not rows:
            result.small_sample_warning = True
            return result

        # ── Verdict counts ──
        wins = sum(1 for r in rows if r.get("is_correct") == "WIN")
        losses = sum(1 for r in rows if r.get("is_correct") == "LOSS")
        holds = sum(1 for r in rows if r.get("is_correct") == "HOLD")
        decided = wins + losses  # non-HOLD

        result.sample_size = len(rows)
        result.win_count = wins
        result.loss_count = losses
        result.hold_count = holds
        result.win_rate = wins / decided if decided > 0 else 0.0
        result.small_sample_warning = decided < _MIN_SAMPLE

        # ── Win-rate CI (Wilson, on WIN / (WIN+LOSS)) ──
        if decided > 0:
            lo, hi = wilson_ci(wins, decided)
            result.win_rate_ci_lower = lo
            result.win_rate_ci_upper = hi

        # ── PnL metrics ──
        # Prefer forward_pnl column; fall back to computing from entry/exit
        pnl_values: List[float] = []
        mfe_values: List[float] = []
        mae_values: List[float] = []

        for r in rows:
            action = (r.get("suggested_action") or "").upper()
            entry = r.get("entry_price")
            exit_p = r.get("exit_price")

            # MFE / MAE from dedicated columns (Phase 0.5)
            mfe = r.get("mfe_pct")
            mae = r.get("mae_pct")
            fwd_pnl = r.get("forward_pnl")

            if mfe is not None:
                mfe_values.append(float(mfe))
            elif entry is not None and entry > 0:
                # Fallback: compute MFE from extreme price
                # BUY: favourable = price up   → MFE from max_price
                # SELL: favourable = price down → MFE from min_price
                extreme = r.get("max_price") if action == "BUY" else r.get("min_price")
                if extreme is not None:
                    if action == "BUY":
                        mfe_values.append((extreme - entry) / entry * 100)
                    elif action == "SELL":
                        mfe_values.append((entry - extreme) / entry * 100)

            if mae is not None:
                mae_values.append(float(mae))
            elif entry is not None and entry > 0:
                # Fallback: compute MAE from extreme price
                # BUY: adverse = price down  → MAE = (entry - min) / entry
                # SELL: adverse = price up   → MAE = (entry - max) / entry (matches forward_tracker)
                if action == "BUY":
                    mn = r.get("min_price")
                    if mn is not None:
                        mae_values.append((entry - mn) / entry * 100)
                elif action == "SELL":
                    mx = r.get("max_price")
                    if mx is not None:
                        mae_values.append((entry - mx) / entry * 100)

            if fwd_pnl is not None:
                pnl_values.append(float(fwd_pnl))
            elif entry is not None and exit_p is not None and entry > 0:
                # Fallback: compute from entry/exit
                if action == "BUY":
                    pnl_values.append((exit_p - entry) / entry * 100)
                elif action == "SELL":
                    pnl_values.append((entry - exit_p) / entry * 100)
                else:
                    pnl_values.append(0.0)

        if pnl_values:
            result.avg_pnl = round(sum(pnl_values) / len(pnl_values), 6)
            result.total_pnl = round(sum(pnl_values), 6)
            if len(pnl_values) >= 2:
                variance = sum((v - result.avg_pnl) ** 2 for v in pnl_values) / (len(pnl_values) - 1)
                result.std_pnl = round(math.sqrt(variance), 6)
                lo, hi = mean_ci(pnl_values)
                result.pnl_ci_lower = lo
                result.pnl_ci_upper = hi

        if mfe_values:
            result.avg_mfe = round(sum(mfe_values) / len(mfe_values), 6)
        if mae_values:
            result.avg_mae = round(sum(mae_values) / len(mae_values), 6)

        return result

    # ------------------------------------------------------------------
    # Public API — Q1: 某类新闻历史是否有效？
    # ------------------------------------------------------------------

    def evaluate_news_category(
        self,
        market_category: str,
        target_asset: Optional[str] = None,
        suggested_action: Optional[str] = None,
        days: int = 90,
    ) -> StatsResult:
        """Evaluate historical performance of signals from a news category.

        Example::

            result = pq.evaluate_news_category("CRYPTO", asset="BTC")
            # → "Crypto news on BTC: n=52, WR=65%, avg PnL=+0.8%"

        Args:
            market_category:  "CRYPTO", "GOLD", "OIL", "MACRO", "OTHER"
            target_asset:     Narrow to specific asset (e.g. "BTC", "XAU")
            suggested_action: "BUY", "SELL", or None (all actions)
            days:             Look-back window (default 90)
        """
        cat = market_category.upper()
        filters: Dict[str, Any] = {"market_category": cat, "days": days}
        if target_asset:
            filters["target_asset"] = target_asset.upper()
        if suggested_action:
            filters["suggested_action"] = suggested_action.upper()

        rows = self._fetch_settled(
            days=days,
            target_asset=target_asset,
            suggested_action=suggested_action,
            market_category=cat,
        )

        asset_str = f" on {target_asset.upper()}" if target_asset else ""
        action_str = f" {suggested_action}" if suggested_action else ""
        desc = f"{cat} news{action_str}{asset_str} (past {days}d)"

        result = self._compute_stats(rows, description=desc, filters=filters)

        # ── Also break down by asset if no specific asset was requested ──
        if not target_asset and rows:
            result.by_asset = {}
            asset_groups: Dict[str, List[Dict[str, Any]]] = {}
            for r in rows:
                a = r.get("target_asset", "NONE")
                asset_groups.setdefault(a, []).append(r)
            for a, group in sorted(asset_groups.items()):
                result.by_asset[a] = self._compute_stats(
                    group,
                    description=f"{cat} → {a} (past {days}d)",
                    filters={"market_category": cat, "target_asset": a, "days": days},
                )

        return result

    # ------------------------------------------------------------------
    # Public API — Q2: 某 prediction_type 在不同 regime 下胜率？
    # ------------------------------------------------------------------

    def evaluate_prediction_by_regime(
        self,
        prediction_type: str,
        regime: str = "Bull",
        target_asset: Optional[str] = None,
        days: int = 90,
    ) -> StatsResult:
        """Evaluate win rate of a prediction_type under a specific market regime.

        Example::

            result = pq.evaluate_prediction_by_regime("continuation", regime="Bull", asset="BTC")
            # → "Continuation calls during Bull regime on BTC: n=18, WR=72% [小样本⚠]"

        The 'regime' is extracted from decision_context JSON (trend field).
        Values: "Bull", "Bear", "Ranging", "unknown"

        Args:
            prediction_type: "reversal", "continuation", "breakout"
            regime:          "Bull", "Bear", "Ranging", "unknown"
            target_asset:    Narrow to specific asset.
            days:            Look-back window.
        """
        pt = prediction_type.lower()
        if pt not in _VALID_PREDICTION_TYPES:
            raise ValueError(
                f"Invalid prediction_type '{prediction_type}'. "
                f"Must be one of {_VALID_PREDICTION_TYPES}"
            )

        filters: Dict[str, Any] = {
            "prediction_type": pt,
            "regime": regime,
            "days": days,
        }
        if target_asset:
            filters["target_asset"] = target_asset.upper()

        # Fetch with SQL-level filters (prediction_type, asset) —
        # regime filtering happens in Python because it lives inside JSON.
        rows = self._fetch_settled(
            days=days,
            target_asset=target_asset,
            prediction_type=pt,
        )

        # Python-side regime filter
        regime_rows = self._filter_by_regime(rows, regime, target_asset=target_asset)

        asset_str = f" on {target_asset.upper()}" if target_asset else " (all assets)"
        desc = f"prediction_type={pt} × regime={regime}{asset_str} (past {days}d)"

        return self._compute_stats(regime_rows, description=desc, filters=filters)

    def evaluate_prediction_type(
        self,
        prediction_type: str,
        target_asset: Optional[str] = None,
        suggested_action: Optional[str] = None,
        days: int = 90,
    ) -> StatsResult:
        """Evaluate overall performance of a prediction_type (any regime).

        Example::

            result = pq.evaluate_prediction_type("continuation", asset="BTC")
            print(result.summary())
        """
        pt = prediction_type.lower()
        if pt not in _VALID_PREDICTION_TYPES:
            raise ValueError(
                f"Invalid prediction_type '{prediction_type}'. "
                f"Must be one of {_VALID_PREDICTION_TYPES}"
            )

        rows = self._fetch_settled(
            days=days,
            target_asset=target_asset,
            suggested_action=suggested_action,
            prediction_type=pt,
        )

        asset_str = f" on {target_asset.upper()}" if target_asset else ""
        action_str = f" {suggested_action}" if suggested_action else ""
        desc = f"prediction_type={pt}{action_str}{asset_str} (past {days}d)"

        return self._compute_stats(rows, description=desc, filters={
            "prediction_type": pt,
            "target_asset": target_asset,
            "days": days,
        })

    # ------------------------------------------------------------------
    # Public API — Q3: 当前信号是否处于历史高胜率环境？
    # ------------------------------------------------------------------

    def evaluate_signal_context(
        self,
        signal_id: int,
        days: int = 90,
    ) -> Dict[str, Any]:
        """Evaluate a specific signal against its historical peer group.

        Answers: "For signals like this one (same prediction_type,
        target_asset, action, regime), what has been the historical outcome?"

        Returns a dict with:
          - signal:           The queried signal's key fields
          - peer_stats:       StatsResult for matching historical signals
          - baseline_stats:   StatsResult for all same-asset signals (reference)
          - assessment:       Human-readable verdict + recommendation
          - signal_regime:    Extracted regime of the queried signal

        Example::

            assessment = pq.evaluate_signal_context(signal_id=1234)
            print(assessment["assessment"])
            # → "Historical peer group: continuation BUY on BTC in Bull regime
            #    → WR=67% (n=24), avg PnL=+0.82%.  Current environment is high-confidence (above baseline WR=58%)."
        """
        signal = self._get_signal_by_id(signal_id)
        if not signal:
            return {
                "error": f"Signal #{signal_id} not found",
                "signal_id": signal_id,
            }

        # ── Extract signal dimensions ──
        action = (signal.get("suggested_action") or "").upper()
        asset = (signal.get("target_asset") or "").upper()
        pt = (signal.get("prediction_type") or "continuation").lower()
        ctx_json = signal.get("decision_context", "") or ""
        trend = _extract_trend(ctx_json, asset)
        regime = _classify_regime(trend)
        score = signal.get("sentiment_score")
        market_cat = (signal.get("market_category") or "").upper()

        # ── Peer group: same prediction_type + asset + action + regime ──
        peer_rows_raw = self._fetch_settled(
            days=days,
            target_asset=asset,
            suggested_action=action,
            prediction_type=pt,
        )
        # Exclude the signal itself
        peer_rows_raw = [r for r in peer_rows_raw if r["id"] != signal_id]
        # Filter by same regime
        peer_rows = self._filter_by_regime(peer_rows_raw, regime, target_asset=asset)

        peer_stats = self._compute_stats(
            peer_rows,
            description=f"Peers: {pt} {action} on {asset} in {regime} regime (past {days}d)",
            filters={
                "prediction_type": pt,
                "suggested_action": action,
                "target_asset": asset,
                "regime": regime,
                "days": days,
            },
        )

        # ── Baseline: all same-asset signals (any prediction_type, any regime) ──
        baseline_rows = self._fetch_settled(
            days=days,
            target_asset=asset,
            suggested_action=action,
        )
        baseline_rows = [r for r in baseline_rows if r["id"] != signal_id]
        baseline_stats = self._compute_stats(
            baseline_rows,
            description=f"Baseline: all {action} on {asset} (past {days}d)",
            filters={"suggested_action": action, "target_asset": asset, "days": days},
        )

        # ── Also break peers down by sub-regime if we have granular trend ──
        regime_breakdown: Dict[str, StatsResult] = {}
        tagged = self._tag_regimes(peer_rows_raw)
        by_regime: Dict[str, List[Dict[str, Any]]] = {}
        for r in tagged:
            reg = r.get("_regime", "unknown")
            by_regime.setdefault(reg, []).append(r)
        for reg, group in sorted(by_regime.items()):
            regime_breakdown[reg] = self._compute_stats(
                group,
                description=f"Peers: {pt} {action} on {asset} × regime={reg}",
            )

        # ── Assessment ──
        assessment = _build_assessment(signal, peer_stats, baseline_stats, regime)

        return {
            "signal": {
                "id": signal_id,
                "action": action,
                "target_asset": asset,
                "prediction_type": pt,
                "market_category": market_cat,
                "sentiment_score": round(score, 2) if score is not None else None,
                "regime": regime,
                "regime_detail": trend,
                "created_at": signal.get("created_at"),
            },
            "signal_regime": regime,
            "regime_detail": trend,
            "peer_stats": peer_stats.to_dict(),
            "baseline_stats": baseline_stats.to_dict(),
            "regime_breakdown": {k: v.to_dict() for k, v in regime_breakdown.items()},
            "assessment": assessment,
        }

    # ------------------------------------------------------------------
    # Public API — Generic cross-section
    # ------------------------------------------------------------------

    def cross_section(
        self,
        days: int = 90,
        target_asset: Optional[str] = None,
        suggested_action: Optional[str] = None,
        market_category: Optional[str] = None,
        prediction_type: Optional[str] = None,
        event_phase: Optional[str] = None,
        market_confirmation: Optional[str] = None,
        regime: Optional[str] = None,
    ) -> StatsResult:
        """Generic multi-dimensional performance query.

        Combine any set of filters.  Regime filtering is Python-side (from JSON);
        all other filters are applied in SQL.

        Example::

            result = pq.cross_section(
                target_asset="BTC",
                prediction_type="continuation",
                regime="Bull",
            )
        """
        rows = self._fetch_settled(
            days=days,
            target_asset=target_asset,
            suggested_action=suggested_action,
            market_category=market_category,
            prediction_type=prediction_type,
            event_phase=event_phase,
            market_confirmation=market_confirmation,
        )

        if regime:
            rows = self._filter_by_regime(rows, regime, target_asset=target_asset)

        filters: Dict[str, Any] = {"days": days}
        for k, v in [
            ("target_asset", target_asset),
            ("suggested_action", suggested_action),
            ("market_category", market_category),
            ("prediction_type", prediction_type),
            ("event_phase", event_phase),
            ("market_confirmation", market_confirmation),
            ("regime", regime),
        ]:
            if v is not None:
                filters[k] = v

        parts = [f"{k}={v}" for k, v in filters.items() if k != "days"]
        desc = " × ".join(parts) + f" (past {days}d)" if parts else f"All settled (past {days}d)"

        return self._compute_stats(rows, description=desc, filters=filters)

    # ------------------------------------------------------------------
    # Public API — Regime landscape
    # ------------------------------------------------------------------

    def regime_landscape(
        self,
        prediction_type: str = "continuation",
        target_asset: Optional[str] = None,
        days: int = 90,
    ) -> Dict[str, StatsResult]:
        """Break down a prediction_type's performance across all regimes.

        Returns a dict mapping regime → StatsResult.

        Example::

            landscape = pq.regime_landscape("continuation", asset="BTC")
            for regime, stats in landscape.items():
                print(f"{regime}: {stats.summary()}")
        """
        rows = self._fetch_settled(
            days=days,
            target_asset=target_asset,
            prediction_type=prediction_type,
        )
        tagged = self._tag_regimes(rows)
        by_regime: Dict[str, List[Dict[str, Any]]] = {}
        for r in tagged:
            reg = r.get("_regime", "unknown")
            by_regime.setdefault(reg, []).append(r)

        result: Dict[str, StatsResult] = {}
        asset_str = f" on {target_asset}" if target_asset else ""
        for reg in ["Bull", "Bear", "Ranging", "unknown"]:
            group = by_regime.get(reg, [])
            result[reg] = self._compute_stats(
                group,
                description=f"{prediction_type} in {reg} regime{asset_str} (past {days}d)",
                filters={
                    "prediction_type": prediction_type,
                    "regime": reg,
                    "target_asset": target_asset,
                    "days": days,
                },
            )
        return result

    # ------------------------------------------------------------------
    # Quick health-check
    # ------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Return overall system health metrics.

        Includes: total signals, settled count, global win rate,
        data freshness, and column availability.
        """
        conn = self._connect()
        try:
            total = conn.execute("SELECT COUNT(*) FROM ai_decisions").fetchone()[0]
            settled = conn.execute(
                "SELECT COUNT(*) FROM ai_decisions WHERE settled = 1"
            ).fetchone()[0]
            wins = conn.execute(
                "SELECT COUNT(*) FROM ai_decisions WHERE settled = 1 AND is_correct = 'WIN'"
            ).fetchone()[0]
            losses = conn.execute(
                "SELECT COUNT(*) FROM ai_decisions WHERE settled = 1 AND is_correct = 'LOSS'"
            ).fetchone()[0]
            holds = conn.execute(
                "SELECT COUNT(*) FROM ai_decisions WHERE settled = 1 AND is_correct = 'HOLD'"
            ).fetchone()[0]
            latest = conn.execute(
                "SELECT MAX(created_at) FROM ai_decisions"
            ).fetchone()[0]
            latest_settled = conn.execute(
                "SELECT MAX(created_at) FROM ai_decisions WHERE settled = 1"
            ).fetchone()[0]
        finally:
            conn.close()

        decided = wins + losses
        wr = wins / decided if decided > 0 else 0.0
        wr_lo, wr_hi = wilson_ci(wins, decided)

        has_ctx = False
        if settled > 0:
            conn2 = self._connect()
            try:
                with_ctx = conn2.execute(
                    "SELECT COUNT(*) FROM ai_decisions "
                    "WHERE settled = 1 AND decision_context != '{}' AND decision_context != ''"
                ).fetchone()[0]
                has_ctx = with_ctx > 0
            finally:
                conn2.close()

        return {
            "total_signals": total,
            "settled_signals": settled,
            "wins": wins,
            "losses": losses,
            "holds": holds,
            "global_win_rate": round(wr, 4),
            "global_win_rate_ci_95": [wr_lo, wr_hi],
            "latest_signal": latest,
            "latest_settled": latest_settled,
            "has_outcome_metrics": self._has_outcome_metrics(),
            "has_decision_context": has_ctx,
            "reliable": decided >= _MIN_SAMPLE,
        }


# ---------------------------------------------------------------------------
# Assessment builder (used by evaluate_signal_context)
# ---------------------------------------------------------------------------

def _build_assessment(
    signal: Dict[str, Any],
    peer: StatsResult,
    baseline: StatsResult,
    regime: str,
) -> str:
    """Build a human-readable assessment string for evaluate_signal_context."""
    action = (signal.get("suggested_action") or "").upper()
    asset = (signal.get("target_asset") or "").upper()
    pt = (signal.get("prediction_type") or "continuation").lower()

    lines = [f"Signal #{signal.get('id')} — {pt} {action} on {asset} in {regime} regime"]

    # Peer group
    if peer.sample_size == 0:
        lines.append(
            f"  Peer group: NO historical matches found for {pt} {action} on {asset} in {regime}."
        )
    elif peer.small_sample_warning:
        lines.append(
            f"  Peer group: n={peer.sample_size} (decided={peer.win_count + peer.loss_count}) "
            f"— ⚠ SMALL SAMPLE, statistics are indicative only."
        )
        if peer.win_count + peer.loss_count > 0:
            lines.append(
                f"  Observed WR={peer.win_rate:.0%}, "
                f"CI=[{peer.win_rate_ci_lower:.0%}–{peer.win_rate_ci_upper:.0%}], "
                f"avg PnL={peer.avg_pnl:+.2f}%" if peer.avg_pnl is not None
                else f"  Observed WR={peer.win_rate:.0%}, PnL data unavailable"
            )
    else:
        lines.append(
            f"  Peer group: n={peer.sample_size}, WR={peer.win_rate:.0%} "
            f"(CI=[{peer.win_rate_ci_lower:.0%}–{peer.win_rate_ci_upper:.0%}])"
        )
        if peer.avg_pnl is not None and peer.avg_mfe is not None:
            lines.append(
                f"    avg PnL={peer.avg_pnl:+.2f}%, "
                f"avg MFE={peer.avg_mfe:+.2f}%, avg MAE={peer.avg_mae:+.2f}%"
            )

    # Baseline comparison
    if baseline.sample_size > 0 and (baseline.win_count + baseline.loss_count) > 0:
        lines.append(
            f"  Baseline ({action} on {asset}, all types/regimes): "
            f"WR={baseline.win_rate:.0%} (n={baseline.sample_size})"
        )

        # Peer vs baseline judgement
        if peer.win_count + peer.loss_count >= 5 and not peer.small_sample_warning:
            if peer.win_rate > baseline.win_rate + 0.10:
                lines.append(
                    f"  ✅ Current setup ({pt} in {regime}) outperforms baseline by "
                    f">10pp — HIGH CONFIDENCE environment."
                )
            elif peer.win_rate > baseline.win_rate:
                lines.append(
                    f"  ✓ Current setup slightly above baseline "
                    f"(+{(peer.win_rate - baseline.win_rate):.0%}pp) — MODERATE confidence."
                )
            elif peer.win_rate >= baseline.win_rate - 0.05:
                lines.append(
                    f"  ≈ Current setup in line with baseline — NEUTRAL."
                )
            else:
                lines.append(
                    f"  ⚠ Current setup underperforms baseline "
                    f"({(peer.win_rate - baseline.win_rate):.0%}pp) — LOW CONFIDENCE environment."
                )
    else:
        lines.append("  Baseline: insufficient data for comparison.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

# Singleton instance for quick access
_default_query: Optional[PerformanceQuery] = None


def get_query(db_path: Optional[str] = None) -> PerformanceQuery:
    """Return a (cached) PerformanceQuery instance."""
    global _default_query
    if _default_query is None or db_path is not None:
        _default_query = PerformanceQuery(db_path)
    return _default_query


# ---------------------------------------------------------------------------
# Smoke test (run directly)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pq = PerformanceQuery()

    print("=" * 60)
    print("Phase 1 — Performance Intelligence Layer — Smoke Test")
    print("=" * 60)

    # Health check
    hc = pq.health_check()
    print("\n── Health Check ──")
    for k, v in hc.items():
        print(f"  {k}: {v}")

    # Q1: News category
    print("\n── Q1: evaluate_news_category('CRYPTO') ──")
    r1 = pq.evaluate_news_category("CRYPTO", days=90)
    print(f"  {r1.summary()}")
    if r1.by_asset:
        for a, s in r1.by_asset.items():
            print(f"    {a}: {s.summary()}")

    print("\n── Q1: evaluate_news_category('GOLD') ──")
    r2 = pq.evaluate_news_category("GOLD", days=90)
    print(f"  {r2.summary()}")

    # Q2: Prediction type (note: currently all "continuation", so cross-section is limited)
    print("\n── Q2: evaluate_prediction_type('continuation', asset='BTC') ──")
    r3 = pq.evaluate_prediction_type("continuation", target_asset="BTC", days=90)
    print(f"  {r3.summary()}")

    # Q3: Signal context (use the latest settled signal)
    conn = sqlite3.connect(pq.db_path)
    latest_id = conn.execute(
        "SELECT MAX(id) FROM ai_decisions WHERE settled = 1"
    ).fetchone()[0]
    conn.close()

    if latest_id:
        print(f"\n── Q3: evaluate_signal_context(id={latest_id}) ──")
        ctx = pq.evaluate_signal_context(latest_id)
        print(f"  Signal regime: {ctx.get('signal_regime', '?')} "
              f"(detail: {ctx.get('regime_detail', '?')})")
        print(f"  Peer stats: {pq._compute_stats.__name__}")  # placeholder
        if "assessment" in ctx:
            print(f"  Assessment:\n{ctx['assessment']}")

    # Regime landscape
    print("\n── Regime Landscape: continuation on BTC ──")
    landscape = pq.regime_landscape("continuation", target_asset="BTC", days=90)
    for reg, stats in landscape.items():
        print(f"  {reg:10s}: {stats.summary()}")

    print("\n" + "=" * 60)
    print("Smoke test complete.")
    print("=" * 60)
