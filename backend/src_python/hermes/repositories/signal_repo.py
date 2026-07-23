"""
Hermes Repository — SignalRepository

Wraps the `ai_decisions` table.  Read-only aggregate queries — never writes.
Supports forward-tracking analytics (is_correct, entry_price, exit_price,
forward_pnl, settled) for the performance feedback loop.

All SQL is contained within this file; no tool function ever touches a cursor
or execute() call.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .base import BaseRepository, assert_readonly_sql, _now


class SignalRepository(BaseRepository):
    """Read-only access to ai_decisions (Kimi K3 signals + forward tracking).

    Usage::

        repo = SignalRepository()
        signals = repo.find_signals(asset="BTC", hours=24, limit=20)
        stats  = repo.get_performance_stats(asset="BTC", days=7)
        consensus = repo.get_consensus_breakdown(asset="BTC", hours=24)
    """

    # -- recognised actions -------------------------------------------------
    _VALID_ACTIONS = {"BUY", "SELL", "HOLD"}

    # -- recognised market categories ---------------------------------------
    _VALID_CATEGORIES = {"CRYPTO", "GOLD", "OIL", "MACRO", "OTHER"}

    # -- recognised assets (used for target_asset filtering) -----------------
    # Maps user-facing names → DB target_asset values
    _ASSET_MAP: Dict[str, str] = {
        "BTC":     "BTC",
        "ETH":     "ETH",
        "GOLD":    "XAU",
        "XAU":     "XAU",
        "XAUUSD":  "XAU",
        "WTI":     "WTI",
        "OIL":     "WTI",
        "原油":     "WTI",
        "比特币":   "BTC",
        "以太坊":   "ETH",
        "黄金":     "XAU",
    }

    @classmethod
    def _resolve_asset(cls, asset: Optional[str]) -> Optional[str]:
        """Map user-facing asset names to DB target_asset values."""
        if not asset:
            return None
        return cls._ASSET_MAP.get(asset.upper(), asset.upper())

    # ------------------------------------------------------------------
    # Read methods — Signal query
    # ------------------------------------------------------------------

    def find_signals(
        self,
        asset: Optional[str] = None,
        action: Optional[str] = None,
        hours: int = 24,
        limit: int = 20,
        min_score: Optional[float] = None,
        with_forward_tracking: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return recent trading signals with full detail.

        Args:
            asset:         Filter by target_asset (e.g. "BTC", "XAU").
            action:        Filter by suggested_action ("BUY", "SELL", "HOLD").
            hours:         Look-back window.
            limit:         Max rows (capped at 100).
            min_score:     Minimum absolute sentiment score (e.g. 0.3 for conviction signals).
            with_forward_tracking: Include entry_price, exit_price, is_correct, settled.
        """
        limit = min(max(limit, 1), 100)
        params: List[Any] = [f"-{hours} hours"]

        columns = (
            "id, news_id, suggested_action, sentiment_score, reasoning, "
            "reasoning_path, market_category, target_asset, vip_tag, "
            "created_at, status"
        )
        if with_forward_tracking:
            columns += (
                ", entry_price, exit_price, max_price, min_price, "
                "is_correct, settled, entry_time, mfe_pct, mae_pct, "
                "forward_pnl, mfe_time_mins"
            )

        sql = f"SELECT {columns} FROM ai_decisions WHERE created_at >= datetime('now', 'localtime', ?)"

        if asset:
            resolved = self._resolve_asset(asset)
            sql += " AND target_asset = ?"
            params.append(resolved)

        if action:
            action_upper = action.upper()
            if action_upper not in self._VALID_ACTIONS:
                raise ValueError(
                    f"Invalid action '{action}'. Must be one of {self._VALID_ACTIONS}"
                )
            sql += " AND suggested_action = ?"
            params.append(action_upper)

        if min_score is not None:
            sql += " AND ABS(sentiment_score) >= ?"
            params.append(min_score)

        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        results = self._rows_to_dicts(rows)
        # Parse embedded JSON fields
        for r in results:
            if "extra_models_consensus" in r:
                r["extra_models_consensus"] = self._safe_json_parse(
                    r.get("extra_models_consensus", ""), {}
                )
        return results

    def get_latest_signal(self, asset: str) -> Optional[Dict[str, Any]]:
        """Return the single most recent signal for an asset (or None)."""
        results = self.find_signals(asset=asset, limit=1)
        return results[0] if results else None

    # ------------------------------------------------------------------
    # Read methods — Performance analytics (forward-tracking feedback)
    # ------------------------------------------------------------------

    def get_performance_stats(
        self,
        asset: Optional[str] = None,
        action: Optional[str] = None,
        days: int = 30,
        require_settled: bool = True,
    ) -> Dict[str, Any]:
        """Compute aggregate performance metrics from forward-tracking data.

        Reads is_correct, forward_pnl, entry_price, exit_price, settled columns.
        Never writes — forward tracker in engine.py is the sole writer.

        Args:
            asset:           Filter by target_asset.
            action:          Filter by suggested_action.
            days:            Look-back window.
            require_settled: If True, only count signals where settled=1.

        Returns:
            Dict with keys: total_signals, settled_count, win_count, loss_count,
            win_rate, avg_forward_pnl, total_pnl, avg_abs_score, score_by_outcome.
        """
        params: List[Any] = [f"-{days} days"]

        # Forward-pnl / outcome metrics — may not exist on older DBs
        pnl_col = self._column_exists("ai_decisions", "forward_pnl")
        mfe_col = self._column_exists("ai_decisions", "mfe_pct")
        mae_col = self._column_exists("ai_decisions", "mae_pct")

        settled_clause = "AND settled = 1" if require_settled else ""

        extra_select: List[str] = []
        if pnl_col:
            extra_select.append(
                "AVG(CASE WHEN settled = 1 THEN forward_pnl ELSE NULL END) AS avg_forward_pnl")
            extra_select.append(
                "SUM(CASE WHEN settled = 1 THEN forward_pnl ELSE 0 END) AS total_pnl")
        if mfe_col:
            extra_select.append(
                "AVG(CASE WHEN settled = 1 THEN mfe_pct ELSE NULL END) AS avg_mfe_pct")
        if mae_col:
            extra_select.append(
                "AVG(CASE WHEN settled = 1 THEN mae_pct ELSE NULL END) AS avg_mae_pct")

        sql = (
            "SELECT "
            "  COUNT(*) AS total_signals, "
            "  SUM(CASE WHEN settled = 1 THEN 1 ELSE 0 END) AS settled_count, "
            "  SUM(CASE WHEN settled = 1 AND is_correct = '1' THEN 1 ELSE 0 END) AS win_count, "
            "  SUM(CASE WHEN settled = 1 AND is_correct = '0' THEN 1 ELSE 0 END) AS loss_count, "
            "  AVG(CASE WHEN settled = 1 THEN ABS(sentiment_score) ELSE NULL END) AS avg_abs_score"
        )
        if extra_select:
            sql += ", " + ", ".join(extra_select)
        sql += (
            " FROM ai_decisions "
            "WHERE created_at >= datetime('now', 'localtime', ?)"
        )

        if asset:
            resolved = self._resolve_asset(asset)
            sql += " AND target_asset = ?"
            params.append(resolved)

        if action:
            action_upper = action.upper()
            if action_upper not in self._VALID_ACTIONS:
                raise ValueError(f"Invalid action '{action}'")
            sql += " AND suggested_action = ?"
            params.append(action_upper)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            row = conn.execute(sql, params).fetchone()
        finally:
            conn.close()

        if not row:
            return {
                "total_signals": 0,
                "settled_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "win_rate": 0.0,
                "avg_abs_score": 0.0,
                "query": {"asset": asset, "action": action, "days": days},
            }

        settled = row["settled_count"] or 0
        wins = row["win_count"] or 0
        result: Dict[str, Any] = {
            "total_signals": row["total_signals"] or 0,
            "settled_count": settled,
            "win_count": wins,
            "loss_count": row["loss_count"] or 0,
            "win_rate": round(wins / settled, 4) if settled > 0 else 0.0,
            "avg_abs_score": round(row["avg_abs_score"] or 0.0, 4),
            "query": {"asset": asset, "action": action, "days": days},
        }
        if pnl_col:
            result["avg_forward_pnl"] = round(row["avg_forward_pnl"] or 0.0, 6)
            result["total_pnl"] = round(row["total_pnl"] or 0.0, 6)
        if mfe_col:
            result["avg_mfe_pct"] = round(row["avg_mfe_pct"] or 0.0, 6)
        if mae_col:
            result["avg_mae_pct"] = round(row["avg_mae_pct"] or 0.0, 6)
        return result

    def get_score_by_outcome(
        self,
        asset: Optional[str] = None,
        days: int = 30,
    ) -> Dict[str, Any]:
        """Break down average |score| by correct vs incorrect outcomes.

        Useful for answering:  "Does a higher score predict better outcomes?"
        """
        params: List[Any] = [f"-{days} days"]
        sql = (
            "SELECT "
            "  is_correct, "
            "  COUNT(*) AS cnt, "
            "  AVG(ABS(sentiment_score)) AS avg_abs_score, "
            "  AVG(sentiment_score) AS avg_score "
            "FROM ai_decisions "
            "WHERE settled = 1 "
            "  AND is_correct IN ('0', '1') "
            "  AND created_at >= datetime('now', 'localtime', ?)"
        )
        if asset:
            resolved = self._resolve_asset(asset)
            sql += " AND target_asset = ?"
            params.append(resolved)
        sql += " GROUP BY is_correct ORDER BY is_correct DESC"

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        result: Dict[str, Any] = {"correct": {}, "incorrect": {}}
        for r in rows:
            bucket = "correct" if r["is_correct"] == "1" else "incorrect"
            result[bucket] = {
                "count": r["cnt"],
                "avg_abs_score": round(r["avg_abs_score"] or 0.0, 4),
                "avg_score": round(r["avg_score"] or 0.0, 4),
            }
        return result

    # ------------------------------------------------------------------
    # Read methods — Market context / aggregation
    # ------------------------------------------------------------------

    def get_consensus_breakdown(
        self,
        asset: Optional[str] = None,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """Return signal distribution by action + avg score for an asset.

        Used by market_tool to paint the 'sentiment landscape'.
        """
        params: List[Any] = [f"-{hours} hours"]
        sql = (
            "SELECT suggested_action, COUNT(*) AS cnt, "
            "AVG(sentiment_score) AS avg_score, "
            "AVG(ABS(sentiment_score)) AS avg_abs_score "
            "FROM ai_decisions "
            "WHERE created_at >= datetime('now', 'localtime', ?)"
        )
        if asset:
            resolved = self._resolve_asset(asset)
            sql += " AND target_asset = ?"
            params.append(resolved)
        sql += " GROUP BY suggested_action"

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        breakdown: Dict[str, Any] = {
            "BUY":  {"count": 0, "avg_score": 0.0, "avg_abs_score": 0.0},
            "SELL": {"count": 0, "avg_score": 0.0, "avg_abs_score": 0.0},
            "HOLD": {"count": 0, "avg_score": 0.0, "avg_abs_score": 0.0},
        }
        total = 0
        for r in rows:
            action = r["suggested_action"]
            if action in breakdown:
                breakdown[action] = {
                    "count": r["cnt"],
                    "avg_score": round(r["avg_score"] or 0.0, 4),
                    "avg_abs_score": round(r["avg_abs_score"] or 0.0, 4),
                }
                total += r["cnt"]

        bullish_pct = round(breakdown["BUY"]["count"] / total * 100, 1) if total > 0 else 0.0
        bearish_pct = round(breakdown["SELL"]["count"] / total * 100, 1) if total > 0 else 0.0

        return {
            "asset": asset or "ALL",
            "hours": hours,
            "total_signals": total,
            "breakdown": breakdown,
            "bullish_pct": bullish_pct,
            "bearish_pct": bearish_pct,
            "net_bias": "bullish" if bullish_pct > bearish_pct else ("bearish" if bearish_pct > bullish_pct else "neutral"),
        }

    def get_recent_vip_signals(self, hours: int = 24, limit: int = 10) -> List[Dict[str, Any]]:
        """Return signals tagged as VIP (matched against watchlist keywords)."""
        limit = min(max(limit, 1), 50)
        params: List[Any] = [f"-{hours} hours", limit]

        sql = (
            "SELECT id, news_id, suggested_action, sentiment_score, reasoning, "
            "target_asset, vip_tag, created_at "
            "FROM ai_decisions "
            "WHERE created_at >= datetime('now', 'localtime', ?) "
            "  AND vip_tag IS NOT NULL AND vip_tag != '' "
            "ORDER BY id DESC LIMIT ?"
        )
        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return self._rows_to_dicts(rows)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _column_exists(self, table: str, column: str) -> bool:
        """Check whether a column exists in a table (safe introspection)."""
        conn = self._connect()
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        finally:
            conn.close()
        return any(r["name"] == column for r in rows)

    # ────────────────────────────────────────────────────────────────
    # Schema migration (called once at startup via BaseRepository)
    # ────────────────────────────────────────────────────────────────

    @classmethod
    def migration_sql(cls) -> List[str]:
        """ai_decisions is created by engine.py — Hermes only reads it.
        Add indices for common Hermes query patterns (optional, for perf)."""
        return [
            "CREATE INDEX IF NOT EXISTS idx_hermes_signal_asset_time "
            "ON ai_decisions(target_asset, created_at);",
            "CREATE INDEX IF NOT EXISTS idx_hermes_signal_settled "
            "ON ai_decisions(settled, is_correct, created_at);",
        ]
