"""
Hermes Repository — ObservationRepository (L0)

L0 = Raw Observations.  Machine-generated, objective facts with source attribution.
These are the ONLY kind of memory that Hermes writes automatically.

Rules encoded in this class:
  1. Write methods (insert / insert_batch) are LOCAL — only tool functions and
     the agent runtime can call them.  They are NEVER exposed as a TOOL to LLMs.
  2. Content is always tagged with source_tool + source_row_id so every observation
     can be traced back to its origin row (raw_news.id or ai_decisions.id).
  3. Deduplication: same source_row_id + observation_type → skipped (idempotent).
  4. Rate limit: max OBS_MAX_PER_SESSION observations per `reset_session` context.
     Caller (tool function) tracks this; the repo exposes `count_in_window` to help.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any, Dict, List, Optional

from .base import BaseRepository, _now

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Valid observation types (prevents misspelled types from polluting memory)
_OBS_TYPES = {
    "news_headline",     # raw_news row summarised
    "signal_action",     # ai_decisions row summarised
    "market_snapshot",   # Aggregated market state at a point in time
    "price_data",        # External price feed snapshot
    "system_event",      # Notable system lifecycle event
}


class ObservationRepository(BaseRepository):
    """L0 — Raw Observations.  Write-protected: no LLM tool can call these methods.

    The repository itself does NOT enforce per-session rate limits — that is
    the caller's responsibility (tool functions).  It DOES enforce:
      - Valid observation_type enum
      - Deduplication by (source_tool, source_row_id, observation_type)
      - content length cap (10 000 chars)

    All writes go to `research_observations` — a table separate from
    `research_insights` (L1) to enforce physical L0/L1 isolation.
    """

    # ------------------------------------------------------------------
    # Write methods (LOCAL USE ONLY — never exposed to LLM)
    # ------------------------------------------------------------------

    def insert(
        self,
        observation_type: str,
        content: str,
        source_tool: str,
        source_row_id: Optional[int] = None,
        asset: str = "",
        tags: Optional[List[str]] = None,
    ) -> Optional[int]:
        """Insert a single raw observation. Returns the new row id, or None if
        a duplicate already exists (same source_tool + source_row_id + observation_type).

        Args:
            observation_type: One of the valid _OBS_TYPES.
            content:          The observation text (capped at 10 000 chars).
            source_tool:      Which tool or component created this (e.g. "query_news").
            source_row_id:    FK-ish: the id of the source row in raw_news or ai_decisions.
            asset:            Related asset (BTC, XAU, WTI, etc.).
            tags:             Optional list of classification tags.

        Raises:
            ValueError if observation_type is invalid.
        """
        if observation_type not in _OBS_TYPES:
            raise ValueError(
                f"Invalid observation_type '{observation_type}'. "
                f"Must be one of {sorted(_OBS_TYPES)}"
            )
        content = content[:10_000].strip()
        if not content:
            return None

        tags_json = self._safe_json_dumps(tags if tags else [])

        conn = self._connect()
        try:
            # Dedup check
            if source_tool and source_row_id is not None:
                dup = conn.execute(
                    "SELECT id FROM research_observations "
                    "WHERE source_tool = ? AND source_row_id = ? AND observation_type = ?",
                    (source_tool, source_row_id, observation_type),
                ).fetchone()
                if dup:
                    return None  # Already recorded — idempotent

            cursor = conn.execute(
                """INSERT INTO research_observations
                   (observation_type, content, source_tool, source_row_id,
                    asset, tags, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    observation_type,
                    content,
                    source_tool,
                    source_row_id,
                    asset,
                    tags_json,
                    _now(),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def insert_batch(
        self,
        observations: List[Dict[str, Any]],
    ) -> int:
        """Insert multiple observations in a single transaction.

        Each dict must have keys: observation_type, content, source_tool.
        Optional keys: source_row_id, asset, tags.

        Returns the number of rows actually inserted (excluding duplicates).
        """
        if not observations:
            return 0

        inserted = 0
        conn = self._connect()
        try:
            for obs in observations:
                obs_type = obs.get("observation_type", "")
                if obs_type not in _OBS_TYPES:
                    continue
                content = (obs.get("content", "") or "")[:10_000].strip()
                if not content:
                    continue
                source_tool = obs.get("source_tool", "")
                source_row_id = obs.get("source_row_id")

                # Dedup
                if source_tool and source_row_id is not None:
                    dup = conn.execute(
                        "SELECT id FROM research_observations "
                        "WHERE source_tool = ? AND source_row_id = ? AND observation_type = ?",
                        (source_tool, source_row_id, obs_type),
                    ).fetchone()
                    if dup:
                        continue

                tags_json = self._safe_json_dumps(obs.get("tags", []))
                conn.execute(
                    """INSERT INTO research_observations
                       (observation_type, content, source_tool, source_row_id,
                        asset, tags, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        obs_type,
                        content,
                        source_tool,
                        obs.get("source_row_id"),
                        obs.get("asset", ""),
                        tags_json,
                        _now(),
                    ),
                )
                inserted += 1
            conn.commit()
        finally:
            conn.close()
        return inserted

    # ------------------------------------------------------------------
    # Read methods (exposed to tool functions for search / recall)
    # ------------------------------------------------------------------

    def search_by_tags(
        self,
        tags: List[str],
        limit: int = 20,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """Find observations tagged with ALL specified tags (AND logic).

        Tags are stored as a JSON array in the DB; this uses LIKE matching
        which is fast enough for the expected volume (< 50k rows) given the
        time-range filter.
        """
        if not tags:
            return []
        limit = min(max(limit, 1), 100)
        params: List[Any] = [f"-{days} days"]

        sql = (
            "SELECT id, observation_type, content, source_tool, source_row_id, "
            "asset, tags, created_at "
            "FROM research_observations "
            "WHERE created_at >= datetime('now', 'localtime', ?)"
        )
        for tag in tags:
            sql += " AND tags LIKE ?"
            params.append(f'%"{tag}"%')
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        results = self._rows_to_dicts(rows)
        for r in results:
            r["tags"] = self._safe_json_parse(r.get("tags", "[]"), [])
        return results

    def get_recent(
        self,
        observation_type: Optional[str] = None,
        asset: Optional[str] = None,
        limit: int = 20,
        hours: int = 72,
    ) -> List[Dict[str, Any]]:
        """Return recent observations, optionally filtered by type and asset."""
        limit = min(max(limit, 1), 100)
        params: List[Any] = [f"-{hours} hours"]
        sql = (
            "SELECT id, observation_type, content, source_tool, source_row_id, "
            "asset, tags, created_at "
            "FROM research_observations "
            "WHERE created_at >= datetime('now', 'localtime', ?)"
        )
        if observation_type:
            if observation_type not in _OBS_TYPES:
                raise ValueError(f"Invalid observation_type '{observation_type}'")
            sql += " AND observation_type = ?"
            params.append(observation_type)
        if asset:
            sql += " AND asset = ?"
            params.append(asset.upper())
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        results = self._rows_to_dicts(rows)
        for r in results:
            r["tags"] = self._safe_json_parse(r.get("tags", "[]"), [])
        return results

    def count_in_window(self, hours: int = 1, source_tool: Optional[str] = None) -> int:
        """Count observations in the last N hours. Used for rate-limiting."""
        params: List[Any] = [f"-{hours} hours"]
        sql = (
            "SELECT COUNT(*) AS cnt FROM research_observations "
            "WHERE created_at >= datetime('now', 'localtime', ?)"
        )
        if source_tool:
            sql += " AND source_tool = ?"
            params.append(source_tool)

        conn = self._connect()
        try:
            row = conn.execute(sql, params).fetchone()
        finally:
            conn.close()
        return row["cnt"] if row else 0

    def get_by_source_row(
        self,
        source_tool: str,
        source_row_id: int,
    ) -> List[Dict[str, Any]]:
        """Look up all observations derived from a specific source row."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT id, observation_type, content, source_tool, source_row_id, "
                "asset, tags, created_at "
                "FROM research_observations "
                "WHERE source_tool = ? AND source_row_id = ? "
                "ORDER BY created_at DESC",
                (source_tool, source_row_id),
            ).fetchall()
        finally:
            conn.close()

        results = self._rows_to_dicts(rows)
        for r in results:
            r["tags"] = self._safe_json_parse(r.get("tags", "[]"), [])
        return results

    # ────────────────────────────────────────────────────────────────
    # Schema migration (called once at startup)
    # ────────────────────────────────────────────────────────────────

    @classmethod
    def migration_sql(cls) -> List[str]:
        return [
            """CREATE TABLE IF NOT EXISTS research_observations (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_type  TEXT    NOT NULL,
                content           TEXT    NOT NULL,
                source_tool       TEXT    NOT NULL,
                source_row_id     INTEGER,
                asset             TEXT    DEFAULT '',
                tags              TEXT    DEFAULT '[]',
                created_at        TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            );""",
            "CREATE INDEX IF NOT EXISTS idx_obs_type ON research_observations(observation_type);",
            "CREATE INDEX IF NOT EXISTS idx_obs_asset ON research_observations(asset);",
            "CREATE INDEX IF NOT EXISTS idx_obs_created ON research_observations(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_obs_dedup ON research_observations(source_tool, source_row_id, observation_type);",
        ]
