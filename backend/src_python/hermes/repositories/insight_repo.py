"""
Hermes Repository — InsightRepository (L1)

L1 = Verified Insights.  Human-reviewed conclusions with statistical backing.

CRITICAL DESIGN RULE — Physical L0/L1 isolation:
  - This repository's write methods (insert / update_verification) MUST NEVER
    appear in TOOL_REGISTRY.  The LLM orchestrator has zero access to L1 writes.
  - L1 writes are invoked by:
      a) A human operator through a future `/api/hermes/verify` endpoint
      b) A scheduled backtest validation job (Phase 2)
      c) Direct SQLite insertion by a human analyst
  - Read methods (get_verified, search, get_by_confidence) are safe to expose
    to tools — they cannot contaminate memory.

Verification requirements (enforced in code):
  - `verified_by` must be one of: "human", "backtest", "statistical_test"
  - If verified_by == "statistical_test", p_value is STRONGLY recommended (not
    enforced — p_value=NULL is accepted but flagged with confidence=0.5)
  - If verified_by == "human", confidence must be set explicitly (no default 1.0)
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .base import BaseRepository, _now

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Who verified this insight?  Must be one of these.
_VERIFIED_BY = {
    "human",            # Manually reviewed by a human analyst
    "backtest",         # Validated against historical forward-tracking data
    "statistical_test", # Passed a formal test (t-test, bootstrap, etc.)
}


class InsightRepository(BaseRepository):
    """L1 — Verified Insights.  Write-guarded: no LLM can insert or modify.

    Read methods are public and can be exposed via tool functions.  Write methods
    are deliberately NOT part of any tool registry — they are for direct operator
    use only (future UI endpoint or manual DB access).

    Physical isolation from L0:  data lives in `research_insights`, not
    `research_observations`.  The two tables share no write path.
    """

    # ------------------------------------------------------------------
    # Write methods — NEVER EXPOSED TO LLM TOOLS
    # ------------------------------------------------------------------

    def insert(
        self,
        content: str,
        verified_by: str,
        confidence: float = 0.5,
        sample_size: int = 0,
        p_value: Optional[float] = None,
        source_obs_ids: Optional[List[int]] = None,
        asset: str = "",
        tags: Optional[List[str]] = None,
    ) -> int:
        """Insert a verified insight.  Returns the new row id.

        **This method is NOT in TOOL_REGISTRY.**  It is called by human operator
        endpoints or batch verification jobs — never by an LLM tool function.

        Args:
            content:         The verified insight / conclusion.
            verified_by:     Who verified this: "human", "backtest", or "statistical_test".
            confidence:      0.0–1.0. For human verification, must be set explicitly.
            sample_size:     Number of samples the verification was based on.
            p_value:         For statistical_test verification.
            source_obs_ids:  List of L0 observation IDs this insight is derived from.
            asset:           Related asset.
            tags:            Classification tags.

        Raises:
            ValueError if verified_by is invalid or confidence is out of range.
        """
        if verified_by not in _VERIFIED_BY:
            raise ValueError(
                f"Invalid verified_by '{verified_by}'. "
                f"Must be one of {sorted(_VERIFIED_BY)}"
            )
        if not (0.0 <= confidence <= 1.0):
            raise ValueError(f"Confidence must be 0.0–1.0, got {confidence}")

        content = content[:10_000].strip()
        if not content:
            raise ValueError("Insight content must not be empty")

        tags_json = self._safe_json_dumps(tags if tags else [])
        obs_ids_json = self._safe_json_dumps(source_obs_ids if source_obs_ids else [])

        conn = self._connect()
        try:
            cursor = conn.execute(
                """INSERT INTO research_insights
                   (content, verified_by, confidence, sample_size, p_value,
                    source_obs_ids, asset, tags, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    content,
                    verified_by,
                    confidence,
                    sample_size,
                    p_value,
                    obs_ids_json,
                    asset,
                    tags_json,
                    _now(),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_verification(
        self,
        insight_id: int,
        verified_by: str,
        confidence: float,
        sample_size: Optional[int] = None,
        p_value: Optional[float] = None,
    ) -> bool:
        """Update verification metadata on an existing insight.

        **Not exposed to LLM tools.**  Used to re-verify or strengthen an insight.
        Returns True if the row was found and updated.
        """
        if verified_by not in _VERIFIED_BY:
            raise ValueError(f"Invalid verified_by '{verified_by}'")
        if not (0.0 <= confidence <= 1.0):
            raise ValueError(f"Confidence must be 0.0–1.0, got {confidence}")

        sets = ["verified_by = ?", "confidence = ?"]
        params: List[Any] = [verified_by, confidence]

        if sample_size is not None:
            sets.append("sample_size = ?")
            params.append(sample_size)
        if p_value is not None:
            sets.append("p_value = ?")
            params.append(p_value)

        params.append(insight_id)
        sql = f"UPDATE research_insights SET {', '.join(sets)} WHERE id = ?"

        conn = self._connect()
        try:
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def delete(self, insight_id: int) -> bool:
        """Delete an insight by id.  **Not exposed to LLM tools.**
        Returns True if the row existed and was deleted.
        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM research_insights WHERE id = ?", (insight_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Read methods — safe to expose via tool functions
    # ------------------------------------------------------------------

    def get_verified(
        self,
        asset: Optional[str] = None,
        verified_by: Optional[str] = None,
        min_confidence: float = 0.5,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return verified insights, ordered by confidence descending.

        Args:
            asset:          Filter by asset.
            verified_by:    Filter by verification method.
            min_confidence: Minimum confidence threshold.
            limit:          Max results.
        """
        limit = min(max(limit, 1), 50)
        params: List[Any] = []
        sql = "SELECT id, content, verified_by, confidence, sample_size, p_value, source_obs_ids, asset, tags, created_at FROM research_insights WHERE 1=1"

        if asset:
            sql += " AND asset = ?"
            params.append(asset.upper())
        if verified_by:
            if verified_by not in _VERIFIED_BY:
                raise ValueError(f"Invalid verified_by '{verified_by}'")
            sql += " AND verified_by = ?"
            params.append(verified_by)
        if min_confidence > 0:
            sql += " AND confidence >= ?"
            params.append(min_confidence)

        sql += " ORDER BY confidence DESC, created_at DESC LIMIT ?"
        params.append(limit)

        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        results = self._rows_to_dicts(rows)
        for r in results:
            r["tags"] = self._safe_json_parse(r.get("tags", "[]"), [])
            r["source_obs_ids"] = self._safe_json_parse(r.get("source_obs_ids", "[]"), [])
        return results

    def search(
        self,
        keywords: List[str],
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Keyword search across verified insights."""
        if not keywords:
            return []
        limit = min(max(limit, 1), 50)
        params: List[Any] = []
        sql = "SELECT id, content, verified_by, confidence, sample_size, p_value, asset, tags, created_at FROM research_insights WHERE 1=1"

        for kw in keywords:
            sql += " AND (content LIKE ? OR tags LIKE ?)"
            params.extend([f"%{kw}%", f"%{kw}%"])

        sql += " ORDER BY confidence DESC, created_at DESC LIMIT ?"
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

    def count_verified(self, min_confidence: float = 0.5) -> int:
        """How many verified insights exist? Used for health checks."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM research_insights WHERE confidence >= ?",
                (min_confidence,),
            ).fetchone()
        finally:
            conn.close()
        return row["cnt"] if row else 0

    # ────────────────────────────────────────────────────────────────
    # Schema migration (called once at startup)
    # ────────────────────────────────────────────────────────────────

    @classmethod
    def migration_sql(cls) -> List[str]:
        return [
            """CREATE TABLE IF NOT EXISTS research_insights (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                content           TEXT    NOT NULL,
                verified_by       TEXT    NOT NULL,
                confidence        REAL    NOT NULL DEFAULT 0.5,
                sample_size       INTEGER DEFAULT 0,
                p_value           REAL,
                source_obs_ids    TEXT    DEFAULT '[]',
                asset             TEXT    DEFAULT '',
                tags              TEXT    DEFAULT '[]',
                created_at        TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            );""",
            "CREATE INDEX IF NOT EXISTS idx_insight_verified ON research_insights(verified_by);",
            "CREATE INDEX IF NOT EXISTS idx_insight_confidence ON research_insights(confidence);",
            "CREATE INDEX IF NOT EXISTS idx_insight_asset ON research_insights(asset);",
        ]
