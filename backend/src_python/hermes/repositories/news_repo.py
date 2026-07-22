"""
Hermes Repository — NewsRepository

Wraps the `raw_news` table.  Read-only — never writes to the production
news pipeline.  All SQL is contained within this file; no tool function
ever touches a cursor or execute() call.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .base import BaseRepository, assert_readonly_sql


class NewsRepository(BaseRepository):
    """Read-only access to raw_news (FinancialJuice WS + TreeNews webhook).

    Usage::

        repo = NewsRepository()
        headlines = repo.find_recent(asset="BTC", hours=24, limit=20)
        count = repo.count_recent(hours=1)
    """

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def find_recent(
        self,
        asset: Optional[str] = None,
        hours: int = 24,
        limit: int = 20,
        sources: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return recent news headlines.

        Args:
            asset:   Optional keyword filter on content (e.g. "BTC", "GOLD").
            hours:   Look-back window in hours.
            limit:   Max rows returned (capped at 100).
            sources: Optional list of source names to filter (e.g. ["FinancialJuice", "tree_news"]).
        """
        limit = min(max(limit, 1), 100)
        params: List[Any] = [f"-{hours} hours"]

        sql = (
            "SELECT id, source, content, timestamp, status "
            "FROM raw_news "
            "WHERE timestamp >= datetime('now', 'localtime', ?)"
        )

        if asset:
            sql += " AND content LIKE ?"
            params.append(f"%{asset}%")

        if sources:
            placeholders = ",".join("?" for _ in sources)
            sql += f" AND source IN ({placeholders})"
            params.extend(sources)

        sql += " AND status IN ('DONE', 'PENDING')"
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return self._rows_to_dicts(rows)

    def search_by_keyword(
        self,
        keywords: List[str],
        hours: int = 72,
        limit: int = 30,
    ) -> List[Dict[str, Any]]:
        """Full-text-like search across news content.

        Args:
            keywords: List of search terms (AND logic across terms).
            hours:    Look-back window.
            limit:    Max results (capped at 50).
        """
        limit = min(max(limit, 1), 50)
        if not keywords:
            return []

        params: List[Any] = [f"-{hours} hours"]
        sql = (
            "SELECT id, source, content, timestamp, status "
            "FROM raw_news "
            "WHERE timestamp >= datetime('now', 'localtime', ?)"
        )
        for kw in keywords:
            sql += " AND content LIKE ?"
            params.append(f"%{kw}%")

        sql += " AND status IN ('DONE', 'PENDING')"
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return self._rows_to_dicts(rows)

    def count_recent(self, hours: int = 1, source: Optional[str] = None) -> int:
        """Count how many news items arrived in the last N hours.

        Args:
            hours:  Look-back window.
            source: Optional source filter.
        """
        params: List[Any] = [f"-{hours} hours"]
        sql = (
            "SELECT COUNT(*) AS cnt FROM raw_news "
            "WHERE timestamp >= datetime('now', 'localtime', ?)"
        )
        if source:
            sql += " AND source = ?"
            params.append(source)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            row = conn.execute(sql, params).fetchone()
        finally:
            conn.close()
        return row["cnt"] if row else 0

    def get_headlines_since(
        self,
        since_ts: str,
        asset: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get headlines newer than a specific ISO timestamp.

        Useful for polling: 'what's new since last check?'
        """
        limit = min(max(limit, 1), 100)
        params: List[Any] = [since_ts]

        sql = (
            "SELECT id, source, content, timestamp, status "
            "FROM raw_news WHERE timestamp > ?"
        )
        if asset:
            sql += " AND content LIKE ?"
            params.append(f"%{asset}%")

        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        assert_readonly_sql(sql)
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return self._rows_to_dicts(rows)

    # ────────────────────────────────────────────────────────────────
    # Schema migration (called once at startup via BaseRepository)
    # ────────────────────────────────────────────────────────────────

    @classmethod
    def migration_sql(cls) -> List[str]:
        """raw_news is created by engine.py — Hermes only reads it.
        No new tables or indices needed for this repository."""
        return []
