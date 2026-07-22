"""
Hermes Repository Layer — Base

All database access in the Hermes system flows through Repository classes.
No tool function, agent runtime, or memory component is allowed to write raw SQL
against the production database. This module defines the abstract base contract.

L0/L1 Data Protection:
  - L0 (Observations):  Auto-generated facts.  Repositories allow programmatic writes.
  - L1 (Insights):      Verified conclusions.   Repositories require a `verified_by`
                        field and are NEVER writable by LLM tool functions.

Every Repository accepts a `db_path` at construction time so the same class can
target different databases (production, test fixture, in-memory) without code changes.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Database path resolution
# ---------------------------------------------------------------------------

_TZ_SHANGHAI = timezone(timedelta(hours=8))


def _resolve_db_path(db_path: Optional[str] = None) -> str:
    """Return the absolute path to trident_event_bus.db.

    Priority:
      1. Explicit `db_path` argument
      2. Environment variable TRIDENT_DB_PATH
      3. Default: backend/trident_event_bus.db (relative to this file)
    """
    if db_path:
        return os.path.abspath(db_path)
    env_path = os.getenv("TRIDENT_DB_PATH", "")
    if env_path:
        return os.path.abspath(env_path)
    # Default: backend/trident_event_bus.db
    # This file lives at backend/src_python/hermes/repositories/base.py
    # backend/ is 3 levels up
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    return os.path.join(backend_dir, "trident_event_bus.db")


def _now() -> str:
    """ISO 8601 timestamp in Shanghai timezone."""
    return datetime.now(_TZ_SHANGHAI).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# SQL Injection / Safety gate
# ---------------------------------------------------------------------------

# Whitelisted SQL keywords allowed in Repository SQL.
# Any SQL string that contains INSERT / UPDATE / DELETE outside of explicit
# Repository write methods is rejected.  Tool functions are audited at code-review
# time:  `grep -rn "execute\|executemany" tools/` must return zero hits.

_READ_ONLY_KEYWORDS = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|AS|ON|AND|OR|IN|NOT|NULL|IS|LIKE|BETWEEN|EXISTS)\b",
    re.IGNORECASE,
)


def assert_readonly_sql(sql: str) -> str:
    """Raise ValueError if `sql` contains any write-side-effect keyword.

    Called by every Repository read method as a last-resort safety belt.
    Repository write methods (insert/update/delete) are deliberately NOT
    decorated — they are the *only* places writes may occur.
    """
    upper = sql.upper().strip()
    # Block DDL
    for kw in ("DROP", "ALTER", "CREATE", "TRUNCATE", "ATTACH", "DETACH", "PRAGMA"):
        if upper.startswith(kw) or f" {kw} " in f" {upper} ":
            raise ValueError(f"DDL keyword '{kw}' not allowed in read query: {sql[:120]}")
    # Block DML writes
    for kw in ("INSERT ", "UPDATE ", "DELETE ", "REPLACE "):
        if kw in upper:
            raise ValueError(f"Write keyword '{kw.strip()}' not allowed in read query: {sql[:120]}")
    return sql


# ---------------------------------------------------------------------------
# Abstract Base Repository
# ---------------------------------------------------------------------------

class BaseRepository(ABC):
    """Abstract base for all Hermes repositories.

    Subclasses MUST:
      - Call super().__init__(db_path) in their own __init__
      - Implement at least the read methods required by their contract
      - Use self._connect() / self._connect_async() for all DB access
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = _resolve_db_path(db_path)
        self._label = self.__class__.__name__

    # -- sync connection (for simple reads, migrations, tests) --
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

    # -- Helpers --
    @staticmethod
    def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        return [dict(r) for r in rows]

    @staticmethod
    def _safe_json_parse(text: str, default: Any = None) -> Any:
        """Parse a JSON string safely — never raises."""
        if not text or not text.strip():
            return default if default is not None else {}
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return default if default is not None else {}

    @staticmethod
    def _safe_json_dumps(obj: Any) -> str:
        """Serialize to JSON string safely — never raises."""
        try:
            return json.dumps(obj, ensure_ascii=False)
        except (TypeError, ValueError):
            return "[]" if isinstance(obj, list) else "{}"

    # -- Schema migration (called once at startup) --
    @classmethod
    def migrate_schema(cls, db_path: Optional[str] = None):
        """Run all Hermes schema migrations.  Idempotent — safe to call every startup.

        Individual Repository subclasses contribute their CREATE TABLE / ALTER
        statements via the _migration_sql() classmethod.
        """
        resolved = _resolve_db_path(db_path)
        conn = sqlite3.connect(resolved)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            # Schema version tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    component TEXT PRIMARY KEY,
                    version   INTEGER NOT NULL
                );
            """)
            conn.commit()
        finally:
            conn.close()
