#!/usr/bin/env python3
"""
Trident Agent MVP — Database Layer
===================================

Single authoritative schema + migration entry point for trident_event_bus.db.

Consolidates what used to live in three places:
  - backend/init_db.py                     (two CREATE TABLE statements)
  - engine.py  :: _ensure_db_exists()      (~20 incremental ALTER TABLEs)
  - api_server.py :: _migrate_schema()     (ALTER TABLE subset)

`migrate(conn)` is idempotent: CREATE TABLE IF NOT EXISTS with the full
latest column set, then a per-column existence check (PRAGMA table_info)
before any ALTER TABLE — no try/except swallowing.
"""

from __future__ import annotations

import sqlite3
from typing import List, Tuple

import config

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_db_path() -> str:
    """Return the event-bus DB path (TRIDENT_DB_PATH override aware)."""
    return config.DB_PATH


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ---------------------------------------------------------------------------
# Schema — latest complete definitions
# ---------------------------------------------------------------------------

_CREATE_RAW_NEWS = """
    CREATE TABLE IF NOT EXISTS raw_news (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        source          TEXT    NOT NULL,
        content         TEXT    NOT NULL,
        timestamp       TEXT    NOT NULL DEFAULT (datetime('now')),
        status          TEXT    NOT NULL DEFAULT 'PENDING'
            CHECK (status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED')),
        is_noise        INTEGER NOT NULL DEFAULT 0,
        relevance_score REAL    NOT NULL DEFAULT 0.0
    );
"""

_CREATE_AI_DECISIONS = """
    CREATE TABLE IF NOT EXISTS ai_decisions (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        news_id           INTEGER NOT NULL,
        sentiment_score   REAL    NOT NULL,
        suggested_action  TEXT    NOT NULL,
        reasoning         TEXT    NOT NULL,
        created_at        TEXT    NOT NULL DEFAULT (datetime('now')),
        status            TEXT    NOT NULL DEFAULT 'UNREAD'
            CHECK (status IN ('UNREAD', 'APPROVED', 'REJECTED', 'REVIEWED', 'AUTO_APPROVED')),
        market_category   TEXT    NOT NULL DEFAULT 'OTHER',
        target_asset      TEXT    NOT NULL DEFAULT 'NONE',
        parent_id         INTEGER DEFAULT NULL,
        child_count       INTEGER DEFAULT 0,
        aggregation_key   TEXT    DEFAULT '',
        cluster_size      INTEGER DEFAULT 1,
        reasoning_path    TEXT    DEFAULT '',
        vip_tag           TEXT    DEFAULT '',
        doubao_action     TEXT    DEFAULT 'HOLD',
        doubao_reasoning  TEXT    DEFAULT '',
        extra_models_consensus TEXT DEFAULT '',
        entry_price       REAL    DEFAULT NULL,
        exit_price        REAL    DEFAULT NULL,
        max_price         REAL    DEFAULT NULL,
        min_price         REAL    DEFAULT NULL,
        max_price_time    INTEGER DEFAULT 0,
        min_price_time    INTEGER DEFAULT 0,
        is_correct        TEXT    DEFAULT '',
        settled           INTEGER DEFAULT 0,
        entry_time        TEXT    DEFAULT '',
        prediction_type        TEXT    DEFAULT 'continuation',
        event_phase            TEXT    DEFAULT 'mid',
        market_confirmation    TEXT    DEFAULT 'unknown',
        expected_horizon       TEXT    DEFAULT '1-3d',
        invalidation_condition TEXT    DEFAULT '',
        decision_context       TEXT    DEFAULT '{}',
        mfe_pct           REAL    DEFAULT NULL,
        mae_pct           REAL    DEFAULT NULL,
        forward_pnl       REAL    DEFAULT NULL,
        mfe_time_mins     REAL    DEFAULT NULL,
        event_strength    TEXT    DEFAULT 'medium',
        direct_catalyst   INTEGER DEFAULT 0,
        timeframe_match   TEXT    DEFAULT 'intraday',
        FOREIGN KEY (news_id) REFERENCES raw_news(id) ON DELETE CASCADE
    );
"""

# Incremental columns for DBs created by older CREATE TABLE versions.
# (name, column definition) — added via ALTER TABLE only when missing.
_RAW_NEWS_COLUMNS: List[Tuple[str, str]] = [
    ("is_noise",        "INTEGER NOT NULL DEFAULT 0"),
    ("relevance_score", "REAL    NOT NULL DEFAULT 0.0"),
]

_AI_DECISIONS_COLUMNS: List[Tuple[str, str]] = [
    ("market_category",  "TEXT    NOT NULL DEFAULT 'OTHER'"),
    ("target_asset",     "TEXT    NOT NULL DEFAULT 'NONE'"),
    ("parent_id",        "INTEGER DEFAULT NULL"),
    ("child_count",      "INTEGER DEFAULT 0"),
    ("aggregation_key",  "TEXT    DEFAULT ''"),
    ("cluster_size",     "INTEGER DEFAULT 1"),
    ("reasoning_path",   "TEXT    DEFAULT ''"),
    ("vip_tag",          "TEXT    DEFAULT ''"),
    ("doubao_action",    "TEXT    DEFAULT 'HOLD'"),
    ("doubao_reasoning", "TEXT    DEFAULT ''"),
    ("extra_models_consensus", "TEXT    DEFAULT ''"),
    ("entry_price",      "REAL    DEFAULT NULL"),
    ("exit_price",       "REAL    DEFAULT NULL"),
    ("max_price",        "REAL    DEFAULT NULL"),
    ("min_price",        "REAL    DEFAULT NULL"),
    ("max_price_time",   "INTEGER DEFAULT 0"),
    ("min_price_time",   "INTEGER DEFAULT 0"),
    ("is_correct",       "TEXT    DEFAULT ''"),
    ("settled",          "INTEGER DEFAULT 0"),
    ("entry_time",       "TEXT    DEFAULT ''"),
    ("prediction_type",        "TEXT    DEFAULT 'continuation'"),
    ("event_phase",            "TEXT    DEFAULT 'mid'"),
    ("market_confirmation",    "TEXT    DEFAULT 'unknown'"),
    ("expected_horizon",       "TEXT    DEFAULT '1-3d'"),
    ("invalidation_condition", "TEXT    DEFAULT ''"),
    ("decision_context",       "TEXT    DEFAULT '{}'"),
    ("mfe_pct",          "REAL    DEFAULT NULL"),
    ("mae_pct",          "REAL    DEFAULT NULL"),
    ("forward_pnl",      "REAL    DEFAULT NULL"),
    ("mfe_time_mins",    "REAL    DEFAULT NULL"),
    ("event_strength",   "TEXT    DEFAULT 'medium'"),
    ("direct_catalyst",  "INTEGER DEFAULT 0"),
    ("timeframe_match",  "TEXT    DEFAULT 'intraday'"),
]

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ai_decisions_status ON ai_decisions(status);",
    "CREATE INDEX IF NOT EXISTS idx_raw_news_status ON raw_news(status);",
    "CREATE INDEX IF NOT EXISTS idx_ai_parent ON ai_decisions(parent_id);",
    "CREATE INDEX IF NOT EXISTS idx_ai_agg_key ON ai_decisions(aggregation_key);",
]


def _ensure_columns(conn: sqlite3.Connection, table: str,
                    columns: List[Tuple[str, str]]) -> None:
    """ALTER TABLE ADD COLUMN for each missing column. Idempotent by design."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for col, col_def in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def};")


def migrate(conn: sqlite3.Connection) -> None:
    """Apply the full schema to `conn`. Safe to call on every startup."""
    conn.execute(_CREATE_RAW_NEWS)
    conn.execute(_CREATE_AI_DECISIONS)

    # Backfill columns on databases created by older schema versions
    _ensure_columns(conn, "raw_news", _RAW_NEWS_COLUMNS)
    _ensure_columns(conn, "ai_decisions", _AI_DECISIONS_COLUMNS)

    for stmt in _INDEXES:
        conn.execute(stmt)

    conn.commit()


# ---------------------------------------------------------------------------
# SQL safety gate (recovered from hermes/repositories/base.py)
# ---------------------------------------------------------------------------

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
