"""Smoke tests for src_python/db.py — schema migration + SQL safety gate."""

import sqlite3

import pytest

import db


def test_migrate_idempotent_in_memory():
    conn = sqlite3.connect(":memory:")
    db.migrate(conn)
    db.migrate(conn)  # second run must not raise
    conn.close()


def test_tables_exist(temp_db):
    tables = {
        r[0] for r in temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert "raw_news" in tables
    assert "ai_decisions" in tables


def test_ai_decisions_key_columns(temp_db):
    cols = {r[1] for r in temp_db.execute("PRAGMA table_info(ai_decisions)")}
    expected = {
        "parent_id", "child_count", "aggregation_key", "cluster_size",
        "reasoning_path", "vip_tag",
        "doubao_action", "doubao_reasoning", "extra_models_consensus",
        "entry_price", "exit_price", "max_price", "min_price",
        "max_price_time", "min_price_time",
        "is_correct", "settled", "entry_time",
        "mfe_pct", "mae_pct", "forward_pnl", "mfe_time_mins",
        "prediction_type", "event_phase", "market_confirmation",
        "expected_horizon", "invalidation_condition", "decision_context",
        "event_strength", "direct_catalyst", "timeframe_match",
    }
    assert expected <= cols


def test_raw_news_columns(temp_db):
    cols = {r[1] for r in temp_db.execute("PRAGMA table_info(raw_news)")}
    assert {"is_noise", "relevance_score", "status", "content"} <= cols


def test_migrate_backfills_old_schema():
    """A DB created with the oldest two-table schema gets all columns added."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE raw_news (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " source TEXT NOT NULL, content TEXT NOT NULL,"
        " timestamp TEXT NOT NULL DEFAULT (datetime('now')),"
        " status TEXT NOT NULL DEFAULT 'PENDING'"
        " CHECK (status IN ('PENDING','PROCESSING','DONE','FAILED')))"
    )
    conn.execute(
        "CREATE TABLE ai_decisions (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " news_id INTEGER NOT NULL, sentiment_score REAL NOT NULL,"
        " suggested_action TEXT NOT NULL, reasoning TEXT NOT NULL,"
        " created_at TEXT NOT NULL DEFAULT (datetime('now')),"
        " status TEXT NOT NULL DEFAULT 'UNREAD'"
        " CHECK (status IN ('UNREAD','APPROVED','REJECTED','REVIEWED','AUTO_APPROVED')),"
        " market_category TEXT NOT NULL DEFAULT 'OTHER',"
        " target_asset TEXT NOT NULL DEFAULT 'NONE',"
        " FOREIGN KEY (news_id) REFERENCES raw_news(id) ON DELETE CASCADE)"
    )
    db.migrate(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(ai_decisions)")}
    assert "mfe_pct" in cols and "vip_tag" in cols and "parent_id" in cols
    rcols = {r[1] for r in conn.execute("PRAGMA table_info(raw_news)")}
    assert "is_noise" in rcols
    conn.close()


def test_assert_readonly_sql_allows_select():
    sql = "SELECT id, sentiment_score FROM ai_decisions WHERE settled = 0"
    assert db.assert_readonly_sql(sql) == sql


@pytest.mark.parametrize("bad", [
    "INSERT INTO raw_news (source, content) VALUES ('a', 'b')",
    "UPDATE ai_decisions SET settled = 1",
    "DELETE FROM raw_news",
    "DROP TABLE ai_decisions",
    "ALTER TABLE ai_decisions ADD COLUMN x TEXT",
    "SELECT 1; DELETE FROM raw_news",
])
def test_assert_readonly_sql_blocks_writes(bad):
    with pytest.raises(ValueError):
        db.assert_readonly_sql(bad)
