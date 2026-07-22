#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# Trident Agent MVP — SQLite Event Bus Initialization
# ---------------------------------------------------------------------------

import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trident_event_bus.db")


def init_db() -> None:
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_news (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT    NOT NULL,
            content     TEXT    NOT NULL,
            timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
            status      TEXT    NOT NULL DEFAULT 'PENDING'
                CHECK (status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED'))
        );
        """
    )

    conn.execute(
        """
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
            vip_tag            TEXT    DEFAULT '',
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
            FOREIGN KEY (news_id) REFERENCES raw_news(id) ON DELETE CASCADE
        );
        """
    )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_decisions_status ON ai_decisions(status);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_news_status ON raw_news(status);"
    )

    conn.commit()
    conn.close()

    print(f"[init_db] Database initialized at {DB_PATH}")
    print(f"[init_db]   raw_news     — ready for ingestion")
    print(f"[init_db]   ai_decisions — ready for agent output")
    print(f"[init_db]   WAL mode     — enabled (concurrent reads + writes)")


if __name__ == "__main__":
    init_db()
