#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# Trident Agent MVP — SQLite Event Bus Initialization (fresh deployments only)
# ---------------------------------------------------------------------------
# 安全说明：本脚本【不会删除已有数据库】。schema 的单一权威定义在
# backend/src_python/db.py — migrate() 幂等，重复执行只会补齐缺失的列。
# 日常启动无需运行本脚本：engine.py / api_server.py 启动时会自动 migrate。

import os
import sqlite3
import sys

# 将 src_python 加入 sys.path，以便 import config / db
_SRC_PYTHON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src_python")
if _SRC_PYTHON not in sys.path:
    sys.path.insert(0, _SRC_PYTHON)

import db  # noqa: E402


def init_db() -> None:
    conn = db.get_connection()
    try:
        db.migrate(conn)
    finally:
        conn.close()

    print(f"[init_db] Database ready at {db.get_db_path()}")
    print(f"[init_db]   raw_news     — ready for ingestion")
    print(f"[init_db]   ai_decisions — ready for agent output")
    print(f"[init_db]   WAL mode     — enabled (concurrent reads + writes)")
    print(f"[init_db]   (existing data preserved — migrate is idempotent)")


if __name__ == "__main__":
    init_db()
