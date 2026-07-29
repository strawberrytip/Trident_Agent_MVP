"""Pytest bootstrap for Trident backend smoke tests.

- Puts backend/src_python on sys.path so tests can import
  config / db / engine.* / realtime_filter / market_snapshot / api_server.
- Provides `temp_db`: a tmp_path SQLite DB with db.migrate() applied.

All tests are offline: no network, no API keys required.
"""

import os
import sqlite3
import sys

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PYTHON = os.path.join(BACKEND_DIR, "src_python")
if SRC_PYTHON not in sys.path:
    sys.path.insert(0, SRC_PYTHON)

import db  # noqa: E402


@pytest.fixture()
def temp_db(tmp_path):
    """Fresh migrated SQLite DB in a tmp dir. Yields a connection."""
    db_file = tmp_path / "test_event_bus.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA foreign_keys=ON;")
    db.migrate(conn)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
