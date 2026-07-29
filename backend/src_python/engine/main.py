"""Bootstrap — DB migration + task orchestration (entry: main())."""

from __future__ import annotations

import asyncio
import os

import db
from config import DB_PATH

from .ai_worker import MODELS, ai_worker
from .forward import forward_tracker
from .ingest import websocket_ingest
from .webhook import _TREE_NEWS_PORT, _tree_news_handler

# ── 代理注入 — 完全由 .env / 环境变量控制 ──
# 本地 Windows: .env 设置 HTTP_PROXY=http://127.0.0.1:10808
# 服务器首尔:  不设代理, 直连即可
_proxy_set = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
if _proxy_set:
    print(f"[MAIN] 代理已配置: {_proxy_set}")
else:
    print("[MAIN] 无代理, 直连模式")


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def _ensure_db_exists() -> None:
    """Create the SQLite event bus if it doesn't exist, or repair a missing-table DB.

    Schema 统一定义在 db.py — 幂等，可安全地在每次启动时调用。
    """
    need_init = not os.path.exists(DB_PATH)

    conn = db.get_connection()
    try:
        db.migrate(conn)
    finally:
        conn.close()

    tag = "CREATED" if need_init else "VERIFIED"
    print(f"[ENGINE] DB {tag}: {DB_PATH}")


# -- main -----------------------------------------------------------------

async def main():
    print("="*50)
    print("[MAIN] Trident Agent MVP - engine starting")
    print("="*50)

    # 打印模型配置
    print(f"[MAIN] ══════════════════════════════════════════════")
    print(f"[MAIN] 模型配置:")
    for m in MODELS:
        key_status = "✅" if m.get("api_key") else "❌"
        json_mode = "Strict" if m.get("json_mode") else "Prompt"
        print(f"[MAIN]   {key_status} {m['label']:12s} | {m['id']:35s} | {json_mode} | Timeout: 45s")
    print(f"[MAIN] ══════════════════════════════════════════════")

    # Auto-migrate DB schema
    _ensure_db_exists()
    loop = asyncio.get_running_loop()

    # Start TCP server for local webhook ingestion (tree_news, telegram, etc.)
    webhook_server = await asyncio.start_server(
        _tree_news_handler, "127.0.0.1", _TREE_NEWS_PORT
    )
    print(f"[MAIN] Webhook server listening on 127.0.0.1:{_TREE_NEWS_PORT}")

    # Create background workers
    tasks = [
        asyncio.create_task(websocket_ingest(loop), name="ws_ingest"),
        asyncio.create_task(ai_worker(loop), name="ai_worker"),
        asyncio.create_task(forward_tracker(), name="forward_tracker"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("[MAIN] Shutting down...")
    finally:
        webhook_server.close()
        await webhook_server.wait_closed()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
