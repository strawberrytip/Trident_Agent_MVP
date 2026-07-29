"""Task A — FinancialJuice WebSocket Ingest (Centrifugo protocol)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, List

# 实时新闻过滤器 — ingest 阶段拦截垃圾新闻
from realtime_filter import evaluate_news

from .utils import (
    _clean_html,
    _detect_vip,
    _is_content_junk,
    _now,
    _open_db,
    _ts,
)
from .webhook import _translate_if_english
from .ws_client import (
    _FJ_HEADERS,
    _StdlibWebSocket,
    _extract_centrifugo_config,
)


# FinancialJuice WebSocket — real-time ingest
FJ_WS_URL = "wss://rt.financialjuice.com/connection/websocket"
FJ_ORIGIN = "https://www.financialjuice.com"
FJ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/131.0.0.0 Safari/537.36"
)


def _extract_news_text(msg: dict) -> str | None:
    """
    Walk a Centrifugo publication message and extract news text.

    FinancialJuice structure (from debug output):
      push.pub.data = {"ev":"sendUpdates", "msg":"[{...}, {...}]"}
      where msg is a JSON-stringified array of objects with:
        NewsID, Title (TitleCase!), Description, Tags, PostedShort, etc.

    We try lowercase and TitleCase field names, and parse nested JSON strings.
    """
    parts: List[str] = []

    def _try_parse_json(s: str) -> Any:
        """If s looks like JSON, parse it and return the result."""
        s = s.strip()
        if (s.startswith("[") and s.endswith("]")) or \
           (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except (json.JSONDecodeError, TypeError):
                pass
        return s

    def _dig(d: Any) -> None:
        if isinstance(d, dict):
            # Try both lowercase and TitleCase field names
            for k in ("title", "Title", "headline", "Headline",
                       "body", "Body", "text", "Text",
                       "content", "Content",
                       "summary", "Summary",
                       "description", "Description",
                       "name", "Name", "message", "Message"):
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    parts.append(v.strip())
            # Recurse into nested objects
            for v in d.values():
                _dig(v)
        elif isinstance(d, list):
            for item in d:
                _dig(item)
        elif isinstance(d, str):
            # FinancialJuice nests data inside JSON strings (the "msg" field)
            parsed = _try_parse_json(d)
            if parsed is not d:
                _dig(parsed)

    _dig(msg)
    if not parts:
        return None
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return ". ".join(unique)


# ── WebSocket helpers ────────────────────────────────────────────────────

async def _safe_close(ws: _StdlibWebSocket) -> None:
    """Gracefully close a WebSocket, ignoring any errors."""
    try:
        await ws.close()
    except Exception:
        pass


async def websocket_ingest(loop: asyncio.AbstractEventLoop) -> None:
    """
    Persistent WSS connection to FinancialJuice real-time feed.

    Protocol: Centrifugo (JSON pub/sub over raw WebSocket frames).
      1) Fetch centrifugoToken from homepage HTML
      2) WebSocket connect + TLS upgrade
      3) Send connect: {"connect": {"token": "<token>"}}
      4) Loop: receive text frames, parse JSON, extract news text, INSERT.

    Token is re-fetched on every reconnect to prevent expiry.
    """

    reconnect_delay = 5

    while True:
        # ---- Fetch fresh config + token + cookies ------------------------
        cfg = await loop.run_in_executor(None, _extract_centrifugo_config)
        token = cfg.get("token")
        if not token:
            print(f"[INGEST] No token — retrying in {reconnect_delay}s ...")
            await asyncio.sleep(reconnect_delay)
            continue

        fresh_cookies = cfg.get("cookies", "")

        # ---- Connect with format #3 (the only one Centrifugo accepts) --------
        connect_payload = {"id": 1, "connect": {"token": token}}
        ok_connect_resp = None

        ws = _StdlibWebSocket()
        try:
            ws_headers = {
                "Origin": FJ_ORIGIN,
                "User-Agent": _FJ_HEADERS["User-Agent"],
            }
            if fresh_cookies:
                ws_headers["Cookie"] = fresh_cookies

            await ws.connect(FJ_WS_URL, extra_headers=ws_headers, timeout=15.0)
            msg = json.dumps(connect_payload)
            await ws.send_text(msg)

            raw_resp = await ws.recv_text(timeout=10.0)
            if raw_resp is None:
                code_info = ws.close_info or "no reason"
                print(f"[INGEST] Centrifugo rejected: {code_info} — retrying in {reconnect_delay}s ...")
                await _safe_close(ws)
                await asyncio.sleep(reconnect_delay)
                continue

            resp = json.loads(raw_resp.strip())
            if isinstance(resp, dict) and resp.get("error"):
                print(f"[INGEST] Centrifugo error: {resp['error']} — retrying in {reconnect_delay}s ...")
                await _safe_close(ws)
                await asyncio.sleep(reconnect_delay)
                continue

            # Success
            print(f"[INGEST] Connected — {_now()}")
            ok_connect_resp = resp
        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            print(f"[INGEST] Connect error: {exc} — retrying in {reconnect_delay}s ...")
            await _safe_close(ws)
            await asyncio.sleep(reconnect_delay)
            continue

        # ws is now the winning connection — start message loop
        try:
            # Parse connect response to find available channels
            try:
                conn_resp = ok_connect_resp
                subs = conn_resp.get("connect", {}).get("subs", {})
            except Exception:
                subs = {}
            channels = list(subs.keys())
            if not channels:
                channels = ["feed:all", "feed:lite"]

            print(f"[INGEST] Listening on {len(channels)} channels ...")

            # ---- Message loop ------------------------------------------
            inserted_total = 0

            while True:
                raw = await ws.recv_text(timeout=300.0)
                if raw is None:
                    print(f"[INGEST] Connection ended ({_now()}) — inserted={inserted_total}")
                    break

                raw_s = raw.strip()

                # Centrifugo sends {} as application-level heartbeat (ping).
                if raw_s == "{}":
                    await ws.send_text("{}")
                    continue
                if raw_s in ("", "[]"):
                    continue

                # Centrifugo sends one JSON object per frame (no \x1e needed)
                try:
                    msg = json.loads(raw_s)
                except json.JSONDecodeError:
                    continue

                if not isinstance(msg, dict):
                    continue

                # Handle ping (server heartbeat — must reply pong)
                ping_val = msg.get("ping")
                if ping_val is not None:
                    pong = json.dumps({"pong": ping_val})
                    await ws.send_text(pong)
                    continue

                # Handle push messages
                push = msg.get("push")
                if isinstance(push, dict):
                    channel = push.get("channel", "")
                    pub = push.get("pub") or push.get("data") or push
                    text = _extract_news_text(pub)
                    if text:
                        text = _clean_html(text)
                        if not _is_content_junk(text) and not text.startswith("http"):
                            original_text = text

                            # ── 去重检查 (翻译前 — 省 API 调用) ──
                            h = hashlib.sha256(original_text.encode()).hexdigest()[:16]
                            def _check_dup(hash_val: str) -> bool:
                                conn = _open_db()
                                try:
                                    ex = conn.execute(
                                        "SELECT id FROM raw_news WHERE content LIKE ? LIMIT 1",
                                        (f"[hash:{hash_val}]%",),
                                    ).fetchone()
                                    return ex is not None
                                finally:
                                    conn.close()
                            if await loop.run_in_executor(None, _check_dup, h):
                                continue  # 已处理过, 跳过

                            # ═════════════════════════════════════════════════════════════════════
                            # PRE-TRANSLATOR INTERCEPTOR (WebSocket 入口)
                            # ═════════════════════════════════════════════════════════════════════
                            text = await _translate_if_english(original_text, loop)
                            if text != original_text:
                                print(f"  [WS-TRANSLATE] {original_text[:30]} → {text[:30]}")

                            def _insert_push() -> int | None:
                                conn = _open_db()
                                try:
                                    ts = _ts()
                                    cleaned = f"[hash:{h}] {text[:500]}"
                                    f_result = evaluate_news(cleaned)
                                    cur = conn.execute(
                                        "INSERT INTO raw_news"
                                        " (source, content, timestamp, status, is_noise, relevance_score)"
                                        " VALUES (?, ?, ?, ?, ?, ?);",
                                        (
                                            f"WS:fj:{channel}" if channel else "WS:financialjuice",
                                            cleaned,
                                            ts,
                                            "PENDING",  # 统一用 PENDING, is_noise 区分噪音
                                            f_result["is_noise"],
                                            f_result["relevance_score"],
                                        ),
                                    )
                                    conn.commit()
                                    return cur.lastrowid
                                finally:
                                    conn.close()

                            rowid = await loop.run_in_executor(None, _insert_push)
                            if rowid is not None:
                                inserted_total += 1
                                print(
                                    f"\n  [{_now()}] INGEST #{rowid}"
                                    f" | {text[:80]}"
                                )
                    continue

                # Handle result (publication or system ack)
                result = msg.get("result")
                if isinstance(result, dict):
                    channel = result.get("channel", "")
                    data = result.get("data")

                    # Subscribe / unsubscribe ack — skip silently
                    if result.get("type") is not None:
                        continue

                    # Publication payload (less common path; most news comes via push)
                    if data is not None:
                        text = _extract_news_text(data)
                        if not text:
                            text = _extract_news_text(result)

                        if text:
                            text = _clean_html(text)
                            if not _is_content_junk(text) and not text.startswith("http"):
                                original_text = text

                                # ── 去重检查 (翻译前) ──
                                h = hashlib.sha256(original_text.encode()).hexdigest()[:16]
                                def _check_dup2(hash_val: str) -> bool:
                                    conn = _open_db()
                                    try:
                                        ex = conn.execute(
                                            "SELECT id FROM raw_news WHERE content LIKE ? LIMIT 1",
                                            (f"[hash:{hash_val}]%",),
                                        ).fetchone()
                                        return ex is not None
                                    finally:
                                        conn.close()
                                if await loop.run_in_executor(None, _check_dup2, h):
                                    continue

                                # ═════════════════════════════════════════════════════════════════════
                                # PRE-TRANSLATOR INTERCEPTOR (Result 分支)
                                # ═════════════════════════════════════════════════════════════════════
                                text = await _translate_if_english(original_text, loop)
                                if text != original_text:
                                    print(f"  [WS-TRANSLATE] {original_text[:30]} → {text[:30]}")

                                def _insert() -> int | None:
                                    conn = _open_db()
                                    try:
                                        ex = conn.execute(
                                            "SELECT id FROM raw_news"
                                            " WHERE content LIKE ? LIMIT 1",
                                            (f"[hash:{h}]%",),
                                        ).fetchone()
                                        if ex:
                                            return None
                                        vip_tag, _ = _detect_vip(text)
                                        ts = _ts()
                                        source_label = f"WS:fj:{channel}" if channel else "WS:financialjuice"
                                        if vip_tag:
                                            source_label = f"{source_label} {vip_tag}"
                                        cleaned = f"[hash:{h}] {text[:500]}"
                                        f_result = evaluate_news(cleaned)
                                        cur = conn.execute(
                                            "INSERT INTO raw_news"
                                            " (source, content, timestamp, status, is_noise, relevance_score)"
                                            " VALUES (?, ?, ?, ?, ?, ?);",
                                            (
                                                source_label,
                                                cleaned,
                                                ts,
                                                "PENDING",  # 统一用 PENDING, is_noise 区分噪音
                                                f_result["is_noise"],
                                                f_result["relevance_score"],
                                            ),
                                        )
                                        conn.commit()
                                        return cur.lastrowid
                                    finally:
                                        conn.close()

                                rowid = await loop.run_in_executor(None, _insert)
                                if rowid is not None:
                                    inserted_total += 1
                                    print(
                                        f"\n  [{_now()}] INGEST #{rowid}"
                                        f" | {text[:80]}"
                                    )
                        continue

            # If we exit the recv loop cleanly (server closed)
            print(f"[INGEST] Server closed ({_now()}) — inserted={inserted_total}")

        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            print(f"[INGEST] Connection error: {exc}")
        except Exception as exc:
            print(f"[INGEST] Unexpected error: {type(exc).__name__}: {exc}")
        finally:
            try:
                await ws.close()
            except Exception:
                pass

        print(f"[INGEST] Reconnecting in {reconnect_delay}s ...")
        await asyncio.sleep(reconnect_delay)
