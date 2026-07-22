#!/usr/bin/env python3
"""
Tree News WebSocket → Trident Webhook Bridge
=============================================
Connects to wss://news.treeofalpha.com/ws, parses incoming real-time
crypto/macro news, and forwards each message as an HTTP POST to the
Trident engine's webhook listener on 127.0.0.1:9000.

Usage:
  cd backend
  pip install websocket-client --break-system-packages   # one-time
  python src_python/treenews_bridge.py

The engine.py webhook handler (_tree_news_handler) does:
  - Dedup via SHA-256 content hash
  - VIP KOL detection (Trump / Musk / Powell / etc.)
  - Score amplification (×1.25)
  - Insert into raw_news → picked up by ai_worker next batch

Requirements: websocket-client (pip install websocket-client)
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ── graceful fallback if websocket-client not installed ──────────────────
try:
    import websocket
except ImportError:
    print("[BRIDGE] websocket-client not installed.")
    print("[BRIDGE] Run: pip install websocket-client --break-system-packages")
    raise SystemExit(1)

# ── config ───────────────────────────────────────────────────────────────
WS_URL = "wss://news.treeofalpha.com/ws"
WEBHOOK_URL = "http://127.0.0.1:9000/webhook"
RECONNECT_DELAY = 5          # seconds between reconnect attempts
MAX_RECONNECT_DELAY = 60     # cap exponential backoff
PING_INTERVAL = 30           # WS keepalive ping seconds

TZ_SHANGHAI = timezone(timedelta(hours=8))


# ── helpers ──────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now(TZ_SHANGHAI).strftime("%H:%M:%S")


def _forward_to_webhook(text: str, source: str = "tree_news", url: str = "") -> bool:
    """POST a single news item to the Trident webhook. Returns True on success."""
    payload = json.dumps({
        "text": text,
        "source": source,
        "url": url,
    }).encode("utf-8")

    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode())
            return body.get("ok", False)
    except urllib.error.URLError as e:
        print(f"  [{_now()}] webhook unreachable: {e.reason}")
        return False
    except Exception as e:
        print(f"  [{_now()}] webhook error: {e}")
        return False


# ── WebSocket callbacks ──────────────────────────────────────────────────
def on_message(ws, message: str):
    """Parse incoming Tree News message and forward to webhook."""
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        # occasionally raw text — forward as-is if short enough
        text = message.strip()[:500]
        if len(text) >= 5:
            _forward_to_webhook(text)
        return

    # ── extract text: try common field names ──
    # Tree of Alpha sends varied structures. For Twitter/X posts, the actual
    # tweet body often lives in "body" / "en" / "text" / "description" while
    # "title" / "source" contain only the author name (e.g. "Elon (@elonmusk)").
    # Strategy: collect candidates, pick the most substantive one, then try to
    # enrich with additional fields if the best candidate looks like an author.
    candidates: list[str] = []
    for field in ("body", "en", "text", "content", "description",
                  "tweet_text", "full_text", "summary", "title", "message"):
        val = str(data.get(field, "")).strip()
        if val and len(val) >= 3:
            candidates.append(val)

    if not candidates:
        return  # nothing useful at all

    # Pick the longest / most substantive candidate as primary body
    body_text = max(candidates, key=len)

    # Compose: if body_text is short and there's a longer secondary, combine
    if len(body_text) < 40 and len(candidates) > 1:
        # Sort by length descending, take top 2
        sorted_cands = sorted(set(candidates), key=len, reverse=True)
        if len(sorted_cands) >= 2 and len(sorted_cands[1]) > len(body_text):
            body_text = f"{sorted_cands[1]} — {body_text}"

    # ── Pre-filter: discard account-name-only / empty / trivial ──
    text = body_text.strip()

    # Remove common noise patterns

    # Is this effectively just a Twitter handle or author name?
    # Patterns: "@username", "Name (@handle)", "Name (@handle):"
    handle_only = re.match(
        r'^@?\w+\s*(\(@?\w+\))?\s*:?\s*$', text
    )
    # Or the text is just a name like "Donald J. Trump" with optional handle
    name_only = (
        len(text) < 25
        and not re.search(r'[.,!?;:]{2,}|http|USD|EUR|CNY|JPY|GBP|oil|crude|gold|btc|eth|fed|trump|musk|powell|bank|rate|inflation|market|price|stock|bond|yield', text, re.IGNORECASE)
        and re.match(r'^[\w\s.,\'\"@()\-]+$', text)
    )

    if handle_only or name_only or len(text) < 12:
        print(f"  [{_now()}] [Filter] Ignored empty/name-only content from TreeNews: {text[:80]}")
        return

    # ── extract source label ──
    source = str(data.get("source", "tree_news")).strip()[:64]
    news_url = str(data.get("url", "")).strip()

    # ── extract timestamp for display ──
    ts_raw = data.get("created_at") or data.get("timestamp") or data.get("time")
    if ts_raw:
        try:
            ts_val = int(ts_raw)
            if ts_val > 1e12:          # millisecond timestamp
                ts_val /= 1000
            ts_display = datetime.fromtimestamp(ts_val, TZ_SHANGHAI).strftime("%H:%M:%S")
        except (ValueError, OSError):
            ts_display = str(ts_raw)[:8]
    else:
        ts_display = _now()

    # ── forward ──
    ok = _forward_to_webhook(text, source, news_url)
    status = "OK" if ok else "FAIL"
    print(f"[{ts_display}] [{source}] {status} | {text[:90]}")
    if news_url:
        print(f"    → {news_url}")


def on_error(ws, error):
    print(f"[{_now()}] WS error: {type(error).__name__}: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[{_now()}] WS disconnected  code={close_status_code}  msg={close_msg}")


def on_open(ws):
    print(f"[{_now()}] connected to Tree News WS — forwarding to {WEBHOOK_URL}")
    # If you have a paid Tree of Alpha API key, uncomment and fill in:
    # ws.send(json.dumps({"api_key": "YOUR_KEY"}))


# ── main loop ────────────────────────────────────────────────────────────
def main():
    print(f"[{_now()}] Tree News Bridge starting...")
    print(f"[{_now()}] WS: {WS_URL}")
    print(f"[{_now()}] Webhook: {WEBHOOK_URL}")
    print(f"[{_now()}] Make sure engine.py is running (Task C listens on :9000)")
    print("-" * 60)

    backoff = RECONNECT_DELAY

    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        try:
            ws.run_forever(ping_interval=PING_INTERVAL, ping_timeout=10)
        except KeyboardInterrupt:
            print(f"\n[{_now()}] shutting down...")
            break
        except Exception as e:
            print(f"[{_now()}] connection lost: {type(e).__name__}: {e}")

        # exponential backoff with cap
        print(f"[{_now()}] reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_RECONNECT_DELAY)


if __name__ == "__main__":
    main()
