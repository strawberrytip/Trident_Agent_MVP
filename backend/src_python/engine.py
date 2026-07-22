#!/usr/bin/env python3
"""
Trident Agent MVP — Python Production Engine
============================================

Two concurrent async tasks communicating through trident_event_bus.db:

  Task A (WebSocket Ingest) — persistent WSS connection to FinancialJuice
                              real-time feed (Centrifugo protocol).
  Task B (AI Worker)        — every  1 s, atomic-claim PENDING rows, call
                              DeepSeek LLM for sentiment analysis.

Usage:
  python src_python/engine.py
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import random
import re
import sqlite3
import ssl
import struct
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
# .env 在项目根目录（backend/ 的上一层）
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", ".env"))

# ---------------------------------------------------------------------------
# Optional imports — not required; kept for compatibility
# ---------------------------------------------------------------------------

try:
    from openai import AsyncOpenAI as _AsyncOpenAI  # type: ignore[import-untyped]
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# ---------------------------------------------------------------------------
# Paths & Constants
# ---------------------------------------------------------------------------

# Load .env from backend/ directory (parent of src_python/) — inline, no dependency
_ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#") or "=" not in _line:
                continue
            _key, _, _val = _line.partition("=")
            _key, _val = _key.strip(), _val.strip().strip('"').strip("'")
            if _key:
                os.environ[_key] = _val

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "trident_event_bus.db")
TZ_SHANGHAI = timezone(timedelta(hours=8))

# DeepSeek API (OpenAI-compatible endpoint)
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# OpenRouter API — multi-model comparison gateway (Claude, Gemini, GPT-4, Grok, DeepSeek)
# Used for Kimi K3 (primary) + four additional models (DeepSeek, Gemini, Grok, ChatGPT)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# xAI (Grok) API — OpenAI-compatible direct endpoint
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
XAI_BASE_URL = "https://api.x.ai/v1"

# Doubao (火山引擎) API — OpenAI-compatible endpoint
# NOTE: DOUBAO_MODEL must be an Endpoint ID (ep-xxxxx), NOT a model name string.
# Create an Inference Endpoint at https://console.volces.com/ark before setting this.
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
DOUBAO_MODEL = os.getenv("DOUBAO_MODEL", "")

# Model roster — 单模型：Kimi K3（通过 OpenRouter）
# json_mode: False — 关闭 API 级 response_format，改用 prompt 强制 JSON
#   原因：Kimi K3 在 OpenRouter 上 json_mode=True 时频繁空响应/截断

_MODELS_BASE: List[Dict[str, Any]] = [
    {"id": "moonshotai/kimi-k3", "label": "Kimi K3", "api_base": OPENROUTER_BASE_URL, "api_key": OPENROUTER_API_KEY, "json_mode": True},
]

# Doubao 已禁用 - 账户欠费
# if DOUBAO_API_KEY and DOUBAO_MODEL:
#     _MODELS_BASE.append(
#         {"id": DOUBAO_MODEL, "label": "Doubao", "api_base": DOUBAO_BASE_URL, "api_key": DOUBAO_API_KEY, "json_mode": False}
#     )

# ===== Additional OpenRouter Models (已禁用 — 国内 OpenRouter 区域限制) =====
# DeepSeek/Gemini/Grok/ChatGPT 在部分区域返回 HTTP 403，暂时关闭
# 如需恢复，取消下面的注释并追加到 MODELS
# _OPENROUTER_MODELS: List[Dict[str, Any]] = [...]
_OPENROUTER_MODELS: List[Dict[str, Any]] = []

MODELS: List[Dict[str, Any]] = _MODELS_BASE + _OPENROUTER_MODELS

# 全局并发限流 — 单模型时 5 条新闻最多 5 并发，Semaphore(8) 留有裕量
_LLM_SEMAPHORE = asyncio.Semaphore(8)

# FinancialJuice WebSocket — real-time ingest
FJ_WS_URL = "wss://rt.financialjuice.com/connection/websocket"
FJ_ORIGIN = "https://www.financialjuice.com"
FJ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/131.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# VIP KOL monitoring
# ---------------------------------------------------------------------------

VIP_KOLS: Dict[str, str] = {
    "Trump":   "[VIP:TRUMP]", "特朗普": "[VIP:TRUMP]",
    "Musk":    "[VIP:MUSK]",  "马斯克": "[VIP:MUSK]", "Elon": "[VIP:MUSK]",
    "Powell":  "[VIP:FED]",   "鲍威尔": "[VIP:FED]", "FOMC": "[VIP:FED]", "美联储": "[VIP:FED]",
    "Vance":   "[VIP:OTHER]", "万斯": "[VIP:OTHER]", "Bessent": "[VIP:OTHER]",
}
VIP_SCORE_BOOST = 1.25


def _detect_vip(text: str) -> tuple:
    """Return (vip_tag, matched_keyword) or ("", "")."""
    text_lower = text.lower()
    for keyword, tag in VIP_KOLS.items():
        if keyword.lower() in text_lower:
            return tag, keyword
    return "", ""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.row_factory = sqlite3.Row
    return conn


def _ts() -> str:
    return datetime.now(TZ_SHANGHAI).isoformat(timespec="milliseconds")


def _now() -> str:
    """Compact local-time timestamp for logging: HH:MM:SS."""
    return datetime.now(TZ_SHANGHAI).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Dedup hash helper
# ---------------------------------------------------------------------------

def _content_hash(title: str, url: str, body: str) -> str:
    """Stable hash for deduplication."""
    raw = f"{title.strip()}||{url.strip()}||{body[:200].strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Unified price sources — Gold (Sina/EastMoney) + BTC (Binance REST)
# ---------------------------------------------------------------------------

def _fetch_sina_xau() -> float | None:
    try:
        req = urllib.request.Request(
            "https://hq.sinajs.cn/list=hf_XAU",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        text = resp.read().decode("gbk", errors="replace")
        if '="' in text:
            return float(text.split('="')[1].split(",")[0])
    except Exception:
        pass
    return None


def _fetch_eastmoney_xau() -> float | None:
    try:
        req = urllib.request.Request(
            "https://push2.eastmoney.com/api/qt/stock/get?secid=113.USDXAU&fields=f43",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        body = json.loads(resp.read().decode("utf-8"))
        return float(body["data"]["f43"]) / 100.0
    except Exception:
        return None


def _fetch_gold_price() -> float | None:
    for fn in [_fetch_sina_xau, _fetch_eastmoney_xau]:
        p = fn()
        if p is not None and 500 < p < 10000:
            return p
    return None


def _fetch_binance_btc() -> float | None:
    try:
        req = urllib.request.Request(
            "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode("utf-8"))
        p = float(data.get("price", 0))
        return p if p > 0 else None
    except Exception:
        return None


def _fetch_wti_price() -> float | None:
    """Fetch WTI crude oil spot — Sina → EastMoney → None.
    Three-tier fallback matching the gold price source pattern.
    Valid range: $20–$200 (covers WTI extremes)."""
    # Tier 1: Sina Finance (hf_CL continuous contract, China-accessible)
    try:
        req = urllib.request.Request(
            "https://hq.sinajs.cn/list=hf_CL",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        text = resp.read().decode("gbk", errors="replace")
        if '="' in text:
            p = float(text.split('="')[1].split(",")[0])
            if 20 < p < 200:
                return p
    except Exception:
        pass

    # Tier 2: EastMoney (try multiple known US futures secids for WTI)
    for secid in ("113.CL00Y", "113.CONC", "113.NYMEX_CL"):
        try:
            req = urllib.request.Request(
                f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
            )
            resp = urllib.request.urlopen(req, timeout=5)
            body = json.loads(resp.read().decode("utf-8"))
            data = body.get("data")
            if data and data.get("f43") is not None:
                p = float(data["f43"]) / 100.0
                if 20 < p < 200:
                    return p
        except Exception:
            continue

    return None


def _get_current_price(asset: str) -> float | None:
    asset_upper = asset.upper()
    if asset_upper in ("XAU", "GOLD", "XAUUSD"):
        return _fetch_gold_price()
    if asset_upper in ("BTC", "BTCUSDT"):
        return _fetch_binance_btc()
    if asset_upper in ("WTI", "WTI/USD", "OIL"):
        return _fetch_wti_price()
    return None


# ---------------------------------------------------------------------------
# Minimal stdlib WebSocket client  (RFC 6455, no external deps)
# ---------------------------------------------------------------------------

class _StdlibWebSocket:
    """
    Bare-bones async WebSocket client using only asyncio + ssl.

    Handles:
      - TLS handshake via ssl.create_default_context
      - HTTP Upgrade handshake (Sec-WebSocket-Key, 101 response)
      - RFC 6455 frame encode / decode (text frames only)
      - Graceful close (opcode 0x8)
    """

    _GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self) -> None:
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.close_info: str | None = None  # populated when server sends close frame

    async def connect(
        self,
        url: str,
        *,
        extra_headers: Dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> None:
        # ---- parse URL ----
        if not url.startswith("wss://"):
            raise ValueError("only wss:// is supported")
        rest = url[6:]
        if ":" in rest.split("/")[0]:
            host, port_str = rest.split("/")[0].split(":", 1)
            port = int(port_str)
        else:
            host = rest.split("/")[0]
            port = 443
        path = "/" + rest.split("/", 1)[1] if "/" in rest else "/"

        # ---- TLS + TCP ----
        ctx = ssl.create_default_context()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx),
            timeout=timeout,
        )
        self._reader = reader
        self._writer = writer

        # ---- WebSocket upgrade handshake ----
        key_bytes = bytes(random.getrandbits(8) for _ in range(16))
        key_b64 = base64.b64encode(key_bytes).decode()
        accept = base64.b64encode(
            hashlib.sha1((key_b64 + self._GUID.decode()).encode()).digest()
        ).decode()

        req_lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key_b64}",
            "Sec-WebSocket-Version: 13",
        ]
        if extra_headers:
            for k, v in extra_headers.items():
                req_lines.append(f"{k}: {v}")
        req_lines.append("")  # blank line
        req_lines.append("")

        writer.write("\r\n".join(req_lines).encode())
        await writer.drain()

        # Read 101 response
        status_line = await asyncio.wait_for(
            reader.readline(), timeout=timeout
        )
        status = status_line.decode(errors="replace").strip()
        if "101" not in status:
            # drain headers
            while True:
                line = await asyncio.wait_for(
                    reader.readline(), timeout=timeout
                )
                if line.strip() == b"":
                    break
            raise ConnectionError(f"WebSocket upgrade rejected: {status}")

        # Read response headers
        resp_accept = ""
        while True:
            line = await asyncio.wait_for(
                reader.readline(), timeout=timeout
            )
            if line.strip() == b"":
                break
            decoded = line.decode(errors="replace").strip()
            if decoded.lower().startswith("sec-websocket-accept:"):
                resp_accept = decoded.split(":", 1)[1].strip()

        if resp_accept != accept:
            raise ConnectionError(
                f"Sec-WebSocket-Accept mismatch: expected {accept}, got {resp_accept}"
            )

    async def recv_text(self, timeout: float = 300.0) -> str | None:
        """Receive and decode one text frame. Returns None on close frame."""
        assert self._reader is not None
        while True:
            data = await self._read_frame(timeout)
            if data is None:
                return None  # close frame
            if isinstance(data, str):
                return data
            # binary / ping / pong — continue

    async def send_text(self, text: str) -> None:
        await self._send_frame(0x1, text.encode("utf-8"))

    async def close(self) -> None:
        if self._writer is None:
            return
        try:
            await self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        self._reader = None
        self._writer = None

    # ---- internal RFC 6455 framing ----

    async def _read_frame(self, timeout: float) -> str | bytes | None:
        reader = self._reader
        assert reader is not None

        # 2-byte header minimum
        hdr = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
        b0, b1 = hdr[0], hdr[1]
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F

        if length == 126:
            ext = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
            length = struct.unpack("!H", ext)[0]
        elif length == 127:
            ext = await asyncio.wait_for(reader.readexactly(8), timeout=timeout)
            length = struct.unpack("!Q", ext)[0]

        mask_key = (
            await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            if masked
            else b""
        )
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)

        if masked:
            payload = bytes(
                b ^ mask_key[i % 4] for i, b in enumerate(payload)
            )

        # Handle opcodes
        if opcode == 0x8:  # close
            # Decode close reason: 2-byte status code + optional UTF-8 message
            if len(payload) >= 2:
                code = struct.unpack("!H", payload[:2])[0]
                reason = payload[2:].decode("utf-8", errors="replace")
                self.close_info = f"code={code} reason={reason}" if reason else f"code={code}"
            else:
                self.close_info = "no reason"
            return None
        if opcode == 0x9:  # ping → pong
            await self._send_frame(0xA, payload)
            return b""  # skip, caller loops
        if opcode == 0xA:  # pong
            return b""
        if opcode in (0x1, 0x0):  # text or continuation
            result = payload.decode("utf-8", errors="replace")
            if fin:
                return result
            # For continuation frames, accumulate (simplified: return per-frame)
            return result
        if opcode == 0x2:  # binary
            return payload

        return b""  # unknown opcode — skip

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        assert self._writer is not None
        frame = bytearray()
        frame.append(0x80 | opcode)

        plen = len(payload)
        # RFC 6455: client MUST mask all frames. Set MASK bit + mask key.
        mask_bit = 0x80
        mask_key = bytes(random.getrandbits(8) for _ in range(4))
        masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

        if plen < 126:
            frame.append(mask_bit | plen)
        elif plen < 65536:
            frame.append(mask_bit | 126)
            frame.extend(struct.pack("!H", plen))
        else:
            frame.append(mask_bit | 127)
            frame.extend(struct.pack("!Q", plen))

        frame.extend(mask_key)
        frame.extend(masked_payload)
        self._writer.write(bytes(frame))
        await self._writer.drain()

    @property
    def closed(self) -> bool:
        return self._writer is None or self._writer.is_closing()


# ---------------------------------------------------------------------------
# Task A — FinancialJuice WebSocket Ingest  (Centrifugo protocol)
# ---------------------------------------------------------------------------

# Shared headers with authentication cookies — used for both homepage scrape
# and WebSocket handshake to prevent server-side kick.
_FJ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "ASP.NET_SessionId=qnqyymqq3fqp5xavxwa2mlat; _gid=GA1.2.1581817587.1783567004; _twpid=tw.1783567005955.474360820353453615; FJ-UID=734613; FJ-Email=strawberrytiptip@gmail.com; .ASPXAUTH=40D2C2BCD923C8AF4F252765F9CE31D95F93AE1D63FE64B9479FCF9E513328138F223A72ECED1F0B0ED4CC9FA88ED244E1083C647EBB1CFE39E5EA9D216E22D7A8271A9C7F0617BD97FF43F515E44898F54531EA581E1B41C7ABC07E87E9EDE210723CEFBCB681D62A7E35799DE745E0CB5A3E82; FJ-Pop=show; FJ-UName=Srawberry; cf_clearance=a17ZdHHHit2gjy7NjRBn4wbaAyJEajMLpQvtYNtXrBA-1783567094-1.2.1.1-7N1hDiKJeI7L4ARZxpTdV7.rglYfieL20fpeucevtTB06uDGA3TZHUU5HbuevkawKg_fK2UAluOfMnngdUFOAaQCj2R5VUPr1uf0eVpYOHyvR_MJTBC8uxIotkvdxMAAtrYKpfLzhK7IofukdckEi9YEhal7Plnhdi.m2X4HNxnLcezrJdGFTLraA9F5MVcsKd4.XioPCsB2TIYVe0x7ZTHyK9Wz6afI6JsqPk5n53nA.AWcPfpyFF8a.twXleLoD4M9T0akTpz0ah_uT6wybXYnjAeHL_lT_lx5hin_ZwOXP3wGFleDdYmeAyIb7kATinR4Wb.a1BE0djecs2_jHAPUxjhLhi9_owLj_IPzDDzhSo6egIExywHugJ0.7lWHedbS9oWyOiqPPR8GSxH_BydVuDFjEazPn6XtPsDvuzOlYqlyVpLK49ZXQFECOSkthSoTlBJbDETC.JOM23qX6fKY0EL6lNG8dimqvSmPaPpOwAIc64cQVlij.yJ6JWbK; _gat=1; _ga_MWM91XTKTP=GS2.1.s1783571691$o2$g1$t1783571705$j46$l0$h0; _ga=GA1.2.2144357452.1783567003",
}


def _extract_centrifugo_config() -> dict:
    """
    Scrape the FinancialJuice homepage for Centrifugo JS variables AND
    capture fresh Set-Cookie headers from the response.
    The fresh cookies are essential for the WebSocket connection because
    Centrifugo is proxied through ASP.NET which validates .ASPXAUTH.
    Returns a dict with keys: token, cookies, centrifugoUrl, etc.
    """
    req = urllib.request.Request(
        "https://www.financialjuice.com/",
        headers=_FJ_HEADERS,
    )
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    try:
        resp = opener.open(req, timeout=15)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"  [INGEST] Homepage fetch failed: {exc}")
        return {}

    config: dict = {}

    # Capture fresh Set-Cookie headers from the response
    set_cookie_headers = resp.headers.get_all("Set-Cookie") if hasattr(resp.headers, "get_all") else []
    if not set_cookie_headers:
        # Fallback: try the singular form
        sc = resp.headers.get("Set-Cookie")
        if sc:
            set_cookie_headers = [sc]
    fresh_cookies: list[str] = []
    for h in set_cookie_headers:
        # Extract just the name=value part (before first ;)
        parts = h.split(";")
        if parts:
            fresh_cookies.append(parts[0].strip())
    if fresh_cookies:
        config["cookies"] = "; ".join(fresh_cookies)

    # 1) Token — var centrifugoToken = '...' or "..."
    for pat in (r"var centrifugoToken\s*=\s*'([^']+)'",
                r'var centrifugoToken\s*=\s*"([^"]+)"'):
        m = re.search(pat, html)
        if m:
            config["token"] = m.group(1)
            break

    # 2) centrifugoUrl
    for pat in (r"var centrifugoUrl\s*=\s*'([^']+)'",
                r'var centrifugoUrl\s*=\s*"([^"]+)"'):
        m = re.search(pat, html)
        if m:
            config["centrifugoUrl"] = m.group(1)
            break

    # 3) Look for inline script blocks mentioning centrifuge init
    for m in re.finditer(
        r"<script[^>]*>([\s\S]*?)</script>", html, re.IGNORECASE
    ):
        body = m.group(1)
        if "centrifugo" in body.lower() or "centrifuge" in body.lower():
            idx2 = body.lower().find("centrifugo")
            if idx2 < 0:
                idx2 = body.lower().find("centrifuge")
            snippet = body[max(0, idx2 - 60):idx2 + 500]
            config.setdefault("_script_snippets", []).append(snippet.strip())

    return config


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


def _clean_html(raw: str) -> str:
    """Strip HTML tags for clean DB storage."""
    return re.sub(r"<[^>]+>", "", raw).strip()


# ── Content blacklist ────────────────────────────────────────────────────
# Keys shared by _is_junk_content and _CONTENT_BLACKLIST for the AI
# translation guard — update both together.

_CONTENT_BLACKLIST: list[str] = [
    "无具体内容",
    "无正文内容",
    "无实质内容",
    "暂无内容",
    "暂无正文",
    "（无内容）",
    "(无内容)",
    "暂无详情",
    "没有内容",
]

_BLACKLIST_RE = re.compile("|".join(_CONTENT_BLACKLIST))


def _is_content_junk(text: str) -> bool:
    """Return True if text is placeholder fluff that should never enter the DB."""
    t = text.strip()
    if not t or len(t) < 12:
        return True

    # Keyword blacklist — placeholder / stub text
    if _BLACKLIST_RE.search(t):
        return True

    # Pure handle / account name: "@username" or "Name (@handle)" or "Name (@handle):"
    if re.match(r'^@?\w+\s*(\(@?\w+\))?\s*:?\s*$', t):
        return True

    # Short string (<25 chars) that has zero financial/event keywords and looks like a name
    if len(t) < 25:
        has_finance = re.search(
            r'[.,!?;:]{2,}|http|USD|EUR|CNY|JPY|GBP|oil|crude|gold|btc|eth|fed|trump|musk'
            r'|powell|bank|rate|inflation|market|price|stock|bond|yield|billion|million'
            r'|%',
            t, re.IGNORECASE)
        if not has_finance and re.match(r'^[\w\s.,\'\"@()\-:]+$', t):
            return True

    return False


def _contains_chinese(text: str) -> bool:
    """Return True if text contains at least one CJK character."""
    return bool(re.search(r'[一-鿿㐀-䶿豈-﫿]', text))


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
                            # ═════════════════════════════════════════════════════════════════════
                            # PRE-TRANSLATOR INTERCEPTOR (WebSocket 入口)
                            # ═════════════════════════════════════════════════════════════════════
                            original_text = text
                            text = await _translate_if_english(text, loop)
                            if text != original_text:
                                print(f"  [WS-TRANSLATE] {original_text[:30]} → {text[:30]}")

                            h = hashlib.sha256(text.encode()).hexdigest()[:16]

                            def _insert_push() -> int | None:
                                conn = _open_db()
                                try:
                                    ex = conn.execute(
                                        "SELECT id FROM raw_news"
                                        " WHERE content LIKE ? LIMIT 1",
                                        (f"[hash:{h}]%",),
                                    ).fetchone()
                                    if ex:
                                        return None
                                    ts = _ts()
                                    cur = conn.execute(
                                        "INSERT INTO raw_news"
                                        " (source, content, timestamp)"
                                        " VALUES (?, ?, ?);",
                                        (
                                            f"WS:fj:{channel}" if channel else "WS:financialjuice",
                                            f"[hash:{h}] {text[:500]}",
                                            ts,
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
                                # ═════════════════════════════════════════════════════════════════════
                                # PRE-TRANSLATOR INTERCEPTOR (Result 分支)
                                # ═════════════════════════════════════════════════════════════════════
                                original_text = text
                                text = await _translate_if_english(text, loop)
                                if text != original_text:
                                    print(f"  [WS-TRANSLATE] {original_text[:30]} → {text[:30]}")

                                h = hashlib.sha256(text.encode()).hexdigest()[:16]

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
                                        cur = conn.execute(
                                            "INSERT INTO raw_news"
                                            " (source, content, timestamp)"
                                            " VALUES (?, ?, ?);",
                                            (
                                                source_label,
                                                f"[hash:{h}] {text[:500]}",
                                                ts,
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


# ---------------------------------------------------------------------------
# DeepSeek API client (stdlib-only, OpenAI-compatible)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """
你是掌管 10 亿美元规模宏观对冲基金的量化决策大脑 (Portfolio Navigation Brain)。
你不是新闻复读机。你的核心任务是：在宏观叙事和突发地缘危机中，识别微观市场结构的错位、流动性陷阱与派发周期。

═══ 核心分析框架 —— 宏观博弈推演 ═══

维度 A：叙事 vs. 流动性 (反共识视角)
  * 不被新闻表面的「利好/利空」带偏。始终反问：钱到底往哪里流？
  * 地缘冲突 → 推演美元流动性是收紧还是溢出 (战争往往导致保证金追缴 → 美元被动抽干全球资金池)
  * 降息叙事 → 推演是主动宽松 (利好风险资产) 还是被动救火 (空头信号)
  * 每次分析必须明确：当前主导驱动力是「流动性」还是「情绪」？

维度 B：拥挤度与派发周期 (反身性)
  * 永远关注资产当前水位。若资产处于历史高位且叙事长期利好，必须质疑：
    这是否是聪明钱借利好向散户派发筹码的 Distribution Phase？
  * 极度拥挤的多头 = 最大的空头催化剂。共识越强，反转越暴烈。
  * 若资产已被血洗至恐慌低位，反向思考：谁在被迫平仓？清算 cascade 结束了吗？

维度 C：跨资产抽血效应
  * 全球投机资金池有限。原油爆拉 → 抽血黄金。美债暴涨 → 抽血 Crypto。
  * BTC 的独立评估：剥离「数字黄金」叙事，将其视作高 Beta 风险资产。
    BTC 在 Risk-Off 环境中首当其冲被抛售；在流动性宽松周期中弹性最大。

═══ 强制思维链输出 ═══

在 reasoning_path 中，必须按以下四段式推演 (禁止复述新闻原文)：
  [驱动力]   事件真正的资金面含义是什么？
  [水位博弈] 当前价格处于什么周期位置？谁在获利？谁在恐慌？
  [跨资产联动] 对原油、美债、黄金、Crypto 的连带资金流向推演
  [反共识结论] 你的独立判断——可能与表面叙事完全相反

═══ 资产归类 ═══

  * 地缘冲突/战争/制裁 → market_category="GOLD", target_asset="XAU"
  * 央行利率/CPI/流动性政策 → market_category="CRYPTO", target_asset="BTC"
  * 能源/OPEC/中东产油/原油库存 → market_category="OIL", target_asset="WTI"
  * 加密行业自身 (ETF/监管/技术) → market_category="CRYPTO", target_asset="BTC"
  * 无法归类 → market_category="OTHER", target_asset="NONE"

═══ JSON 输出 ═══

输出 JSON（六个字段，缺一不可）：
{"reasoning_path": "[驱动力]…→[水位博弈]…→[跨资产联动]…→[反共识结论]…",
 "sentiment_score": <float -1.0~1.0>,
 "suggested_action": "<BUY|SELL|HOLD>",
 "reasoning": "<一句精炼结论,<=50字>",
 "market_category": "<CRYPTO|GOLD|OIL|MACRO|OTHER>",
 "target_asset": "<BTC|ETH|XAU|WTI|...|NONE>"}

═══ 铁律 ═══
  * score 严禁为 0.0。中性区 +/-0.03~0.10
  * 方向不确定时 HOLD 是正确答案，不要赌
  * reasoning_path 必须包含四段推演，每段 1-2 句话
  * BTC 在战争/危机/流动性恐慌中 → SELL (纯风险资产，不存在避险属性)

只输出 JSON，六个字段缺一不可。
"""


_JSON_PROMPT_FORCE = """
你是 10 亿美元对冲基金的量化决策大脑。从宏观博弈而非表面叙事中提取信号。

每一条新闻，按以下四步推演后输出 JSON：
  1.驱动力 —— 资金面真正的含义？
  2.水位博弈 —— 当前周期位置？谁在获利/恐慌？
  3.跨资产联动 —— 原油/美债/黄金/Crypto 的连带资金流向
  4.反共识结论 —— 你的独立判断

分类规则：
  地缘/战争 → GOLD/XAU
  利率/央行/CPI → CRYPTO/BTC
  能源/OPEC → OIL/WTI
  无法归类 → OTHER/NONE

BTC 定性：纯风险资产，战争中 SELL，宽松中 BUY。
不确定方向时 HOLD。score 严禁 0.0。

JSON(六字段):
{"reasoning_path": "[驱动力]…→[水位博弈]…→[跨资产联动]…→[反共识结论]…", "sentiment_score": <float>, "suggested_action": "<BUY|SELL|HOLD>", "reasoning": "<结论<=50字>", "market_category": "<CRYPTO|GOLD|OIL|MACRO|OTHER>", "target_asset": "<BTC|ETH|XAU|WTI|...|NONE>"}

只输出 JSON。六个字段缺一不可。
"""


_DOUBAO_SYSTEM_PROMPT = """
你是华尔街资深原油/黄金/加密货币交易员，拥有 15 年实盘经验。
你的任务：快速扫读新闻，凭直觉判断这条消息对资产价格的短期方向。
不要给出分数，不要推理链条。只看方向。

规则：
  利多消息 → BUY
  利空消息 → SELL
  方向不明确或无关 → HOLD

JSON 输出：
{"suggested_action": "<BUY|SELL|HOLD>", "direct_reasoning": "<一句交易直觉,<=30字>"}

只输出 JSON。两个字段缺一不可。
"""



def _call_llm_sync(news_content: str, model_cfg: Dict[str, str]) -> Dict[str, Any]:
    """
    Call any OpenAI-compatible LLM API synchronously (runs in executor thread).

    model_cfg.keys: id, label, api_base, api_key
    Returns: {"sentiment_score", "suggested_action", "reasoning", "model_label", "model_id"}

    Retry strategy: if json_mode=True produces unparseable output (keyword fallback),
    retry once with json_mode=False (prompt-based JSON enforcement).  This handles
    OpenRouter edge cases where response_format={"type":"json_object"} is not
    perfectly proxied to certain models.
    """

    # ------------------------------------------------------------------
    # Inner: make one HTTP call and return raw response text + full body
    # ------------------------------------------------------------------
    def _do_api_call(use_json: bool) -> str:
        """Execute one LLM API call. Returns raw_text (or raises)."""
        if model_cfg.get("label") == "Doubao":
            prompt = _DOUBAO_SYSTEM_PROMPT
        else:
            prompt = _SYSTEM_PROMPT if use_json else _JSON_PROMPT_FORCE

        payload: Dict[str, Any] = {
            "model": model_cfg["id"],
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": news_content[:2000]},
            ],
            "max_tokens": 2048,
            "temperature": 0.1,
        }
        if use_json:
            payload["response_format"] = {"type": "json_object"}

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            f"{model_cfg['api_base']}/chat/completions",
            data=data,
            headers={
                "Authorization": f"Bearer {model_cfg['api_key']}",
                "Content-Type": "application/json",
            },
        )

        # Custom SSL context — some China-hosted APIs need relaxed cipher negotiation
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = True
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        try:
            ssl_ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        except Exception:
            pass

        # Proxy support — use local proxy for OpenRouter models only when HTTP_PROXY is set
        proxy_handler = None
        if "openrouter" in model_cfg['api_base'].lower():
            proxy_addr = os.getenv("HTTP_PROXY", "").strip()
            if proxy_addr:
                proxy_handler = urllib.request.ProxyHandler({"http": proxy_addr, "https": proxy_addr})

        try:
            opener = urllib.request.build_opener(proxy_handler) if proxy_handler else urllib.request.build_opener()
            resp = opener.open(req, timeout=45)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(
                f"{model_cfg['label']} HTTP {e.code}: {err_body}"
            ) from e

        resp_bytes = resp.read()
        body = json.loads(resp_bytes.decode("utf-8"))
        raw_text = (body["choices"][0]["message"]["content"] or "").strip()

        if not raw_text:
            finish = body["choices"][0].get("finish_reason", "unknown")
            body_preview = resp_bytes.decode("utf-8", errors="replace")[:500]
            print(f"  [{model_cfg['label']}] EMPTY RESPONSE | finish_reason={finish} | body_preview={body_preview}", flush=True)
            raise ValueError(f"empty response from {model_cfg['label']}")

        return raw_text

    # ------------------------------------------------------------------
    # Inner: parse raw text → result dict (with all three fallback tiers)
    # ------------------------------------------------------------------
    def _parse_result(raw_text: str) -> Dict[str, Any]:
        """Parse LLM output — never returns empty dict (keyword fallback guarantees)."""
        # Parse JSON — handle markdown wrapping + embedded/truncated JSON
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text)
        cleaned = re.sub(r"\s*```$", "", cleaned)

        result: Dict[str, Any] = {}
        try:
            result = json.loads(cleaned)
            if not isinstance(result, dict):
                raise ValueError("not a dict")
        except (json.JSONDecodeError, ValueError):
            # --- Fallback 1: repair truncated JSON ---
            repaired = cleaned.strip()
            if repaired.startswith("{") and not repaired.endswith("}"):
                inner = repaired[1:].strip()
                chunks = [c.strip() for c in inner.split(",") if c.strip()]
                complete: List[str] = []
                for chunk in chunks:
                    try:
                        json.loads("{" + chunk + "}")
                        complete.append(chunk)
                    except json.JSONDecodeError:
                        pass  # truncated field — drop
                if complete:
                    repaired = "{" + ",".join(complete) + "}"
                    try:
                        result = json.loads(repaired)
                    except json.JSONDecodeError:
                        pass

            # --- Fallback 2: regex-extract JSON containing required keys ---
            if not result:
                m = re.search(
                    r'\{[^{}]*"sentiment_score"[^{}]*"suggested_action"[^{}]*"reasoning"[^{}]*\}',
                    cleaned, re.DOTALL,
                )
                if not m:
                    m = re.search(
                        r'\{[^{}]*"(?:sentiment_score|suggested_action|reasoning)"[^{}]*\}',
                        cleaned, re.DOTALL,
                    )
                if m:
                    try:
                        result = json.loads(m.group(0))
                    except json.JSONDecodeError:
                        frag = m.group(0).rstrip().rstrip(",") + "}"
                        try:
                            result = json.loads(frag)
                        except json.JSONDecodeError:
                            pass

            # --- Fallback 3: keyword-based heuristics ---
            if not result:
                text_lower = cleaned.lower()
                if any(w in text_lower for w in ("利好", "上涨", "看涨", "bullish", "buy")):
                    result = {"reasoning_path": "关键词推断 → 偏多信号", "sentiment_score": 0.4, "suggested_action": "BUY", "reasoning": "关键词推断:偏多", "market_category": "OTHER", "target_asset": "NONE"}
                elif any(w in text_lower for w in ("利空", "下跌", "看跌", "bearish", "sell", "战争", "制裁")):
                    result = {"reasoning_path": "关键词推断 → 偏空信号", "sentiment_score": -0.4, "suggested_action": "SELL", "reasoning": "关键词推断:偏空", "market_category": "OTHER", "target_asset": "NONE"}
                else:
                    result = {"reasoning_path": "关键词推断 → 中性观望", "sentiment_score": 0.05, "suggested_action": "HOLD", "reasoning": "关键词推断:观望", "market_category": "OTHER", "target_asset": "NONE"}

        return result

    # ==================================================================
    # Main call flow
    # ==================================================================
    use_json_mode = model_cfg.get("json_mode", True)

    # First attempt — with configured json_mode
    raw_text = _do_api_call(use_json_mode)
    result = _parse_result(raw_text)

    # Retry: if strict JSON mode fell through to keyword heuristics, try prompt mode
    if (use_json_mode
            and model_cfg.get("label") != "Doubao"
            and result.get("reasoning_path", "").startswith("关键词推断")
            and len(raw_text) > 10):
        print(f"  [{model_cfg['label']}] json_mode=True → keyword, retrying prompt-mode...", flush=True)
        try:
            raw_text2 = _do_api_call(False)
            result2 = _parse_result(raw_text2)
            if not result2.get("reasoning_path", "").startswith("关键词推断"):
                result = result2
                print(f"  [{model_cfg['label']}] retry OK (prompt-JSON)", flush=True)
        except Exception:
            pass  # Keep original keyword result if retry fails

    # ── Doubao fast path: simplified response, no scoring or CoT ──
    if model_cfg.get("label") == "Doubao":
        action = str(result.get("suggested_action", "HOLD")).upper()
        if action not in ("BUY", "SELL", "HOLD"):
            action = "HOLD"
        direct_reasoning = str(result.get("direct_reasoning", "")).strip()[:80]
        if not direct_reasoning:
            direct_reasoning = f"{action} 直觉判断"
        print(f"  [{model_cfg['label']}] {action} | {direct_reasoning}")
        return {
            "sentiment_score": 0.0,
            "suggested_action": action,
            "reasoning": direct_reasoning,
            "translated_title": direct_reasoning[:40],  # 使用 reasoning 的前 40 字符作为显示标题
            "market_category": "OTHER",
            "target_asset": "NONE",
            "reasoning_path": "",
            "model_label": model_cfg["label"],
            "model_id": model_cfg["id"],
        }

    # Validate & normalise fields
    score = float(result.get("sentiment_score", 0))
    score = max(-1.0, min(1.0, score))
    # Safety net: never store exactly 0.0 — it's useless for aggregation
    if abs(score) < 0.001:
        score = 0.05  # tiny bullish bias, better than zero
    action = str(result.get("suggested_action", "HOLD")).upper()
    if action not in ("BUY", "SELL", "HOLD"):
        action = "HOLD"
    # Consistency: model may output HOLD with strong score — override
    if score > 0.3 and action == "HOLD":
        action = "BUY"
    elif score < -0.3 and action == "HOLD":
        action = "SELL"
    reasoning = str(result.get("reasoning", "")).strip()[:80]
    # Empty reasoning fallback
    if not reasoning:
        reasoning = f"{action}信号,得分{score:+.2f}"

    # ── Display title: 从 reasoning 提取简短中文标题（用于前端显示） ──
    # 注意：前置翻译拦截器已确保输入为中文，这里仅做显示用途的截取
    display_title = reasoning[:40]

    market_category = str(result.get("market_category", "OTHER")).upper().strip()
    if market_category not in ("CRYPTO", "GOLD", "OIL", "MACRO", "OTHER"):
        market_category = "OTHER"
    target_asset = str(result.get("target_asset", "NONE")).upper().strip()[:20]
    if not target_asset:
        target_asset = "NONE"

    # Extract reasoning_path — stored in DB & pushed to Feishu
    reasoning_path = str(result.get("reasoning_path", "")).strip()[:2000]
    if reasoning_path:
        print(f"  [{model_cfg['label']}] 推导链: {reasoning_path[:120]}")
    elif news_content:
        # Fallback: if model didn't provide CoT, show truncated news as debug
        short = news_content[:60].replace('\n', ' ')
        print(f"  [{model_cfg['label']}] (无CoT) 新闻: {short}")

    return {
        "sentiment_score": score,
        "suggested_action": action,
        "reasoning": reasoning,
        "translated_title": display_title,  # 使用 reasoning 的前 40 字符作为显示标题
        "market_category": market_category,
        "target_asset": target_asset,
        "reasoning_path": reasoning_path,
        "model_label": model_cfg["label"],
        "model_id": model_cfg["id"],
    }




# ---------------------------------------------------------------------------
# Task B — Concurrent Batch AI Worker
# ---------------------------------------------------------------------------

BATCH_SIZE = 10


async def _process_single(
    news_row: sqlite3.Row,
    model_cfg: Dict[str, str],
    loop: asyncio.AbstractEventLoop,
) -> Dict[str, Any]:
    """
    Process a single raw_news row through ONE model's LLM pipeline (Kimi K3).

    单模型模式：Kimi K3 使用 45 秒超时（在 _call_llm_sync 内部 urllib timeout=45s）。
    信号量控制并发上限，避免突发新闻潮打爆 OpenRouter。

    Returns:
      {"news_id": int, "pre_ts": str, "model_label": str,
       "result": dict | None, "error": str | None}
    """
    news_id = news_row["id"]
    content = re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', news_row["content"])
    pre_ts = news_row["timestamp"]

    try:
        async with _LLM_SEMAPHORE:
            llm_result = await loop.run_in_executor(
                None, _call_llm_sync, content, model_cfg,
            )
        return {
            "news_id": news_id,
            "pre_ts": pre_ts,
            "model_label": model_cfg["label"],
            "result": llm_result,
            "error": None,
        }
    except Exception as exc:
        # 安全 Fallback：任何异常都返回 HOLD 对象
        fallback_result = {
            "sentiment_score": 0.05,
            "suggested_action": "HOLD",
            "reasoning": f"{model_cfg['label']} 异常降级: {type(exc).__name__}",
            "market_category": "OTHER",
            "target_asset": "NONE",
            "reasoning_path": "",
            "model_label": model_cfg["label"],
            "model_id": model_cfg["id"],
        }
        return {
            "news_id": news_id,
            "pre_ts": pre_ts,
            "model_label": model_cfg["label"],
            "result": fallback_result,
            "error": f"{type(exc).__name__}: {str(exc)[:150]}",
        }


async def send_feishu_alert(news_text: str, action: str, score: float, reason: str) -> None:
    """
    Push a trading signal card to Feishu bot.

    Color logic: BUY/LONG → green, SELL/SHORT → red, else grey.
    Uses stdlib urllib in an executor thread — no aiohttp needed.
    Failures are silently swallowed (fire-and-forget).
    """
    FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/473eaf03-e315-4ec6-9df0-e782629f0289"

    action_upper = action.upper()
    if "BUY" in action_upper or "LONG" in action_upper:
        color = "green"
    elif "SELL" in action_upper or "SHORT" in action_upper:
        color = "red"
    else:
        color = "grey"

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": "[Trident 交易信号]"},
                "template": color,
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"**新闻原文**: {news_text}\n\n**方向**: {action} | **得分**: {score}\n\n**AI 逻辑**: {reason}",
                }
            ],
        },
    }

    def _post_sync() -> None:
        try:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                FEISHU_WEBHOOK,
                data=data,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            resp = urllib.request.urlopen(req, timeout=10)
            body = resp.read().decode("utf-8", errors="replace")
            print(f"[{_now()}] [FEISHU] HTTP {resp.status} — {body[:200]}")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"[{_now()}] [FEISHU] HTTP {e.code} — {body[:200]}")
        except Exception as e:
            print(f"[{_now()}] [FEISHU] ERROR: {type(e).__name__}: {e}")

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _post_sync)


# ---------------------------------------------------------------------------
# Active-trade aggregation helpers
# ---------------------------------------------------------------------------

_AGG_WINDOW_HOURS = 1          # how long a parent stays "active" (tight window to avoid over-clustering)
_AGG_MIN_SCORE    = 0.25       # only aggregate signals with |score| >= this


def _find_active_parent(
    conn: sqlite3.Connection,
    category: str,
    asset: str,
    action: str,
) -> int | None:
    """
    Look for an existing parent event (parent_id IS NULL, BUY or SELL)
    with the same market_category, target_asset, and suggested_action
    created within the aggregation window.

    Returns the parent's ai_decisions.id, or None if no match.
    """
    row = conn.execute(
        """
        SELECT id, child_count
        FROM ai_decisions
        WHERE parent_id IS NULL
          AND market_category = ?
          AND target_asset    = ?
          AND suggested_action = ?
          AND suggested_action IN ('BUY', 'SELL')
          AND created_at > datetime('now', ?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (category, asset, action, f"-{_AGG_WINDOW_HOURS} hours"),
    ).fetchone()
    return row[0] if row else None


def _bump_parent_score(
    conn: sqlite3.Connection,
    parent_id: int,
    child_score: float,
    child_reasoning: str,
) -> bool:
    """
    If the child's score is more extreme than the parent's, update the
    parent's sentiment_score and prepend a note to the reasoning field.
    Returns True if the score was bumped.
    """
    row = conn.execute(
        "SELECT sentiment_score FROM ai_decisions WHERE id = ?",
        (parent_id,),
    ).fetchone()
    if not row:
        return False

    parent_score = row[0] or 0.0

    # "More extreme" = further from zero in the same direction
    if abs(child_score) > abs(parent_score) and (child_score * parent_score >= 0):
        conn.execute(
            "UPDATE ai_decisions SET sentiment_score = ? WHERE id = ?",
            (round(child_score, 4), parent_id),
        )
        return True
    return False



async def ai_worker(loop: asyncio.AbstractEventLoop) -> None:
    """
    Concurrent batch processor.

    Every 1 second:
      1) Atomically claim up to BATCH_SIZE PENDING rows (BEGIN IMMEDIATE)
      2) Fire all LLM calls concurrently via asyncio.gather
      3) Persist successes (-> DONE) and failures (-> FAILED) in one transaction.

    Head-of-line blocking is eliminated: 10 items complete in ~2-3 s
    instead of 10 * 2 s = 20 s serial.
    """

    idle_ticks = 0  # heartbeat counter when no PENDING data

    while True:
        await asyncio.sleep(1)

        # ==================================================================
        # Phase 1 - Batch atomic claim
        # ==================================================================
        def _batch_claim() -> List[sqlite3.Row]:
            conn = _open_db()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                rows = conn.execute(
                    "SELECT * FROM raw_news WHERE status = 'PENDING'"
                    " ORDER BY id ASC LIMIT ?",
                    (BATCH_SIZE,),
                ).fetchall()
                if not rows:
                    conn.rollback()
                    return []

                ids = [r["id"] for r in rows]
                placeholders = ",".join("?" * len(ids))
                conn.execute(
                    f"UPDATE raw_news SET status = 'PROCESSING'"
                    f" WHERE id IN ({placeholders})",
                    ids,
                )
                conn.commit()
                return rows
            except sqlite3.OperationalError:
                conn.rollback()
                return []
            finally:
                conn.close()

        batch = await loop.run_in_executor(None, _batch_claim)
        if not batch:
            idle_ticks += 1
            if idle_ticks % 30 == 1:
                print(f"[{_now()}] [AI] idle ({idle_ticks}s)")
            continue

        idle_ticks = 0  # reset heartbeat on activity

        batch_ids = [r["id"] for r in batch]
        # Map news_id → content for downstream use (Feishu alerts etc.)
        content_map: Dict[int, str] = {}
        for row in batch:
            c = row["content"]
            # Strip [hash:xxx] prefix for cleaner display
            c = re.sub(r'\[hash:[a-zA-Z0-9]+\]\s*', '', c)
            content_map[row["id"]] = c
        print(
            f"\n[{_now()}] [AI] Claimed {len(batch)} items: {batch_ids}"
        )

        # ==================================================================
        # Phase 2 - Concurrent LLM analysis (news × models)
        # ==================================================================
        batch_start = _ts()
        tasks = [
            _process_single(row, model_cfg, loop)
            for row in batch
            for model_cfg in MODELS
        ]
        results: List[Dict[str, Any]] = await asyncio.gather(
            *tasks, return_exceptions=True
        )
        batch_end = _ts()

        # Separate successes from failures
        successes: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []

        for r in results:
            if isinstance(r, Exception):
                failures.append({
                    "news_id": -1, "pre_ts": "?", "model_label": "?",
                    "error": f"gather:{type(r).__name__}",
                })
                continue
            if r["error"] is not None:
                failures.append(r)
            else:
                successes.append(r)

        # ==================================================================
        # Phase 3 - Batch persist (model label in reasoning prefix)
        # ==================================================================
        def _batch_persist():
            """
            Two-phase merge:
              Phase 3a — Kimi K3 results: INSERT ai_decisions (aggregation, entry price).
              Phase 3b — Doubao results: UPDATE the matching Kimi K3 row with
                         doubao_action / doubao_reasoning.
            Both phases complete inside ONE transaction — SSE consumers see
            fully-populated rows with no race window where Doubao data is missing.
            """
            conn = _open_db()
            written: List[Dict[str, Any]] = []
            done_news_ids: set[int] = set()
            parent_cache: Dict[str, int | None] = {}
            # news_id → decision_id  mapping for Doubao UPDATE pass
            kimi_decision_ids: Dict[int, int] = {}
            try:
                # ============================================================
                # Phase 3a — Kimi K3 (primary) INSERT — aggregation + tracking
                # ============================================================
                kimi_results = [s for s in successes if s["model_label"] == "Kimi K3"]

                for s in kimi_results:
                    res = s["result"]
                    label = s["model_label"]
                    nid = s["news_id"]
                    done_news_ids.add(nid)
                    tagged_reason = f"[{label}] {res['reasoning']}"
                    score = res["sentiment_score"]
                    action = res["suggested_action"]

                    # VIP score boost — multiply |score| by 1.25 when VIP matched
                    news_text_for_vip = content_map.get(nid, "")
                    vip_tag, vip_name = _detect_vip(news_text_for_vip)
                    if vip_tag and action in ("BUY", "SELL"):
                        boosted = round(score * VIP_SCORE_BOOST, 4)
                        if abs(boosted) <= 1.0:
                            score = boosted
                    category = res.get("market_category", "OTHER")
                    asset = res.get("target_asset", "NONE")

                    # ── Aggregation: bundle into parent if same asset+direction ──
                    parent_id: int | None = None
                    agg_key = ""
                    if action in ("BUY", "SELL") and abs(score) >= _AGG_MIN_SCORE:
                        agg_key = f"{category}|{asset}|{action}"
                        if agg_key in parent_cache:
                            parent_id = parent_cache[agg_key]
                        else:
                            parent_id = _find_active_parent(
                                conn, category, asset, action
                            )
                            parent_cache[agg_key] = parent_id

                    cur = conn.execute(
                        """
                        INSERT INTO ai_decisions
                          (news_id, sentiment_score, suggested_action,
                           reasoning, created_at, market_category, target_asset,
                           parent_id, child_count, aggregation_key, reasoning_path, vip_tag)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?);
                        """,
                        (
                            nid,
                            score,
                            action,
                            tagged_reason,
                            batch_end,
                            category,
                            asset,
                            parent_id,
                            agg_key if parent_id is None else "",
                            res.get("reasoning_path", ""),
                            vip_tag,
                        ),
                    )
                    decision_id = cur.lastrowid
                    kimi_decision_ids[nid] = decision_id

                    # Capture entry price for new parent signals (BUY/SELL only)
                    entry_price_val: Optional[float] = None
                    now_ts = _ts()
                    if parent_id is None and action in ("BUY", "SELL"):
                        entry_price_val = _get_current_price(asset)
                        if entry_price_val is not None:
                            conn.execute(
                                "UPDATE ai_decisions SET entry_price = ?, entry_time = ? WHERE id = ?",
                                (entry_price_val, now_ts, decision_id),
                            )

                    if parent_id is not None:
                        # Each child gets its OWN independent entry price (not inherited)
                        if action in ("BUY", "SELL"):
                            child_entry = _get_current_price(asset)
                            if child_entry is not None:
                                conn.execute(
                                    "UPDATE ai_decisions SET entry_price = ?, entry_time = ? WHERE id = ?",
                                    (child_entry, now_ts, decision_id),
                                )
                        # Backfill parent's entry_price if missing (pre-feature data)
                        parent_row = conn.execute(
                            "SELECT entry_price FROM ai_decisions WHERE id = ?",
                            (parent_id,),
                        ).fetchone()
                        if (not parent_row or parent_row["entry_price"] is None) and action in ("BUY", "SELL"):
                            parent_backfill = _get_current_price(asset)
                            if parent_backfill is not None:
                                conn.execute(
                                    "UPDATE ai_decisions SET entry_price = ?, entry_time = ? WHERE id = ?",
                                    (parent_backfill, now_ts, parent_id),
                                )
                        conn.execute(
                            "UPDATE ai_decisions SET child_count = child_count + 1"
                            " WHERE id = ?",
                            (parent_id,),
                        )
                        _bump_parent_score(conn, parent_id, score, tagged_reason)

                        # ── Consensus: count cluster density within 30 min ──
                        cluster_count = conn.execute(
                            """
                            SELECT COUNT(*) FROM ai_decisions
                            WHERE (parent_id = ? OR id = ?)
                              AND created_at > datetime('now', '-30 minutes', 'localtime')
                            """,
                            (parent_id, parent_id),
                        ).fetchone()[0]
                        conn.execute(
                            """
                            UPDATE ai_decisions SET cluster_size = ?
                            WHERE (parent_id = ? OR id = ?)
                              AND created_at > datetime('now', '-30 minutes', 'localtime')
                            """,
                            (cluster_count, parent_id, parent_id),
                        )
                    else:
                        conn.execute(
                            "UPDATE ai_decisions SET aggregation_key = ? WHERE id = ?",
                            (agg_key, decision_id),
                        )

                    written.append({
                        "decision_id": decision_id,
                        "news_id": nid,
                        "model_label": label,
                        "pre_ts": s["pre_ts"],
                        "result": res,
                        "parent_id": parent_id,
                    })

                # ============================================================
                # Phase 3b — Doubao UPDATE — merge into existing Kimi K3 rows
                # ============================================================
                doubao_results = [s for s in successes if s["model_label"] == "Doubao"]
                for d in doubao_results:
                    nid = d["news_id"]
                    res = d["result"]
                    if nid in kimi_decision_ids:
                        conn.execute(
                            "UPDATE ai_decisions SET doubao_action = ?, doubao_reasoning = ? WHERE id = ?",
                            (res["suggested_action"], res["reasoning"], kimi_decision_ids[nid]),
                        )
                        # Append Doubao info to the written record for terminal output
                        for w in written:
                            if w["news_id"] == nid:
                                w["doubao"] = {"action": res["suggested_action"], "reasoning": res["reasoning"]}
                                break
                    else:
                        # Kimi K3 failed for this news_id — Doubao has nothing to merge into.
                        # We still mark the raw_news row as completed for Kimi K3.
                        pass

                # ============================================================
                # Mark DONE for all news_ids that produced a Kimi K3 row
                # ============================================================
                for nid in done_news_ids:
                    conn.execute(
                        "UPDATE raw_news SET status = 'DONE'"
                        " WHERE id = ? AND status = 'PROCESSING';",
                        (nid,),
                    )
                for f in failures:
                    nid = f["news_id"]
                    if nid > 0 and nid not in done_news_ids:
                        conn.execute(
                            "UPDATE raw_news SET status = 'FAILED'"
                            " WHERE id = ? AND status = 'PROCESSING';",
                            (nid,),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            return written

        # 调用 _batch_persist，如果数据库锁定则重试
        max_retries = 3
        retry_delay = 0.5  # 秒
        decision_infos = []
        for attempt in range(max_retries):
            try:
                decision_infos = _batch_persist()
                break  # 成功则跳出重试循环
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    print(f"[{_now()}] [AI] 数据库锁定，第 {attempt + 1} 次重试...")
                    await asyncio.sleep(retry_delay * (attempt + 1))  # 指数退避
                else:
                    raise  # 重试次数用完或其他错误，抛出异常

        # ==================================================================
        # Phase 4 - Print comparison & send Feishu (with Chinese translation)
        # ==================================================================
        # Group decisions by news_id
        by_news: Dict[int, List[Dict[str, Any]]] = {}
        for d in decision_infos:
            by_news.setdefault(d["news_id"], []).append(d)

        for nid, group in by_news.items():
            # 优先使用真正的新闻原文，而不是模型的 translated_title
            # translated_title 实际上是 Kimi K3 的推理结论，不应该作为新闻原文显示
            original_news = content_map.get(nid, f"News #{nid}")
            display_title = original_news
            print(f"\n  ┌─ News #{nid}: {display_title[:70]}")
            # Collect for Feishu card (one card per news item)
            feishu_lines: List[str] = []
            consensus: Dict[str, int] = {}
            for d2 in group:
                res2 = d2["result"]
                label = d2["model_label"]
                action2 = res2["suggested_action"]
                score2 = res2["sentiment_score"]
                # 飞书推送使用完整推导链，回退到短结论
                model_reason = res2.get("reasoning_path") or res2["reasoning"]
                print(
                    f"  │ [{label:8s}] {action2:4s} {score2:+.3f} | {model_reason[:100]}"
                )
                consensus[action2] = consensus.get(action2, 0) + 1
                feishu_lines.append(
                    f"**{label}**: {action2} ({score2:+.3f}) — {model_reason}"
                )
                # Doubao secondary verification — if present, append to Feishu card
                doubao = d2.get("doubao")
                if doubao:
                    db_action = doubao["action"]
                    db_reason = doubao["reasoning"]
                    print(
                        f"  │ [Doubao  ] {db_action:4s}       | {db_reason}"
                    )
                    consensus[db_action] = consensus.get(db_action, 0) + 1
                    feishu_lines.append(
                        f"**Doubao**: {db_action} — {db_reason}"
                    )

            # Consensus line
            parts = [f"{v}×{k}" for k, v in sorted(consensus.items(), key=lambda x: -x[1])]
            consensus_str = " | ".join(parts)
            print(f"  └─ Consensus: {consensus_str}")
            # Fire-and-forget Feishu comparison card (Chinese title)
            feishu_body = "\n\n".join(feishu_lines)
            asyncio.create_task(
                send_feishu_alert(
                    f"{display_title}\n\n**Consensus**: {consensus_str}",
                    consensus_str,
                    0.0,
                    feishu_body,
                )
            )

        for f in failures:
            label = f.get("model_label", "?")
            error_msg = f.get("error", "Unknown error")
            print(
                f"[{_now()}] [AI] news=#{f['news_id']} [{label}] FAILED:"
                f" {error_msg}"
            )
        # Batch summary
        try:
            s_dt = datetime.fromisoformat(batch_start)
            e_dt = datetime.fromisoformat(batch_end)
            batch_latency = (e_dt - s_dt).total_seconds()
        except Exception:
            batch_latency = -1
        total_tasks = len(batch) * len(MODELS)
        print(
            f"[{_now()}] [AI] Batch done | {len(successes)}/{total_tasks} ok"
            f" | {len(failures)} failed | wall={batch_latency:.1f}s"
        )

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def _ensure_db_exists() -> None:
    """Create the SQLite event bus if it doesn't exist, or repair a missing-table DB.

    Uses CREATE TABLE IF NOT EXISTS so it's safe to call every startup.
    No external dependency on init_db.py — fully self-contained.
    """
    need_init = not os.path.exists(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA foreign_keys=ON;")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_news (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT    NOT NULL,
                content     TEXT    NOT NULL,
                timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
                status      TEXT    NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED'))
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_decisions (                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                news_id           INTEGER NOT NULL,
                sentiment_score   REAL    NOT NULL,
                suggested_action  TEXT    NOT NULL,
                reasoning         TEXT    NOT NULL,
                created_at        TEXT    NOT NULL DEFAULT (datetime('now')),
                status            TEXT    NOT NULL DEFAULT 'UNREAD'
                    CHECK (status IN ('UNREAD', 'APPROVED', 'REJECTED', 'REVIEWED', 'AUTO_APPROVED')),
                market_category   TEXT    NOT NULL DEFAULT 'OTHER',
                target_asset      TEXT    NOT NULL DEFAULT 'NONE',
                FOREIGN KEY (news_id) REFERENCES raw_news(id) ON DELETE CASCADE
            );
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_decisions_status ON ai_decisions(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_news_status ON raw_news(status);")

        # ── Migration: add aggregation columns (safe to run every startup) ──
        for col, col_def in [
            ("market_category",  "TEXT    NOT NULL DEFAULT 'OTHER'"),
            ("target_asset",     "TEXT    NOT NULL DEFAULT 'NONE'"),
            ("parent_id",        "INTEGER DEFAULT NULL"),
            ("child_count",      "INTEGER DEFAULT 0"),
            ("aggregation_key",  "TEXT    DEFAULT ''"),
            ("cluster_size",     "INTEGER DEFAULT 1"),
        ]:
            try:
                conn.execute(f"ALTER TABLE ai_decisions ADD COLUMN {col} {col_def};")
            except sqlite3.OperationalError:
                pass  # column already exists

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ai_parent ON ai_decisions(parent_id);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ai_agg_key ON ai_decisions(aggregation_key);"
        )

        # reasoning_path column (may not exist in old DBs)
        try:
            conn.execute(
                "ALTER TABLE ai_decisions ADD COLUMN reasoning_path TEXT DEFAULT '';"
            )
        except sqlite3.OperationalError:
            pass

        # vip_tag column
        try:
            conn.execute(
                "ALTER TABLE ai_decisions ADD COLUMN vip_tag TEXT DEFAULT '';"
            )
        except sqlite3.OperationalError:
            pass

        # ── Doubao secondary verification columns ──
        for col, col_def in [
            ("doubao_action",    "TEXT DEFAULT 'HOLD'"),
            ("doubao_reasoning", "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE ai_decisions ADD COLUMN {col} {col_def};")
            except sqlite3.OperationalError:
                pass

        # ── Extra Models Consensus (四模并发扩展) ──
        # 新增字段：存储额外四个模型（DeepSeek, Gemini, Grok, ChatGPT）的投票结果
        # JSON 格式：{"DeepSeek": {"action": "BUY", "score": 0.5, "reasoning": "..."}, ...}
        try:
            conn.execute(
                "ALTER TABLE ai_decisions ADD COLUMN extra_models_consensus TEXT DEFAULT '';"
            )
        except sqlite3.OperationalError:
            pass  # column already exists

        # ── Forward tracking columns (2h simulated trade verification) ──
        for col, col_def in [
            ("entry_price", "REAL DEFAULT NULL"),
            ("exit_price",  "REAL DEFAULT NULL"),
            ("max_price",   "REAL DEFAULT NULL"),
            ("min_price",   "REAL DEFAULT NULL"),
            ("is_correct",  "TEXT DEFAULT ''"),
            ("settled",     "INTEGER DEFAULT 0"),
            ("entry_time",  "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE ai_decisions ADD COLUMN {col} {col_def};")
            except sqlite3.OperationalError:
                pass

        # ── Impact verification: extreme-price timestamps ──
        for col, col_def in [
            ("max_price_time", "INTEGER DEFAULT 0"),
            ("min_price_time", "INTEGER DEFAULT 0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE ai_decisions ADD COLUMN {col} {col_def};")
            except sqlite3.OperationalError:
                pass

        conn.commit()
    finally:
        conn.close()

    tag = "CREATED" if need_init else "VERIFIED"
    print(f"[ENGINE] DB {tag}: {DB_PATH}")


# ---------------------------------------------------------------------------
# Pre-Translator Middleware (前置翻译拦截器)
# ---------------------------------------------------------------------------
# 在数据进入推演池前翻译英文标题，确保 6 模型推演时 100% 使用中文
# ---------------------------------------------------------------------------

def _is_english_text(text: str) -> bool:
    """
    激进英文检测 - 拦截一切可能的英文标题。
    多条件 OR 逻辑：满足任一条件即判定为英文。
    """
    t = text.strip()
    if not t or len(t) < 5:
        return False

    # ── 条件 1: 以英文关键词开头（如 BREAKING, NEWS, ALERT 等） ──
    english_prefixes = (
        "BREAKING", "NEWS", "ALERT", "UPDATE", "URGENT", "JUST IN",
        "FLASH", "Ticker", "Report", "Market", "Stock", "Crypto",
        "Fed", "SEC", "FOMC", "OPEC", "API", "ISM", "NFP", "CPI",
        "GDP", "PMI", "PCE", "NBER", "ECB", "BOE", "BOJ", "SNB"
    )
    words = re.split(r'[\s:，-]+', t.upper())
    if words and words[0] in english_prefixes:
        return True

    # ── 条件 2: 英文字母占比超过 30%（激进阈值） ──
    alpha_chars = sum(1 for c in t if c.isalpha())
    if alpha_chars > 0:
        english_alpha = sum(1 for c in t if c.isalpha() and ord(c) < 128)
        english_ratio = english_alpha / alpha_chars
        if english_ratio > 0.30:  # 30% 英文字母即判定
            return True

    # ── 条件 3: 包含连续 3 个英文单词 ──
    # 英文单词：至少 2 个字母，主要由 a-z 组成
    def is_english_word(w: str) -> bool:
        if len(w) < 2:
            return False
        english_count = sum(1 for c in w if c.isalpha() and ord(c) < 128)
        return english_count >= len(w) * 0.7

    consecutive_english = 0
    for word in words:
        if is_english_word(word):
            consecutive_english += 1
            if consecutive_english >= 3:
                return True
        else:
            consecutive_english = 0

    # ── 条件 4: 包含常见英文财经关键词 ──
    financial_keywords = (
        "inflation", "recession", "interest rate", "treasury", "yield",
        "unemployment", "payroll", "retail sales", "manufacturing",
        "services", "consumer", "producer", "price", "index", "durable",
        "orders", "trade", "balance", "surplus", "deficit", "imports",
        "exports", "oil", "inventory", "crude", "production", "supply",
        "demand", "earnings", "revenue", "guidance", "dividend", "buyback",
        "ipo", "merger", "acquisition", "default", "bankruptcy", "credit",
        "rating", "downgrade", "upgrade", "outlook", "forecast", "estimate"
    )
    t_lower = t.lower()
    for keyword in financial_keywords:
            return True

    # ── 条件 5: 包含数字和英文混合（如 "S&P 500", "10Y Yield"） ──
    # 模式：数字 + 英文 或 英文 + 数字
    if re.search(r'\d+[a-zA-Z]{2,}|[a-zA-Z]{2,}\d+', t):
        # 进一步检查：如果这个模式周围没有中文，可能是英文
        # 例如 "S&P 500" 是英文，"上证3000点" 是中文
        sample_words = [w for w in words if any(c.isalpha() for c in w)]
        if sample_words:
            english_word_ratio = sum(1 for w in sample_words if is_english_word(w)) / len(sample_words)
            if english_word_ratio > 0.4:
                return True

    return False


def _translate_title_sync(original_title: str) -> str:
    """
    翻译英文标题 - 完整的 Doubao -> Gemini -> DeepSeek 兜底链。
    这是一个同步调用，在 executor 线程中运行，不阻塞 async loop。
    """
    # 翻译 prompt：极其简单，要求纯翻译结果
    translate_prompt = "请将以下英文新闻标题翻译成简体中文，只需返回翻译结果，不要任何解释或额外内容："

    # ── 构建模型候选列表（按优先级排序） ──
    model_candidates = []

    # 候选 1: Doubao（火山引擎 - 国内最快）
    if DOUBAO_API_KEY and DOUBAO_MODEL:
        model_candidates.append({
            "id": DOUBAO_MODEL,
            "label": "Doubao-Translator",
            "api_base": DOUBAO_BASE_URL,
            "api_key": DOUBAO_API_KEY,
        })

    # 候选 2: Gemini 2.5 Flash（OpenRouter - 快速备用）
    if OPENROUTER_API_KEY:
        model_candidates.append({
            "id": "google/gemini-2.5-flash",
            "label": "Gemini-Translator",
            "api_base": OPENROUTER_BASE_URL,
            "api_key": OPENROUTER_API_KEY,
        })

    # 候选 3: DeepSeek Chat（OpenRouter - 最快的大模型）
    if OPENROUTER_API_KEY:
        model_candidates.append({
            "id": "deepseek/deepseek-chat",
            "label": "DeepSeek-Translator",
            "api_base": OPENROUTER_BASE_URL,
            "api_key": OPENROUTER_API_KEY,
        })

    # ── 依次尝试每个模型，直到成功 ──
    last_error = None

    for i, model_cfg in enumerate(model_candidates):
        model_label = model_cfg["label"]
        is_last = (i == len(model_candidates) - 1)

        try:
            # ── 构建请求 ──
            payload = {
                "model": model_cfg["id"],
                "messages": [
                    {"role": "system", "content": translate_prompt},
                    {"role": "user", "content": original_title[:500]},
                ],
                "max_tokens": 256,
                "temperature": 0.1,
            }

            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                f"{model_cfg['api_base']}/chat/completions",
                data=data,
                headers={
                    "Authorization": f"Bearer {model_cfg['api_key']}",
                    "Content-Type": "application/json",
                },
            )

            ssl_ctx = ssl.create_default_context()
            try:
                ssl_ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
            except Exception:
                pass

            # ── 发起请求（超时保护：15秒） ──
            resp = urllib.request.urlopen(req, timeout=15, context=ssl_ctx)
            body = json.loads(resp.read().decode("utf-8"))

            # ── 解析响应 ──
            if "choices" not in body or not body["choices"]:
                raise ValueError(f"响应格式异常: {body}")

            translated = body["choices"][0]["message"]["content"].strip()

            # 清理可能的 markdown 标记
            translated = re.sub(r'^```\w*\s*', '', translated)
            translated = re.sub(r'\s*```$', '', translated)

            # ── 验证翻译结果 ──
            if not translated:
                raise ValueError("翻译结果为空")

            if not _contains_chinese(translated):
                raise ValueError(f"翻译结果不包含中文: {translated[:100]}")

            # ── 翻译成功！ ──
            print(f"  [PRE-TRANSLATE] ✅ 翻译成功 ({model_label}): {translated[:100]}")
            return translated[:200]

        except Exception as exc:
            last_error = exc
            error_type = type(exc).__name__
            error_msg = str(exc)[:150]

            if is_last:
                # 最后一个模型也失败了 - 致命错误
                print(f"  [PRE-TRANSLATE] 🚨 翻译 API 全面崩溃！")
                print(f"  [PRE-TRANSLATE] 🚨 最后失败模型: {model_label}")
                print(f"  [PRE-TRANSLATE] 🚨 异常类型: {error_type}")
                print(f"  [PRE-TRANSLATE] 🚨 异常信息: {error_msg}")
                print(f"  [PRE-TRANSLATE] 🚨 原文标题: {original_title[:150]}")
                print(f"  [PRE-TRANSLATE] 🚨 ⚠️ 被迫降级为英文原文进入数据库！")
            else:
                # 中间模型失败 - 尝试下一个
                print(f"  [PRE-TRANSLATE] ⚠️ {model_label} 失败: {error_type} - 切换备选模型...")

    # ── 所有模型都失败，返回原文 ──
    return original_title[:500]


async def _translate_if_english(text: str, loop: asyncio.AbstractEventLoop) -> str:
    """
    异步包装器：检测英文并翻译。
    在 executor 线程中运行同步翻译调用，避免阻塞 async loop。
    """
    if not _is_english_text(text):
        return text

    # ── 高亮日志：拦截到英文新闻 ──
    print(f"  [PRE-TRANSLATE] 🔍 拦截到英文新闻，正在调用翻译 API...")
    print(f"  [PRE-TRANSLATE] 📖 原文: {text[:150]}")

    # 在线程池中执行同步翻译（完整兜底链）
    translated = await loop.run_in_executor(None, _translate_title_sync, text)

    # ── 翻译结果验证 ──
    if translated == text:
        print(f"  [PRE-TRANSLATE] ⚠️ 翻译失败，使用原文（前端将显示英文）")
    else:
        print(f"  [PRE-TRANSLATE] ✅ 最终结果: {translated[:100]}")

    return translated


# ---------------------------------------------------------------------------
# Tree News / Telegram Webhook Listener
# ---------------------------------------------------------------------------
# Accepts POST JSON: {"text": "...", "source": "tree_news" | "telegram" | ...}
# Allows third-party fast-news bots to inject into the Trident pipeline.
# ---------------------------------------------------------------------------

_TREE_NEWS_PORT = int(os.getenv("TREE_NEWS_PORT", "9000"))


async def _tree_news_handler(reader, writer) -> None:
    """Minimal async HTTP POST handler - with top-level exception guard."""
    loop = asyncio.get_running_loop()
    text = ""
    source = "tree_news"

    def _send_response(status_code, status_text, body):
        payload = body.encode("utf-8")
        header = (
            f"HTTP/1.1 {status_code} {status_text}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(header + payload)

    try:
        try:
            raw = await asyncio.wait_for(reader.read(65536), timeout=10.0)
        except asyncio.TimeoutError:
            return
        if not raw:
            return

        decoded = raw.decode("utf-8", errors="replace")
        if "POST" not in decoded.split("\r\n")[0]:
            _send_response(405, "Method Not Allowed", json.dumps({"ok": False, "error": "POST only"}))
            await writer.drain()
            return

        body_start = decoded.find("\r\n\r\n")
        if body_start < 0:
            _send_response(400, "Bad Request", json.dumps({"ok": False, "error": "no body"}))
            await writer.drain()
            return

        body_text = decoded[body_start + 4:].strip()
        try:
            data = json.loads(body_text)
        except json.JSONDecodeError:
            _send_response(400, "Bad Request", json.dumps({"ok": False, "error": "invalid JSON"}))
            await writer.drain()
            return

        text = str(data.get("text", data.get("content", data.get("message", "")))).strip()
        source = str(data.get("source", "tree_news")).strip()[:64]

        # ── Pre-filter: hard blacklist + junk detection ──
        if _is_content_junk(text):
            print(f"\n  [{_now()}] [Filter] Ignored junk content: {text[:80]}")
            _send_response(204, "No Content", json.dumps({"ok": True, "filtered": True, "reason": "junk"}))
            await writer.drain()
            return

        # ═════════════════════════════════════════════════════════════════════
        # PRE-TRANSLATOR INTERCEPTOR (前置翻译拦截器)
        # ═════════════════════════════════════════════════════════════════════
        # 检测英文新闻并立即翻译，确保后续所有处理（数据库 INSERT + 6 模型推演）
        # 统一使用中文标题。这是关注点分离的关键：翻译逻辑完全前置，模型专注推演。
        # ═════════════════════════════════════════════════════════════════════
        original_text = text
        text = await _translate_if_english(text, loop)
        if text != original_text:
            print(f"  [{_now()}] [TRANSLATE] English → Chinese: {original_text[:40]} → {text[:40]}")

        vip_tag, vip_name = _detect_vip(text)
        h = hashlib.sha256(text.encode()).hexdigest()[:16]

        def _webhook_insert() -> int | None:
            conn = _open_db()
            try:
                ex = conn.execute(
                    "SELECT id FROM raw_news WHERE content LIKE ? LIMIT 1",
                    (f"[hash:{h}]%",),
                ).fetchone()
                if ex:
                    return None
                source_label = f"WEB:{source}"
                if vip_tag:
                    source_label = f"{source_label} {vip_tag}"
                ts = _ts()
                cur = conn.execute(
                    "INSERT INTO raw_news (source, content, timestamp) VALUES (?, ?, ?);",
                    (source_label, f"[hash:{h}] {text[:500]}", ts),
                )
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()

        rowid = await loop.run_in_executor(None, _webhook_insert)
        if rowid is not None:
            vip_info = f" {vip_tag}" if vip_tag else ""
            print(f"\n  [{_now()}] WEBHOOK #{rowid} [{source}]{vip_info} | {text[:80]}")

        _send_response(200, "OK", json.dumps({"ok": True, "rowid": rowid, "vip": vip_tag or ""}))
        await writer.drain()

    except Exception as exc:
        print(f"\n  [{_now()}] WEBHOOK ERROR {type(exc).__name__}: {exc} | text={text[:80]}")
        try:
            _send_response(500, "Internal Server Error", json.dumps({"ok": False, "error": str(exc)[:200]}))
            await writer.drain()
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


_FORWARD_DURATION_HOURS = 2


async def forward_tracker() -> None:
    """2-hour simulated trade verification. Tracks max/min, settles with WIN/LOSS verdict."""
    print("[TRACKER] Forward tracker started - 2h verification window")
    loop = asyncio.get_running_loop()

    while True:
        try:

            def _track_cycle():
                updates = []
                conn = _open_db()
                try:
                    rows = conn.execute(
                        "SELECT id, suggested_action, target_asset, entry_price,"
                        " max_price, min_price, max_price_time, min_price_time, entry_time, settled"
                        " FROM ai_decisions"
                        " WHERE settled = 0 AND entry_price IS NOT NULL AND entry_time != ''"
                    ).fetchall()

                    # Asset-specific impact thresholds for verdict ruling
                    IMPACT_THRESHOLD = {"BTC": 2.0, "ETH": 2.0, "SOL": 2.0,
                                        "XAU": 1.0, "GOLD": 1.0,
                                        "WTI": 1.5}

                    for row in rows:
                        eid = row["id"]
                        action = (row["suggested_action"] or "").upper()
                        asset_raw = (row["target_asset"] or "NONE").upper()
                        entry = row["entry_price"]
                        cur_max = row["max_price"]
                        cur_min = row["min_price"]
                        max_ptime = row["max_price_time"] or 0
                        min_ptime = row["min_price_time"] or 0
                        entry_ts = row["entry_time"]

                        try:
                            et = datetime.fromisoformat(entry_ts)
                        except (ValueError, TypeError):
                            continue

                        elapsed = datetime.now(TZ_SHANGHAI) - et
                        price = _get_current_price(asset_raw)  # used for tracking only

                        if elapsed.total_seconds() >= _FORWARD_DURATION_HOURS * 3600:
                            exit_p = price if price is not None else entry
                            entry_unix = int(et.timestamp())

                            # ── Impact metrics (defensive against zero entry) ──
                            mfe_pct: float = 0.0
                            mae_pct: float = 0.0
                            mfe_time_mins: float = 0.0

                            if entry and entry > 0:
                                if action == "BUY":
                                    if cur_max and cur_max > 0:
                                        mfe_pct = (cur_max - entry) / entry * 100
                                    if cur_min and cur_min > 0:
                                        mae_pct = (entry - cur_min) / entry * 100
                                    if max_ptime > 0:
                                        mfe_time_mins = (max_ptime - entry_unix) / 60
                                elif action == "SELL":
                                    if cur_min and cur_min > 0:
                                        mfe_pct = (entry - cur_min) / entry * 100
                                    if cur_max and cur_max > 0:
                                        mae_pct = (entry - cur_max) / entry * 100
                                    if min_ptime > 0:
                                        mfe_time_mins = (min_ptime - entry_unix) / 60

                            # ── Get asset-specific threshold ──
                            threshold = IMPACT_THRESHOLD.get(asset_raw, 1.0)

                            # ── 3D empirical verdict decision tree ──
                            if mfe_pct < threshold and mae_pct < threshold:
                                # Condition A: neither side exceeded threshold
                                verdict = "HOLD"    # NO_IMPACT — market did not break window
                            elif mfe_pct >= threshold and mfe_time_mins <= 45 and mfe_pct > mae_pct:
                                # Condition B: favourable excursion hit hard & fast
                                verdict = "WIN"     # CORRECT — direction right, rapid reaction
                            elif mae_pct >= threshold and mae_pct > mfe_pct:
                                # Condition C: adverse excursion dominated
                                verdict = "LOSS"    # INCORRECT — direction wrong
                            else:
                                # Catch-all: e.g. MFE hit but too slow (>45min), or tie
                                verdict = "HOLD"    # NOT_DRIVEN — late/sector move, not news-driven

                            conn.execute(
                                "UPDATE ai_decisions SET exit_price = ?, is_correct = ?, settled = 1 WHERE id = ?",
                                (round(exit_p, 2), verdict, eid),
                            )
                            conn.commit()
                            updates.append({
                                "id": eid, "asset": asset_raw, "action": action,
                                "entry": entry, "exit": round(exit_p, 2), "verdict": verdict,
                            })
                        elif price is not None:
                            now_unix = int(time.time())
                            new_max = round(max(cur_max or price, price), 2)
                            new_min = round(min(cur_min or price, price), 2)
                            sets: List[str] = []
                            params: list = []
                            if new_max != cur_max:
                                sets.append("max_price = ?")
                                params.append(new_max)
                                sets.append("max_price_time = ?")
                                params.append(now_unix)
                            if new_min != cur_min:
                                sets.append("min_price = ?")
                                params.append(new_min)
                                sets.append("min_price_time = ?")
                                params.append(now_unix)
                            if sets:
                                params.append(eid)
                                conn.execute(
                                    f"UPDATE ai_decisions SET {', '.join(sets)} WHERE id = ?",
                                    params,
                                )
                    conn.commit()

                finally:
                    conn.close()
                return updates

            settled = await loop.run_in_executor(None, _track_cycle)
            for s in settled:
                print(
                    f"  [{_now()}] SETTLED #{s['id']} {s['action']} {s['asset']}"
                                 f" | entry={s['entry']} exit={s['exit']} -> {s['verdict']}"
                )

        except Exception as e:
            print(f"[TRACKER] ERROR: {type(e).__name__}: {e}")

        await asyncio.sleep(30)


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


if __name__ == "__main__":
    asyncio.run(main())
