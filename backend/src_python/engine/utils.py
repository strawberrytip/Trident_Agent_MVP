"""Shared helpers — VIP detection, DB handle, timestamps, dedup hash, content hygiene.

Leaf module: depends only on config + stdlib.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime

from config import DB_PATH, TZ_SHANGHAI, VIP_KOLS


# ---------------------------------------------------------------------------
# VIP KOL monitoring — VIP_KOLS / VIP_SCORE_BOOST 定义见 config.py
# ---------------------------------------------------------------------------


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
