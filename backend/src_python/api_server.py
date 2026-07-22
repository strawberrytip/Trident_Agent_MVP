#!/usr/bin/env python3
"""
Trident Agent MVP — FastAPI Backend Server (SSE Edition)
=========================================================

Usage:
  cd backend/src_python
  python api_server.py
"""

from __future__ import annotations

import asyncio
import io
import json
import math
import os
import re
import time
import urllib.request
import urllib.error
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import aiosqlite
from pydantic import BaseModel
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "trident_event_bus.db")
from dotenv import load_dotenv
# .env 在项目根目录（backend/ 的上一层）
load_dotenv(os.path.join(os.path.dirname(BASE_DIR), ".env"))

TZ_SHANGHAI = timezone(timedelta(hours=8))

_SSE_QUEUES: List[asyncio.Queue] = []

def _now() -> str:
    return datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")

def _format_time(iso_ts):
    if not iso_ts:
        return "——"
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return dt.astimezone(TZ_SHANGHAI).strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return (iso_ts or "")[:8] or "——"

def _safe(row, key, default=None):
    """sqlite3.Row safe access — no .get() method."""
    return row[key] if key in row.keys() else default

def _row_to_event(row) -> Dict[str, Any]:
    """Map a DB row to the frontend ApiEvent shape."""
    reason = _safe(row, "reason") or ""
    reason = re.sub(r"^\[.*?\]\s*", "", reason)
    market = (_safe(row, "market_category") or "OTHER").upper()
    if market not in ("CRYPTO", "GOLD", "OIL", "MACRO", "OTHER"):
        market = "OTHER"
    # ── Parse extra_models_consensus JSON → individual model fields ──
    raw_consensus = _safe(row, "extra_models_consensus") or ""
    if isinstance(raw_consensus, str):
        raw_consensus = raw_consensus.strip()
    try:
        consensus = json.loads(raw_consensus) if raw_consensus else {}
    except (json.JSONDecodeError, TypeError):
        consensus = {}
    if not isinstance(consensus, dict):
        consensus = {}

    return {
        "id": row["id"],
        "timestamp": _format_time(_safe(row, "timestamp")),
        "ai_time": _format_time(_safe(row, "created_at")),
        "source": _safe(row, "source") or "FinancialJuice",
        "news_text": re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', (_safe(row, "news_text") or "")[:100]),
        "action": row["action"],
        "score": round(row["score"], 2) if _safe(row, "score") is not None else 0.0,
        "reason": reason[:80],
        "market_category": market,
        "target_asset": (_safe(row, "target_asset") or "NONE").upper(),
        "parent_id": _safe(row, "parent_id"),
        "child_count": _safe(row, "child_count") or 0,
        "reasoning_path": (_safe(row, "reasoning_path") or "")[:200],
        "vip_tag": _safe(row, "vip_tag") or "",
        "entry_price": _safe(row, "entry_price"),
        "exit_price": _safe(row, "exit_price"),
        "max_price": _safe(row, "max_price"),
        "min_price": _safe(row, "min_price"),
        "max_price_time": _safe(row, "max_price_time") or 0,
        "min_price_time": _safe(row, "min_price_time") or 0,
        "entry_time": _safe(row, "entry_time") or "",
        "is_correct": _safe(row, "is_correct") or "",
        "settled": _safe(row, "settled") or 0,
        "doubao_action": _safe(row, "doubao_action") or "HOLD",
        "doubao_reasoning": _safe(row, "doubao_reasoning") or "",
        "deepseek_action": (consensus.get("DeepSeek", {}) if isinstance(consensus.get("DeepSeek"), dict) else {}).get("action") or _safe(row, "deepseek_action") or "HOLD",
        "deepseek_reasoning": (consensus.get("DeepSeek", {}) if isinstance(consensus.get("DeepSeek"), dict) else {}).get("reasoning") or _safe(row, "deepseek_reasoning") or "",
        "gemini_action": (consensus.get("Gemini", {}) if isinstance(consensus.get("Gemini"), dict) else {}).get("action") or _safe(row, "gemini_action") or "HOLD",
        "gemini_reasoning": (consensus.get("Gemini", {}) if isinstance(consensus.get("Gemini"), dict) else {}).get("reasoning") or _safe(row, "gemini_reasoning") or "",
        "grok_action": (consensus.get("Grok", {}) if isinstance(consensus.get("Grok"), dict) else {}).get("action") or _safe(row, "grok_action") or "HOLD",
        "grok_reasoning": (consensus.get("Grok", {}) if isinstance(consensus.get("Grok"), dict) else {}).get("reasoning") or _safe(row, "grok_reasoning") or "",
        "chatgpt_action": (consensus.get("ChatGPT", {}) if isinstance(consensus.get("ChatGPT"), dict) else {}).get("action") or _safe(row, "chatgpt_action") or "HOLD",
        "chatgpt_reasoning": (consensus.get("ChatGPT", {}) if isinstance(consensus.get("ChatGPT"), dict) else {}).get("reasoning") or _safe(row, "chatgpt_reasoning") or "",
        "cluster_size": _safe(row, "cluster_size") or 1,
    }

# -- SSE helpers -----------------------------------------------------------

async def _broadcast_sse(data: Dict[str, Any]) -> None:
    text = json.dumps(data, ensure_ascii=False)
    dead = []
    for q in _SSE_QUEUES:
        try:
            q.put_nowait(text)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _SSE_QUEUES.remove(q)
        except ValueError:
            pass

async def _fetch_newest_id() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            cursor = await db.execute("SELECT MAX(id) FROM ai_decisions")
            row = await cursor.fetchone()
            await cursor.close()
            return row[0] if row[0] is not None else 0
        except aiosqlite.OperationalError:
            return 0

async def _fetch_rows_since(last_id: int) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                ad.id,
                rn.timestamp,
                rn.source,
                rn.content                          AS news_text,
                UPPER(ad.suggested_action)           AS action,
                ad.sentiment_score                   AS score,
                ad.reasoning                         AS reason,
                ad.market_category,
                ad.target_asset,
                ad.created_at,
                ad.parent_id,
                ad.child_count,
                ad.reasoning_path,
                ad.vip_tag,
                ad.entry_price,
                ad.exit_price,
                ad.max_price,
                ad.min_price,
                ad.max_price_time,
                ad.min_price_time,
                ad.is_correct,
                ad.settled,
                ad.doubao_action,
                ad.doubao_reasoning,
                ad.extra_models_consensus,
                ad.entry_time,
                ad.cluster_size
            FROM ai_decisions ad
            INNER JOIN raw_news rn ON rn.id = ad.news_id
            WHERE ad.id > ?
            ORDER BY ad.id ASC
            """,
            (last_id,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return [_row_to_event(r) for r in rows]


# -- Gold price helpers ----------------------------------------------------

_last_gold_price: float | None = None

class _MT5State:
    def __init__(self):
        self.ok = False
        self.init_done = False

_mt5 = _MT5State()

def _mt5_init() -> None:
    if _mt5.init_done:
        return
    _mt5.init_done = True
    try:
        import MetaTrader5 as mt5_mod
        if not mt5_mod.initialize():
            print("[MT5] initialize() returned False")
            return
        _mt5.ok = True
        print("[MT5] connected — XAUUSD + WTIUSD — tick streaming active")
    except ImportError:
        print("[MT5] MetaTrader5 package not installed")
    except Exception as e:
        print(f"[MT5] init error: {type(e).__name__}: {e}")

def _mt5_read_tick() -> float | None:
    try:
        import MetaTrader5 as mt5_mod
        tick = mt5_mod.symbol_info_tick("XAUUSD")
        if tick and tick.bid and 500 < tick.bid < 10000:
            return tick.bid
        return None
    except Exception:
        return None

def _mt5_recheck() -> None:
    _mt5.ok = False
    try:
        import MetaTrader5 as mt5_mod
        tick = mt5_mod.symbol_info_tick("XAUUSD")
        if tick and tick.bid:
            _mt5.ok = True
    except Exception:
        pass

def _fetch_sina_xau() -> float | None:
    req = urllib.request.Request(
        "https://hq.sinajs.cn/list=hf_XAU",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
    )
    resp = urllib.request.urlopen(req, timeout=8)
    text = resp.read().decode("gbk", errors="replace")
    if '="' in text:
        return float(text.split('="')[1].split(",")[0])
    return None

def _fetch_eastmoney_xau() -> float | None:
    req = urllib.request.Request(
        "https://push2.eastmoney.com/api/qt/stock/get?secid=113.USDXAU&fields=f43",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
    )
    resp = urllib.request.urlopen(req, timeout=8)
    body = json.loads(resp.read().decode("utf-8"))
    return float(body["data"]["f43"]) / 100.0

def _http_fetch_gold() -> tuple[float | None, str]:
    for name, fn in [("Sina", _fetch_sina_xau), ("EastMoney", _fetch_eastmoney_xau)]:
        try:
            p = fn()
            if p is not None and 500 < p < 10000:
                return p, name
        except Exception:
            pass
    return None, ""

async def _broadcast_gold(price: float, src: str):
    global _last_gold_price
    if _last_gold_price is None or abs(price - _last_gold_price) >= 0.01:
        _last_gold_price = price
        await _broadcast_sse({
            "type": "price_update",
            "asset": "XAU",
            "price": round(price, 2),
            "ts": time.time(),
        })

async def gold_http_watcher():
    loop = asyncio.get_running_loop()
    while True:
        try:
            price, src = await loop.run_in_executor(None, _http_fetch_gold)
            if price is not None:
                await _broadcast_gold(price, src)
        except Exception:
            pass
        await asyncio.sleep(1.0)

async def gold_mt5_watcher():
    print("[GOLD] MT5 watcher started")
    while True:
        try:
            if _mt5.ok:
                price = _mt5_read_tick()
                if price is not None and 500 < price < 10000:
                    await _broadcast_gold(price, "MT5")
                    await asyncio.sleep(0.1)
                    continue
                _mt5_recheck()
                if not _mt5.ok:
                    print("[GOLD] MT5 terminal unreachable — HTTP only")
                await asyncio.sleep(1.0)
                continue
            if not _mt5.init_done:
                _mt5_init()
            if not _mt5.ok:
                _mt5.init_done = False
                await asyncio.sleep(30)
                continue
        except Exception as e:
            print(f"[GOLD] MT5 error: {type(e).__name__}: {e}")
            await asyncio.sleep(5)


# -- WTI price helpers ----------------------------------------------------

_last_wti_price: float | None = None

def _mt5_read_wti_tick() -> float | None:
    try:
        import MetaTrader5 as mt5_mod
        tick = mt5_mod.symbol_info_tick("WTIUSD")
        if tick and tick.bid and 30 < tick.bid < 200:
            return tick.bid
        return None
    except Exception:
        return None

def _fetch_sina_wti() -> float | None:
    req = urllib.request.Request(
        "https://hq.sinajs.cn/list=hf_CL",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
    )
    resp = urllib.request.urlopen(req, timeout=8)
    text = resp.read().decode("gbk", errors="replace")
    if '="' in text:
        return float(text.split('="')[1].split(",")[0])
    return None

def _fetch_eastmoney_wti() -> float | None:
    req = urllib.request.Request(
        "https://push2.eastmoney.com/api/qt/stock/get?secid=113.USDWTI&fields=f43",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
    )
    resp = urllib.request.urlopen(req, timeout=8)
    body = json.loads(resp.read().decode("utf-8"))
    return float(body["data"]["f43"]) / 100.0

def _http_fetch_wti() -> tuple[float | None, str]:
    for name, fn in [("Sina", _fetch_sina_wti), ("EastMoney", _fetch_eastmoney_wti)]:
        try:
            p = fn()
            if p is not None and 30 < p < 200:
                return p, name
        except Exception:
            pass
    return None, ""

async def _broadcast_wti(price: float, src: str):
    global _last_wti_price
    if _last_wti_price is None or abs(price - _last_wti_price) >= 0.01:
        _last_wti_price = price
        await _broadcast_sse({
            "type": "price_update",
            "asset": "WTI",
            "price": round(price, 2),
            "ts": time.time(),
        })

async def wti_http_watcher():
    print("[WTI] HTTP watcher started (Sina -> EastMoney)")
    loop = asyncio.get_running_loop()
    while True:
        try:
            price, src = await loop.run_in_executor(None, _http_fetch_wti)
            if price is not None:
                await _broadcast_wti(price, src)
        except Exception:
            pass
        await asyncio.sleep(1.0)

async def wti_mt5_watcher():
    print("[WTI] MT5 watcher started")
    while True:
        try:
            if _mt5.ok:
                price = _mt5_read_wti_tick()
                if price is not None and 30 < price < 200:
                    await _broadcast_wti(price, "MT5")
                    await asyncio.sleep(0.1)
                    continue
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[WTI] MT5 error: {type(e).__name__}: {e}")
            await asyncio.sleep(5)


# -- DB watcher ------------------------------------------------------------

async def db_watcher() -> None:
    last_id = await _fetch_newest_id()
    while True:
        try:
            rows = await _fetch_rows_since(last_id)
            for ev in rows:
                last_id = max(last_id, ev["id"])
                await _broadcast_sse(ev)
        except Exception:
            pass
        await asyncio.sleep(1.0)


# -- Kline helpers ---------------------------------------------------------

def _mock_klines(limit: int, *, base: float, seed: int):
    import random
    result = []
    now = datetime.now(TZ_SHANGHAI)
    rng = random.Random(seed)
    jitter = base * 0.015
    for i in range(limit):
        t = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=limit - i)
        ts = int(t.timestamp())
        o = base + rng.uniform(-jitter, jitter)
        h = o + rng.uniform(0, jitter * 0.5)
        l = o - rng.uniform(0, jitter * 0.5)
        c = l + rng.uniform(0, h - l)
        result.append({
            "time": ts,
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "volume": int(rng.uniform(5000, 30000)),
        })
    return result


# -- Schema migration ------------------------------------------------------

def _migrate_schema() -> None:
    """Add any missing columns to ai_decisions. Safe to call repeatedly."""
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(DB_PATH)
    try:
        for col, col_def in [
            ("parent_id",        "INTEGER DEFAULT NULL"),
            ("child_count",      "INTEGER DEFAULT 0"),
            ("aggregation_key",  "TEXT DEFAULT ''"),
            ("reasoning_path",   "TEXT DEFAULT ''"),
            ("vip_tag",          "TEXT DEFAULT ''"),
            ("doubao_action",       "TEXT DEFAULT 'HOLD'"),
            ("doubao_reasoning",    "TEXT DEFAULT ''"),
            ("extra_models_consensus", "TEXT DEFAULT ''"),
            ("entry_price",      "REAL DEFAULT NULL"),
            ("exit_price",       "REAL DEFAULT NULL"),
            ("max_price",        "REAL DEFAULT NULL"),
            ("min_price",        "REAL DEFAULT NULL"),
            ("is_correct",       "TEXT DEFAULT ''"),
            ("settled",          "INTEGER DEFAULT 0"),
            ("entry_time",       "TEXT DEFAULT ''"),
            ("max_price_time",   "INTEGER DEFAULT 0"),
            ("min_price_time",   "INTEGER DEFAULT 0"),
            ("cluster_size",     "INTEGER DEFAULT 1"),
        ]:
            try:
                conn.execute(f"ALTER TABLE ai_decisions ADD COLUMN {col} {col_def};")
            except _sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


# -- Lifespan --------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _migrate_schema()
    tasks = [
        asyncio.create_task(db_watcher(), name="db_watcher"),
        asyncio.create_task(gold_http_watcher(), name="gold_http"),
        asyncio.create_task(gold_mt5_watcher(), name="gold_mt5"),
        asyncio.create_task(wti_http_watcher(), name="wti_http"),
        asyncio.create_task(wti_mt5_watcher(), name="wti_mt5"),
    ]
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# -- FastAPI app definition ------------------------------------------------

app = FastAPI(title="Trident Agent API", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- SSE stream route ------------------------------------------------------

@app.get("/api/events/stream")
async def sse_stream(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=256)
    _SSE_QUEUES.append(q)

    async def event_generator():
        last_id = await _fetch_newest_id()
        try:
            while True:
                if await request.is_disconnected():
                    break
                rows = await _fetch_rows_since(last_id)
                for ev in rows:
                    last_id = max(last_id, ev["id"])
                    yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            try:
                _SSE_QUEUES.remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# -- REST routes -----------------------------------------------------------

@app.get("/api/events")
async def get_events() -> List[Dict[str, Any]]:
    """Return the latest 50 rows -- used for initial page load."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_decisions'"
        )
        if not await cursor.fetchone():
            return []

        cursor = await db.execute(
            """
            SELECT
                ad.id,
                rn.timestamp,
                rn.source,
                rn.content                          AS news_text,
                UPPER(ad.suggested_action)           AS action,
                ad.sentiment_score                   AS score,
                ad.reasoning                         AS reason,
                ad.market_category,
                ad.target_asset,
                ad.created_at,
                ad.parent_id,
                ad.child_count,
                ad.reasoning_path,
                ad.vip_tag,
                ad.entry_price,
                ad.exit_price,
                ad.max_price,
                ad.min_price,
                ad.max_price_time,
                ad.min_price_time,
                ad.is_correct,
                ad.settled,
                ad.doubao_action,
                ad.doubao_reasoning,
                ad.extra_models_consensus,
                ad.entry_time,
                ad.cluster_size
            FROM ai_decisions ad
            INNER JOIN raw_news rn ON rn.id = ad.news_id
            ORDER BY ad.id DESC
            LIMIT 50
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

    return [_row_to_event(r) for r in rows]


@app.get("/api/klines/{symbol}")
async def get_klines(symbol: str, limit: int = 72):
    ticker_map = {
        "BTCUSDT": ("BTC-USD", 80000),
        "XAUUSD":  ("GC=F",    2500),
        "WTIUSD":  ("CL=F",      70),
    }
    yf_sym, mock_base = ticker_map.get(symbol.upper(), (None, None))
    if yf_sym is None:
        return _mock_klines(limit, base=80000, seed=1)
    try:
        import yfinance as yf
        ticker = yf.Ticker(yf_sym)
        hist = ticker.history(period="3d", interval="1h")
        if hist.empty:
            return _mock_klines(limit, base=mock_base, seed=1)
        result = []
        for idx, row in hist.iterrows():
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=TZ_SHANGHAI)
            result.append({
                "time": int(ts.timestamp()),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]) if not math.isnan(float(row["Volume"])) else 0,
            })
        if result:
            return result[:limit]
    except Exception:
        pass
    return _mock_klines(limit, base=mock_base, seed=1)


@app.get("/api/export/excel")
async def export_excel():
    """Export today's buy/sell signals as Excel."""
    try:
        return await _do_export_excel()
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return StreamingResponse(
            io.BytesIO(f"Export error: {exc}".encode("utf-8")),
            media_type="text/plain; charset=utf-8",
            status_code=500,
        )


async def _do_export_excel():
    """Core Excel export logic, separated for clean error handling."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                ad.id,
                rn.timestamp,
                rn.source,
                rn.content AS news_text,
                UPPER(ad.suggested_action) AS action,
                ad.sentiment_score AS score,
                ad.reasoning_path,
                ad.reasoning,
                ad.extra_models_consensus,
                ad.target_asset,
                ad.vip_tag,
                ad.entry_price, ad.exit_price, ad.max_price, ad.min_price,
                ad.max_price_time, ad.min_price_time,
                ad.entry_time,
                ad.is_correct,
                ad.cluster_size
            FROM ai_decisions ad
            INNER JOIN raw_news rn ON rn.id = ad.news_id
            WHERE date(ad.created_at) >= date('now', 'localtime', '-5 days')
              AND UPPER(ad.suggested_action) IN ('BUY', 'SELL')
              AND UPPER(ad.target_asset) IN ('XAU', 'GOLD', 'BTC', 'WTI')
              AND ad.entry_price IS NOT NULL
            ORDER BY ad.id DESC
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

    import xlsxwriter
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output)
    ws = wb.add_worksheet("Signals")
    headers = [
        "ID", "时间", "新闻内容", "品种", "方向", "评分", "入场价", "最高价", "最低价", "出场价",
        "最大浮盈%", "最大浮亏%", "到达极值(min)", "强影响", "胜负", "标签", "Kimi K3归因", "extra_models_consensus",
    ]
    for c, h in enumerate(headers):
        ws.write(0, c, h)

    # Impact thresholds: MFE must exceed asset-specific percentage
    IMPACT_THRESHOLDS = {"BTC": 2.0, "XAU": 1.0, "GOLD": 1.0, "WTI": 1.5}

    for r, row in enumerate(rows, start=1):
        asset = (row["target_asset"] or "").upper()
        action_raw = (row["action"] or "").upper()
        entry = row["entry_price"]
        exit_p = row["exit_price"]
        max_p = row["max_price"]
        min_p = row["min_price"]
        max_ptime = row["max_price_time"] or 0
        min_ptime = row["min_price_time"] or 0
        entry_time_str = row["entry_time"] or ""
        raw_verdict = (row["is_correct"] or "").strip().upper()
        if raw_verdict == "WIN":
            verdict = "正确"
        elif raw_verdict == "LOSS":
            verdict = "错误"
        else:
            verdict = raw_verdict or "—"
        # ── Derived impact metrics (defensive against div-by-zero) ──
        mfe_str = "—"
        mae_str = "—"
        time_to_extreme = "—"
        high_impact = "否"

        if entry and entry > 0:
            try:
                if action_raw == "BUY":
                    if max_p is not None and max_p > 0:
                        mfe_val = (max_p - entry) / entry * 100
                        mfe_str = f"{mfe_val:+.2f}%"
                    if min_p is not None and min_p > 0:
                        mae_val = (min_p - entry) / entry * 100
                        mae_str = f"{mae_val:+.2f}%"
                    if max_ptime > 0:
                        try:
                            et_dt = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
                            et_unix = int(et_dt.timestamp())
                            minutes = round((max_ptime - et_unix) / 60, 1)
                            if minutes >= 0:
                                time_to_extreme = f"{minutes}"
                        except (ValueError, TypeError, OSError):
                            pass
                elif action_raw == "SELL":
                    if entry > 0:
                        if min_p is not None and min_p > 0:
                            mfe_val = (entry - min_p) / entry * 100
                            mfe_str = f"{mfe_val:+.2f}%"
                        if max_p is not None and max_p > 0:
                            mae_val = (entry - max_p) / entry * 100
                            mae_str = f"{mae_val:+.2f}%"
                    if min_ptime > 0:
                        try:
                            et_dt = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
                            et_unix = int(et_dt.timestamp())
                            minutes = round((min_ptime - et_unix) / 60, 1)
                            if minutes >= 0:
                                time_to_extreme = f"{minutes}"
                        except (ValueError, TypeError, OSError):
                            pass

                if mfe_str != "—":
                    mfe_number = float(mfe_str.replace("%", "").replace("+", ""))
                    threshold = IMPACT_THRESHOLDS.get(asset, 2.0)
                    if mfe_number > threshold:
                        high_impact = "是"
            except Exception:
                pass

        ws.write(r, 0, row["id"])
        ws.write(r, 1, _format_time(row["timestamp"]))
        ws.write(r, 2, re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', (row["news_text"] or "")[:200]))
        ws.write(r, 3, asset)
        action_display = "多" if action_raw == "BUY" else ("空" if action_raw == "SELL" else action_raw)
        ws.write(r, 4, action_display)
        ws.write(r, 5, round(row["score"], 2) if row["score"] else 0)
        ws.write(r, 6, entry)
        ws.write(r, 7, max_p)
        ws.write(r, 8, min_p)
        ws.write(r, 9, exit_p)
        ws.write(r, 10, mfe_str)
        ws.write(r, 11, mae_str)
        ws.write(r, 12, time_to_extreme)
        ws.write(r, 13, high_impact)
        ws.write(r, 14, verdict)
        ws.write(r, 15, (row["vip_tag"] or "").replace("[", "").replace("]", ""))
                # Kimi K3 归因: 优先完整推导链，回退到短结论
        ws.write(r, 16, ((row["reasoning_path"] or row["reasoning"] or "")[:2000]).strip())
        # ── extra_models_consensus: 直接从 DB 列写入，已经是合法 JSON ──
        raw_consensus = _safe(row, "extra_models_consensus") or ""
        if isinstance(raw_consensus, str):
            raw_consensus = raw_consensus.strip()
        if raw_consensus:
            # 校验是否为合法 JSON，非法则写入原始字符串
            try:
                parsed = json.loads(raw_consensus) if isinstance(raw_consensus, str) else raw_consensus
                ws.write(r, 17, json.dumps(parsed, ensure_ascii=False))
            except (json.JSONDecodeError, TypeError, ValueError):
                ws.write(r, 17, str(raw_consensus)[:2000])
        else:
            ws.write(r, 17, "")
    wb.close()
    data = output.getvalue()
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=trident_signals.xlsx"},
    )


# ---------------------------------------------------------------------------
# Agent Chat (Data Copilot) — Text-to-SQL + DB query
# ---------------------------------------------------------------------------

class AgentChatRequest(BaseModel):
    user_message: str
    active_market: str = "ALL"
    context: Dict[str, Any] = {}

class AgentChatResponse(BaseModel):
    reply: str
    sql: str = ""
    rows: List[Dict[str, Any]] = []
    error: str = ""

# ── Agent LLM config: DeepSeek direct (OpenAI-compatible) ──
AGENT_LLM_BASE_URL = os.getenv("AGENT_LLM_BASE_URL", "https://api.deepseek.com/v1")
AGENT_LLM_API_KEY = os.getenv("AGENT_LLM_API_KEY", os.getenv("DEEPSEEK_API_KEY", ""))
AGENT_LLM_MODEL = os.getenv("AGENT_LLM_MODEL", "deepseek-chat")

# ── DB Schema description for the Agent LLM ──

_SCHEMA_TEXT = """
You are Trident Data Copilot, an expert quant trading analyst with read-only SQLite access.

Tables & columns:

  ai_decisions: id, news_id(FK→raw_news.id), created_at, suggested_action(BUY|SELL|HOLD),
    sentiment_score(-1..+1), reasoning(short), reasoning_path(full chain-of-thought), market_category,
    target_asset, vip_tag, entry_price, exit_price, max_price, min_price, max_price_time,
    min_price_time, entry_time, is_correct(WIN|LOSS|""), settled(0|1), parent_id, child_count,
    cluster_size, doubao_action(HOLD), doubao_reasoning,
    extra_models_consensus(JSON: {"DeepSeek":{"action":"BUY","reasoning":"..."}, ...})

  raw_news: id, timestamp, source, content(full news text), status(NEW|PROCESSING|DONE|FAILED)

One ai_decisions row = one Kimi K3 decision + JSON-packed sub-model votes.
Use date(ad.created_at) for date filters. LIKE is case-insensitive.
json_extract(extra_models_consensus, '$.DeepSeek.action') pulls sub-model data.
Default LIMIT 20 if not specified.

Respond with a single valid JSON: {"sql": "<SELECT only or empty string>", "reply": "<Chinese explanation>"}

IMPORTANT — when to return empty sql:
- If the user is just greeting, chatting, saying thanks, or asking a question that does NOT require database access, set sql to "" and reply with a friendly Chinese greeting or acknowledgement.
- Only populate sql when the user is explicitly asking about trading signals, positions, win/loss stats, model votes, news events, or anything that requires querying ai_decisions or raw_news.

SQL rules:
- SELECT queries ONLY. Never INSERT/UPDATE/DELETE/DROP/ALTER.
- ORDER BY ad.id DESC for recent data.
- GROUP BY/COUNT/AVG/SUM for stats.
- NEVER access system tables or sqlite_master.
"""

# ── LLM-powered Text-to-SQL ──

def _time_context() -> str:
    """Return current system time for injection into LLM prompts."""
    now = datetime.now()
    return (
        f"【重要上下文】当前系统精准时间是: {now.strftime('%Y-%m-%d %H:%M:%S')} "
        f"(星期{['一','二','三','四','五','六','日'][now.weekday()]})。"
        f"当用户提到'今天'、'最近'、'昨天'、'本周'或省略年份的日期时，"
        f"请务必以此时间为基准来计算SQL的时间范围，切勿自行猜测年份！"
    )

def _llm_text_to_sql_sync(user_message: str, active_market: str) -> tuple[str, str]:
    """Call the LLM synchronously (runs in a thread via asyncio.to_thread)."""
    import openai

    client = openai.OpenAI(
        base_url=AGENT_LLM_BASE_URL,
        api_key=AGENT_LLM_API_KEY,
    )

    resp = client.chat.completions.create(
        model=AGENT_LLM_MODEL,
        temperature=0.0,
        max_tokens=800,
        messages=[
            {"role": "system", "content": _SCHEMA_TEXT},
            {"role": "system", "content": _time_context()},
            {
                "role": "user",
                "content": (
                    f"当前活跃市场: {active_market}\n"
                    f"用户提问: {user_message}\n\n"
                    f"请根据表结构生成SQL查询，返回JSON。"
                ),
            },
        ],
        response_format={"type": "json_object"},
    )

    raw = resp.choices[0].message.content.strip()
    result = json.loads(raw)
    return result.get("reply", "查询完成"), result.get("sql", "")

# ── Data-to-Text: second LLM pass for natural-language summary ──

_SUMMARIZE_PROMPT = """You are a professional quantitative trader writing a concise internal briefing.

Given the user's original question and a JSON array of database query results, produce a short, insightful answer in Chinese (≤150 characters).

Rules:
- Do NOT list every row. Extract the key pattern, trend, or answer.
- Mention counts, dominant assets, price ranges, and win/loss when relevant.
- Use professional but plain language. No markdown, no bullet points.
- If results are empty, say so honestly.
- Format: just the summary text, nothing else."""

def _clean_rows_for_frontend(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Strip verbose/text-heavy columns so the frontend table stays compact."""
    _STRIP_COLS = {
        "reasoning", "reasoning_path", "extra_models_consensus",
        "news_text", "content", "doubao_reasoning", "doubao_action",
    }
    cleaned = []
    for r in rows:
        cleaned.append({k: v for k, v in r.items() if k not in _STRIP_COLS})
    return cleaned

def _summarize_results_sync(user_message: str, rows: List[Dict[str, Any]]) -> str:
    """Second LLM call: data → natural-language insight."""
    import openai

    # Only send first 5 rows to keep tokens low
    sample = rows[:5]
    # Also strip verbose fields from the sample sent to LLM
    _STRIP_FOR_LLM = {"reasoning_path", "extra_models_consensus", "doubao_reasoning"}
    sample_clean = [
        {k: v for k, v in r.items() if k not in _STRIP_FOR_LLM}
        for r in sample
    ]
    data_json = json.dumps(sample_clean, ensure_ascii=False, default=str)

    client = openai.OpenAI(
        base_url=AGENT_LLM_BASE_URL,
        api_key=AGENT_LLM_API_KEY,
    )

    resp = client.chat.completions.create(
        model=AGENT_LLM_MODEL,
        temperature=0.0,
        max_tokens=300,
        messages=[
            {"role": "system", "content": _SUMMARIZE_PROMPT},
            {"role": "system", "content": _time_context()},
            {
                "role": "user",
                "content": (
                    f"用户提问: {user_message}\n"
                    f"共 {len(rows)} 条结果，以下是前 {len(sample)} 条:\n"
                    f"{data_json}"
                ),
            },
        ],
    )

    return resp.choices[0].message.content.strip()

@app.post("/api/agent_chat", response_model=AgentChatResponse)
async def agent_chat_endpoint(req: AgentChatRequest):
    """
    Data Copilot — Two-pass LLM:
      1. Text-to-SQL → execute → get rows
      2. Data-to-Text → natural-language insight summary
    """
    # ── Step 1: LLM Text-to-SQL ──
    try:
        explanation, sql = await asyncio.to_thread(
            _llm_text_to_sql_sync, req.user_message, req.active_market
        )
    except Exception as e:
        return AgentChatResponse(
            reply="抱歉，LLM 调用失败，请稍后重试或换个问法。",
            error=f"LLM error: {e}",
        )

    # ── Step 2: Empty SQL = chitchat ──
    if not sql or not sql.strip():
        return AgentChatResponse(reply=explanation, sql="", rows=[])

    # ── Step 3: Safety gate ──
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT"):
        return AgentChatResponse(
            reply="出于安全考虑，我只执行 SELECT 查询。请重新描述你的需求。",
            sql=sql.strip(),
            error="Non-SELECT statement blocked",
        )

    if any(bad in stripped for bad in ("SQLITE_MASTER", "PRAGMA", "ATTACH", "DETACH")):
        return AgentChatResponse(
            reply="不允许访问系统表或执行管理命令。",
            sql=sql.strip(),
            error="Forbidden system access blocked",
        )

    # ── Step 4: Execute SQL ──
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(sql)
            rows_raw = await cursor.fetchall()
            await cursor.close()

        rows = [dict(r) for r in rows_raw]

        if not rows:
            return AgentChatResponse(
                reply="没有找到匹配的数据。换个条件试试？",
                sql=sql.strip(),
                rows=[],
            )

        # ── Step 5: Second LLM pass — data → natural-language insight ──
        try:
            summary = await asyncio.to_thread(
                _summarize_results_sync, req.user_message, rows
            )
        except Exception:
            summary = f"查询返回 {len(rows)} 条记录，详见下方表格。"

        # ── Step 6: Clean rows for compact frontend table ──
        clean_rows = _clean_rows_for_frontend(rows)

        return AgentChatResponse(
            reply=summary,
            sql=sql.strip(),
            rows=clean_rows,
        )

    except Exception as e:
        return AgentChatResponse(
            reply=f"SQL 执行出错，请换个问法试试。",
            sql=sql.strip(),
            error=str(e),
        )


@app.get("/api/health")
async def health_check():
    """Liveness probe."""
    return {
        "status": "ok",
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "sse_clients": len(_SSE_QUEUES),
    }


# -- Entry point -----------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False, log_level="info")
