#!/usr/bin/env python3
"""
Trident Agent MVP — Market Snapshot Module
===========================================

A lightweight, self-contained module that pulls real-time market data
from Binance USDT-M Futures via ccxt.  Designed to be called once per
AI batch (NOT once per news item) — the returned dict is injected into
the LLM prompt so Kimi K3 sees market context alongside each headline.

Core assets monitored:
  BTC/USDT  — Bitcoin U本位永续合约
  XAU/USDT  — 黄金 U本位永续合约

Usage:
    from market_snapshot import get_snapshot

    # Async (preferred — call from engine's async batch loop)
    snap = await get_snapshot()

    # Sync (for debugging or non-async contexts)
    snap = get_snapshot_sync()

Returns:
    Dict with per-asset price, 24h change, funding rate, and a
    human-readable summary string ready for prompt injection.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# ccxt — must be installed (pip install ccxt)
# ---------------------------------------------------------------------------
try:
    import ccxt
    import ccxt.async_support as ccxt_async
    HAS_CCXT = True
except ImportError:
    HAS_CCXT = False
    ccxt = None  # type: ignore[assignment]
    ccxt_async = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# 核心监控资产 (Binance USDT-M Futures)
_CORE_SYMBOLS: List[Tuple[str, str]] = [
    ("BTC/USDT",  "BTC"),
    ("XAU/USDT",  "XAU"),   # 黄金 U本位永续合约
]

# 没有资金费率的标的 (现货 / 特殊品种)
_SKIP_FUNDING: set = set()

# Debug 日志开关 — 稳定后设为 False, 避免刷屏
_LOUD = False

def _debug(msg: str) -> None:
    if _LOUD:
        print(msg)

# 代理 — ccxt 直连 Binance 在某些地区受限 (HTTP 451)
# 懒加载: exchange 创建时才读取环境变量, 避免模块导入时 env 未就绪
def _get_proxy_kwargs() -> Dict[str, Any]:
    """Build proxy kwargs dict if HTTP_PROXY/HTTPS_PROXY env vars are set."""
    proxy_http = os.getenv("HTTP_PROXY", "").strip() or os.getenv("http_proxy", "").strip()
    proxy_https = os.getenv("HTTPS_PROXY", "").strip() or os.getenv("https_proxy", "").strip()
    if proxy_http or proxy_https:
        return {
            "proxies": {
                "http": proxy_http,
                "https": proxy_https,
            }
        }
    return {}
_FETCH_TIMEOUT_MS = 8000       # 单次请求超时 (毫秒)
_FETCH_OHLCV_TIMEOUT_MS = 15000  # OHLCV 拉取超时（数据量大，需要更长时间）
_RETRY_DELAY_S = 1.0           # 网络失败后重试间隔 (秒)
_MAX_RETRIES = 1               # 额外重试次数 (总共 1 + 1 = 2 次尝试)

# 趋势判定阈值
_TREND_BULL_THRESHOLD = 3.0    # 7日涨幅 > 3% 判定为 Bull
_TREND_BEAR_THRESHOLD = -3.0   # 7日涨幅 < -3% 判定为 Bear
_ATR_DAYS = 7                   # ATR 计算窗口 (日线)
_TREND_CONSISTENCY_DAYS = 5     # 连续上涨/下跌天数阈值 (强趋势判定)

# ccxt 交易所缓存 (避免每次请求重建 session)
_EXCHANGE: Any = None          # type: ccxt.binance (sync)
_EXCHANGE_ASYNC: Any = None    # type: ccxt.async_support.binance (async)


# ---------------------------------------------------------------------------
# Exchange factory
# ---------------------------------------------------------------------------

def _get_exchange() -> Any:
    """Return a configured synchronous ccxt Binance futures exchange (reuse across calls)."""
    global _EXCHANGE
    if _EXCHANGE is None:
        if not HAS_CCXT:
            raise RuntimeError("ccxt 未安装，无法获取行情快照。 pip install ccxt")
        kwargs: Dict[str, Any] = {
            "options": {"defaultType": "future"},
            "timeout": _FETCH_TIMEOUT_MS,
            "enableRateLimit": True,
        }
        proxy = _get_proxy_kwargs()
        kwargs.update(proxy)
        proxy_note = f" proxy={proxy['proxies']['http']}" if proxy else " (直连)"
        _debug(f"  [SNAPSHOT:DEBUG] 创建 sync Binance exchange...{proxy_note}")
        _EXCHANGE = ccxt.binance(kwargs)
        # Warm up: load markets metadata (cached after first call)
        try:
            start = time.time()
            _EXCHANGE.load_markets()
            elapsed = round((time.time() - start) * 1000)
            _debug(f"  [SNAPSHOT:DEBUG] load_markets() OK in {elapsed}ms")
        except Exception as e:
            _debug(f"  [SNAPSHOT:DEBUG] load_markets() 失败: {type(e).__name__}: {str(e)[:120]}")
            import traceback
            traceback.print_exc()
            # 预热失败不影响后续单次请求
    return _EXCHANGE


def _get_exchange_async() -> Any:
    """Return a configured async ccxt Binance futures exchange."""
    global _EXCHANGE_ASYNC
    if _EXCHANGE_ASYNC is None:
        if not HAS_CCXT:
            raise RuntimeError("ccxt 未安装，无法获取行情快照。 pip install ccxt")
        kwargs: Dict[str, Any] = {
            "options": {"defaultType": "future"},
            "timeout": _FETCH_TIMEOUT_MS,
            "enableRateLimit": True,
        }
        proxy = _get_proxy_kwargs()
        kwargs.update(proxy)
        proxy_note = f" proxy={proxy['proxies']['http']}" if proxy else " (直连)"
        _debug(f"  [SNAPSHOT:DEBUG] 创建 async Binance exchange...{proxy_note}")
        _EXCHANGE_ASYNC = ccxt_async.binance(kwargs)
        # load_markets 在首次 fetch_ticker 时隐式调用, 这里仅打日志
        _debug(f"  [SNAPSHOT:DEBUG] async exchange 已创建 (load_markets 将在首次 fetch 隐式执行)")
    return _EXCHANGE_ASYNC


# ---------------------------------------------------------------------------
# Single-ticker fetchers
# ---------------------------------------------------------------------------

def _fetch_ticker_sync(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch ticker from Binance Futures for ONE symbol.  Returns None on failure."""
    ex = _get_exchange()
    for attempt in range(1 + _MAX_RETRIES):
        try:
            ticker = ex.fetch_ticker(symbol)
            # 验证关键字段存在
            if ticker.get("last") is None:
                return None
            return ticker
        except Exception as e:
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY_S)
            else:
                print(f"  [SNAPSHOT] fetch_ticker({symbol}) 失败: {type(e).__name__}: {str(e)[:80]}")
    return None


async def _fetch_ticker_async(symbol: str) -> Optional[Dict[str, Any]]:
    """Async fetch ticker from Binance Futures for ONE symbol."""
    ex = _get_exchange_async()
    for attempt in range(1 + _MAX_RETRIES):
        try:
            t0 = time.time()
            ticker = await ex.fetch_ticker(symbol)
            elapsed_ms = round((time.time() - t0) * 1000)
            _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) attempt={attempt+1} "
                  f"HTTP_OK elapsed={elapsed_ms}ms")
            # ── response 验证 ──
            if ticker is None:
                _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) → response 为 None/空")
                return None
            last = _extract_price(ticker)
            if last is None:
                _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) → 所有价格字段缺失: "
                      f"keys={list(ticker.keys())[:12]}")
                # Print raw info for diagnostics
                raw_info = ticker.get("info", {})
                if isinstance(raw_info, dict):
                    _debug(f"  [SNAPSHOT:DEBUG]   raw info keys: {list(raw_info.keys())[:10]}")
                    _debug(f"  [SNAPSHOT:DEBUG]   raw info sample: {json.dumps(raw_info, ensure_ascii=False)[:300]}")
                return None
            _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) → last={last}, "
                  f"bid={ticker.get('bid')}, ask={ticker.get('ask')}, "
                  f"change_24h={ticker.get('percentage')}")
            return ticker
        except Exception as e:
            exc_type = type(e).__name__
            exc_msg = str(e)[:200]
            _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) attempt={attempt+1} 异常: "
                  f"{exc_type}: {exc_msg}")
            # ── 分类错误类型 ──
            if "ConnectionError" in exc_type or "ConnectTimeout" in exc_type or "ProxyError" in exc_type:
                _debug(f"  [SNAPSHOT:DEBUG] → 网络连接失败 (DNS/代理/防火墙?)")
            elif "Timeout" in exc_type or "timed" in exc_msg.lower():
                _debug(f"  [SNAPSHOT:DEBUG] → 请求超时 (Binance API 响应慢/网络延迟)")
            elif "RateLimit" in exc_type or "DDoS" in exc_msg:
                _debug(f"  [SNAPSHOT:DEBUG] → 被 Binance 限流")
            elif "BadSymbol" in exc_msg or "not found" in exc_msg.lower():
                _debug(f"  [SNAPSHOT:DEBUG] → 交易对不存在 (需检查 defaultType=future)")
            elif "ExchangeNotAvailable" in exc_type:
                _debug(f"  [SNAPSHOT:DEBUG] → Binance 服务不可用")
            else:
                import traceback
                _debug(f"  [SNAPSHOT:DEBUG] → 未分类异常, 完整 traceback:")
                traceback.print_exc()
            if attempt < _MAX_RETRIES:
                _debug(f"  [SNAPSHOT:DEBUG] → 等待 {_RETRY_DELAY_S}s 后重试...")
                await asyncio.sleep(_RETRY_DELAY_S)
            else:
                _debug(f"  [SNAPSHOT:DEBUG] fetch_ticker({symbol}) 所有重试已耗尽, 返回 None")
    return None


def _fetch_funding_rate_sync(symbol: str) -> Optional[float]:
    """Fetch current funding rate (as decimal, e.g. 0.0001 = 0.01%)."""
    ex = _get_exchange()
    try:
        info = ex.fetch_funding_rate(symbol)
        rate = info.get("fundingRate") or info.get("info", {}).get("lastFundingRate")
        if rate is not None:
            return float(rate)
    except Exception:
        pass
    return None


async def _fetch_funding_rate_async(symbol: str) -> Optional[float]:
    """Async fetch current funding rate."""
    ex = _get_exchange_async()
    try:
        info = await ex.fetch_funding_rate(symbol)
        rate = info.get("fundingRate") or info.get("info", {}).get("lastFundingRate")
        if rate is not None:
            return float(rate)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Safe field parsers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Coerce a value to float or return default."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _extract_price(ticker: Dict[str, Any]) -> Optional[float]:
    """Extract current price from a ccxt ticker dict with multi-key fallback.

    ccxt normalizes Binance spot ticker differently from futures ticker.
    This function tries every known key path.
    """
    # 1. ccxt normalized keys
    for key in ("last", "close"):
        val = ticker.get(key)
        if val is not None:
            return _safe_float(val)
    # 2. Binance raw response
    info = ticker.get("info", {})
    if isinstance(info, dict):
        for key in ("lastPrice", "last", "price"):
            val = info.get(key)
            if val is not None:
                return _safe_float(val)
    # 3. Bid/ask midpoint (last resort)
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    if bid is not None and ask is not None:
        return round((_safe_float(bid) + _safe_float(ask)) / 2.0, 4)
    return None


# ---------------------------------------------------------------------------
# OHLCV fetchers — daily candles (4h fallback) for 7d trend/ATR computation
# ---------------------------------------------------------------------------

# 4h fallback: 42 根 4h 蜡烛 ≈ 7 天, 精度略低于日线但趋势方向可用
_OHLCV_FALLBACK_LIMIT = 42

_OHLCV_TIMEFRAMES = [
    ("1d", 8, "日线"),
    ("4h", _OHLCV_FALLBACK_LIMIT, "4h(降级)"),
]


def _fetch_ohlcv_with_fallback_sync(symbol: str) -> Optional[Tuple[List[List[float]], str]]:
    """Fetch OHLCV: try 1d first, fall back to 4h on failure.

    Returns (candles, timeframe_label) or None.
    """
    ex = _get_exchange()
    for tf, limit, label in _OHLCV_TIMEFRAMES:
        try:
            candles = ex.fetch_ohlcv(
                symbol, timeframe=tf, limit=limit,
                params={"timeout": _FETCH_OHLCV_TIMEOUT_MS},
            )
            if candles and len(candles) >= 2:
                return (candles, label)
        except Exception as e:
            print(f"  [SNAPSHOT] fetch_ohlcv({symbol} {label}) 失败: {type(e).__name__}: {str(e)[:80]}")
    return None


async def _fetch_ohlcv_with_fallback_async(symbol: str) -> Optional[Tuple[List[List[float]], str]]:
    """Async fetch OHLCV: try 1d first, fall back to 4h on failure.

    Returns (candles, timeframe_label) or None.
    """
    ex = _get_exchange_async()
    for tf, limit, label in _OHLCV_TIMEFRAMES:
        try:
            candles = await ex.fetch_ohlcv(
                symbol, timeframe=tf, limit=limit,
                params={"timeout": _FETCH_OHLCV_TIMEOUT_MS},
            )
            if candles and len(candles) >= 2:
                return (candles, label)
        except Exception as e:
            print(f"  [SNAPSHOT] fetch_ohlcv({symbol} {label}) 失败: {type(e).__name__}: {str(e)[:80]}")
    return None


# ---------------------------------------------------------------------------
# Trend & regime computation
# ---------------------------------------------------------------------------

def _compute_7d_stats(
    candles: List[List[float]],
) -> Dict[str, Any]:
    """From up to 8 daily OHLCV candles, compute 7d return, ATR, and trend.

    Args:
        candles: List of [timestamp_ms, open, high, low, close, volume].

    Returns dict with:
        price_7d_ago, return_7d_pct, return_7d_str,
        atr_pct, atr_value, atr_str,
        trend, trend_strength, n_up_days, n_down_days,
        highest_7d, lowest_7d, range_7d_pct.
    """
    if not candles or len(candles) < 2:
        return _empty_7d_stats()

    closes = [c[4] for c in candles]   # index 4 = close
    highs = [c[2] for c in candles]     # index 2 = high
    lows = [c[3] for c in candles]      # index 3 = low

    current = closes[-1]
    prior = closes[0]
    if prior <= 0 or current <= 0:
        return _empty_7d_stats()

    ret_7d = (current - prior) / prior * 100.0
    highest_7d = max(highs)
    lowest_7d = min(lows)
    range_7d = (highest_7d - lowest_7d) / current * 100.0 if current > 0 else 0.0

    # ── ATR (Average True Range) ──
    tr_values: List[float] = []
    for i in range(1, len(candles)):
        h, l, prev_c = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_values.append(tr)
    atr_value = sum(tr_values) / len(tr_values) if tr_values else 0.0
    atr_pct = (atr_value / current * 100.0) if current > 0 else 0.0

    # ── Trend classification ──
    n_up = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
    n_down = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])

    if n_up >= _TREND_CONSISTENCY_DAYS and ret_7d > 0:
        trend = "Strong Bull"
        trend_strength = "high"
    elif n_down >= _TREND_CONSISTENCY_DAYS and ret_7d < 0:
        trend = "Strong Bear"
        trend_strength = "high"
    elif ret_7d > _TREND_BULL_THRESHOLD:
        trend = "Bull"
        trend_strength = "medium"
    elif ret_7d < _TREND_BEAR_THRESHOLD:
        trend = "Bear"
        trend_strength = "medium"
    elif abs(ret_7d) < 1.0:
        trend = "Ranging"
        trend_strength = "low"
    elif ret_7d > 0:
        trend = "Mild Bull"
        trend_strength = "low"
    else:
        trend = "Mild Bear"
        trend_strength = "low"

    return {
        "price_7d_ago": round(prior, 4),
        "return_7d_pct": round(ret_7d, 2),
        "return_7d_str": _format_pct(ret_7d),
        "atr_pct": round(atr_pct, 3),
        "atr_value": round(atr_value, 4),
        "atr_str": f"{atr_pct:.3f}%",
        "trend": trend,
        "trend_strength": trend_strength,
        "n_up_days": n_up,
        "n_down_days": n_down,
        "highest_7d": round(highest_7d, 4),
        "lowest_7d": round(lowest_7d, 4),
        "range_7d_pct": round(range_7d, 2),
    }


def _empty_7d_stats() -> Dict[str, Any]:
    """Return empty 7d stats for error/degraded paths."""
    return {
        "price_7d_ago": 0.0,
        "return_7d_pct": 0.0,
        "return_7d_str": "N/A",
        "atr_pct": 0.0,
        "atr_value": 0.0,
        "atr_str": "N/A",
        "trend": "Unknown",
        "trend_strength": "none",
        "n_up_days": 0,
        "n_down_days": 0,
        "highest_7d": 0.0,
        "lowest_7d": 0.0,
        "range_7d_pct": 0.0,
    }


def _format_pct(value: float, decimals: int = 2) -> str:
    """Format a float as a signed percentage string, e.g. '+2.35%'."""
    return f"{value:+.{decimals}f}%"


def _format_price(value: float, decimals: int = 2) -> str:
    """Format a price with appropriate decimal places."""
    if value >= 1000:
        return f"{value:,.{decimals}f}"
    return f"{value:.{decimals}f}"


# ---------------------------------------------------------------------------
# Main snapshot API
# ---------------------------------------------------------------------------

async def get_snapshot() -> Dict[str, Any]:
    """
    Async — pull a multi-asset market snapshot from Binance USDT-M Futures.

    Returns a dict:
      {
        "timestamp": "2026-07-23 14:30:01 CST",
        "epoch_ms": 1753285801000,
        "assets": {
          "BTC": {
            "symbol": "BTC/USDT",
            "price": 67200.50,
            "price_str": "67,200.50",
            "change_24h_pct": 2.35,
            "change_24h_str": "+2.35%",
            "funding_rate_pct": 0.0100,
            "funding_rate_str": "+0.0100%",
            "status": "ok",
          },
          "XAU": {
            "symbol": "XAU/USDT",
            "price": 2680.00,
            ...
            "status": "ok",            # 或 "unavailable" / "degraded"
            "status_note": "现货代理, 无资金费率",
          },
          ...
        },
        "summary": "...",              # 人类可读的摘要, 可直接注入 Prompt
        "status": "ok",                # 整体状态: "ok" | "partial" | "down"
      }

    On graceful degradation:
      - 单个标的拉不到 → status="unavailable", 不影响其他标的
      - 全部拉不到 → status="down", summary 返回占位字符串
      - 网络/ccxt 异常 → 永不抛异常, 最差情况返回空快照
    """
    if not HAS_CCXT:
        _debug(f"  [SNAPSHOT:DEBUG] ❌ HAS_CCXT=False — ccxt 未安装!")
        return _empty_snapshot("ccxt 未安装 (pip install ccxt)")

    _debug(f"  [SNAPSHOT:DEBUG] ccxt version={ccxt.__version__}, "
          f"async_ccxt version={ccxt_async.__version__ if ccxt_async else 'None'}")
    ex = _get_exchange_async()
    _debug(f"  [SNAPSHOT:DEBUG] exchange={type(ex).__name__}, "
          f"urls.api={ex.urls.get('api', 'N/A') if hasattr(ex, 'urls') else 'N/A'}")
    t0 = time.time()
    ts = int(t0 * 1000)

    assets: Dict[str, Dict[str, Any]] = {}
    ok_count = 0
    fail_count = 0

    # ── 并行拉取 ticker + OHLCV + 资金费率 (三个维度并发) ──
    _debug(f"  [SNAPSHOT:DEBUG] 启动并行 fetch: ticker × {len(_CORE_SYMBOLS)}, "
          f"ohlcv × {len(_CORE_SYMBOLS)}, funding × {len(_CORE_SYMBOLS)}")
    ticker_tasks = {
        asset_id: asyncio.create_task(_fetch_ticker_async(symbol))
        for symbol, asset_id in _CORE_SYMBOLS
    }
    ohlcv_tasks = {
        asset_id: asyncio.create_task(_fetch_ohlcv_with_fallback_async(symbol))
        for symbol, asset_id in _CORE_SYMBOLS
    }
    funding_tasks: Dict[str, asyncio.Task] = {}
    for symbol, asset_id in _CORE_SYMBOLS:
        if asset_id in _SKIP_FUNDING:
            continue
        funding_tasks[asset_id] = asyncio.create_task(_fetch_funding_rate_async(symbol))

    # 等待全部完成
    ticker_results: Dict[str, Optional[Dict]] = {}
    for asset_id, task in ticker_tasks.items():
        ticker_results[asset_id] = await task
    ohlcv_results: Dict[str, Optional[Tuple[List, str]]] = {}
    for asset_id, task in ohlcv_tasks.items():
        ohlcv_results[asset_id] = await task
    funding_results: Dict[str, Optional[float]] = {}
    for asset_id, task in funding_tasks.items():
        funding_results[asset_id] = await task

    # ── DEBUG: 打印原始 task 结果 ──
    for asset_id in ticker_results:
        t = ticker_results.get(asset_id)
        _debug(f"  [SNAPSHOT:DEBUG] ticker_result[{asset_id}] = "
              f"{'None' if t is None else f'OK(keys={list(t.keys())[:5]})'}")
    for asset_id in ohlcv_results:
        o = ohlcv_results.get(asset_id)
        if o is None:
            _debug(f"  [SNAPSHOT:DEBUG] ohlcv_result[{asset_id}] = None")
        else:
            candles, tf_label = o
            _debug(f"  [SNAPSHOT:DEBUG] ohlcv_result[{asset_id}] = OK(len={len(candles)}, tf={tf_label})")
    for asset_id in funding_results:
        f = funding_results.get(asset_id)
        _debug(f"  [SNAPSHOT:DEBUG] funding_result[{asset_id}] = "
              f"{'None' if f is None else f'{f:.6f}'}")

    # ── 组装结果 ──
    for symbol, asset_id in _CORE_SYMBOLS:
        ticker = ticker_results.get(asset_id)
        funding_rate = funding_results.get(asset_id)
        ohlcv_pair = ohlcv_results.get(asset_id)     # (candles, tf_label) or None
        ohlcv, ohlcv_tf = ohlcv_pair if ohlcv_pair else (None, None)

        # 计算 7d 趋势统计
        stats_7d = _compute_7d_stats(ohlcv) if ohlcv else _empty_7d_stats()
        if ohlcv_tf:
            stats_7d["source_tf"] = ohlcv_tf    # 标注数据来源: "日线" 或 "4h(降级)"

        if ticker is None:
            assets[asset_id] = {
                "symbol": symbol,
                "price": 0.0,
                "price_str": "获取失败",
                "change_24h_pct": 0.0,
                "change_24h_str": "N/A",
                "funding_rate_pct": 0.0,
                "funding_rate_str": "N/A",
                "status": "unavailable",
                "status_note": "网络请求失败",
                "stats_7d": stats_7d,
            }
            fail_count += 1
            continue

        price = _extract_price(ticker) or 0.0
        change_pct = _safe_float(ticker.get("percentage"), 0.0)

        # 资金费率 — 转为 % (ccxt 返回小数, 如 0.0001 = 0.01%)
        fr_pct = 0.0
        fr_str = "N/A"
        if funding_rate is not None:
            fr_pct = funding_rate * 100.0  # 小数 → %
            fr_str = f"{fr_pct:+.4f}%"
        elif asset_id in _SKIP_FUNDING:
            fr_str = "N/A (现货)"

        asset_entry = {
            "symbol": symbol,
            "price": round(price, 4),
            "price_str": _format_price(price, 2),
            "change_24h_pct": round(change_pct, 2),
            "change_24h_str": _format_pct(change_pct),
            "funding_rate_pct": round(fr_pct, 4),
            "funding_rate_str": fr_str,
            "status": "ok",
            "stats_7d": stats_7d,
        }

        assets[asset_id] = asset_entry
        ok_count += 1

    # ── 整体状态 ──
    total = len(_CORE_SYMBOLS)
    if ok_count == total:
        overall_status = "ok"
    elif ok_count == 0:
        overall_status = "down"
    else:
        overall_status = "partial"

    # ── 宏观指标 (Binance 不提供 DXY/US10Y/VIX — Phase 1 接入外部数据源) ──
    macro: Dict[str, Any] = {
        "dxy":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不支持 DXY — 需接入 TradingView/FRED"},
        "us10y": {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不支持美债收益率 — 需接入 FRED/宏观数据 API"},
        "oil":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 无原油期货 USDT-M — 考虑接入 Bybit/宏观 API"},
        "vix":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不提供 VIX — 需接入 Yahoo Finance/CBOE"},
    }

    # ── 生成人类可读摘要 (直接注入 Prompt) ──
    summary = _build_summary(assets, macro, overall_status)

    elapsed = round(time.time() - t0, 3)
    print(f"  [SNAPSHOT] {overall_status.upper()} ({ok_count}/{total} OK) "
          f"in {elapsed}s — {summary.split(chr(10))[0]}")

    return {
        "timestamp": _fmt_ts(),
        "epoch_ms": ts,
        "assets": assets,
        "macro": macro,
        "summary": summary,
        "status": overall_status,
    }


def get_snapshot_sync() -> Dict[str, Any]:
    """
    Synchronous wrapper — for debugging, REPL, or non-async scripts.

    usage:
        snap = get_snapshot_sync()
        print(snap["summary"])
    """
    if not HAS_CCXT:
        return _empty_snapshot("ccxt 未安装 (pip install ccxt)")

    ex = _get_exchange()
    t0 = time.time()
    ts = int(t0 * 1000)

    assets: Dict[str, Dict[str, Any]] = {}
    ok_count = 0
    fail_count = 0

    for symbol, asset_id in _CORE_SYMBOLS:
        ticker = _fetch_ticker_sync(symbol)
        ohlcv_pair = _fetch_ohlcv_with_fallback_sync(symbol)   # (candles, label) or None
        ohlcv, ohlcv_tf = ohlcv_pair if ohlcv_pair else (None, None)
        stats_7d = _compute_7d_stats(ohlcv) if ohlcv else _empty_7d_stats()
        if ohlcv_tf:
            stats_7d["source_tf"] = ohlcv_tf

        funding_rate = None
        if asset_id not in _SKIP_FUNDING:
            funding_rate = _fetch_funding_rate_sync(symbol)

        if ticker is None:
            assets[asset_id] = {
                "symbol": symbol,
                "price": 0.0, "price_str": "获取失败",
                "change_24h_pct": 0.0, "change_24h_str": "N/A",
                "funding_rate_pct": 0.0, "funding_rate_str": "N/A",
                "status": "unavailable", "status_note": "网络请求失败",
                "stats_7d": stats_7d,
            }
            fail_count += 1
            continue

        price = _extract_price(ticker) or 0.0
        change_pct = _safe_float(ticker.get("percentage"), 0.0)
        fr_pct = 0.0
        fr_str = "N/A"
        if funding_rate is not None:
            fr_pct = funding_rate * 100.0
            fr_str = f"{fr_pct:+.4f}%"
        elif asset_id in _SKIP_FUNDING:
            fr_str = "N/A (现货)"

        assets[asset_id] = {
            "symbol": symbol,
            "price": round(price, 4),
            "price_str": _format_price(price, 2),
            "change_24h_pct": round(change_pct, 2),
            "change_24h_str": _format_pct(change_pct),
            "funding_rate_pct": round(fr_pct, 4),
            "funding_rate_str": fr_str,
            "status": "ok",
            "stats_7d": stats_7d,
        }
        ok_count += 1

    macro = {
        "dxy":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不支持 DXY"},
        "us10y": {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不支持美债收益率"},
        "oil":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 无原油期货"},
        "vix":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable",
                   "note": "Binance 不提供 VIX"},
    }

    total = len(_CORE_SYMBOLS)
    overall_status = "ok" if ok_count == total else ("down" if ok_count == 0 else "partial")
    summary = _build_summary(assets, macro, overall_status)
    elapsed = round(time.time() - t0, 3)
    print(f"  [SNAPSHOT] {overall_status.upper()} ({ok_count}/{total} OK) "
          f"in {elapsed}s")

    return {
        "timestamp": _fmt_ts(),
        "epoch_ms": ts,
        "assets": assets,
        "macro": macro,
        "summary": summary,
        "status": overall_status,
    }


# ──────────────────────────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────────────────────────

def _fmt_ts() -> str:
    """格式化当前上海时间戳为可读字符串"""
    from datetime import datetime, timezone, timedelta
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S CST")


def _build_summary(assets: Dict[str, Dict], macro: Dict[str, Any], overall_status: str) -> str:
    """
    将快照数据组装为一段紧凑的文本摘要, 可直接嵌入 LLM system/user prompt。

    Returns compact multi-line string with price, 24h & 7d returns,
    trend/regime, ATR, and macro indicators.
    """
    if overall_status == "down":
        return "⚠️ 市场数据不可用 — 所有标的拉取失败。LLM 请仅基于新闻文本做方向性判断。"

    lines = ["── 市场快照 ──"]

    for asset_id in ["BTC", "XAU"]:
        a = assets.get(asset_id)
        if a is None:
            continue

        if a.get("status") == "unavailable":
            lines.append(f"{asset_id}: 数据不可用")
            continue

        stats = a.get("stats_7d", {})

        line = (
            f"{asset_id} ${a['price']:,.2f} | "
            f"24h {a['change_24h_str']} | "
            f"7d {stats.get('return_7d_str', 'N/A')} | "
            f"资金费率 {a['funding_rate_str']}"
        )
        lines.append(line)

        # 趋势 + 波动率 (一行)
        trend = stats.get("trend", "Unknown")
        atr = stats.get("atr_str", "N/A")
        strength = stats.get("trend_strength", "none")
        lines.append(
            f"  趋势: {trend} (强度={strength}, "
            f"连涨{stats.get('n_up_days', 0)}/连跌{stats.get('n_down_days', 0)}天) | "
            f"ATR(7d)={atr} | "
            f"7d区间: {stats.get('lowest_7d', 0):,.2f}–{stats.get('highest_7d', 0):,.2f}"
        )

    # ── 宏观指标 ──
    lines.append("── 宏观指标 ──")
    macro_line_parts = []
    for key, label in [("dxy", "DXY"), ("us10y", "US10Y"), ("oil", "Oil"), ("vix", "VIX")]:
        m = macro.get(key, {})
        if m.get("status") == "unavailable":
            macro_line_parts.append(f"{label}: N/A")
        else:
            macro_line_parts.append(f"{label}: ${m.get('value', 0):.2f} ({m.get('change_24h_str', 'N/A')})")
    lines.append(" | ".join(macro_line_parts))
    lines.append("(DXY/US10Y/Oil/VIX 需接入外部宏观数据源 — 当前不可用)")

    lines.append(f"数据状态: {overall_status}")
    return "\n".join(lines)


def _empty_snapshot(reason: str = "ccxt 未安装") -> Dict[str, Any]:
    """返回一个安全的空快照 — 用于所有降级场景"""
    assets = {}
    for symbol, asset_id in _CORE_SYMBOLS:
        assets[asset_id] = {
            "symbol": symbol,
            "price": 0.0, "price_str": "N/A",
            "change_24h_pct": 0.0, "change_24h_str": "N/A",
            "funding_rate_pct": 0.0, "funding_rate_str": "N/A",
            "status": "unavailable", "status_note": reason,
            "stats_7d": _empty_7d_stats(),
        }
    macro = {
        "dxy":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable", "note": reason},
        "us10y": {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable", "note": reason},
        "oil":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable", "note": reason},
        "vix":   {"value": 0.0, "change_24h_str": "N/A", "status": "unavailable", "note": reason},
    }
    return {
        "timestamp": _fmt_ts(),
        "epoch_ms": int(time.time() * 1000),
        "assets": assets,
        "macro": macro,
        "summary": f"⚠️ 市场数据不可用: {reason}",
        "status": "down",
    }


async def close() -> None:
    """释放 ccxt async session (engine shutdown 时调用)"""
    global _EXCHANGE_ASYNC
    if _EXCHANGE_ASYNC is not None:
        try:
            await _EXCHANGE_ASYNC.close()
        except Exception:
            pass
        _EXCHANGE_ASYNC = None


# ──────────────────────────────────────────────────────────────────
# 独立运行：python3 market_snapshot.py
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("═" * 60)
    print("[SNAPSHOT] Trident Market Snapshot — 独立测试")
    print("═" * 60)

    if not HAS_CCXT:
        print("❌ ccxt 未安装. 请运行: pip install ccxt --break-system-packages")
        sys.exit(1)

    # Try async first; fall back to sync if no event loop
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're inside a running loop (e.g. Jupyter) — use sync
            snap = get_snapshot_sync()
        else:
            snap = asyncio.run(get_snapshot())
    except RuntimeError:
        snap = get_snapshot_sync()

    print()
    print("── 市场摘要 (可直接注入 LLM Prompt) ──")
    print(snap["summary"])
    print()
    print("── 完整 JSON ──")
    import json
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    print()
    print(f"整体状态: {snap['status']}")
