"""Unified price sources — Gold (Sina/EastMoney) + BTC (Binance REST) + WTI.

Leaf module: depends only on stdlib.
"""

from __future__ import annotations

import json
import urllib.request


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
