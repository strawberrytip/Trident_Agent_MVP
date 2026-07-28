#!/usr/bin/env python3
"""
Trident Agent MVP — 历史决策回测 & 直接分析
============================================

模式:
    python scripts/backtest.py                    # 导出已结算决策 → Excel
    python scripts/backtest.py --days 30          # 最近30天
    python scripts/backtest.py --all              # 全部历史
    python scripts/backtest.py --direct           # 不走 engine，直接调 Kimi K3 分析历史新闻 → 入库 → Excel
    python scripts/backtest.py --direct --dry-run # dry-run: 调 LLM 但不入库，从 stdout 看结果
    python scripts/backtest.py --replay 30        # 注入历史新闻进 raw_news，等 engine 拾取
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

TZ_SHANGHAI = timezone(timedelta(hours=8))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "trident_event_bus.db")

# ── 加载 .env ──
_dotenv_path = os.path.join(BASE_DIR, ".env")
if os.path.isfile(_dotenv_path):
    with open(_dotenv_path, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _k, _v = _k.strip(), _v.strip()
                if _k not in os.environ:
                    os.environ[_k] = _v

# 强影响阈值 — 与前端导出逻辑一致
IMPACT_THRESHOLDS = {"BTC": 2.0, "XAU": 1.0, "GOLD": 1.0, "WTI": 1.5}

BOLD_FORMAT = None  # set at xlsxwriter runtime


def _now() -> str:
    return datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_time(val: str | None) -> str:
    if not val:
        return "—"
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return dt.astimezone(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(val)[:19]


def _pct_str(v: float | None) -> str:
    if v is None:
        return "N/A"
    return f"{v:+.2%}"


# ═══════════════════════════════════════════════════════════════════════════
# Kimi K3 Direct Call (self-contained, no engine dependency)
# ═══════════════════════════════════════════════════════════════════════════

_KIMI_SYSTEM_PROMPT = """
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

═══ 历史回测注意事项 ═══

user prompt 中的 [新闻时间] 是你必须使用的分析参考时间点。
请以该时间点的已知信息做判断，不要假设「未来」会发生什么。

═══ JSON 输出 ═══

输出 JSON（14 个字段，缺一不可）：
{"reasoning_path": "[驱动力]…→[水位博弈]…→[跨资产联动]…→[反共识结论]…",
 "sentiment_score": <float -1.0~1.0>,
 "suggested_action": "<BUY|SELL|HOLD>",
 "reasoning": "<一句精炼结论,<=50字>",
 "market_category": "<CRYPTO|GOLD|OIL|MACRO|OTHER>",
 "target_asset": "<BTC|ETH|XAU|WTI|...|NONE>",
 "prediction_type": "<reversal|continuation|breakout>",
 "event_phase": "<early|mid|late>",
 "market_confirmation": "<positive|negative|unknown>",
 "expected_horizon": "<intraday|1-3d|1w+>",
 "invalidation_condition": "<什么情况下这个判断失效,<=40字>",
 "event_strength": "<low|medium|high>",
 "direct_catalyst": <true|false>,
 "timeframe_match": "<intraday|swing|macro>"}

═══ 铁律 ═══
  * score 严禁为 0.0。中性区 +/-0.03~0.10
  * 方向不确定时 HOLD 是正确答案，不要赌
  * reasoning_path 必须包含四段推演，每段 1-2 句话
  * BTC 在战争/危机/流动性恐慌中 → SELL (纯风险资产，不存在避险属性)
  * 缺少市场数据时 market_confirmation 必须为 "unknown"

只输出 JSON，14 个字段缺一不可。
"""


def _parse_kimi_result(raw_text: str) -> dict:
    """Parse Kimi K3 JSON output with fallback tiers."""

    # Clean markdown wrapping
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    result: dict = {}
    try:
        result = json.loads(cleaned)
        if not isinstance(result, dict):
            raise ValueError("not a dict")
    except (json.JSONDecodeError, ValueError):
        # Fallback 1: repair truncated JSON
        repaired = cleaned.strip()
        if repaired.startswith("{") and not repaired.endswith("}"):
            inner = repaired[1:].strip()
            chunks = [c.strip() for c in inner.split(",") if c.strip()]
            complete = []
            for chunk in chunks:
                try:
                    json.loads("{" + chunk + "}")
                    complete.append(chunk)
                except json.JSONDecodeError:
                    pass
            if complete:
                repaired = "{" + ",".join(complete) + "}"
                try:
                    result = json.loads(repaired)
                except json.JSONDecodeError:
                    pass

        # Fallback 2: regex-extract
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

        # Fallback 3: keywords
        if not result:
            text_lower = cleaned.lower()
            if any(w in text_lower for w in ("利好", "上涨", "看涨", "bullish", "buy")):
                result = {"reasoning_path": "关键词推断 → 偏多信号", "sentiment_score": 0.4,
                           "suggested_action": "BUY", "reasoning": "关键词推断:偏多",
                           "market_category": "OTHER", "target_asset": "NONE",
                           "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}
            elif any(w in text_lower for w in ("利空", "下跌", "看跌", "bearish", "sell", "战争", "制裁")):
                result = {"reasoning_path": "关键词推断 → 偏空信号", "sentiment_score": -0.4,
                           "suggested_action": "SELL", "reasoning": "关键词推断:偏空",
                           "market_category": "OTHER", "target_asset": "NONE",
                           "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}
            else:
                result = {"reasoning_path": "关键词推断 → 中性观望", "sentiment_score": 0.05,
                           "suggested_action": "HOLD", "reasoning": "关键词推断:观望",
                           "market_category": "OTHER", "target_asset": "NONE",
                           "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}

    # ── Defaults for missing metadata fields ──
    _META_DEFAULTS = {
        "prediction_type":        ("reversal", "continuation", "breakout"),
        "event_phase":            ("early", "mid", "late"),
        "market_confirmation":    ("positive", "negative", "unknown"),
        "expected_horizon":       ("intraday", "1-3d", "1w+"),
        "invalidation_condition": "",
        "event_strength":         ("low", "medium", "high"),
        "timeframe_match":        ("intraday", "swing", "macro"),
    }
    for key, valid in _META_DEFAULTS.items():
        if key not in result or not isinstance(result[key], str):
            result[key] = valid[-1] if isinstance(valid, tuple) else valid
        elif isinstance(valid, tuple):
            if result[key].strip().lower() not in valid:
                result[key] = valid[-1]

    # direct_catalyst boolean handling
    dc = result.get("direct_catalyst")
    if isinstance(dc, str):
        result["direct_catalyst"] = dc.strip().lower() == "true"
    elif not isinstance(dc, bool):
        result["direct_catalyst"] = False

    # Ensure sentiment_score is float
    try:
        result["sentiment_score"] = float(result["sentiment_score"])
    except (KeyError, ValueError, TypeError):
        result["sentiment_score"] = 0.05

    return result


def _call_kimi_sync(news_text: str, news_timestamp: str = "",
                    api_key: str = "", model_id: str = "moonshotai/kimi-k3",
                    api_base: str = "https://openrouter.ai/api/v1") -> dict:
    """Call Kimi K3 via OpenRouter — synchronous, for backtesting."""

    # Build user prompt with timestamp
    user_text = news_text[:2000]
    if news_timestamp:
        try:
            ts_dt = datetime.fromisoformat(news_timestamp.replace("Z", "+00:00"))
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M UTC")
        except (ValueError, TypeError):
            ts_str = news_timestamp[:19]
        user_text = f"[新闻时间] {ts_str}\n\n{user_text}"

    payload = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": _KIMI_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        "max_tokens": 2048,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{api_base}/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = True
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    try:
        ssl_ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
    except Exception:
        pass

    # Proxy (OpenRouter only, if set)
    proxy_handler = None
    proxy_addr = os.getenv("HTTP_PROXY", "").strip()
    if proxy_addr and "openrouter" in api_base.lower():
        proxy_handler = urllib.request.ProxyHandler({"http": proxy_addr, "https": proxy_addr})

    # Retry on 429
    retry_delays = [5.0, 10.0, 20.0]
    for attempt, delay in enumerate([0] + retry_delays):
        try:
            if delay > 0:
                time.sleep(delay)
            opener = urllib.request.build_opener(proxy_handler) if proxy_handler else urllib.request.build_opener()
            resp = opener.open(req, timeout=90)
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < len(retry_delays):
                print(f"  HTTP 429, retry {attempt+1}/{len(retry_delays)} after {delay}s", flush=True)
                continue
            raise RuntimeError(f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:300]}") from e
    else:
        raise RuntimeError("HTTP 429 exhausted retries")

    body = json.loads(resp.read().decode("utf-8"))
    raw_text = (body["choices"][0]["message"]["content"] or "").strip()

    if not raw_text:
        finish = body["choices"][0].get("finish_reason", "unknown")
        raise ValueError(f"empty response, finish_reason={finish}")

    return _parse_kimi_result(raw_text)


# ═══════════════════════════════════════════════════════════════════════════
# Stats computation
# ═══════════════════════════════════════════════════════════════════════════

def compute_stats(rows: list[dict]) -> dict:
    n = len(rows)
    if n == 0:
        return {"n": 0}
    wins = sum(1 for r in rows if r.get("is_correct") == "WIN")
    losses = sum(1 for r in rows if r.get("is_correct") == "LOSS")
    decided = wins + losses
    wr = wins / decided if decided > 0 else 0
    pnls = [r.get("forward_pnl") for r in rows if r.get("forward_pnl") is not None]
    avg_pnl = sum(pnls) / len(pnls) if pnls else None
    total_pnl = sum(pnls) if pnls else 0
    if pnls and len(pnls) >= 3:
        mean = avg_pnl
        var = sum((x - mean) ** 2 for x in pnls) / (len(pnls) - 1)
        std = var ** 0.5
        sharpe = mean / std * (55 ** 0.5) if std > 0 else 0
    else:
        std = sharpe = None
    mfes = [r.get("mfe_pct") for r in rows if r.get("mfe_pct") is not None]
    maes = [r.get("mae_pct") for r in rows if r.get("mae_pct") is not None]
    return {
        "n": n, "wins": wins, "losses": losses, "decided": decided,
        "win_rate": wr, "avg_pnl": avg_pnl, "total_pnl": total_pnl,
        "std": std, "sharpe": sharpe,
        "avg_mfe": sum(mfes) / len(mfes) if mfes else None,
        "avg_mae": sum(maes) / len(maes) if maes else None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Excel Export (same format as api_server.py _do_export_excel)
# ═══════════════════════════════════════════════════════════════════════════

def write_signals_sheet(ws, rows: list[dict]) -> None:
    headers = [
        "ID", "时间", "新闻内容", "品种", "方向", "评分", "入场价",
        "最高价", "最低价", "出场价", "最大浮盈%", "最大浮亏%",
        "到达极值(min)", "强影响", "胜负", "标签",
        "Kimi K3 归因", "判定 PnL", "MFE%", "MAE%", "预测类型",
        "事件阶段", "事件强度", "是否为直接催化", "时间周期",
    ]
    n_cols = len(headers)

    for c, h in enumerate(headers):
        ws.write(0, c, h, BOLD_FORMAT)

    for r_idx, row in enumerate(rows, start=1):
        asset = (row.get("target_asset") or "").upper()
        action_raw = (row.get("suggested_action") or "").upper()
        entry = row.get("entry_price")
        exit_p = row.get("exit_price")
        max_p = row.get("max_price")
        min_p = row.get("min_price")
        max_ptime = row.get("max_price_time") or 0
        min_ptime = row.get("min_price_time") or 0
        entry_time_str = row.get("entry_time") or ""

        verdict_raw = (row.get("is_correct") or "").strip().upper()
        if verdict_raw == "WIN":
            verdict = "正确"
        elif verdict_raw == "LOSS":
            verdict = "错误"
        else:
            verdict = verdict_raw or "—"

        # MFE / MAE (same logic as frontend export)
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
                            minutes = round((max_ptime - int(et_dt.timestamp())) / 60, 1)
                            if minutes >= 0:
                                time_to_extreme = str(minutes)
                        except (ValueError, TypeError):
                            pass
                elif action_raw == "SELL":
                    if min_p is not None and min_p > 0:
                        mfe_val = (entry - min_p) / entry * 100
                        mfe_str = f"{mfe_val:+.2f}%"
                    if max_p is not None and max_p > 0:
                        mae_val = (entry - max_p) / entry * 100
                        mae_str = f"{mae_val:+.2f}%"
                    if min_ptime > 0:
                        try:
                            et_dt = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
                            minutes = round((min_ptime - int(et_dt.timestamp())) / 60, 1)
                            if minutes >= 0:
                                time_to_extreme = str(minutes)
                        except (ValueError, TypeError):
                            pass

                if mfe_str != "—":
                    mfe_number = float(mfe_str.replace("%", "").replace("+", ""))
                    threshold = IMPACT_THRESHOLDS.get(asset, 2.0)
                    if mfe_number > threshold:
                        high_impact = "是"
            except Exception:
                pass

        db_mfe = row.get("mfe_pct")
        db_mae = row.get("mae_pct")
        db_pnl = row.get("forward_pnl")

        ws.write(r_idx, 0, row.get("id", ""))
        ws.write(r_idx, 1, _fmt_time(row.get("timestamp")))
        ws.write(r_idx, 2, re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', (row.get("news_text") or "")[:200]))
        ws.write(r_idx, 3, asset)
        ws.write(r_idx, 4, "多" if action_raw == "BUY" else ("空" if action_raw == "SELL" else action_raw))
        ws.write(r_idx, 5, round(row.get("sentiment_score") or 0, 2))
        ws.write(r_idx, 6, entry)
        ws.write(r_idx, 7, max_p)
        ws.write(r_idx, 8, min_p)
        ws.write(r_idx, 9, exit_p)
        ws.write(r_idx, 10, mfe_str)
        ws.write(r_idx, 11, mae_str)
        ws.write(r_idx, 12, time_to_extreme)
        ws.write(r_idx, 13, high_impact)
        ws.write(r_idx, 14, verdict)
        ws.write(r_idx, 15, (row.get("vip_tag") or "").replace("[", "").replace("]", ""))
        ws.write(r_idx, 16, ((row.get("reasoning_path") or row.get("reasoning") or "")[:2000]).strip())
        ws.write(r_idx, 17, f"{db_pnl:+.2f}%" if db_pnl is not None else "—")
        ws.write(r_idx, 18, f"{db_mfe:+.2f}%" if db_mfe is not None else "—")
        ws.write(r_idx, 19, f"{db_mae:+.2f}%" if db_mae is not None else "—")
        ws.write(r_idx, 20, row.get("prediction_type") or "—")
        ws.write(r_idx, 21, row.get("event_phase") or "—")
        ws.write(r_idx, 22, row.get("event_strength") or "—")
        ws.write(r_idx, 23, "是" if row.get("direct_catalyst") else "否")
        ws.write(r_idx, 24, row.get("timeframe_match") or "—")


def write_summary_sheet(ws, all_rows: list[dict]) -> None:

    def _w(row, col, val, fmt=None):
        ws.write(row, col, val, fmt if fmt else None)

    g = compute_stats(all_rows)

    _w(0, 0, "Trident Agent MVP — 历史决策回测", BOLD_FORMAT)
    _w(1, 0, f"生成时间: {_now()}")
    _w(2, 0, f"已结算决策: {g['n']} 条")
    _w(3, 0, "")

    _w(4, 0, "═══ 全局概览 ═══", BOLD_FORMAT)
    overview = [
        ("已结算", g.get("n", 0)),
        ("胜 / 负", f"{g.get('wins', 0)} / {g.get('losses', 0)}"),
        ("胜率", _pct_str(g.get("win_rate"))),
        ("平均 PnL", _pct_str(g.get("avg_pnl"))),
        ("累计 PnL", _pct_str(g.get("total_pnl"))),
        ("年化 Sharpe", f"{g.get('sharpe', 0) or 0:.2f}"),
        ("平均 MFE", _pct_str(g.get("avg_mfe"))),
        ("平均 MAE", _pct_str(g.get("avg_mae"))),
    ]
    for i, (label, value) in enumerate(overview):
        _w(5 + i, 0, label)
        _w(5 + i, 1, value)

    start_row = 14
    _w(start_row, 0, "═══ 按资产 × 方向 ═══", BOLD_FORMAT)
    _w(start_row + 1, 0, "资产"); _w(start_row + 1, 1, "方向")
    _w(start_row + 1, 2, "N"); _w(start_row + 1, 3, "胜率")
    _w(start_row + 1, 4, "平均PnL"); _w(start_row + 1, 5, "Sharpe")
    r = start_row + 2
    groups = {}
    for row in all_rows:
        key = (row.get("target_asset", "").upper(), row.get("suggested_action", "").upper())
        groups.setdefault(key, []).append(row)
    for (asset, action), grp in groups.items():
        s = compute_stats(grp)
        if s.get("n", 0) < 3:
            continue
        _w(r, 0, asset); _w(r, 1, action); _w(r, 2, s["n"])
        _w(r, 3, _pct_str(s.get("win_rate")))
        _w(r, 4, _pct_str(s.get("avg_pnl")))
        _w(r, 5, f"{s.get('sharpe', 0) or 0:.2f}")
        r += 1

    r += 1
    _w(r, 0, "═══ 按预测类型 ═══", BOLD_FORMAT)
    _w(r + 1, 0, "类型"); _w(r + 1, 1, "N"); _w(r + 1, 2, "胜率"); _w(r + 1, 3, "平均PnL")
    r += 2
    for pt in ("continuation", "reversal", "breakout"):
        grp = [x for x in all_rows if x.get("prediction_type", "").lower() == pt]
        s = compute_stats(grp)
        if s.get("n", 0) < 3:
            continue
        _w(r, 0, pt); _w(r, 1, s["n"]); _w(r, 2, _pct_str(s.get("win_rate"))); _w(r, 3, _pct_str(s.get("avg_pnl")))
        r += 1

    r += 1
    _w(r, 0, "═══ 按事件强度 ═══", BOLD_FORMAT)
    _w(r + 1, 0, "强度"); _w(r + 1, 1, "N"); _w(r + 1, 2, "胜率"); _w(r + 1, 3, "平均PnL")
    r += 2
    for es in ("low", "medium", "high"):
        grp = [x for x in all_rows if x.get("event_strength", "").lower() == es]
        s = compute_stats(grp)
        if s.get("n", 0) < 3:
            continue
        _w(r, 0, es); _w(r, 1, s["n"]); _w(r, 2, _pct_str(s.get("win_rate"))); _w(r, 3, _pct_str(s.get("avg_pnl")))
        r += 1


# ═══════════════════════════════════════════════════════════════════════════
# Export settled decisions → xlsx
# ═══════════════════════════════════════════════════════════════════════════

def do_export_xlsx(days: int | None = None, extra_rows: list[dict] | None = None) -> str:
    """Export settled decisions as xlsx. extra_rows: direct-mode results to include."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")

    where_clause = "1=1"
    if days is not None and days > 0:
        where_clause = f"date(ad.created_at) >= date('now', 'localtime', '-{days} days')"
    elif days != 99999:
        where_clause = "date(ad.created_at) >= date('now', 'localtime', '-5 days')"

    sql = f"""
        SELECT
            ad.id,
            rn.content AS news_text,
            rn.timestamp,
            ad.sentiment_score,
            ad.suggested_action,
            ad.reasoning_path,
            ad.reasoning,
            ad.target_asset,
            ad.vip_tag,
            ad.entry_price, ad.exit_price, ad.max_price, ad.min_price,
            ad.max_price_time, ad.min_price_time, ad.entry_time,
            ad.is_correct,
            ad.mfe_pct, ad.mae_pct, ad.forward_pnl, ad.mfe_time_mins,
            ad.prediction_type, ad.event_phase, ad.market_confirmation,
            ad.expected_horizon, ad.event_strength, ad.direct_catalyst, ad.timeframe_match,
            ad.market_category, ad.decision_context, ad.extra_models_consensus,
            ad.created_at
        FROM ai_decisions ad
        INNER JOIN raw_news rn ON rn.id = ad.news_id
        WHERE {where_clause}
          AND ad.entry_price IS NOT NULL
        ORDER BY ad.id DESC
    """
    rows = [dict(r) for r in conn.execute(sql).fetchall()]
    conn.close()

    # Merge extra rows from direct mode (if any)
    if extra_rows:
        rows = extra_rows + rows

    if not rows:
        print(f"[{_now()}] ⚠️ 无已结算决策数据。")
        return ""

    import xlsxwriter
    global BOLD_FORMAT

    date_str = datetime.now(TZ_SHANGHAI).strftime("%Y%m%d")
    if days is not None and days > 0:
        date_str = f"{date_str}_d{days}"
    fname = os.path.join(BASE_DIR, f"backtest_{date_str}.xlsx")

    wb = xlsxwriter.Workbook(fname)
    BOLD_FORMAT = wb.add_format({"bold": True})

    write_signals_sheet(wb.add_worksheet("Signals"), rows)
    write_summary_sheet(wb.add_worksheet("Summary"), rows)

    wb.close()
    return fname


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1 — DeepSeek Batch Filter
# ═══════════════════════════════════════════════════════════════════════════

_HAIKU_FILTER_PROMPT = """You are a macro/crypto trading backtest curator. Review the news list below.
Each item is formatted as: ID|TIMESTAMP|HEADLINE

Your job:
- Select items that would be genuinely actionable for a macro/crypto trader at that point in time.
- Actionable = events that can move BTC, ETH, gold, or oil prices (rate decisions, CPI, wars, ETF flows, regulation, sanctions, liquidations, major hacks, etc.)
- NOT actionable = marketing, airdrops, tutorials, AMAs, routine exchange listings, minor alts, NFT hype, technical analysis/commentary, community votes.

Output a JSON object with a single key "selected_ids" containing an array of integer IDs you recommend.
Example: {"selected_ids": [3, 7, 12, 18, 25]}

Be selective — only pick the ones that could genuinely generate a trading signal.
Do NOT output any other text."""


def _haiku_batch_filter(news_rows: list[dict], api_key: str) -> list[int]:
    """Send all headlines to Claude DeepSeek in one batch, get back selected IDs."""

    if not news_rows:
        return []

    # Build compact bullet list: "ID|TIMESTAMP|HEADLINE"
    lines: list[str] = []
    for r in news_rows:
        nid = r["news_id"]
        ts = (r["timestamp"] or "")[:16]
        text = re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', (r["content"] or ""))[:120]
        lines.append(f"{nid}|{ts}|{text}")

    news_bullet = "\n".join(lines)
    total_chars = len(news_bullet)
    print(f"[{_now()}] 📋 Phase 1: DeepSeek 批量筛选 {len(news_rows)} 条 (共 {total_chars} chars)")

    payload = {
        "model": "deepseek/deepseek-chat",
        "messages": [
            {"role": "system", "content": _HAIKU_FILTER_PROMPT},
            {"role": "user", "content": news_bullet},
        ],
        "max_tokens": 1024,
        "temperature": 0.0,
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    ssl_ctx = ssl.create_default_context()
    try:
        ssl_ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
    except Exception:
        pass

    proxy_handler = None
    proxy_addr = os.getenv("HTTP_PROXY", "").strip()
    if proxy_addr:
        proxy_handler = urllib.request.ProxyHandler({"http": proxy_addr, "https": proxy_addr})

    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(3)
            opener = urllib.request.build_opener(proxy_handler) if proxy_handler else urllib.request.build_opener()
            resp = opener.open(req, timeout=45)
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                print(f"  DeepSeek HTTP 429, retry {attempt+1}/2")
                continue
            raise RuntimeError(f"DeepSeek HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:300]}") from e
    else:
        raise RuntimeError("DeepSeek HTTP 429 exhausted")

    body = json.loads(resp.read().decode("utf-8"))
    raw = (body["choices"][0]["message"]["content"] or "").strip()

    # Parse JSON
    try:
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        parsed = json.loads(cleaned)
        ids = parsed.get("selected_ids", [])
        if isinstance(ids, list) and ids:
            return [int(x) for x in ids if isinstance(x, (int, float))]
    except (json.JSONDecodeError, ValueError, KeyError):
        pass

    # Fallback: extract numbers from text
    nums = re.findall(r'\b\d+\b', raw)
    seen: set[int] = set()
    result: list[int] = []
    for n in nums:
        nid = int(n)
        if nid not in seen:
            seen.add(nid)
            result.append(nid)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2 — Kimi K3 Deep Analysis (one-by-one on filtered subset)
# ═══════════════════════════════════════════════════════════════════════════

def do_direct() -> str:
    """Two-phase backtest → Excel. 不入库，只出 xlsx。

    Phase 1: 所有新闻 → Claude DeepSeek (1 次调用) → 筛选有回测价值的 ID
    Phase 2: 命中新闻 → Kimi K3 逐条深挖 → 直接生成 Excel
    """
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        print("[ERROR] OPENROUTER_API_KEY not set in .env")
        return ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")

    all_rows = conn.execute("""
        SELECT rn.id AS news_id, rn.content, rn.timestamp
        FROM raw_news rn
        WHERE rn.id IN (SELECT DISTINCT news_id FROM ai_decisions WHERE settled = 1)
          AND rn.status IN ('PENDING', 'DONE', 'PROCESSING')
        ORDER BY rn.id ASC
    """).fetchall()

    if not all_rows:
        print("[ERROR] 没有可分析的新闻。先跑 engine 产生一些 settled ai_decisions。")
        conn.close()
        return ""

    conn.close()

    n_total = len(all_rows)
    print(f"[{_now()}] 📊 直接分析模式: {n_total} 条历史新闻")
    print(f"[{_now()}] Phase 1: DeepSeek 批量筛选 (1 次 API 调用)")
    print(f"[{_now()}] Phase 2: Kimi K3 逐条深挖 (仅命中新闻)")
    print("-" * 60)

    # ═══ Phase 1: DeepSeek filter ═══
    selected_ids = []
    haiku_ok = True
    try:
        selected_ids = _haiku_batch_filter(all_rows, api_key)
    except Exception as e:
        print(f"[{_now()}] ❌ DeepSeek 筛选失败: {type(e).__name__}: {str(e)[:200]}")
        haiku_ok = False

    if not selected_ids:
        print(f"[{_now()}] ⚠️ DeepSeek 无结果，降级取前 20 条")
        haiku_ok = False

    if not haiku_ok:
        selected_ids = [r["news_id"] for r in all_rows][:20]

    all_by_id = {r["news_id"]: r for r in all_rows}
    filtered_rows = [all_by_id[nid] for nid in selected_ids if nid in all_by_id]
    skipped = n_total - len(filtered_rows)

    print(f"[{_now()}] ✅ Phase 1 完成: {len(filtered_rows)} 条入选, {skipped} 条跳过")
    print(f"[{_now()}] 💰 节省 Kimi K3 token: ~{skipped / max(n_total, 1) * 100:.0f}%")
    print("-" * 60)

    # ═══ Phase 2: Kimi K3 deep analysis ═══
    n_kimi = len(filtered_rows)
    results: list[dict] = []
    success = 0
    fail = 0

    for i, r in enumerate(filtered_rows, start=1):
        news_id = r["news_id"]
        news_text = re.sub(r'\[hash:[a-fA-F0-9]+\]\s*', '', (r["content"] or ""))[:500]
        ts = r["timestamp"] or ""

        print(f"\n[{i}/{n_kimi}] ID={news_id} | {ts[:19]} | {news_text[:80]}...")

        try:
            llm_result = _call_kimi_sync(
                news_text=news_text,
                news_timestamp=ts,
                api_key=api_key,
            )
            success += 1

            print(f"  → {llm_result.get('suggested_action', '?')} "
                  f"| score={llm_result.get('sentiment_score', 0):+.2f} "
                  f"| {llm_result.get('target_asset', '?')}"
                  f" | {llm_result.get('prediction_type', '?')}"
                  f" | strength={llm_result.get('event_strength', '?')}")
            print(f"  → {llm_result.get('reasoning', '')[:80]}")
            rp = llm_result.get('reasoning_path', '')
            if rp:
                for seg in rp.replace('→', '\n    →').split('\n'):
                    print(f"    {seg.strip()}")

            results.append({
                "id": f"D{news_id}",
                "timestamp": ts,
                "news_text": news_text,
                "target_asset": llm_result.get("target_asset", "NONE").upper(),
                "suggested_action": llm_result.get("suggested_action", "HOLD"),
                "sentiment_score": llm_result.get("sentiment_score", 0),
                "entry_price": None, "exit_price": None, "max_price": None, "min_price": None,
                "max_price_time": 0, "min_price_time": 0, "entry_time": "",
                "is_correct": "—",
                "mfe_pct": None, "mae_pct": None, "forward_pnl": None,
                "reasoning_path": llm_result.get("reasoning_path", ""),
                "reasoning": llm_result.get("reasoning", ""),
                "vip_tag": "DIRECT",
                "prediction_type": llm_result.get("prediction_type", ""),
                "event_phase": llm_result.get("event_phase", ""),
                "event_strength": llm_result.get("event_strength", ""),
                "direct_catalyst": llm_result.get("direct_catalyst", False),
                "timeframe_match": llm_result.get("timeframe_match", ""),
                "market_confirmation": llm_result.get("market_confirmation", ""),
                "market_category": llm_result.get("market_category", ""),
            })

        except Exception as e:
            fail += 1
            print(f"  ❌ {type(e).__name__}: {str(e)[:200]}")

        if i < n_kimi:
            time.sleep(8)

    print(f"\n{'-' * 60}")
    print(f"[{_now()}] ✅ 分析完成: {success} 成功, {fail} 失败")
    print(f"[{_now()}] 📊 {n_total} → DeepSeek → {n_kimi} → Kimi K3 → {success}")

    fname = do_export_xlsx(days=99999, extra_rows=results)
    return fname


# ═══════════════════════════════════════════════════════════════════════════
# Replay mode: inject history into raw_news for engine pickup
# ═══════════════════════════════════════════════════════════════════════════

def replay_history(limit: int = 30) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    rows = conn.execute("""
        SELECT DISTINCT rn.id AS news_id, rn.content, rn.timestamp
        FROM raw_news rn
        JOIN ai_decisions ad ON ad.news_id = rn.id
        WHERE ad.settled = 1
        ORDER BY rn.id DESC LIMIT ?
    """, (limit,)).fetchall()
    inserted = 0
    for r in rows:
        h = hashlib.sha256((r["content"] + "_REPLAY").encode()).hexdigest()[:16]
        # 保留原始新闻时间戳，让 AI 以历史时间点视角分析
        orig_ts = r["timestamp"] or datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO raw_news (source, content, timestamp, status, is_noise, relevance_score)"
            " VALUES (?, ?, ?, 'PENDING', 0, 0.90);",
            (f"REPLAY", f"[hash:{h}] {r['content'][:500]}", orig_ts),
        )
        inserted += 1
    conn.commit()
    conn.close()
    print(f"[{_now()}] ✅ 回放: {inserted} 条历史新闻已注入为 PENDING，engine 将自动拾取")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    os.chdir(BASE_DIR)

    if "--replay" in sys.argv:
        arg_idx = sys.argv.index("--replay")
        limit = int(sys.argv[arg_idx + 1]) if arg_idx + 1 < len(sys.argv) else 30
        replay_history(limit)
        return

    if "--direct" in sys.argv:
        fname = do_direct()
        if fname:
            print(f"\n[{_now()}] ✅ Excel 回测报告已生成: {fname}")
            print(f"     → Signals 页: 交易明细")
            print(f"     → Summary 页: 统计汇总")
        return

    days: int | None = None
    if "--all" in sys.argv:
        days = 99999
    elif "--days" in sys.argv:
        arg_idx = sys.argv.index("--days")
        days = int(sys.argv[arg_idx + 1]) if arg_idx + 1 < len(sys.argv) else 5

    fname = do_export_xlsx(days)
    if fname:
        print(f"[{_now()}] ✅ Excel 回测报告已生成: {fname}")
        print(f"     → Signals 页: 交易明细")
        print(f"     → Summary 页: 统计汇总")


if __name__ == "__main__":
    main()
