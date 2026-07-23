#!/usr/bin/env python3
"""
Trident Agent MVP — Real-Time News Filter (Ingest-Time)
=========================================================

Replaces the batch `news_ingestion_pipeline.py` for ingest-time filtering.
Call `evaluate_news()` at the moment a news item arrives (WebSocket, Webhook,
polling loop), BEFORE writing to `raw_news`.  The returned dict tells you what
`status`, `is_noise`, and `relevance_score` to insert.

This eliminates the "race condition" where `engine.py` picks up junk news
(`status='PENDING'`) before the batch pipeline has a chance to filter it.

Architecture:
    News Source → evaluate_news() → INSERT INTO raw_news → engine.py
                    │    │                                  (only sees
               L1 rules  L2 Haiku                          status='PENDING'
              (microsec) (≤3 sec)                           clean news)

Two-layer funnel (same semantic as the batch pipeline):
  L1 — Substring keyword matching (zero cost, ~10µs)
        JUNK_WORDS hit → is_noise=1, status='FILTERED'
        CORE_WORDS hit → is_noise=0, status='PENDING', relevance_score=0.90

  L2 — Claude Haiku via OpenRouter (≤3s timeout)
        Only called when L1 is UNCERTAIN.
        Prompt: "Output 1 or 0 — is this actionable for a macro/crypto trader?"
        Timeout/error → conservative: is_noise=1, status='FILTERED'
        Uses `anthropic/claude-haiku-latest` — always the newest version.

Integration example (see also module bottom):
    from realtime_filter import evaluate_news

    result = evaluate_news(news_title, news_content)

    conn.execute(
        "INSERT INTO raw_news (source, content, timestamp, status, "
        "is_noise, relevance_score) VALUES (?, ?, datetime('now','localtime'), "
        "?, ?, ?)",
        (source, content, result["status"], result["is_noise"],
         result["relevance_score"]),
    )

Usage (CLI test):
    python realtime_filter.py "美联储暗示9月降息"
    python realtime_filter.py "BTC空投活动来袭，注册即送100USDT"
"""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ── Timezone ──────────────────────────────────────────────────────────────
TZ_SHANGHAI = timezone(timedelta(hours=8))


def _ts() -> str:
    return datetime.now(TZ_SHANGHAI).strftime("%H:%M:%S")


# ═══════════════════════════════════════════════════════════════════════════
# L1 — Rule-Based Keyword Filter (zero-cost, microsecond latency)
# ═══════════════════════════════════════════════════════════════════════════

# ── JUNK keywords → immediate discard ──
# Hit any of these → is_noise=1, status='FILTERED'.
# Sorted by domain for maintainability; flattened for matching.

JUNK_WORDS: List[str] = [
    # ── 营销 / 空投 / 抽奖 ──
    "空投", "airdrop", "抽奖", "赠金", "红包", "领取", "白名单",
    "giveaway", "lottery", "奖励发放", "福利", "返佣", "邀请返利",
    "注册即送", "开户即送", "新用户", "注册奖励", "邀请码", "推荐码",
    "affiliate", "referral", "bonus", "promo",

    # ── 教程 / 入门 / 科普 ──
    "新手教程", "入门指南", "怎么买", "什么是", "科普", "小课堂",
    "初学者", "小白", "零基础", "tutorial", "beginner", "how to",
    "指南", "攻略", "技巧", "秘籍",

    # ── AMA / 直播 / 预告 ──
    "AMA", "Ask Me Anything", "直播预告", "直播", "webinar",
    "Twitter Space", "社区活动", "meetup", "线上活动",
    "活动预告", "活动时间", "敬请期待",

    # ── NFT / 元宇宙 ──
    "NFT", "mint", "铸造", "盲盒", "元宇宙", "metaverse",
    "GameFi", "链游", "P2E", "土地",

    # ── 测试网 / 主网上线 ──
    "测试网", "testnet", "devnet", "previewnet",
    "主网上线", "mainnet launch", "升级公告", "版本更新",
    "patch", "hotfix", "sprint",

    # ── 治理 / 社区投票 ──
    "社区投票", "治理提案", "governance", "提案通过",
    "DAO", "Snapshot", "投票开始", "社区治理",

    # ── 周报 / 月报 / 技术分析 ──
    "周报", "weekly report", "月报", "monthly report",
    "日报", "daily report", "晨报", "晚报", "早报",
    "复盘", "总结", "回顾", "相关性矩阵", "相关性分析",
    "技术分析", "K线图", "行情分析", "走势分析", "盘面分析",

    # ── 社交媒体噪音 ──
    "涨粉", "粉丝", "follow", "点赞", "转发抽奖",
    "评论有奖", "互动有奖", "打卡",

    # ── 交易所上币公告 ──
    "新币上线", "代币上线", "token listing", "上架公告",
    "下架公告", "delisting", "暂停交易",

    # ── 假消息 / 辟谣 ──
    "辟谣", "假消息", "fake news", "谣言", "rumor",
    "不实信息", "虚假信息",
]

# ── CORE keywords → bypass L2, immediate high-relevance ──

CORE_WORDS: List[str] = [
    # ── 央行 / 货币政策 ──
    "美联储", "FOMC", "Powell", "鲍威尔", "降息", "加息",
    "interest rate", "利率决议", "联邦基金利率", "点阵图",
    "央行", "ECB", "欧洲央行", "BOJ", "日本央行", "BOE", "英国央行",
    "PBOC", "人民银行", "LPR", "MLF", "逆回购", "降准", "存款准备金",
    "量化宽松", "QE", "量化紧缩", "QT", "缩表", "扩表",
    "taper", "缩减购债", "收益率曲线控制", "YCC",
    "负利率", "零利率", "NIRP", "ZIRP",

    # ── 通胀 / 就业 / 经济数据 ──
    "CPI", "PPI", "PCE", "通胀", "核心通胀", "inflation",
    "房价", "房地产", "住房", "房屋销售", "抵押贷款",
    "非农", "NFP", "失业率", "初请失业金", "就业数据",
    "GDP", "PMI", "ISM", "制造业", "服务业", "零售销售",
    "消费者信心", "密歇根", "耐用品订单", "贸易逆差",

    # ── 监管 / 政策 ──
    "SEC", "CFTC", "监管", "合规", "执法", "起诉", "罚款",
    "法案", "立法",
    "stablecoin", "稳定币法案", "MiCA", "FIT21",
    "禁止", "ban", "限制", "restriction",
    "牌照", "许可", "license", "注册",

    # ── 地缘冲突 / 制裁 ──
    "地缘", "战争", "冲突", "制裁", "sanction",
    "中东", "伊朗", "以色列", "俄乌", "乌克兰", "台海", "朝鲜",
    "OPEC", "欧佩克", "原油", "石油", "能源危机",
    "核", "nuclear", "导弹", "军事",

    # ── ETF / 机构资金 ──
    "ETF", "spot ETF", "期货ETF", "比特币ETF", "以太坊ETF",
    "流入", "inflow", "流出", "outflow", "净流入", "净流出",
    "BlackRock", "贝莱德", "Fidelity", "富达", "Grayscale", "灰度",
    "GBTC", "IBIT", "FBTC",
    "资产管理", "AUM", "持仓", "增持", "减持",

    # ── 清算 / 爆仓 / 挤兑 ──
    "清算", "liquidation", "爆仓", "强制平仓", "margin call",
    "挤兑", "bank run", "赎回", "暂停赎回",
    "违约", "default", "破产", "bankruptcy", "暴雷",

    # ── 交易所安全 / 黑客 ──
    "黑客", "hack", "被盗", "exploit", "漏洞", "攻击",
    "安全事件", "私钥", "热钱包", "冷钱包",
    "链上异动", "大额转账", "whale", "巨鲸",

    # ── 总统 / 政策人物 ──
    "特朗普", "拜登", "Trump", "Biden",
    "总统", "大选", "中选", "就职",
    "财长", "财政部长", "Yellen", "耶伦",

    # ── 市场结构 / 流动性 ──
    "流动性", "liquidity", "做市商", "market maker",
    "深度", "滑点", "价差", "波动率", "VIX",
    "轧空", "逼空", "short squeeze", "多头挤压",

    # ── 汇率 / 美债 ──
    "美元指数", "DXY", "美元", "美债", "国债收益率",
    "yield", "收益率倒挂", "期限利差",
    "汇率", "forex", "日元", "欧元", "英镑",
    "去美元化", "de-dollarization", "BRICS",

    # ── 减半 / 供应事件 ──
    "减半", "halving", "区块奖励", "供应冲击",
    "减仓", "仓位", "持仓量", "OI", "open interest",

    # ── 稳定币 ──
    "USDT", "USDC", "DAI", "脱锚", "depeg",
    "稳定币", "Tether", "Circle",

    # ── 紧急/突发信号词 ──
    "紧急", "urgent", "突发", "breaking",
    "闪崩", "crash", "暴跌", "暴涨", "熔断",
]

# Pre-compute lowercase lists (done once at import — O(1) per call)
_JUNK: List[str] = [w.lower() for w in JUNK_WORDS]
_CORE: List[str] = [w.lower() for w in CORE_WORDS]

# Hash-stripping regex (engine dedup prefix like "[hash:1a2b3c4d] ")
_HASH_RE: re.Pattern = re.compile(r"\[hash:[a-fA-F0-9]+\]\s*")


def _l1_classify(text: str) -> Optional[Dict[str, Any]]:
    """Run L1 keyword matching on a news item.

    Returns:
        None         — UNCERTAIN (needs L2)
        dict         — definitive verdict ready to return
    """
    # Strip hash prefix, lowercase, trim
    clean = _HASH_RE.sub("", text).lower()
    # Content can be long; cap at 500 chars for matching
    clean = clean[:500]

    # JUNK check first (conservative: "BTC 空投活动" is still noise)
    for kw in _JUNK:
        if kw in clean:
            return {
                "is_noise": 1,
                "relevance_score": 0.0,
                "status": "FILTERED",
                "layer": "L1",
                "verdict": "JUNK",
                "matched_keyword": kw,
            }

    # CORE check
    for kw in _CORE:
        if kw in clean:
            return {
                "is_noise": 0,
                "relevance_score": 0.90,
                "status": "PENDING",
                "layer": "L1",
                "verdict": "CORE",
                "matched_keyword": kw,
            }

    return None  # UNCERTAIN → escalate to L2


# ═══════════════════════════════════════════════════════════════════════════
# L2 — Claude 3.5 Haiku Binary Classifier (≤3s timeout)
# ═══════════════════════════════════════════════════════════════════════════

# ── Config (env-var overridable) ──
_OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
_OPENROUTER_URL = "https://openrouter.ai/api/v1"

# Default: Claude Haiku latest (auto-tracks newest version via OpenRouter alias).
# Override with REALTIME_FILTER_MODEL.
_FILTER_MODEL = os.getenv(
    "REALTIME_FILTER_MODEL",
    "anthropic/claude-haiku-latest",
)
# Timeout in seconds (L2 must return fast or we bail)
_FILTER_TIMEOUT_S = float(os.getenv("REALTIME_FILTER_TIMEOUT_S", "3.0"))

# Proxy (same pattern as engine.py)
_HTTP_PROXY = os.getenv("HTTP_PROXY", "").strip()

# ── Ultra-minimal prompt (~40 tokens → fast + cheap) ──
_L2_SYSTEM = (
    "You are a financial news filter. Output ONLY a single digit: "
    "1 if the headline is actionable for a macro/crypto trader, "
    "0 if it is noise.\n"
    "Do NOT output any other text, punctuation, or explanation."
)


def _l2_classify_haiku(title: str, content: str = "") -> int:
    """Call Claude 3.5 Haiku for binary classification.  Returns 0 or 1.

    Never raises — all error paths return 0 (conservative: treat as noise).

    Timeout: 3s (configurable via REALTIME_FILTER_TIMEOUT_S).
    """
    if not _OPENROUTER_KEY:
        return 0  # No API key → can't call L2 → conservative

    # Build minimal user message (title is the most signal-dense)
    text = title.strip()
    if content.strip():
        # Add first 150 chars of content for context
        text = f"标题: {text[:200]}\n摘要: {content[:150]}"

    payload = {
        "model": _FILTER_MODEL,
        "messages": [
            {"role": "system", "content": _L2_SYSTEM},
            {"role": "user", "content": text},
        ],
        "max_tokens": 4,          # "1" or "0" is 1 token; 4 is safe margin
        "temperature": 0.0,       # Deterministic
    }
    # NOTE: NOT using response_format={"type":"json_object"} — Anthropic models
    # behave more reliably with plain-text prompts when only a single digit is needed.

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{_OPENROUTER_URL}/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {_OPENROUTER_KEY}",
            "Content-Type": "application/json",
            # OpenRouter headers for reduced latency
            "X-Title": "Trident-RealtimeFilter",
        },
    )

    # SSL context
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = True
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    try:
        ssl_ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
    except Exception:
        pass

    proxy_handler = None
    if _HTTP_PROXY:
        proxy_handler = urllib.request.ProxyHandler(
            {"http": _HTTP_PROXY, "https": _HTTP_PROXY}
        )

    try:
        opener = (
            urllib.request.build_opener(proxy_handler)
            if proxy_handler
            else urllib.request.build_opener()
        )
        resp = opener.open(req, timeout=_FILTER_TIMEOUT_S)
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        print(f"[FILTER] {_ts()} L2 HTTP {e.code} | {err_body}", flush=True)
        return 0
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        print(f"[FILTER] {_ts()} L2 network/timeout | {e}", flush=True)
        return 0

    try:
        body = json.loads(resp.read().decode("utf-8"))
        raw = (body.get("choices", [{}])[0]
               .get("message", {})
               .get("content", "") or "")
        raw = raw.strip()
    except (json.JSONDecodeError, KeyError, IndexError, AttributeError):
        return 0

    # Extract the first "0" or "1" from the response
    m = re.search(r"\b([01])\b", raw)
    if m:
        return int(m.group(1))
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_news(title: str, content: str = "") -> Dict[str, Any]:
    """Filter a single news item at ingest time.

    Call this BEFORE inserting into `raw_news`.  The returned dict gives you
    the correct `status`, `is_noise`, and `relevance_score` to write.

    Args:
        title:   News headline / title.
        content: Optional body text (first ~150 chars used for L2 context).

    Returns:
        {
            "is_noise":         0 or 1,
            "relevance_score":  float 0.0–1.0,
            "status":           "PENDING" or "FILTERED",
            "layer":            "L1" or "L2",
            "verdict":          "CORE" | "JUNK" | "L2_SIGNAL" | "L2_NOISE" | "L2_TIMEOUT",
            "matched_keyword":  str | None (L1 only),
            "elapsed_ms":       float,
        }
    """
    t0 = time.perf_counter()

    # ── L1: keyword matching (microseconds) ──
    l1_result = _l1_classify(title + " " + content[:300])
    if l1_result is not None:
        elapsed = (time.perf_counter() - t0) * 1000
        l1_result["elapsed_ms"] = round(elapsed, 2)
        print(
            f"[FILTER] {_ts()} L1 {l1_result['verdict']:5s} | "
            f"kw='{l1_result['matched_keyword']}' | "
            f"{elapsed:.1f}ms | "
            f"'{title[:60]}'",
            flush=True,
        )
        return l1_result

    # ── L2: Claude 3.5 Haiku (≤3s) ──
    l1_elapsed = (time.perf_counter() - t0) * 1000
    print(
        f"[FILTER] {_ts()} L1 UNCERTAIN ({l1_elapsed:.1f}ms) → "
        f"L2 ({_FILTER_MODEL}) | '{title[:60]}'",
        flush=True,
    )

    t_l2 = time.perf_counter()
    score = _l2_classify_haiku(title, content)
    l2_elapsed = (time.perf_counter() - t_l2) * 1000
    total_elapsed = (time.perf_counter() - t0) * 1000

    if score == 1:
        result: Dict[str, Any] = {
            "is_noise": 0,
            "relevance_score": 0.50,
            "status": "PENDING",
            "layer": "L2",
            "verdict": "L2_SIGNAL",
            "matched_keyword": None,
            "elapsed_ms": round(total_elapsed, 2),
        }
    else:
        result = {
            "is_noise": 1,
            "relevance_score": 0.0,
            "status": "FILTERED",
            "layer": "L2",
            "verdict": "L2_NOISE",
            "matched_keyword": None,
            "elapsed_ms": round(total_elapsed, 2),
        }

    print(
        f"[FILTER] {_ts()} L2 {result['verdict']:9s} | "
        f"Haiku {l2_elapsed:.0f}ms | "
        f"total {total_elapsed:.0f}ms | "
        f"'{title[:60]}'",
        flush=True,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CLI — quick test
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python realtime_filter.py <news headline>")
        print()
        print("Examples:")
        print('  python realtime_filter.py "美联储暗示9月降息"')
        print('  python realtime_filter.py "BTC空投活动来袭"')
        print('  python realtime_filter.py "ETH质押收益率上升"')
        sys.exit(1)

    headline = " ".join(sys.argv[1:])
    result = evaluate_news(headline)
    print()
    print(json.dumps(result, ensure_ascii=False, indent=2))
