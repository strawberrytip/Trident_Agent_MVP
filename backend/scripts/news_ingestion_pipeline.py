#!/usr/bin/env python3
"""
Trident Agent MVP — Multi-Layer News Ingestion Pipeline
=========================================================

A standalone data-cleaning worker that sits UPSTREAM of the Hermes Research Agent.
It reads raw news from `raw_news` (produced by engine.py), filters out 90%+ noise
through a three-layer funnel, and tags surviving news with `relevance_score` and
`is_noise` flags.  Hermes tools then query only `is_noise = 0` rows.

Architecture (absolute isolation):
    engine.py  ──► raw_news  ──►  [THIS PIPELINE]  ──►  raw_news (enriched)
                                        │
                                   Hermes tools read is_noise=0 only

Layers:
  L1 — Rule-based keyword filter (zero cost, sub-millisecond)
        JUNK_KEYWORDS → discard immediately
        CORE_KEYWORDS → bypass L2, tag as high-relevance

  L2 — Lightweight LLM intent scorer (~$0.0002/call)
        Cheap model binary-classifies uncertain headlines.
        Prompt: "Is this headline actionable for a macro/crypto trader? 1 or 0."

  L3 — DB write
        Updates raw_news.relevance_score and raw_news.is_noise.

Usage:
    python backend/scripts/news_ingestion_pipeline.py
    python backend/scripts/news_ingestion_pipeline.py --hours 2 --batch-size 80
    python backend/scripts/news_ingestion_pipeline.py --dry-run   # L1 only, no LLM cost
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import ssl
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path resolution — find the project root
# ---------------------------------------------------------------------------

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.dirname(_SCRIPT_DIR)          # backend/
_PROJECT_DIR = os.path.dirname(_BACKEND_DIR)          # Trident_Agent_MVP/

# Allow importing from the hermes package
sys.path.insert(0, os.path.join(_BACKEND_DIR, "src_python"))

# Load .env from project root
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_DIR, ".env"))
except ImportError:
    pass

DB_PATH = os.path.join(_BACKEND_DIR, "trident_event_bus.db")

TZ_SHANGHAI = timezone(timedelta(hours=8))


def _now() -> str:
    return datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# API config — lightweight intent scorer (cheap model, binary output)
# ---------------------------------------------------------------------------

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Claude 3.5 Haiku — 低延迟 (~400ms), 中英文原生支持, 二进制分类准确率高
# 备选: moonshotai/moonshot-v1-8k (更便宜), deepseek/deepseek-chat
INTENT_SCORER_MODEL = os.getenv(
    "PIPELINE_INTENT_MODEL",
    "anthropic/claude-3.5-haiku",
)

# Proxy support (same pattern as engine.py)
_HTTP_PROXY = os.getenv("HTTP_PROXY", "").strip()

# ---------------------------------------------------------------------------
# L1 — Rule-Based Keyword Filter
# ---------------------------------------------------------------------------

# ── JUNK keywords → immediate discard ──
# 这些词命中任一即判为噪音，跳过 L2 直接标记 is_noise=1。
# 列表按领域分组便于维护，实际匹配时扁平化处理。

JUNK_KEYWORDS: List[str] = [
    # ── 营销 / 空投 / 抽奖 ──
    "空投", "airdrop", "抽奖", "赠金", "红包", "领取", "白名单",
    "giveaway", "lottery", "奖励发放", "福利", "返佣", "邀请返利",
    "注册即送", "开户即送", "新用户", "注册奖励", "邀请码", "推荐码",
    "affiliate", "referral", "bonus", "promo",

    # ── 教程 / 入门 / 科普 (无交易信号) ──
    "新手教程", "入门指南", "怎么买", "什么是", "科普", "小课堂",
    "初学者", "小白", "零基础", "tutorial", "beginner", "how to",
    "指南", "攻略", "技巧", "秘籍",

    # ── AMA / 直播 / 预告 (纯粹的社区运营) ──
    "AMA", "Ask Me Anything", "直播预告", "直播", "webinar",
    "Twitter Space", "社区活动", "meetup", "线上活动",
    "活动预告", "活动时间", "敬请期待",

    # ── NFT / 元宇宙 (低价值叙事，除非与宏观联动) ──
    "NFT", "mint", "铸造", "盲盒", "元宇宙", "metaverse",
    "GameFi", "链游", "P2E", "土地",

    # ── 测试网 / 主网上线 (技术公告，无即时价格影响) ──
    "测试网", "testnet", "devnet", "previewnet",
    "主网上线", "mainnet launch", "升级公告", "版本更新",
    "patch", "hotfix", "sprint",

    # ── 治理 / 社区投票 ──
    "社区投票", "治理提案", "governance", "提案通过",
    "DAO", "Snapshot", "投票开始", "社区治理",

    # ── 周报 / 月报 (非突发事件) ──
    "周报", "weekly report", "月报", "monthly report",
    "日报", "daily report", "晨报", "晚报", "早报",
    "复盘", "总结", "回顾", "相关性矩阵", "相关性分析",
    "技术分析", "K线图", "行情分析", "走势分析", "盘面分析",

    # ── 社交媒体噪音 ──
    "涨粉", "粉丝", "follow", "点赞", "转发抽奖",
    "评论有奖", "互动有奖", "打卡",

    # ── 交易所上币公告 (除非是顶级交易所) ──
    "新币上线", "代币上线", "token listing", "上架公告",
    "下架公告", "delisting", "暂停交易",

    # ── 假消息 / 辟谣 / 争议 ──
    "辟谣", "假消息", "fake news", "谣言", "rumor",
    "不实信息", "虚假信息",
]

# ── CORE keywords → bypass L2, immediate high-relevance ──
# 命中任一即跳过 L2 LLM，直接赋予高 relevance_score。

CORE_KEYWORDS: List[str] = [
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
    "众议院", "参议院", "国会", "法案", "立法",
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

    # ── 总统 / 政策人物 (影响市场的政治事件) ──
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

# Compile regex for efficiency — match whole words where possible
# For Chinese, we do substring matching since word boundaries don't apply
# For English, we match case-insensitive substrings

# Pre-process: lowercase everything for case-insensitive matching
_JUNK_LOWER = [kw.lower() for kw in JUNK_KEYWORDS]
_CORE_LOWER = [kw.lower() for kw in CORE_KEYWORDS]

# Regex to strip engine dedup hash prefixes: [hash:1a2b3c4d...]
_HASH_RE = re.compile(r"\[hash:[a-fA-F0-9]+\]\s*")


def _keyword_match(text_lower: str, keywords_lower: List[str]) -> Tuple[bool, Optional[str]]:
    """Return (matched, first_matching_keyword).  O(n*k) but k ~ 200 and text ~ 200 chars.  Fast enough."""
    for kw in keywords_lower:
        if kw in text_lower:
            return True, kw
    return False, None


# ---------------------------------------------------------------------------
# L2 — Lightweight LLM Intent Scorer
# ---------------------------------------------------------------------------

# Minimal binary-classification prompt — designed for cheap models.
# Must fit in ~100 tokens to minimise cost.
_INTENT_SCORER_SYSTEM_PROMPT = """\
你是一个金融新闻过滤器。判断一条新闻标题对宏观交易员或加密货币交易员是否有即时交易价值。

规则:
- 涉及央行政策、利率、通胀数据、监管行动、地缘冲突、ETF资金流、交易所安全事件、大额清算 → 1
- 纯营销活动、空投、NFT、社区投票、教程、周报、交易所上币公告 → 0

只输出 1 或 0，不要其他任何文字。"""


def _call_intent_scorer(news_title: str, news_content: str = "") -> int:
    """Call a lightweight LLM to binary-classify one headline.

    Returns 1 (relevant/signal) or 0 (noise).  Never raises — on any failure,
    defaults to 0 (conservative: don't let a broken scorer pass garbage).
    """
    # Truncate — the L2 scorer only needs the title + first 200 chars
    combined = f"标题: {news_title[:200]}\n摘要: {news_content[:200]}"

    payload = {
        "model": INTENT_SCORER_MODEL,
        "messages": [
            {"role": "system", "content": _INTENT_SCORER_SYSTEM_PROMPT},
            {"role": "user", "content": combined},
        ],
        "max_tokens": 4,       # "1" or "0" is 1 token
        "temperature": 0.0,    # Deterministic
        "response_format": {"type": "json_object"},
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{OPENROUTER_BASE_URL}/chat/completions",
        data=data,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
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

    proxy_handler = None
    if _HTTP_PROXY:
        proxy_handler = urllib.request.ProxyHandler({"http": _HTTP_PROXY, "https": _HTTP_PROXY})

    try:
        opener = urllib.request.build_opener(proxy_handler) if proxy_handler else urllib.request.build_opener()
        resp = opener.open(req, timeout=10)  # 10s timeout for lightweight model
    except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
        err_str = str(e)[:120]
        # Rate-limit / timeout → sleep briefly and return 0 (conservative)
        if "429" in err_str:
            time.sleep(2.0)
        return 0

    try:
        body = json.loads(resp.read().decode("utf-8"))
        text = (body["choices"][0]["message"]["content"] or "").strip()
    except (json.JSONDecodeError, KeyError, IndexError):
        return 0

    # Extract the first digit
    m = re.search(r"\b([01])\b", text)
    if m:
        return int(m.group(1))
    return 0


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

def _migrate_schema(db_path: str = DB_PATH) -> None:
    """Add relevance_score and is_noise columns to raw_news.
    Idempotent — safe to call every pipeline run.
    Does NOT touch any existing column or constraint.
    """
    conn = sqlite3.connect(db_path)
    try:
        for col, col_def, default_val in [
            ("relevance_score", "REAL    DEFAULT 0.0", 0.0),
            ("is_noise",        "INTEGER DEFAULT 0",   0),
        ]:
            try:
                conn.execute(
                    f"ALTER TABLE raw_news ADD COLUMN {col} {col_def};"
                )
                # Backfill existing rows
                conn.execute(
                    f"UPDATE raw_news SET {col} = ? WHERE {col} IS NULL",
                    (default_val,),
                )
            except sqlite3.OperationalError:
                pass  # Column already exists
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# NewsIngestionPipeline
# ---------------------------------------------------------------------------

class NewsIngestionPipeline:
    """Three-layer news filtration system.

    Usage::

        pipeline = NewsIngestionPipeline()
        stats = pipeline.run(hours=1, batch_size=50)
        print(f"Saved {stats['api_cost_est']} in LLM costs")
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._stats: Dict[str, int] = {
            "total_scanned": 0,
            "l1_junk": 0,
            "l1_core": 0,
            "l2_called": 0,
            "l2_signal": 0,
            "l2_noise": 0,
            "l3_written": 0,
            "api_calls": 0,
            "api_errors": 0,
        }
        # Ensure schema is ready
        _migrate_schema(self.db_path)

    # ------------------------------------------------------------------
    # L1 — Rule-based filter
    # ------------------------------------------------------------------

    def _l1_classify(self, title: str, content: str) -> Tuple[str, float, Optional[str]]:
        """Classify a news item at the rule layer.

        Returns:
            (verdict, relevance_score, matched_keyword)

            verdict ∈ {"JUNK", "CORE", "UNCERTAIN"}
        """
        # Build searchable text: title is most important, content adds context
        # but skip overly long content to keep matching fast.
        # Strip engine dedup hash prefixes like "[hash:1a2b3c4d] " before matching.
        search_text = _HASH_RE.sub("", f"{title} {content[:300]}").lower()

        # 1. Check JUNK first (conservative: if it's junk AND core, junk wins —
        #    a "BTC 空投活动" is still marketing noise, not a trading signal)
        is_junk, junk_kw = _keyword_match(search_text, _JUNK_LOWER)
        if is_junk:
            return "JUNK", 0.0, junk_kw

        # 2. Check CORE
        is_core, core_kw = _keyword_match(search_text, _CORE_LOWER)
        if is_core:
            return "CORE", 0.90, core_kw

        return "UNCERTAIN", 0.0, None

    # ------------------------------------------------------------------
    # L2 — LLM intent scorer
    # ------------------------------------------------------------------

    def _l2_score(self, title: str, content: str) -> Tuple[int, float]:
        """Score an uncertain headline via lightweight LLM.

        Returns:
            (is_relevant, confidence)

            is_relevant ∈ {0, 1}
            confidence is always 0.5 for now (binary model has no logprobs)
        """
        self._stats["api_calls"] += 1
        try:
            score = _call_intent_scorer(title, content)
            if score == 1:
                self._stats["l2_signal"] += 1
                return 1, 0.50
            else:
                self._stats["l2_noise"] += 1
                return 0, 0.50
        except Exception:
            self._stats["api_errors"] += 1
            return 0, 0.0  # Conservative: errors → noise

    # ------------------------------------------------------------------
    # L3 — Write results back to DB
    # ------------------------------------------------------------------

    def _l3_store(self, news_id: int, relevance_score: float, is_noise: int) -> None:
        """Update a single raw_news row with filtration results."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "UPDATE raw_news SET relevance_score = ?, is_noise = ? WHERE id = ?",
                (round(relevance_score, 4), is_noise, news_id),
            )
            conn.commit()
            self._stats["l3_written"] += 1
        finally:
            conn.close()

    def _l3_store_batch(self, updates: List[Tuple[int, float, int]]) -> int:
        """Batch-update multiple rows in a single transaction.  Returns count written."""
        if not updates:
            return 0
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executemany(
                "UPDATE raw_news SET relevance_score = ?, is_noise = ? WHERE id = ?",
                [(round(score, 4), is_noise, nid) for nid, score, is_noise in updates],
            )
            conn.commit()
            self._stats["l3_written"] += len(updates)
            return len(updates)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Fetch unprocessed news
    # ------------------------------------------------------------------

    def _fetch_unprocessed(
        self, hours: int = 1, limit: int = 200
    ) -> List[Dict[str, Any]]:
        """Read raw_news rows that haven't been filtered yet.

        A row is 'unprocessed' if is_noise is still 0 AND relevance_score is 0
        (the migration default).  Once the pipeline tags it, it won't be picked
        up again on the next run.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """SELECT id, source, content, timestamp
                   FROM raw_news
                   WHERE timestamp >= datetime('now', 'localtime', ?)
                     AND is_noise = 0
                     AND relevance_score = 0.0
                     AND status IN ('DONE', 'PENDING')
                   ORDER BY timestamp DESC
                   LIMIT ?""",
                (f"-{hours} hours", limit),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Stats / reporting
    # ------------------------------------------------------------------

    @property
    def api_cost_est(self) -> str:
        """Estimate API cost for the L2 scorer.
        moonshot-v1-8k: ~$0.20/M input tokens, ~$0.20/M output tokens.
        Each call is ~150 input tokens + 1 output → ~$0.00003/call.
        """
        cost = self._stats["api_calls"] * 0.000_03
        if cost < 0.01:
            return f"~${cost:.6f}"
        return f"~${cost:.4f}"

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def run(
        self,
        hours: float = 1.0,
        batch_size: int = 50,
        rate_limit_rps: float = 5.0,  # max LLM calls per second
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute the three-layer filtration pipeline.

        Args:
            hours:          How far back to scan (default 1 = only the last hour).
            batch_size:     Max news items to process per run.
            rate_limit_rps: Max L2 LLM calls per second (avoids rate-limiting).
            dry_run:        If True, run L1 only — no LLM calls, no DB writes.

        Returns:
            Dict with stats, cost estimate, and timing.
        """
        t0 = time.time()
        self._stats = {k: 0 for k in self._stats}  # Reset

        print(f"[PIPELINE] ═══ {_now()} — Starting ingestion run ═══")
        print(f"[PIPELINE]   Window: last {hours}h | Batch: {batch_size} | "
              f"Rate limit: {rate_limit_rps} rps | Dry run: {dry_run}")
        print(f"[PIPELINE]   L2 Model: {INTENT_SCORER_MODEL}")

        # ── Fetch ──
        rows = self._fetch_unprocessed(hours=hours, limit=batch_size)
        self._stats["total_scanned"] = len(rows)
        print(f"[PIPELINE]   Fetched {len(rows)} unprocessed news items")

        if not rows:
            print(f"[PIPELINE]   Nothing to do — all caught up.")
            return {**self._stats, "duration_s": round(time.time() - t0, 1), "api_cost_est": self.api_cost_est}

        # ── L1 pass (zero cost, run on all rows first) ──
        l1_results: List[Tuple[Dict, str, float, Optional[str]]] = []
        for row in rows:
            title = (row.get("content") or "")[:150]
            # content = title here — raw_news.content IS the full text
            verdict, score, kw = self._l1_classify(title, "")

            if verdict == "JUNK":
                self._stats["l1_junk"] += 1
            elif verdict == "CORE":
                self._stats["l1_core"] += 1

            l1_results.append((row, verdict, score, kw))

        # ── Immediate L3 writes for L1-decided items ──
        immediate_writes: List[Tuple[int, float, int]] = []
        uncertain: List[Tuple[Dict, str, float]] = []

        for row, verdict, score, kw in l1_results:
            if verdict == "JUNK":
                immediate_writes.append((row["id"], 0.0, 1))  # is_noise=1
            elif verdict == "CORE":
                immediate_writes.append((row["id"], score, 0))  # is_noise=0
            else:
                uncertain.append((row, verdict, score))

        if not dry_run and immediate_writes:
            self._l3_store_batch(immediate_writes)

        print(f"[PIPELINE]   L1 done | JUNK={self._stats['l1_junk']} "
              f"CORE={self._stats['l1_core']} UNCERTAIN={len(uncertain)}")

        # ── L2 pass (LLM scoring — only for UNCERTAIN items) ──
        if uncertain and not dry_run:
            print(f"[PIPELINE]   L2 starting — {len(uncertain)} items need LLM scoring...")
            l2_writes: List[Tuple[int, float, int]] = []
            min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0

            for i, (row, _, _) in enumerate(uncertain):
                # Rate limiting
                if i > 0 and min_interval > 0:
                    time.sleep(min_interval)

                title = (row.get("content") or "")[:150]
                is_relevant, confidence = self._l2_score(title, "")

                if is_relevant:
                    l2_writes.append((row["id"], 0.50, 0))   # passed L2, is_noise=0
                else:
                    l2_writes.append((row["id"], 0.0, 1))     # L2 says noise

                # Progress every 20 items
                if (i + 1) % 20 == 0:
                    print(f"[PIPELINE]     ... {i+1}/{len(uncertain)} scored "
                          f"(signal={self._stats['l2_signal']}, noise={self._stats['l2_noise']})")

            if l2_writes:
                self._l3_store_batch(l2_writes)

            print(f"[PIPELINE]   L2 done | signal={self._stats['l2_signal']} "
                  f"noise={self._stats['l2_noise']} errors={self._stats['api_errors']}")
        elif dry_run and uncertain:
            print(f"[PIPELINE]   L2 SKIPPED (dry-run) — {len(uncertain)} items would need scoring")
            for row, _, _ in uncertain[:5]:
                title = (row.get("content") or "")[:80]
                print(f"[PIPELINE]     • {title}...")

        # ── Summary ──
        elapsed = round(time.time() - t0, 1)
        total_signal = self._stats["l1_core"] + self._stats["l2_signal"]
        total_noise = self._stats["l1_junk"] + self._stats["l2_noise"]
        print(f"[PIPELINE] ═══ Run complete in {elapsed}s ═══")
        print(f"[PIPELINE]   Scanned:  {self._stats['total_scanned']}")
        print(f"[PIPELINE]   Signal:   {total_signal} (L1-core={self._stats['l1_core']}, L2-signal={self._stats['l2_signal']})")
        print(f"[PIPELINE]   Noise:    {total_noise} (L1-junk={self._stats['l1_junk']}, L2-noise={self._stats['l2_noise']})")
        print(f"[PIPELINE]   DB writes: {self._stats['l3_written']}")
        print(f"[PIPELINE]   API calls: {self._stats['api_calls']} | Errors: {self._stats['api_errors']}")
        print(f"[PIPELINE]   Est. cost: {self.api_cost_est}")
        if self._stats["total_scanned"] > 0:
            noise_pct = round(total_noise / self._stats["total_scanned"] * 100, 1)
            print(f"[PIPELINE]   Noise ratio: {noise_pct}%")

        return {
            **self._stats,
            "total_signal": total_signal,
            "total_noise": total_noise,
            "duration_s": elapsed,
            "api_cost_est": self.api_cost_est,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Trident News Ingestion Pipeline — Multi-layer news filtration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python news_ingestion_pipeline.py                        # Process last 1h, up to 50 items
  python news_ingestion_pipeline.py --hours 4 --batch 200  # Deep scan last 4h
  python news_ingestion_pipeline.py --dry-run              # L1 only, no API cost
  python news_ingestion_pipeline.py --rps 3                # Slow down L2 to 3 calls/sec
        """,
    )
    parser.add_argument(
        "--hours", type=float, default=1.0,
        help="Look-back window in hours (default: 1, use 0.5 for 30min)",
    )
    parser.add_argument(
        "--batch-size", "--batch", type=int, default=50,
        help="Max news items per run (default: 50)",
    )
    parser.add_argument(
        "--rps", type=float, default=5.0,
        help="Max L2 LLM calls per second (default: 5)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="L1 only — no LLM calls, no DB writes.  Shows what WOULD be filtered.",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Override database path (default: backend/trident_event_bus.db)",
    )
    args = parser.parse_args()

    db_path = args.db or DB_PATH

    if not os.path.exists(db_path):
        print(f"[PIPELINE] ERROR: Database not found at {db_path}")
        sys.exit(1)

    # Minimal API key check (only needed for L2)
    if not args.dry_run and not OPENROUTER_API_KEY:
        print("[PIPELINE] WARNING: OPENROUTER_API_KEY not set.")
        print("[PIPELINE]          L2 LLM scoring will fail.  Use --dry-run for L1-only mode.")
        if input("[PIPELINE] Continue anyway? [y/N] ").strip().lower() != "y":
            sys.exit(0)

    pipeline = NewsIngestionPipeline(db_path=db_path)
    stats = pipeline.run(
        hours=args.hours,
        batch_size=args.batch_size,
        rate_limit_rps=args.rps,
        dry_run=args.dry_run,
    )

    return stats


if __name__ == "__main__":
    main()
