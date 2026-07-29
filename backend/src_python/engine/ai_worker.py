"""Task B — Concurrent Batch AI Worker
+ stdlib-only OpenAI-compatible LLM client + model roster + performance feedback.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional

from config import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    DOUBAO_API_KEY,
    DOUBAO_BASE_URL,
    DOUBAO_MODEL,
    VIP_SCORE_BOOST,
    BATCH_SIZE,
    _AGG_WINDOW_HOURS,
    _AGG_MIN_SCORE,
)

# 市场快照 — 每轮 AI batch 前拉取一次 BTC/XAU 行情
from market_snapshot import get_snapshot

from .alerts import send_feishu_alert
from .prices import _get_current_price
from .utils import _detect_vip, _now, _open_db, _ts

# ---------------------------------------------------------------------------
# Optional imports — not required; kept for compatibility
# ---------------------------------------------------------------------------

try:
    from openai import AsyncOpenAI as _AsyncOpenAI  # type: ignore[import-untyped]
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


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

═══ 市场上下文 ═══

每条新闻的 user prompt 开头会附带实时市场快照。你必须将市场数据作为
价格验证层与你的宏观推演交叉验证：
  * user prompt 中的 [新闻时间] 是你分析的参考时间点。历史回测时，请以
    该时间点的市场状态做判断，不要假设"未来"会发生什么。
  * 新闻利多 + 价格已大涨 + 资金费率极端 → 利好出尽，警惕 SELL
  * 新闻利空 + 价格已大跌 + 资金费率负极端 → 空头拥挤，警惕 BUY
  * 新闻方向与当前趋势一致 → continuation 信号，置信度可上调
  * 新闻方向与当前趋势相反 → reversal 信号，置信度必须下调，需更强证据
  * 趋势强度为 Strong Bull/Bear → reversal 信号需极高证据门槛
  * ATR 升高 → 市场在重新定价，新闻冲击力放大
  * 黄金趋势 Bull → 地缘冲突新闻更可能 continuation 而非 reversal

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

其中多选字段的有效值:
  prediction_type:   reversal (反转) | continuation (趋势延续) | breakout (突破)
  event_phase:       early (事件初期,冲击最大) | mid (事件中期,市场已定价) | late (事件末期,可能出尽)
  market_confirmation: positive (市场已在按新闻方向走) | negative (市场表现与新闻方向背离) | unknown (无明确印证)
  expected_horizon:  intraday | 1-3d | 1w+
  invalidation_condition: 具体可验证的失效条件,如"BTC跌破66500则失效"
  event_strength:   low (弱催化,盘面不会剧变) | medium (中等冲击) | high (强催化,可能引发趋势/反转)
  direct_catalyst:  true (事件直接针对该资产,如BTC ETF获批) | false (间接传导,如宏观CPI → 通过利率预期影响BTC)
  timeframe_match:  intraday (事件影响<24h) | swing (影响2-7天) | macro (影响数周至数月)

═══ 铁律 ═══
  * score 严禁为 0.0。中性区 +/-0.03~0.10
  * 方向不确定时 HOLD 是正确答案，不要赌
  * reasoning_path 必须包含四段推演，每段 1-2 句话
  * BTC 在战争/危机/流动性恐慌中 → SELL (纯风险资产，不存在避险属性)
  * 市场快照价格与你的方向判断矛盾时 → confidence 降级，market_confirmation 设 negative
  * 缺少市场数据时 market_confirmation 必须为 "unknown"

═══ Score 锚定框架 ═══

sentiment_score 反映的是「2 小时窗口内该事件推动价格方向的置信度与幅度预期」。
按以下 5 级锚定，严禁拍脑袋给分：

  ±0.05 ~ ±0.15  弱信号 — 情绪面扰动，无实质性资金流变化。例：官员口头表态但无政策落地、第三方评论。
  ±0.15 ~ ±0.35  轻度信号 — 有资金面含义但影响间接。例：二级经济数据超预期、关联市场异动、监管传闻。
  ±0.35 ~ ±0.55  中度信号 — 直接推动资产供需或风险偏好。例：CPI/NFP 大幅偏离预期、美元指数剧烈波动、
                         交易所黑客/挤兑事件、主要机构增持/减持。
  ±0.55 ~ ±0.75  强信号 — 直接催化 + 趋势共振，大概率引发波段行情。例：FOMC 意外转向、ETF 获批/拒绝、
                        OPEC 减产决议、大国制裁升级。
  ±0.75 ~ ±0.95  极端信号 — 结构性突变 / 黑天鹅。例：BTC ETF 历史性获批、战争爆发、主权违约、
                          央行无限 QE、交易所破产。
  ±1.00          绝对确信 — 几乎不使用。仅在「事后看不可能错」的极端事件中使用。

锚定叠加规则：
  * direct_catalyst=true → 对应档位上浮一档（如中度 0.45 → 强信号 0.65）
  * event_strength=low → 上限 ±0.35；medium → 上限 ±0.70；high → 无上限
  * market_confirmation=negative → 对应档位下调一档（市场在反向走，置信度必须降低）
  * 历史绩效样本≥10 且胜率<30% → 下调一档；胜率>70% → 可上浮一档
  * 多个事件因子叠加（如 CPI+FOMC+地缘同时发酵）→ 取最强因子上浮半档
  * 方向与 1H 趋势同向 → +0.05~0.10；反向 → −0.05~0.10

═══ 历史绩效参考 ═══

每条新闻的 user prompt 会附带 [Historical Performance] 区块，列出历史上类似信号的
真实表现（2h forward-tracking 结算数据）：

  * 作为研究参考，不强制修改你的判断。你是独立决策者。
  * 如果某类信号历史上胜率极低（<30%），考虑降低置信度或选 HOLD
  * 如果某类信号历史上胜率很高（>70%），可以适度上调置信度
  * 如果显示 "Insufficient sample"，说明该组合样本不足，忽略即可
  * 历史不代表未来。结合当前 market context 综合判断。

只输出 JSON，14 个字段缺一不可。
"""


_JSON_PROMPT_FORCE = """
你是 10 亿美元对冲基金的量化决策大脑。从宏观博弈而非表面叙事中提取信号。

每条新闻的 user prompt 开头附带实时市场快照和 [新闻时间]。你必须将市场数据作为价格验证层：
  以 [新闻时间] 为参考点做判断，不要假设未来信息。
  新闻利多+价格已大涨+费率极端 → 警惕 SELL
  新闻与趋势方向一致 → continuation
  新闻与趋势方向相反 → reversal (置信度下调)

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
市场快照价格与方向矛盾 → confidence 降级，market_confirmation=negative。

Score 锚定（2h 窗口内价格推动置信度）：
  ±0.05~0.15 弱信号 | ±0.15~0.35 轻度 | ±0.35~0.55 中度 | ±0.55~0.75 强信号 | ±0.75~0.95 极端
  direct_catalyst=true → +1档 | event_strength=low → 上限±0.35 | market_confirmation=negative → -1档
  趋势同向 +0.05~0.10 | 趋势反向 −0.05~0.10

user prompt 中的 [Historical Performance] 是历史信号2h结算数据，作为研究参考。Insufficient sample 时忽略。

JSON(14字段):
{"reasoning_path": "...", "sentiment_score": <float>, "suggested_action": "<BUY|SELL|HOLD>", "reasoning": "<结论<=50字>", "market_category": "<CRYPTO|GOLD|OIL|MACRO|OTHER>", "target_asset": "<BTC|ETH|XAU|WTI|...|NONE>", "prediction_type": "<reversal|continuation|breakout>", "event_phase": "<early|mid|late>", "market_confirmation": "<positive|negative|unknown>", "expected_horizon": "<intraday|1-3d|1w+>", "invalidation_condition": "<失效条件,<=40字>", "event_strength": "<low|medium|high>", "direct_catalyst": <true|false>, "timeframe_match": "<intraday|swing|macro>"}

只输出 JSON。14 个字段缺一不可。
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



# ---------------------------------------------------------------------------
# Phase 1 — Performance Feedback Injection
# ---------------------------------------------------------------------------
# Queries historical settled-signal performance for key asset × action ×
# prediction_type combos.  Injected into the LLM prompt as research reference
# — does NOT auto-modify LLM output.  Only the LLM decides.
# ---------------------------------------------------------------------------

# Assets and actions we care about for performance feedback
_PERF_ASSETS = ("BTC", "XAU")
_PERF_ACTIONS = ("BUY", "SELL")
_PERF_PREDICTION_TYPES = ("continuation", "reversal", "breakout")
_PERF_LOOKBACK_DAYS = 90
# Minimum sample for "reliable" display
_PERF_MIN_SAMPLE = 8


def _build_performance_context() -> str:
    """Build [Historical Performance] block for prompt injection.

    Queries settled ai_decisions for each asset × action × prediction_type
    combo.  Returns a concise multi-line string suitable for prepending to
    the LLM user prompt.  Runs once per batch (O(1) near-instant SQL).
    """
    try:
        conn = _open_db()
    except Exception:
        return ""

    try:
        lines: List[str] = []
        lines.append("[Historical Performance]")
        lines.append("Similar signals:")

        # Query: for each (asset, action, prediction_type) combo
        for asset in _PERF_ASSETS:
            for action in _PERF_ACTIONS:
                # ── Overall (all prediction_types) ──
                overall = conn.execute(
                    "SELECT COUNT(*) AS n, "
                    "  SUM(CASE WHEN is_correct = 'WIN' THEN 1 ELSE 0 END) AS wins, "
                    "  SUM(CASE WHEN is_correct = 'LOSS' THEN 1 ELSE 0 END) AS losses, "
                    "  AVG(CASE WHEN forward_pnl IS NOT NULL THEN forward_pnl ELSE NULL END) AS avg_pnl "
                    "FROM ai_decisions "
                    "WHERE settled = 1 "
                    "  AND is_correct IN ('WIN', 'LOSS') "
                    "  AND target_asset = ? "
                    "  AND suggested_action = ? "
                    "  AND created_at >= datetime('now', 'localtime', ?)",
                    (asset, action, f"-{_PERF_LOOKBACK_DAYS} days"),
                ).fetchone()

                for pt in _PERF_PREDICTION_TYPES:
                    row = conn.execute(
                        "SELECT COUNT(*) AS n, "
                        "  SUM(CASE WHEN is_correct = 'WIN' THEN 1 ELSE 0 END) AS wins, "
                        "  SUM(CASE WHEN is_correct = 'LOSS' THEN 1 ELSE 0 END) AS losses, "
                        "  AVG(CASE WHEN forward_pnl IS NOT NULL THEN forward_pnl ELSE NULL END) AS avg_pnl "
                        "FROM ai_decisions "
                        "WHERE settled = 1 "
                        "  AND is_correct IN ('WIN', 'LOSS') "
                        "  AND target_asset = ? "
                        "  AND suggested_action = ? "
                        "  AND prediction_type = ? "
                        "  AND created_at >= datetime('now', 'localtime', ?)",
                        (asset, action, pt, f"-{_PERF_LOOKBACK_DAYS} days"),
                    ).fetchone()

                    n = row["n"] or 0
                    wins = row["wins"] or 0
                    losses = row["losses"] or 0
                    decided = wins + losses
                    avg_pnl = row["avg_pnl"]

                    if n < _PERF_MIN_SAMPLE or decided == 0:
                        lines.append(
                            f"{asset} {pt} {action}: "
                            f"Insufficient sample"
                        )
                    else:
                        wr = wins / decided
                        pnl_str = (
                            f"{avg_pnl:+.2f}%" if avg_pnl is not None else "N/A"
                        )
                        lines.append(
                            f"{asset} {pt} {action}: "
                            f"sample: {decided} "
                            f"win rate: {wr:.0%} "
                            f"avg pnl: {pnl_str}"
                        )

                # ── Also add the overall (all prediction_types) line ──
                n_all = overall["n"] or 0
                wins_all = overall["wins"] or 0
                losses_all = overall["losses"] or 0
                decided_all = wins_all + losses_all
                if decided_all >= _PERF_MIN_SAMPLE:
                    wr_all = wins_all / decided_all
                    pnl_all = overall["avg_pnl"]
                    pnl_str = (
                        f"{pnl_all:+.2f}%" if pnl_all is not None else "N/A"
                    )
                    lines.append(
                        f"{asset} * {action}: "
                        f"sample: {decided_all} "
                        f"win rate: {wr_all:.0%} "
                        f"avg pnl: {pnl_str}"
                    )

        return "\n".join(lines)

    except Exception as e:
        print(f"[PERF] 查询历史绩效失败: {type(e).__name__}: {str(e)[:80]}")
        return ""
    finally:
        conn.close()


def _call_llm_sync(news_content: str, model_cfg: Dict[str, str],
                   market_context: str = "",
                   performance_context: str = "",
                   news_timestamp: str = "") -> Dict[str, Any]:
    """
    Call any OpenAI-compatible LLM API synchronously (runs in executor thread).

    model_cfg.keys: id, label, api_base, api_key
    market_context:  Optional multi-line market snapshot string, prepended to user_content.
    news_timestamp:  ISO datetime string of the news event — prepended so AI knows the
                     historical time point when replaying old news for backtesting.
    Returns: {"sentiment_score", "suggested_action", "reasoning", "model_label", "model_id",
              ... + 5 metadata fields}

    Retry strategy: if json_mode=True produces unparseable output (keyword fallback),
    retry once with json_mode=False (prompt-based JSON enforcement).
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

        # 组装 user content: [新闻时间] + [市场快照] + [历史绩效] + [新闻正文]
        user_text = news_content[:2000]
        if news_timestamp:
            try:
                ts_dt = datetime.fromisoformat(news_timestamp.replace("Z", "+00:00"))
                ts_str = ts_dt.strftime("%Y-%m-%d %H:%M UTC")
            except (ValueError, TypeError):
                ts_str = news_timestamp[:19]
            user_text = f"[新闻时间] {ts_str}\n\n{user_text}"
        if market_context:
            user_text = market_context + "\n\n" + user_text
        if performance_context:
            user_text = performance_context + "\n\n" + user_text

        payload: Dict[str, Any] = {
            "model": model_cfg["id"],
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_text},
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

        # HTTP 429 retry logic — Kimi K3 upstream rate-limit is transient
        retry_delays = [5.0, 10.0, 20.0]  # progressive backoff
        last_err = None
        for attempt, delay in enumerate([0] + retry_delays):
            try:
                if delay > 0:
                    time.sleep(delay)
                opener = urllib.request.build_opener(proxy_handler) if proxy_handler else urllib.request.build_opener()
                resp = opener.open(req, timeout=45)
                break  # success → exit retry loop
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code == 429 and attempt < len(retry_delays):
                    print(f"  [{model_cfg['label']}] HTTP 429, retry {attempt+1}/{len(retry_delays)} after {delay}s", flush=True)
                    continue
                err_body = e.read().decode("utf-8", errors="replace")[:300]
                raise RuntimeError(
                    f"{model_cfg['label']} HTTP {e.code}: {err_body}"
                ) from e
        else:
            # All retries exhausted
            err_body = last_err.read().decode("utf-8", errors="replace")[:300] if last_err else "unknown"
            raise RuntimeError(
                f"{model_cfg['label']} HTTP 429 (exhausted retries): {err_body}"
            )

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
                    result = {"reasoning_path": "关键词推断 → 偏多信号", "sentiment_score": 0.4, "suggested_action": "BUY", "reasoning": "关键词推断:偏多", "market_category": "OTHER", "target_asset": "NONE", "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}
                elif any(w in text_lower for w in ("利空", "下跌", "看跌", "bearish", "sell", "战争", "制裁")):
                    result = {"reasoning_path": "关键词推断 → 偏空信号", "sentiment_score": -0.4, "suggested_action": "SELL", "reasoning": "关键词推断:偏空", "market_category": "OTHER", "target_asset": "NONE", "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}
                else:
                    result = {"reasoning_path": "关键词推断 → 中性观望", "sentiment_score": 0.05, "suggested_action": "HOLD", "reasoning": "关键词推断:观望", "market_category": "OTHER", "target_asset": "NONE", "event_strength": "low", "direct_catalyst": False, "timeframe_match": "intraday"}

        # ── 元数据字段默认值 (LLM 可能不返回或返回无效值) ──
        _META_DEFAULTS: Dict[str, Any] = {
            "prediction_type":        ("reversal", "continuation", "breakout"),
            "event_phase":            ("early", "mid", "late"),
            "market_confirmation":    ("positive", "negative", "unknown"),
            "expected_horizon":       ("intraday", "1-3d", "1w+"),
            "invalidation_condition": "",
            # Phase 2: Event Quality Layer
            "event_strength":         ("low", "medium", "high"),
            "timeframe_match":        ("intraday", "swing", "macro"),
        }
        for key, valid in _META_DEFAULTS.items():
            if key not in result or not isinstance(result[key], str):
                # If valid is a tuple, take the last (most conservative) value; if str, use empty
                result[key] = valid[-1] if isinstance(valid, tuple) else valid
            elif isinstance(valid, tuple):
                val_lower = result[key].strip().lower()
                # Check if the value matches any valid option (fuzzy)
                ok = False
                for v in valid:
                    if v in val_lower or val_lower == v:
                        result[key] = v  # normalize to canonical form
                        ok = True
                        break
                if not ok:
                    result[key] = valid[-1]  # default conservative

        # direct_catalyst 布尔值特殊处理 (LLM 可能返回 JSON true/false 或字符串)
        dc = result.get("direct_catalyst")
        if isinstance(dc, bool):
            pass  # already correct
        elif isinstance(dc, str) and dc.strip().lower() in ("true", "1", "yes"):
            result["direct_catalyst"] = True
        else:
            result["direct_catalyst"] = False

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
            # Phase 2: Event Quality Layer (Doubao 不输出这些 → 默认值)
            "event_strength": "medium",
            "direct_catalyst": False,
            "timeframe_match": "intraday",
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
        # Phase 2: Event Quality Layer
        "event_strength": result.get("event_strength", "medium"),
        "direct_catalyst": bool(result.get("direct_catalyst", False)),
        "timeframe_match": result.get("timeframe_match", "intraday"),
    }




# ---------------------------------------------------------------------------
# Task B — Concurrent Batch AI Worker (BATCH_SIZE 定义见 config.py)
# ---------------------------------------------------------------------------


async def _process_single(
    news_row: sqlite3.Row,
    model_cfg: Dict[str, str],
    loop: asyncio.AbstractEventLoop,
    market_context: str = "",
    performance_context: str = "",
) -> Dict[str, Any]:
    """
    Process a single raw_news row through ONE model's LLM pipeline (Kimi K3).

    单模型模式：Kimi K3 使用 45 秒超时（在 _call_llm_sync 内部 urllib timeout=45s）。
    信号量控制并发上限，避免突发新闻潮打爆 OpenRouter。

    market_context:  注入到 User Prompt 的市场快照字符串。
    performance_context: 注入到 User Prompt 的历史绩效字符串（Phase 1）。

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
                None, _call_llm_sync, content, model_cfg, market_context,
                performance_context, pre_ts,
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
            "prediction_type": "continuation",
            "event_phase": "mid",
            "market_confirmation": "unknown",
            "expected_horizon": "1-3d",
            "invalidation_condition": "系统异常,无失效条件",
            "event_strength": "low",
            "direct_catalyst": False,
            "timeframe_match": "intraday",
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


# ---------------------------------------------------------------------------
# Active-trade aggregation helpers (_AGG_WINDOW_HOURS / _AGG_MIN_SCORE 见 config.py)
# ---------------------------------------------------------------------------


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

    # 失败冷却: snapshot 连续 DOWN 后 5 分钟内不重试, 减少无意义等待
    _SNAPSHOT_COOLDOWN_S = 300
    _last_snapshot_down_ts: float = 0.0

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
                    "SELECT * FROM raw_news WHERE status = 'PENDING' AND is_noise = 0"
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

        # ==================================================================
        # Phase 0 — Pull market snapshot (once per batch, before LLM calls)
        # ==================================================================

        market_context = ""
        decision_context = "{}"  # 完整快照 JSON, 供 Hermes 复盘
        now_ts = time.time()
        if now_ts - _last_snapshot_down_ts > _SNAPSHOT_COOLDOWN_S:
            try:
                snap = await get_snapshot()
                market_context = snap.get("summary", "")
                decision_context = json.dumps(snap, ensure_ascii=False)
                if market_context:
                    print(f"\n  [SNAPSHOT] {snap['status'].upper()} | "
                          f"BTC={snap['assets']['BTC'].get('price_str','?')} | "
                          f"XAU={snap['assets']['XAU'].get('price_str','?')}")
                if snap['status'] == 'down':
                    _last_snapshot_down_ts = now_ts
            except Exception as e:
                print(f"  [SNAPSHOT] 获取失败: {type(e).__name__}: {str(e)[:80]}")
                market_context = ""
                decision_context = json.dumps({"error": str(e), "status": "down"}, ensure_ascii=False)
                _last_snapshot_down_ts = now_ts
        else:
            # 冷却中, 跳过本轮 snapshot 请求
            remaining = _SNAPSHOT_COOLDOWN_S - int(now_ts - _last_snapshot_down_ts)
            market_context = ""
            decision_context = json.dumps({"status": "down", "cooldown": True}, ensure_ascii=False)
            if int(now_ts) % 60 == 0:  # 每分钟只打一次
                print(f"  [SNAPSHOT] 跳过 (冷却中, {remaining}s 后重试)")

        # ==================================================================
        # Phase 0.5 — Build historical performance reference (once per batch)
        # ==================================================================
        performance_context = ""
        try:
            performance_context = await loop.run_in_executor(
                None, _build_performance_context,
            )
            if performance_context:
                print(f"  [PERF] 历史绩效已注入 prompt ({len(performance_context)} chars)")
        except Exception as e:
            print(f"  [PERF] 构建失败: {type(e).__name__}: {str(e)[:80]}")
            performance_context = ""

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
        timestamp_map: Dict[int, str] = {}
        for row in batch:
            c = row["content"]
            # Strip [hash:xxx] prefix for cleaner display
            c = re.sub(r'\[hash:[a-zA-Z0-9]+\]\s*', '', c)
            content_map[row["id"]] = c
            timestamp_map[row["id"]] = row["timestamp"] or ""
        print(
            f"\n[{_now()}] [AI] Claimed {len(batch)} items: {batch_ids}"
        )

        # ==================================================================
        # Phase 2 - Concurrent LLM analysis (news × models)
        # ==================================================================
        batch_start = _ts()
        tasks = [
            _process_single(row, model_cfg, loop, market_context, performance_context)
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

                    # ── 元数据提取 (LLM 可能不返回 → 默认值兜底) ──
                    pred_type = res.get("prediction_type", "continuation")
                    evt_phase = res.get("event_phase", "mid")
                    mkt_confirm = res.get("market_confirmation", "unknown")
                    exp_horizon = res.get("expected_horizon", "1-3d")
                    inval_cond = res.get("invalidation_condition", "")
                    # Phase 2: Event Quality Layer
                    evt_strength = res.get("event_strength", "medium")
                    direct_cat = 1 if res.get("direct_catalyst", False) else 0
                    tf_match = res.get("timeframe_match", "intraday")

                    cur = conn.execute(
                        """
                        INSERT INTO ai_decisions
                          (news_id, sentiment_score, suggested_action,
                           reasoning, created_at, market_category, target_asset,
                           parent_id, child_count, aggregation_key, reasoning_path, vip_tag,
                           prediction_type, event_phase, market_confirmation,
                           expected_horizon, invalidation_condition,
                           event_strength, direct_catalyst, timeframe_match,
                           decision_context)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?,
                                ?, ?, ?, ?, ?,
                                ?, ?, ?,
                                ?);
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
                            pred_type,
                            evt_phase,
                            mkt_confirm,
                            exp_horizon,
                            inval_cond[:200],  # 截断超长失效条件
                            evt_strength,
                            direct_cat,
                            tf_match,
                            decision_context,
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
            news_ts = timestamp_map.get(nid, "")
            asyncio.create_task(
                send_feishu_alert(
                    f"{display_title}\n\n**Consensus**: {consensus_str}",
                    consensus_str,
                    0.0,
                    feishu_body,
                    news_ts,
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
