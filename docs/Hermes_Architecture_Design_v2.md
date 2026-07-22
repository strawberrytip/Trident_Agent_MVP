# Hermes Research Agent — Architecture Design v2

> **Status:** Updated for Implementation  
> **Date:** 2026-07-22  
> **Author:** Claude (Architecture)  
> **Target:** Trident Agent MVP v2  
> **Previous version:** `docs/Hermes_Architecture_Design.md` (v1, 2026-07-21)

---

## 0. Change Log (v1 → v2)

Following a review of v1 against the current production environment, five critical modifications have been incorporated:

| # | Modification | v1 Approach | v2 Approach |
|---|-------------|-------------|-------------|
| 1 | **Data Protection** | Auto-write LLM conclusions to `research_memory` | Raw Observations only; no subjective conclusions stored without verification |
| 2 | **Repository Layer** | Tool functions write raw SQL | All DB access through Repository classes; tools call Repository methods |
| 3 | **Performance Feedback** | Not present | New `performance_tool.py` reads `is_correct`, `entry_price`, `exit_price`, `settled` for backtest validation |
| 4 | **Vector Store Abstraction** | Chroma called directly from `history_tool.py` | `VectorStoreInterface` ABC with Chroma implementation; tools depend on interface |
| 5 | **Intent Router** | All queries go through full LLM plan→execute→synthesize | Lightweight regex/keyword router bypasses LLM planning for simple queries |

---

## 1. Executive Summary

Hermes is a **Read-Only Research Agent** that sits beside Trident's existing production pipeline. It answers natural-language questions like "最近24小时 BTC 为什么上涨？" by orchestrating multi-tool queries (news, market context, signals, historical events, performance analytics) and synthesizing a research-grade answer via LLM. It does NOT execute trades, modify production data, or touch the trading subsystem.

**Critical constraint:** `engine.py`, `trading/*`, `binance_execution.py`, and all existing live execution paths are **never imported, modified, or touched**. All additions are purely additive.

---

## 2. Current Production Context

### 2.1 What's Running

```
Cloud Server (Ubuntu):
  api_server.py    → FastAPI :8000  (SSE, export, agent_chat)
  engine.py        → Background loop (WS ingest, AI worker, forward tracker)
  Nginx            → Reverse proxy /api/* → :8000, SSE enabled
  Next.js :3000    → Frontend with NEXT_PUBLIC_API_URL= (relative paths)

Local (Windows):
  engine.py        → Same background loop (separate DB instance)
  api_server.py    → FastAPI :8000 (for local frontend)
  treenews_bridge  → Webhook relay to engine
```

### 2.2 Database Schema (trident_event_bus.db)

| Table | Key Columns | Purpose |
|-------|------------|---------|
| `raw_news` | id, timestamp, source, content, status | Incoming news (FinancialJuice WS + TreeNews webhook) |
| `ai_decisions` | id, news_id(FK), suggested_action, sentiment_score, reasoning, reasoning_path, market_category, target_asset, entry_price, exit_price, is_correct, settled, extra_models_consensus(JSON) | Kimi K3 decisions with forward tracking P&L |

### 2.3 Existing Agent Chat (Data Copilot)

The current `/api/agent_chat` is a stateless two-pass SQL generator:
- Pass 1: LLM Text-to-SQL → execute via aiosqlite
- Pass 2: LLM Data-to-Text summary

It will be preserved unchanged. Hermes endpoints are additive and live alongside it.

---

## 3. V2 Modifications in Detail

### 3.1 Modification 1: Raw vs Cooked Data (Data Protection)

**Problem:** v1's `_remember()` step auto-saved LLM-generated conclusions to `research_memory`. If the LLM hallucinates a causal relationship (e.g., "BTC rose because of the SEC announcement"), that falsehood becomes persistent "knowledge" that future queries will retrieve as "fact."

**Solution:**

The `research_memory` table is split into two tiers:

| Tier | Table | What Goes In | Who Writes |
|------|-------|-------------|------------|
| **L0 — Raw Observations** | `research_observations` | Verbatim news headlines, signal actions, price data points, timestamps, source attribution. Machine-generated metadata only (e.g., `sentiment_score=+0.62`). | **Tool functions automatically** |
| **L1 — Verified Insights** | `research_insights` | Human-reviewed conclusions, statistically validated correlations (p < 0.05, n ≥ 30), backtest-verified strategy rules. | **Human operator only** (via a future `/api/hermes/verify` endpoint or direct DB insert) |

**Rules enforced in code:**

- `save_observation()` can write to L0. It stores: `{observation_type, content, source_tool, source_row_id, timestamp, embedding}`.
- `save_insight()` requires a `verification` field: `{verified_by: "human" | "backtest", confidence: float, sample_size: int}`. Without this, the write is rejected.
- The agent loop's synthesize step NEVER auto-persists. It only returns the answer to the user.
- `memory_tool.py` exposes `save_observation()` but NOT `save_insight()` to the LLM. The LLM cannot trigger L1 writes.

**Why this matters:** An alpha research system's memory is its most valuable asset. Contaminating it with unverified LLM output is equivalent to training on your own predictions — it creates a feedback loop of increasing hallucination confidence.

### 3.2 Modification 2: Repository Layer

**Problem:** v1's tool functions constructed raw SQL strings and executed them directly via aiosqlite. This means:
- SQL injection risk (even with parameterization, the pattern is fragile)
- No single place to audit DB access
- If the schema changes, every tool file must be updated
- Testing tools requires a real SQLite database

**Solution:** Insert a Repository layer between tools and the database.

```
Tool Function
    │
    ▼
RepositoryInterface (ABC)
    │
    ▼
SqliteRepository (implementation)
    │
    ▼
aiosqlite / sqlite3
```

**Repository classes (one per aggregate):**

| Repository | Responsibility | Backed By |
|-----------|---------------|-----------|
| `NewsRepository` | `find_recent(asset, hours, limit)`, `search_by_keyword(query, limit)` | `raw_news` |
| `SignalRepository` | `find_signals(asset, hours, limit)`, `get_performance_stats(asset, days)`, `get_consensus_breakdown(asset, hours)` | `ai_decisions` |
| `ObservationRepository` | `insert_observation(...)`, `search_by_tags(tags)`, `get_recent(limit)` | `research_observations` |
| `InsightRepository` | `insert_insight(...)`, `get_verified(asset)`, `get_by_confidence(min_confidence)` | `research_insights` |

**Key rule:** No `.py` file outside `backend/src_python/hermes/repositories/` is allowed to contain SQL string literals. Tools import and call repository methods.

**Example contrast:**

```python
# v1 (banned):
async def query_news(asset: str, hours: int = 24) -> List[Dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT ... FROM raw_news WHERE content LIKE ? ...",
            (f"%{asset}%",),
        )
        ...

# v2 (required):
from hermes.repositories.news_repo import NewsRepository

async def query_news(asset: str, hours: int = 24) -> List[Dict]:
    repo = NewsRepository(DB_PATH)
    return await repo.find_recent(asset=asset, hours=hours)
```

### 3.3 Modification 3: Performance Feedback Loop

**Problem:** v1 had no way for Hermes to answer questions like "过去一周哪个模型的信号最准？" or "BUY 信号在 BTC 上的盈利率是多少？" This is critical for strategy evaluation.

**Solution:** New `performance_tool.py` reads from `ai_decisions` forward-tracking columns (`is_correct`, `entry_price`, `exit_price`, `settled`, `forward_pnl`). It exposes aggregate analytics without touching the trading subsystem.

**PerformanceTool methods:**

```python
# Exposed to the LLM via TOOL_REGISTRY:
"analyze_performance": {
    "description": "Analyze historical signal performance: win rate, avg PnL, Sharpe-like ratio. Filter by asset, model, action, or time range.",
    "parameters": {
        "asset": "str (e.g., BTC, XAU, WTI)",
        "action": "str (optional: BUY, SELL, HOLD)",
        "days": "int (default 30)",
    }
}
```

**What the tool computes (read-only from ai_decisions):**

| Metric | Source Columns | Formula |
|--------|---------------|---------|
| Win Rate | `is_correct`, `settled` | `COUNT(is_correct=1) / COUNT(settled=1)` |
| Avg Forward PnL | `forward_pnl`, `settled` | `AVG(forward_pnl) WHERE settled=1` |
| Avg Score by Outcome | `sentiment_score`, `is_correct` | `AVG(ABS(score)) GROUP BY is_correct` |
| Signal Count by Action | `suggested_action` | `COUNT(*) GROUP BY action` |
| Consensus vs Accuracy | `extra_models_consensus`, `is_correct` | Split by consensus strength (unanimous vs split) |

**Important constraint:** These are read-only aggregate queries. The tool never modifies `ai_decisions`. The forward tracker (in `engine.py`) remains the sole writer of `is_correct`, `forward_pnl`, and `settled`.

### 3.4 Modification 4: Vector Store Abstraction

**Problem:** v1's `history_tool.py` and `memory_tool.py` imported Chroma directly. If we switch to pgvector, Qdrant, or LanceDB later, every tool file must change.

**Solution:** Define a `VectorStoreInterface` abstract base class. Chroma is one implementation. Tools depend only on the interface.

```python
# hermes/memory/vector_store_interface.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List

class VectorStoreInterface(ABC):
    """Abstract interface for vector similarity search and storage."""

    @abstractmethod
    async def search(self, collection: str, query_vec: List[float],
                     n_results: int = 10, where: Dict = None) -> List[Dict]:
        """Return nearest neighbors with metadata and similarity scores."""
        ...

    @abstractmethod
    async def add(self, collection: str, ids: List[str],
                  embeddings: List[List[float]],
                  documents: List[str], metadatas: List[Dict]) -> None:
        ...

    @abstractmethod
    async def delete(self, collection: str, ids: List[str]) -> None:
        ...

    @abstractmethod
    async def count(self, collection: str) -> int:
        ...


class ChromaVectorStore(VectorStoreInterface):
    """Chroma implementation — embedded mode, local persistence."""

    def __init__(self, persist_dir: str):
        import chromadb
        self._client = chromadb.PersistentClient(path=persist_dir)

    async def search(self, collection, query_vec, n_results=10, where=None):
        col = self._client.get_or_create_collection(collection)
        results = col.query(
            query_embeddings=[query_vec],
            n_results=n_results,
            where=where,
        )
        return self._format_results(results)

    # ... (add, delete, count implementations)
```

**Dependency injection:** The HermesAgent constructor accepts a `VectorStoreInterface` instance. Default is `ChromaVectorStore`, but tests can inject a mock.

```python
class HermesAgent:
    def __init__(self,
                 tool_registry: Dict,
                 vector_store: VectorStoreInterface,
                 repositories: Dict[str, Any]):
        self.vector_store = vector_store  # ← depends on interface, not Chroma
```

### 3.5 Modification 5: Lightweight Intent Router

**Problem:** v1 routes every query through the full LLM planning loop (Plan → Execute → Synthesize). For simple queries like "BTC现在有什么信号？" or "今天有几条新闻？", this wastes ~1-2 seconds and a full LLM call on something a regex could handle.

**Solution:** Insert a pre-processing router BEFORE the agent planner. It classifies the query intent and either (a) routes directly to a single tool, or (b) falls through to the full LLM planner.

**Router design:**

```python
# hermes/intent_router.py

# Pattern → (tool_name, args_template)
# Ordered: first match wins. Each pattern is a compiled regex.
DIRECT_ROUTES: List[Tuple[re.Pattern, str, Dict]] = [
    # ── Signal queries ──
    (r"(?P<asset>BTC|ETH|XAU|GOLD|WTI|原油|黄金|比特币|以太坊).{0,10}(信号|交易信号|操作)",
     "query_signals", {"asset": "{asset}", "hours": 24}),

    (r"(信号|交易信号|操作).{0,10}(?P<asset>BTC|ETH|XAU|GOLD|WTI)",
     "query_signals", {"asset": "{asset}", "hours": 24}),

    # ── News queries ──
    (r"(?P<asset>BTC|ETH|XAU|GOLD|WTI).{0,6}(新闻|快讯|最新消息|发生了什么)",
     "query_news", {"asset": "{asset}", "hours": 24}),

    (r"(今天|最近|最新).*(新闻|快讯)",
     "query_news", {"asset": None, "hours": 24}),

    (r"有几条新闻|多少条新闻|新闻数量",
     "count_news", {"hours": 24}),

    # ── Market context ──
    (r"(?P<asset>BTC|ETH|XAU|GOLD|WTI).{0,6}(价格|行情|涨了|跌了|走势)",
     "market_context", {"asset": "{asset}"}),

    # ── Performance ──
    (r"(胜率|准确率|表现|盈亏|赚了|赔了).{0,10}(?P<asset>BTC|ETH|XAU|GOLD|WTI)",
     "analyze_performance", {"asset": "{asset}", "days": 30}),
]

# Minimum confidence threshold for direct routing
MIN_CONFIDENCE = 0.85  # if router confidence < 0.85, fall through to LLM planner
```

**Router decision tree:**

```
User Query
    │
    ▼
┌─────────────────────────────┐
│  IntentRouter.classify(q)   │
│                             │
│  Returns:                   │
│    IntentRoute.DIRECT       │
│      → tool_name, args      │
│    IntentRoute.DELEGATE     │
│      → full agent loop      │
└─────────────────────────────┘
    │
    ├── DIRECT route:
    │   Call tool → Synthesize (single LLM call) → Return
    │   (Saves 1 LLM call: ~1-2s, ~$0.002)
    │
    └── DELEGATE route:
        Plan (LLM) → Execute → Synthesize (LLM) → Return
        (Full research workflow)
```

**Routing confidence:** If the regex matches but captures ambiguous args (e.g., "BTC 怎么样？" which could be signals OR market context), the router returns DELEGATE instead of guessing. The keyword `怎么样` or `如何` alone triggers delegation since it's semantically ambiguous.

---

## 4. Updated Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Hermes Research Layer (v2)                       │
│                                                                           │
│  ┌────────────────────────┐    ┌─────────────────────────────────────┐   │
│  │  intent_router.py       │    │  tools/  (5 stateless functions)     │   │
│  │                         │    │                                      │   │
│  │  classify(question)     │    │  news_tool.py      ─┐               │   │
│  │  → DIRECT: tool+args    │    │  market_tool.py     │               │   │
│  │  → DELEGATE: full loop  │    │  signal_tool.py     ├─ Repository   │   │
│  └──────────┬─────────────┘    │  history_tool.py    │  Layer         │   │
│             │                  │  performance_tool.py┘  (read-only)   │   │
│             ▼                  └─────────────────────────────────────┘   │
│  ┌────────────────────────┐                                              │
│  │  hermes_runtime.py      │    ┌─────────────────────────────────────┐   │
│  │  (HermesAgent)          │    │  repositories/                       │   │
│  │                         │    │                                      │   │
│  │  DIRECT path:           │    │  base.py       Repository (ABC)      │   │
│  │    tool → synthesize    │    │  news_repo.py  → raw_news            │   │
│  │                         │    │  signal_repo.py → ai_decisions       │   │
│  │  DELEGATE path:         │    │  obs_repo.py   → research_           │   │
│  │    plan → exec → synth  │    │                  observations (L0)   │   │
│  │                         │    │  insight_repo.py→ research_          │   │
│  │  (NO auto-persist)      │    │                  insights (L1)       │   │
│  └────────────────────────┘    └─────────────────────────────────────┘   │
│                                                                           │
│  ┌────────────────────────┐    ┌─────────────────────────────────────┐   │
│  │  memory/                │    │  hermes_config.py                    │   │
│  │                         │    │  - System prompts                    │   │
│  │  vector_store_          │    │  - TOOL_REGISTRY                     │   │
│  │    interface.py (ABC)   │    │  - INTENT_ROUTES                     │   │
│  │                         │    │  - LLM config (model, temp, tokens)  │   │
│  │  chroma_store.py        │    │                                      │   │
│  │    (impl)               │    └─────────────────────────────────────┘   │
│  │                         │                                              │
│  │  embeddings.py          │                                              │
│  │    (OpenAI-compat API)  │                                              │
│  └────────────────────────┘                                              │
│                                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │  api_server.py (additions — zero existing code changed)            │   │
│  │                                                                    │   │
│  │  POST /api/hermes/chat    ← research agent endpoint                │   │
│  │  POST /api/hermes/report  ← (Phase 2) scheduled report endpoint    │   │
│  │  GET  /api/hermes/memory  ← browse research observations (L0)      │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│  ════════════════ UNTOUCHED BOUNDARY ════════════════                      │
│                                                                           │
│  ┌────────────────────────┐    ┌─────────────────────────────────────┐   │
│  │  engine.py (prod loop)  │    │  trading/*  (execution pipeline)     │   │
│  │  api_server.py (existing)│   │  binance_execution.py                │   │
│  │  treenews_bridge.py     │    │  frontend/* (existing components)    │   │
│  └────────────────────────┘    └─────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Updated Module Breakdown

### 5.1 Directory Structure

```
backend/src_python/
  hermes/
    __init__.py
    hermes_runtime.py        # HermesAgent class
    hermes_config.py         # Prompts, TOOL_REGISTRY, INTENT_ROUTES
    intent_router.py         # Lightweight query classifier
  tools/
    __init__.py
    news_tool.py             # Tool: query_news, count_news
    market_tool.py           # Tool: market_context
    signal_tool.py           # Tool: query_signals
    history_tool.py          # Tool: search_history (uses VectorStoreInterface)
    performance_tool.py      # Tool: analyze_performance  ← NEW in v2
  repositories/
    __init__.py
    base.py                  # BaseRepository (ABC) + helpers
    news_repo.py             # NewsRepository → raw_news
    signal_repo.py           # SignalRepository → ai_decisions
    obs_repo.py              # ObservationRepository → research_observations
    insight_repo.py          # InsightRepository → research_insights
  memory/
    __init__.py
    vector_store_interface.py # VectorStoreInterface (ABC)
    chroma_store.py           # ChromaVectorStore implementation
    embeddings.py             # embed_text() via OpenAI-compatible API
```

### 5.2 Module Count

| Layer | Files | Purpose |
|-------|-------|---------|
| Agent Runtime | 3 | Router, Agent loop, Config |
| Tools | 5 | Stateless tool functions exposed to LLM |
| Repositories | 5 | DB access layer (ABC + 4 implementations) |
| Memory | 3 | Vector store abstraction + embeddings |
| api_server.py | +1 endpoint | POST /api/hermes/chat |

**Total: 17 new files. Zero existing files modified (except api_server.py additions at end of file).**

---

## 6. Database Extensions (v2)

### 6.1 New Table: `research_observations` (L0 — Raw Observations)

```sql
CREATE TABLE IF NOT EXISTS research_observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_type TEXT NOT NULL,          -- 'news_headline', 'signal_action', 'price_data', 'market_snapshot'
    content         TEXT NOT NULL,           -- The raw observation text
    source_tool     TEXT NOT NULL,           -- Which tool created this: 'query_news', 'query_signals', etc.
    source_row_id   INTEGER,                -- FK-ish: id in raw_news or ai_decisions
    asset           TEXT DEFAULT '',         -- BTC, XAU, WTI, etc.
    tags            TEXT DEFAULT '[]',       -- JSON array of tags
    embedding_id    TEXT DEFAULT '',         -- Chroma document ID (for cross-reference)
    created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_obs_type ON research_observations(observation_type);
CREATE INDEX IF NOT EXISTS idx_obs_asset ON research_observations(asset);
CREATE INDEX IF NOT EXISTS idx_obs_created ON research_observations(created_at);
```

### 6.2 New Table: `research_insights` (L1 — Verified Insights)

```sql
CREATE TABLE IF NOT EXISTS research_insights (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content         TEXT NOT NULL,           -- The verified insight/conclusion
    verified_by     TEXT NOT NULL,           -- 'human' | 'backtest' | 'statistical_test'
    confidence      REAL NOT NULL DEFAULT 1.0, -- 0.0–1.0
    sample_size     INTEGER DEFAULT 0,      -- n for statistical verification
    p_value         REAL,                   -- For statistical tests
    source_obs_ids  TEXT DEFAULT '[]',      -- JSON array of observation IDs this insight is based on
    asset           TEXT DEFAULT '',
    tags            TEXT DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_insight_verified ON research_insights(verified_by);
CREATE INDEX IF NOT EXISTS idx_insight_confidence ON research_insights(confidence);
```

### 6.3 Migration Strategy

Both tables are versioned in a `schema_version` table (new):

```sql
CREATE TABLE IF NOT EXISTS schema_version (
    component TEXT PRIMARY KEY,
    version   INTEGER NOT NULL
);
```

The migration function checks `schema_version` and only runs unapplied migrations. Production tables (`raw_news`, `ai_decisions`) never touched.

---

## 7. Data Flow (End-to-End Example)

User asks: **"过去一周 BTC 的 BUY 信号表现怎么样？"**

```
┌─ Step 0: Intent Router ────────────────────────────────────┐
│  Input: "过去一周 BTC 的 BUY 信号表现怎么样？"                │
│  Regex match: 表现.*信号 → analyze_performance              │
│  Args extracted: asset=BTC, days=7                          │
│  Confidence: HIGH → DIRECT route                            │
│  Skipping LLM planning — saves ~1.5s                        │
└────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 1: Execute (single tool, ~0.2s) ─────────────────────┐
│  performance_tool.analyze_performance(asset="BTC", days=7)  │
│    → SignalRepository.get_performance_stats("BTC", 7)       │
│    → SELECT is_correct, forward_pnl, settled,               │
│             suggested_action, sentiment_score               │
│      FROM ai_decisions                                      │
│      WHERE target_asset='BTC'                               │
│        AND created_at >= datetime('now', '-7 days')         │
│        AND suggested_action='BUY'                           │
│                                                             │
│    Returns: {                                               │
│      "total_signals": 15,                                   │
│      "settled_count": 11,                                   │
│      "win_rate": 0.636,                                     │
│      "avg_forward_pnl": "+0.42%",                           │
│      "avg_abs_score": 0.58,                                 │
│      "score_by_outcome": {                                  │
│        "correct": {"avg_score": 0.63},                      │
│        "incorrect": {"avg_score": 0.41}                     │
│      }                                                      │
│    }                                                        │
└────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 2: Synthesize (LLM call #1, ~1.0s) ─────────────────┐
│  Input:  SYNTHESIZE_PROMPT + user question + tool results   │
│  Output:                                                    │
│  "BTC BUY 信号过去一周表现分析 (置信度: 高)                  │
│                                                             │
│   1. **胜率**: 63.6% (11个已结算信号中7个盈利)                │
│   2. **平均收益**: +0.42% per signal                        │
│   3. **信号质量观察**: 正确信号的评分均值(0.63)显著高于        │
│      错误信号(0.41)，评分系统具有区分度。                     │
│   4. **注意**: 仅统计已结算信号，4个未结算信号未纳入计算。"     │
└────────────────────────────────────────────────────────────┘
```

**Comparison with v1:** v1 would have taken this query through full LLM planning (Plan → 4 parallel tools unnecessarily → Synthesize), costing 2 LLM calls and ~5s. v2 does it with 1 LLM call in ~1.5s and gets a more focused answer.

---

## 8. Risk Analysis (v2 Additions)

### 8.1 Repository Layer Injection

| Risk | Severity | Mitigation |
|------|----------|------------|
| Repository bypass (tool writes raw SQL anyway) | Low | Code review gate: all new tool PRs checked for `execute(` or `cursor.execute(` outside repositories/. CI lint rule: `grep -r 'db.execute\|cursor.execute' tools/` fails the build. |

### 8.2 Observation Spam

| Risk | Severity | Mitigation |
|------|----------|------------|
| Tools auto-write too many L0 observations, bloating DB | Medium | `ObservationRepository.insert_observation()` has a per-session rate limit (max 50 per research query). Deduplication: same `source_row_id` + `observation_type` → skip. Exceptions logged but not raised to user. |

### 8.3 Intent Router False Positives

| Risk | Severity | Mitigation |
|------|----------|------------|
| Router routes a complex question directly, skipping needed tools | Low | Router returns confidence alongside the route. Below threshold, falls through to full agent loop. User can add `?` or `详细分析` keyword to force delegation. Also: the synthesize step can detect "I don't have enough data" and escalate back to the full planner. |

### 8.4 Vector Store Abstraction Overhead

| Risk | Severity | Mitigation |
|------|----------|------------|
| ABC adds unnecessary complexity for single implementation | Low | The interface is minimal (4 methods: search, add, delete, count). If we never swap Chroma, the cost is ~30 lines of ABC that cost nothing at runtime. If we DO swap, it saves rewriting 3 files. |

---

## 9. Phase 1 Implementation Plan

### 9.1 Guiding Principles

- **Additive only** — no existing file is modified beyond appending to api_server.py
- **Vertical slice** — each step is independently testable with curl
- **Repository-first** — repos built before tools that use them
- **Concrete granularity** — each step is one file, one purpose

### 9.2 Step-by-Step

| Step | File(s) Created | Lines | Description | Testable With |
|------|----------------|-------|-------------|---------------|
| **1.1** | `hermes/__init__.py` | 3 | Package init | — |
| **1.2** | `hermes/hermes_config.py` | ~70 | `HERMES_SYSTEM_PROMPT`, `SYNTHESIZE_PROMPT`, `TOOL_REGISTRY`, `INTENT_ROUTES`, LLM model config | `python -c "from hermes.hermes_config import TOOL_REGISTRY; print(len(TOOL_REGISTRY))"` |
| **1.3** | `repositories/__init__.py`, `repositories/base.py` | ~30 | `BaseRepository` ABC with `__init__(db_path)` | Import check |
| **1.4** | `repositories/news_repo.py` | ~50 | `NewsRepository.find_recent()`, `.search_by_keyword()`, `.count_recent()` | `python -c "from hermes.repositories.news_repo import NewsRepository; ..."` — read raw_news |
| **1.5** | `repositories/signal_repo.py` | ~70 | `SignalRepository.find_signals()`, `.get_performance_stats()`, `.get_consensus_breakdown()` | Read ai_decisions with aggregate queries |
| **1.6** | `repositories/obs_repo.py` | ~50 | `ObservationRepository.insert_observation()`, `.search_by_tags()`, `.get_recent()` | Insert/read from research_observations |
| **1.7** | `repositories/insight_repo.py` | ~50 | `InsightRepository.insert_insight()`, `.get_verified()`, `.get_by_confidence()` | Insert/read from research_insights |
| **1.8** | `memory/__init__.py`, `memory/vector_store_interface.py` | ~40 | `VectorStoreInterface` ABC with 4 abstract methods | Import check |
| **1.9** | `memory/embeddings.py` | ~35 | `embed_text()` via OpenRouter embeddings endpoint (OpenAI-compatible) | `python -c "from hermes.memory.embeddings import embed_text; v = asyncio.run(embed_text('test')); print(len(v))"` |
| **1.10** | `memory/chroma_store.py` | ~50 | `ChromaVectorStore` implementing the interface, graceful degradation if import fails | Search/add against test collection |
| **1.11** | `hermes/intent_router.py` | ~80 | `IntentRouter.classify(question)` → `IntentRoute(DIRECT\|DELEGATE, tool_name?, args?)` with ~10 regex patterns | `python -c "from hermes.intent_router import IntentRouter; r = IntentRouter(); print(r.classify('BTC有什么信号'))"` |
| **1.12** | `tools/__init__.py`, `tools/news_tool.py` | ~40 | `query_news()`, `count_news()` — wrap `NewsRepository` | CLI test with real DB |
| **1.13** | `tools/market_tool.py` | ~50 | `market_context()` — wrap `SignalRepository` for aggregation | CLI test |
| **1.14** | `tools/signal_tool.py` | ~45 | `query_signals()` — wrap `SignalRepository` | CLI test |
| **1.15** | `tools/history_tool.py` | ~45 | `search_history()` — uses `VectorStoreInterface` + `ObservationRepository` | CLI test |
| **1.16** | `tools/performance_tool.py` | ~60 | `analyze_performance()` — wraps `SignalRepository.get_performance_stats()` | CLI test with real DB |
| **1.17** | `hermes/hermes_runtime.py` | ~150 | `HermesAgent` class: `research()` method. DIRECT path (router → tool → synthesize) and DELEGATE path (plan → execute → synthesize). Reads from `TOOL_REGISTRY` and injects repositories + vector_store. | `curl -X POST /api/hermes/chat` with simple queries |
| **1.18** | `api_server.py` (additions) | ~50 | `POST /api/hermes/chat` endpoint, `_migrate_schema_hermes()` call in lifespan. All appended at end of file — no existing code touched. | Full end-to-end: curl + frontend |

### 9.3 Phase 1 Total

| Metric | Count |
|--------|-------|
| New files | 17 |
| Lines of new code | ~970 |
| Lines of existing code changed | 0 (except api_server.py append) |
| New dependencies | `chromadb` (lightweight, pip install) |
| New API endpoints | 1 (`POST /api/hermes/chat`) |

### 9.4 Phase 2 (Out of Scope, Listed for Reference)

| Feature | Description |
|---------|-------------|
| `POST /api/hermes/report` | Scheduled multi-asset Alpha Report generation |
| `GET /api/hermes/memory` | Browse L0/L1 memory from frontend |
| `POST /api/hermes/verify` | Human operator promotes L0 → L1 |
| Frontend HermesChat component | Mode toggle in AgentChat, markdown renderer, tool call audit display |
| `mcp__scheduled-tasks` daily trigger | Auto-generate morning Alpha Report |

---

## 10. Acceptance Criteria

| # | Criterion | Verification Method |
|---|-----------|-------------------|
| 1 | "BTC 现在有什么信号？" returns signals via direct router path (1 LLM call, <2s) | curl + timing check |
| 2 | "过去一周 BTC 为什么上涨？" returns multi-factor analysis via full agent loop (2-3 LLM calls, <8s) | curl + inspect `tool_calls_made` |
| 3 | "过去一周 BTC 的 BUY 信号表现怎么样？" returns win rate + avg PnL from `ai_decisions` forward tracking | curl + verify metrics against raw DB query |
| 4 | Intent router correctly classifies 10 predefined test queries (accuracy ≥ 90%) | Unit test: `test_intent_router.py` |
| 5 | No `.py` file in `tools/` or `hermes/` (excluding `repositories/`) contains SQL string literals | `grep -r "SELECT\|INSERT\|UPDATE\|DELETE" tools/ hermes/hermes_runtime.py hermes/intent_router.py hermes/hermes_config.py` |
| 6 | `research_observations` table created on startup; no `research_insights` written without `verified_by` field | Check DB after fresh start; attempt invalid insert |
| 7 | Vector store gracefully degrades: remove `chromadb`, verify `search_history` returns `[]` without crashing | `pip uninstall chromadb`, run query, check no error |
| 8 | Existing `/api/agent_chat` (Data Copilot) unchanged and functional | Regression: same queries as before |
| 9 | Existing `/api/events` SSE stream not blocked | Open EventBus during Hermes query |
| 10 | No import of `engine`, `trading/*`, or `binance_execution` | `grep -r "engine\|trading\|binance_execution" hermes/ tools/ repositories/ memory/` |

---

## 11. Appendix: File Change Summary

```
NEW FILES (17):
  backend/src_python/hermes/__init__.py
  backend/src_python/hermes/hermes_runtime.py
  backend/src_python/hermes/hermes_config.py
  backend/src_python/hermes/intent_router.py
  backend/src_python/tools/__init__.py
  backend/src_python/tools/news_tool.py
  backend/src_python/tools/market_tool.py
  backend/src_python/tools/signal_tool.py
  backend/src_python/tools/history_tool.py
  backend/src_python/tools/performance_tool.py
  backend/src_python/hermes/repositories/__init__.py
  backend/src_python/hermes/repositories/base.py
  backend/src_python/hermes/repositories/news_repo.py
  backend/src_python/hermes/repositories/signal_repo.py
  backend/src_python/hermes/repositories/obs_repo.py
  backend/src_python/hermes/repositories/insight_repo.py
  backend/src_python/hermes/memory/__init__.py
  backend/src_python/hermes/memory/vector_store_interface.py
  backend/src_python/hermes/memory/embeddings.py
  backend/src_python/hermes/memory/chroma_store.py

MODIFIED FILES (1, append-only):
  backend/src_python/api_server.py  — add POST /api/hermes/chat, schema migration call at end of file

UNTOUCHED (key files):
  backend/src_python/engine.py
  backend/src_python/trading/*
  backend/src_python/binance_execution_demo.py
  backend/src_python/treenews_bridge.py
  frontend/app/page.tsx
  frontend/lib/quant-data.ts
  frontend/components/quant/agent-chat.tsx
  frontend/components/quant/event-bus.tsx
  frontend/components/quant/kline-chart.tsx
  dashboard/app.py
```
