# Hermes Research Agent Layer — Architecture Design Document

> **Status:** Draft for Review  
> **Date:** 2026-07-21  
> **Author:** Claude (Architecture)  
> **Target:** Trident Agent MVP v2

---

## 1. Executive Summary

Hermes is a **Read-Only Research Agent** that sits beside Trident's existing production pipeline. It answers natural-language questions like "最近24小时 BTC 为什么上涨？" by orchestrating multi-tool queries (news, market context, signals, historical events, research memory) and synthesizing a research-grade answer via LLM. It does NOT execute trades, modify data, or touch the trading subsystem.

---

## 2. Current Trident Architecture (As-Is)

### 2.1 Process Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                        Trident Agent MVP                         │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   engine.py       │    │  api_server.py    │                   │
│  │   (prod loop)     │    │  (FastAPI :8000)  │                   │
│  │                   │    │                   │                   │
│  │  WS ingest ───┐   │    │  SSE /api/events  │──► frontend      │
│  │  AI worker ───┤   │    │  GET /api/events  │    EventBus       │
│  │  fwd tracker ─┤   │    │  POST /api/agent_ │──► AgentChat      │
│  │               │   │    │        chat       │    (Data Copilot) │
│  │  sync sqlite3 ▼   │    │  GET /api/export  │──► Excel export   │
│  │  ┌──────────────┐ │    │                   │                   │
│  │  │trident_event │◄┼────┤  async aiosqlite  │                   │
│  │  │  _bus.db     │ │    │  (reads only)     │                   │
│  │  │              │ │    └──────────────────┘                   │
│  │  │ raw_news     │ │                                           │
│  │  │ ai_decisions │ │    ┌──────────────────┐                   │
│  │  └──────────────┘ │    │  trading/         │                   │
│  └──────────────────┘    │  (own DB)         │                   │
│                           │  Signal→Aggregate │                   │
│  ┌──────────────────┐    │  →Decision→Risk   │                   │
│  │ treenews_bridge  │    │  →Execute(Binance) │                   │
│  │ (webhook ingest) │    └──────────────────┘                   │
│  └──────────────────┘                                           │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  frontend/  (Next.js 14 :3000)                            │   │
│  │  EventBus ←─SSE── AgentChat ←─POST /api/agent_chat        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow (Writes)

```
FinancialJuice WS ──► engine.py ──► raw_news (INSERT)
                  ──► engine.py ──► ai_worker (6-model parallel)
                  ──► engine.py ──► ai_decisions (INSERT + JSON consensus)
                  ──► engine.py ──► forward_tracker (UPDATE after 2h)
```

### 2.3 Data Flow (Reads — API Server)

```
api_server.py db_watcher: poll every 1s → _fetch_rows_since(last_id)
  → _row_to_event(row) → parse extra_models_consensus JSON
  → _broadcast_sse(event) → all connected EventSource clients

api_server.py agent_chat_endpoint:
  → LLM Text-to-SQL → safety gate → execute via aiosqlite
  → LLM Data-to-Text summary → return {reply, sql, rows}
```

### 2.4 Database Schema (trident_event_bus.db, WAL mode)

| Table | Key Columns | Purpose |
|-------|------------|---------|
| `raw_news` | id, timestamp, source, content, status | Incoming news items from FinancialJuice + TreeNews |
| `ai_decisions` | id, news_id(FK), suggested_action, sentiment_score, reasoning, reasoning_path, market_category, target_asset, vip_tag, extra_models_consensus(JSON), entry_price, exit_price, is_correct, settled | AI decisions with 6-model voting, forward tracking P&L |

### 2.5 Existing Agent Chat (What Hermes Replaces/Extends)

The current Data Copilot (`/api/agent_chat`) is a **stateless two-pass SQL generator**. It:
- Converts natural language → SQL via DeepSeek LLM (pass 1)
- Executes SQL against `trident_event_bus.db` directly
- Summarizes results via LLM (pass 2)

**Limitations it shares that Hermes must fix:**
- No multi-tool orchestration (only SQL)
- No memory/context across queries
- No vector search for semantic "similar events"
- No structured research report output
- Stateless — each question starts from scratch
- Raw DB access violates the "Tools API only" principle

---

## 3. Hermes Target Architecture (To-Be)

### 3.1 Design Principles

1. **Read-Only Isolation** — Hermes never writes to `trident_event_bus.db`. All DB access goes through a Tools API wrapper (new endpoints on api_server.py). Hermes runtime calls these endpoints via HTTP internally, or (simpler for MVP) calls the tool functions directly since they run in the same process.
2. **No Refactoring** — `engine.py`, `trading/*`, `binance_execution.py`, `frontend/*` are untouched. Hermes is additive only.
3. **Lightweight** — No LangChain, no CrewAI, no complex Multi-Agent framework. A simple Tool-Use loop with an LLM orchestrator.
4. **Two-Phase Delivery** — Phase 1: On-demand research queries. Phase 2: Scheduled daily Alpha Reports.

### 3.2 Component Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Hermes Research Layer                         │
│                                                                       │
│  ┌──────────────────────┐    ┌──────────────────────────────────┐   │
│  │  hermes_runtime.py    │    │  tools/                           │   │
│  │  (Agent Loop)         │    │  (5 Tool Functions)               │   │
│  │                       │    │                                   │   │
│  │  ┌─────────────────┐  │    │  news_tool.py       ──► raw_news │   │
│  │  │ HermesAgent      │  │    │  market_tool.py     ──► prices  │   │
│  │  │ - plan()         │──┼────┤  signal_tool.py     ──► ai_     │   │
│  │  │ - execute()      │  │    │                        decisions│   │
│  │  │ - synthesize()   │  │    │  history_tool.py    ──► vector  │   │
│  │  │ - remember()     │  │    │  memory_tool.py     ──► research│   │
│  │  └─────────────────┘  │    │                        _memory   │   │
│  │         │              │    └──────────────────────────────────┘   │
│  │         ▼              │                                           │
│  │  ┌─────────────────┐  │    ┌──────────────────────────────────┐   │
│  │  │ LLM Client       │  │    │  memory/                          │   │
│  │  │ (DeepSeek/OpenAI │  │    │                                   │   │
│  │  │  compatible)     │  │    │  vector_store/  (Chroma)          │   │
│  │  └─────────────────┘  │    │  research_notes/ (SQLite table)    │   │
│  └──────────────────────┘    └──────────────────────────────────┘   │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  api_server.py  (additions — no existing code changed)        │   │
│  │                                                               │   │
│  │  POST /api/hermes/chat     ← new: research agent endpoint     │   │
│  │  POST /api/hermes/report   ← new: generate research report    │   │
│  │  GET  /api/hermes/memory   ← new: browse research memory      │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.3 Insertion Point: Why Co-locate with api_server.py

| Option | Pros | Cons |
|--------|------|------|
| **A) New endpoints on api_server.py** ✅ | Share DB path, env vars, existing LLM config; one process to manage; tools can call aiosqlite directly | Adds ~400 lines to a 1058-line file; but modular tools keep it clean |
| B) Separate FastAPI on different port | Full isolation, independent scaling | Two processes to manage, need cross-service HTTP calls for tools, port conflicts |
| C) Standalone headless agent | No HTTP overhead | No frontend integration, hard to trigger ad-hoc queries |

**Decision: Option A** — add new endpoints to api_server.py with tool modules in a separate `tools/` package. The Hermes runtime runs as an async agent loop invoked by the new endpoint, same pattern as the existing `agent_chat_endpoint`. Keep api_server.py clean by extracting all Hermes logic into `hermes_runtime.py` — api_server.py only does request routing.

---

## 4. New Modules (File-Level Breakdown)

### 4.1 Directory Structure

```
backend/src_python/
  hermes/
    __init__.py              # Package exports
    hermes_runtime.py        # HermesAgent class + agent loop
    hermes_config.py         # Config constants, prompts, tool registry
  tools/                     # Tool implementations (stateless functions)
    __init__.py
    news_tool.py             # Tool 1: News Query
    market_tool.py           # Tool 2: Market Context
    signal_tool.py           # Tool 3: Signal Query
    history_tool.py          # Tool 4: Historical Event Search (vector)
    memory_tool.py           # Tool 5: Research Memory (CRUD)
  memory/
    __init__.py
    vector_store.py          # Chroma wrapper (embed + search)
    embeddings.py            # Text → vector (OpenAI/DeepSeek embeddings API)
```

### 4.2 api_server.py Additions (net-new code block at end of file)

```python
# ── Hermes Research Agent ──
from hermes.hermes_runtime import HermesAgent
from hermes.hermes_config import HERMES_SYSTEM_PROMPT, TOOL_REGISTRY

class HermesChatRequest(BaseModel):
    user_message: str
    session_id: str = ""       # for multi-turn memory
    active_market: str = "ALL"

class HermesChatResponse(BaseModel):
    answer: str
    tool_calls_made: List[str]  # audit trail
    sources: List[Dict]         # citations
    session_id: str

@app.post("/api/hermes/chat", response_model=HermesChatResponse)
async def hermes_chat_endpoint(req: HermesChatRequest):
    agent = HermesAgent(tool_registry=TOOL_REGISTRY)
    result = await agent.research(req.user_message, req.active_market, req.session_id)
    return result

@app.post("/api/hermes/report")
async def hermes_report_endpoint(req: HermesReportRequest):
    """Generate a structured research report (Phase 2 — daily Alpha loop)."""
    ...
```

### 4.3 Module Details

#### 4.3.1 `hermes_runtime.py` — HermesAgent (Agent Loop)

The core agent loop. A single Python class with async methods:

```
class HermesAgent:
    tool_registry: Dict[str, ToolDef]   # {tool_name: {fn, description, parameters}}
    llm_client: openai.AsyncOpenAI       # reuses existing DeepSeek config
    session_id: str

    async def research(question, market, session_id) → HermesChatResponse:
        """
        Multi-turn agent loop:
        1. Plan: LLM decides which tools to call (1-N tools, sequenced)
        2. Execute: Call each tool, collect results
        3. Synthesize: LLM writes research answer from tool outputs
        4. Remember: Save findings to research_memory (optional)
        5. Return: Answer + tool_call audit trail + citations
        """

    async def _plan(user_message, tool_descriptions) → List[ToolCall]:
        """LLM: given the question and available tools, produce a plan.
           Returns list of {tool_name, args}. Max 5 tool calls per research query."""

    async def _execute(tool_calls) → Dict[str, ToolResult]:
        """Call each tool in sequence (can parallelize independent calls)."""

    async def _synthesize(user_message, tool_results) → str:
        """LLM: synthesize tool results into a research answer."""

    async def _remember(question, answer, sources):
        """Save to research_memory if the answer is substantive."""
```

**Agent loop pattern (Tool-Use, not ReAct):**

```
User: "最近24小时 BTC 为什么上涨？"
  │
  ▼
Step 1 — Plan (LLM call #1):
  system: HERMES_SYSTEM_PROMPT + TOOL_DESCRIPTIONS
  user:   "最近24小时 BTC 为什么上涨？"
  
  LLM output (JSON):
  {
    "plan": [
      {"tool": "query_news", "args": {"asset": "BTC", "hours": 24, "limit": 20}},
      {"tool": "market_context", "args": {"asset": "BTC"}},
      {"tool": "query_signals", "args": {"asset": "BTC", "hours": 24}},
      {"tool": "search_history", "args": {"query": "BTC 上涨 原因", "days": 30}}
    ]
  }
  │
  ▼
Step 2 — Execute (parallel tool calls):
  query_news()     → [{news_text, timestamp, source, vip_tag}, ...]
  market_context() → {current_price, change_24h, sentiment_distribution}
  query_signals()  → [{action, score, reasoning, model_consensus}, ...]
  search_history() → [{similar_event, similarity_score, date}, ...]
  │
  ▼
Step 3 — Synthesize (LLM call #2):
  system: SYNTHESIZE_PROMPT
  user:   "最近24小时 BTC 为什么上涨？" + compacted tool results
  
  LLM output (markdown text):
  "根据多维数据分析，BTC 过去24小时上涨的核心驱动因素有：
   1. **宏观利好**：FOMC 会议纪要偏鸽派，市场预期降息...
   2. **链上信号**：Kimi K3 模型在 3 条关键新闻上给出 BUY 评级...
   3. **历史模式相似**：当前走势与 2024-01-12 的反弹形态高度相关..."
  │
  ▼
Step 4 — Remember (optional, async fire-and-forget):
  Save to research_memory: {question, answer_summary, sources, embedding}
  │
  ▼
Step 5 — Return to frontend:
  { answer, tool_calls_made, sources, session_id }
```

#### 4.3.2 `hermes_config.py` — Prompts & Tool Registry

```python
# System prompt for the planning LLM
HERMES_SYSTEM_PROMPT = """
You are Hermes, a quantitative research agent for the Trident trading system.
You have access to read-only tools that query market data, news, signals, and history.
Your job: given a user's research question, decide which tools to call, in what order.

Rules:
- Prefer parallel tool calls when tools are independent.
- Max 5 tool calls per research query.
- For "why did X move" questions, always query: news, market_context, signals, AND history.
- Respond with a JSON plan only.

Available tools:
{tool_descriptions}
"""

SYNTHESIZE_PROMPT = """
You are a professional quantitative research analyst.
Given a user's question and the tool results below, produce a structured research answer.

Rules:
- Identify the top 2-3 driving factors behind the market movement.
- Cite specific news headlines, signal actions, and model names.
- Use Chinese, professional but accessible tone.
- Mention data sources: news source, model name, time range.
- If results are sparse, say so honestly rather than fabricating.
- Format: markdown with clear sections.
- Include a confidence level (高/中/低) based on data quality.
"""

# Tool registry — maps tool name → function + description for LLM
TOOL_REGISTRY = {
    "query_news": {
        "fn": query_news,
        "description": "Query news articles filtered by asset, time range, and source. Returns headline, content, timestamp, VIP tags.",
        "parameters": {"asset": "str", "hours": "int (default 24)", "limit": "int (default 20)"},
    },
    "market_context": {
        "fn": market_context,
        "description": "Get current market state: price, 24h change, sentiment distribution across models, recent signal count by action type.",
        "parameters": {"asset": "str"},
    },
    "query_signals": {
        "fn": query_signals,
        "description": "Query AI trading signals with model consensus breakdown. Returns action, score, reasoning from Kimi K3 + sub-models.",
        "parameters": {"asset": "str", "hours": "int (default 24)", "limit": "int (default 20)"},
    },
    "search_history": {
        "fn": search_history,
        "description": "Semantic search over past events and research notes. Good for finding 'similar patterns in history'.",
        "parameters": {"query": "str (natural language)", "days": "int (default 30)", "limit": "int (default 10)"},
    },
    "save_memory": {
        "fn": save_memory,
        "description": "Save a research finding to persistent memory for future reference.",
        "parameters": {"content": "str", "tags": "List[str]", "question": "str (optional)"},
    },
}
```

#### 4.3.3 `tools/news_tool.py` — Tool 1: News Query

```python
"""
Tool: query_news(asset, hours, limit) → List[NewsItem]

Reads raw_news via aiosqlite (same pattern as api_server.py).
Never writes. Filters by: asset keyword in content, time range, source.
"""
async def query_news(asset: str, hours: int = 24, limit: int = 20) -> List[Dict]:
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "trident_event_bus.db")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT rn.id, rn.timestamp, rn.source,
                   substr(rn.content, 0, 500) as news_text,
                   rn.status
            FROM raw_news rn
            WHERE rn.timestamp >= datetime('now', 'localtime', ?)
              AND rn.content LIKE ?
            ORDER BY rn.timestamp DESC
            LIMIT ?
        """, (f"-{hours} hours", f"%{asset}%", limit))
        rows = await cursor.fetchall()
        await cursor.close()
    return [dict(r) for r in rows]
```

#### 4.3.4 `tools/market_tool.py` — Tool 2: Market Context

```python
"""
Tool: market_context(asset) → MarketContext

Aggregates: current sentiment distribution, recent signal counts,
model voting summary. Pure read — no external API calls (uses cached DB data).
"""
async def market_context(asset: str) -> Dict:
    DB_PATH = ...
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Signal distribution (last 24h)
        cursor = await db.execute("""
            SELECT suggested_action, COUNT(*) as cnt,
                   AVG(sentiment_score) as avg_score
            FROM ai_decisions
            WHERE target_asset = ? AND created_at >= datetime('now', 'localtime', '-24 hours')
            GROUP BY suggested_action
        """, (asset,))
        actions = [dict(r) for r in await cursor.fetchall()]

        # Model consensus summary
        cursor = await db.execute("""
            SELECT extra_models_consensus
            FROM ai_decisions
            WHERE target_asset = ? AND created_at >= datetime('now', 'localtime', '-24 hours')
            ORDER BY id DESC LIMIT 50
        """, (asset,))
        consensus_rows = [dict(r) for r in await cursor.fetchall()]
        await cursor.close()

    # Parse consensus JSON to count per-model actions
    model_votes = _aggregate_consensus(consensus_rows)

    return {
        "asset": asset,
        "signal_distribution": actions,
        "model_votes": model_votes,   # {"DeepSeek": {"BUY": 3, "SELL": 1}, ...}
        "query_time": datetime.now().isoformat(),
    }
```

#### 4.3.5 `tools/signal_tool.py` — Tool 3: Signal Query

```python
"""
Tool: query_signals(asset, hours, limit) → List[SignalItem]

Returns detailed trading signals with full reasoning and model consensus.
"""
async def query_signals(asset: str, hours: int = 24, limit: int = 20) -> List[Dict]:
    DB_PATH = ...
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT ad.id, ad.created_at, ad.suggested_action, ad.sentiment_score,
                   ad.reasoning, ad.reasoning_path, ad.market_category,
                   ad.vip_tag, ad.extra_models_consensus,
                   ad.is_correct, ad.entry_price, ad.exit_price
            FROM ai_decisions ad
            WHERE ad.target_asset = ?
              AND ad.created_at >= datetime('now', 'localtime', ?)
            ORDER BY ad.id DESC
            LIMIT ?
        """, (asset, f"-{hours} hours", limit))
        rows = await cursor.fetchall()
        await cursor.close()
    return _enrich_consensus([dict(r) for r in rows])
```

#### 4.3.6 `tools/history_tool.py` — Tool 4: Historical Event Search (Vector)

```python
"""
Tool: search_history(query, days, limit) → List[SimilarEvent]

Uses Chroma vector store to find semantically similar past events.
1. Embed the query → vector via OpenAI/DeepSeek embeddings API
2. Query Chroma collection for nearest neighbors
3. Return ranked results with similarity scores
"""
async def search_history(query: str, days: int = 30, limit: int = 10) -> List[Dict]:
    from memory.vector_store import get_collection
    from memory.embeddings import embed_text

    query_vec = await embed_text(query)
    collection = get_collection("research_memory")
    results = collection.query(
        query_embeddings=[query_vec],
        n_results=limit,
        where={"timestamp": {"$gte": (datetime.now() - timedelta(days=days)).isoformat()}},
    )
    return _format_chroma_results(results)
```

#### 4.3.7 `tools/memory_tool.py` — Tool 5: Research Memory

```python
"""
Tool: save_memory(content, tags, question) → Dict
Tool: recall_memory(query, limit) → List[Dict]  (also via vector search)

CRUD for the research_memory SQLite table + Chroma collection.
Every save: write to SQLite (structured) + embed into Chroma (semantic).
"""
async def save_memory(content: str, tags: List[str] = [], question: str = "") -> Dict:
    DB_PATH = ...
    from memory.embeddings import embed_text
    from memory.vector_store import get_collection

    memory_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    # SQLite: structured storage
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO research_memory (id, question, content, tags, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (memory_id, question, content, json.dumps(tags), now))
        await db.commit()

    # Chroma: semantic search
    embedding = await embed_text(f"{question} {content}")
    collection = get_collection("research_memory")
    collection.add(
        ids=[memory_id],
        embeddings=[embedding],
        metadatas=[{"tags": ",".join(tags), "question": question, "created_at": now}],
        documents=[content],
    )

    return {"id": memory_id, "saved": True}
```

---

## 5. Database Extensions

### 5.1 New SQLite Table: `research_memory`

Added to `trident_event_bus.db` via schema migration (same ALTER TABLE pattern).

```sql
CREATE TABLE IF NOT EXISTS research_memory (
    id          TEXT PRIMARY KEY,              -- UUID
    question    TEXT DEFAULT '',               -- original research question
    content     TEXT NOT NULL,                 -- research finding/answer
    tags        TEXT DEFAULT '[]',             -- JSON array of tags
    created_at  TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_research_memory_created
    ON research_memory(created_at);
CREATE INDEX IF NOT EXISTS idx_research_memory_tags
    ON research_memory(tags);
```

### 5.2 Vector Store: Chroma (Embedded Mode)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Vector DB | **Chroma** (not pgvector) | Zero-infra: embedded Python library, no PostgreSQL dependency. Trident runs on SQLite — adding PG just for vectors is overkill for MVP |
| Embedding Model | **DeepSeek embeddings** (`text-embedding-3-small` compatible) or **OpenAI `text-embedding-3-small`** | Reuses existing API key; 1536-dim vectors |
| Collection | Single `research_memory` collection | Stores: news events (summarized), research findings, user-saved notes |
| Persistence | `backend/chroma_data/` directory | Survives restarts; auto-created on first use |

**Why not pgvector:**
- Requires PostgreSQL installation and management
- Trident is a single-machine MVP, not distributed
- Chroma's embedded mode is perfect for <100K documents
- Migration path: if we outgrow Chroma, the abstraction in `vector_store.py` means we swap the backend without touching tools

### 5.3 Schema Migration Strategy

Follow the existing pattern from api_server.py's `_migrate_schema()`:

```python
def _migrate_schema_hermes():
    """Add Hermes tables. Safe to call repeatedly — uses IF NOT EXISTS."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS research_memory (...)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_research_memory_created ...")
        conn.commit()
    finally:
        conn.close()
```

Called once at api_server.py startup (in lifespan), before db_watcher starts.

---

## 6. Frontend Integration

### 6.1 New Component: `HermesChat`

A new tab or mode in the existing AgentChat component, or a separate component. The user switches between "Data Copilot" (SQL mode) and "Hermes Research" (multi-tool mode).

**Minimal approach (recommended for MVP):** Extend the existing AgentChat component with a mode toggle. The Hermes mode posts to `/api/hermes/chat` instead of `/api/agent_chat`.

```
┌─ AgentChat ──────────────────────────────────────────┐
│  [Data Copilot] [Hermes Research]  ← mode toggle     │
│                                                       │
│  ┌─────────────────────────────────────────────────┐ │
│  │ Assistant: 你好，我是 Hermes 研究助手...         │ │
│  │                                                  │ │
│  │ User: 最近24小时 BTC 为什么上涨？                 │ │
│  │                                                  │ │
│  │ Hermes:                                          │ │
│  │ ## BTC 上涨分析 (置信度: 高)                      │ │
│  │                                                  │ │
│  │ ### 1. 宏观催化剂                                 │ │
│  │ ...                                              │ │
│  │                                                  │ │
│  │ ⚙️ 工具调用: query_news, market_context,          │ │
│  │    query_signals, search_history                 │ │
│  │                                                  │ │
│  │ 📎 来源: FinancialJuice (3篇), DeepSeek 模型...   │ │
│  └─────────────────────────────────────────────────┘ │
│                                                       │
│  [_____________________________] [Send]               │
└───────────────────────────────────────────────────────┘
```

### 6.2 API Contract

```typescript
// POST /api/hermes/chat
interface HermesChatRequest {
  user_message: string
  session_id?: string       // empty = new session
  active_market?: string    // "CRYPTO" | "GOLD" | ...
}

interface HermesChatResponse {
  answer: string            // markdown-formatted research answer
  tool_calls_made: string[] // ["query_news", "market_context", ...]
  sources: {
    type: string            // "news" | "signal" | "history" | "memory"
    count: number
    summary: string
  }[]
  session_id: string
}
```

---

## 7. Tool Execution Flow (End-to-End Example)

User asks: **"最近24小时 BTC 为什么上涨？"**

```
┌─ Step 1: Plan (LLM #1, ~0.8s) ──────────────────────────┐
│  Input:  user question + HERMES_SYSTEM_PROMPT +          │
│          TOOL_DESCRIPTIONS + time_context()              │
│  Output: {"plan": [                                      │
│    {"tool":"query_news","args":{"asset":"BTC","hours":24}},│
│    {"tool":"market_context","args":{"asset":"BTC"}},      │
│    {"tool":"query_signals","args":{"asset":"BTC","hours":24}},│
│    {"tool":"search_history","args":{"query":"BTC price surge cause","days":30}}│
│  ]}                                                      │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 2: Execute (parallel, ~0.3s) ─────────────────────┐
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ query_news   │  │market_context│  │query_signals │    │
│  │ → 18 results │  │→ {dist,      │  │→ 12 results  │    │
│  │              │  │   votes}     │  │              │    │
│  └─────────────┘  └──────────────┘  └──────────────┘    │
│  ┌──────────────┐                                        │
│  │search_history│  (Chroma query)                        │
│  │→ 5 results   │                                        │
│  └──────────────┘                                        │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 3: Compact results ───────────────────────────────┐
│  news: "Fed dovish minutes... SEC drops case...          │
│         whale accumulation... Bitcoin ETF inflow $450M"  │
│  market: {BUY:8, SELL:2, HOLD:3}, DeepSeek BUY:6/10,    │
│          Gemini BUY:5/10                                 │
│  signals: avg_score=+0.62, 8 BUY / 2 SELL / 2 HOLD      │
│  history: similar pattern on 2024-01-12 (ETF approval    │
│           rally), 2024-03-20 (Fed pivot)                 │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 4: Synthesize (LLM #2, ~1.5s) ────────────────────┐
│  Input:  SYNTHESIZE_PROMPT + user question + compacted   │
│          tool results                                    │
│  Output: markdown research answer (see example below)    │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─ Step 5: Remember (async, fire-and-forget) ─────────────┐
│  save_memory({                                          │
│    question: "最近24小时 BTC 为什么上涨？",               │
│    content: <summary of answer>,                         │
│    tags: ["BTC", "price_analysis", "macro"]              │
│  })                                                     │
│  → SQLite INSERT + Chroma upsert                        │
└──────────────────────────────────────────────────────────┘
```

---

## 8. Risk Analysis

### 8.1 Tool Safety

| Risk | Severity | Mitigation |
|------|----------|------------|
| Tools inadvertently write to DB | Medium | All tool functions use read-only aiosqlite connections (SELECT only). Code review: every `db.execute()` in tools/ must be SELECT. Add a `_assert_readonly(sql)` wrapper that raises if INSERT/UPDATE/DELETE detected. |
| LLM hallucinates tool calls | Low | The plan JSON is validated against `TOOL_REGISTRY` before execution. Unknown tool names → rejected with error message. Invalid args → caught by Python type checking. |
| Tool call explosion (too many parallel queries) | Low | Hard cap: max 5 tool calls per research query. Each tool has a row limit (max 50). Overall timeout: 30s per research query. |
| Prompt injection via user message | Medium | User message is passed to LLM in a `user` role message, never in `system` role. The system prompt is immutable. LLM output is JSON-parsed and validated before tool execution. |

### 8.2 Query Isolation

| Risk | Severity | Mitigation |
|------|----------|------------|
| Hermes queries slow down the main SSE pipeline | Low | Hermes uses its own aiosqlite connections (separate `async with` blocks). SQLite WAL mode allows concurrent readers. Hermes reads are lightweight SELECTs with LIMITs. |
| Chroma blocks the event loop | Medium | Chroma query is synchronous by default. Wrap in `asyncio.to_thread()`. Embedding API call is already async via `openai.AsyncOpenAI`. |

### 8.3 Vector Store Dependency

| Risk | Severity | Mitigation |
|------|----------|------------|
| Chroma not installed / fails to start | Medium | `search_history` tool gracefully degrades: if Chroma unavailable, returns empty results + logs warning. Research still works with other 4 tools. |
| Embedding API quota exhausted | Low | Same graceful degradation. Embedding failure → skip history search, proceed with news+signals+market. |
| Vector store corruption (Chroma data dir) | Low | Chroma data is derivative (re-embeddable from SQLite `research_memory`). Recovery: delete `chroma_data/`, re-embed from SQLite. Add a `--rebuild-vectors` CLI command. |

### 8.4 Performance Impact

| Scenario | Expected Latency | Bottleneck |
|----------|-----------------|------------|
| Simple question ("今天BTC信号如何？") | 1-2s | 1 LLM call (plan) + 1 tool call |
| Complex question ("BTC为什么上涨？") | 3-5s | 2 LLM calls (plan + synthesize) + 4 parallel tool calls |
| History search with embeddings | +0.5-1s | Embedding API call (~200ms) + Chroma query (~50ms) |

All well within acceptable chat UX (user expects 3-8s for research answers).

### 8.5 Key Non-Risks (Things That Are Safe)

- **No risk to trading**: Hermes never calls `trading/` modules. Completely separate code path.
- **No risk to engine.py**: Hermes doesn't import from engine.py. Both read the same DB, but engine writes via sync sqlite3, Hermes reads via async aiosqlite — no lock contention in WAL mode.
- **No risk to frontend stability**: New endpoints are additive. Existing `/api/events`, `/api/agent_chat` are unchanged.
- **No risk of data corruption**: Hermes is read-only on `ai_decisions` and `raw_news`. It writes only to its own `research_memory` table.

---

## 9. Implementation Phases

### Phase 1: MVP (Target: ~400 lines new code)

| Step | Module | Lines | Description |
|------|--------|-------|-------------|
| 1.1 | `hermes/hermes_config.py` | ~60 | System prompt, synthesize prompt, TOOL_REGISTRY |
| 1.2 | `tools/news_tool.py` | ~50 | query_news() |
| 1.3 | `tools/market_tool.py` | ~60 | market_context() + consensus aggregation helper |
| 1.4 | `tools/signal_tool.py` | ~50 | query_signals() + consensus enricher |
| 1.5 | `tools/history_tool.py` | ~40 | search_history() with Chroma (graceful degrade) |
| 1.6 | `memory/vector_store.py` | ~40 | Chroma wrapper (init, get_collection) |
| 1.7 | `memory/embeddings.py` | ~30 | embed_text() via OpenAI-compatible API |
| 1.8 | `hermes/hermes_runtime.py` | ~120 | HermesAgent class: plan→execute→synthesize→remember |
| 1.9 | `api_server.py` (additions) | ~60 | POST /api/hermes/chat endpoint, schema migration call |
| 1.10 | `agent-chat.tsx` (mode toggle) | ~30 | Add "Hermes Mode" toggle, POST to /api/hermes/chat |

**Total: ~540 lines of new code, zero lines of existing code modified.**

### Phase 2: Daily Alpha Reports (Future)

| Feature | Description |
|---------|-------------|
| Scheduled research loop | Cron-triggered: every morning, Hermes auto-queries overnight news, generates a structured Alpha Report |
| Report storage | Save reports to `research_memory` with type="alpha_report" |
| Frontend report viewer | New view in AgentChat: list past reports, open full report |
| Multi-asset sweep | Auto-generate reports for BTC, ETH, GOLD, OIL in one run |
| Scheduled task integration | Use the existing `mcp__scheduled-tasks__create_scheduled_task` to trigger daily reports |

---

## 10. Acceptance Criteria

| # | Criterion | Verification |
|---|-----------|-------------|
| 1 | "最近24小时 BTC 为什么上涨？" returns a multi-factor analysis citing news, signals, market context, and history | Manual test via curl + frontend |
| 2 | Hermes never modifies `ai_decisions` or `raw_news` | Code review: all tool functions are SELECT-only |
| 3 | `research_memory` table is created on first startup | Check DB schema after fresh start |
| 4 | Chroma gracefully degrades if not installed | Remove `chromadb` package, verify other 4 tools still work |
| 5 | Existing `/api/agent_chat` (Data Copilot) still works unchanged | Regression test existing chat queries |
| 6 | SSE event stream is not blocked by Hermes queries | Open EventBus + Hermes chat simultaneously |
| 7 | Multi-turn memory: second question can reference first | "那ETH呢？" after BTC question → understands context |
| 8 | No import of engine.py, trading/*, or binance_execution.py | `grep -r "import.*engine\|from.*trading" hermes/ tools/ memory/` |

---

## 11. Open Questions for Review

1. **LLM for Hermes**: Use the same DeepSeek (`deepseek-chat`) as the Data Copilot, or should Hermes use a different model (e.g., Kimi K3 via OpenRouter for better reasoning)?
2. **Embedding model**: `text-embedding-3-small` (OpenAI, $0.02/1M tokens) or DeepSeek's embedding endpoint? DeepSeek is simpler (one API key) but OpenAI embeddings are more mature.
3. **Session persistence**: Should multi-turn memory survive server restarts? If yes, we need to load past messages from `research_memory`. If no, in-memory dict is fine.
4. **Frontend mode toggle**: Should Hermes replace the Data Copilot entirely, or coexist as a toggle? Recommendation: coexist as toggle — they serve different purposes (SQL exploration vs. research analysis).
5. **Chroma data directory**: `backend/chroma_data/` — OK? Or should it live alongside the DB at the project root?

---

## 12. Appendix: File Change Summary

```
NEW FILES (9):
  backend/src_python/hermes/__init__.py
  backend/src_python/hermes/hermes_runtime.py
  backend/src_python/hermes/hermes_config.py
  backend/src_python/tools/__init__.py
  backend/src_python/tools/news_tool.py
  backend/src_python/tools/market_tool.py
  backend/src_python/tools/signal_tool.py
  backend/src_python/tools/history_tool.py
  backend/src_python/tools/memory_tool.py
  backend/src_python/memory/__init__.py
  backend/src_python/memory/vector_store.py
  backend/src_python/memory/embeddings.py

MODIFIED FILES (2):
  backend/src_python/api_server.py     — add POST /api/hermes/chat, schema migration
  frontend/components/quant/agent-chat.tsx — add Hermes mode toggle (minimal)

UNTOUCHED FILES (key):
  backend/src_python/engine.py
  backend/src_python/trading/*
  backend/src_python/binance_execution_demo.py
  frontend/app/page.tsx
  frontend/lib/quant-data.ts
  dashboard/app.py
```

---

> **Next Step:** Review this document. Once approved, I'll implement Phase 1 in the order specified in Section 9.
