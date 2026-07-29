# Trident Agent MVP

新闻 → AI → 交易信号的实时量化系统。持续采集财经快讯（FinancialJuice WebSocket、
TreeNews webhook），经规则 + LLM 双层过滤去噪后，由 Kimi K3（经 OpenRouter）做宏观
博弈推演，产出带方向/得分/推导链的交易信号，并在 2 小时后用真实行情做前向验证
（WIN/LOSS 结算）。信号通过 SSE 实时推送到 Next.js 前端，同时推送飞书群机器人。

## 架构

```
 FinancialJuice WSS ──┐
                      ├─→ realtime_filter (L1 关键词 / L2 LLM) ─→ raw_news (SQLite, WAL)
 TreeNews webhook ────┘        (engine/ingest.py, engine/webhook.py)        │
 127.0.0.1:9000                                                             ▼
                                              engine/ai_worker.py (Kimi K3 via OpenRouter,
                                              市场快照 + 历史绩效注入, 父子信号聚合)
                                                                             │
                                                                             ▼
                                                                      ai_decisions 表
                                                                       │           │
                                                       engine/forward.py            │
                                                       2h 前向验证 (MFE/MAE/PnL,    │
                                                       WIN/LOSS 结算)               │
                                                                                   ▼
                                            api_server.py (FastAPI: REST + SSE, 127.0.0.1:8000)
                                                             │                │
                                                             ▼                ▼
                                            frontend/ (Next.js 14, :3000)   飞书告警
                                                                            (engine/alerts.py)

 dashboard/ (Streamlit :8501) — 离线复盘看板，直接读 SQLite，不在实时链路内
```

## 目录结构

```
Trident_Agent_MVP/
├── backend/
│   ├── src_python/
│   │   ├── engine.py            # 薄入口：python engine.py 启动引擎
│   │   ├── engine/              # 引擎包
│   │   │   ├── main.py          # 启动编排（DB migrate + 任务调度）
│   │   │   ├── ingest.py        # Task A: FinancialJuice WS 采集
│   │   │   ├── ai_worker.py     # Task B: LLM 批量分析 + 信号落库
│   │   │   ├── forward.py       # 2h 前向验证结算
│   │   │   ├── webhook.py       # TreeNews webhook + 英文前置翻译
│   │   │   ├── alerts.py        # 飞书告警推送
│   │   │   ├── prices.py        # 黄金/BTC/WTI 行情源
│   │   │   ├── ws_client.py     # stdlib WebSocket 客户端 (RFC 6455)
│   │   │   └── utils.py         # VIP 检测/去重 hash/垃圾文本判定等
│   │   ├── api_server.py        # FastAPI + SSE 后端服务
│   │   ├── realtime_filter.py   # L1 关键词 + L2 LLM 实时新闻过滤
│   │   ├── market_snapshot.py   # BTC/XAU 市场快照（prompt 注入用）
│   │   ├── treenews_bridge.py   # TreeNews WS → 本地 webhook 桥（独立进程）
│   │   ├── config.py            # 唯一配置入口（.env 加载/密钥/阈值）
│   │   └── db.py                # schema 迁移（唯一权威）+ SQL 只读安全门
│   ├── scripts/                 # 回测、历史新闻注入等辅助脚本
│   ├── tests/                   # pytest 冒烟套件（43 用例，全离线）
│   ├── init_db.py               # 全新部署的 DB 初始化（幂等，不删数据）
│   ├── requirements.txt         # 运行依赖
│   └── requirements-dev.txt     # 测试依赖
├── frontend/                    # Next.js 14 实时信号台
├── dashboard/                   # Streamlit 离线复盘看板（见 dashboard/README.md）
├── deploy/                      # systemd units + nginx 示例 + 部署文档
├── scripts/dev_start.bat        # Windows 一键开发启动
├── docs/                        # 设计文档（archive/ 为已下架方向的存档）
└── .env.example                 # 环境变量模板（复制为 backend/.env）
```

## 快速开始（Windows）

前置：Python 3.11+、Node.js 18+。

```cmd
copy .env.example backend\.env
REM 编辑 backend\.env，至少填入 OPENROUTER_API_KEY

pip install -r backend\requirements.txt
cd frontend && npm ci && cd ..

scripts\dev_start.bat
```

`dev_start.bat` 会开三个窗口：engine、api_server（127.0.0.1:8000）、frontend（localhost:3000）。

手动方式（三个终端）：

```cmd
cd backend\src_python && python engine.py
cd backend\src_python && uvicorn api_server:app --host 127.0.0.1 --port 8000
cd frontend && npm run dev
```

可选第四条链路（TreeNews 快讯源）：

```cmd
cd backend\src_python && python treenews_bridge.py
```

## 环境变量

复制 `.env.example` 为 `backend/.env` 后填写。完整注释见模板文件。

| 变量 | 必填 | 说明 |
|---|---|---|
| `OPENROUTER_API_KEY` | 是 | Kimi K3 主模型 + 实时过滤 L2 的调用通道 |
| `DEEPSEEK_API_KEY` / `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL` | 否 | Agent Chat 数据副驾（前端问答） |
| `AGENT_LLM_BASE_URL` / `AGENT_LLM_API_KEY` / `AGENT_LLM_MODEL` | 否 | Agent Chat 专用配置，默认可复用 DeepSeek |
| `XAI_API_KEY` | 否 | Grok 备用 |
| `DOUBAO_API_KEY` / `DOUBAO_MODEL` / `DOUBAO_BASE_URL` | 否 | 火山引擎（当前停用，DOUBAO_MODEL 须为 Endpoint ID） |
| `FEISHU_WEBHOOK_URL` | 建议 | 飞书群机器人告警地址；为空则跳过推送 |
| `CORS_ALLOW_ORIGINS` | 否 | API 跨域白名单，逗号分隔，默认 `http://localhost:3000,http://127.0.0.1:3000` |
| `HTTP_PROXY` | 视网络 | 国内机器访问 OpenRouter/Binance 需要；海外直连不要设 |
| `TREE_NEWS_PORT` | 否 | engine webhook 监听端口，默认 9000 |
| `TRIDENT_DB_PATH` | 否 | 覆盖 SQLite 路径，默认 `backend/trident_event_bus.db` |
| `MT5_*` / `BINANCE_*` | 否 | MetaTrader 5 本地行情 / 币安账户（均非必需） |

## 测试

```bash
cd backend
pip install -r requirements-dev.txt
python -m pytest tests/ -q     # 43 个用例，全部离线
```

## 部署

Linux 生产部署（systemd + nginx）见 [deploy/README.md](deploy/README.md)。

## 子项目

- `frontend/` — Next.js 14 实时信号台。开发 `npm run dev`（:3000），生产 `npm run build && npm start`。环境变量见 `frontend/.env.example`。
- `dashboard/` — Streamlit 离线复盘看板（:8501），直接读 SQLite/Excel 做历史信号分析，不在实时链路内。详见 `dashboard/README.md`。
- `docs/` — 架构设计文档；`docs/archive/` 是已移除的 Hermes 研究 Agent 层设计存档。
