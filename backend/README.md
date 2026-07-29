# Trident Backend

实时新闻 → AI → 交易信号的后端。运行时代码全部在 `src_python/` 下，
以 `cd backend/src_python` 为工作目录启动（`config`/`db`/`realtime_filter` 等
均为同目录裸导入）。

## 模块结构

### engine/ 包（新闻引擎，`python engine.py` 启动）

| 模块 | 职责 |
|---|---|
| `engine/main.py` | 启动编排：代理状态打印、`db.migrate`、拉起 ingest / ai_worker / forward_tracker / webhook server |
| `engine/ingest.py` | Task A：FinancialJuice WSS 采集（Centrifugo 协议），去重 + 过滤 + 前置翻译后写 `raw_news` |
| `engine/ai_worker.py` | Task B：每秒批量认领 PENDING 新闻，Kimi K3（OpenRouter）推演，父子信号聚合后写 `ai_decisions`；含模型 roster、prompt、历史绩效注入 |
| `engine/forward.py` | 2h 前向验证：跟踪 max/min，到期按 IMPACT_THRESHOLD 结算 WIN/LOSS/HOLD，写 MFE/MAE/PnL |
| `engine/webhook.py` | TreeNews/Telegram webhook（127.0.0.1:9000）+ 英文前置翻译拦截器（Doubao→Gemini→DeepSeek 兜底链） |
| `engine/alerts.py` | 飞书群机器人信号卡片推送（webhook 未配置时警告一次并跳过） |
| `engine/prices.py` | 黄金（Sina/EastMoney）、BTC（Binance）、WTI 现价抓取 |
| `engine/ws_client.py` | 纯 stdlib 的 RFC 6455 WebSocket 客户端 + FinancialJuice token/cookie 抓取 |
| `engine/utils.py` | VIP KOL 检测、DB 连接、时间戳、去重 hash、垃圾文本判定 |

### 顶层模块

| 模块 | 职责 |
|---|---|
| `api_server.py` | FastAPI + SSE（127.0.0.1:8000）：事件流 `/api/events/stream`、REST `/api/events`、K 线、Excel 导出、Agent Chat（Text-to-SQL 数据副驾） |
| `realtime_filter.py` | ingest 前置过滤：L1 关键词（JUNK/CORE 词表）+ L2 LLM 二分类 |
| `market_snapshot.py` | 每轮 AI batch 前拉取 BTC/XAU 行情快照，注入 prompt |
| `treenews_bridge.py` | 独立进程：wss://news.treeofalpha.com → POST 到 engine webhook:9000（依赖 websocket-client） |
| `config.py` | 唯一配置入口：加载 `backend/.env`（仅一次）、密钥、DB 路径、CORS、全部阈值常量 |
| `db.py` | schema 迁移唯一权威（幂等 `migrate()`）+ `assert_readonly_sql` 只读安全门 |

### scripts/（辅助脚本，非常驻）

| 脚本 | 用途 |
|---|---|
| `backtest.py` | 历史决策回测 & 直接分析 |
| `feed_news.py` | 历史新闻批量注入 `raw_news`（回测用） |
| `news_ingestion_pipeline.py` | 多层新闻采集管线 |
| `check_credits.py` | 查 OpenRouter 余额 |
| `check_mt5.py` | MT5 终端连通性诊断（黄金 tick 问题排查） |
| `test_grok.py` | xAI Grok API key/模型可用性快测 |

## 启动

```bash
cd backend/src_python
python engine.py                                            # 引擎
uvicorn api_server:app --host 127.0.0.1 --port 8000         # API
python treenews_bridge.py                                   # 可选：TreeNews 桥
```

## 测试

```bash
cd backend
python -m pytest tests/ -q     # 43 个用例，全部离线，不需要 API key
```

## schema 迁移规则

- 表结构变更**只在 `db.py` 里改**：`_CREATE_*` 全量定义 + `_AI_DECISIONS_COLUMNS` /
  `_RAW_NEWS_COLUMNS` 增量列清单，两处同步加。
- 必须幂等：新列通过 `PRAGMA table_info` 存在性检查后 `ALTER TABLE`，禁止 try/except 吞错。
- engine、api_server、`init_db.py` 启动时都会调 `db.migrate()`，无需手工迁移。
- 改完跑 `python -m pytest tests/ -q`，`test_db.py` 覆盖幂等性、列完备性和旧库补齐。
