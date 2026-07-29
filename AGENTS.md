# AGENTS.md — AI 协作者项目约定

## 目录速览

- `backend/src_python/engine/` — 新闻引擎包（ingest / ai_worker / forward / webhook / alerts / prices / ws_client / utils / main），`engine.py` 是薄入口
- `backend/src_python/api_server.py` — FastAPI + SSE 服务（127.0.0.1:8000）
- `backend/src_python/config.py` — 唯一配置入口（.env 加载、密钥、阈值常量）
- `backend/src_python/db.py` — schema 迁移唯一权威 + `assert_readonly_sql`
- `backend/tests/` — pytest 冒烟套件（全离线）
- `frontend/` — Next.js 14 前端（:3000）
- `dashboard/` — Streamlit 离线复盘看板（:8501）
- `deploy/` — systemd + nginx 部署配置
- 更详细的模块说明见 `backend/README.md`，部署见 `deploy/README.md`

## 常用命令

```bash
# 启动引擎（在 backend/src_python 下）
python engine.py
# 启动 API（在 backend/src_python 下）
uvicorn api_server:app --host 127.0.0.1 --port 8000
# 测试（在 backend 下，43 用例，全离线）
python -m pytest tests/ -q
# 前端
cd frontend && npm run dev        # 开发
cd frontend && npm run build      # 生产构建
# 语法快速检查
python -m py_compile <file>
```

Windows 一键开发启动：`scripts\dev_start.bat`。

## 铁律

1. **schema 迁移只改 `backend/src_python/db.py`**。engine/api_server/init_db 都调用 `db.migrate()`；禁止在别处写 CREATE/ALTER。迁移必须幂等（列存在性检查，不用 try/except 吞错）。
2. **密钥只进 `.env` 不进代码**。新配置项加进 `config.py` + `.env.example`。任何 URL/Key 硬编码都是事故。
3. **不动生产 DB 文件** `backend/trident_event_bus.db`（及其 .bak）。测试一律用 tmp_path/内存库；conftest.py 的 `temp_db` fixture 就是干这个的。
4. **engine 业务逻辑改动需同步 `backend/tests/`**。改过滤规则/prompt/阈值/聚合逻辑时，先确认现有 43 个用例仍绿，并为新行为补用例（保持离线可跑，网络/LLM 一律 mock 或不测）。
5. **frontend 不准开 `ignoreBuildErrors`**（next.config 里的 typescript/eslint 忽略项）。类型错误就是错误，构建必须过。
6. **不改业务逻辑语义**：过滤关键词表、LLM prompt、阈值数值（VIP_SCORE_BOOST、_AGG_*、IMPACT_THRESHOLD、BATCH_SIZE）的修改属于产品决策，不夹带在重构提交里。
7. 提交前跑 `python -m pytest tests/ -q` 和涉及文件的 `py_compile`。
