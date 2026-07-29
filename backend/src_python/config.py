#!/usr/bin/env python3
"""
Trident Agent MVP — Central Configuration
==========================================

Single entry point for environment loading, API credentials, DB path and
tuning thresholds. Both engine.py and api_server.py import this module
(plain `import config` — backend/src_python is on sys.path at runtime).

.env is loaded exactly once, here, from backend/.env (parent of src_python/).
"""

from __future__ import annotations

import os
from datetime import timezone, timedelta
from typing import Dict

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths & .env
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # backend/

load_dotenv(os.path.join(BASE_DIR, ".env"))

DB_PATH = os.getenv("TRIDENT_DB_PATH") or os.path.join(BASE_DIR, "trident_event_bus.db")
TZ_SHANGHAI = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# LLM API credentials
# ---------------------------------------------------------------------------

# DeepSeek API (OpenAI-compatible endpoint)
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

# OpenRouter API — multi-model comparison gateway (Claude, Gemini, GPT-4, Grok, DeepSeek)
# Used for Kimi K3 (primary) + four additional models (DeepSeek, Gemini, Grok, ChatGPT)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# xAI (Grok) API — OpenAI-compatible direct endpoint
XAI_API_KEY = os.getenv("XAI_API_KEY", "")
XAI_BASE_URL = "https://api.x.ai/v1"

# Doubao (火山引擎) API — OpenAI-compatible endpoint
# NOTE: DOUBAO_MODEL must be an Endpoint ID (ep-xxxxx), NOT a model name string.
# Create an Inference Endpoint at https://console.volces.com/ark before setting this.
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY", "")
DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
DOUBAO_MODEL = os.getenv("DOUBAO_MODEL", "")

# ---------------------------------------------------------------------------
# Integrations
# ---------------------------------------------------------------------------

# 飞书告警机器人 webhook — 为空时 engine 只打印一次警告并跳过发送
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")

# CORS 白名单 — 逗号分隔，默认本地 Next.js 开发端口
_cors_raw = os.getenv("CORS_ALLOW_ORIGINS", "")
CORS_ALLOW_ORIGINS = [o.strip() for o in _cors_raw.split(",") if o.strip()] or [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# ---------------------------------------------------------------------------
# Tuning thresholds (数值与 engine.py 原定义一致，勿改)
# ---------------------------------------------------------------------------

# VIP KOL monitoring
VIP_KOLS: Dict[str, str] = {
    "Trump":   "[VIP:TRUMP]", "特朗普": "[VIP:TRUMP]",
    "Musk":    "[VIP:MUSK]",  "马斯克": "[VIP:MUSK]", "Elon": "[VIP:MUSK]",
    "Powell":  "[VIP:FED]",   "鲍威尔": "[VIP:FED]", "FOMC": "[VIP:FED]", "美联储": "[VIP:FED]",
    "Vance":   "[VIP:OTHER]", "万斯": "[VIP:OTHER]", "Bessent": "[VIP:OTHER]",
}
VIP_SCORE_BOOST = 1.25

# Active-trade aggregation
_AGG_WINDOW_HOURS = 1          # how long a parent stays "active" (tight window to avoid over-clustering)
_AGG_MIN_SCORE    = 0.25       # only aggregate signals with |score| >= this

# AI worker batch size
BATCH_SIZE = 10

# Asset-specific impact thresholds for forward-tracker verdict ruling
IMPACT_THRESHOLD = {"BTC": 2.0, "ETH": 2.0, "SOL": 2.0,
                    "XAU": 1.0, "GOLD": 1.0,
                    "WTI": 1.5}
