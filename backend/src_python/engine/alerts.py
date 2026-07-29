"""Feishu (飞书) trading-signal alert push — fire-and-forget."""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from datetime import datetime

import config

from .utils import _now


_feishu_warned = False  # 模块级标志位 — webhook 未配置时只警告一次


async def send_feishu_alert(news_text: str, action: str, score: float, reason: str,
                           news_time: str = "") -> None:
    """
    Push a trading signal card to Feishu bot.

    Color logic: BUY/LONG → green, SELL/SHORT → red, else grey.
    Uses stdlib urllib in an executor thread — no aiohttp needed.
    Failures are silently swallowed (fire-and-forget).
    news_time: ISO timestamp of the news event, displayed prominently on the card.
    """
    FEISHU_WEBHOOK = config.FEISHU_WEBHOOK_URL
    if not FEISHU_WEBHOOK:
        global _feishu_warned
        if not _feishu_warned:
            _feishu_warned = True
            print("[FEISHU] FEISHU_WEBHOOK_URL 未配置, 告警发送已跳过")
        return

    action_upper = action.upper()
    if "BUY" in action_upper or "LONG" in action_upper:
        color = "green"
    elif "SELL" in action_upper or "SHORT" in action_upper:
        color = "red"
    else:
        color = "grey"

    # ── 格式化新闻时间戳 ──
    ts_display = ""
    if news_time:
        try:
            ts_dt = datetime.fromisoformat(news_time.replace("Z", "+00:00"))
            ts_display = ts_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (ValueError, TypeError):
            ts_display = news_time[:19]

    time_line = f"\n🕐 **新闻时间**: {ts_display}" if ts_display else ""

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": "[Trident 交易信号]"},
                "template": color,
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"**新闻原文**: {news_text}{time_line}\n\n**方向**: {action} | **得分**: {score}\n\n**AI 逻辑**: {reason}",
                }
            ],
        },
    }

    def _post_sync() -> None:
        try:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                FEISHU_WEBHOOK,
                data=data,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            resp = urllib.request.urlopen(req, timeout=10)
            body = resp.read().decode("utf-8", errors="replace")
            print(f"[{_now()}] [FEISHU] HTTP {resp.status} — {body[:200]}")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"[{_now()}] [FEISHU] HTTP {e.code} — {body[:200]}")
        except Exception as e:
            print(f"[{_now()}] [FEISHU] ERROR: {type(e).__name__}: {e}")

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _post_sync)
