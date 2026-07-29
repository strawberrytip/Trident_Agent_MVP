# Trident AI 信号 API — 交易系统对接文档

## 1. 接入方式

API 基于 HTTP REST + SSE 实时推送。有两种接入地址：

```
方式一（推荐，走 nginx，有密码保护）:
Base URL:  http://<服务器IP>
所有请求带 HTTP Basic Auth 头（账号密码向管理员索取）
Python 示例: requests.get(url, auth=("用户名", "密码"))

方式二（直连后端，仅限内网/测试）:
Base URL:  http://<服务器IP>:8000
注意：需服务端以 --host 0.0.0.0 启动且安全组放行 8000；无鉴权，勿对公网开放
```

## 2. 交易系统核心接口

### 2.1 获取最新信号（按品种）

```
GET /api/trading/latest
```

返回 5 个品种各自最新一条 AI 信号，适用于交易系统启动时快速获取全局状态。

**响应示例：**

```json
{
  "BTC": {
    "id": 14532,
    "news_time": "2026-07-29T14:22:31",
    "asset": "BTC",
    "action": "BUY",
    "score": 0.52,
    "reasoning": "降息预期叠加ETF资金持续流入,短期看涨至68000",
    "reasoning_path": "[驱动力]美联储鸽派信号→美元走弱→BTC受益...",
    "market_category": "CRYPTO",
    "event_strength": "medium",
    "direct_catalyst": true,
    "prediction_type": "continuation",
    "market_confirmation": "positive",
    "entry_price": 67220.0,
    "created_at": "2026-07-29T14:22:35"
  },
  "ETH": { ... },
  "XAU": { ... },
  "WTI": { ... },
  "SOL": null
}
```

`null` 表示该品种暂无信号。

### 2.2 按条件查询信号历史

```
GET /api/trading/signals?asset=BTC&action=BUY&limit=20&settled=-1
```

| 参数 | 说明 | 可选值 |
|------|------|--------|
| `asset` | 品种过滤 | BTC / ETH / XAU / WTI / SOL（留空=全部） |
| `action` | 方向过滤 | BUY / SELL / HOLD（留空=全部） |
| `limit` | 返回条数 | 1~200，默认 20 |
| `settled` | 结算状态 | 0=未结算 / 1=已结算 / -1=全部 |

**响应字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 信号唯一 ID |
| `news_time` | str | 新闻发布时间（ISO 8601） |
| `asset` | str | 交易品种 |
| `action` | str | BUY / SELL / HOLD |
| `score` | float | 情感得分 -1.0 ~ +1.0（见下方评分标准） |
| `reasoning` | str | AI 一句结论（≤50 字） |
| `reasoning_path` | str | 完整四段推导链：驱动力→水位博弈→跨资产联动→反共识 |
| `market_category` | str | CRYPTO / GOLD / OIL / MACRO / OTHER |
| `event_strength` | str | 事件强度 low / medium / high |
| `direct_catalyst` | int | 是否直接催化该资产（1=是 / 0=否，非 JSON true/false） |
| `prediction_type` | str | reversal / continuation / breakout |
| `market_confirmation` | str | 市场是否已在按新闻方向走 |
| `entry_price` | float | 信号触发时的入场价（现货） |
| `exit_price` | float | 2h 结算出场价（settled=1 时有值） |
| `is_correct` | str | WIN / LOSS（settled=1 时有效） |
| `settled` | int | 0=未结算 / 1=已结算 |
| `created_at` | str | AI 分析时间 |

## 3. Score 评分标准（交易系统阈值参考）

`score` 反映 **2 小时内事件推动价格方向的置信度与幅度预期**：

| 范围 | 等级 | 含义 | 建议动作 |
|------|------|------|----------|
| ±0.05~0.15 | 弱信号 | 情绪扰动，无实质资金流 | 过滤掉，不交易 |
| ±0.15~0.35 | 轻度 | 有资金含义但间接 | 小仓位或观望 |
| ±0.35~0.55 | 中度 | 直接推动供需或风险偏好 | 标准仓位 |
| ±0.55~0.75 | 强信号 | 直接催化+趋势共振 | 重仓，设止损 |
| ±0.75~0.95 | 极端 | 结构性突变/黑天鹅 | 全仓，严格风控 |

**叠加规则（模型内部自动计算，你只需用最终 score）：**
- `direct_catalyst=true` → +1 档
- `event_strength=low` → 上限 |score| ≤ 0.35
- `market_confirmation=negative` → −1 档
- 趋势同向 +0.05~0.10，反向 −0.05~0.10

### 建议交易阈值

```python
# 根据你的风险偏好调整
SIGNAL_THRESHOLD = 0.35   # |score| >= 此值才开仓
STRONG_THRESHOLD = 0.55   # |score| >= 此值用更大仓位
FILTER_DIRECT_ONLY = True  # 只交易 direct_catalyst=true 的信号
```

## 4. 完整信号列表接口

```
GET /api/events
```

返回最近 50 条信号，字段包含完整的 extra_models_consensus、MFE/MAE 复盘指标等。字段较多，适合展示面板而非自动交易。

## 5. 实时推送（SSE）

```
GET /api/events/stream
```

Server-Sent Events，每当引擎产出新信号时自动推送。适合需要实时响应的交易策略。

**Event 格式：**
```
data: {"id":14533,"action":"SELL","asset":"XAU","score":-0.48,...}
```

**客户端示例（Python）：**

```python
import requests
import json

url = "http://<服务器IP>/api/events/stream"  # 走 nginx 时加 auth=("用户名","密码")
with requests.get(url, stream=True) as r:
    for line in r.iter_lines(decode_unicode=True):
        if line.startswith("data:"):
            signal = json.loads(line[5:])
            if signal.get("type") != "price_update":
                process_signal(signal)  # 你的交易逻辑
```

## 6. 健康检查

```
GET /api/health
```

```json
{"status":"ok","db_path":"...","db_exists":true,"sse_clients":0}
```

## 7. 典型接入流程

```
1. 启动时: GET /api/trading/latest  → 获取当前全局信号状态
2. 主循环: GET /api/trading/signals?asset=BTC&settled=0&limit=1  → 轮询最新 BTC 未结算信号
   或
   SSE 长连接: GET /api/events/stream → 实时接收新信号
3. 决策:
   if |score| >= THRESHOLD and direct_catalyst:
       开仓(action, asset, score)
4. 复盘: GET /api/trading/signals?asset=BTC&settled=1&limit=50  → 查看已结算信号胜率
```

## 8. 注意事项

- `score` 是相对评分而非绝对概率，不同市场环境下同一个 score 对应的实际胜率可能不同。建议回测后再设阈值
- `entry_price` 只在 BUY/SELL 信号时有值，HOLD 信号为 null
- SSE 断线重连后从**最新信号**开始推送，断线期间的信号**不会**自动补发。交易系统重连后必须先调用 `GET /api/trading/signals?limit=50` 回填缺口，再恢复实时监听
- API 无鉴权，请部署在内网或加 nginx basic auth
