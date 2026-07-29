# Trident Agent MVP — Linux 部署指南

目标环境：Ubuntu 22.04+，部署路径 `/opt/trident`（可自行调整，systemd unit 里的路径同步改即可）。

三个常驻进程：

| systemd unit | 进程 | 说明 |
|---|---|---|
| `trident-engine.service` | `python engine.py` | 新闻引擎：WS 采集 + AI worker + 前向验证 + webhook:9000 |
| `trident-api.service` | `uvicorn api_server:app` | FastAPI + SSE，127.0.0.1:8000 |
| `trident-bridge.service` | `python treenews_bridge.py` | TreeNews → engine webhook 桥（可选） |

前端 Next.js 生产模式可用 `npm start` 跑在 3000 端口（用 pm2 或自行包一个 unit），由 Nginx 反代对外。

## 1. 系统依赖

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv nodejs npm nginx git
# Node 建议用 nvm 装 18+，apt 源版本可能过旧
```

## 2. 拉取代码 + Python 环境

```bash
sudo mkdir -p /opt/trident
sudo useradd -r -s /usr/sbin/nologin trident   # 专用用户
sudo chown trident:trident /opt/trident
sudo -u trident git clone <仓库地址> /opt/trident

cd /opt/trident
sudo -u trident python3.11 -m venv venv
sudo -u trident venv/bin/pip install -r backend/requirements.txt
# requirements.txt 已包含 websocket-client（treenews_bridge 需要）；
# 若用的是旧版 requirements，请单独补装：venv/bin/pip install websocket-client
```

## 3. 配置环境变量

```bash
sudo -u trident cp .env.example backend/.env
sudo -u trident nano backend/.env
```

必填：`OPENROUTER_API_KEY`（Kimi K3 主模型）。

`FEISHU_WEBHOOK_URL`：飞书群机器人告警地址，从机器人后台复制填入。
不配置则引擎只在启动时警告一次并跳过告警，不影响其他功能。

国内服务器调用 OpenRouter 需要代理，在 `.env` 里设置 `HTTP_PROXY`；
直连可通的海外服务器不要设置该项。

## 4. 初始化数据库

```bash
cd /opt/trident
sudo -u trident venv/bin/python backend/init_db.py
```

幂等，只建表补列，不会删除已有数据。日常启动无需重复执行（engine/api 启动时会自动 migrate）。

## 5. 前端构建

```bash
cd /opt/trident/frontend
sudo -u trident npm ci
sudo -u trident npm run build
# 生产跑在 3000 端口（可用 pm2 守护）：
sudo -u trident npm start
```

生产环境确认 `frontend/.env.local` 中 `NEXT_PUBLIC_API_URL` 为空（走 Nginx 相对路径），
详见 `frontend/.env.example` 注释。

## 6. 安装 systemd units

```bash
sudo cp /opt/trident/deploy/trident-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now trident-engine trident-api trident-bridge
```

## 7. Nginx

```bash
sudo cp /opt/trident/deploy/nginx.conf.example /etc/nginx/sites-available/trident
sudo nano /etc/nginx/sites-available/trident   # 改 server_name
sudo ln -s /etc/nginx/sites-available/trident /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

## 8. 首次启动验证清单

```bash
# 1) 三个服务都在跑
systemctl status trident-engine trident-api trident-bridge

# 2) 引擎日志：应看到 DB VERIFIED、Webhook server listening on 127.0.0.1:9000、
#    [INGEST] Connected
journalctl -u trident-engine -f

# 3) API 健康检查
curl http://127.0.0.1:8000/api/health

# 4) REST 拉取事件列表（有数据时返回 JSON 数组）
curl http://127.0.0.1:8000/api/events

# 5) SSE 流（保持连接，新信号实时推送，Ctrl-C 退出）
curl -N http://127.0.0.1:8000/api/events/stream

# 6) 通过 Nginx 验证全链路
curl -N http://<你的域名>/api/events/stream
```

如果 engine 日志里 `[SNAPSHOT]` 持续报 Binance 超时而服务器在国内，
检查 `.env` 的 `HTTP_PROXY` 是否生效（日志首行会打印代理状态）。
