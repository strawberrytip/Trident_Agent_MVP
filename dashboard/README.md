# Trident 量化复盘看板

专业的离线数据分析看板，用于客户回测展示和交易复盘。

## 功能特性

- 🎨 **暗色主题**：契合专业量化终端的视觉风格
- 📊 **K线图表**：使用 Plotly 绘制交互式1分钟级K线图
- 📍 **事件标注**：自动标注新闻发布时刻和入场价格
- 🤖 **AI归因展示**：优雅展示 Kimi K3 主模型推理及 4 个子模型（DeepSeek、Gemini、Grok、ChatGPT）的共识结果
- 📁 **Excel支持**：直接上传 `trident_signals.xlsx` 分析
- 🛡️ **容错机制**：数据获取失败时仍可查看AI文本

## 快速开始

### 1. 安装依赖

```bash
cd dashboard
pip install -r requirements.txt
```

### 2. 准备数据文件

确保您有 `trident_signals.xlsx` 文件，应包含以下列：
- 时间
- 新闻内容
- 品种 (XAU/BTC/WTI等)
- 方向 (多/空)
- 入场价
- 最大浮盈%
- 最大浮亏%
- 评分
- Kimi K3归因
- extra_models_consensus

### 3. 启动应用

```bash
streamlit run app.py --server.port 8501 --server.address 127.0.0.1
```

应用将在 `http://127.0.0.1:8501` 启动。

## 使用说明

1. **上传文件**：在侧边栏上传信号Excel文件
2. **选择日期**：选择交易发生的日期
3. **选择信号**：从下拉列表中选择要分析的信号
4. **查看图表**：
   - 左侧：K线图（带新闻发布虚线、入场价实线）
   - 右侧：AI推导逻辑展示
   - 顶部：核心指标（MFE、MAE、评分）

## 品种映射

系统自动将品种映射到币安 U 本位永续合约（`fapi.binance.com/fapi/v1/klines`）：

| Trident代码 | 币安合约代码 | 中文名称 |
|------------|-------------|---------|
| XAU/GOLD   | XAUUSDT     | 黄金    |
| BTC        | BTCUSDT     | 比特币  |
| ETH        | ETHUSDT     | 以太坊  |
| SOL        | SOLUSDT     | Solana  |
| WTI/OIL    | XAUUSDT     | 原油（币安无原油合约，回退黄金） |
| 其他       | `{代码}USDT` | 自动拼接 |

## 注意事项

⚠️ **币安 K 线数据限制**：
- 1分钟级K线单次最多返回1000根，代码已循环拉取
- 仅能查询该合约在币安上市后的历史数据

💡 **图表交互**：
- 鼠标悬停查看详细价格
- 拖拽/缩放查看不同时段
- 双击重置视图

## 技术架构

- **前端框架**: Streamlit
- **图表库**: Plotly Graph Objects
- **数据源**: 币安合约公开 API (fapi.binance.com)
- **数据处理**: pandas
