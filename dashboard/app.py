#!/usr/bin/env python3
"""
Trident Agent MVP — 离线可视化复盘面板
================================================

Streamlit + Plotly 专业的量化回测看板，用于客户演示和交易复盘。

功能：
  - 读取 trident_signals.xlsx 信号文件
  - 自动拉取币安 XAUUSDT 1分钟级K线数据
  - 标注新闻发布时刻、入场价、最大浮盈/浮亏
  - 展示 Kimi K3 主模型及 4 个子模型 (DeepSeek/Gemini/Grok/ChatGPT) 的AI推导逻辑

运行：
  cd dashboard
  streamlit run app.py --server.port 8501 --server.address 127.0.0.1
"""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timedelta, timezone, time as dtime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# Page Config — Dark Theme + Wide Mode
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Trident 量化复盘看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Custom CSS — Dark Theme Professional Styling
# -----------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* Dark theme base */
    :root {
        --bg-primary: #0e1117;
        --bg-secondary: #151a23;
        --text-primary: #e8eaf0;
        --text-secondary: #9fa6b2;
        --accent-green: #00d26a;
        --accent-red: #f87171;
        --accent-blue: #3b82f6;
        --accent-gold: #fbbf24;
        --border-color: #2d3342;
    }

    /* Main container */
    .main {
        background-color: var(--bg-primary);
        color: var(--text-primary);
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: var(--bg-secondary);
        border-right: 1px solid var(--border-color);
    }

    /* Headers */
    h1, h2, h3 {
        color: var(--text-primary) !important;
        font-weight: 600;
    }

    h1 {
        font-size: 2rem;
        margin-bottom: 1rem;
    }

    /* Cards */
    .signal-card {
        background-color: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }

    /* Metric values */
    [data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 600;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: var(--bg-secondary);
        color: var(--text-primary);
    }

    /* Date input */
    [data-testid="stDateInput"] {
        background-color: var(--bg-secondary);
    }

    /* File upload */
    [data-testid="stFileUploadUploader"] {
        background-color: var(--bg-secondary);
    }

    /* AI reasoning boxes */
    .ai-reasoning {
        background-color: #1a1f2e;
        border-left: 4px solid var(--accent-blue);
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        line-height: 1.6;
    }

    .kimi-reasoning {
        border-left-color: #8b5cf6; /* Purple for Kimi K3 */
    }

    /* Sub-model compact cards (DeepSeek / Gemini / Grok / ChatGPT) */
    .sub-model-card {
        background-color: #1a1f2e;
        border-left: 3px solid #3b82f6;
        padding: 0.6rem 1rem;
        border-radius: 6px;
        margin: 0.4rem 0;
        font-size: 0.85rem;
        line-height: 1.6;
        color: #e0e0e0 !important;
    }

    .doubao-reasoning   { border-left-color: #f97316; }
    .deepseek-reasoning { border-left-color: #14b8a6; }
    .gemini-reasoning   { border-left-color: #60a5fa; }
    .grok-reasoning     { border-left-color: #facc15; }
    .chatgpt-reasoning  { border-left-color: #34d399; }

    .sub-model-card * {
        color: #e0e0e0 !important;
        -webkit-text-fill-color: #e0e0e0 !important;
    }

    /* Direction badges */
    .badge-long {
        background-color: rgba(0, 210, 106, 0.2);
        color: var(--accent-green);
        padding: 0.25rem 0.75rem;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.875rem;
    }

    .badge-short {
        background-color: rgba(248, 113, 113, 0.2);
        color: var(--accent-red);
        padding: 0.25rem 0.75rem;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.875rem;
    }

    /* News title highlight */
    .news-title {
        color: var(--accent-gold);
        font-size: 0.95rem;
        line-height: 1.5;
    }

    /* Asset tag */
    .asset-tag {
        background-color: rgba(59, 130, 246, 0.2);
        color: var(--accent-blue);
        padding: 0.25rem 0.75rem;
        border-radius: 6px;
        font-size: 0.875rem;
        font-weight: 600;
    }

    /* Plotly chart dark theme */
    .js-plotly-plot {
        background-color: var(--bg-secondary);
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    ::-webkit-scrollbar-track {
        background: var(--bg-secondary);
    }

    ::-webkit-scrollbar-thumb {
        background: var(--border-color);
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: #3d4456;
    }

    /* Warning box */
    .warning-box {
        background-color: rgba(251, 191, 36, 0.1);
        border: 1px solid #fbbf24;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }

    /* ═══════════════════════════════════════════════════════════
       BugFix: 终极样式覆盖
       ═══════════════════════════════════════════════════════════ */

    /* 1. 侧边栏下拉框 — 强制白色文字 */
    section[data-testid="stSidebar"] div[data-baseweb="select"] * {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }

    /* 下拉框选中的值 (输入框内部) */
    section[data-testid="stSidebar"] div[data-baseweb="select"] input {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        caret-color: #ffffff !important;
    }

    /* 2. 下拉菜单弹出选项 */
    ul[data-baseweb="menu"] li {
        color: #ffffff !important;
        background-color: #1e2130 !important;
    }

    ul[data-baseweb="menu"] li:hover,
    ul[data-baseweb="menu"] li[aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: #ffffff !important;
    }

    /* 3. AI 推导逻辑框 — 强制亮色文字 */
    .ai-reasoning,
    .ai-reasoning *,
    div[data-testid="stMarkdownContainer"] .ai-reasoning * {
        color: #e8eaf0 !important;
        -webkit-text-fill-color: #e8eaf0 !important;
    }

    .kimi-reasoning,
    .kimi-reasoning * {
        color: #e8eaf0 !important;
        -webkit-text-fill-color: #e8eaf0 !important;
    }

    /* 确保 AI 框中任何深色 inline style 都被覆盖 */
    div[data-testid="stMarkdownContainer"] div[style*="background"] * {
        color: #e8eaf0 !important;
        -webkit-text-fill-color: #e8eaf0 !important;
    }

    /* ═══════════════════════════════════════════════════════════
       BugFix: 侧边栏全局亮色覆盖
       ═══════════════════════════════════════════════════════════ */

    /* 1. 侧边栏标签、段落、标题、指标数值 → 亮色 */
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] div[data-testid="stMetricValue"] {
        color: #e0e0e0 !important;
    }

    /* 2. 侧边栏下拉框 — 选中值 & 内部元素 */
    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] *,
    section[data-testid="stSidebar"] div[data-testid="stSelectbox"] span {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }

    /* 3. 侧边栏提示框 (info/success/error) 文字 */
    section[data-testid="stSidebar"] div[data-testid="stAlert"] * {
        color: #ffffff !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Asset Mapping — Trident symbol → Binance kline symbol
# -----------------------------------------------------------------------------

ASSET_NAMES: Dict[str, str] = {
    "XAU": "黄金",    "GOLD": "黄金",
    "BTC": "比特币",  "ETH": "以太坊",
    "SOL": "Solana",  "BNB": "BNB",
    "DOGE": "狗狗币", "LTC": "莱特币",
    "LINK": "Chainlink",
    "WTI": "原油",    "OIL": "原油",
}

# -----------------------------------------------------------------------------
# Data Loading Functions
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_signal_data(uploaded_file) -> pd.DataFrame:
    """加载并预处理信号Excel数据。"""
    try:
        df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"读取Excel文件失败: {e}")
        return pd.DataFrame()

    # 标准化列名（处理可能的列名变化）
    column_mapping = {
        "时间": "时间",
        "新闻内容": "新闻内容",
        "品种": "品种",
        "方向": "方向",
        "入场价": "入场价",
        "最高价": "最高价",
        "最低价": "最低价",
        "出场价": "出场价",
        "最大浮盈%": "最大浮盈%",
        "最大浮亏%": "最大浮亏%",
        "评分": "评分",
        "Kimi K3归因": "Kimi K3归因",
        "DeepSeek归因": "DeepSeek归因",
        "Gemini归因": "Gemini归因",
        "Grok归因": "Grok归因",
        "ChatGPT归因": "ChatGPT归因",
        "胜负": "胜负",
        "强影响": "强影响",
    }

    # 检查必需的列是否存在
    required_cols = ["时间", "新闻内容", "品种", "方向", "入场价"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        st.error(f"Excel缺少必需的列: {missing_cols}")
        st.write(f"当前列名: {list(df.columns)}")
        return pd.DataFrame()

    # 清洗数据
    df = df.copy()
    df = df.dropna(subset=["时间", "品种"])

    # 标准化品种代码 — 去掉括号、中文、空格，只保留字母/数字
    def clean_asset(val: str) -> str:
        if pd.isna(val):
            return "UNKNOWN"
        val = str(val).upper().strip()
        # 去掉方括号和中文字符（如 "[BTC-多]" → "BTC"）
        val = re.sub(r'[\[\]（）【】\-_\s]', '', val)
        val = re.sub(r'[一-鿿]+', '', val)
        # 常见别名归一
        alias = {"GOLD": "XAU", "OIL": "WTI", "BITCOIN": "BTC",
                 "ETHEREUM": "ETH", "SOLANA": "SOL", "BNB": "BNB",
                 "DOGE": "DOGE", "LTC": "LTC", "LINK": "LINK"}
        return alias.get(val, val)
    df["品种"] = df["品种"].apply(clean_asset)

    # 标准化方向
    df["方向"] = df["方向"].str.upper().str.strip()
    df = df[df["方向"].isin(["BUY", "SELL", "多", "空", "LONG", "SHORT"])]

    # 提取时间用于显示和K线查询
    df["时间_原始"] = df["时间"].astype(str)  # 保留原始字符串用于显示
    def extract_time(t):
        if pd.isna(t):
            return None
        if isinstance(t, str):
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%H:%M:%S", "%H:%M"]:
                try:
                    dt = datetime.strptime(t.strip(), fmt)
                    return dt.time()  # 只返回时间部分，日期由用户选择器决定
                except ValueError:
                    continue
        return t

    df["时间_提取"] = df["时间"].apply(extract_time)

    # 添加缺失的可选列
    MODEL_COLUMNS = ["Kimi K3归因", "DeepSeek归因", "Gemini归因", "Grok归因", "ChatGPT归因"]
    for col in MODEL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    if "最大浮盈%" not in df.columns:
        df["最大浮盈%"] = None
    if "最大浮亏%" not in df.columns:
        df["最大浮亏%"] = None
    if "评分" not in df.columns:
        df["评分"] = 0.0

    return df


def create_signal_label(row: pd.Series) -> str:
    """创建信号标签，用于Selectbox显示。"""
    asset = row.get("品种", "UNKNOWN")
    direction = row.get("方向", "")
    time_val = str(row.get("时间", ""))
    # 如果有完整日期就显示完整，否则只截 HH:MM:SS
    time_str = time_val[:19] if len(time_val) >= 10 else time_val[:8]
    news = str(row.get("新闻内容", ""))[:40] + "..." if len(str(row.get("新闻内容", ""))) > 40 else str(row.get("新闻内容", ""))

    # 标准化方向显示
    if direction in ["BUY", "多", "LONG"]:
        dir_symbol = "多"
    elif direction in ["SELL", "空", "SHORT"]:
        dir_symbol = "空"
    else:
        dir_symbol = direction

    return f"[{asset}-{dir_symbol}] {time_str} {news}"


# -----------------------------------------------------------------------------
# K线数据获取
# -----------------------------------------------------------------------------

# 币安 K line 品种映射：内部代码 → USDT 永续合约代码
BINANCE_KLINE_SYMBOLS: Dict[str, str] = {
    "XAU": "XAUUSDT",
    "GOLD": "XAUUSDT",
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "DOGE": "DOGEUSDT",
    "LTC": "LTCUSDT",
    "LINK": "LINKUSDT",
    "WTI": "XAUUSDT",    # 原油没有币安合约，回退黄金
    "OIL": "XAUUSDT",
}

PROXY_URL = os.getenv("HTTP_PROXY", "").strip() or os.getenv("http_proxy", "").strip()
BINANCE_KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"


def _resolve_binance_symbol(asset_code: str) -> str:
    """
    将内部品种代码解析为币安 U 本位合约符号。

    优先级:
        1. 查 BINANCE_KLINE_SYMBOLS 精确匹配
        2. asset_code + "USDT" 拼接 (如 "1000PEPE" → "1000PEPEUSDT")
        3. 仍找不到则原样返回

    >>> _resolve_binance_symbol("BTC")
    'BTCUSDT'
    >>> _resolve_binance_symbol("1000PEPE")
    '1000PEPEUSDT'
    """
    code = asset_code.upper().strip()
    if code in BINANCE_KLINE_SYMBOLS:
        return BINANCE_KLINE_SYMBOLS[code]
    # 回退：直接加 USDT 后缀
    return f"{code}USDT"


@st.cache_data(ttl=3600)  # 缓存1小时，避免频繁请求被限流
def fetch_kline_data(
    asset: str,
    event_datetime: datetime,
    before_minutes: int = 30,
    after_minutes: int = 120,
) -> Optional[pd.DataFrame]:
    """
    从币安 U 本位合约 REST API 获取1分钟级K线数据。

    参数:
        asset: 品种代码 (如 XAU, BTC, ETH — 来自 Excel "品种" 列)
        event_datetime: 事件发生的完整日期时间
        before_minutes: 事件前多少分钟 (默认120)
        after_minutes: 事件后多少分钟 (默认120)

    返回:
        包含OHLCV数据的DataFrame，带上海时区 DatetimeIndex
    """
    binance_symbol = _resolve_binance_symbol(asset)

    # ── 毫秒时间戳 ──
    start_time = event_datetime - timedelta(minutes=before_minutes)
    end_time = event_datetime + timedelta(minutes=after_minutes)

    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)

    try:
        resp = requests.get(
            BINANCE_KLINE_URL,
            params={
                "symbol": binance_symbol,
                "interval": "1m",
                "startTime": start_ts,
                "endTime": end_ts,
                "limit": 1000,
            },
            proxies={"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None,
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()

        if not raw or not isinstance(raw, list) or len(raw) == 0:
            st.warning(f"币安未返回 {binance_symbol} 的K线数据（时间段内无成交或品种不存在）")
            return None

        # ── 解析为 DataFrame ──
        cols = ["open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_vol", "trades", "taker_buy_vol",
                "taker_buy_quote_vol", "ignore"]
        df = pd.DataFrame(raw, columns=cols)

        # 提取前6列：时间戳 → open/high/low/close/volume
        df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()
        df["open"] = pd.to_numeric(df["open"])
        df["high"] = pd.to_numeric(df["high"])
        df["low"] = pd.to_numeric(df["low"])
        df["close"] = pd.to_numeric(df["close"])
        df["volume"] = pd.to_numeric(df["volume"])

        # 时间戳 (ms) → datetime → 上海时区索引
        df["datetime"] = pd.to_datetime(df["open_time"], unit="ms")
        df["datetime"] = df["datetime"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
        df = df.set_index("datetime")
        df = df.drop(columns=["open_time"])

        # 重命名列以匹配 chart 函数的预期
        df = df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })

        return df

    except requests.exceptions.RequestException as e:
        st.warning(f"币安API请求失败: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        st.warning(f"获取K线数据失败: {type(e).__name__}: {e}")
        return None


# -----------------------------------------------------------------------------
# Plotly K线图表绘制
# -----------------------------------------------------------------------------

def create_candlestick_chart(
    kline_df: pd.DataFrame,
    event_time: datetime,
    entry_price: float,
    direction: str,
    symbol_label: str = "",
) -> go.Figure:
    """
    创建专业的K线图表，带事件标注线。

    参数:
        kline_df: K线数据 (带Datetime索引)
        event_time: 事件发生时间
        entry_price: 入场价格
        direction: 交易方向 (BUY/SELL)
        symbol_label: 币安合约代码 (如 XAUUSDT, BTCUSDT)
    """
    if kline_df is None or kline_df.empty:
        # 创建空图表
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            title="无K线数据",
            height=500,
        )
        return fig

    # 确定颜色主题
    if direction in ["BUY", "多", "LONG"]:
        entry_color = "#00d26a"  # Green for long
        event_color = "#fbbf24"  # Gold for news
    else:
        entry_color = "#f87171"  # Red for short
        event_color = "#fbbf24"

    fig = go.Figure()

    # K线图
    fig.add_trace(
        go.Candlestick(
            x=kline_df.index,
            open=kline_df["Open"],
            high=kline_df["High"],
            low=kline_df["Low"],
            close=kline_df["Close"],
            name="K线",
            increasing_line_color="#00d26a",
            decreasing_line_color="#f87171",
        )
    )

    # 成交量（底部柱状图）
    fig.add_trace(
        go.Bar(
            x=kline_df.index,
            y=kline_df["Volume"],
            name="成交量",
            yaxis="y2",
            marker_color="rgba(148, 163, 184, 0.3)",
        )
    )

    # 新闻发布垂直虚线
    fig.add_vline(
        x=event_time,
        line_dash="dash",
        line_width=2,
        line_color=event_color,
        annotation_text="📰 新闻发布",
        annotation_position="top",
        annotation_font_size=12,
        annotation_font_color=event_color,
    )

    # 入场价水平实线
    fig.add_hline(
        y=entry_price,
        line_width=2,
        line_color=entry_color,
        annotation_text=f"💰 入场: {entry_price:.2f}",
        annotation_position="right",
        annotation_font_size=11,
        annotation_font_color=entry_color,
        annotation_bgcolor="rgba(0,0,0,0.7)",
    )

    # 布局设置
    fig.update_layout(
        template="plotly_dark",
        height=550,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis_rangeslider_visible=False,
        xaxis=dict(
            title="时间（上海时区）",
            gridcolor="#2d3342",
            showgrid=True,
        ),
        yaxis=dict(
            title=f"价格 ({symbol_label})" if symbol_label else "价格",
            gridcolor="#2d3342",
            showgrid=True,
            side="left",
        ),
        yaxis2=dict(
            title="成交量",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    return fig


# -----------------------------------------------------------------------------
# Sidebar UI Components
# -----------------------------------------------------------------------------

def render_sidebar(df: pd.DataFrame) -> Tuple[pd.Series, datetime]:
    """渲染侧边栏，返回选中的信号行和完整的日期时间。"""

    st.sidebar.header("⚙️ 数据控制")

    # 文件上传
    uploaded_file = st.sidebar.file_uploader(
        "📁 上传信号文件 (trident_signals.xlsx)",
        type=["xlsx", "xls"],
        help="请上传包含交易信号的Excel文件",
    )

    if uploaded_file is None:
        # 显示示例说明
        st.sidebar.info(
            """
            👋 欢迎使用 Trident 复盘看板！

            请上传 `trident_signals.xlsx` 文件开始分析。

            文件应包含以下列：
            - 时间
            - 新闻内容
            - 品种 (XAU/BTC/WTI)
            - 方向 (多/空)
            - 入场价
            - 最大浮盈%
            - 最大浮亏%
            - Kimi K3归因 (主模型)
            - DeepSeek归因 / Gemini归因 / Grok归因 / ChatGPT归因
            """
        )
        return None, None

    # 加载数据
    df = load_signal_data(uploaded_file)
    if df.empty:
        st.sidebar.error("无法加载信号数据")
        return None, None

    st.sidebar.success(f"✅ 已加载 {len(df)} 条信号")

    # 日期选择器
    st.sidebar.subheader("📅 选择交易日期")
    today = datetime.now().date()
    selected_date = st.sidebar.date_input(
        "选择日期",
        value=today,
        max_value=today,
        help="信号文件中的时间将与此日期组合",
    )

    # 创建信号标签列表
    df["信号标签"] = df.apply(create_signal_label, axis=1)

    st.sidebar.subheader("📊 选择交易信号")
    selected_label = st.sidebar.selectbox(
        "点击信号查看详情",
        options=df["信号标签"].tolist(),
        help="格式: [品种-方向] 时间 新闻内容...",
    )

    # 找到选中的行
    selected_row = df[df["信号标签"] == selected_label].iloc[0]

    # K线查询用用户选的日期 + Excel里的时分秒
    time_obj = selected_row.get("时间_提取")
    if time_obj is None:
        st.sidebar.error(f"无法解析时间: {selected_row.get('时间')}")
        return None, None

    event_datetime = datetime.combine(selected_date, time_obj)
    # 显示用的完整原始时间戳
    display_ts = str(selected_row.get("时间_原始", selected_row.get("时间", "")))[:19]

    # 显示信号概要
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 信号概要")
    st.sidebar.caption(f"🕐 {display_ts}")
    st.sidebar.metric("品种", f"{selected_row['品种']} ({ASSET_NAMES.get(selected_row['品种'], selected_row['品种'])})")
    st.sidebar.metric("方向", "做多" if selected_row["方向"] in ["BUY", "多", "LONG"] else "做空")
    if pd.notna(selected_row.get("入场价")):
        st.sidebar.metric("入场价", f"{selected_row['入场价']:.2f}")

    return selected_row, event_datetime


# -----------------------------------------------------------------------------
# Main View Components
# -----------------------------------------------------------------------------

def render_signal_header(row: pd.Series):
    """渲染信号头部信息。"""
    asset = row.get("品种", "UNKNOWN")
    direction = row.get("方向", "")
    news = row.get("新闻内容", "")

    asset_name = ASSET_NAMES.get(asset, asset)

    # 标准化方向
    if direction in ["BUY", "多", "LONG"]:
        direction_display = "做多"
        badge_class = "badge-long"
    else:
        direction_display = "做空"
        badge_class = "badge-short"

    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
            <span class="asset-tag">{asset_name} ({asset})</span>
            <span class="{badge_class}">{direction_display}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ts_str = row.get("时间", "")[:19]
    ts_caption = f" 🕐 {ts_str}" if ts_str else ""
    st.markdown(f"<div class='news-title'>📰 {news}{ts_caption}</div>", unsafe_allow_html=True)


def render_metrics(row: pd.Series):
    """渲染核心指标。"""
    mfe = row.get("最大浮盈%")
    mae = row.get("最大浮亏%")
    score = row.get("评分", 0)

    # 解析百分比字符串（可能是 "2.5%" 格式）
    def parse_pct(val):
        if pd.isna(val):
            return None
        if isinstance(val, str):
            val = val.rstrip("%")
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    mfe_val = parse_pct(mfe)
    mae_val = parse_pct(mae)
    score_val = float(score) if score is not None else 0.0

    col1, col2, col3 = st.columns(3)

    with col1:
        mfe_color = "normal" if mfe_val is None else ("inverse" if mfe_val < 0 else "normal")
        st.metric(
            "最大浮盈 (MFE)",
            f"{mfe_val:+.2f}%" if mfe_val is not None else "—",
            delta_color=mfe_color,
            help="Maximum Favorable Excursion - 最大有利浮动",
        )

    with col2:
        mae_color = "normal" if mae_val is None else ("inverse" if mae_val > 0 else "normal")
        st.metric(
            "最大浮亏 (MAE)",
            f"{mae_val:+.2f}%" if mae_val is not None else "—",
            delta_color=mae_color,
            help="Maximum Adverse Excursion - 最大不利浮动",
        )

    with col3:
        st.metric(
            "共识评分",
            f"{score_val:+.2f}",
            help="AI模型对信号的强度评分（-1.0 到 +1.0）",
        )


# ── 子模型展示配置 ──
_MODEL_CONFIG = {
    "DeepSeek": {"emoji": "🔍", "css_class": "deepseek-reasoning", "label": "DeepSeek"},
    "Gemini":   {"emoji": "💎", "css_class": "gemini-reasoning",   "label": "Gemini"},
    "Grok":     {"emoji": "⚡", "css_class": "grok-reasoning",     "label": "Grok"},
    "ChatGPT":  {"emoji": "🤖", "css_class": "chatgpt-reasoning",  "label": "ChatGPT"},
}

_ACTION_BADGES = {"BUY": "🟢 做多", "SELL": "🔴 做空", "HOLD": "⚪ 观望"}


def _parse_extra_consensus(raw) -> dict:
    """安全解析 extra_models_consensus 字段为 Python dict。"""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    raw_str = str(raw).strip()
    if not raw_str:
        return {}
    try:
        return json.loads(raw_str)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def render_ai_reasoning(row: pd.Series):
    """渲染 AI 推导逻辑：Kimi K3 主模型展开 + extra_models_consensus JSON 拆解为独立折叠卡片。"""
    st.markdown("### 🤖 AI 推导逻辑")

    kimi_reasoning = row.get("Kimi K3归因", "")
    sub_models = _parse_extra_consensus(row.get("extra_models_consensus"))

    if not kimi_reasoning and not sub_models:
        st.info("该信号暂无AI推导逻辑记录")
        return

    # ── 主模型: Kimi K3 (始终展开，紫色左边框) ──
    if kimi_reasoning:
        st.markdown("#### 🧠 Kimi K3 (主模型)")
        cleaned = str(kimi_reasoning).strip()
        if len(cleaned) > 2000:
            cleaned = cleaned[:2000] + "\n\n... (内容过长，已截断)"
        st.markdown(
            f"<div class='ai-reasoning kimi-reasoning'>{cleaned}</div>",
            unsafe_allow_html=True,
        )

    # ── 子模型: 逐个渲染折叠卡片 ──
    if sub_models:
        for model_name, model_data in sub_models.items():
            # 提取 action 和 reasoning（兼容 dict / 纯字符串）
            if isinstance(model_data, dict):
                action = (model_data.get("action") or "HOLD").strip().upper()
                reasoning = (model_data.get("reasoning") or "").strip()
            else:
                action = "HOLD"
                reasoning = str(model_data).strip()

            cfg = _MODEL_CONFIG.get(model_name, {})
            emoji = cfg.get("emoji", "🤖")
            css_cls = cfg.get("css_class", "sub-model-card")
            label = cfg.get("label", model_name)
            badge = _ACTION_BADGES.get(action, action)

            if len(reasoning) > 1200:
                reasoning = reasoning[:1200] + "\n\n... (内容过长，已截断)"

            with st.expander(f"{emoji} {label} — {badge}", expanded=False):
                st.markdown(
                    f"<div class='sub-model-card {css_cls}'>"
                    f"<strong>{emoji} {label} · 投票: {badge}</strong>"
                    f"<br><br>{reasoning}"
                    f"</div>",
                    unsafe_allow_html=True,
                )


# -----------------------------------------------------------------------------
# Main Application
# -----------------------------------------------------------------------------

def main():
    """主应用逻辑。"""

    # 页面标题
    st.title("📊 Trident 量化复盘看板")
    st.markdown("---")

    # 侧边栏
    uploaded_file_state = st.session_state.get("uploaded_file")
    if uploaded_file_state is None:
        st.session_state["uploaded_file"] = True

    # 尝试加载默认数据（如果有的话）
    df = pd.DataFrame()

    # 渲染侧边栏
    selected_row, event_datetime = render_sidebar(df)

    if selected_row is None:
        st.info("👈 请在侧边栏上传信号文件并选择信号")
        return

    # 主视图布局
    render_signal_header(selected_row)

    # 指标行
    st.markdown("---")
    render_metrics(selected_row)
    st.markdown("---")

    # ── 驱动新闻展示 ──
    news_text = selected_row.get("新闻内容", "")
    if news_text and str(news_text).strip():
        news_text = str(news_text).strip()
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #1e293b 0%, #1a2235 100%);
                border: 1px solid #334155;
                border-left: 4px solid #f59e0b;
                border-radius: 8px;
                padding: 1rem 1.2rem;
                margin: 0.5rem 0 1rem 0;
                color: #e2e8f0 !important;
            ">
                <div style="font-size: 1.05rem; margin-bottom: 0.4rem; color: #fbbf24 !important;">
                    📰 驱动新闻
                </div>
                <div style="font-size: 0.92rem; line-height: 1.7; color: #cbd5e1 !important;">
                    {news_text}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("---")

    # 左右分栏布局（7:3）
    left_col, right_col = st.columns([7, 3])

    with left_col:
        st.subheader("📈 K线微观结构")

        # 获取K线数据
        asset = selected_row.get("品种")
        entry_price = float(selected_row.get("入场价", 0))
        direction = selected_row.get("方向")
        binance_symbol = _resolve_binance_symbol(asset)

        if entry_price == 0:
            st.warning("⚠️ 入场价数据缺失，无法绘制入场价线")

        kline_df = fetch_kline_data(asset, event_datetime)

        if kline_df is not None and not kline_df.empty:
            # 创建K线图
            fig = create_candlestick_chart(
                kline_df, event_datetime, entry_price, direction,
                symbol_label=binance_symbol,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(
                """
                ⚠️ 无法获取K线数据

                可能的原因：
                1. 币安API网络连接问题（直连模式，检查防火墙或网络策略）
                2. 该时间段品种无成交
                3. 品种代码映射错误

                但您仍可查看右侧的AI推导逻辑。
                """
            )

        # 额外信息
        if kline_df is not None and not kline_df.empty:
            with st.expander("📊 K线统计信息", expanded=False):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("K线根数", len(kline_df))
                with col2:
                    st.metric("时间跨度", f"{len(kline_df)} 分钟")
                with col3:
                    st.metric("最高价", f"{kline_df['High'].max():.2f}")
                with col4:
                    st.metric("最低价", f"{kline_df['Low'].min():.2f}")

    with right_col:
        render_ai_reasoning(selected_row)

        # 额外信息展示（如果有）
        with st.expander("ℹ️ 信号详情", expanded=False):
            st.json({
                "品种": selected_row.get("品种"),
                "方向": selected_row.get("方向"),
                "入场价": str(selected_row.get("入场价")),
                "评分": str(selected_row.get("评分")),
            })

    # 底部提示
    st.markdown("---")
    st.caption(
        """
        💡 **使用提示**：
        - 鼠标悬停在K线上可查看详细价格信息
        - 拖拽和缩放图表查看不同时间段
        - 点击图例可隐藏/显示数据系列
        - 使用右侧复选框展开更多信息
        """
    )


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    main()
