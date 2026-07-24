import os
# 代理从 .env 读取，不再硬编码
if not os.getenv("HTTP_PROXY") and not os.getenv("http_proxy"):
    pass  # 直连模式 — 服务器端无需代理

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import ccxt
import datetime
import sys

st.set_page_config(page_title="Trident 宏观复盘面板", layout="wide", initial_sidebar_state="expanded")

def fetch_all_ohlcv(exchange, symbol, start_dt, end_dt):
    """突破限制：循环拉取指定时间段内的所有 1m K线"""
    all_ohlcv = []
    # timezone-aware .timestamp() → UTC epoch，避免 naive 按本机时区隐式解释
    since_ms = int(pd.Timestamp(start_dt).timestamp() * 1000)
    end_ms = int(pd.Timestamp(end_dt).timestamp() * 1000)

    # DEBUG: 打印初始时间范围
    print(f"[DEBUG] fetch_all_ohlcv 开始: symbol={symbol}, start_dt={start_dt}, end_dt={end_dt}", file=sys.stderr)
    print(f"[DEBUG] since_ms={since_ms}, end_ms={end_ms}", file=sys.stderr)

    # 【重要】检查是否为未来时间
    now_ms = int(pd.Timestamp.now(tz='UTC').timestamp() * 1000)
    if since_ms > now_ms:
        print(f"[DEBUG] 检测到未来时间，改为拉取最近可用数据", file=sys.stderr)
        # 拉取最近 1000 条数据
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1m', limit=1000)
            print(f"[DEBUG] 拉取最近数据完成，获得 {len(ohlcv)} 条", file=sys.stderr)
            return ohlcv
        except Exception as e:
            print(f"[DEBUG] 拉取最近数据失败: {e}", file=sys.stderr)
            return []

    loop_count = 0
    max_loops = 100  # 防止无限循环的安全限制

    while since_ms < end_ms and loop_count < max_loops:
        loop_count += 1
        print(f"[DEBUG] 第 {loop_count} 轮循环: since_ms={since_ms}", file=sys.stderr)

        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1m', since=since_ms, limit=1000)
            print(f"[DEBUG] 第 {loop_count} 轮获取到 {len(ohlcv)} 条数据", file=sys.stderr)

            if not ohlcv:
                print(f"[DEBUG] 第 {loop_count} 轮无数据，退出循环", file=sys.stderr)
                break

            all_ohlcv.extend(ohlcv)
            since_ms = ohlcv[-1][0] + 60000

            # 如果最后一条数据已经超过结束时间，退出
            if ohlcv[-1][0] >= end_ms:
                print(f"[DEBUG] 已到达结束时间，退出循环", file=sys.stderr)
                break

        except Exception as e:
            print(f"[DEBUG] 第 {loop_count} 轮发生异常: {e}", file=sys.stderr)
            raise

    print(f"[DEBUG] fetch_all_ohlcv 完成: 总共获取 {len(all_ohlcv)} 条数据", file=sys.stderr)
    return all_ohlcv

def main():
    st.sidebar.title("🔱 Trident 宏观控制台")
    uploaded_file = st.sidebar.file_uploader("上传 trident_signals.xlsx", type=['xlsx'])

    if uploaded_file is not None:
        df = pd.read_excel(uploaded_file)
        trade_date = st.sidebar.date_input("选择复盘日期", datetime.date.today())

        # 组装完整时间（Excel 时间为北京墙钟，显式标注 Asia/Shanghai）
        df['完整时间'] = pd.to_datetime(
            trade_date.strftime('%Y-%m-%d') + ' ' + df['时间'].astype(str)
        ).dt.tz_localize('Asia/Shanghai')

        symbols = df['品种'].dropna().unique()
        target_symbol = st.sidebar.selectbox("选择要复盘的标的 (全局视角)", symbols)

        symbol_df = df[df['品种'] == target_symbol].copy()
        symbol_df = symbol_df.sort_values(by='完整时间')

        st.header(f"🎯 {target_symbol} 全天候 AI 信号复盘面板")

        if symbol_df.empty:
            st.warning("该品种在当天无交易信号。")
            return

        if target_symbol not in ['BTC', 'ETH', 'SOL', 'BNB', 'DOGE']:
            st.info(f"ℹ️ {target_symbol} 属于传统资产。CCXT 节点仅支持加密货币图表展示。")
            return

        # 【修复1】使用 datetime.timedelta 替换 pd.Timedelta，干掉终端烦人的警告
        signal_start = symbol_df['完整时间'].min() - datetime.timedelta(hours=1)
        signal_end = symbol_df['完整时间'].max() + datetime.timedelta(hours=1)

        # 【时间映射逻辑】检测未来时间，映射到当前真实世界
        now = pd.Timestamp.now(tz='Asia/Shanghai')
        if signal_start > now:
            # 信号时间是未来，映射到当前时间段（保持相对时间差）
            print(f"[DEBUG] 检测到未来时间，启用时间映射模式", file=sys.stderr)

            # 计算信号时间跨度
            time_span = signal_end - signal_start

            # 映射到当前时间（往前推）
            start_time = now - datetime.timedelta(hours=1)
            end_time = start_time + time_span

            st.info(f"⏰ **时间映射模式**: 信号时间为未来（{signal_start.strftime('%Y-%m-%d')}），已自动映射到当前真实世界数据")
        else:
            start_time = signal_start
            end_time = signal_end

        st.write(f"**数据区间:** `{start_time.strftime('%H:%M')}` 至 `{end_time.strftime('%H:%M')}` | **共产生信号:** `{len(symbol_df)}` 次")

        # DEBUG: 打印时间范围检查
        print(f"[DEBUG] 准备拉取 K 线: start_time={start_time}, end_time={end_time}", file=sys.stderr)
        print(f"[DEBUG] 时间戳范围: {int(start_time.timestamp() * 1000)} 至 {int(end_time.timestamp() * 1000)}", file=sys.stderr)

        with st.spinner(f'正在拉取 {target_symbol} 全局 K 线网络数据，请稍候...'):
            try:
                print(f"[DEBUG] 开始初始化 CCXT 交易所...", file=sys.stderr)

                # 代理从 .env 读取
                _proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
                _proxies = {"http": _proxy, "https": _proxy} if _proxy else {}
                exchange = ccxt.binance({
                    'timeout': 10000,
                    'connectTimeout': 5000,
                    'enableRateLimit': True,
                    'proxies': _proxies,
                    'options': {
                        'defaultType': 'future',
                    }
                })

                print(f"[DEBUG] CCXT 交易所初始化完成", file=sys.stderr)

                ccxt_symbol = f"{target_symbol}/USDT"
                print(f"[DEBUG] 准备调用 fetch_all_ohlcv: symbol={ccxt_symbol}", file=sys.stderr)

                ohlcv = fetch_all_ohlcv(exchange, ccxt_symbol, start_time, end_time)

                print(f"[DEBUG] fetch_all_ohlcv 返回，获得 {len(ohlcv) if ohlcv else 0} 条数据", file=sys.stderr)

                if ohlcv:
                    raw_data = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    # Binance/CCXT 为 UTC ms → 统一转为 Asia/Shanghai，与信号同一套时区
                    raw_data['timestamp'] = pd.to_datetime(
                        raw_data['timestamp'], unit='ms', utc=True
                    ).dt.tz_convert('Asia/Shanghai')
                    raw_data.set_index('timestamp', inplace=True)

                    print(f"[DEBUG] DataFrame 构建完成，行数: {len(raw_data)}", file=sys.stderr)
                    print(f"[DEBUG] K线时区: {raw_data.index.tz}, 信号时区: {symbol_df['完整时间'].dt.tz}", file=sys.stderr)

                    fig = go.Figure(data=[go.Candlestick(x=raw_data.index,
                                    open=raw_data['Open'], high=raw_data['High'],
                                    low=raw_data['Low'], close=raw_data['Close'],
                                    name='K线')])

                    # 遍历打上所有信号标签（x 与 K 线 index 同为 Asia/Shanghai）
                    for idx, row in symbol_df.iterrows():
                        t = row['完整时间']
                        p = row['入场价']
                        direction = row.get('方向', '多')
                        win_loss = row.get('胜负', '—')

                        if win_loss == '胜':
                            marker_color = '#FF3333'
                        elif win_loss == '负':
                            marker_color = '#00FF00'
                        else:
                            marker_color = '#AAAAAA'

                        marker_text = "b ➡" if direction == "多" else "s ⭕"

                        fig.add_annotation(
                            x=t, y=p,
                            text=f"<b>{marker_text}</b>",
                            showarrow=True,
                            arrowhead=2,
                            arrowsize=1,
                            arrowwidth=2,
                            arrowcolor=marker_color,
                            font=dict(color="white", size=13),
                            bgcolor=marker_color,
                            bordercolor="white",
                            borderwidth=1,
                            borderpad=4,
                            ax=0, ay=40 if direction == "多" else -40,
                            hovertext=f"时间: {t.strftime('%H:%M')}<br>类型: {direction}<br>结果: {win_loss}"
                        )

                    print(f"[DEBUG] 图表构建完成，准备渲染...", file=sys.stderr)

                    fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, height=650, margin=dict(l=0, r=0, t=30, b=0))
                    st.plotly_chart(fig, use_container_width=True)

                    print(f"[DEBUG] 图表渲染完成", file=sys.stderr)

                    st.subheader("📝 信号日志与 AI 归因归档")
                    for idx, row in symbol_df.iterrows():
                        with st.expander(f"[{row['方向']}] {row['时间']} | 价格: ${row['入场价']} | 战绩: {row.get('胜负', '—')}"):
                            st.markdown(f"**📰 触发新闻:** {row.get('新闻内容', '无')}")
                            kimi = str(row.get('Kimi K3归因', '无数据')).replace("→", "\n\n**→** ")
                            st.success(f"**Kimi K3 深度推演:**\n\n{kimi}")
                else:
                    print(f"[DEBUG] ohlcv 为空，跳过图表渲染", file=sys.stderr)
                    st.warning("未获取到 K 线数据")

            except Exception as e:
                print(f"[DEBUG] 发生异常: {type(e).__name__}: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                st.error(f"网络阻断，K线数据获取失败: {e}")

if __name__ == "__main__":
    main()
