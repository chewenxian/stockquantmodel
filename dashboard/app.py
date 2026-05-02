"""
Streamlit 可视化看板 — 股票量化分析系统 (v7.0)

覆盖系统全部功能模块：
1. 市场总览 — KPI、情绪仪表盘、信号概览、热点新闻
2. 个股分析 — K线/技术指标、情绪趋势、AI报告、资金流向、关联新闻公告、龙虎榜
3. 资金情绪 — 资金流向、北向资金、融资融券、股吧情绪
4. 龙虎榜 — 龙虎榜数据浏览
5. 信号看板 — 信号分级展示
6. 热点板块 — 个股热度、板块行情
7. 政策宏观 — 政策新闻、宏观数据、国债收益率
8. 回测系统 — 策略回测与绩效
9. 系统管理 — 数据源、采集器健康、归档、日志

启动： streamlit run dashboard/app.py
"""
import sys
import os
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from storage.database import Database
from analyzer.backtest import BacktestEngine

# ═══════════════════════════════════════════════════════════
# 页面配置
# ═══════════════════════════════════════════════════════════

st.set_page_config(
    page_title="股票量化分析系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 深色主题 CSS
st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: #fafafa; }
    .stTabs [data-baseweb="tab"] { color: #9e9e9e; font-weight: 500; }
    .stTabs [aria-selected="true"] { color: #00d4ff; }
    .stButton > button { border-radius: 8px; font-weight: 600; }
    .stSelectbox label, .stSlider label { color: #9e9e9e !important; }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #1e2538 100%);
        border: 1px solid #2a3040;
        border-radius: 12px;
        padding: 20px;
        margin: 8px 0;
    }
    .metric-card h3 { color: #9e9e9e; font-size: 14px; margin: 0 0 8px 0; }
    .metric-card .value { color: #fafafa; font-size: 28px; font-weight: 700; }
    .metric-card .sub { color: #6b7280; font-size: 12px; margin-top: 4px; }
    .positive { color: #00c853 !important; }
    .negative { color: #ff1744 !important; }
    .neutral { color: #ffd600 !important; }
    .info-card {
        background: #1a1f2e; border-radius: 8px; padding: 12px; margin: 4px 0;
    }
    .info-card .title { font-size: 14px; font-weight: 600; }
    .info-card .meta { font-size: 12px; color: #6b7280; margin-top: 4px; }
    .info-card .body { font-size: 13px; color: #9e9e9e; margin-top: 6px; }
    .signal-S { border-left: 4px solid #ff1744; background: #1a1f2e; padding: 12px; margin: 4px 0; border-radius: 4px; }
    .signal-A { border-left: 4px solid #ff6d00; background: #1a1f2e; padding: 12px; margin: 4px 0; border-radius: 4px; }
    .signal-B { border-left: 4px solid #ffd600; background: #1a1f2e; padding: 12px; margin: 4px 0; border-radius: 4px; }
    .signal-C { border-left: 4px solid #78909c; background: #1a1f2e; padding: 12px; margin: 4px 0; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# 全局缓存与工具函数
# ═══════════════════════════════════════════════════════════

@st.cache_resource
def get_db():
    try:
        from config import get_db_path
        db_path = os.path.join(_project_root, get_db_path())
    except Exception:
        db_path = os.path.join(_project_root, "data", "stock_news.db")
    return Database(db_path)


@st.cache_resource
def get_backtest_engine():
    return BacktestEngine(get_db())


def safe_format(val, fmt="{:.2f}"):
    if val is None:
        return "--"
    try:
        return fmt.format(val)
    except (ValueError, TypeError):
        return str(val)


def color_pct(val):
    if val is None:
        return ""
    return "positive" if val >= 0 else "negative"


def sentiment_color(val):
    if val is None:
        return "#9e9e9e"
    if val >= 0.5: return "#00c853"
    if val >= 0.1: return "#69f0ae"
    if val >= -0.1: return "#ffd600"
    if val >= -0.5: return "#ff9100"
    return "#ff1744"


def signal_level(sentiment, confidence):
    """简单信号分级（同 api/main.py 逻辑）"""
    if confidence >= 0.8 and abs(sentiment) >= 0.5:
        return "S"
    if confidence >= 0.6 and abs(sentiment) >= 0.3:
        return "A"
    if confidence >= 0.4:
        return "B"
    if confidence >= 0.2:
        return "C"
    return "无效"


def signal_color(level):
    return {"S": "#ff1744", "A": "#ff6d00", "B": "#ffd600", "C": "#78909c", "无效": "#6b7280"}.get(level, "#6b7280")


def render_metric(value, label, sub="", val_class=""):
    st.markdown(f"""
    <div class="metric-card">
        <h3>{label}</h3>
        <div class="value {val_class}">{value}</div>
        {f'<div class="sub">{sub}</div>' if sub else ''}
    </div>
    """, unsafe_allow_html=True)


def render_info_card(title, meta, body="", extra=""):
    st.markdown(f"""
    <div class="info-card">
        <div class="title">{title}</div>
        <div class="meta">{meta}</div>
        {f'<div class="body">{body}</div>' if body else ''}
        {extra}
    </div>
    """, unsafe_allow_html=True)


def query_sql(db, sql: str, params=()) -> List[Dict]:
    """执行 SQL 查询并返回字典列表"""
    conn = db._connect()
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        db._close(conn)


# ═══════════════════════════════════════════════════════════
# 数据库实例
# ═══════════════════════════════════════════════════════════

db = get_db()
bt = get_backtest_engine()

# ═══════════════════════════════════════════════════════════
# 侧边栏导航
# ═══════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding:12px 0;">
        <span style="font-size:48px;">📈</span>
        <h2 style="margin:8px 0 0 0;">股票量化分析</h2>
        <p style="color:#6b7280; font-size:12px;">v7.0 · 全功能版</p>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "导航",
        [
            "🏠 市场总览",
            "🔍 个股分析",
            "💰 资金情绪",
            "🐉 龙虎榜",
            "📡 信号看板",
            "📊 热点板块",
            "🏛️ 政策宏观",
            "📈 回测系统",
            "⚙️ 系统管理",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    st.caption(f"📅 {now_str}")
    st.caption("⚡ 数据源: SQLite")

    if st.button("🔄 刷新数据", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════════
# 页面1: 🏠 市场总览
# ═══════════════════════════════════════════════════════════

def page_market_overview():
    st.header("🏠 市场总览")

    try:
        stats = db.get_stats()
    except Exception:
        stats = {}

    # ▸ KPI 行
    kp1, kp2, kp3, kp4, kp5 = st.columns(5)
    with kp1:
        render_metric(stats.get('total_news', 0), "📰 新闻总数",
                      f"今日: {stats.get('today_news', 0)}")
    with kp2:
        render_metric(stats.get('total_stocks', 0), "🏢 自选股",
                      f"已分析: {stats.get('total_analysis', 0)}")
    with kp3:
        render_metric(stats.get('total_policies', 0), "📜 政策",
                      f"公告: {stats.get('total_announcements', 0)}")
    with kp4:
        processed = stats.get('processed_news', 0)
        unprocessed = stats.get('unprocessed_news', 0)
        total_n = stats.get('total_news', 1) or 1
        pct = processed / total_n * 100
        render_metric(f"{pct:.0f}%", "🔧 处理率",
                      f"已处理 {processed} / 未处理 {unprocessed}")
    with kp5:
        render_metric(safe_format(stats.get('db_size_mb', 0), '{:.1f}') + " MB",
                      "💾 数据库",
                      f"最早: {(stats.get('oldest_news_date') or '--')[:10]}")

    st.divider()

    # ▸ 两列：情绪仪表盘 + 信号概览
    left_col, right_col = st.columns([3, 2])

    with left_col:
        st.subheader("💭 今日情绪仪表盘")
        try:
            analyses = db.get_today_analysis()
            if analyses:
                df = pd.DataFrame(analyses)
                df["label"] = df.apply(
                    lambda r: f"{r.get('stock_code', '')} {r.get('name', '')}", axis=1)
                df["sentiment_display"] = df["avg_sentiment"].fillna(0).clip(-1, 1)

                fig = px.bar(
                    df.sort_values("sentiment_display"),
                    x="sentiment_display", y="label",
                    color="sentiment_display",
                    color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                    range_color=[-1, 1],
                    title=None,
                    labels={"sentiment_display": "情绪得分", "label": ""},
                )
                fig.update_layout(
                    height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无今日分析数据，请先运行采集和分析任务。")
        except Exception as e:
            st.warning(f"情绪数据加载失败: {e}")

    with right_col:
        st.subheader("🚦 今日信号概览")
        try:
            analyses = db.get_today_analysis()
            if analyses:
                signal_counts = {"S": 0, "A": 0, "B": 0, "C": 0, "无效": 0}
                for a in analyses:
                    lv = signal_level(a.get("avg_sentiment", 0) or 0,
                                      a.get("confidence", 0) or 0)
                    signal_counts[lv] = signal_counts.get(lv, 0) + 1

                df_sig = pd.DataFrame([
                    {"级别": k, "数量": v}
                    for k, v in signal_counts.items()
                ])
                colors = ["#ff1744", "#ff6d00", "#ffd600", "#78909c", "#6b7280"]
                fig = px.pie(
                    df_sig, values="数量", names="级别",
                    color_discrete_sequence=colors,
                    hole=0.4,
                )
                fig.update_layout(
                    height=200, plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=0, b=0),
                    showlegend=True, legend=dict(orientation="h", y=-0.2),
                )
                st.plotly_chart(fig, use_container_width=True)

                # S/A 级信号快速列表
                sa_signals = [(a, signal_level(a.get("avg_sentiment", 0) or 0,
                                              a.get("confidence", 0) or 0))
                              for a in analyses]
                sa_signals = [(a, lv) for a, lv in sa_signals if lv in ("S", "A")]
                for a, lv in sorted(sa_signals,
                                     key=lambda x: x[1] == "S", reverse=True)[:5]:
                    code = a.get("stock_code", "")
                    name = a.get("name", "")
                    sug = a.get("suggestion", "持有")
                    st.markdown(
                        f'<div class="signal-{lv}">'
                        f'<span style="font-weight:600;">[{lv}] {code} {name}</span> '
                        f'<span style="color:{signal_color(lv)};">→ {sug}</span>'
                        f'</div>',
                        unsafe_allow_html=True)
            else:
                st.info("暂无信号数据。")
        except Exception as e:
            st.warning(f"信号概览加载失败: {e}")

    st.divider()

    # ▸ 热点新闻 + 新闻分类
    col_news, col_cat = st.columns([3, 2])
    with col_news:
        st.subheader("🔥 最新新闻")
        try:
            rows = query_sql(
                db,
                "SELECT title, source, published_at, summary, "
                "       COALESCE(category, '其他') as category "
                "FROM news WHERE published_at IS NOT NULL "
                "ORDER BY published_at DESC LIMIT 15"
            )
            for n in rows:
                render_info_card(
                    n.get("title", "无标题"),
                    f"📰 {n.get('source','未知')}  |  🕐 {(n.get('published_at') or '')[:19]}  |  🏷️ {n.get('category','其他')}",
                    (n.get("summary", "") or "")[:200],
                )
        except Exception as e:
            st.warning(f"新闻列表加载失败: {e}")

    with col_cat:
        st.subheader("📂 新闻分类分布")
        try:
            categories = stats.get("categories", {})
            if categories:
                df_cat = pd.DataFrame(list(categories.items()), columns=["分类", "数量"])
                fig = px.bar(
                    df_cat, x="数量", y="分类", orientation="h",
                    color="数量", color_continuous_scale="Viridis",
                )
                fig.update_layout(
                    height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=10, b=0),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无分类数据。")
        except Exception as e:
            st.warning(f"分类统计加载失败: {e}")

    st.divider()

    # ▸ 数据源分布 + 自选股影响
    col_src, col_impact = st.columns(2)
    with col_src:
        st.subheader("📡 数据源分布")
        try:
            sources = stats.get("sources", {})
            if sources:
                df_src = pd.DataFrame(list(sources.items()), columns=["source", "count"])
                fig = px.pie(
                    df_src, values="count", names="source",
                    color_discrete_sequence=px.colors.sequential.Viridis_r,
                )
                fig.update_layout(
                    height=350, plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=10, b=0),
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据源统计。")
        except Exception as e:
            st.warning(f"数据源分布加载失败: {e}")

    with col_impact:
        st.subheader("📋 今日自选股影响")
        try:
            analyses = db.get_today_analysis()
            if analyses:
                data = []
                for a in analyses:
                    sent = a.get("avg_sentiment", 0) or 0
                    conf = a.get("confidence", 0) or 0
                    sug = a.get("suggestion", "持有")
                    if sent >= 0.5: impact = "🟢 利好"
                    elif sent <= -0.5: impact = "🔴 利空"
                    elif sent >= 0.1: impact = "🟡 偏多"
                    elif sent <= -0.1: impact = "🟠 偏空"
                    else: impact = "⚪ 中性"
                    data.append({
                        "股票": f"{a.get('stock_code','')} {a.get('name','')}",
                        "情绪": sent, "置信度": conf,
                        "影响": impact, "建议": sug,
                        "新闻数": a.get("news_count", 0),
                    })
                df = pd.DataFrame(data)
                st.dataframe(df, column_config={
                    "股票": st.column_config.TextColumn("股票"),
                    "情绪": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.NumberColumn(format="%.2f"),
                    "影响": st.column_config.TextColumn("影响"),
                    "建议": st.column_config.TextColumn("建议"),
                    "新闻数": st.column_config.NumberColumn("新闻数"),
                }, use_container_width=True, hide_index=True)
            else:
                st.info("暂无今日分析数据。")
        except Exception as e:
            st.warning(f"影响评估加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面2: 🔍 个股分析
# ═══════════════════════════════════════════════════════════

def page_stock_detail():
    st.header("🔍 个股深度分析")

    try:
        stocks = db.load_stocks()
    except Exception:
        stocks = []

    if not stocks:
        st.warning("暂无自选股数据，请先采集。")
        return

    stock_options = {f"{s['code']} {s['name']}": s["code"] for s in stocks}
    col_sel, col_days, col_extra = st.columns([3, 2, 2])
    with col_sel:
        selected = st.selectbox("选择股票", list(stock_options.keys()), key="stock_sel")
    stock_code = stock_options[selected]
    stock_name = selected.split(" ", 1)[1] if " " in selected else ""

    with col_days:
        days = st.slider("分析天数", 7, 120, 60, key="stock_days")
    with col_extra:
        tabs_display = st.radio("视图", ["综合", "技术", "资讯"], horizontal=True, key="stock_view")

    # 获取该股票代码在 stocks 表中的详细信息
    stock_info = None
    for s in stocks:
        if s["code"] == stock_code:
            stock_info = s
            break

    # 获取今日分析
    try:
        today_analysis = db.get_recent_analysis(stock_code, limit=1)
    except Exception:
        today_analysis = []

    # 获取最新行情快照
    try:
        snapshot = db.get_latest_market_snapshot(stock_code)
    except Exception:
        snapshot = None

    # KPI 卡片行
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        price = snapshot.get("price", "--") if snapshot else "--"
        change = snapshot.get("change_pct", "--") if snapshot else "--"
        chg_cls = color_pct(change)
        change_str = safe_format(change * 100, '{:+.2f}%') if isinstance(change, (int, float)) else "--"
        render_metric(price if isinstance(price, str) else safe_format(price, '{:.2f}'),
                      f"💰 {stock_code}", f"涨跌幅: <span class='{chg_cls}'>{change_str}</span>")
    with k2:
        if today_analysis:
            a = today_analysis[0]
            sug = a.get("suggestion", "持有")
            sug_color_map = {"买入": "positive", "强烈买入": "positive", "持有": "neutral",
                             "关注": "positive", "观望": "neutral", "卖出": "negative", "强烈卖出": "negative", "回避": "negative"}
            sc = "neutral"
            for k, v in sug_color_map.items():
                if k in sug: sc = v; break
            render_metric(sug, "🤖 AI建议", val_class=sc)
        else: render_metric("--", "🤖 AI建议", "暂无数据")
    with k3:
        if today_analysis:
            conf = (today_analysis[0].get("confidence") or 0) * 100
            render_metric(safe_format(conf, '{:.0f}%'), "🎯 置信度")
        else: render_metric("--", "🎯 置信度")
    with k4:
        if today_analysis:
            risk = today_analysis[0].get("risk_level", "中")
            rc = {"低": "positive", "中": "neutral", "高": "negative"}.get(risk, "neutral")
            render_metric(risk, "⚠️ 风险等级", val_class=rc)
        else: render_metric("--", "⚠️ 风险等级")
    with k5:
        if snapshot:
            vol = snapshot.get("volume", 0) or 0
            amt = snapshot.get("amount", 0) or 0
            render_metric(safe_format(vol, '{:.0f}'), "📊 成交量",
                          f"金额: {safe_format(amt/1e8, '{:.2f}')}亿")
        else: render_metric("--", "📊 成交量")

    st.divider()

    # ═══════════════════════════════════════════
    # TAB: 综合视图
    # ═══════════════════════════════════════════
    if tabs_display == "综合":
        col_chart, col_info = st.columns([3, 2])

        with col_chart:
            st.subheader("📊 K线走势")
            try:
                price_data = db.get_price_history(stock_code, days=days)
                if price_data:
                    df_price = pd.DataFrame(price_data)
                    df_price["trade_date"] = pd.to_datetime(df_price["trade_date"])
                    df_price = df_price.sort_values("trade_date")

                    # 判断是否有完整 K 线数据
                    has_candle = all(c in df_price.columns
                                     for c in ["open_price", "close_price", "high_price", "low_price"])
                    has_all = has_candle and df_price["open_price"].notna().sum() > 0

                    fig = go.Figure()

                    if has_all:
                        # 真阳线/阴线判断
                        colors = ["#00c853" if row["close_price"] >= row["open_price"]
                                  else "#ff1744" for _, row in df_price.iterrows()]

                        fig.add_trace(go.Candlestick(
                            x=df_price["trade_date"],
                            open=df_price["open_price"],
                            high=df_price["high_price"],
                            low=df_price["low_price"],
                            close=df_price["close_price"],
                            name="K线",
                            increasing_line_color="#00c853",
                            decreasing_line_color="#ff1744",
                        ))
                        price_col = "close_price"
                    else:
                        fig.add_trace(go.Scatter(
                            x=df_price["trade_date"], y=df_price.get("price", df_price.get("close_price")),
                            mode="lines+markers", name="价格",
                            line=dict(color="#00d4ff", width=2),
                            marker=dict(size=4, color="#00d4ff"),
                        ))
                        price_col = "price" if "price" in df_price.columns else "close_price"

                    # 均线
                    if price_col in df_price.columns:
                        for ma_period, ma_color, ma_name in [(5, "#ffd600", "MA5"),
                                                              (10, "#ff9100", "MA10"),
                                                              (20, "#ff6d00", "MA20"),
                                                              (60, "#d500f9", "MA60")]:
                            if len(df_price) >= ma_period:
                                ma = df_price[price_col].rolling(ma_period).mean()
                                fig.add_trace(go.Scatter(
                                    x=df_price["trade_date"], y=ma,
                                    mode="lines", name=ma_name,
                                    line=dict(color=ma_color, width=1.5 - 0.2 * (ma_period > 20)),
                                    opacity=0.8 - 0.15 * (ma_period > 20),
                                ))

                    # 成交量柱
                    vol_col = "volume" if "volume" in df_price.columns else None
                    if vol_col and df_price[vol_col].notna().sum() > 0:
                        fig.add_trace(go.Bar(
                            x=df_price["trade_date"],
                            y=df_price[vol_col],
                            name="成交量",
                            marker_color="rgba(0, 212, 255, 0.3)",
                            yaxis="y2",
                            opacity=0.5,
                        ))

                    fig.update_layout(
                        height=500,
                        plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        yaxis2=dict(overlaying="y", side="right", showgrid=False),
                        xaxis_rangeslider_visible=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # 成交量与换手率额外指标
                    if vol_col and df_price[vol_col].notna().sum() > 0:
                        avg_vol = df_price[vol_col].mean()
                        last_vol = df_price[vol_col].iloc[-1]
                        vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1
                        st.caption(f"💡 日均成交量: {avg_vol:,.0f}  |  "
                                   f"最新成交量: {last_vol:,.0f}  |  "
                                   f"量比: {vol_ratio:.2f}")
                else:
                    st.info(f"{stock_code} 暂无行情数据。")
            except Exception as e:
                st.warning(f"行情数据加载失败: {e}")

        # ▸ 右侧信息面板
        with col_info:
            # AI 分析报告
            st.subheader("🤖 AI 分析报告")
            try:
                recent = db.get_recent_analysis(stock_code, limit=1)
                if recent:
                    a = recent[0]
                    llm_text = a.get("llm_analysis", "") or "暂无分析内容"
                    suggestion = a.get("suggestion", "持有")
                    confidence = a.get("confidence", 0) or 0
                    risk = a.get("risk_level", "中")
                    sentiment = a.get("avg_sentiment", 0) or 0

                    sug_colors = {"买入": "#00c853", "强烈买入": "#00e676", "持有": "#ffd600",
                                  "关注": "#69f0ae", "观望": "#78909c", "卖出": "#ff1744",
                                  "强烈卖出": "#d50000", "回避": "#ff1744"}
                    s_color = "#78909c"
                    for k, v in sug_colors.items():
                        if k in suggestion: s_color = v; break

                    st.markdown(f"""
                    <div style="background:#1a1f2e; border-radius:12px; padding:16px;">
                        <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
                            <div style="background:#2a3040; border-radius:8px; padding:8px 12px; text-align:center; flex:1;">
                                <div style="color:#6b7280; font-size:11px;">建议</div>
                                <div style="color:{s_color}; font-size:16px; font-weight:700;">{suggestion}</div>
                            </div>
                            <div style="background:#2a3040; border-radius:8px; padding:8px 12px; text-align:center; flex:1;">
                                <div style="color:#6b7280; font-size:11px;">置信度</div>
                                <div style="color:#fafafa; font-size:16px; font-weight:700;">{safe_format(confidence*100, '{:.0f}')}%</div>
                            </div>
                            <div style="background:#2a3040; border-radius:8px; padding:8px 12px; text-align:center; flex:1;">
                                <div style="color:#6b7280; font-size:11px;">风险</div>
                                <div style="color:{'#00c853' if risk=='低' else '#ffd600' if risk=='中' else '#ff1744'}; font-size:16px; font-weight:700;">{risk}</div>
                            </div>
                            <div style="background:#2a3040; border-radius:8px; padding:8px 12px; text-align:center; flex:1;">
                                <div style="color:#6b7280; font-size:11px;">情绪</div>
                                <div style="color:{sentiment_color(sentiment)}; font-size:16px; font-weight:700;">{safe_format(sentiment, '{:+.2f}')}</div>
                            </div>
                        </div>
                        <div style="color:#e0e0e0; font-size:13px; line-height:1.6; white-space:pre-wrap;">{llm_text}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    # 关键主题
                    key_topics_raw = a.get("key_topics", "[]")
                    try:
                        topics = json.loads(key_topics_raw) if isinstance(key_topics_raw, str) else (key_topics_raw or [])
                        if topics:
                            st.markdown("**🏷️ 关键主题:**  " + "  ".join(
                                f"<span style='background:#2a3040; padding:2px 8px; border-radius:10px; font-size:12px;'>{t}</span>"
                                for t in topics
                            ), unsafe_allow_html=True)
                    except Exception:
                        pass
                else:
                    st.info("暂无 AI 分析记录。")
            except Exception as e:
                st.warning(f"AI 分析加载失败: {e}")

            st.divider()

            # 资金流向摘要
            st.subheader("💰 资金流向")
            try:
                mf = db.get_latest_money_flow(stock_code)
                if mf:
                    cols = st.columns(4)
                    labels = [("主力净流入", "main_net"), ("散户净流入", "retail_net"),
                              ("北向净流入", "north_net"), ("大单净流入", "large_order_net")]
                    for i, (label, key) in enumerate(labels):
                        val = mf.get(key, 0) or 0
                        cls = color_pct(val)
                        with cols[i]:
                            st.markdown(f"""
                            <div style="background:#2a3040; border-radius:8px; padding:8px; text-align:center;">
                                <div style="color:#6b7280; font-size:11px;">{label}</div>
                                <div style="color:{'#00c853' if val >= 0 else '#ff1744'}; font-size:14px; font-weight:600;">{safe_format(val/1e4, '{:+.2f}')}万</div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.caption("暂无资金流向数据。")
            except Exception as e:
                st.warning(f"资金流向加载失败: {e}")

            st.divider()

            # 龙虎榜摘要
            st.subheader("🐉 龙虎榜")
            try:
                dt_rows = query_sql(
                    db,
                    "SELECT trade_date, net_amount, buy_amount, sell_amount, reason "
                    "FROM dragon_tiger WHERE stock_code = ? "
                    "ORDER BY trade_date DESC LIMIT 5",
                    (stock_code,)
                )
                if dt_rows:
                    for r in dt_rows:
                        net = r.get("net_amount", 0) or 0
                        st.markdown(f"""
                        <div style="background:#1a1f2e; border-radius:6px; padding:8px; margin:4px 0;">
                            <span style="font-size:12px;">📅 {r.get('trade_date','')}</span>
                            <span style="font-size:12px; margin-left:12px; color:{'#00c853' if net>=0 else '#ff1744'};">
                                净额: {safe_format(net/1e4, '{:+.2f}')}万
                            </span>
                            <div style="font-size:11px; color:#6b7280;">{r.get('reason','')[:80]}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.caption("暂无龙虎榜数据。")
            except Exception as e:
                st.warning(f"龙虎榜加载失败: {e}")

    # ═══════════════════════════════════════════
    # TAB: 技术视图
    # ═══════════════════════════════════════════
    elif tabs_display == "技术":
        st.subheader("📉 情绪趋势")
        try:
            recent = db.get_recent_analysis(stock_code, limit=days)
            if recent:
                df_sent = pd.DataFrame(recent)
                df_sent["date"] = pd.to_datetime(df_sent["date"])
                df_sent = df_sent.sort_values("date")

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df_sent["date"], y=df_sent["avg_sentiment"],
                    name="情绪得分",
                    marker_color=df_sent["avg_sentiment"].apply(
                        lambda x: "#00c853" if (x or 0) >= 0 else "#ff1744"),
                ))
                fig.add_trace(go.Scatter(
                    x=df_sent["date"], y=df_sent["confidence"],
                    mode="lines+markers", name="置信度",
                    line=dict(color="#ffd600", width=2),
                    marker=dict(size=6), yaxis="y2",
                ))
                fig.update_layout(
                    height=350, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=10, b=0),
                    yaxis=dict(title="情绪得分", range=[-1.2, 1.2]),
                    yaxis2=dict(title="置信度", overlaying="y", side="right", range=[0, 1.2]),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"{stock_code} 暂无情绪分析记录。")
        except Exception as e:
            st.warning(f"情绪趋势加载失败: {e}")

        st.divider()

        # 技术指标摘要
        st.subheader("📐 技术指标")
        try:
            price_data = db.get_price_history(stock_code, days=120)
            if price_data and len(price_data) >= 20:
                df_tech = pd.DataFrame(price_data)
                price_col = "close_price" if "close_price" in df_tech.columns and df_tech["close_price"].notna().sum() > 0 else "price"
                prices = df_tech[price_col].dropna().values

                # 计算指标
                ma20 = prices[-20:].mean() if len(prices) >= 20 else None
                ma60 = prices[-60:].mean() if len(prices) >= 60 else None
                last_price = prices[-1] if len(prices) > 0 else None
                highest = prices.max() if len(prices) > 0 else None
                lowest = prices.min() if len(prices) > 0 else None

                # RSI(14)
                if len(prices) >= 15:
                    gains = [max(prices[i] - prices[i-1], 0) for i in range(-14, 0)]
                    losses = [max(prices[i-1] - prices[i], 0) for i in range(-14, 0)]
                    avg_gain = sum(gains) / 14
                    avg_loss = sum(losses) / 14
                    rsi = 50 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
                else:
                    rsi = None

                tc1, tc2, tc3, tc4, tc5 = st.columns(5)
                with tc1:
                    render_metric(safe_format(last_price, '{:.2f}'), "当前价")
                with tc2:
                    render_metric(safe_format(ma20, '{:.2f}') if ma20 else "--", "MA20")
                with tc3:
                    render_metric(safe_format(ma60, '{:.2f}') if ma60 else "--", "MA60")
                with tc4:
                    render_metric(safe_format(rsi, '{:.1f}') if rsi else "--", "RSI(14)",
                                  "超买>70 超卖<30")
                with tc5:
                    pct_high = (last_price / highest - 1) * 100 if last_price and highest else 0
                    render_metric(safe_format(pct_high, '{:+.1f}%'), "距高点",
                                  f"最高: {safe_format(highest, '{:.2f}')}" if highest else "")

                # 布林带
                st.subheader("📊 布林带")
                if len(prices) >= 20:
                    df_bb = pd.DataFrame(price_data[-60:] if len(price_data) > 60 else price_data)
                    df_bb["trade_date"] = pd.to_datetime(df_bb["trade_date"])
                    df_bb = df_bb.sort_values("trade_date")

                    bb_prices = df_bb[price_col].values
                    bb_ma = pd.Series(bb_prices).rolling(20).mean()
                    bb_std = pd.Series(bb_prices).rolling(20).std()
                    upper = bb_ma + 2 * bb_std
                    lower = bb_ma - 2 * bb_std

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_bb["trade_date"], y=df_bb[price_col],
                        mode="lines", name="价格",
                        line=dict(color="#00d4ff", width=2),
                    ))
                    fig.add_trace(go.Scatter(
                        x=df_bb["trade_date"], y=upper,
                        mode="lines", name="上轨",
                        line=dict(color="rgba(0,200,83,0.3)", width=1),
                    ))
                    fig.add_trace(go.Scatter(
                        x=df_bb["trade_date"], y=bb_ma,
                        mode="lines", name="中轨",
                        line=dict(color="rgba(255,214,0,0.5)", width=1, dash="dash"),
                    ))
                    fig.add_trace(go.Scatter(
                        x=df_bb["trade_date"], y=lower,
                        mode="lines", name="下轨",
                        line=dict(color="rgba(255,23,68,0.3)", width=1),
                        fill="tonexty", fillcolor="rgba(128,128,128,0.05)",
                    ))
                    fig.update_layout(
                        height=300, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=10, b=0),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("技术指标需要至少20个交易日的数据。")
        except Exception as e:
            st.warning(f"技术指标加载失败: {e}")

    # ═══════════════════════════════════════════
    # TAB: 资讯视图
    # ═══════════════════════════════════════════
    elif tabs_display == "资讯":
        tab_n, tab_a = st.tabs(["📰 相关新闻", "📋 公司公告"])
        with tab_n:
            st.subheader(f"📰 {stock_code} 相关新闻")
            try:
                news_items = db.get_stock_news_sentiment(stock_code, days=days)
                if news_items:
                    for n in news_items[:20]:
                        render_info_card(
                            n.get("title", "无标题"),
                            f"📰 {n.get('source','未知')}  |  🕐 {(n.get('published_at') or '')[:19]}  |  💭 {safe_format(n.get('sentiment',0), '{:+.2f}')}",
                            (n.get("summary") or "")[:200],
                        )
                else:
                    st.info(f"{stock_code} 暂无相关新闻。")
            except Exception as e:
                st.warning(f"新闻加载失败: {e}")

        with tab_a:
            st.subheader(f"📋 {stock_code} 公司公告")
            try:
                ann_items = query_sql(
                    db,
                    "SELECT title, announce_type, publish_date, summary, url "
                    "FROM announcements WHERE stock_code = ? "
                    "ORDER BY publish_date DESC LIMIT 20",
                    (stock_code,)
                )
                if ann_items:
                    for a in ann_items:
                        render_info_card(
                            a.get("title", "无标题"),
                            f"📋 {a.get('announce_type','公告')}  |  🕐 {(a.get('publish_date') or '')[:10]}",
                            (a.get("summary") or "")[:200],
                        )
                else:
                    st.info(f"{stock_code} 暂无公告数据。")
            except Exception as e:
                st.warning(f"公告加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面3: 💰 资金情绪
# ═══════════════════════════════════════════════════════════

def page_money_flow():
    st.header("💰 资金情绪分析")

    tab_mf, tab_nf, tab_mt, tab_gb = st.tabs([
        "💰 资金流向", "🌊 北向资金", "🏦 融资融券", "💬 股吧情绪"])

    # ─── TAB 1: 资金流向 ───
    with tab_mf:
        st.subheader("最新资金流向")
        try:
            mf_data = query_sql(
                db,
                "SELECT mf.* FROM money_flow mf "
                "WHERE mf.date = (SELECT MAX(date) FROM money_flow) "
                "ORDER BY ABS(mf.main_net) DESC LIMIT 30"
            )
            if mf_data:
                df = pd.DataFrame(mf_data)
                # 从 money_flow 的 stock_code 推断板块名称（同花顺指数代码）
                # 显示原始代码作为标识
                df["name"] = df["stock_code"]
                cols = ["stock_code", "name", "date", "main_net", "retail_net",
                        "north_net", "large_order_net", "total_amount"]
                display_cols = [c for c in cols if c in df.columns]
                df_display = df[display_cols].copy()
                for c in ["main_net", "retail_net", "north_net", "large_order_net", "total_amount"]:
                    if c in df_display.columns:
                        df_display[c] = df_display[c].apply(lambda x: safe_format((x or 0)/1e4, '{:+.2f}') + "万")
                df_display.columns = ["板块代码", "板块代码", "日期", "主力净流入", "散户净流入",
                                       "北向净流入", "大单净流入", "成交总额"]
                st.dataframe(df_display, use_container_width=True, hide_index=True)

                # 主力净流入 Top 条形图
                if "main_net" in df.columns:
                    df_bar = df.nlargest(15, "main_net")
                    df_bar["label"] = df_bar["stock_code"]
                    fig = px.bar(
                        df_bar.sort_values("main_net"),
                        x="main_net", y="label", orientation="h",
                        color="main_net",
                        color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                        title="板块主力净流入 Top15",
                        labels={"main_net": "主力净流入(元)", "label": ""},
                    )
                    fig.update_layout(
                        height=500, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无资金流向数据。")
        except Exception as e:
            st.warning(f"资金流向加载失败: {e}")

    # ─── TAB 2: 北向资金 ───
    with tab_nf:
        st.subheader("🌊 北向资金历史走势")
        try:
            nf_data = query_sql(
                db,
                "SELECT * FROM north_flow ORDER BY trade_date DESC LIMIT 120"
            )
            if nf_data:
                df = pd.DataFrame(nf_data)
                df = df.sort_values("trade_date")
                df["trade_date"] = pd.to_datetime(df["trade_date"])

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df["trade_date"], y=df["total_net"],
                    name="当日净流入",
                    marker_color=df["total_net"].apply(
                        lambda x: "#00c853" if (x or 0) >= 0 else "#ff1744"),
                ))
                if "cumulative_net" in df.columns:
                    fig.add_trace(go.Scatter(
                        x=df["trade_date"], y=df["cumulative_net"],
                        mode="lines", name="累计净流入",
                        line=dict(color="#ffd600", width=2),
                        yaxis="y2",
                    ))
                fig.update_layout(
                    height=450, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                    yaxis=dict(title="当日净流入(元)"),
                    yaxis2=dict(title="累计净流入(元)", overlaying="y", side="right"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)

                # 最近5条
                st.subheader("📋 最近记录")
                df_recent = df.tail(5).copy()
                display = df_recent[["trade_date", "sh_net", "sz_net", "total_net", "cumulative_net"]]
                for c in ["sh_net", "sz_net", "total_net", "cumulative_net"]:
                    if c in display.columns:
                        display[c] = display[c].apply(lambda x: safe_format((x or 0)/1e8, '{:.2f}') + "亿")
                display.columns = ["日期", "沪股通", "深股通", "总计", "累计"]
                st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.info("暂无北向资金数据。")
        except Exception as e:
            st.warning(f"北向资金加载失败: {e}")

    # ─── TAB 3: 融资融券 ───
    with tab_mt:
        st.subheader("🏦 融资融券")
        try:
            mt_data = query_sql(
                db,
                "SELECT * FROM margin_trading "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM margin_trading) "
                "ORDER BY ABS(margin_net_buy) DESC LIMIT 30"
            )
            if mt_data:
                df = pd.DataFrame(mt_data)
                cols = ["stock_code", "stock_name", "margin_balance", "short_balance",
                        "margin_net_buy", "trade_date"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                for c in ["margin_balance", "short_balance", "margin_net_buy"]:
                    if c in df_d.columns:
                        df_d[c] = df_d[c].apply(lambda x: safe_format((x or 0)/1e8, '{:.2f}') + "亿")
                df_d.columns = ["代码", "名称", "融资余额", "融券余额", "净买入", "日期"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)

                if "margin_net_buy" in df.columns:
                    df_top = df.nlargest(15, "margin_net_buy")
                    df_top["label"] = df_top.apply(
                        lambda r: f"{r['stock_code']} {r.get('stock_name','')}", axis=1)
                    fig = px.bar(
                        df_top.sort_values("margin_net_buy"),
                        x="margin_net_buy", y="label", orientation="h",
                        color="margin_net_buy",
                        color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                        title="融资净买入 Top15",
                        labels={"margin_net_buy": "净买入(元)", "label": ""},
                    )
                    fig.update_layout(
                        height=500, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无融资融券数据。")
        except Exception as e:
            st.warning(f"融资融券加载失败: {e}")

    # ─── TAB 4: 股吧情绪 ───
    with tab_gb:
        st.subheader("💬 股吧情绪")
        try:
            gb_data = query_sql(
                db,
                "SELECT * FROM guba_sentiment "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM guba_sentiment) "
                "ORDER BY ABS(sentiment_score) DESC LIMIT 30"
            )
            if gb_data:
                df = pd.DataFrame(gb_data)
                fig = px.scatter(
                    df, x="bullish_ratio", y="sentiment_score",
                    size="post_count", hover_name="stock_name",
                    color="sentiment_score",
                    color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                    title="股吧情绪散点图",
                    labels={"bullish_ratio": "看涨比例", "sentiment_score": "情绪得分"},
                )
                fig.update_layout(
                    height=500, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

                cols = ["stock_code", "stock_name", "post_count", "view_count",
                        "bullish_ratio", "bearish_ratio", "sentiment_score"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                df_d.columns = ["代码", "名称", "帖子数", "阅读数", "看涨比", "看跌比", "情绪分"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)
            else:
                st.info("暂无股吧情绪数据。")
        except Exception as e:
            st.warning(f"股吧情绪加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面4: 🐉 龙虎榜
# ═══════════════════════════════════════════════════════════

def page_dragon_tiger():
    st.header("🐉 龙虎榜")

    try:
        stocks = db.load_stocks()
    except Exception:
        stocks = []

    stock_codes = [""] + [s["code"] for s in stocks]
    sel_code = st.selectbox("📌 筛选股票", stock_codes, format_func=lambda x: x if x else "全部股票")

    try:
        if sel_code:
            dt_data = query_sql(
                db,
                "SELECT dt.*, s.name FROM dragon_tiger dt "
                "LEFT JOIN stocks s ON dt.stock_code = s.code "
                "WHERE dt.stock_code = ? "
                "ORDER BY dt.trade_date DESC LIMIT 30",
                (sel_code,)
            )
        else:
            dt_data = query_sql(
                db,
                "SELECT dt.*, s.name FROM dragon_tiger dt "
                "LEFT JOIN stocks s ON dt.stock_code = s.code "
                "ORDER BY dt.trade_date DESC LIMIT 50"
            )

        if dt_data:
            df = pd.DataFrame(dt_data)

            # KPI 行
            total_net = df["net_amount"].sum() if "net_amount" in df.columns else 0
            k1, k2, k3 = st.columns(3)
            with k1: render_metric(len(df), "📋 上榜次数")
            with k2: render_metric(
                safe_format(total_net/1e8, '{:+.2f}') + "亿", "💰 合计净额",
                val_class=color_pct(total_net))
            with k3:
                latest_date = df["trade_date"].iloc[0] if "trade_date" in df.columns else "--"
                render_metric(latest_date, "📅 最近上榜")

            st.divider()

            # 表格
            cols = ["trade_date", "stock_code", "name", "net_amount", "buy_amount",
                    "sell_amount", "reason"]
            dc = [c for c in cols if c in df.columns]
            df_d = df[dc].copy()
            for c in ["net_amount", "buy_amount", "sell_amount"]:
                if c in df_d.columns:
                    df_d[c] = df_d[c].apply(lambda x: safe_format((x or 0)/1e4, '{:+.2f}') + "万")
            df_d.columns = ["日期", "代码", "名称", "净额", "买入额", "卖出额", "原因"]
            st.dataframe(df_d, use_container_width=True, hide_index=True)

            # 净额条形图
            st.divider()
            st.subheader("📊 净额分布")
            if "net_amount" in df.columns:
                df_bar = df.copy()
                df_bar["label"] = df_bar.apply(
                    lambda r: f"{r.get('trade_date','')} {r.get('stock_code','')}", axis=1)
                fig = px.bar(
                    df_bar.sort_values("trade_date"),
                    x="label", y="net_amount",
                    color="net_amount",
                    color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                    title="逐笔净额",
                    labels={"net_amount": "净额(元)", "label": ""},
                )
                fig.update_layout(
                    height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                    xaxis_tickangle=-45,
                )
                st.plotly_chart(fig, use_container_width=True)

            # 显示 Top Buyers/Sellers JSON
            if "top_buyers" in df.columns:
                with st.expander("📋 查看买卖席位详情"):
                    for _, r in df.iterrows():
                        st.markdown(f"**{r.get('stock_code','')} · {r.get('trade_date','')}**")
                        buyers = r.get("top_buyers", "[]")
                        sellers = r.get("top_sellers", "[]")
                        try:
                            buyers_data = json.loads(buyers) if isinstance(buyers, str) else buyers
                            sellers_data = json.loads(sellers) if isinstance(sellers, str) else sellers
                        except Exception:
                            buyers_data = []
                            sellers_data = []
                        col_b, col_s = st.columns(2)
                        with col_b:
                            st.caption("🏅 买入前五")
                            if buyers_data:
                                st.json(buyers_data)
                            else:
                                st.caption("无数据")
                        with col_s:
                            st.caption("🏅 卖出前五")
                            if sellers_data:
                                st.json(sellers_data)
                            else:
                                st.caption("无数据")
                        st.divider()
        else:
            st.info("暂无龙虎榜数据。")
    except Exception as e:
        st.warning(f"龙虎榜加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面5: 📡 信号看板
# ═══════════════════════════════════════════════════════════

def page_signal_board():
    st.header("📡 信号看板")

    tab_sig_today, tab_sig_hist = st.tabs(["🚦 今日信号", "📊 信号历史"])

    with tab_sig_today:
        try:
            analyses = db.get_today_analysis()
            if not analyses:
                st.info("暂无今日分析数据，请先运行分析任务。")
                return

            # 分级
            graded = []
            for a in analyses:
                sent = a.get("avg_sentiment", 0) or 0
                conf = a.get("confidence", 0) or 0
                lv = signal_level(sent, conf)
                graded.append({**a, "signal_level": lv})

            # 统计卡片
            counts = {"S": 0, "A": 0, "B": 0, "C": 0, "无效": 0}
            for g in graded:
                counts[g["signal_level"]] = counts.get(g["signal_level"], 0) + 1

            k1, k2, k3, k4, k5 = st.columns(5)
            with k1: render_metric(counts["S"], "🔴 S级", "强烈", "negative")
            with k2: render_metric(counts["A"], "🟠 A级", "强", "neutral")
            with k3: render_metric(counts["B"], "🟡 B级", "中")
            with k4: render_metric(counts["C"], "⚪ C级", "弱")
            with k5: render_metric(counts["无效"], "⬜ 无效", "数据不足")

            st.divider()

            # 分级详情
            level_filter = st.radio("信号级别筛选",
                                    ["全部", "S级", "A级", "B级", "C级", "无效"],
                                    horizontal=True)

            for g in graded:
                lv = g["signal_level"]
                if level_filter != "全部" and lv != level_filter.replace("级", ""):
                    continue

                code = g.get("stock_code", "")
                name = g.get("name", "")
                sug = g.get("suggestion", "持有")
                conf = g.get("confidence", 0) or 0
                sent = g.get("avg_sentiment", 0) or 0
                risk = g.get("risk_level", "中")
                summary = (g.get("llm_analysis") or "")[:150]

                st.markdown(f"""
                <div class="signal-{lv}" style="margin:8px 0;">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-weight:700; font-size:16px;">[{lv}] {code} {name}</span>
                            <span style="margin-left:12px; color:{signal_color(lv)};">→ {sug}</span>
                        </div>
                        <div style="font-size:13px; color:#6b7280;">
                            置信度: {safe_format(conf*100, '{:.0f}')}%
                            | 情绪: <span style="color:{sentiment_color(sent)};">{safe_format(sent, '{:+.2f}')}</span>
                            | 风险: {risk}
                        </div>
                    </div>
                    <div style="margin-top:6px; font-size:13px; color:#9e9e9e;">{summary}</div>
                </div>
                """, unsafe_allow_html=True)

        except Exception as e:
            st.warning(f"信号加载失败: {e}")

    with tab_sig_hist:
        st.subheader("📊 历史信号统计")
        try:
            # 获取过去30天的分析记录
            hist_data = query_sql(
                db,
                "SELECT date, stock_code, avg_sentiment, confidence, suggestion, risk_level "
                "FROM analysis WHERE date >= date('now', '-30 days', 'localtime') "
                "ORDER BY date DESC"
            )
            if hist_data:
                df = pd.DataFrame(hist_data)
                df["signal_level"] = df.apply(
                    lambda r: signal_level(r.get("avg_sentiment", 0) or 0,
                                          r.get("confidence", 0) or 0),
                    axis=1,
                )
                df["date"] = pd.to_datetime(df["date"])

                # 每日信号分布
                daily_counts = df.groupby([df["date"].dt.date, "signal_level"]).size().unstack(fill_value=0)
                fig = go.Figure()
                for lv in ["S", "A", "B", "C", "无效"]:
                    if lv in daily_counts.columns:
                        fig.add_trace(go.Bar(
                            x=daily_counts.index, y=daily_counts[lv],
                            name=lv, marker_color=signal_color(lv),
                        ))
                fig.update_layout(
                    barmode="stack",
                    height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)

                # 表格
                st.dataframe(
                    df[["date", "stock_code", "avg_sentiment", "confidence",
                        "suggestion", "signal_level", "risk_level"]].head(100),
                    column_config={
                        "date": "日期", "stock_code": "代码",
                        "avg_sentiment": st.column_config.NumberColumn("情绪", format="%.2f"),
                        "confidence": st.column_config.NumberColumn("置信度", format="%.2f"),
                        "suggestion": "建议", "signal_level": "级别", "risk_level": "风险",
                    },
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("暂无历史信号数据。")
        except Exception as e:
            st.warning(f"历史信号加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面6: 📊 热点板块
# ═══════════════════════════════════════════════════════════

def page_hot_board():
    st.header("📊 热点与板块")

    tab_hot, tab_board = st.tabs(["🔥 个股热度", "📊 板块行情"])

    with tab_hot:
        st.subheader("🔥 个股热度排行")
        try:
            hot_data = query_sql(
                db,
                "SELECT * FROM stock_hot "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM stock_hot) "
                "ORDER BY hot_rank ASC LIMIT 50"
            )
            if hot_data:
                df = pd.DataFrame(hot_data)
                cols = ["hot_rank", "stock_code", "stock_name", "hot_score", "change_pct"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                df_d.columns = ["排名", "代码", "名称", "热度分", "涨幅%"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)

                # 热度前20条形图
                if "hot_score" in df.columns and "stock_name" in df.columns:
                    df_top = df.head(20)
                    fig = px.bar(
                        df_top.sort_values("hot_score"),
                        x="hot_score", y="stock_name", orientation="h",
                        color="change_pct" if "change_pct" in df_top.columns else "hot_score",
                        color_continuous_scale="Viridis_r",
                        title="热度 Top20",
                        labels={"hot_score": "热度分", "stock_name": ""},
                    )
                    fig.update_layout(
                        height=600, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无热度数据。")
        except Exception as e:
            st.warning(f"热度数据加载失败: {e}")

    with tab_board:
        st.subheader("📊 板块行情")
        try:
            board_data = query_sql(
                db,
                "SELECT * FROM board_index "
                "ORDER BY snapshot_time DESC LIMIT 30"
            )
            if board_data:
                df = pd.DataFrame(board_data)
                # 去重取最新
                if "board_name" in df.columns:
                    df = df.drop_duplicates(subset=["board_name"], keep="first")

                cols = ["board_name", "board_code", "change_pct"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                df_d.columns = ["板块名称", "代码", "涨跌幅%"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)

                if "change_pct" in df.columns and "board_name" in df.columns:
                    fig = px.bar(
                        df.sort_values("change_pct"),
                        x="change_pct", y="board_name", orientation="h",
                        color="change_pct",
                        color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                        title="板块涨跌幅",
                        labels={"change_pct": "涨跌幅%", "board_name": ""},
                    )
                    fig.update_layout(
                        height=500, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=30, b=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无板块行情数据。")
        except Exception as e:
            st.warning(f"板块行情加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面7: 🏛️ 政策宏观
# ═══════════════════════════════════════════════════════════

def page_policy_macro():
    st.header("🏛️ 政策与宏观")

    tab_pl, tab_mc, tab_by = st.tabs(["📜 政策新闻", "📊 宏观数据", "🏦 国债收益率"])

    with tab_pl:
        st.subheader("📜 最新政策新闻")
        try:
            pol_data = query_sql(
                db,
                "SELECT * FROM policies ORDER BY publish_date DESC LIMIT 30"
            )
            if pol_data:
                for p in pol_data:
                    related = p.get("related_sectors", "")
                    try:
                        sectors = json.loads(related) if isinstance(related, str) and related else []
                    except Exception:
                        sectors = []
                    sector_tags = "".join(
                        f"<span style='background:#2a3040; padding:2px 8px; border-radius:10px; font-size:11px; margin-right:4px;'>{s}</span>"
                        for s in (sectors if isinstance(sectors, list) else [str(sectors)])
                    ) if sectors else ""
                    render_info_card(
                        p.get("title", "无标题"),
                        f"📜 {p.get('source','未知')}  |  🏛️ {p.get('department','')}  |  🕐 {(p.get('publish_date') or '')[:10]}",
                        (p.get("summary") or "")[:300],
                        extra=f"<div style='margin-top:6px;'>{sector_tags}</div>" if sector_tags else "",
                    )
            else:
                st.info("暂无政策新闻数据。")
        except Exception as e:
            st.warning(f"政策新闻加载失败: {e}")

    with tab_mc:
        st.subheader("📊 宏观数据")
        try:
            mc_data = query_sql(
                db,
                "SELECT * FROM macro_data ORDER BY release_date DESC LIMIT 30"
            )
            if mc_data:
                df = pd.DataFrame(mc_data)
                cols = ["indicator", "value", "unit", "release_date", "source"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                df_d.columns = ["指标", "数值", "单位", "发布日期", "来源"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)

                # 按指标分组趋势
                if "indicator" in df.columns and "value" in df.columns:
                    indicators = df["indicator"].unique()
                    sel_ind = st.selectbox("选择指标查看趋势", indicators)
                    ind_data = df[df["indicator"] == sel_ind].copy()
                    if len(ind_data) > 1 and "release_date" in ind_data.columns:
                        ind_data["release_date"] = pd.to_datetime(ind_data["release_date"])
                        ind_data = ind_data.sort_values("release_date")
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=ind_data["release_date"], y=ind_data["value"],
                            mode="lines+markers", name=sel_ind,
                            line=dict(color="#00d4ff", width=2),
                            marker=dict(size=8),
                        ))
                        fig.update_layout(
                            height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                            font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无宏观数据。")
        except Exception as e:
            st.warning(f"宏观数据加载失败: {e}")

    with tab_by:
        st.subheader("🏦 国债收益率")
        try:
            by_data = query_sql(
                db,
                "SELECT * FROM bond_yield ORDER BY trade_date DESC LIMIT 50"
            )
            if by_data:
                df = pd.DataFrame(by_data)
                if "yield_name" in df.columns:
                    yield_names = df["yield_name"].unique()
                    sel_yield = st.selectbox("选择收益率曲线", yield_names)
                    y_data = df[df["yield_name"] == sel_yield].copy()
                    yield_chart_name = sel_yield
                else:
                    y_data = df
                    yield_chart_name = "收益率"

                if "trade_date" in y_data.columns:
                    y_data["trade_date"] = pd.to_datetime(y_data["trade_date"])
                    y_data = y_data.sort_values("trade_date")

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=y_data["trade_date"], y=y_data["yield_value"],
                        mode="lines+markers", name=yield_chart_name,
                        line=dict(color="#00d4ff", width=2),
                        marker=dict(size=6),
                        fill="tozeroy", fillcolor="rgba(0,212,255,0.1)",
                    ))
                    fig.update_layout(
                        height=400, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                        font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                        yaxis=dict(title="收益率(%)"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                cols = ["yield_name", "yield_value", "unit", "trade_date"]
                dc = [c for c in cols if c in df.columns]
                df_d = df[dc].copy()
                df_d.columns = ["名称", "收益率", "单位", "日期"]
                st.dataframe(df_d, use_container_width=True, hide_index=True)
            else:
                st.info("暂无国债收益率数据。")
        except Exception as e:
            st.warning(f"国债收益率加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面8: 📈 回测系统
# ═══════════════════════════════════════════════════════════

def page_backtest():
    st.header("📈 回测系统")

    try:
        stocks = db.load_stocks()
    except Exception:
        stocks = []

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        bt_days = st.selectbox("回测周期", [30, 60, 90, 180, 365], index=2, key="bt_days")
    with col2:
        stock_codes = ["全部股票"] + [s["code"] for s in stocks]
        sel_stock = st.selectbox("选择股票", stock_codes, key="bt_stock")
    with col3:
        st.write("")
        st.write("")
        run_btn = st.button("🚀 运行回测", use_container_width=True, key="bt_run")

    if run_btn:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=bt_days)).strftime("%Y-%m-%d")

        with st.spinner("正在运行回测..."):
            try:
                if sel_stock == "全部股票":
                    results = bt.run_all_backtest(start_date, end_date)
                else:
                    single = bt.run_backtest(sel_stock, start_date, end_date)
                    results = [single]
            except Exception as e:
                st.error(f"回测执行失败: {e}")
                results = []

        if results:
            st.subheader("📈 绩效指标")

            if sel_stock == "全部股票":
                try:
                    summary = bt.get_performance_summary()
                except Exception:
                    summary = {}

                k1, k2, k3, k4 = st.columns(4)
                with k1:
                    avg_ret = summary.get("avg_return", 0) or 0
                    pos = summary.get("positive_count", 0)
                    total = summary.get("total_stocks", 0)
                    render_metric(safe_format(avg_ret * 100, '{:.2f}%'),
                                  "📊 平均收益率", f"正收益: {pos}/{total}",
                                  color_pct(avg_ret))
                with k2:
                    wr = summary.get("avg_win_rate", 0) or 0
                    render_metric(safe_format(wr * 100, '{:.1f}%'), "🎯 平均胜率")
                with k3:
                    dd = summary.get("avg_max_drawdown", 0) or 0
                    render_metric(safe_format(dd * 100, '{:.2f}%'), "📉 平均最大回撤",
                                  val_class="negative")
                with k4:
                    sharpe = summary.get("avg_sharpe", 0) or 0
                    render_metric(safe_format(sharpe, '{:.2f}'), "📐 平均夏普比率")
            else:
                r = results[0]
                k1, k2, k3, k4 = st.columns(4)
                with k1:
                    tr = r.get("total_return", 0) or 0
                    render_metric(safe_format(tr * 100, '{:.2f}%'), "📊 总收益率",
                                  val_class=color_pct(tr))
                with k2:
                    ar = r.get("annual_return", 0) or 0
                    render_metric(safe_format(ar * 100, '{:.2f}%'), "📈 年化收益率",
                                  val_class=color_pct(ar))
                with k3:
                    wr = r.get("win_rate", 0) or 0
                    tc = r.get("trade_count", 0)
                    render_metric(safe_format(wr * 100, '{:.1f}%'), "🎯 胜率",
                                  f"交易次数: {tc}")
                with k4:
                    dd = r.get("max_drawdown", 0) or 0
                    sp = r.get("sharpe_ratio", 0) or 0
                    alpha = r.get("alpha", 0) or 0
                    render_metric(safe_format(dd * 100, '{:.2f}%'), "📉 最大回撤",
                                  f"夏普: {safe_format(sp, '{:.2f}')} | α: {safe_format(alpha, '{:.4f}')}",
                                  val_class="negative")

            # 收益曲线
            st.subheader("📈 收益曲线")
            fig = go.Figure()
            has_data = False
            for r in results:
                curve = r.get("equity_curve", [])
                if curve:
                    df_eq = pd.DataFrame(curve)
                    df_eq["date"] = pd.to_datetime(df_eq["date"])
                    label = r.get("stock_code", "unknown")
                    base = df_eq["capital"].iloc[0] if len(df_eq) > 0 else 1
                    df_eq["norm"] = df_eq["capital"] / base
                    fig.add_trace(go.Scatter(
                        x=df_eq["date"], y=df_eq["norm"],
                        mode="lines", name=label, line=dict(width=2),
                    ))
                    has_data = True

            if has_data:
                fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.5)
                fig.update_layout(
                    height=450, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                    yaxis_title="净值",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无收益曲线数据。")

            # 基准对比
            if sel_stock != "全部股票":
                st.subheader("🏆 基准对比 (沪深300)")
                try:
                    comparison = bt.compare_with_benchmark(sel_stock, start_date, end_date)
                    col_a, col_b = st.columns(2)
                    with col_a:
                        stock_ret = comparison.get("stock_return", 0) or 0
                        bm_ret = comparison.get("benchmark_return", 0) or 0
                        excess = comparison.get("excess_return", 0) or 0
                        st.markdown(f"""
                        <div style="background:#1a1f2e; border-radius:12px; padding:16px;">
                            <h4 style="color:#9e9e9e; margin:0 0 12px 0;">收益对比</h4>
                            <div style="display:flex; gap:16px; flex-wrap:wrap;">
                                <div><span style="color:#00d4ff;">策略:</span>
                                    <span class="{color_pct(stock_ret)}">{safe_format(stock_ret*100, '{:+.2f}')}%</span></div>
                                <div><span style="color:#78909c;">基准:</span>
                                    <span class="{color_pct(bm_ret)}">{safe_format(bm_ret*100, '{:+.2f}')}%</span></div>
                                <div><span style="color:#ffd600;">超额:</span>
                                    <span class="{color_pct(excess)}">{safe_format(excess*100, '{:+.2f}')}%</span></div>
                            </div>
                            <div style="margin-top:8px; color:#6b7280; font-size:12px;">
                            α: {safe_format(comparison.get('alpha', 0), '{:.4f}')}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col_b:
                        fig2 = go.Figure()
                        s_curve = comparison.get("stock_equity_curve", [])
                        b_curve = comparison.get("benchmark_equity_curve", [])
                        if s_curve:
                            df_s = pd.DataFrame(s_curve)
                            df_s["date"] = pd.to_datetime(df_s["date"])
                            base_s = df_s["capital"].iloc[0] if len(df_s) > 0 else 1
                            fig2.add_trace(go.Scatter(
                                x=df_s["date"], y=df_s["capital"] / base_s,
                                mode="lines", name="策略", line=dict(color="#00d4ff", width=2),
                            ))
                        if b_curve:
                            df_b = pd.DataFrame(b_curve)
                            df_b["date"] = pd.to_datetime(df_b["date"])
                            fig2.add_trace(go.Scatter(
                                x=df_b["date"], y=df_b["capital"],
                                mode="lines", name="沪深300",
                                line=dict(color="#ff6d00", width=2, dash="dash"),
                            ))
                        fig2.update_layout(
                            height=250, margin=dict(l=0, r=0, t=0, b=0),
                            plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                            font_color="#fafafa", showlegend=True,
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                except Exception as e:
                    st.warning(f"基准对比失败: {e}")

            # 逐笔交易
            st.subheader("📋 逐笔交易记录")
            all_trades = []
            for r in results:
                trades = r.get("trades", [])
                for t in trades:
                    t["stock_code"] = r.get("stock_code", "")
                    all_trades.append(t)
            if all_trades:
                df_trades = pd.DataFrame(all_trades)
                st.dataframe(
                    df_trades,
                    column_config={
                        "stock_code": "股票", "date": "日期", "type": "类型",
                        "price": st.column_config.NumberColumn("价格", format="%.2f"),
                        "position": st.column_config.NumberColumn("仓位", format="%.2f"),
                        "capital": st.column_config.NumberColumn("资产", format="%.4f"),
                    },
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("暂无交易记录。")
        else:
            st.info("👆 选择回测周期和股票，点击「运行回测」查看结果。")

    st.divider()

    # 历史回测结果
    st.subheader("📁 历史回测结果")
    try:
        saved = db.get_backtest_results()
        if saved:
            df_saved = pd.DataFrame(saved)
            dc = ["stock_code", "start_date", "end_date", "total_return",
                  "max_drawdown", "win_rate", "trade_count", "sharpe_ratio"]
            dc = [c for c in dc if c in df_saved.columns]
            df_d = df_saved[dc].copy()
            st.dataframe(df_d, use_container_width=True, hide_index=True)
        else:
            st.info("暂无历史回测记录。")
    except Exception as e:
        st.warning(f"历史回测加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面9: ⚙️ 系统管理
# ═══════════════════════════════════════════════════════════

def page_system():
    st.header("⚙️ 系统管理")

    try:
        stats = db.get_stats()
    except Exception:
        stats = {}

    tab_db, tab_src, tab_op, tab_log = st.tabs([
        "💾 数据库", "📡 采集器", "🔨 操作", "📋 日志"])

    # ─── TAB 1: 数据库 ───
    with tab_db:
        st.subheader("💾 数据库状态")
        col1, col2, col3 = st.columns(3)
        with col1:
            render_metric(stats.get('total_news', 0), "📰 新闻",
                          f"今日: {stats.get('today_news', 0)}")
            render_metric(stats.get('total_announcements', 0), "📋 公告")
        with col2:
            render_metric(stats.get('total_stocks', 0), "🏢 自选股")
            render_metric(stats.get('total_analysis', 0), "📊 分析")
        with col3:
            render_metric(stats.get('total_policies', 0), "📜 政策")
            render_metric(safe_format(stats.get('db_size_mb', 0), '{:.1f}') + " MB", "💾 大小")

        st.divider()

        # 处理统计
        st.subheader("🔧 处理状态")
        col_a, col_b = st.columns(2)
        with col_a:
            processed = stats.get("processed_news", 0)
            unprocessed = stats.get("unprocessed_news", 0)
            total_n = processed + unprocessed or 1
            fig = go.Figure(go.Pie(
                values=[processed, unprocessed],
                labels=["已处理", "未处理"],
                marker_colors=["#00c853", "#78909c"],
                hole=0.4,
            ))
            fig.update_layout(
                height=250, plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="#fafafa", margin=dict(l=0, r=0, t=0, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            render_metric(f"{processed/total_n*100:.1f}%", "处理率",
                          f"已处理: {processed} / 总计: {total_n}")
            render_metric(stats.get('oldest_news_date', '--') or '--', "最早新闻")
            render_metric(stats.get('newest_news_date', '--') or '--', "最新新闻")

    # ─── TAB 2: 采集器 ───
    with tab_src:
        st.subheader("📡 采集器健康状态")
        try:
            health = db.get_fetch_health()
            if health:
                df_h = pd.DataFrame(health)
                st.dataframe(df_h, use_container_width=True, hide_index=True)

                # 状态卡片
                ok_count = sum(1 for h in health if h.get("status") == "ok")
                warn_count = sum(1 for h in health if h.get("status") == "warning")
                err_count = sum(1 for h in health if h.get("status") == "error")

                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1: render_metric(ok_count, "✅ 正常")
                with col_s2: render_metric(warn_count, "⚠️ 警告")
                with col_s3: render_metric(err_count, "❌ 错误")
            else:
                st.info("暂无采集器健康数据。")
        except Exception as e:
            st.warning(f"采集器状态加载失败: {e}")

    # ─── TAB 3: 操作 ───
    with tab_op:
        st.subheader("🔨 手动操作")
        op1, op2, op3 = st.columns(3)
        with op1:
            if st.button("📰 采集新闻", use_container_width=True):
                st.info("请在终端运行: python main.py collect")
        with op2:
            if st.button("🤖 运行分析", use_container_width=True):
                st.info("请在终端运行: python main.py analyze")
                try:
                    count = bt.record_today_signals()
                    st.success(f"已记录 {count} 条回测信号")
                except Exception as e:
                    st.error(f"记录信号失败: {e}")
        with op3:
            if st.button("🗄️ 归档旧数据", use_container_width=True):
                try:
                    result = db.archive_old_data(days=30)
                    st.success(f"已归档: 新闻 {result.get('news', 0)} 条, 公告 {result.get('announcements', 0)} 条")
                except Exception as e:
                    st.error(f"归档失败: {e}")

        st.divider()
        op_a, op_b = st.columns(2)
        with op_a:
            if st.button("🔄 重建FTS索引", use_container_width=True):
                try:
                    db.create_fts_index()
                    st.success("FTS索引创建/更新成功")
                except Exception as e:
                    st.error(f"FTS索引创建失败: {e}")
        with op_b:
            if st.button("📊 刷新统计缓存", use_container_width=True):
                st.cache_data.clear()
                st.success("缓存已清除，将在下次访问时刷新")

    # ─── TAB 4: 日志 ───
    with tab_log:
        st.subheader("📋 最近采集日志")
        try:
            logs = query_sql(
                db,
                "SELECT * FROM collect_logs ORDER BY finished_at DESC LIMIT 50"
            )
            if logs:
                df_logs = pd.DataFrame(logs)
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
            else:
                st.info("暂无采集日志。")
        except Exception as e:
            st.warning(f"采集日志加载失败: {e}")

        st.divider()
        st.subheader("📂 新闻分类统计")
        try:
            categories = stats.get("categories", {})
            if categories:
                df_cat = pd.DataFrame(list(categories.items()), columns=["分类", "数量"])
                fig = px.bar(
                    df_cat, x="分类", y="数量",
                    color="数量", color_continuous_scale="Viridis",
                )
                fig.update_layout(
                    height=300, plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                    font_color="#fafafa", margin=dict(l=0, r=0, t=20, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无分类数据。")
        except Exception as e:
            st.warning(f"分类统计加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 路由
# ═══════════════════════════════════════════════════════════

page_map = {
    "🏠 市场总览": page_market_overview,
    "🔍 个股分析": page_stock_detail,
    "💰 资金情绪": page_money_flow,
    "🐉 龙虎榜": page_dragon_tiger,
    "📡 信号看板": page_signal_board,
    "📊 热点板块": page_hot_board,
    "🏛️ 政策宏观": page_policy_macro,
    "📈 回测系统": page_backtest,
    "⚙️ 系统管理": page_system,
}

if page in page_map:
    page_map[page]()
