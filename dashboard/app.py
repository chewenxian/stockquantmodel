"""
Streamlit 可视化看板 — 股票情报分析系统

多页面应用：
1. 市场总览 — 今日情绪、数据源分布、热点新闻、自选股影响
2. 个股深度分析 — K线、情绪趋势、AI分析、操作建议
3. 回测报告 — 总收益曲线、胜率/最大回撤/夏普、逐笔交易、基准对比
4. 系统管理 — 数据源状态、数据库统计、手动触发

启动： streamlit run dashboard/app.py
"""
import sys
import os
from datetime import datetime, timedelta

# 将项目根目录加入路径
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# 项目模块
from storage.database import Database
from analyzer.backtest import BacktestEngine

# ═══════════════════════════════════════════════════════════
# 页面配置
# ═══════════════════════════════════════════════════════════

st.set_page_config(
    page_title="股票情报分析系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 深色主题的 CSS
st.markdown("""
<style>
    /* 深色主题 */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    .stTabs [data-baseweb="tab"] {
        color: #9e9e9e;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        color: #00d4ff;
    }
    /* 指标卡片样式 */
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #1e2538 100%);
        border: 1px solid #2a3040;
        border-radius: 12px;
        padding: 20px;
        margin: 8px 0;
    }
    .metric-card h3 {
        color: #9e9e9e;
        font-size: 14px;
        margin: 0 0 8px 0;
    }
    .metric-card .value {
        color: #fafafa;
        font-size: 28px;
        font-weight: 700;
    }
    .metric-card .sub {
        color: #6b7280;
        font-size: 12px;
        margin-top: 4px;
    }
    .positive { color: #00c853 !important; }
    .negative { color: #ff1744 !important; }
    .neutral { color: #ffd600 !important; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# 初始化
# ═══════════════════════════════════════════════════════════

@st.cache_resource
def get_db():
    """获取数据库实例（缓存避免重复连接）"""
    # 尝试从项目配置获取数据库路径
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
    """安全格式化数值，处理 None"""
    if val is None:
        return "--"
    try:
        return fmt.format(val)
    except (ValueError, TypeError):
        return str(val)


def color_pct(val):
    """根据正负值返回CSS class"""
    if val is None:
        return ""
    return "positive" if val >= 0 else "negative"


db = get_db()
bt = get_backtest_engine()

# ═══════════════════════════════════════════════════════════
# 侧边栏
# ═══════════════════════════════════════════════════════════

with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/stock.png", width=64)
    st.title("📈 股票情报分析")

    page = st.radio(
        "导航",
        ["市场总览", "个股深度分析", "回测报告", "系统管理"],
        label_visibility="collapsed",
    )

    st.divider()
    st.caption(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.caption("⚡ 数据源: SQLite")

    # 刷新按钮
    if st.button("🔄 刷新数据", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════════
# 页面1: 市场总览
# ═══════════════════════════════════════════════════════════

def page_market_overview():
    st.header("🌍 市场总览")

    try:
        stats = db.get_stats()
    except Exception:
        stats = {}

    # ▸ 统计 KPI 行
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📰 新闻总数</h3>
            <div class="value">{stats.get('total_news', 0)}</div>
            <div class="sub">今日新增: {stats.get('today_news', 0)}</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🏢 自选股</h3>
            <div class="value">{stats.get('total_stocks', 0)}</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📊 分析记录</h3>
            <div class="value">{stats.get('total_analysis', 0)}</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        db_size = stats.get('db_size_mb', 0)
        st.markdown(f"""
        <div class="metric-card">
            <h3>💾 数据库</h3>
            <div class="value">{safe_format(db_size, '{:.1f}')} MB</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ▸ 两列布局：情绪仪表盘 + 数据源分布
    left_col, right_col = st.columns([3, 2])

    with left_col:
        st.subheader("今日情绪仪表盘")
        try:
            analyses = db.get_today_analysis()
            if analyses:
                df = pd.DataFrame(analyses)
                df["stock_label"] = df.apply(
                    lambda r: f"{r.get('stock_code', '')} {r.get('name', '')}", axis=1
                )
                # 得分范围归一化
                df["sentiment_display"] = df["avg_sentiment"].fillna(0).clip(-1, 1)

                fig = px.bar(
                    df.sort_values("sentiment_display"),
                    x="sentiment_display",
                    y="stock_label",
                    color="sentiment_display",
                    color_continuous_scale=["#ff1744", "#ffd600", "#00c853"],
                    range_color=[-1, 1],
                    title="个股情绪评分",
                    labels={"sentiment_display": "情绪得分", "stock_label": "股票"},
                )
                fig.update_layout(
                    height=400,
                    plot_bgcolor="#1a1f2e",
                    paper_bgcolor="#0e1117",
                    font_color="#fafafa",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无今日分析数据，请先运行采集和分析任务。")
        except Exception as e:
            st.warning(f"情绪数据加载失败: {e}")

    with right_col:
        st.subheader("📡 数据源分布")
        try:
            sources = stats.get("sources", {})
            if sources:
                df_src = pd.DataFrame(
                    list(sources.items()), columns=["source", "count"]
                )
                fig = px.pie(
                    df_src,
                    values="count",
                    names="source",
                    title="新闻来源分布",
                    color_discrete_sequence=px.colors.sequential.Viridis_r,
                )
                fig.update_layout(
                    height=400,
                    plot_bgcolor="#0e1117",
                    paper_bgcolor="#0e1117",
                    font_color="#fafafa",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据源统计。")
        except Exception as e:
            st.warning(f"数据源分布加载失败: {e}")

    st.divider()

    # ▸ 热点新闻列表
    st.subheader("🔥 热点新闻")
    try:
        news_list = db.search_news("", limit=20) if hasattr(db, 'search_news') else []
        if not news_list:
            # fallback: 直接查询最新新闻
            news_list = db.get_recent_analysis("", limit=10) if hasattr(db, 'get_recent_analysis') else []

        # 更直接的查询
        import sqlite3
        conn = db._connect()
        try:
            rows = conn.execute(
                "SELECT title, source, published_at, summary FROM news WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 20"
            ).fetchall()
            db._close(conn)
            news_list = [dict(r) for r in rows]
        except Exception:
            db._close(conn)
            news_list = []

        if news_list:
            for news in news_list:
                title = news.get("title", "无标题")
                source = news.get("source", "未知")
                pub_at = news.get("published_at", "")
                summary = news.get("summary", "") or ""
                st.markdown(f"""
                <div style="background:#1a1f2e; border-radius:8px; padding:12px; margin:4px 0;">
                    <div style="font-size:14px; font-weight:600;">{title}</div>
                    <div style="font-size:12px; color:#6b7280; margin-top:4px;">
                        📰 {source} &nbsp;|&nbsp; 🕐 {pub_at or "未知"}
                    </div>
                    <div style="font-size:13px; color:#9e9e9e; margin-top:6px;">{summary[:200]}{'...' if len(summary) > 200 else ''}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("暂无新闻数据。")
    except Exception as e:
        st.warning(f"新闻列表加载失败: {e}")

    st.divider()

    # ▸ 自选股影响评估
    st.subheader("📋 今日自选股影响一览")
    try:
        analyses = db.get_today_analysis()
        if analyses:
            data_rows = []
            for a in analyses:
                sentiment = a.get("avg_sentiment", 0) or 0
                confidence = a.get("confidence", 0) or 0
                suggestion = a.get("suggestion", "持有")

                # 影响评估
                if sentiment >= 0.5:
                    impact = "🟢 利好"
                elif sentiment <= -0.5:
                    impact = "🔴 利空"
                elif sentiment >= 0.1:
                    impact = "🟡 偏多"
                elif sentiment <= -0.1:
                    impact = "🟠 偏空"
                else:
                    impact = "⚪ 中性"

                data_rows.append({
                    "股票": f"{a.get('stock_code', '')} {a.get('name', '')}",
                    "情绪得分": sentiment,
                    "置信度": confidence,
                    "影响评估": impact,
                    "建议": suggestion,
                    "新闻数": a.get("news_count", 0),
                })

            df_impact = pd.DataFrame(data_rows)
            st.dataframe(
                df_impact,
                column_config={
                    "股票": st.column_config.TextColumn("股票"),
                    "情绪得分": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.NumberColumn(format="%.2f"),
                    "影响评估": st.column_config.TextColumn("影响评估"),
                    "建议": st.column_config.TextColumn("建议"),
                    "新闻数": st.column_config.NumberColumn("新闻数"),
                },
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("暂无今日分析数据。")
    except Exception as e:
        st.warning(f"影响评估加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面2: 个股深度分析
# ═══════════════════════════════════════════════════════════

def page_stock_detail():
    st.header("🔍 个股深度分析")

    # 获取股票列表
    try:
        stocks = db.load_stocks()
    except Exception:
        stocks = []

    if not stocks:
        st.warning("暂无自选股数据，请先采集。")
        return

    # 股票选择器
    stock_options = {f"{s['code']} {s['name']}": s["code"] for s in stocks}
    selected = st.selectbox("选择股票", list(stock_options.keys()))
    stock_code = stock_options[selected]

    # 分析天数
    days = st.slider("历史分析天数", 7, 90, 30)

    # ▸ K线图（简化 — 用 Plotly 绘制）
    st.subheader("📊 价格走势")

    try:
        price_data = db.get_price_history(stock_code)
        if price_data:
            df_price = pd.DataFrame(price_data)
            # 按日期聚合取每日平均价格
            df_price["trade_date"] = pd.to_datetime(df_price["trade_date"])
            df_price = df_price.sort_values("trade_date")

            fig = go.Figure()

            # K线（简化：用折线替代完整K线，因为数据是快照聚合）
            fig.add_trace(go.Scatter(
                x=df_price["trade_date"],
                y=df_price["price"],
                mode="lines+markers",
                name="收盘价",
                line=dict(color="#00d4ff", width=2),
                marker=dict(size=4, color="#00d4ff"),
            ))

            # 均线（MA5, MA20）
            if len(df_price) >= 5:
                ma5 = df_price["price"].rolling(5).mean()
                fig.add_trace(go.Scatter(
                    x=df_price["trade_date"],
                    y=ma5,
                    mode="lines",
                    name="MA5",
                    line=dict(color="#ffd600", width=1.5, dash="dash"),
                ))
            if len(df_price) >= 20:
                ma20 = df_price["price"].rolling(20).mean()
                fig.add_trace(go.Scatter(
                    x=df_price["trade_date"],
                    y=ma20,
                    mode="lines",
                    name="MA20",
                    line=dict(color="#ff6d00", width=1.5, dash="dash"),
                ))

            # 布林带（简化 — 使用 MA20 ± 2σ）
            if len(df_price) >= 20:
                ma = df_price["price"].rolling(20).mean()
                std = df_price["price"].rolling(20).std()
                upper = ma + 2 * std
                lower = ma - 2 * std
                fig.add_trace(go.Scatter(
                    x=df_price["trade_date"],
                    y=upper,
                    mode="lines",
                    name="布林上轨",
                    line=dict(color="rgba(0, 200, 83, 0.3)", width=1),
                    showlegend=True,
                ))
                fig.add_trace(go.Scatter(
                    x=df_price["trade_date"],
                    y=lower,
                    mode="lines",
                    name="布林下轨",
                    line=dict(color="rgba(255, 23, 68, 0.3)", width=1),
                    fill="tonexty",
                    fillcolor="rgba(128, 128, 128, 0.05)",
                    showlegend=True,
                ))

            fig.update_layout(
                height=500,
                plot_bgcolor="#1a1f2e",
                paper_bgcolor="#0e1117",
                font_color="#fafafa",
                margin=dict(l=0, r=0, t=20, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                xaxis_title=None,
                yaxis_title="价格",
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"{stock_code} 暂无行情数据。")
    except Exception as e:
        st.warning(f"行情数据加载失败: {e}")

    st.divider()

    # ▸ 情绪趋势图
    st.subheader("💭 情绪趋势")

    try:
        recent = db.get_recent_analysis(stock_code, limit=days)
        if recent:
            df_sent = pd.DataFrame(recent)
            df_sent["date"] = pd.to_datetime(df_sent["date"])
            df_sent = df_sent.sort_values("date")

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_sent["date"],
                y=df_sent["avg_sentiment"],
                name="情绪得分",
                marker_color=df_sent["avg_sentiment"].apply(
                    lambda x: "#00c853" if (x or 0) >= 0 else "#ff1744"
                ),
            ))

            # 置信度折线
            fig.add_trace(go.Scatter(
                x=df_sent["date"],
                y=df_sent["confidence"],
                mode="lines+markers",
                name="置信度",
                line=dict(color="#ffd600", width=2),
                marker=dict(size=6),
                yaxis="y2",
            ))

            fig.update_layout(
                height=350,
                plot_bgcolor="#1a1f2e",
                paper_bgcolor="#0e1117",
                font_color="#fafafa",
                margin=dict(l=0, r=0, t=20, b=0),
                yaxis=dict(title="情绪得分", range=[-1.2, 1.2]),
                yaxis2=dict(
                    title="置信度",
                    overlaying="y",
                    side="right",
                    range=[0, 1.2],
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"{stock_code} 暂无情绪分析记录。")
    except Exception as e:
        st.warning(f"情绪趋势加载失败: {e}")

    st.divider()

    # ▸ AI 分析报告
    st.subheader("🤖 AI 分析报告")

    try:
        recent = db.get_recent_analysis(stock_code, limit=1)
        if recent:
            a = recent[0]
            llm_text = a.get("llm_analysis", "") or "暂无分析内容"
            suggestion = a.get("suggestion", "持有")
            confidence = a.get("confidence", 0) or 0
            risk = a.get("risk_level", "中")

            # 建议颜色
            sug_color = {
                "买入": "#00c853", "强烈买入": "#00e676", "强烈关注": "#00e676",
                "持有": "#ffd600", "关注": "#69f0ae",
                "观望": "#78909c", "卖出": "#ff1744", "强烈卖出": "#d50000",
                "回避": "#ff1744", "强烈回避": "#d50000",
            }
            s_color = "#78909c"
            for k, v in sug_color.items():
                if k in suggestion:
                    s_color = v
                    break

            risk_color = {"低": "#00c853", "中": "#ffd600", "高": "#ff1744"}
            r_color = risk_color.get(risk, "#78909c")

            st.markdown(f"""
            <div style="background:#1a1f2e; border-radius:12px; padding:20px; margin:8px 0;">
                <div style="display:flex; gap:16px; flex-wrap:wrap; margin-bottom:16px;">
                    <div style="background:#2a3040; border-radius:8px; padding:12px; min-width:100px; text-align:center;">
                        <div style="color:#6b7280; font-size:12px;">建议</div>
                        <div style="color:{s_color}; font-size:20px; font-weight:700;">{suggestion}</div>
                    </div>
                    <div style="background:#2a3040; border-radius:8px; padding:12px; min-width:100px; text-align:center;">
                        <div style="color:#6b7280; font-size:12px;">置信度</div>
                        <div style="color:#fafafa; font-size:20px; font-weight:700;">{safe_format(confidence*100, '{:.0f}')}%</div>
                    </div>
                    <div style="background:#2a3040; border-radius:8px; padding:12px; min-width:100px; text-align:center;">
                        <div style="color:#6b7280; font-size:12px;">风险等级</div>
                        <div style="color:{r_color}; font-size:20px; font-weight:700;">{risk}</div>
                    </div>
                    <div style="background:#2a3040; border-radius:8px; padding:12px; min-width:100px; text-align:center;">
                        <div style="color:#6b7280; font-size:12px;">情绪得分</div>
                        <div style="color:#fafafa; font-size:20px; font-weight:700;">{safe_format(a.get('avg_sentiment', 0), '{:.2f}')}</div>
                    </div>
                </div>
                <div style="color:#e0e0e0; font-size:14px; line-height:1.7; white-space:pre-wrap;">{llm_text}</div>
            </div>
            """, unsafe_allow_html=True)

            # 关键主题
            key_topics_raw = a.get("key_topics", "[]")
            try:
                import json
                topics = json.loads(key_topics_raw) if isinstance(key_topics_raw, str) else (key_topics_raw or [])
                if topics:
                    st.markdown("**🏷️ 关键主题:**")
                    cols = st.columns(len(topics))
                    for i, topic in enumerate(topics):
                        with cols[i % len(cols)]:
                            st.markdown(f"<span style='background:#2a3040; padding:4px 10px; border-radius:12px; font-size:12px;'>{topic}</span>", unsafe_allow_html=True)
            except Exception:
                pass
        else:
            st.info(f"{stock_code} 暂无 AI 分析记录。")
    except Exception as e:
        st.warning(f"AI 分析加载失败: {e}")

    st.divider()

    # ▸ 推理链（从分析结果中提取）
    st.subheader("🔗 推理链")
    try:
        recent = db.get_recent_analysis(stock_code, limit=3)
        if recent:
            for a in recent:
                llm_text = a.get("llm_analysis", "") or ""
                date_str = a.get("date", "")
                st.markdown(f"""
                <div style="background:#1a1f2e; border-radius:8px; padding:12px; margin:4px 0;">
                    <div style="font-size:12px; color:#6b7280;">📅 {date_str}</div>
                    <div style="font-size:13px; color:#e0e0e0; margin-top:4px;">{llm_text[:300]}{'...' if len(llm_text) > 300 else ''}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("暂无推理链数据。")
    except Exception as e:
        st.warning(f"推理链加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面3: 回测报告
# ═══════════════════════════════════════════════════════════

def page_backtest_report():
    st.header("📊 回测报告")

    stocks = []
    try:
        stocks = db.load_stocks()
    except Exception:
        pass

    # ▸ 选择回测范围和股票
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        backtest_days = st.selectbox("回测周期", [30, 60, 90, 180, 365], index=2)
    with col2:
        stock_codes = ["全部股票"] + [s["code"] for s in stocks]
        sel_stock = st.selectbox("选择股票", stock_codes)
    with col3:
        st.write("")
        st.write("")
        run_btn = st.button("🚀 运行回测", use_container_width=True)

    if run_btn:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=backtest_days)).strftime("%Y-%m-%d")

        with st.spinner("正在运行回测..."):
            if sel_stock == "全部股票":
                results = bt.run_all_backtest(start_date, end_date)
            else:
                single = bt.run_backtest(sel_stock, start_date, end_date)
                results = [single]

        if results:
            # ▸ 绩效卡片
            st.subheader("📈 绩效指标")

            if sel_stock == "全部股票":
                summary = bt.get_performance_summary()

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    avg_ret = summary.get("avg_return", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📊 平均收益率</h3>
                        <div class="value {color_pct(avg_ret)}">{safe_format(avg_ret*100, '{:.2f}')}%</div>
                        <div class="sub">正收益: {summary.get('positive_count', 0)}/{summary.get('total_stocks', 0)}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    win_rate = summary.get("avg_win_rate", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>🎯 平均胜率</h3>
                        <div class="value">{safe_format(win_rate*100, '{:.1f}')}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    dd = summary.get("avg_max_drawdown", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📉 平均最大回撤</h3>
                        <div class="value negative">{safe_format(dd*100, '{:.2f}')}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col4:
                    sharpe = summary.get("avg_sharpe", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📐 平均夏普比率</h3>
                        <div class="value">{safe_format(sharpe, '{:.2f}')}</div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                # 单只股票详细指标
                r = results[0]
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    tr = r.get("total_return", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📊 总收益率</h3>
                        <div class="value {color_pct(tr)}">{safe_format(tr*100, '{:.2f}')}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    ar = r.get("annual_return", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📊 年化收益率</h3>
                        <div class="value {color_pct(ar)}">{safe_format(ar*100, '{:.2f}')}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    wr = r.get("win_rate", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>🎯 胜率</h3>
                        <div class="value">{safe_format(wr*100, '{:.1f}')}%</div>
                        <div class="sub">交易次数: {r.get('trade_count', 0)}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col4:
                    dd = r.get("max_drawdown", 0) or 0
                    sharpe = r.get("sharpe_ratio", 0) or 0
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📉 最大回撤</h3>
                        <div class="value negative">{safe_format(dd*100, '{:.2f}')}%</div>
                        <div class="sub">夏普: {safe_format(sharpe, '{:.2f}')}</div>
                    </div>
                    """, unsafe_allow_html=True)

            # ▸ 总收益曲线（多只股票的对比）
            st.subheader("📈 收益曲线")
            fig = go.Figure()
            has_data = False
            for r in results:
                curve = r.get("equity_curve", [])
                if curve:
                    df_eq = pd.DataFrame(curve)
                    df_eq["date"] = pd.to_datetime(df_eq["date"])
                    label = r.get("stock_code", "unknown")
                    # 归一化
                    base = df_eq["capital"].iloc[0] if len(df_eq) > 0 else 1
                    df_eq["norm"] = df_eq["capital"] / base
                    fig.add_trace(go.Scatter(
                        x=df_eq["date"],
                        y=df_eq["norm"],
                        mode="lines",
                        name=label,
                        line=dict(width=2),
                    ))
                    has_data = True

            if has_data:
                fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.5)
                fig.update_layout(
                    height=450,
                    plot_bgcolor="#1a1f2e",
                    paper_bgcolor="#0e1117",
                    font_color="#fafafa",
                    margin=dict(l=0, r=0, t=20, b=0),
                    yaxis_title="净值",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无收益曲线数据。")

            # ▸ 基准对比（单只股票时显示）
            if sel_stock != "全部股票":
                st.subheader("🏆 与基准对比")
                try:
                    comparison = bt.compare_with_benchmark(sel_stock, start_date, end_date)
                    col1, col2 = st.columns(2)
                    with col1:
                        stock_ret = comparison.get("stock_return", 0) or 0
                        bm_ret = comparison.get("benchmark_return", 0) or 0
                        excess = comparison.get("excess_return", 0) or 0
                        st.markdown(f"""
                        <div style="background:#1a1f2e; border-radius:12px; padding:16px;">
                            <h3 style="color:#9e9e9e; font-size:14px; margin:0 0 12px 0;">📊 收益对比</h3>
                            <div style="display:flex; gap:20px;">
                                <div><span style="color:#00d4ff;">策略:</span> <span class="{color_pct(stock_ret)}">{safe_format(stock_ret*100, '{:.2f}')}%</span></div>
                                <div><span style="color:#78909c;">基准:</span> <span class="{color_pct(bm_ret)}">{safe_format(bm_ret*100, '{:.2f}')}%</span></div>
                                <div><span style="color:#ffd600;">超额:</span> <span class="{color_pct(excess)}">{safe_format(excess*100, '{:.2f}')}%</span></div>
                            </div>
                            <div style="margin-top:8px; color:#6b7280; font-size:12px;">阿尔法: {safe_format(comparison.get('alpha', 0), '{:.4f}')}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col2:
                        # 对比曲线
                        fig2 = go.Figure()
                        stock_curve = comparison.get("stock_equity_curve", [])
                        bm_curve = comparison.get("benchmark_equity_curve", [])

                        if stock_curve:
                            df_s = pd.DataFrame(stock_curve)
                            df_s["date"] = pd.to_datetime(df_s["date"])
                            base_s = df_s["capital"].iloc[0] if len(df_s) > 0 else 1
                            fig2.add_trace(go.Scatter(
                                x=df_s["date"], y=df_s["capital"] / base_s,
                                mode="lines", name="策略", line=dict(color="#00d4ff", width=2),
                            ))
                        if bm_curve:
                            df_b = pd.DataFrame(bm_curve)
                            df_b["date"] = pd.to_datetime(df_b["date"])
                            fig2.add_trace(go.Scatter(
                                x=df_b["date"], y=df_b["capital"],
                                mode="lines", name="沪深300", line=dict(color="#ff6d00", width=2, dash="dash"),
                            ))
                        fig2.update_layout(
                            height=250, margin=dict(l=0, r=0, t=0, b=0),
                            plot_bgcolor="#1a1f2e", paper_bgcolor="#0e1117",
                            font_color="#fafafa", showlegend=True,
                        )
                        st.plotly_chart(fig2, use_container_width=True)

                except Exception as e:
                    st.warning(f"基准对比失败: {e}")

            # ▸ 逐笔交易列表
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
                        "stock_code": "股票",
                        "date": "日期",
                        "type": "类型",
                        "price": st.column_config.NumberColumn("价格", format="%.2f"),
                        "position": st.column_config.NumberColumn("仓位", format="%.2f"),
                        "capital": st.column_config.NumberColumn("资产", format="%.4f"),
                    },
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("暂无交易记录。")
    else:
        st.info("👆 选择回测周期和股票，点击「运行回测」查看结果。")

    # ▸ 显示已保存的回测结果
    st.divider()
    st.subheader("📁 历史回测结果")
    try:
        saved = db.get_backtest_results()
        if saved:
            df_saved = pd.DataFrame(saved)
            # 只显示关键列
            display_cols = ["stock_code", "start_date", "end_date", "total_return",
                            "max_drawdown", "win_rate", "trade_count", "sharpe_ratio"]
            display = {k: v for k, v in df_saved.items() if k in display_cols}
            if display:
                df_display = pd.DataFrame(display)
                st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("暂无历史回测记录。")
    except Exception as e:
        st.warning(f"历史回测加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 页面4: 系统管理
# ═══════════════════════════════════════════════════════════

def page_system():
    st.header("⚙️ 系统管理")

    # ▸ 数据库概览
    st.subheader("💾 数据库状态")

    try:
        stats = db.get_stats()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>📚 表统计</h3>
                <div style="font-size:13px; line-height:1.8;">
                    <div>新闻: <b>{stats.get('total_news', 0)}</b></div>
                    <div>公告: <b>{stats.get('total_announcements', 0)}</b></div>
                    <div>分析: <b>{stats.get('total_analysis', 0)}</b></div>
                    <div>政策: <b>{stats.get('total_policies', 0)}</b></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>📅 日期范围</h3>
                <div style="font-size:13px; line-height:1.8;">
                    <div>最早新闻: <b>{stats.get('oldest_news_date', '--')}</b></div>
                    <div>最新新闻: <b>{stats.get('newest_news_date', '--')}</b></div>
                    <div>今日新增: <b>{stats.get('today_news', 0)}</b></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <h3>🔧 处理状态</h3>
                <div style="font-size:13px; line-height:1.8;">
                    <div>已处理: <b>{stats.get('processed_news', 0)}</b></div>
                    <div>未处理: <b>{stats.get('unprocessed_news', 0)}</b></div>
                    <div>数据库大小: <b>{safe_format(stats.get('db_size_mb', 0), '{:.1f}')} MB</b></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"数据库统计加载失败: {e}")

    st.divider()

    # ▸ 数据源状态
    st.subheader("📡 数据源状态")

    sources = {
        "东方财富": {"status": "✅ 正常", "type": "新闻/行情/资金"},
        "新浪财经": {"status": "✅ 正常", "type": "行情/新闻"},
        "雪球": {"status": "✅ 正常", "type": "社交情绪"},
        "巨潮资讯": {"status": "✅ 正常", "type": "公司公告"},
        "财联社": {"status": "✅ 正常", "type": "政策快讯"},
        "上交所/深交所/北交所": {"status": "✅ 正常", "type": "交易所公告"},
    }

    try:
        src_stats = stats.get("sources", {})
    except Exception:
        src_stats = {}

    source_data = []
    for name, info in sources.items():
        count = src_stats.get(name, 0) if isinstance(src_stats, dict) else 0
        source_data.append({
            "数据源": name,
            "状态": info["status"],
            "数据类型": info["type"],
            "采集条数": count or "--",
        })

    df_sources = pd.DataFrame(source_data)
    st.dataframe(df_sources, use_container_width=True, hide_index=True)

    st.divider()

    # ▸ 手动触发
    st.subheader("🔨 手动操作")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📰 采集新闻", use_container_width=True):
            st.info("触发采集任务...（需在终端运行 python main.py collect）")
    with col2:
        if st.button("🤖 运行分析", use_container_width=True):
            st.info("触发分析任务...（需在终端运行 python main.py analyze）")
            try:
                count = bt.record_today_signals()
                st.success(f"已记录 {count} 条回测信号")
            except Exception as e:
                st.error(f"记录信号失败: {e}")
    with col3:
        if st.button("🗄️ 归档旧数据", use_container_width=True):
            try:
                result = db.archive_old_data(days=30)
                st.success(f"已归档: 新闻 {result.get('news', 0)} 条, 公告 {result.get('announcements', 0)} 条")
            except Exception as e:
                st.error(f"归档失败: {e}")

    st.divider()

    # ▸ 新闻分类统计（条形图）
    st.subheader("📂 新闻分类分布")
    try:
        categories = stats.get("categories", {})
        if categories:
            df_cat = pd.DataFrame(list(categories.items()), columns=["分类", "数量"])
            fig = px.bar(
                df_cat, x="分类", y="数量",
                color="数量", color_continuous_scale="Viridis",
            )
            fig.update_layout(
                height=300,
                plot_bgcolor="#1a1f2e",
                paper_bgcolor="#0e1117",
                font_color="#fafafa",
                margin=dict(l=0, r=0, t=20, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无分类数据。")
    except Exception as e:
        st.warning(f"分类统计加载失败: {e}")

    # ▸ 最近的采集日志
    st.divider()
    st.subheader("📋 最近采集日志")
    try:
        conn = db._connect()
        logs = conn.execute(
            "SELECT * FROM collect_logs ORDER BY finished_at DESC LIMIT 20"
        ).fetchall()
        db._close(conn)
        if logs:
            df_logs = pd.DataFrame([dict(r) for r in logs])
            st.dataframe(df_logs, use_container_width=True, hide_index=True)
        else:
            st.info("暂无采集日志。")
    except Exception as e:
        st.warning(f"采集日志加载失败: {e}")


# ═══════════════════════════════════════════════════════════
# 路由
# ═══════════════════════════════════════════════════════════

if page == "市场总览":
    page_market_overview()
elif page == "个股深度分析":
    page_stock_detail()
elif page == "回测报告":
    page_backtest_report()
elif page == "系统管理":
    page_system()
