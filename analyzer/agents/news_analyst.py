"""新闻分析师 — 分析新闻情绪和关键事件"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class NewsAnalyst(BaseAnalyst):
    """新闻分析师：分析个股相关新闻的情绪和影响"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "📰 新闻分析师"

    def analyze(self, code: str, name: str,
                historical_context: str = "") -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        if not self.db:
            report.summary = "数据库未连接"
            return report

        # 1. 获取最近7天新闻
        news_items = self._get_news(code, days=7)
        if not news_items:
            report.summary = f"{name} 近7天无相关新闻"
            report.confidence = 0.2
            return report

        # 2. LLM 分析
        if self.nlp:
            try:
                llm_result = self.nlp.analyze_news(
                    news_items,
                    stock_code=code,
                    stock_name=name,
                    historical_context=historical_context,
                )
                report.summary = llm_result.get("summary", "")
                report.sentiment = llm_result.get("avg_sentiment", 0.0)
                report.key_findings = llm_result.get("key_topics", [])
                report.risk_factors = llm_result.get("risk_warnings", [])
                report.opportunities = llm_result.get("opportunities", [])

                # 增强字段：板块归因 + 时效 + 主题
                sectors = llm_result.get("primary_sectors", [])
                if sectors:
                    report.key_findings.insert(0, f"涉及板块: {'/'.join(sectors)}")
                theme = llm_result.get("dominant_theme", "")
                if theme:
                    report.key_findings.append(f"主导主题: {theme}")
                timing = llm_result.get("impact_timing", "")
                if timing:
                    report.details["impact_timing"] = timing
                cross = llm_result.get("cross_correlation", "")
                if cross:
                    report.details["cross_correlation"] = cross

                # 置信度 = 新闻数量归一化 + 情绪强度
                news_score = min(len(news_items) / 20, 1.0) * 0.4
                sentiment_strength = abs(report.sentiment) * 0.6
                report.confidence = min(news_score + sentiment_strength, 1.0)
                report.details["news_count"] = len(news_items)
                report.details["items"] = llm_result.get("items", [])
            except Exception as e:
                logger.warning(f"[新闻分析师] LLM分析失败: {e}")
                report.summary = f"分析异常: {e}"
        else:
            # 无LLM时的回退：简单情绪评分
            report.summary = f"分析了 {len(news_items)} 条新闻"
            report.sentiment = self._simple_sentiment(news_items)
            report.confidence = 0.3

        return report

    def _get_news(self, code: str, days: int = 7) -> List[Dict]:
        """获取个股相关新闻"""
        try:
            conn = self.db._connect()
            since = (datetime.now() - timedelta(days=days)).isoformat()
            rows = conn.execute("""
                SELECT DISTINCT n.title, n.summary, n.source, n.published_at
                FROM news n
                JOIN news_stocks ns ON n.id = ns.news_id
                WHERE ns.stock_code = ? AND n.published_at >= ?
                ORDER BY n.published_at DESC LIMIT 30
            """, (code, since)).fetchall()
            self.db._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[新闻分析师] 获取新闻失败: {e}")
            return []

    def _simple_sentiment(self, news_items: List[Dict]) -> float:
        """无LLM时的简单情绪评分"""
        positive = ["增长", "上涨", "盈利", "突破", "利好", "超预期", "增持", "回购"]
        negative = ["下跌", "亏损", "风险", "减持", "利空", "调查", "处罚", "下滑"]
        score = 0
        for item in news_items:
            text = f"{item.get('title','')} {item.get('summary','')}"
            pos_count = sum(1 for w in positive if w in text)
            neg_count = sum(1 for w in negative if w in text)
            score += (pos_count - neg_count) / max(pos_count + neg_count, 1)
        return max(min(score / max(len(news_items), 1), 1.0), -1.0)
