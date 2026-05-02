"""研究主管 — 汇总所有分析师报告，给出综合投资建议"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class ResearchManager(BaseAnalyst):
    """研究主管：汇总所有分析报告，生成最终投资建议"""

    # 5-tier 评级
    RATINGS = ["买入", "增持", "持有", "减持", "卖出"]

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "🎯 研究主管"

    def analyze(self, code: str, name: str,
                analyst_reports: List[AnalystReport] = None,
                bull_report: AnalystReport = None,
                bear_report: AnalystReport = None,
                historical_context: str = "",
                **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        # 汇总各分析师得分
        if not analyst_reports:
            report.summary = "无分析报告"
            report.details["rating"] = "持有"
            report.details["rating_score"] = 0
            report.confidence = 0.1
            return report

        # 加权评分
        weights = {
            "📰 新闻分析师": 0.25,
            "📈 技术分析师": 0.25,
            "💬 情绪分析师": 0.15,
            "📊 基本面分析师": 0.35,
        }

        total_score = 0
        total_weight = 0
        findings = []
        risks = []
        opps = []

        for r in analyst_reports:
            w = weights.get(r.analyst_name, 0.2)
            # 置信度加权
            effective_sentiment = r.sentiment * r.confidence
            total_score += effective_sentiment * w
            total_weight += w * r.confidence
            findings.extend(r.key_findings[:2])
            risks.extend(r.risk_factors[:2])
            opps.extend(r.opportunities[:2])

        # 多空辩论调整
        if bull_report and bear_report:
            debate_score = bull_report.sentiment * bull_report.confidence * 0.3
            debate_score += bear_report.sentiment * bear_report.confidence * 0.3
            total_score += debate_score
            total_weight += 0.3 * (bull_report.confidence + bear_report.confidence) / 2
            report.details["bull_vs_bear"] = {
                "bull_sentiment": bull_report.sentiment,
                "bear_sentiment": bear_report.sentiment,
                "bull_confidence": bull_report.confidence,
                "bear_confidence": bear_report.confidence,
            }

        # 归一化评分
        final_score = total_score / max(total_weight, 0.01)
        final_score = max(min(final_score, 1.0), -1.0)

        # 确定5-tier评级
        rating, rating_score = self._get_rating(final_score)
        report.sentiment = final_score
        report.confidence = min(total_weight, 1.0)
        report.key_findings = list(dict.fromkeys(findings))[:5]
        report.risk_factors = list(dict.fromkeys(risks))[:3]
        report.opportunities = list(dict.fromkeys(opps))[:3]

        report.details["rating"] = rating
        report.details["rating_score"] = rating_score
        report.details["analyst_scores"] = {
            r.analyst_name: {"sentiment": r.sentiment, "confidence": r.confidence}
            for r in analyst_reports
        }

        report.summary = (
            f"综合评级: **{rating}** (分项: " +
            ", ".join([
                f"{r.analyst_name}={r.sentiment:+.2f}" for r in analyst_reports
            ]) + ")"
        )

        if historical_context:
            report.details["historical_context_used"] = True

        return report

    def _get_rating(self, score: float) -> tuple:
        """将综合评分映射到5-tier评级"""
        if score > 0.5:
            return self.RATINGS[0], 2      # 买入
        elif score > 0.15:
            return self.RATINGS[1], 1      # 增持
        elif score > -0.15:
            return self.RATINGS[2], 0      # 持有
        elif score > -0.5:
            return self.RATINGS[3], -1     # 减持
        else:
            return self.RATINGS[4], -2     # 卖出
