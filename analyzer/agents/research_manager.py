"""研究主管 — 汇总所有分析师报告，给出综合投资建议"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class ResearchManager(BaseAnalyst):
    """研究主管：汇总所有分析报告，生成最终投资建议"""

    # 3-tier 建议体系（更简洁实用）
    RATINGS = ["重点关注", "谨慎持有", "规避"]
    # 对应旧的5-tier映射（兼容）
    RATING_MAP = {
        "重点关注": {"emoji": "🟢", "action": "积极关注或建仓"},
        "谨慎持有": {"emoji": "⚪", "action": "持有观望，不宜加仓"},
        "规避":     {"emoji": "🔴", "action": "减仓或规避风险"},
    }

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

        # 确定3-tier建议
        rating, rating_score = self._get_rating(final_score)
        rating_info = self.RATING_MAP.get(rating, {})
        report.sentiment = final_score
        report.confidence = min(total_weight, 1.0)
        report.key_findings = list(dict.fromkeys(findings))[:5]
        report.risk_factors = list(dict.fromkeys(risks))[:3]
        report.opportunities = list(dict.fromkeys(opps))[:3]

        report.details["rating"] = rating
        report.details["rating_score"] = rating_score
        report.details["rating_action"] = rating_info.get("action", "")
        report.details["rating_emoji"] = rating_info.get("emoji", "")
        report.details["analyst_scores"] = {
            r.analyst_name: {"sentiment": r.sentiment, "confidence": r.confidence}
            for r in analyst_reports
        }

        # 添加时效标签
        timings = [
            r.details.get("impact_timing", "")
            for r in analyst_reports if hasattr(r, "details") and r.details.get("impact_timing")
        ]
        if timings:
            report.details["impact_timing"] = max(set(timings), key=timings.count)

        emoji = rating_info.get("emoji", "")
        report.summary = (
            f"综合评级: **{emoji} {rating}** (分项: " +
            ", ".join([
                f"{r.analyst_name}={r.sentiment:+.2f}" for r in analyst_reports
            ]) + ")"
        )

        if historical_context:
            report.details["historical_context_used"] = True

        return report

    def _get_rating(self, score: float) -> tuple:
        """将综合评分映射到3-tier建议体系

        重点关注: 明确利好信号，多重维度共振
        谨慎持有: 中性偏多或偏空，观望等待信号
        规避:     风险因素明显，建议减仓
        """
        if score > 0.2:
            return self.RATINGS[0], 1      # 重点关注
        elif score > -0.2:
            return self.RATINGS[1], 0      # 谨慎持有
        else:
            return self.RATINGS[2], -1     # 规避
