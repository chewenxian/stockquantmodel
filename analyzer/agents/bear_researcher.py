"""空方研究员 — 从各分析师报告中提炼看空观点"""
import logging
from typing import Dict, List
from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class BearResearcher(BaseAnalyst):
    """空方研究员：从分析师报告中提炼看空理由，参与辩论"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "🔴 空方研究员"

    def analyze(self, code: str, name: str,
                analyst_reports: List[AnalystReport] = None,
                **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        if not analyst_reports:
            report.summary = f"无分析报告可供参考"
            report.confidence = 0.1
            return report

        # 从各分析师报告中提取看空信号
        bearish_signals = []
        total_confidence = 0

        for r in analyst_reports:
            if r.sentiment < -0.1:
                bearish_signals.append({
                    "source": r.analyst_name,
                    "reason": r.summary,
                    "findings": r.key_findings[:3],
                    "risks": r.risk_factors[:3],
                })
                total_confidence += r.confidence
            # 反向：多方的机会可能被空方反驳
            for risk in r.risk_factors:
                if risk not in report.risk_factors:
                    report.risk_factors.append(risk)

        report.details["bearish_signals"] = bearish_signals
        report.details["num_supporting_analysts"] = len(bearish_signals)

        # 综合空方观点
        if bearish_signals:
            sources = [s["source"] for s in bearish_signals]
            report.summary = f"空方信号来自 {'、'.join(sources)}"
            report.sentiment = -0.4 - min(len(bearish_signals) * 0.1, 0.4)
            report.confidence = min(total_confidence / max(len(analyst_reports), 1) + 0.2, 1.0)

            for s in bearish_signals:
                report.key_findings.extend(s["findings"][:2])
                report.risk_factors.extend(s["risks"][:2])
        else:
            report.summary = "未发现明确空方信号"
            report.sentiment = 0.2
            report.confidence = 0.3

        return report
