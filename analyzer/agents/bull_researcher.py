"""多方研究员 — 从各分析师报告中提炼看多观点"""
import logging
from typing import Dict, List
from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class BullResearcher(BaseAnalyst):
    """多方研究员：从分析师报告中提炼看多理由，参与辩论"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "🟢 多方研究员"

    def analyze(self, code: str, name: str,
                analyst_reports: List[AnalystReport] = None,
                **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        if not analyst_reports:
            report.summary = f"无分析报告可供参考"
            report.confidence = 0.1
            return report

        # 从各分析师报告中提取看多信号
        bullish_signals = []
        total_confidence = 0

        for r in analyst_reports:
            if r.sentiment > 0.1:
                bullish_signals.append({
                    "source": r.analyst_name,
                    "reason": r.summary,
                    "findings": r.key_findings[:3],
                    "opportunities": r.opportunities[:3],
                })
                total_confidence += r.confidence
            # 反向：熊方的风险点可以被多方反驳
            for opp in r.opportunities:
                if opp not in report.opportunities:
                    report.opportunities.append(opp)

        report.details["bullish_signals"] = bullish_signals
        report.details["num_supporting_analysts"] = len(bullish_signals)

        # 综合多方观点
        if bullish_signals:
            sources = [s["source"] for s in bullish_signals]
            report.summary = f"多方信号来自 {'、'.join(sources)}"
            report.sentiment = 0.4 + min(len(bullish_signals) * 0.1, 0.4)
            report.confidence = min(total_confidence / max(len(analyst_reports), 1) + 0.2, 1.0)

            for s in bullish_signals:
                report.key_findings.extend(s["findings"][:2])
                report.opportunities.extend(s["opportunities"][:2])
        else:
            report.summary = "未发现明确多方信号"
            report.sentiment = -0.2
            report.confidence = 0.3

        return report
