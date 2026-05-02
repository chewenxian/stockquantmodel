"""空方研究员 — 从各分析师报告中提炼看空观点和风险"""
import logging
from typing import Dict, List
from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class BearResearcher(BaseAnalyst):
    """空方研究员：从分析师报告中识别风险和利空因素，参与辩论"""

    # A股风险规则参考
    _ASHARE_RULES = """
- 立案调查=重大利空，短期内不建议参与
- 业绩预告亏损但营收>1亿，不一定触发ST
- 窗口期前30天董监高减持=违规，注意公告
- 大比例解禁+减持公告并发=重大利空
- ROE持续下滑+PE偏高=估值陷阱典型特征
- 技术面与消息面背离=需高度警惕
"""

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
        risk_count = 0

        for r in analyst_reports:
            # 看空情绪本身就是信号
            if r.sentiment < -0.1:
                bearish_signals.append({
                    "source": r.analyst_name,
                    "sentiment": r.sentiment,
                    "risks": r.risk_factors[:3],
                })
                total_confidence += r.confidence
                risk_count += 1

            # 风险因素直接收集
            for risk in r.risk_factors:
                if risk not in report.risk_factors:
                    report.risk_factors.append(risk)
                    risk_count += 1

        report.details["bearish_signals"] = bearish_signals
        report.details["num_risk_signals"] = risk_count
        report.details["ashare_rules_applied"] = True

        # 综合空方观点
        if risk_count >= 3:
            report.summary = f"识别到 {risk_count} 个风险因素"
            report.sentiment = -0.5 - min(risk_count * 0.05, 0.3)
            report.confidence = min(total_confidence / max(len(analyst_reports), 1) + 0.3, 1.0)
        elif risk_count >= 1:
            report.summary = f"识别到 {risk_count} 个风险点"
            report.sentiment = -0.3
            report.confidence = 0.4
        else:
            report.summary = "未发现明显风险信号"
            report.sentiment = 0.1
            report.confidence = 0.3

        return report
