"""多方研究员 — 从各分析师报告中提炼看多观点"""
import logging
from typing import Dict, List
from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class BullResearcher(BaseAnalyst):
    """多方研究员：从分析师报告中提炼看多理由，参与辩论"""

    # A股规则参考（用于修正判断）
    _ASHARE_RULES = """
- 业绩预增>50%需提前披露，是强利好信号
- 回购注销优于股权激励回购
- 大股东增持+回购并发 = 强信心信号
- 中标金额占营收>10% = 实质性利好
- 行业政策利好 = 中期利好，非短期炒作
"""

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
        signal_count = 0

        for r in analyst_reports:
            if r.sentiment > 0.1:
                bullish_signals.append({
                    "source": r.analyst_name,
                    "reason": r.summary,
                    "findings": r.key_findings[:3],
                    "opportunities": r.opportunities[:3],
                })
                total_confidence += r.confidence
                signal_count += 1

            # 反向：熊方的风险点可以被多方反驳
            for opp in r.opportunities:
                if opp not in report.opportunities:
                    report.opportunities.append(opp)

        report.details["bullish_signals"] = bullish_signals
        report.details["num_supporting_analysts"] = len(bullish_signals)
        report.details["ashare_rules_applied"] = True

        # 综合多方观点
        if signal_count >= 2:
            report.summary = f"多方信号来自 {signal_count} 个维度"
            report.sentiment = 0.4 + min(signal_count * 0.1, 0.4)
            report.confidence = min(total_confidence / max(len(analyst_reports), 1) + 0.2, 1.0)

            for s in bullish_signals:
                report.key_findings.extend(s["findings"][:2])
                report.opportunities.extend(s["opportunities"][:2])
        elif signal_count == 1:
            report.summary = "有1个弱多方信号"
            report.sentiment = 0.2
            report.confidence = 0.3
        else:
            report.summary = "未发现明确多方信号"
            report.sentiment = -0.2
            report.confidence = 0.3

        return report
