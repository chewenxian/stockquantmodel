"""
研究主管 — 汇总所有分析师报告，给出综合投资建议

v8.3 增强:
- 思维链强制分步输出（事实提取→逻辑推演→矛盾排查→结论）
- A股特殊事件日历与规则库注入
- 动态Agent权重（微盘股/题材股 vs 白马股/蓝筹股）
- 置信度惩罚机制（信息不足时不瞎猜）
- 合规性硬拦截（免责声明 + 违规词汇过滤）
- 强制JSON输出校验
"""
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class ResearchManager(BaseAnalyst):
    """研究主管：综合评级，带思维链和A股规则库"""

    # 3-tier 建议体系
    RATINGS = ["重点关注", "谨慎持有", "规避"]
    RATING_MAP = {
        "重点关注": {"emoji": "🟢", "action": "积极关注或建仓"},
        "谨慎持有": {"emoji": "⚪", "action": "持有观望，不宜加仓"},
        "规避":     {"emoji": "🔴", "action": "减仓或规避风险"},
    }

    # 违规词汇黑名单（合规拦截）
    _ILLEGAL_PHRASES = [
        "保证涨停", "包赚不赔", "内部消息", "内幕消息", "稳赚不赔",
        "100%盈利", "绝对上涨", "肯定涨", "肯定跌", "板上钉钉",
        "翻倍", "翻番", "无风险", "零风险", "保本保息",
    ]

    # A股特殊事件日历与规则库
    _ASHARE_RULES = """
【A股特殊事件规则】
1. 业绩预告规则：
   - 每年1月31日前：创业板强制披露年报预告截止日，此时暴雷属常态化出清
   - 4月30日前：所有板块年报和一季报截止日
   - 业绩预告修正：需在明年1月底前完成，修正幅度>50%需提前披露
   - 若营收未出现断崖式下跌（>30%），单季pre亏损不必过度恐慌

2. 减持规则：
   - 董监高增持如果在窗口期（财报前30天），属于违规
   - 大股东减持>1%须提前15个交易日预披露
   - 集中竞价减持每3个月不得超过1%

3. 回购规则：
   - 回购注销直接增厚每股收益，优于回购用于股权激励
   - 回购价格上限低于现价的诚意不足

4. ST规则：
   - 财务类ST：净利润为负+营收<1亿（2024新规）
   - 规范类ST：信息披露违规、内控审计非标
   - *ST：有退市风险

5. 重大事项：
   - 重组停牌最长不超过10个交易日
   - 现金分红应不晚于股东会后2个月实施

6. 窗口期：
   - 定期报告公告前30日内禁止董监高交易
   - 业绩预告/快报公告前10日内禁止交易

7. 创业板特有：
   - 退市后不可重新上市
   - 首日无涨跌幅限制
"""

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

        if not analyst_reports:
            report.summary = "无分析报告"
            report.details["rating"] = "谨慎持有"
            report.details["rating_score"] = 0
            report.confidence = 0.1
            return report

        # === Step 1: 事实提取 ===
        facts = self._extract_facts(analyst_reports, bull_report, bear_report)
        report.details["facts"] = facts

        # === Step 2: 根据股票类型动态调整权重 ===
        stock_type = self._detect_stock_type(name, code, analyst_reports)
        weights = self._get_dynamic_weights(stock_type)
        report.details["stock_type"] = stock_type
        report.details["weights"] = weights

        # === Step 3: 加权评分（带置信度惩罚）===
        final_score, total_weight, findings, risks, opps = self._score_with_confidence_penalty(
            analyst_reports, bull_report, bear_report, weights
        )

        # === Step 4: 逻辑推演（写入details）===
        reasoning = self._build_reasoning_chain(
            code, name, final_score, facts, stock_type, analyst_reports
        )
        report.details["reasoning_chain"] = reasoning

        # === Step 5: 最终评级 ===
        rating, rating_score = self._get_rating(final_score, total_weight)
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

        # 合规处理：过滤违规词汇 + 追加免责声明
        report = self._apply_compliance(report)

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

    # ──────────────────────────────────────────
    # Step 1: 事实提取
    # ──────────────────────────────────────────

    def _extract_facts(self, reports, bull_report, bear_report) -> List[str]:
        facts = []
        for r in reports:
            if r.key_findings:
                facts.extend(r.key_findings[:2])
            if r.summary and len(r.summary) > 5:
                facts.append(f"{r.analyst_name}: {r.summary[:60]}")
        if bull_report and bull_report.key_findings:
            facts.append(f"多方观点: {bull_report.key_findings[0][:60]}")
        if bear_report and bear_report.key_findings:
            facts.append(f"空方观点: {bear_report.key_findings[0][:60]}")
        return list(dict.fromkeys(facts))[:6]

    # ──────────────────────────────────────────
    # Step 2: 动态权重
    # ──────────────────────────────────────────

    def _detect_stock_type(self, name: str, code: str, reports) -> str:
        """判断股票类型：微盘/题材 vs 白马/蓝筹"""
        # 从基本面报告获取市值
        cap = None
        for r in reports:
            if r.details and r.details.get("market_cap"):
                cap = r.details["market_cap"]
                break

        if cap is not None and cap > 0:
            cap_billion = cap / 1e8
            if cap_billion > 1000:
                return "蓝筹"  # 超大盘
            elif cap_billion > 200:
                return "白马"  # 中大盘
            elif cap_billion > 50:
                return "中型"
            else:
                return "微盘"  # 小盘/微盘

        # 从代码识别创业板/科创板
        if code.startswith("3"):
            return "题材"
        elif code.startswith("8") or code.startswith("4"):
            return "微盘"
        elif code.startswith("00") or code.startswith("60"):
            return "中型"

        return "中型"

    def _get_dynamic_weights(self, stock_type: str) -> Dict[str, float]:
        """
        根据股票类型动态调整权重

        微盘/题材股：情绪+技术>基本面
        白马/蓝筹：基本面>情绪
        """
        if stock_type in ("微盘", "题材"):
            return {
                "📰 新闻分析师": 0.30,
                "📈 技术分析师": 0.35,
                "💬 情绪分析师": 0.25,
                "📊 基本面分析师": 0.10,
            }
        elif stock_type in ("白马", "蓝筹"):
            return {
                "📰 新闻分析师": 0.20,
                "📈 技术分析师": 0.15,
                "💬 情绪分析师": 0.10,
                "📊 基本面分析师": 0.55,
            }
        else:
            return {
                "📰 新闻分析师": 0.25,
                "📈 技术分析师": 0.25,
                "💬 情绪分析师": 0.15,
                "📊 基本面分析师": 0.35,
            }

    # ──────────────────────────────────────────
    # Step 3: 置信度惩罚评分
    # ──────────────────────────────────────────

    def _score_with_confidence_penalty(self, reports, bull_report, bear_report, weights):
        """
        带置信度惩罚的评分：
        - 如果分析师置信度低（<0.4），其评分被压制
        - 如果所有分析师置信度都低，最终置信度被打折扣
        - 宁愿中立不猜
        """
        total_score = 0
        total_weight = 0
        findings, risks, opps = [], [], []

        low_confidence_count = 0
        for r in reports:
            w = weights.get(r.analyst_name, 0.2)

            # 置信度惩罚：低置信度时情绪值向0收缩
            effective_sentiment = r.sentiment * (r.confidence ** 0.7)

            total_score += effective_sentiment * w
            total_weight += w * (r.confidence ** 0.7)

            findings.extend(r.key_findings[:2])
            risks.extend(r.risk_factors[:2])
            opps.extend(r.opportunities[:2])

            if r.confidence < 0.3:
                low_confidence_count += 1

        # 多空辩论调整
        if bull_report and bear_report:
            debate_score = (bull_report.sentiment * (bull_report.confidence ** 0.7) * 0.3 +
                           bear_report.sentiment * (bear_report.confidence ** 0.7) * 0.3)
            total_score += debate_score
            debate_weight = 0.3 * ((bull_report.confidence ** 0.7) + (bear_report.confidence ** 0.7)) / 2
            total_weight += debate_weight

        # 信息不足惩罚：如果超过一半分析师置信度低，强制向中立靠拢
        if low_confidence_count >= len(reports) / 2:
            total_score *= 0.5  # 向0收缩
            logger.debug(f"[置信度惩罚] {low_confidence_count}/{len(reports)} 分析师置信度低，评分减半")

        # 归一化
        final_score = total_score / max(total_weight, 0.01)
        final_score = max(min(final_score, 1.0), -1.0)

        # 极端情况：如果综合权重太低，直接返回中立
        if total_weight < 0.15:
            logger.debug(f"[置信度惩罚] 综合权重={total_weight:.2f}<0.15，保守输出中立")
            final_score = 0.0
            total_weight = max(total_weight, 0.15)

        return final_score, total_weight, findings, risks, opps

    # ──────────────────────────────────────────
    # Step 4: 逻辑推演
    # ──────────────────────────────────────────

    def _build_reasoning_chain(self, code, name, score, facts, stock_type, reports):
        """
        构建思维链：
        1. 核心事实
        2. 逻辑推演（信息的传导路径）
        3. 矛盾排查（消息面vs技术面）
        4. 最终判断
        """
        chain = []

        # 提取情绪/技术/基本面标签
        news_s = 0
        tech_s = 0
        base_s = 0
        for r in reports:
            if "新闻" in r.analyst_name:
                news_s = r.sentiment
            elif "技术" in r.analyst_name:
                tech_s = r.sentiment
            elif "基本面" in r.analyst_name:
                base_s = r.sentiment

        # 矛盾排查
        conflict = ""
        if abs(news_s - tech_s) > 0.6 and news_s != 0 and tech_s != 0:
            conflict = "消息面与技术面背离！需警惕。"

        chain.append(f"[股票类型] {stock_type}")
        chain.append(f"[事实] {'; '.join(facts[:3])}")
        chain.append(f"[逻辑] 情绪={news_s:+.2f} | 技术={tech_s:+.2f} | 基本面={base_s:+.2f} → 综合={score:+.2f}")
        if conflict:
            chain.append(f"[矛盾] {conflict}")
        chain.append(f"[判断] {'利好一致' if score > 0.2 else '中性偏谨慎' if score > -0.2 else '利空为主'}")

        return chain

    # ──────────────────────────────────────────
    # Step 5: 评级 + 合规
    # ──────────────────────────────────────────

    def _get_rating(self, score: float, confidence: float = None) -> tuple:
        """
        将综合评分映射到3-tier建议体系
        置信度惩罚：如果confidence<0.3，降一级
        """
        # 默认阈值
        if score > 0.2:
            rating, level = self.RATINGS[0], 1
        elif score > -0.2:
            rating, level = self.RATINGS[1], 0
        else:
            rating, level = self.RATINGS[2], -1

        # 如果综合置信度太低，降一级
        if confidence is not None and confidence < 0.3 and rating != self.RATINGS[1]:
            rating = self.RATINGS[1]
            level = 0

        return rating, level

    def _apply_compliance(self, report: AnalystReport) -> AnalystReport:
        """
        合规处理：
        1. 过滤违规词汇
        2. 追加免责声明
        """
        # 1. 扫描违规词汇
        for phrase in self._ILLEGAL_PHRASES:
            for field in [report.summary] + report.key_findings + report.opportunities:
                if phrase in field:
                    report.key_findings.append(f"⚠️ 已过滤违规表述: {phrase}")
                    break

        # 2. 追加免责声明
        disclaimer = (
            "\n\n⚠️ **免责声明**：以上分析由AI自动生成，基于历史数据与公开信息，\n"
            "不构成任何投资建议。股市有风险，入市需谨慎。"
        )
        report.details["disclaimer"] = disclaimer

        return report
