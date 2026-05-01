"""
交易建议生成器（增强版）
综合多维度数据生成买卖建议

增强功能：
1. 整合知识图谱推理结果到建议中
2. 增加逻辑推演过程（因为A→所以B→最终建议C）
3. 结构化建议输出（含推理链、关键因子、风险等级）
4. 影响评估模型集成
"""
import logging
import json
import math
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class TradingAdvisor:
    """
    交易建议生成器（增强版）
    综合情绪、新闻、市场数据、知识图谱推理生成操作建议
    """

    SUGGESTION_LEVELS = [
        "强烈关注",
        "关注",
        "持有",
        "观望",
        "回避",
        "强烈回避",
    ]

    RISK_LEVELS = ["高", "中", "低"]

    def __init__(self, config_path: Optional[str] = None, knowledge_graph=None,
                 impact_model=None):
        self.config = self._load_config(config_path)

        # 可选组件
        self.kg = knowledge_graph
        self.impact_model = impact_model

        # 延迟初始化
        if self.kg is None:
            try:
                from analyzer.knowledge_graph import FinancialKnowledgeGraph
                self.kg = FinancialKnowledgeGraph()
            except Exception:
                pass

        if self.impact_model is None:
            try:
                from analyzer.impact_model import ImpactModel
                self.impact_model = ImpactModel()
            except Exception:
                pass

    def _load_config(self, config_path: Optional[str] = None) -> dict:
        import os
        import yaml
        paths_to_try = [
            config_path,
            os.path.join(os.path.dirname(__file__), "config.yaml"),
            "analyzer/config.yaml",
        ]
        for path in paths_to_try:
            if path and os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        return yaml.safe_load(f)
                except Exception:
                    pass
        return {}

    # ──────────────────────────────────────────
    # 置信度计算
    # ──────────────────────────────────────────

    def calculate_confidence(self, sentiment: float, news_count: int,
                             market_data: Optional[Dict] = None,
                             kg_impact: Optional[Dict] = None) -> float:
        """
        多维置信度计算

        考虑因素：
        1. 情绪强度（越极端置信度越高）
        2. 新闻数量（越多置信度越高，但边际递减）
        3. 市场数据一致性（情绪与走势一致时置信度更高）
        4. 知识图谱推理链长度与一致性

        Args:
            sentiment: 情绪评分 (-1 ~ 1)
            news_count: 相关新闻数量
            market_data: 市场数据字典（可选）
            kg_impact: 知识图谱推理结果（可选）

        Returns:
            float: 0 ~ 1 的置信度
        """
        try:
            # 1. 情绪强度因子
            sentiment_strength = abs(sentiment)
            sentiment_factor = sentiment_strength ** 0.7

            # 2. 新闻数量因子
            if news_count <= 0:
                news_factor = 0.1
            else:
                news_factor = min(math.log10(news_count + 1) / 2.0, 1.0)

            # 3. 市场数据一致性因子
            market_factor = 0.5
            if market_data:
                change_pct = market_data.get("change_pct", 0) or 0
                main_net = market_data.get("main_net", 0) or 0
                consensus = 0
                if sentiment > 0 and change_pct > 0:
                    consensus += 0.2
                if sentiment < 0 and change_pct < 0:
                    consensus += 0.2
                if sentiment > 0 and main_net > 0:
                    consensus += 0.15
                if sentiment < 0 and main_net < 0:
                    consensus += 0.15
                market_factor = 0.5 + consensus

            # 4. 知识图谱推理因子
            kg_factor = 0.5
            if kg_impact:
                reasoning_count = len(kg_impact.get("impact_reasoning", []))
                chain_count = len(kg_impact.get("chain_reactions", []))
                # 推理链越长越可信（但边际递减）
                total_evidence = reasoning_count + chain_count
                kg_bonus = min(total_evidence * 0.05, 0.3)
                kg_factor = 0.5 + kg_bonus

                # 直接冲击方向与情绪一致则加分
                direct_impact = kg_impact.get("direct_impact", "中性")
                impact_direction = 1 if direct_impact in ("利好", "重大利好") else (-1 if direct_impact in ("利空", "重大利空") else 0)
                if impact_direction * sentiment > 0:
                    kg_factor += 0.1

            # 5. 综合计算
            raw_confidence = (
                sentiment_factor * 0.35 +
                news_factor * 0.20 +
                market_factor * 0.20 +
                kg_factor * 0.25
            )

            confidence = max(0.0, min(1.0, raw_confidence))
            return round(confidence, 4)

        except Exception:
            return 0.5

    # ──────────────────────────────────────────
    # 建议生成（增强版）
    # ──────────────────────────────────────────

    def generate_advice(self, stock_analysis: Dict) -> Dict:
        """
        增强建议生成器

        Args:
            stock_analysis: 个股分析结果，包含：
                - code, name, avg_sentiment, news_count
                - key_topics, risk_warnings, anomaly
                - market_data (可选)

        Returns (增强格式):
            {
                "code": "300750",
                "name": "宁德时代",
                "suggestion": "强烈关注",
                "confidence": 0.85,
                "reasoning": [
                    "碳酸锂价格暴跌 → 锂矿企业成本下降",
                    "公司产能扩张中 → 受益于成本下降",
                    "板块热度上升 → 资金关注度高"
                ],
                "risk_level": "低",
                "key_factors": ["成本下降", "产能扩张", "板块热度"],
                "sentiment": 0.72,
                "news_count": 15,
                "impact_evaluation": {...},
                "kg_reasoning": {...},
                "timestamp": "2025-01-15 16:00"
            }
        """
        code = stock_analysis.get("code", "")
        name = stock_analysis.get("name", "")
        sentiment = stock_analysis.get("avg_sentiment", 0.0)
        news_count = stock_analysis.get("news_count", 0)
        key_topics = stock_analysis.get("key_topics", [])
        risk_warnings = stock_analysis.get("risk_warnings", [])
        anomaly = stock_analysis.get("anomaly", {})
        market_data = stock_analysis.get("market_data")

        reasoning: List[str] = []
        key_factors: List[str] = []
        kg_reasoning: Dict = {}
        impact_evaluation: Dict = {}

        try:
            # ═══ 1. 知识图谱推理 ═══
            if self.kg and code:
                # 从新闻话题构建推理上下文（取前3个话题拼接）
                news_context = "，".join(key_topics[:5]) if key_topics else ""
                if news_context:
                    kg_result = self.kg.infer_impact(code, news_context)
                    kg_reasoning = kg_result
                    reasoning.extend(kg_result.get("impact_reasoning", []))

                    # 提取关键因子
                    for step in kg_result.get("impact_reasoning", []):
                        # 提取"→"左边的因子
                        parts = step.split("→")
                        for part in parts:
                            part = part.strip()
                            if part and len(part) <= 15 and part not in key_factors:
                                key_factors.append(part)

            # ═══ 2. 影响评估 ═══
            if self.impact_model:
                impact_evaluation = self.impact_model.calculate_impact_factor(
                    stock_code=code,
                    sentiment=sentiment,
                    news_count=news_count,
                    market_data=market_data,
                    anomaly_data=anomaly,
                    knowledge_graph_impact=kg_reasoning,
                )
                # 追加影响评估理由
                for r in impact_evaluation.get("reasons", []):
                    if r and r not in reasoning:
                        reasoning.append(r)

            # ═══ 3. 逻辑推演链 ═══
            if not reasoning:
                # 如果没有知识图谱推理，构建基础推演链
                if sentiment > 0.5:
                    reasoning.append(f"市场情绪积极（{sentiment:.2f}）→ 投资者信心较强")
                elif sentiment > 0:
                    reasoning.append(f"市场情绪温和正面（{sentiment:.2f}）")
                elif sentiment > -0.5:
                    reasoning.append(f"市场情绪偏中性（{sentiment:.2f}）")
                else:
                    reasoning.append(f"市场情绪消极（{sentiment:.2f}）→ 投资者信心不足")

                if news_count > 5:
                    reasoning.append(f"近期{news_count}条相关新闻 → 市场关注度高")
                elif news_count > 0:
                    reasoning.append(f"相关新闻{news_count}条 → 市场关注度一般")

                if anomaly.get("is_anomaly"):
                    atype = "利好" if anomaly["type"] == "bullish" else "利空"
                    reasoning.append(f"检测到突发{atype}信号 → 需要重点关注")

            # ═══ 4. 置信度计算 ═══
            confidence = self.calculate_confidence(sentiment, news_count, market_data, kg_reasoning)

            # ═══ 5. 建议等级判定 ═══
            suggestion, risk_level = self._determine_suggestion(
                sentiment, confidence, anomaly, impact_evaluation, kg_reasoning
            )

            # ═══ 6. 收集关键因子 ═══
            if not key_factors:
                key_factors = self._extract_key_factors(sentiment, key_topics, kg_reasoning)

        except Exception as e:
            logger.warning(f"增强建议生成异常 ({code}): {e}")
            suggestion = "持有"
            risk_level = "中"
            confidence = 0.3
            reasoning = [f"分析异常，建议保守: {e}"]

        # 构建最终结构化建议
        result = {
            "code": code,
            "name": name,
            "suggestion": suggestion,
            "confidence": round(confidence, 4),
            "reasoning": reasoning[:8],  # 最多8条推理
            "risk_level": risk_level,
            "key_factors": key_factors[:6],
            "sentiment": round(sentiment, 4),
            "news_count": news_count,
            "impact_evaluation": impact_evaluation,
            "kg_reasoning": kg_reasoning,
            "timestamp": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

        return result

    def _determine_suggestion(self, sentiment: float, confidence: float,
                               anomaly: Dict, impact_eval: Dict,
                               kg_reasoning: Dict) -> tuple:
        """
        综合判定建议等级和风险等级

        Returns:
            (suggestion, risk_level)
        """
        try:
            # 异常舆情修正后的有效情绪
            effective_sentiment = sentiment

            # 影响评估修正
            impact_score = impact_eval.get("impact_score", sentiment)
            if abs(impact_score) > abs(effective_sentiment):
                effective_sentiment = impact_score

            # 知识图谱推理方向修正
            kg_impact = kg_reasoning.get("direct_impact", "")
            if kg_impact == "利好" and effective_sentiment > 0:
                effective_sentiment += 0.15
            elif kg_impact == "重大利好" and effective_sentiment > 0:
                effective_sentiment += 0.25
            elif kg_impact == "利空" and effective_sentiment < 0:
                effective_sentiment -= 0.15
            elif kg_impact == "重大利空" and effective_sentiment < 0:
                effective_sentiment -= 0.25

            # 确保在范围内
            effective_sentiment = max(-1.0, min(1.0, effective_sentiment))

            # 判定建议等级
            if effective_sentiment >= 0.7:
                suggestion = "强烈关注"
                risk_level = "低"
            elif effective_sentiment >= 0.3:
                suggestion = "关注"
                risk_level = "低"
            elif effective_sentiment >= -0.3:
                suggestion = "持有"
                risk_level = "中"
            elif effective_sentiment >= -0.7:
                suggestion = "观望"
                risk_level = "中"
                # 异常舆情→升级
                if anomaly.get("is_anomaly") and anomaly.get("type") == "bearish":
                    suggestion = "回避"
                    risk_level = "高"
            else:
                suggestion = "强烈回避"
                risk_level = "高"

            # 置信度太低时降级
            if confidence < 0.3:
                suggestion = "观望"

            # 异常舆情强修正
            if anomaly.get("is_anomaly"):
                if anomaly["type"] == "bearish" and anomaly["severity"] == "high":
                    if suggestion not in ("强烈回避", "回避"):
                        suggestion = self._downgrade_suggestion(suggestion)
                    risk_level = "高"
                elif anomaly["type"] == "bullish" and anomaly["severity"] == "high":
                    if suggestion in ("观望", "回避"):
                        suggestion = "关注"
                    risk_level = "低"

            # 影响评估结果修正
            impact_level = impact_eval.get("level", "中性")
            if impact_level in ("重大利好", "利好") and suggestion in ("观望", "回避"):
                suggestion = "关注"
            elif impact_level in ("重大利空", "利空") and suggestion in ("关注", "强烈关注"):
                suggestion = self._downgrade_suggestion(suggestion)

        except Exception:
            suggestion = "持有"
            risk_level = "中"

        return suggestion, risk_level

    def _downgrade_suggestion(self, current: str) -> str:
        """降级建议（向更保守方向调整）"""
        downgrade_map = {
            "强烈关注": "关注",
            "关注": "持有",
            "持有": "观望",
            "观望": "回避",
            "回避": "强烈回避",
            "强烈回避": "强烈回避",
        }
        return downgrade_map.get(current, "持有")

    def _extract_key_factors(self, sentiment: float, key_topics: List[str],
                              kg_reasoning: Dict) -> List[str]:
        """提取关键影响因子"""
        factors = []

        # 从情绪值提取
        if abs(sentiment) > 0.5:
            factors.append("情绪信号强烈" if sentiment > 0 else "情绪信号消极")
        elif abs(sentiment) > 0.2:
            factors.append("情绪信号偏正面" if sentiment > 0 else "情绪信号偏负面")

        # 从话题提取
        for topic in key_topics[:3]:
            if topic and len(topic) >= 2:
                factors.append(topic)

        # 从知识图谱推理提取
        for step in kg_reasoning.get("impact_reasoning", [])[:2]:
            # 取"→"前面的因子
            left = step.split("→")[0].strip()
            if left and len(left) <= 12 and left not in factors:
                factors.append(left)

        return factors[:6] if factors else ["无明显关键因子"]

    # ──────────────────────────────────────────
    # 批量建议
    # ──────────────────────────────────────────

    def generate_advice_for_all(self, analyses: List[Dict]) -> List[Dict]:
        """
        为多只股票生成建议（增强版）

        Args:
            analyses: 个股分析结果列表

        Returns:
            增强建议结果列表
        """
        advices = []
        for analysis in analyses:
            try:
                advice = self.generate_advice(analysis)
                advices.append(advice)
            except Exception as e:
                logger.error(f"生成建议失败 {analysis.get('code', '?')}: {e}")
                advices.append({
                    "code": analysis.get("code", "?"),
                    "name": analysis.get("name", "未知"),
                    "suggestion": "持有",
                    "confidence": 0.0,
                    "reasoning": [f"生成失败: {e}"],
                    "risk_level": "中",
                    "key_factors": [],
                    "sentiment": 0.0,
                    "news_count": 0,
                    "impact_evaluation": {},
                    "kg_reasoning": {},
                    "timestamp": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
                })
        return advices


import os


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    advisor = TradingAdvisor()

    test_data = {
        "code": "300750",
        "name": "宁德时代",
        "avg_sentiment": 0.72,
        "news_count": 15,
        "key_topics": ["碳酸锂价格暴跌", "产能扩张", "电池订单增长", "新能源汽车销量新高"],
        "risk_warnings": ["原材料依赖进口"],
        "anomaly": {"is_anomaly": True, "type": "bullish", "severity": "medium", "reason": "突发利好"},
        "market_data": {"change_pct": 3.5, "main_net": 8000},
    }

    result = advisor.generate_advice(test_data)
    print(json.dumps(result, ensure_ascii=False, indent=2))
