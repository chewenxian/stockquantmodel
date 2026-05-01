"""
情报影响评估模型
综合：情感得分 + 板块热度 + 新闻数量 + 历史波动率
计算影响因子

影响等级：
- 重大利好（score ≥ 0.8）
- 利好（score ≥ 0.4）
- 中性（score > -0.4）
- 利空（score ≤ -0.4）
- 重大利空（score ≤ -0.8）
"""
import logging
import math
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ImpactModel:
    """
    情报影响评估模型
    综合多维度数据计算影响因子
    """

    # 影响等级阈值
    IMPACT_LEVELS = {
        "重大利好": 0.8,
        "利好": 0.4,
        "中性": -0.4,
        "利空": -0.8,
        "重大利空": -1.0,
    }

    def __init__(self, db=None):
        self.db = db

    # ──────────────────────────────────────────
    # 核心评估方法
    # ──────────────────────────────────────────

    def calculate_impact_factor(
        self,
        stock_code: str,
        sentiment: float,
        news_count: int,
        sector_heat: Optional[float] = None,
        market_data: Optional[Dict] = None,
        anomaly_data: Optional[Dict] = None,
        knowledge_graph_impact: Optional[Dict] = None,
    ) -> Dict:
        """
        计算综合影响因子

        Args:
            stock_code: 股票代码
            sentiment: 情绪得分 (-1 ~ 1)
            news_count: 新闻数量
            sector_heat: 板块热度 (0 ~ 1), None 则自动查询
            market_data: 行情数据字典（可选）
            anomaly_data: 异常检测结果（可选）
            knowledge_graph_impact: 知识图谱推理结果（可选）

        Returns:
            {
                "impact_score": 0.75,         # -1 ~ 1 的影响分
                "level": "利好",               # 影响等级
                "confidence": 0.85,            # 评估置信度
                "reasons": [...],              # 评估理由列表
                "components": {                # 各维度评分
                    "sentiment_score": 0.7,
                    "heat_score": 0.6,
                    "volume_score": 0.5,
                    "volatility_score": 0.0,
                },
                "historical_context": {...}    # 历史对比
            }
        """
        reasons: List[str] = []
        components: Dict[str, float] = {}

        try:
            # 1. 情感维度
            sentiment_score = self._score_sentiment(sentiment, news_count)
            components["sentiment_score"] = sentiment_score
            if abs(sentiment_score) > 0.3:
                direction = "积极" if sentiment_score > 0 else "消极"
                reasons.append(f"情绪{direction}（{sentiment:.2f}）")

            # 2. 板块热度维度
            if sector_heat is None:
                sector_heat = self._query_sector_heat(stock_code)
            heat_score = self._score_heat(sector_heat)
            components["heat_score"] = heat_score
            if abs(heat_score) > 0.3:
                reasons.append(f"板块热度{'高' if heat_score > 0 else '低'}（{sector_heat:.2f}）")

            # 3. 新闻数量维度
            volume_score = self._score_volume(news_count)
            components["volume_score"] = volume_score
            if news_count > 0:
                reasons.append(f"相关新闻 {news_count} 条")

            # 4. 历史波动率维度
            volatility_score = 0.0
            hist_volatility = self._query_historical_volatility(stock_code)
            if hist_volatility is not None:
                volatility_score = self._score_volatility(hist_volatility, sentiment)
                components["volatility_score"] = volatility_score
                if abs(volatility_score) > 0.3:
                    reasons.append(f"历史波动率{'高' if hist_volatility > 0.3 else '低'}（{hist_volatility:.2f}）")

            # 5. 异常舆情修正
            anomaly_bonus = 0.0
            if anomaly_data and anomaly_data.get("is_anomaly"):
                anomaly_bonus = self._score_anomaly(anomaly_data)
                reasons.append(f"异常舆情: {anomaly_data.get('reason', '')}")

            # 6. 知识图谱推理修正
            kg_bonus = 0.0
            if knowledge_graph_impact:
                kg_bonus = self._score_knowledge_graph(knowledge_graph_impact)
                impacts = knowledge_graph_impact.get("impact_reasoning", [])
                for imp in impacts[:3]:
                    reasons.append(f"推理: {imp}")

            # 7. 综合评分
            weights = {
                "sentiment_score": 0.40,
                "heat_score": 0.20,
                "volume_score": 0.15,
                "volatility_score": 0.10,
            }

            weighted_sum = 0.0
            total_weight = 0.0
            for key, weight in weights.items():
                if key in components:
                    weighted_sum += components[key] * weight
                    total_weight += weight

            if total_weight > 0:
                base_score = weighted_sum / total_weight
            else:
                base_score = sentiment  # 兜底

            # 加上修正因子
            impact_score = base_score + anomaly_bonus * 0.3 + kg_bonus * 0.2

            # 压缩到 [-1, 1]
            impact_score = math.tanh(impact_score * 2)

            # 8. 确定等级
            level = self._determine_level(impact_score)

            # 9. 置信度
            confidence = self._calc_confidence(news_count, sentiment, len(components))

            # 10. 历史对比
            historical_context = self._get_historical_context(stock_code, sentiment)

        except Exception as e:
            logger.error(f"计算影响因子异常 ({stock_code}): {e}")
            return {
                "impact_score": sentiment * 0.5,
                "level": "中性",
                "confidence": 0.3,
                "reasons": [f"评估异常: {e}"],
                "components": {"sentiment_score": sentiment},
                "historical_context": {},
            }

        return {
            "impact_score": round(impact_score, 4),
            "level": level,
            "confidence": round(confidence, 4),
            "reasons": reasons[:8],
            "components": components,
            "historical_context": historical_context,
        }

    # ──────────────────────────────────────────
    # 各维度评分
    # ──────────────────────────────────────────

    def _score_sentiment(self, sentiment: float, news_count: int) -> float:
        """
        情感维度评分
        双曲正切压缩 + 新闻数量置信度修正
        """
        if news_count == 0:
            return sentiment * 0.3  # 无新闻时降低置信度

        # 新闻数量越多，情绪信号的置信度越高
        confidence = min(math.log10(news_count + 1) / 1.5, 1.0)
        score = sentiment * (0.5 + 0.5 * confidence)

        # 极端情绪放大
        if abs(sentiment) > 0.7:
            score = score * 1.2

        return max(-1.0, min(1.0, score))

    def _score_heat(self, heat: Optional[float]) -> float:
        """板块热度评分"""
        if heat is None:
            return 0.0
        # heat 在 0~1 之间
        return (heat - 0.5) * 2  # 映射到 -1~1

    def _score_volume(self, news_count: int) -> float:
        """新闻数量评分"""
        if news_count <= 0:
            return -0.3  # 无新闻倾向中性偏负面
        if news_count <= 3:
            return 0.0
        if news_count <= 10:
            return 0.3
        if news_count <= 30:
            return 0.5
        return 0.7  # 大量新闻往往意味着市场高度关注

    def _score_volatility(self, volatility: float, sentiment: float) -> float:
        """
        波动率评分
        高波动率时：正向情绪更容易产生正影响
        """
        if volatility is None:
            return 0.0
        # 波动率在 0~1 之间（归一化）
        # 在高波动环境下，情绪影响会被放大
        bonus = volatility * 0.3
        return sentiment * bonus

    def _score_anomaly(self, anomaly: Dict) -> float:
        """异常舆情修正分"""
        if not anomaly.get("is_anomaly"):
            return 0.0

        severity = anomaly.get("severity", "low")
        anomaly_type = anomaly.get("type", "none")

        severity_map = {"high": 0.8, "medium": 0.5, "low": 0.2}
        sev = severity_map.get(severity, 0.2)

        if anomaly_type == "bullish":
            return sev
        elif anomaly_type == "bearish":
            return -sev
        return 0.0

    def _score_knowledge_graph(self, kg_impact: Dict) -> float:
        """知识图谱推理修正分"""
        if not kg_impact:
            return 0.0

        direct = kg_impact.get("direct_impact", "中性")
        chain_count = len(kg_impact.get("chain_reactions", []))

        score_map = {"重大利好": 0.8, "利好": 0.4, "中性": 0.0, "利空": -0.4, "重大利空": -0.8}
        base = score_map.get(direct, 0.0)

        # 连锁反应越多，影响越值得关注
        chain_bonus = min(chain_count * 0.05, 0.3)  # 最多加0.3

        # 如果直接冲击方向和连锁反应方向一致，强化
        related_impacts = kg_impact.get("chain_reactions", [])
        consistent = True
        for cr in related_impacts[:5]:
            cr_score = score_map.get(cr.get("impact", "中性"), 0)
            if cr_score * base < 0:  # 方向相反
                consistent = False
                break

        if consistent and base != 0:
            chain_bonus *= 1.5

        return base + chain_bonus

    # ──────────────────────────────────────────
    # 等级判定
    # ──────────────────────────────────────────

    def _determine_level(self, score: float) -> str:
        """根据影响分确定等级"""
        if score >= 0.8:
            return "重大利好"
        elif score >= 0.4:
            return "利好"
        elif score >= -0.4:
            return "中性"
        elif score >= -0.8:
            return "利空"
        else:
            return "重大利空"

    # ──────────────────────────────────────────
    # 置信度计算
    # ──────────────────────────────────────────

    def _calc_confidence(self, news_count: int, sentiment: float,
                         component_count: int) -> float:
        """
        计算评估置信度
        """
        # 新闻数量置信度
        news_conf = min(math.log10(news_count + 1) / 2.0, 1.0)

        # 情绪极端性置信度
        sentiment_conf = abs(sentiment) ** 0.5

        # 数据维度置信度
        dim_conf = min(component_count / 4.0, 1.0)

        # 综合
        confidence = news_conf * 0.4 + sentiment_conf * 0.3 + dim_conf * 0.3

        return max(0.1, min(1.0, confidence))

    # ──────────────────────────────────────────
    # 数据查询
    # ──────────────────────────────────────────

    def _query_sector_heat(self, stock_code: str) -> float:
        """
        查询板块热度
        通过数据库获取板块行情数据

        Returns:
            0 ~ 1 的热度值
        """
        if not self.db:
            return 0.5  # 默认中性

        try:
            # 获取该股票所属行业板块
            conn = self.db._connect()
            row = conn.execute(
                "SELECT industry FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()

            if row and row["industry"]:
                industry = row["industry"]
                # 查询板块最近行情
                board_rows = conn.execute("""
                    SELECT change_pct FROM board_index
                    WHERE board_name LIKE ? OR board_code LIKE ?
                    ORDER BY snapshot_time DESC LIMIT 3
                """, (f"%{industry}%", f"%{industry}%")).fetchall()

                if board_rows:
                    avg_change = sum(r["change_pct"] for r in board_rows) / len(board_rows)
                    conn.close()
                    # 将涨跌幅映射到 0~1
                    # 涨5%以上算1，跌5%以上算0
                    heat = (avg_change + 5) / 10
                    return max(0.0, min(1.0, heat))

            conn.close()
        except Exception as e:
            logger.warning(f"查询板块热度失败 ({stock_code}): {e}")

        return 0.5  # 默认中性

    def _query_historical_volatility(self, stock_code: str) -> Optional[float]:
        """
        查询历史波动率
        通过数据库获取近期价格波动

        Returns:
            0 ~ 1 的归一化波动率
        """
        if not self.db:
            return None

        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT change_pct FROM market_snapshots
                WHERE stock_code = ?
                ORDER BY snapshot_time DESC LIMIT 20
            """, (stock_code,)).fetchall()

            if rows and len(rows) >= 5:
                changes = [abs(r["change_pct"]) for r in rows if r["change_pct"] is not None]
                if changes:
                    avg_volatility = sum(changes) / len(changes)
                    conn.close()
                    # 归一化：10%以上为高波动
                    normalized = min(avg_volatility / 10.0, 1.0)
                    return normalized

            conn.close()
        except Exception as e:
            logger.warning(f"查询历史波动率失败 ({stock_code}): {e}")

        return None

    def _get_historical_context(self, stock_code: str,
                                current_sentiment: float) -> Dict:
        """
        获取历史对比数据

        Returns:
            {
                "avg_sentiment_7d": 0.2,       # 7日平均情绪
                "sentiment_change": 0.3,          # 与均值的差异
                "sentiment_percentile": 0.85,     # 在历史中的百分位
                "is_extreme": False,              # 是否极端值
            }
        """
        context = {
            "avg_sentiment_7d": None,
            "sentiment_change": None,
            "sentiment_percentile": 0.5,
            "is_extreme": False,
        }

        if not self.db:
            return context

        try:
            from datetime import datetime, timedelta
            conn = self.db._connect()
            seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            rows = conn.execute("""
                SELECT avg_sentiment FROM analysis
                WHERE stock_code = ? AND date >= ?
                ORDER BY date DESC
            """, (stock_code, seven_days_ago)).fetchall()

            if rows and len(rows) >= 2:
                sentiments = [r["avg_sentiment"] for r in rows if r["avg_sentiment"] is not None]
                if sentiments:
                    avg_7d = sum(sentiments) / len(sentiments)
                    context["avg_sentiment_7d"] = round(avg_7d, 4)
                    context["sentiment_change"] = round(current_sentiment - avg_7d, 4)

                    # 百分位估算
                    above_count = sum(1 for s in sentiments if s < current_sentiment)
                    context["sentiment_percentile"] = round(
                        above_count / len(sentiments), 2
                    )

                    # 是否极端（与均值偏差 > 0.5 且超过历史极值）
                    min_s = min(sentiments)
                    max_s = max(sentiments)
                    if current_sentiment < min_s - 0.3 or current_sentiment > max_s + 0.3:
                        context["is_extreme"] = True

            conn.close()
        except Exception as e:
            logger.warning(f"获取历史对比失败 ({stock_code}): {e}")

        return context

    # ──────────────────────────────────────────
    # 便捷方法
    # ──────────────────────────────────────────

    def is_significant(self, impact_factor: Dict) -> bool:
        """
        判断影响是否显著（需要关注）

        Args:
            impact_factor: calculate_impact_factor() 的返回结果

        Returns:
            True 表示显著影响
        """
        level = impact_factor.get("level", "中性")
        return level in ("重大利好", "利好", "利空", "重大利空")

    def summarize(self, impact_factor: Dict) -> str:
        """
        生成影响评估摘要

        Args:
            impact_factor: calculate_impact_factor() 的返回结果

        Returns:
            单行摘要文本
        """
        level = impact_factor.get("level", "中性")
        score = impact_factor.get("impact_score", 0.0)
        confidence = impact_factor.get("confidence", 0.0)
        reasons = impact_factor.get("reasons", [])

        level_icons = {
            "重大利好": "🟢🟢",
            "利好": "🟢",
            "中性": "⚪",
            "利空": "🔴",
            "重大利空": "🔴🔴",
        }
        icon = level_icons.get(level, "⚪")

        top_reasons = reasons[:3]
        reason_str = " → ".join(top_reasons) if top_reasons else "无明显信号"

        return (
            f"{icon} {level} (影响分: {score:.2f}, 置信度: {confidence:.0%})\n"
            f"  推理过程: {reason_str}"
        )

    def compare_stocks(self, analyses: List[Dict]) -> List[Dict]:
        """
        多只股票对比评估

        Args:
            analyses: 个股分析结果列表

        Returns:
            [{"code": ..., "name": ..., "impact_score": ..., "level": ..., "delta": ...}, ...]
            按影响分绝对值降序
        """
        results = []
        for analysis in analyses:
            try:
                code = analysis.get("code", "")
                sentiment = analysis.get("avg_sentiment", 0.0)
                news_count = analysis.get("news_count", 0)

                impact = self.calculate_impact_factor(
                    stock_code=code,
                    sentiment=sentiment,
                    news_count=news_count,
                )

                results.append({
                    "code": code,
                    "name": analysis.get("name", code),
                    "impact_score": impact.get("impact_score", 0.0),
                    "level": impact.get("level", "中性"),
                    "confidence": impact.get("confidence", 0.0),
                    "reasons": impact.get("reasons", [])[:2],
                })
            except Exception as e:
                logger.warning(f"对比评估失败 {analysis.get('code', '?')}: {e}")

        # 按影响分绝对值降序排列
        results.sort(key=lambda x: abs(x.get("impact_score", 0)), reverse=True)
        return results


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    model = ImpactModel()

    # 测试影响评估
    result = model.calculate_impact_factor(
        stock_code="300750",
        sentiment=0.72,
        news_count=15,
        sector_heat=0.7,
        anomaly_data={
            "is_anomaly": True,
            "type": "bullish",
            "severity": "medium",
            "reason": "突发利好：新能源政策密集出台",
        },
        knowledge_graph_impact={
            "direct_impact": "利好",
            "impact_reasoning": [
                "碳酸锂价格下降 → 电池成本降低",
                "下游需求持续增长 → 出货量预期上升",
            ],
            "chain_reactions": [{"impact": "利好"}, {"impact": "利好"}],
        },
    )

    print("===== 影响评估结果 =====")
    print(f"影响分: {result['impact_score']}")
    print(f"等级: {result['level']}")
    print(f"置信度: {result['confidence']:.0%}")
    print(f"理由:")
    for r in result['reasons']:
        print(f"  → {r}")
    print(f"\n摘要: {model.summarize(result)}")
