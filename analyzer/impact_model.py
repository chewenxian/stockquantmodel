"""
情报影响评估模型（v5.0 增强版）
综合：情感得分 + 板块热度 + 新闻数量 + 历史波动率 + 技术指标 + 信号分级
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
        tech_data: Optional[Dict] = None,
    ) -> Dict:
        """
        计算综合影响因子 (v5.0 集成技术指标)

        Args:
            stock_code: 股票代码
            sentiment: 情绪得分 (-1 ~ 1)
            news_count: 新闻数量
            sector_heat: 板块热度 (0 ~ 1), None 则自动查询
            market_data: 行情数据字典（可选）
            anomaly_data: 异常检测结果（可选）
            knowledge_graph_impact: 知识图谱推理结果（可选）
            tech_data: 技术指标数据（来自 TechnicalIndicator），
                       为 None 时自动查询

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
                    "technical_score": 0.6,     # 新增：技术指标评分
                },
                "historical_context": {...}    # 历史对比
                "signal_level": "A",           # 新增：信号分级 (S/A/B/C/无效)
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

            # 5. 技术指标维度 (v5.0 新增)
            technical_score, tech_reasons = self._score_technical(tech_data, sentiment, stock_code)
            components["technical_score"] = technical_score
            reasons.extend(tech_reasons)

            # 6. 异常舆情修正
            anomaly_bonus = 0.0
            if anomaly_data and anomaly_data.get("is_anomaly"):
                anomaly_bonus = self._score_anomaly(anomaly_data)
                reasons.append(f"异常舆情: {anomaly_data.get('reason', '')}")

            # 7. 知识图谱推理修正
            kg_bonus = 0.0
            if knowledge_graph_impact:
                kg_bonus = self._score_knowledge_graph(knowledge_graph_impact)
                impacts = knowledge_graph_impact.get("impact_reasoning", [])
                for imp in impacts[:3]:
                    reasons.append(f"推理: {imp}")

            # 8. 综合评分（v5.0 权重调整）
            weights = {
                "sentiment_score": 0.30,
                "heat_score": 0.15,
                "volume_score": 0.10,
                "volatility_score": 0.10,
                "technical_score": 0.20,
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

            # 9. 确定等级
            level = self._determine_level(impact_score)

            # 10. 置信度
            all_components = list(components.keys())
            confidence = self._calc_confidence(news_count, sentiment, len(all_components))

            # 11. 信号分级 (v5.0 新增)
            signal_level = self._evaluate_signal_level(
                impact_score, components, sentiment, stock_code
            )

            # 12. 历史对比
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
                "signal_level": "无效",
            }

        return {
            "impact_score": round(impact_score, 4),
            "level": level,
            "confidence": round(confidence, 4),
            "reasons": reasons[:8],
            "components": components,
            "historical_context": historical_context,
            "signal_level": signal_level,
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

    def _score_technical(self, tech_data: Optional[Dict],
                          sentiment: float,
                          stock_code: str) -> tuple:
        """
        技术指标维度评分 (v5.0 新增)

        Args:
            tech_data: 技术指标字典
            sentiment: 情绪得分
            stock_code: 股票代码（用于自动查询）

        Returns:
            (score: float, reasons: List[str])
        """
        score = 0.0
        reasons: List[str] = []

        try:
            # 如果未提供技术数据且数据库可用，自动查询
            if tech_data is None and self.db:
                from .technical import TechnicalIndicator
                ti = TechnicalIndicator(self.db)
                tech_data = ti.get_all_indicators(stock_code)

            if not tech_data:
                return 0.0, []

            # RSI 信号
            rsi = tech_data.get("rsi")
            rsi_signal = tech_data.get("rsi_signal", "正常")
            if rsi is not None:
                if rsi <= 30:
                    score += 0.5  # 超卖反弹预期
                    reasons.append(f"RSI超卖({rsi})")
                elif rsi >= 70:
                    score -= 0.5  # 超买回调风险
                    reasons.append(f"RSI超买({rsi})")
                elif rsi <= 40:
                    score += 0.2
                    reasons.append(f"RSI偏弱({rsi})")
                elif rsi >= 60:
                    score -= 0.2
                    reasons.append(f"RSI偏强({rsi})")

            # 均线排列
            price_vs_ma = tech_data.get("price_vs_ma", "未知")
            if "多头排列" in price_vs_ma:
                score += 0.4
                reasons.append(f"均线{price_vs_ma}")
            elif "空头排列" in price_vs_ma:
                score -= 0.4
                reasons.append(f"均线{price_vs_ma}")
            elif "突破" in price_vs_ma:
                if "向上" in price_vs_ma:
                    score += 0.3
                else:
                    score -= 0.3
                reasons.append(f"均线{price_vs_ma}")

            # MACD 信号
            macd_signal = tech_data.get("macd_signal", "震荡")
            if macd_signal == "金叉":
                score += 0.4
                reasons.append("MACD金叉")
            elif macd_signal == "死叉":
                score -= 0.4
                reasons.append("MACD死叉")

            # 布林带位置
            bollinger = tech_data.get("bollinger", {})
            position = bollinger.get("position")
            if position is not None:
                if position > 0.85:
                    score += 0.3 * (1 if sentiment > 0 else -1)
                    reasons.append("布林带上轨附近")
                elif position < 0.15:
                    score += 0.3 * (1 if sentiment < 0 else -1)
                    reasons.append("布林带下轨附近")

            # 归一化到 -1~1
            score = max(-1.0, min(1.0, score))

            # 一致性修正：技术方向和情绪方向冲突时降低权重
            if abs(sentiment) > 0.3 and abs(score) > 0.3:
                if (sentiment > 0 and score < 0) or (sentiment < 0 and score > 0):
                    score *= 0.5
                    reasons.append("(技术面与情绪面存在分歧，信号减弱)")

        except Exception as e:
            logger.warning(f"技术指标评分异常 ({stock_code}): {e}")

        return score, reasons

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
    # 信号分级 (v5.0 新增)
    # ──────────────────────────────────────────

    def _evaluate_signal_level(self, impact_score: float,
                                components: Dict[str, float],
                                sentiment: float,
                                stock_code: str) -> str:
        """
        基于多因子共振评估信号级别

        S级: impact ≥ 0.8 且多因子共振
        A级: impact ≥ 0.5 或 强因子支持
        B级: impact ≥ 0.3 或 单一因子明确
        C级: 信号薄弱
        无效: 无明显信号
        """
        try:
            abs_score = abs(impact_score)

            # 统计活跃因子数
            active_factors = sum(
                1 for v in components.values() if abs(v) > 0.2
            )

            # 因子方向一致性
            signs = [1 if v > 0 else -1 for v in components.values() if abs(v) > 0.2]
            consistent = len(set(signs)) == 1 if len(signs) >= 2 else True

            # S级：高分 + 多因子共振 + 方向一致
            if abs_score >= 0.8 and active_factors >= 3 and consistent:
                return "S"

            # A级：高分 或 强因子支持
            if abs_score >= 0.5:
                if active_factors >= 2 and consistent:
                    return "A"
                return "B"

            # B级：中等信号
            if abs_score >= 0.3:
                if active_factors >= 2:
                    return "B"
                return "C"

            # C级：微弱信号
            if abs_score >= 0.15 and active_factors >= 1:
                return "C"

            return "无效"

        except Exception as e:
            logger.warning(f"信号分级评估异常 ({stock_code}): {e}")
            return "无效"

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
        dim_conf = min(component_count / 5.0, 1.0)  # 5个维度

        # 综合
        confidence = news_conf * 0.35 + sentiment_conf * 0.25 + dim_conf * 0.4

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
        signal = impact_factor.get("signal_level", "无效")
        return level in ("重大利好", "利好", "利空", "重大利空") or signal in ("S", "A")

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
        signal_level = impact_factor.get("signal_level", "")
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

        signal_tag = f" [信号级: {signal_level}]" if signal_level else ""

        return (
            f"{icon} {level} (影响分: {score:.2f}, 置信度: {confidence:.0%}){signal_tag}\n"
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
                    "signal_level": impact.get("signal_level", ""),
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

    # 测试影响评估（含技术指标）
    result = model.calculate_impact_factor(
        stock_code="300750",
        sentiment=0.72,
        news_count=15,
        sector_heat=0.7,
        tech_data={
            "rsi": 32,
            "rsi_signal": "超卖",
            "price_vs_ma": "短期向上突破",
            "macd": {"dif": 0.5, "dea": 0.2},
            "macd_signal": "金叉",
            "bollinger": {"upper": 260, "middle": 240, "lower": 220, "position": 0.15},
        },
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

    print("===== 影响评估结果 (v5.0) =====")
    print(f"影响分: {result['impact_score']}")
    print(f"等级: {result['level']}")
    print(f"信号级别: {result['signal_level']}")
    print(f"置信度: {result['confidence']:.0%}")
    print(f"维度评分: {result['components']}")
    print(f"理由:")
    for r in result['reasons']:
        print(f"  → {r}")
    print(f"\n摘要: {model.summarize(result)}")
