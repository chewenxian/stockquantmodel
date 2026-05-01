"""
信号分级系统
基于因子共振的信号过滤与分级

信号级别：
- S级（强烈）：新闻利好 + 技术指标共振 + 主力资金流入 + 板块热度高
- A级（强）：新闻利好 + 部分技术指标支持 + 资金或板块配合
- B级（中）：仅有新闻或单一技术面信号
- C级（弱）：信号薄弱或矛盾
- 无效：数据不足或信号不明确

增强信号需要多重因子共振才触发
"""
import logging
import math
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SignalGrader:
    """
    信号分级与过滤系统

    分级依据：
    1. 新闻情绪 (sentiment) - 基础分
    2. 技术指标 (technical) - 增强/减弱
    3. 资金流向 (money flow) - 增强
    4. 板块热度 (sector heat) - 增强
    """

    # 信号级别及索引
    LEVELS = ["S", "A", "B", "C", "无效"]
    LEVEL_SCORE = {"S": 4, "A": 3, "B": 2, "C": 1, "无效": 0}

    def __init__(self, db=None):
        self.db = db

    # ──────────────────────────────────────────
    # 核心分级
    # ──────────────────────────────────────────

    def grade_signal(
        self,
        stock_code: str,
        news_sentiment: float,
        tech_data: Optional[Dict] = None,
        money_flow: Optional[Dict] = None,
        sector_heat: Optional[float] = None,
    ) -> Dict:
        """
        综合评估信号等级

        Args:
            stock_code: 股票代码
            news_sentiment: 新闻情绪值 (-1 ~ 1)
            tech_data: 技术指标字典（来自 TechnicalIndicator）
            money_flow: 资金流向数据（来自数据库 money_flow 表）
            sector_heat: 板块热度 (0~1)

        Returns:
            {
                "level": "S",           # 信号级别
                "score": 3.5,            # 综合评分 (0~5)
                "confidence": 0.85,      # 置信度 (0~1)
                "direction": "利好",     # 方向
                "factors": {             # 各因子评分
                    "sentiment": 0.8,
                    "technical": 0.7,
                    "money_flow": 0.6,
                    "sector_heat": 0.5,
                },
                "reasons": [...],        # 信号理由
                "warnings": [...],       # 风险提示
            }
        """
        try:
            # 如果数据库未传入但有数据参数，自动查询缺失数据
            if tech_data is None and self.db:
                tech_data = self._query_tech_data(stock_code)
            if money_flow is None and self.db:
                money_flow = self._query_money_flow(stock_code)
            if sector_heat is None and self.db:
                sector_heat = self._query_sector_heat(stock_code)

            factors: Dict[str, float] = {}
            reasons: List[str] = []
            warnings: List[str] = []

            # 1. 新闻情绪因子
            sentiment_factor = self._score_sentiment(news_sentiment)
            factors["sentiment"] = sentiment_factor
            if abs(sentiment_factor) > 0.3:
                dir_text = "利好" if sentiment_factor > 0 else "利空"
                reasons.append(f"新闻情绪{dir_text} ({news_sentiment:.2f})")

            # 2. 技术指标因子
            tech_factor = self._score_technical(tech_data, news_sentiment)
            factors["technical"] = tech_factor
            if tech_factor and abs(tech_factor) > 0.3:
                tech_reasons = self._get_tech_reasons(tech_data)
                reasons.extend(tech_reasons[:2])

            # 3. 资金流向因子
            flow_factor = self._score_money_flow(money_flow)
            factors["money_flow"] = flow_factor
            if flow_factor and abs(flow_factor) > 0.3:
                if money_flow and money_flow.get("main_net", 0) > 0:
                    reasons.append("主力资金净流入")
                elif money_flow and money_flow.get("main_net", 0) < 0:
                    reasons.append("主力资金净流出")

            # 4. 板块热度因子
            heat_factor = self._score_sector_heat(sector_heat)
            factors["sector_heat"] = heat_factor
            if heat_factor and abs(heat_factor) > 0.3:
                if heat_factor > 0:
                    reasons.append("板块热度较高")
                else:
                    reasons.append("板块热度偏低")

            # 计算方向
            direction = self._determine_direction(factors)

            # 计算综合评分
            signal_score = self._calculate_signal_score(factors, direction)

            # 确定信号级别
            level = self._determine_level(signal_score, factors, direction)

            # 置信度
            confidence = self._calc_confidence(factors)

            # 检查冲突信号（生成警告）
            warnings = self._detect_conflicts(
                factors, tech_data, money_flow, stock_code
            )

        except Exception as e:
            logger.error(f"信号分级异常 ({stock_code}): {e}")
            return {
                "level": "无效",
                "score": 0.0,
                "confidence": 0.0,
                "direction": "中性",
                "factors": {},
                "reasons": [f"分级异常: {e}"],
                "warnings": ["评估失败"],
            }

        return {
            "level": level,
            "score": round(signal_score, 2),
            "confidence": round(confidence, 4),
            "direction": direction,
            "factors": factors,
            "reasons": reasons[:6],
            "warnings": warnings[:3],
        }

    # ──────────────────────────────────────────
    # 各因子评分
    # ──────────────────────────────────────────

    def _score_sentiment(self, sentiment: float) -> float:
        """
        新闻情绪因子评分

        Returns:
            -1 ~ 1
        """
        # 阈值过滤：弱情绪忽略
        if abs(sentiment) < 0.15:
            return 0.0

        # 非线性放大
        result = math.copysign(
            min(abs(sentiment) ** 0.75 * 1.2, 1.0),
            sentiment
        )
        return result

    def _score_technical(self, tech_data: Optional[Dict],
                         sentiment: float) -> float:
        """
        技术指标因子评分

        从技术面判断信号方向：
        - RSI 超卖/超买 + 价格在布林带位置 + 均线排列
        - 与技术信号方向一致则增强，相反则减弱

        Returns:
            -1 ~ 1 (与情绪方向一致为正)
        """
        if not tech_data:
            return 0.0

        score = 0.0
        components = 0

        try:
            # RSI 信号
            rsi = tech_data.get("rsi")
            rsi_signal = tech_data.get("rsi_signal", "正常")
            if rsi is not None:
                if rsi <= 30:  # 超卖 → 技术面看涨
                    score += 0.6
                elif rsi <= 40:  # 偏卖 → 偏涨
                    score += 0.3
                elif rsi >= 70:  # 超买 → 技术面看跌
                    score -= 0.6
                elif rsi >= 60:  # 偏买 → 偏跌
                    score -= 0.3
                # 40~60 之间不贡献
                components += 1

            # MACD 信号
            macd = tech_data.get("macd")
            macd_signal = tech_data.get("macd_signal", "震荡")
            if macd_signal == "金叉":
                score += 0.5
                components += 1
            elif macd_signal == "死叉":
                score -= 0.5
                components += 1
            elif macd:
                # 即使没有明确信号，DIF 方向也有参考价值
                dif = macd.get("dif")
                if dif is not None and abs(dif) > 0.01:
                    score += 0.15 if dif > 0 else -0.15
                    components += 1

            # 均线排列
            price_vs_ma = tech_data.get("price_vs_ma", "未知")
            if price_vs_ma == "多头排列":
                score += 0.5
                components += 1
            elif price_vs_ma == "空头排列":
                score -= 0.5
                components += 1
            elif "向上突破" in price_vs_ma:
                score += 0.3
                components += 1
            elif "向下突破" in price_vs_ma:
                score -= 0.3
                components += 1

            # 布林带位置
            bollinger = tech_data.get("bollinger", {})
            position = bollinger.get("position")
            if position is not None:
                if position > 0.9:  # 触碰上轨 → 超买
                    score += 0.5  # 强趋势中高位有支撑
                    components += 1
                elif position < 0.1:  # 触碰下轨 → 超卖
                    score -= 0.5  # 强下跌中低位风险
                    components += 1
                elif 0.3 <= position <= 0.7:
                    # 中位偏正
                    score += 0.1
                    components += 1

            # 归一化
            if components > 0:
                score = score / components

                # 与情绪方向一致性修正
                # 技术面和情绪方向一致 → 增强；相反 → 减弱
                if abs(sentiment) > 0.3:
                    if (sentiment > 0 and score < 0) or (sentiment < 0 and score > 0):
                        score *= 0.5  # 矛盾时减半

        except Exception as e:
            logger.warning(f"技术因子评分异常: {e}")

        return max(-1.0, min(1.0, score))

    def _score_money_flow(self, money_flow: Optional[Dict]) -> float:
        """
        资金流向因子评分

        主力净流入为正 → 利好
        主力净流入为负 → 利空

        Returns:
            -1 ~ 1
        """
        if not money_flow:
            return 0.0

        try:
            main_net = money_flow.get("main_net", 0)
            if main_net == 0:
                return 0.0

            # 计算相对比例（相对于总成交额）
            total_amount = money_flow.get("total_amount", 0)
            if total_amount and total_amount > 0:
                ratio = main_net / total_amount
            else:
                ratio = 0.01  # 默认小比例

            # 主力净流入/流出比例归一化到 -1~1
            # 通常主力资金净流入比例在 -5%~5% 之间
            score = ratio * 20  # 放大到 -1~1
            score = max(-1.0, min(1.0, score))

            return score

        except Exception as e:
            logger.warning(f"资金流向因子评分异常: {e}")

        return 0.0

    def _score_sector_heat(self, sector_heat: Optional[float]) -> float:
        """
        板块热度因子评分

        Args:
            sector_heat: 0~1 的热度值

        Returns:
            -1 ~ 1
        """
        if sector_heat is None:
            return 0.0

        try:
            # 0.5 中性，映射到 -1~1
            score = (sector_heat - 0.5) * 2
            return max(-1.0, min(1.0, score))

        except Exception as e:
            logger.warning(f"板块热度因子评分异常: {e}")

        return 0.0

    # ──────────────────────────────────────────
    # 综合评分与分级
    # ──────────────────────────────────────────

    def _determine_direction(self, factors: Dict[str, float]) -> str:
        """
        根据各因子方向确定总方向
        """
        positive = sum(max(v, 0) for v in factors.values())
        negative = sum(abs(min(v, 0)) for v in factors.values())

        if positive > negative * 2:
            return "利好"
        elif negative > positive * 2:
            return "利空"
        elif positive > 0 or negative > 0:
            return "中性偏多" if positive > negative else "中性偏空"
        return "中性"

    def _calculate_signal_score(self, factors: Dict[str, float],
                                 direction: str) -> float:
        """
        计算综合信号评分 (0~5)
        """
        if not factors:
            return 0.0

        weights = {
            "sentiment": 0.30,
            "technical": 0.30,
            "money_flow": 0.25,
            "sector_heat": 0.15,
        }

        weighted_sum = 0.0
        total_weight = 0.0

        abs_sum = 0.0
        for key, w in weights.items():
            v = factors.get(key, 0)
            weighted_sum += abs(v) * w
            abs_sum += abs(v)
            total_weight += w

        # 信号强度（绝对值加权）
        if total_weight > 0:
            score = weighted_sum / total_weight
        else:
            score = 0.0

        # 乘以方向因子（一致性越好越强）
        if direction == "利好":
            direction_bonus = 1.0
        elif direction == "利空":
            direction_bonus = 1.0
        else:
            direction_bonus = 0.5  # 方向不明确时减半

        # 因子多样性奖励（多因子共振）
        active_factors = sum(1 for v in factors.values() if abs(v) > 0.2)
        diversity_bonus = min(active_factors / 3.0, 1.0) * 0.3

        score = score * direction_bonus + diversity_bonus

        return max(0.0, min(5.0, score))

    def _determine_level(self, score: float, factors: Dict[str, float],
                          direction: str) -> str:
        """
        根据综合评分确定信号等级

        S级 → 多重因子共振，方向明确
        A级 → 主要因子支持，有一定共振
        B级 → 单一因子支持
        C级 → 信号薄弱
        无效 → 无明显信号
        """
        # 超高分
        if score >= 4.0:
            return "S"

        # 高分，且至少有3个因子活动
        active_factors = sum(1 for v in factors.values() if abs(v) > 0.2)
        if score >= 2.5:
            if active_factors >= 2 and direction in ("利好", "利空"):
                if active_factors >= 3 and score >= 3.0:
                    return "S"
                return "A"
            return "B"

        # 中等
        if score >= 1.0:
            return "B" if active_factors >= 1 else "C"

        # 低分
        if score >= 0.3:
            return "C"

        return "无效"

    def _calc_confidence(self, factors: Dict[str, float]) -> float:
        """
        计算信号置信度

        基于：
        - 因子数量
        - 因子一致性
        - 因子强度
        """
        if not factors:
            return 0.0

        # 因子活跃度
        active = sum(1 for v in factors.values() if abs(v) > 0.2)

        # 因子一致性
        signs = []
        for v in factors.values():
            if abs(v) > 0.2:
                signs.append(1 if v > 0 else -1)

        if len(signs) >= 2:
            consistent = all(s == signs[0] for s in signs)
        else:
            consistent = True

        # 平均强度
        avg_strength = sum(abs(v) for v in factors.values()) / len(factors)

        # 综合置信度
        active_conf = min(active / 3.0, 1.0) * 0.4
        consistency_conf = 0.3 if consistent else 0.1
        strength_conf = min(avg_strength, 1.0) * 0.3

        confidence = active_conf + consistency_conf + strength_conf

        return max(0.0, min(1.0, confidence))

    # ──────────────────────────────────────────
    # 冲突检测
    # ──────────────────────────────────────────

    def _detect_conflicts(self, factors: Dict[str, float],
                          tech_data: Optional[Dict],
                          money_flow: Optional[Dict],
                          stock_code: str) -> List[str]:
        """
        检测信号冲突
        """
        warnings = []

        try:
            # 情绪与技术面冲突
            sentiment = factors.get("sentiment", 0)
            technical = factors.get("technical", 0)

            if abs(sentiment) > 0.3 and abs(technical) > 0.3:
                if (sentiment > 0 and technical < 0) or (sentiment < 0 and technical > 0):
                    warnings.append("新闻情绪与技术面信号方向相反")

            # 情绪与资金流向冲突
            flow = factors.get("money_flow", 0)
            if abs(sentiment) > 0.3 and abs(flow) > 0.3:
                if (sentiment > 0 and flow < 0) or (sentiment < 0 and flow > 0):
                    warnings.append("新闻情绪与主力资金动向相反")

            # RSI 极端值警告
            if tech_data:
                rsi = tech_data.get("rsi")
                rsi_signal = tech_data.get("rsi_signal", "正常")
                if sentiment > 0.3 and rsi_signal == "超买":
                    warnings.append(f"RSI={rsi} 超买区域，追涨风险较大")
                elif sentiment < -0.3 and rsi_signal == "超卖":
                    warnings.append(f"RSI={rsi} 超卖区域，杀跌需谨慎")

            # MACD 与趋势背离
            macd = tech_data.get("macd") if tech_data else None
            if macd and sentiment > 0.3:
                macd_signal = tech_data.get("macd_signal", "震荡")
                if macd_signal == "死叉":
                    warnings.append("利好情绪但MACD死叉，注意短期风险")

        except Exception as e:
            logger.warning(f"冲突检测异常 ({stock_code}): {e}")

        return warnings

    # ──────────────────────────────────────────
    # 技术理由提取
    # ──────────────────────────────────────────

    def _get_tech_reasons(self, tech_data: Optional[Dict]) -> List[str]:
        """从技术指标提取可读理由"""
        if not tech_data:
            return []

        reasons = []

        rsi = tech_data.get("rsi")
        rsi_signal = tech_data.get("rsi_signal", "正常")
        if rsi is not None:
            reasons.append(f"RSI({rsi}) {rsi_signal}")

        price_vs_ma = tech_data.get("price_vs_ma", "未知")
        if price_vs_ma and price_vs_ma != "未知":
            reasons.append(f"均线{price_vs_ma}")

        macd_signal = tech_data.get("macd_signal")
        if macd_signal and macd_signal != "震荡":
            reasons.append(f"MACD{macd_signal}")

        bollinger = tech_data.get("bollinger", {})
        position = bollinger.get("position")
        if position is not None:
            if position > 0.8:
                reasons.append("布林带高位运行")
            elif position < 0.2:
                reasons.append("布林带低位运行")

        return reasons

    # ──────────────────────────────────────────
    # 数据查询
    # ──────────────────────────────────────────

    def _query_tech_data(self, stock_code: str) -> Optional[Dict]:
        """查询技术指标数据"""
        try:
            from .technical import TechnicalIndicator
            ti = TechnicalIndicator(self.db)
            return ti.get_all_indicators(stock_code)
        except Exception as e:
            logger.warning(f"查询技术指标失败 ({stock_code}): {e}")
            return None

    def _query_money_flow(self, stock_code: str) -> Optional[Dict]:
        """查询资金流向数据"""
        if not self.db:
            return None
        try:
            return self.db.get_latest_money_flow(stock_code)
        except Exception as e:
            logger.warning(f"查询资金流向失败 ({stock_code}): {e}")
            return None

    def _query_sector_heat(self, stock_code: str) -> Optional[float]:
        """查询板块热度"""
        if not self.db:
            return None
        try:
            conn = self.db._connect()
            row = conn.execute(
                "SELECT industry FROM stocks WHERE code = ?", (stock_code,)
            ).fetchone()
            if row and row["industry"]:
                industry = row["industry"]
                board_rows = conn.execute("""
                    SELECT change_pct FROM board_index
                    WHERE board_name LIKE ? OR board_code LIKE ?
                    ORDER BY snapshot_time DESC LIMIT 3
                """, (f"%{industry}%", f"%{industry}%")).fetchall()
                if board_rows:
                    avg_change = sum(r["change_pct"] for r in board_rows) / len(board_rows)
                    heat = (avg_change + 5) / 10
                    return max(0.0, min(1.0, heat))
        except Exception as e:
            logger.warning(f"查询板块热度失败 ({stock_code}): {e}")
        finally:
            try: conn.close()
            except: pass
        return 0.5

    # ──────────────────────────────────────────
    # 便捷方法
    # ──────────────────────────────────────────

    def is_signal(self, grade_result: Dict) -> bool:
        """是否有有效信号"""
        return grade_result.get("level", "无效") not in ("无效", "C")

    def is_strong_signal(self, grade_result: Dict) -> bool:
        """是否为强信号 (S/A级)"""
        return grade_result.get("level", "无效") in ("S", "A")

    def get_level_order(self, level: str) -> int:
        """信号级别排序值"""
        return self.LEVEL_SCORE.get(level, 0)

    def summarize(self, grade_result: Dict) -> str:
        """
        生成信号摘要
        """
        level = grade_result.get("level", "无效")
        score = grade_result.get("score", 0.0)
        direction = grade_result.get("direction", "中性")
        reasons = grade_result.get("reasons", [])
        warnings = grade_result.get("warnings", [])

        level_icons = {"S": "🔴🔴", "A": "🔴", "B": "🟡", "C": "🟢", "无效": "⚪"}
        icon = level_icons.get(level, "⚪")

        lines = [f"{icon} {level}级信号 (评分: {score:.1f}, 方向: {direction})"]

        if reasons:
            lines.append(f"  理由: {' | '.join(reasons[:4])}")

        if warnings:
            lines.append(f"  ⚠️ {' | '.join(warnings[:2])}")

        return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    grader = SignalGrader()

    # 测试场景1：强利好信号
    print("===== 场景1: S级信号（多重共振利好） =====")
    result1 = grader.grade_signal(
        stock_code="300750",
        news_sentiment=0.72,
        tech_data={
            "rsi": 32,
            "rsi_signal": "超卖",
            "price_vs_ma": "短期向上突破",
            "macd": {"dif": 0.5, "dea": 0.2},
            "macd_signal": "金叉",
            "bollinger": {"upper": 260, "middle": 240, "lower": 220, "position": 0.15},
        },
        money_flow={"main_net": 50000000, "total_amount": 500000000},
        sector_heat=0.75,
    )
    print(grader.summarize(result1))

    # 测试场景2：中性偏弱
    print("\n===== 场景2: C级信号（微弱） =====")
    result2 = grader.grade_signal(
        stock_code="600519",
        news_sentiment=0.12,
        tech_data={
            "rsi": 55,
            "rsi_signal": "正常",
            "price_vs_ma": "震荡整理",
            "macd": {"dif": 0.02, "dea": 0.01},
            "macd_signal": "震荡",
            "bollinger": {"upper": 200, "middle": 190, "lower": 180, "position": 0.5},
        },
        money_flow={"main_net": 1000000, "total_amount": 300000000},
        sector_heat=0.48,
    )
    print(grader.summarize(result2))

    # 测试场景3：冲突信号
    print("\n===== 场景3: 信号冲突 =====")
    result3 = grader.grade_signal(
        stock_code="002594",
        news_sentiment=0.65,
        tech_data={
            "rsi": 78,
            "rsi_signal": "超买",
            "price_vs_ma": "多头排列",
            "macd": {"dif": 1.2, "dea": 0.8},
            "macd_signal": "金叉",
            "bollinger": {"upper": 350, "middle": 300, "lower": 250, "position": 0.85},
        },
        money_flow={"main_net": -80000000, "total_amount": 600000000},
        sector_heat=0.82,
    )
    print(grader.summarize(result3))
    print(f"\n警告: {result3.get('warnings', [])}")
