"""
交易建议生成器
基于多维度数据生成买卖建议

功能：
1. 根据情绪分析、新闻数量、市场数据计算置信度
2. 生成分级交易建议
3. 风险评估
"""
import logging
import json
import math
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class TradingAdvisor:
    """
    交易建议生成器
    综合情绪、新闻、市场数据生成操作建议
    """

    # 建议等级定义
    SUGGESTION_LEVELS = [
        "强烈买入",
        "买入",
        "持有",
        "观望",
        "卖出",
        "强烈卖出",
    ]

    RISK_LEVELS = ["高", "中", "低"]

    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        advice_cfg = self.config.get("advice", {})

        self.min_confidence = advice_cfg.get("min_confidence", 0.3)
        self.strong_buy_threshold = advice_cfg.get("strong_buy_threshold", 0.7)
        self.buy_threshold = advice_cfg.get("buy_threshold", 0.3)
        self.sell_threshold = advice_cfg.get("sell_threshold", -0.3)
        self.strong_sell_threshold = advice_cfg.get("strong_sell_threshold", -0.7)

    def _load_config(self, config_path: Optional[str] = None) -> dict:
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
                             market_data: Optional[Dict] = None) -> float:
        """
        计算建议置信度

        考虑因素：
        1. 情绪强度（越极端置信度越高）
        2. 新闻数量（越多置信度越高，但边际递减）
        3. 市场数据一致性（情绪与走势一致时置信度更高）
        4. 综合非线性变换

        Args:
            sentiment: 情绪评分 (-1 ~ 1)
            news_count: 相关新闻数量
            market_data: 市场数据字典（可选）

        Returns:
            float: 0 ~ 1 的置信度
        """
        # 1. 情绪强度因子
        sentiment_strength = abs(sentiment)
        sentiment_factor = sentiment_strength ** 0.7  # 非线性放大

        # 2. 新闻数量因子（对数增长）
        if news_count <= 0:
            news_factor = 0.1
        else:
            news_factor = min(math.log10(news_count + 1) / 2.0, 1.0)

        # 3. 市场数据一致性因子
        market_factor = 0.5  # 默认中性
        if market_data:
            change_pct = market_data.get("change_pct", 0)
            main_net = market_data.get("main_net", 0)

            # 情绪与走势一致时加分
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

        # 4. 综合计算
        raw_confidence = (
            sentiment_factor * 0.5 +
            news_factor * 0.3 +
            market_factor * 0.2
        )

        # 5. 确保在 [0, 1] 区间
        confidence = max(0.0, min(1.0, raw_confidence))

        return round(confidence, 4)

    # ──────────────────────────────────────────
    # 建议生成
    # ──────────────────────────────────────────

    def generate_advice(self, stock_analysis: Dict) -> Dict:
        """
        生成个股买卖建议

        Args:
            stock_analysis: 个股分析结果，包含：
                - code: 股票代码
                - name: 股票名称
                - avg_sentiment: 平均情绪
                - news_count: 新闻数量
                - key_topics: 关键主题
                - risk_warnings: 风险提示
                - market_data: 市场数据（可选）

        Returns:
            {
                "code": "600519",
                "name": "贵州茅台",
                "suggestion": "买入",
                "confidence": 0.75,
                "risk_level": "中",
                "reason": "...",
                "sentiment": 0.5,
                "news_count": 10,
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

        # 计算置信度
        confidence = self.calculate_confidence(sentiment, news_count, market_data)

        # 如果置信度太低，默认持有
        if confidence < self.min_confidence:
            suggestion = "持有"
            risk_level = "中"
            reason_parts = ["信息不足，暂不建议操作"]

        else:
            # 根据情绪阈值确定建议等级
            if sentiment >= self.strong_buy_threshold:
                suggestion = "强烈买入"
                risk_level = "低" if anomaly.get("type") == "bullish" else "中"
            elif sentiment >= self.buy_threshold:
                suggestion = "买入"
                risk_level = "低"
            elif sentiment >= self.sell_threshold:
                suggestion = "持有"
                risk_level = "中"
            elif sentiment >= self.strong_sell_threshold:
                suggestion = "观望"
                risk_level = "中"
                if anomaly.get("type") == "bearish":
                    suggestion = "卖出"
                    risk_level = "高"
            else:
                suggestion = "强烈卖出"
                risk_level = "高"

            # 异常舆情修正
            if anomaly.get("is_anomaly"):
                if anomaly["type"] == "bearish" and anomaly["severity"] == "high":
                    if suggestion not in ["强烈卖出"]:
                        suggestion = self._downgrade_suggestion(suggestion)
                    risk_level = "高"

            # 构建建议理由
            reason_parts = self._build_reason(sentiment, news_count, key_topics,
                                              risk_warnings, anomaly, market_data)

        return {
            "code": code,
            "name": name,
            "suggestion": suggestion,
            "confidence": confidence,
            "risk_level": risk_level,
            "reason": "；".join(reason_parts),
            "sentiment": sentiment,
            "news_count": news_count,
            "key_topics": key_topics[:5],
            "risk_warnings": risk_warnings[:3],
            "timestamp": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    def _downgrade_suggestion(self, current: str) -> str:
        """降级建议（向更保守方向调整）"""
        downgrade_map = {
            "强烈买入": "买入",
            "买入": "持有",
            "持有": "观望",
            "观望": "卖出",
            "卖出": "强烈卖出",
            "强烈卖出": "强烈卖出",
        }
        return downgrade_map.get(current, "持有")

    def _build_reason(self, sentiment: float, news_count: int,
                      key_topics: List[str], risk_warnings: List[str],
                      anomaly: Dict, market_data: Optional[Dict]) -> List[str]:
        """构建建议理由"""
        parts = []

        # 情绪描述
        if sentiment > 0.5:
            parts.append(f"市场情绪积极 (评分 {sentiment:.2f})")
        elif sentiment > 0:
            parts.append(f"市场情绪略偏正面 (评分 {sentiment:.2f})")
        elif sentiment > -0.5:
            parts.append(f"市场情绪略偏负面 (评分 {sentiment:.2f})")
        else:
            parts.append(f"市场情绪消极 (评分 {sentiment:.2f})")

        # 新闻热度
        if news_count > 0:
            parts.append(f"近期 {news_count} 条相关新闻")

        # 关键主题
        if key_topics:
            topic_str = "、".join(key_topics[:3])
            parts.append(f"热点主题：{topic_str}")

        # 异常舆情
        if anomaly.get("is_anomaly"):
            parts.append(f"⚠️ {anomaly.get('reason', '检测到异常舆情')}")

        # 风险提示
        if risk_warnings:
            parts.append(f"风险：{risk_warnings[0]}")

        # 市场数据
        if market_data:
            change = market_data.get("change_pct", 0)
            if change:
                direction = "上涨" if change > 0 else "下跌"
                parts.append(f"当前股价{direction} {abs(change):.2f}%")

        return parts

    def generate_advice_for_all(self, analyses: List[Dict]) -> List[Dict]:
        """
        为多只股票生成建议

        Args:
            analyses: 个股分析结果列表

        Returns:
            建议结果列表
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
                    "risk_level": "中",
                    "reason": f"分析失败: {e}",
                    "sentiment": 0.0,
                    "news_count": 0,
                    "key_topics": [],
                    "risk_warnings": [],
                    "timestamp": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
                })
        return advices


import os


# 测试用
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    advisor = TradingAdvisor()

    test_data = {
        "code": "600519",
        "name": "贵州茅台",
        "avg_sentiment": 0.65,
        "news_count": 15,
        "key_topics": ["业绩增长", "提价", "外资买入"],
        "risk_warnings": ["大盘回调风险"],
        "anomaly": {"is_anomaly": False, "type": "none"},
        "market_data": {"change_pct": 2.5, "main_net": 5000},
    }

    advice = advisor.generate_advice(test_data)
    print(json.dumps(advice, ensure_ascii=False, indent=2))
