"""
综合情绪分析模块
提供多维度的情绪评分、趋势分析、异常检测

功能：
1. 基于新闻文本/标题的规则情绪评分
2. 综合多维度情绪（新闻情绪、资金流向、市场情绪）
3. 情绪趋势分析
4. 异常舆情检测（突发利空/利好）
"""
import logging
import math
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

import yaml

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    综合情绪分析器
    结合规则基础和统计方法进行情绪分析
    """

    # 基础情绪词典（A股常用词）
    POSITIVE_WORDS = set([
        "增长", "大涨", "涨停", "突破", "利好", "盈利", "分红", "中标",
        "合同", "扩产", "创新高", "买入", "增持", "回购", "扭亏", "预增",
        "翻红", "反弹", "放量上涨", "资金流入", "北向买入", "主力买入",
        "放量", "突破新高", "加速", "景气", "供不应求", "提价", "涨价",
        "政策支持", "补贴", "减税", "降准", "降息", "放水",
        "行业龙头", "市场份额提升", "强于大盘", "获得订单",
    ])

    NEGATIVE_WORDS = set([
        "下跌", "大跌", "跌停", "亏损", "减持", "立案", "处罚", "利空",
        "风险", "下调", "降级", "违约", "诉讼", "ST", "退市", "预亏",
        "爆雷", "崩盘", "闪崩", "资金流出", "北向卖出", "主力卖出",
        "缩量下跌", "破位", "跌穿", "出货", "利空出尽",
        "监管", "调查", "警示", "暂停", "终止", "取消",
        "行业下滑", "需求萎缩", "库存积压", "降价", "减薪", "裁员",
    ])

    # 权重调整词（增强/减弱情绪）
    INTENSIFIERS = set(["大幅", "严重", "显著", "明显", "剧烈", "历史性"])
    DIMINISHERS = set(["小幅", "略", "微增", "轻微", "有限", "部分"])

    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.window_days = self.config.get("sentiment", {}).get("window_days", 5)
        self.anomaly_threshold = self.config.get("sentiment", {}).get("anomaly_threshold", 0.6)

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
    # 文本情绪分析（规则基础）
    # ──────────────────────────────────────────

    def analyze_text_sentiment(self, title: str, content: str = "") -> float:
        """
        基于词典规则分析单条文本的情绪值

        Args:
            title: 新闻标题
            content: 新闻正文/摘要

        Returns:
            float: -1.0 ~ 1.0 的情绪值
        """
        text = f"{title} {content}"
        if not text.strip():
            return 0.0

        pos_count = 0
        neg_count = 0

        # 分词匹配（简单双向最大匹配）
        for word in self.POSITIVE_WORDS:
            if word in text:
                pos_count += text.count(word)

        for word in self.NEGATIVE_WORDS:
            if word in text:
                neg_count += text.count(word)

        # 增强词/减弱词调整
        for word in self.INTENSIFIERS:
            if word in text:
                pos_count *= 1.5 if pos_count > 0 else 1
                neg_count *= 1.5 if neg_count > 0 else 1

        for word in self.DIMINISHERS:
            if word in text:
                pos_count *= 0.7 if pos_count > 0 else 1
                neg_count *= 0.7 if neg_count > 0 else 1

        # 计算情绪值
        total = pos_count + neg_count
        if total == 0:
            return 0.0

        raw_score = (pos_count - neg_count) / total

        # 压缩到 [-1, 1] 区间，同时保留非线性
        compressed = math.tanh(raw_score * 2)

        return round(compressed, 4)

    # ──────────────────────────────────────────
    # 综合情绪计算
    # ──────────────────────────────────────────

    def calculate_sentiment_score(self, news_items: List[Dict]) -> float:
        """
        综合多维度情绪评分

        考虑因素：
        1. 各条新闻的情绪值（规则分析）
        2. 新闻来源权重
        3. 新闻时效权重
        4. 新闻数量置信度

        Args:
            news_items: 新闻列表，每项含 title, summary/content, source, published_at

        Returns:
            float: -1.0 ~ 1.0 的综合情绪分
        """
        if not news_items:
            return 0.0

        # 来源权重（权威来源权重更高）
        source_weights = {
            "巨潮资讯": 1.5,    # 官方公告
            "东方财富": 1.0,
            "新浪财经": 1.0,
            "雪球": 0.6,
            "财联社": 1.2,
            "华尔街见闻": 1.1,
            "金十数据": 1.0,
            "上交所": 1.5,
            "深交所": 1.5,
            "证券时报": 1.2,
        }

        total_weight = 0.0
        weighted_sum = 0.0
        now = datetime.now()

        for item in news_items:
            title = item.get("title", "")
            content = item.get("summary", item.get("content", ""))
            source = item.get("source", "")
            published = item.get("published_at", "")

            # 单条情绪
            sentiment = self.analyze_text_sentiment(title, content)

            # 来源权重
            source_w = 1.0
            for key, w in source_weights.items():
                if key in source:
                    source_w = w
                    break

            # 时效权重（越新的新闻权重越高）
            time_w = 1.0
            if published:
                try:
                    pub_time = datetime.strptime(str(published)[:10], "%Y-%m-%d")
                    hours_ago = (now - pub_time).total_seconds() / 3600
                    if hours_ago < 1:
                        time_w = 1.5  # 1小时内
                    elif hours_ago < 6:
                        time_w = 1.2  # 6小时内
                    elif hours_ago < 24:
                        time_w = 1.0  # 1天内
                    elif hours_ago < 72:
                        time_w = 0.8  # 3天内
                    else:
                        time_w = 0.5  # 3天以上
                except (ValueError, TypeError):
                    time_w = 0.8

            weight = source_w * time_w
            weighted_sum += sentiment * weight
            total_weight += weight

        if total_weight == 0:
            return 0.0

        # 归一化后压缩到 [-1, 1]
        raw_score = weighted_sum / total_weight
        score = math.tanh(raw_score * 2)

        # 置信度调整：新闻越多越自信，但防止过度放大
        n = len(news_items)
        confidence_bonus = min(n / 20, 1.0)  # 20条以上视为充分
        adjusted_score = score * (0.5 + 0.5 * confidence_bonus)

        return round(adjusted_score, 4)

    # ──────────────────────────────────────────
    # 情绪趋势
    # ──────────────────────────────────────────

    def get_sentiment_trend(self, news_by_day: Dict[str, List[Dict]],
                            days: int = 5) -> List[Dict]:
        """
        计算情绪趋势

        Args:
            news_by_day: 按日期分组的新闻字典
            days: 统计天数

        Returns:
            [{"date": "2025-01-15", "sentiment": 0.3, "news_count": 5}, ...]
        """
        trend = []
        now = datetime.now()

        for i in range(days - 1, -1, -1):
            date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            day_news = news_by_day.get(date, [])

            if day_news:
                score = self.calculate_sentiment_score(day_news)
            else:
                score = 0.0

            trend.append({
                "date": date,
                "sentiment": score,
                "news_count": len(day_news),
            })

        return trend

    def get_sentiment_trend_from_db(self, code: str, days: int = 5,
                                    db=None) -> List[Dict]:
        """
        从数据库读取数据计算情绪趋势

        Args:
            code: 股票代码
            days: 统计天数
            db: Database 实例

        Returns:
            情绪趋势列表
        """
        if not db:
            logger.warning("未提供数据库实例，无法获取趋势数据")
            return []

        news_by_day = defaultdict(list)
        news_items = db.get_stock_news_sentiment(code, days=days)

        for item in news_items:
            pub_date = str(item.get("published_at", ""))[:10]
            news_by_day[pub_date].append(item)

        return self.get_sentiment_trend(dict(news_by_day), days)

    # ──────────────────────────────────────────
    # 异常检测
    # ──────────────────────────────────────────

    def detect_anomaly(self, news_list: List[Dict]) -> Dict:
        """
        检测异常舆情（突发利空/利好）

        检测逻辑：
        1. 短时间集中大量负面/正面新闻
        2. 情绪突变（与历史趋势比较）
        3. 出现极端情绪新闻

        Args:
            news_list: 新闻列表

        Returns:
            {
                "is_anomaly": bool,       # 是否检测到异常
                "type": str,              # "bullish"/"bearish"/"none"
                "severity": str,          # "high"/"medium"/"low"
                "score": float,           # 异常得分
                "reason": str,            # 异常原因说明
                "trigger_news": List[str] # 触发异常的关键新闻标题
            }
        """
        if not news_list or len(news_list) < 3:
            return {
                "is_anomaly": False,
                "type": "none",
                "severity": "low",
                "score": 0.0,
                "reason": "新闻数量不足，无法检测",
                "trigger_news": []
            }

        # 分析每条新闻的情绪
        sentiments = []
        for item in news_list:
            title = item.get("title", "")
            content = item.get("summary", item.get("content", ""))
            sent = self.analyze_text_sentiment(title, content)
            sentiments.append({
                "title": title,
                "sentiment": sent,
            })

        # 1. 检查极端情绪新闻
        extreme_positive = [s for s in sentiments if s["sentiment"] > 0.7]
        extreme_negative = [s for s in sentiments if s["sentiment"] < -0.7]

        # 2. 计算情绪集中度
        pos_count = len([s for s in sentiments if s["sentiment"] > 0.3])
        neg_count = len([s for s in sentiments if s["sentiment"] < -0.3])
        total = len(sentiments)

        if total == 0:
            return {
                "is_anomaly": False,
                "type": "none",
                "severity": "low",
                "score": 0.0,
                "reason": "所有新闻均为中性",
                "trigger_news": []
            }

        pos_ratio = pos_count / total
        neg_ratio = neg_count / total

        # 3. 判断异常
        trigger_news = []

        # 突发利空
        if neg_ratio > 0.6 or len(extreme_negative) >= 2:
            anomaly_type = "bearish"
            severity = "high" if neg_ratio > 0.8 or len(extreme_negative) >= 3 else "medium"
            score = abs(neg_ratio - 0.5) * 2  # 0~1 异常度
            reason = f"突发利空：负面新闻占比 {neg_ratio:.0%}"
            trigger_news = [s["title"] for s in extreme_negative][:5]

        # 突发利好
        elif pos_ratio > 0.6 or len(extreme_positive) >= 2:
            anomaly_type = "bullish"
            severity = "high" if pos_ratio > 0.8 or len(extreme_positive) >= 3 else "medium"
            score = abs(pos_ratio - 0.5) * 2
            reason = f"突发利好：正面新闻占比 {pos_ratio:.0%}"
            trigger_news = [s["title"] for s in extreme_positive][:5]

        else:
            return {
                "is_anomaly": False,
                "type": "none",
                "severity": "low",
                "score": 0.0,
                "reason": "舆情正常，无明显异常信号",
                "trigger_news": []
            }

        return {
            "is_anomaly": True,
            "type": anomaly_type,
            "severity": severity,
            "score": round(score, 2),
            "reason": reason,
            "trigger_news": trigger_news
        }


# 需要 os 模块
import os


# 测试用
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sa = SentimentAnalyzer()

    test_news = [
        {"title": "贵州茅台业绩增长超预期，净利润同比增20%", "summary": "业绩亮眼",
         "source": "东方财富", "published_at": "2025-01-15 10:00"},
        {"title": "茅台酒出厂价上调，经销商拿货成本增加", "summary": "提价利好",
         "source": "新浪财经", "published_at": "2025-01-15 11:00"},
        {"title": "北向资金持续买入茅台，外资看好消费复苏", "summary": "资金流入",
         "source": "雪球", "published_at": "2025-01-15 14:00"},
    ]

    # 综合评分
    score = sa.calculate_sentiment_score(test_news)
    print(f"综合情绪评分: {score}")

    # 异常检测
    anomaly = sa.detect_anomaly(test_news)
    print(f"\n异常检测: {anomaly}")
