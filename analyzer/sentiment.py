"""
综合情绪分析模块
提供多维度的情绪评分、趋势分析、异常检测

功能：
1. 基于新闻文本/标题的规则情绪评分（支持jieba分词，回退双向最大匹配）
2. 否定词处理与强调词权重增强
3. 综合多维度情绪（新闻情绪、资金流向、市场情绪）
4. 情绪趋势分析
5. 异常舆情检测（突发利空/利好）
"""
import logging
import math
import os
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

    # 基础情绪词典（A股常用词，共200+）
    POSITIVE_WORDS = set([
        # 基础词汇
        "增长", "大涨", "涨停", "突破", "利好", "盈利", "分红", "中标",
        "合同", "扩产", "创新高", "买入", "增持", "回购", "扭亏", "预增",
        "翻红", "反弹", "放量上涨", "资金流入", "北向买入", "主力买入",
        "放量", "突破新高", "加速", "景气", "供不应求", "提价", "涨价",
        "政策支持", "补贴", "减税", "降准", "降息", "放水",
        "行业龙头", "市场份额提升", "强于大盘", "获得订单",
        # 扩展A股常用正面词汇
        "放量突破", "超预期", "封板", "低开高走", "触底反弹", "V型反转",
        "强势涨停", "一字板", "连板", "涨停板", "打板", "扫货",
        "拉升", "主升浪", "反包", "反抽", "探底回升", "翘板",
        "大单净流入", "机构增持", "外资增持", "国家队进场", "产业资本增持",
        "业绩超预期", "订单超预期", "出货量增长", "毛利率提升", "净利润增长",
        "营收增长", "现金流改善", "负债率下降", "ROE提升", "每股收益增长",
        "高送转", "高分红", "特别分红", "转增", "送股", "配股",
        "收购", "并购", "资产注入", "借壳", "重组", "混改",
        "中标大单", "签大单", "战略合作", "强强联手", "产能释放",
        "新药获批", "创新药", "注册批准", "临床获批", "产品上市",
        "新能源汽车", "光伏", "碳中和", "储能", "芯片国产化",
        "数字经济", "人工智能", "国产替代", "消费升级", "新基建",
        "顺周期", "通胀受益", "防御属性", "估值修复", "戴维斯双击",
        "抄底", "加仓", "多头", "看多", "乐观", "积极",
        "回暖", "复苏", "企稳回升", "底部放量", "空中加油",
        "政策利好", "行业拐点", "市场景气度提升", "供销两旺",
        "降本增效", "技改完成", "产能满负荷", "供不应求",
        "溢价", "正收益", "超额收益", "跑赢大盘", "领涨", "抗跌",
        "护盘", "拉升指数", "权重股发力", "题材活跃", "热点扩散",
        "量价齐升", "稳步攀升", "震荡上行", "震荡走高", "持续走强",
        "资金追捧", "主力抢筹", "机构调研", "外资加仓", "公募加仓",
        "险资加仓", "社保加仓", "产业资本回购", "员工持股", "股权激励",
        "注销回购", "分红率提高", "持续分红", "派息", "股息率",
    ])

    NEGATIVE_WORDS = set([
        # 基础词汇
        "下跌", "大跌", "跌停", "亏损", "减持", "立案", "处罚", "利空",
        "风险", "下调", "降级", "违约", "诉讼", "ST", "退市", "预亏",
        "爆雷", "崩盘", "闪崩", "资金流出", "北向卖出", "主力卖出",
        "缩量下跌", "破位", "跌穿", "出货", "利空出尽",
        "监管", "调查", "警示", "暂停", "终止", "取消",
        "行业下滑", "需求萎缩", "库存积压", "降价", "减薪", "裁员",
        # 扩展A股常用负面词汇
        "缩量下跌", "炸板", "高开低走", "冲高回落", "天地板", "地天板失败",
        "跌停板", "破板", "烂板", "尾盘跳水", "恐慌性抛售",
        "杀跌", "砸盘", "核按钮", "核爆", "踩踏", "多杀多",
        "大单净流出", "机构减持", "外资减持", "大股东减持", "清仓式减持",
        "业绩暴雷", "业绩变脸", "商誉减值", "资产减值", "坏账", "债务违约",
        "营收下滑", "净利润亏损", "毛利率下降", "现金流枯竭", "负债率攀升",
        "资不抵债", "破产", "清算", "债务逾期", "本金逾期",
        "财务造假", "信息披露违规", "内幕交易", "操纵股价", "关联交易",
        "函询", "警示函", "问询函", "关注函", "立案调查",
        "行政处罚", "罚款", "没收", "市场禁入", "强制退市",
        "被ST", "披星戴帽", "暂停上市", "终止上市", "面值退市",
        "解禁", "限售股解禁", "巨额解禁", "定增解禁",
        "减持公告", "减持计划", "减持完成", "减持进行中",
        "业绩预告亏损", "业绩快报变脸", "一季报暴雷", "半年报暴雷",
        "贸易摩擦", "关税", "制裁", "禁令", "断供",
        "行业监管", "政策收紧", "限贷", "限购", "限价",
        "行业饱和", "产能过剩", "供过于求", "价格战", "恶性竞争",
        "需求萎缩", "订单减少", "开工率下降", "停工", "减产",
        "空头", "看空", "悲观", "避险", "回避",
        "利空出尽", "利好兑现", "见光死", "利好出尽是利空",
        "流动性紧张", "资金面收紧", "利率上行", "通胀压力",
        "降薪", "拖欠工资", "欠薪", "劳务纠纷", "劳动仲裁",
        "董事长失联", "高管离职", "核心团队离职", "实控人变更",
        "商誉雷", "担保雷", "质押雷", "融资融券爆仓",
        "放量下跌", "破位下行", "阴跌", "阴跌不止", "无量空跌",
        "流动性枯竭", "资金踩踏", "机构出逃", "恐慌蔓延", "市场恐慌",
        "信用风险", "质押风险", "担保风险", "流动性风险", "政策风险",
        "汇率波动", "外围市场大跌", "美股暴跌", "港股暴跌",
        "黑天鹅", "灰犀牛", "系统性风险", "非系统性风险",
        "跑输大盘", "领跌", "垫底", "弱势", "疲软",
        "滞涨", "横盘整理", "缩量整理", "无量反弹", "弱势反弹",
        "回落", "下探", "下行", "走弱", "走低",
        "预警", "警告", "风险提示", "特别提示",
    ])

    # 否定词表：如果情感词前3字符内出现这些词，反转极性
    NEGATION_WORDS = set([
        "不", "没", "无", "未", "并非", "不是", "没有",
        "尚未", "绝非", "不要", "不用", "不会", "不可能", "难以",
        "缺乏", "缺少", "杜绝", "避免", "防止", "否认", "未必",
        "毫不", "从不", "从未", "不可", "不足", "不至于", "不太",
        "不该", "不必", "不敢", "不能", "不得", "并非",
    ])

    # 权重调整词（增强/减弱情绪）
    INTENSIFIERS = set([
        "大幅", "严重", "显著", "明显", "剧烈", "历史性", "极其",
        "非常", "十分", "特别", "极度", "相当", "异常", "迅猛",
        "强势", "暴力", "疯狂", "巨幅", "猛烈", "持续", "加速",
        "全面", "彻底", "绝对", "完全", "高度", "严重", "大幅",
    ])
    DIMINISHERS = set([
        "小幅", "略", "微增", "轻微", "有限", "部分", "略微",
        "稍微", "稍稍", "微幅", "微弱", "轻度", "低度",
    ])

    # 中文金融停用词表（过滤无意义词汇）
    STOP_WORDS = set([
        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一",
        "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着",
        "没有", "看", "好", "自己", "这", "他", "她", "它", "们",
        "那", "些", "什么", "怎么", "因为", "所以", "但是", "如果",
        "虽然", "而且", "或者", "还是", "然后", "那么",
        "可以", "可能", "应该", "已经", "正在", "通过", "进行",
        "以及", "等等", "比如", "例如", "包括", "其中", "之后",
        "之前", "同时", "此外", "另外", "因此", "由于", "关于",
        "对于", "从", "把", "被", "让", "给", "为", "以", "向",
        "与", "及", "或", "之", "于", "按", "将",
        "第", "每", "各", "该", "本", "此", "其", "何", "哪",
        "今日", "昨日", "明日", "上午", "下午", "晚间", "目前",
        "近期", "日前", "未来", "过去", "此前", "最新",
        "首次", "再次", "持续", "继续", "仍然", "依然",
        "全年", "半年", "季度", "本月", "本周", "今年",
        "记者", "报道", "获悉", "了解", "据悉", "消息", "来源",
        "编辑", "作者", "责编", "校对", "审核", "发布",
        "公告", "通知", "提示", "说明", "显示", "公布",
        "我们", "他们", "它们", "这些", "那些",
        "之间", "方面", "方式", "情况", "问题", "原因", "结果",
        "数据", "信息", "内容", "时间", "范围", "程度", "部分",
        "左右", "上下", "前后", "内外", "累计", "同比",
        "环比", "合计", "平均", "总体", "进一步", "较为",
        "分别", "某", "任何", "一切", "所有", "全部",
        "仅", "只", "共", "总", "约", "近", "超", "达",
        "于", "为", "在", "与", "将", "把", "被", "从", "对", "按",
        "以", "向", "往", "朝", "给", "替", "凭", "由", "让",
        "和", "跟", "同", "与", "及", "以及", "或", "或者",
        "并", "并且", "而", "而且", "不但", "不仅",
        "虽然", "但是", "然而", "不过", "可是", "但",
        "如果", "假如", "假若", "要是", "若",
        "因为", "由于", "所以", "因此", "因而",
        "为了", "为", "以", "以便", "以免", "以防",
    ])

    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.window_days = self.config.get("sentiment", {}).get("window_days", 5)
        self.anomaly_threshold = self.config.get("sentiment", {}).get("anomaly_threshold", 0.6)
        # 尝试使用jieba分词
        self._jieba_available = self._check_jieba()

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

    def _check_jieba(self) -> bool:
        """
        检查jieba分词库是否可用
        如果可用则将情感词典词加入jieba自定义词典

        Returns:
            bool: jieba是否可用
        """
        try:
            import jieba  # noqa: F811
            # 将情感词典词加入jieba自定义词典以提高分词准确度
            for word in self.POSITIVE_WORDS | self.NEGATIVE_WORDS:
                jieba.add_word(word)
            logger.info("jieba 分词库可用，启用高级分词模式")
            return True
        except ImportError:
            logger.info("jieba 不可用，使用双向最大匹配回退分词")
            return False
        except Exception as e:
            logger.warning(f"jieba 初始化异常: {e}")
            return False

    def _segment_jieba(self, text: str) -> List[str]:
        """
        使用jieba进行分词

        Args:
            text: 待分词文本

        Returns:
            分词结果列表
        """
        try:
            import jieba
            words = list(jieba.cut(text, cut_all=False))
            # 过滤停用词
            filtered = [w.strip() for w in words if w.strip() and w not in self.STOP_WORDS]
            return filtered
        except Exception as e:
            logger.warning(f"jieba分词异常，回退到双向最大匹配: {e}")
            return self._segment_bidirectional(text)

    def _segment_bidirectional(self, text: str) -> List[str]:
        """
        双向最大匹配分词（回退方案）

        Args:
            text: 待分词文本

        Returns:
            分词结果列表
        """
        max_word_len = 6
        words = []
        i = 0
        text_len = len(text)

        while i < text_len:
            matched = False
            # 正向最大匹配
            for j in range(min(max_word_len, text_len - i), 0, -1):
                candidate = text[i:i + j]
                if (candidate in self.POSITIVE_WORDS or
                        candidate in self.NEGATIVE_WORDS or
                        candidate in self.INTENSIFIERS or
                        candidate in self.DIMINISHERS or
                        candidate in self.NEGATION_WORDS):
                    words.append(candidate)
                    i += j
                    matched = True
                    break
            if not matched:
                # 未匹配到词典词，按单字切分（停用词过滤）
                char = text[i]
                if char not in self.STOP_WORDS:
                    words.append(char)
                i += 1

        return words

    def _segment(self, text: str) -> List[str]:
        """
        分词入口：优先jieba，回退到双向最大匹配

        Args:
            text: 待分词文本

        Returns:
            分词结果列表
        """
        if self._jieba_available:
            return self._segment_jieba(text)
        return self._segment_bidirectional(text)

    # ──────────────────────────────────────────
    # 文本情绪分析（规则基础，增强版）
    # ──────────────────────────────────────────

    def analyze_text_sentiment(self, title: str, content: str = "") -> float:
        """
        基于词典规则分析单条文本的情绪值（增强版）

        改进：
        1. jieba分词（回退双向最大匹配）
        2. 否定词处理（情感词前3个分词单元内出现否定词则反转极性）
        3. 强调词处理（权重x1.5）
        4. 停用词过滤

        Args:
            title: 新闻标题
            content: 新闻正文/摘要

        Returns:
            float: -1.0 ~ 1.0 的情绪值
        """
        try:
            text = f"{title} {content}"
            if not text.strip():
                return 0.0

            # 分词
            words = self._segment(text)
            if not words:
                return 0.0

            pos_score = 0.0
            neg_score = 0.0

            # 遍历分词结果匹配情感词典
            for idx, word in enumerate(words):
                weight = 1.0

                # 检查是否为情感词
                is_positive = word in self.POSITIVE_WORDS
                is_negative = word in self.NEGATIVE_WORDS

                if not is_positive and not is_negative:
                    continue

                # 检查否定词（看前3个分词单元内是否有否定词）
                has_negation = False
                start_idx = max(0, idx - 3)
                for j in range(start_idx, idx):
                    if words[j] in self.NEGATION_WORDS:
                        has_negation = True
                        break

                # 检查强调词（看前2个分词单元内是否有强调词）
                has_intensifier = False
                intensify_start = max(0, idx - 2)
                for j in range(intensify_start, idx):
                    if words[j] in self.INTENSIFIERS:
                        has_intensifier = True
                        break

                # 检查减弱词
                has_diminisher = False
                for j in range(intensify_start, idx):
                    if words[j] in self.DIMINISHERS:
                        has_diminisher = True
                        break

                # 权重调整
                if has_intensifier:
                    weight *= 1.5
                if has_diminisher:
                    weight *= 0.7

                # 否定词反转极性
                if has_negation:
                    is_positive, is_negative = is_negative, is_positive

                if is_positive:
                    pos_score += weight
                if is_negative:
                    neg_score += weight

            # 计算情绪值
            total = pos_score + neg_score
            if total == 0:
                return 0.0

            raw_score = (pos_score - neg_score) / total

            # 压缩到 [-1, 1] 区间，同时保留非线性
            compressed = math.tanh(raw_score * 2.5)

            return round(compressed, 4)

        except Exception as e:
            logger.error(f"文本情绪分析异常: {e}")
            return 0.0

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
        try:
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

        except Exception as e:
            logger.error(f"综合情绪计算异常: {e}")
            return 0.0

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
        try:
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

        except Exception as e:
            logger.error(f"情绪趋势计算异常: {e}")
            return []

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
        try:
            if not db:
                logger.warning("未提供数据库实例，无法获取趋势数据")
                return []

            news_by_day = defaultdict(list)
            news_items = db.get_stock_news_sentiment(code, days=days)

            for item in news_items:
                pub_date = str(item.get("published_at", ""))[:10]
                news_by_day[pub_date].append(item)

            return self.get_sentiment_trend(dict(news_by_day), days)

        except Exception as e:
            logger.error(f"从数据库获取情绪趋势异常: {e}")
            return []

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
        default_result = {
            "is_anomaly": False,
            "type": "none",
            "severity": "low",
            "score": 0.0,
            "reason": "新闻数量不足，无法检测",
            "trigger_news": []
        }

        try:
            if not news_list or len(news_list) < 3:
                return default_result

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

        except Exception as e:
            logger.error(f"异常舆情检测异常: {e}")
            return default_result


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
