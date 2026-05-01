"""
多源降级链：按优先级依次尝试数据源，全部失败才报错

使用方式：
    from collector.fallback import FallbackChain

    fallback = FallbackChain(scheduler)
    fallback.get_quotes(stocks)       # 行情: 东方财富 → 新浪
    fallback.get_news()               # 新闻: 东方财富 → 新浪 → 雪球
"""
import logging

logger = logging.getLogger(__name__)


class FallbackChain:
    """
    按优先级降级链

    构造时传 scheduler，FallbackChain 从 scheduler.collectors 里
    按优先级依次取采集器尝试，成功就返回，失败就试下一个。
    """

    # 降级优先级（从左到右依次尝试）
    QUOTES_PRIORITY = ["东方财富", "新浪财经"]
    NEWS_PRIORITY = ["东方财富", "新浪财经", "雪球", "同花顺快讯"]
    ANNOUNCEMENTS_PRIORITY = ["巨潮资讯", "上交所公告", "深交所公告", "北交所公告"]

    def __init__(self, scheduler):
        self.s = scheduler

    def get_quotes(self, stocks) -> int:
        """行情降级：东方财富 → 新浪财经"""
        for name in self.QUOTES_PRIORITY:
            c = self.s.collectors.get(name)
            if not c:
                continue
            try:
                logger.info(f"[降级链] 尝试 {name} 采集行情...")
                result = c._collect_quotes(stocks)
                if result and result > 0:
                    logger.info(f"[降级链] {name} 行情采集成功: {result} 条")
                    return result
            except Exception as e:
                logger.warning(f"[降级链] {name} 行情采集失败: {e}，尝试下一个...")
        logger.error("[降级链] 所有行情源均不可用")
        return 0

    def get_news(self) -> int:
        """新闻降级：东方财富 → 新浪财经 → 雪球 → 同花顺快讯"""
        for name in self.NEWS_PRIORITY:
            c = self.s.collectors.get(name)
            if not c:
                continue
            try:
                logger.info(f"[降级链] 尝试 {name} 采集新闻...")
                if hasattr(c, '_collect_news'):
                    result = c._collect_news()
                else:
                    # 有的采集器 collect() 返回 dict，尝试取 news 字段
                    result = c.collect()
                    if isinstance(result, dict):
                        result = result.get("news", 0)
                if result and result > 0:
                    logger.info(f"[降级链] {name} 新闻采集成功: {result} 条")
                    return result
            except Exception as e:
                logger.warning(f"[降级链] {name} 新闻采集失败: {e}，尝试下一个...")
        logger.error("[降级链] 所有新闻源均不可用")
        return 0

    def get_announcements(self, stocks) -> int:
        """公告降级：巨潮 → 上交所 → 深交所 → 北交所"""
        for name in self.ANNOUNCEMENTS_PRIORITY:
            c = self.s.collectors.get(name)
            if not c:
                continue
            try:
                logger.info(f"[降级链] 尝试 {name} 采集公告...")
                if hasattr(c, '_collect_announcements'):
                    result = c._collect_announcements(stocks)
                else:
                    result = c.collect()
                    if isinstance(result, dict):
                        result = result.get("announcements", 0)
                if result and result > 0:
                    logger.info(f"[降级链] {name} 公告采集成功: {result} 条")
                    return result
            except Exception as e:
                logger.warning(f"[降级链] {name} 公告采集失败: {e}，尝试下一个...")
        logger.error("[降级链] 所有公告源均不可用")
        return 0

    def collect_with_fallback(self, data_type: str, stocks=None) -> int:
        """
        统一入口：按数据类型自动选择降级链

        Args:
            data_type: 'quotes' / 'news' / 'announcements'
            stocks: 股票列表（行情和公告需要）

        Returns:
            成功采集到的数量
        """
        chain = {
            "quotes": lambda: self.get_quotes(stocks),
            "news": lambda: self.get_news(),
            "announcements": lambda: self.get_announcements(stocks),
        }
        fetcher = chain.get(data_type)
        if not fetcher:
            logger.error(f"[降级链] 未知数据类型: {data_type}")
            return 0
        return fetcher()
