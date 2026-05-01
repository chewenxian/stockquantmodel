"""
采集调度器：统一管理所有数据源的采集任务
支持全量采集和增量采集
"""
import time
import logging
from datetime import datetime
from typing import Dict, Optional
import yaml

from storage.database import Database
from collector.spiders.eastmoney import EastMoneyCollector
from collector.spiders.sina_finance import SinaFinanceCollector
from collector.spiders.xueqiu import XueqiuCollector
from collector.spiders.cninfo import CninfoCollector
from collector.spiders.policy_collector import PolicyCollector
from collector.spiders.jin10 import Jin10Collector
from collector.spiders.sse import SSECollector
from collector.spiders.szse import SZSECollector
from collector.spiders.bse import BSECollector
from collector.spiders.stcn import STCNCollector
from collector.spiders.hexin import HexinCollector
from collector.spiders.gov_policy import GovPolicyCollector
from collector.spiders.history_quotes import HistoryQuotesCollector
from collector.spiders.north_flow import NorthFlowCollector
from collector.spiders.margin_trading import MarginTradingCollector
from collector.spiders.guba_sentiment import GubaSentimentCollector
from collector.spiders.bond_yield import BondYieldCollector
from collector.spiders.stock_hot import StockHotCollector
from collector.fallback import FallbackChain
from output.realtime_pusher import RealtimePusher

logger = logging.getLogger(__name__)


class CollectScheduler:
    """
    采集调度器
    管理所有采集器并按需执行采集任务
    """

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.db = Database(self.config.get("system", {}).get("db_path", "data/stock_news.db"))

        # 代理设置
        proxy_cfg = self.config.get("proxy", {})
        proxy = {
            "http": proxy_cfg.get("http", ""),
            "https": proxy_cfg.get("https", ""),
        } if proxy_cfg.get("http") else None

        # 初始化所有采集器
        sources = self.config.get("collector", {}).get("sources", {})
        self.collectors = {}
        self.fallback = FallbackChain(self)
        self.realtime_pusher = RealtimePusher(db=self.db)

        if sources.get("eastmoney", True):
            self.collectors["东方财富"] = EastMoneyCollector(self.db, proxy)

        if sources.get("sina", True):
            self.collectors["新浪财经"] = SinaFinanceCollector(self.db, proxy)

        if sources.get("xueqiu", True):
            self.collectors["雪球"] = XueqiuCollector(self.db, proxy)

        if sources.get("cninfo", True):
            self.collectors["巨潮资讯"] = CninfoCollector(self.db, proxy)

        if sources.get("cls", True) or sources.get("wallstreet", True):
            self.collectors["政策宏观"] = PolicyCollector(self.db, proxy)

        if sources.get("jin10", True):
            self.collectors["金十数据"] = Jin10Collector(self.db, proxy)

        if sources.get("sse", True):
            self.collectors["上交所公告"] = SSECollector(self.db, proxy)

        if sources.get("szse", True):
            self.collectors["深交所公告"] = SZSECollector(self.db, proxy)

        if sources.get("bse", True):
            self.collectors["北交所公告"] = BSECollector(self.db, proxy)

        if sources.get("stcn", True):
            self.collectors["证券时报"] = STCNCollector(self.db, proxy)

        if sources.get("10jqka", True):
            self.collectors["同花顺快讯"] = HexinCollector(self.db, proxy)

        if sources.get("gov_policy", True):
            self.collectors["政府政策"] = GovPolicyCollector(self.db, proxy)

        if sources.get("history_quotes", True):
            self.collectors["历史K线"] = HistoryQuotesCollector(self.db, proxy)

        if sources.get("north_flow", True):
            self.collectors["北向资金"] = NorthFlowCollector(self.db, proxy)

        if sources.get("margin_trading", True):
            self.collectors["融资融券"] = MarginTradingCollector(self.db, proxy)

        if sources.get("guba_sentiment", True):
            self.collectors["股吧情绪"] = GubaSentimentCollector(self.db, proxy)

        if sources.get("bond_yield", True):
            self.collectors["国债收益率"] = BondYieldCollector(self.db, proxy)

        if sources.get("stock_hot", True):
            self.collectors["股票热度"] = StockHotCollector(self.db, proxy)

        logger.info(f"采集器初始化完成: {list(self.collectors.keys())}")

    def _load_config(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def collect_all(self) -> Dict[str, Dict[str, int]]:
        """全量采集：运行所有采集器"""
        results = {}
        start_time = datetime.now()
        logger.info(f"===== 开始全量采集: {start_time.strftime('%Y-%m-%d %H:%M')} =====")

        for name, collector in self.collectors.items():
            try:
                logger.info(f"[{name}] 开始采集...")
                collector_results = collector.collect()
                results[name] = collector_results
                logger.info(f"[{name}] 采集完成: {collector_results}")
            except Exception as e:
                logger.error(f"[{name}] 采集失败: {e}", exc_info=True)
                results[name] = {"error": str(e)}
                self.db.log_collect(name, "all", status="error", error_msg=str(e)[:200])

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"===== 全量采集完成, 耗时 {elapsed:.1f}s =====")
        return results

    def collect_with_push(self, use_fallback: bool = True) -> Dict[str, int]:
        """
        采集 + 实时推送：采集完成后自动推送重要事件

        Args:
            use_fallback: 是否启用降级链

        Returns:
            采集结果
        """
        if use_fallback:
            results = self.collect_with_fallback()
        else:
            results = self.collect_all()

        # 采集完成后检查新数据，推送重要事件
        try:
            pushed = self.realtime_pusher.process_new_items()
            if pushed > 0:
                logger.info(f"[实时推送] 本次采集触发 {pushed} 条事件推送")
        except Exception as e:
            logger.warning(f"[实时推送] 执行失败: {e}")

        return results

    def collect_with_fallback(self) -> Dict[str, int]:
        """
        降级采集：各数据源优先使用主采集器，失败自动降级
        """
        results = {}
        stocks = self.db.load_stocks()
        start_time = datetime.now()
        logger.info(f"===== 降级采集: {start_time.strftime('%Y-%m-%d %H:%M')} =====")

        # 公告：优先巨潮，失败降级到交易所
        logger.info("[降级链] 开始公告采集...")
        results["announcements"] = self.fallback.get_announcements(stocks)

        # 新闻：优先东方财富，失败降级到新浪/雪球
        logger.info("[降级链] 开始新闻采集...")
        results["news"] = self.fallback.get_news()

        # 行情：优先东方财富，失败降级到新浪
        logger.info("[降级链] 开始行情采集...")
        results["quotes"] = self.fallback.get_quotes(stocks)

        # 补充采集（独立运行，不参与降级）
        supplement = [
            "政策宏观", "金十数据", "政府政策",
            "北向资金", "融资融券", "股吧情绪",
            "国债收益率", "股票热度", "历史K线",
            "证券时报",
        ]
        for name in supplement:
            c = self.collectors.get(name)
            if not c:
                continue
            try:
                logger.info(f"[{name}] 补充采集...")
                r = c.collect()
                results[name] = r if isinstance(r, dict) else {"count": r}
                if self.db:
                    self.db.mark_fetched(name, 
                        item_count=sum(r.values()) if isinstance(r, dict) else r,
                        error="")
            except Exception as e:
                logger.error(f"[{name}] 采集失败: {e}")
                results[name] = {"error": str(e)[:100]}
                if self.db:
                    self.db.mark_fetched(name, error=str(e)[:200])

        elapsed = (datetime.now() - start_time).total_seconds()
        total = sum(
            sum(v.values()) if isinstance(v, dict) else v
            for v in results.values() if v
        )
        logger.info(f"===== 降级采集完成, 共 {total} 条, 耗时 {elapsed:.1f}s =====")
        return results

    def quick_collect(self) -> Dict[str, int]:
        """快速采集：仅采集新闻和行情"""
        results = {}
        if "东方财富" in self.collectors:
            try:
                c = self.collectors["东方财富"]
                results["news"] = c._collect_news()
                stocks = self.db.load_stocks()
                results["quotes"] = c._collect_quotes(stocks)
                results["money_flow"] = c._collect_money_flow(stocks)
            except Exception as e:
                logger.error(f"快速采集失败: {e}")

        if "新浪财经" in self.collectors:
            try:
                c = self.collectors["新浪财经"]
                results["sina_news"] = c._collect_news()
            except Exception as e:
                logger.error(f"新浪快速采集失败: {e}")

        logger.info(f"[快速采集] 完成: {results}")
        return results

    def get_stats(self) -> dict:
        """获取采集统计"""
        return {
            "total_news": self.db.get_today_news_count(),
            "collectors": list(self.collectors.keys()),
            "stock_count": len(self.db.load_stocks()),
        }


if __name__ == "__main__":
    # 独立运行测试
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    scheduler = CollectScheduler()
    results = scheduler.collect_all()
    print(f"\n📊 采集结果汇总:")
    for name, res in results.items():
        print(f"  {name}: {res}")
