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
