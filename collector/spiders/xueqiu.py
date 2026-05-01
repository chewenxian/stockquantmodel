"""
雪球数据源：用户讨论 + 热帖 + 情绪指标
"""
import re
import json
import logging
from datetime import datetime
from typing import List, Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class XueqiuCollector(BaseCollector):
    """
    雪球数据源
    - 个股讨论/帖子
    - 热门股票
    - 情绪指标
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def collect(self) -> Dict[str, int]:
        results = {}
        stocks = self.db.load_stocks()
        results["hot_stocks"] = self._collect_hot_stocks()
        results["stock_discussions"] = self._collect_stock_discussions(stocks)
        total = sum(results.values())
        logger.info(f"[雪球] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_hot_stocks(self) -> int:
        """雪球热门股票排行"""
        count = 0
        url = "https://xueqiu.com/stock/sqrank.json"
        params = {
            "category": "cn",
            "size": 30,
            "type": "follow",
        }
        headers = {
            "Referer": "https://xueqiu.com/hq",
            "Accept": "application/json",
        }
        data = self.get_json(url, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("list", [])
            for item in items:
                try:
                    code = str(item.get("stock_id", ""))
                    change = round(float(item.get("percent", 0) or 0), 2)
                    # 存储为新闻（热榜讨论）
                    title = f"【雪球热股】{item.get('name', '')} ({code}) - 热度#{item.get('rank', '')}"
                    self.db.insert_news(
                        title=title,
                        url=f"https://xueqiu.com/S/{code}",
                        source="雪球热榜",
                        summary=f"雪球热议度第{item.get('rank', '')} | 涨幅 {change}%",
                        published_at=datetime.now().isoformat()
                    )
                    count += 1
                except:
                    pass
        return count

    def _collect_stock_discussions(self, stocks: List[Dict]) -> int:
        """按个股采集雪球讨论"""
        count = 0
        for stock in stocks[:10]:  # 前10只
            try:
                code = stock["code"]
                url = "https://xueqiu.com/statuses/stock/timeline.json"
                params = {
                    "symbol_id": f"{'SH' if stock['market']=='SH' else 'SZ'}{code}",
                    "count": 20,
                    "type": "all",
                }
                headers = {"Referer": "https://xueqiu.com/S/SH600519"}
                data = self.get_json(url, params, headers=headers)

                if data and data.get("list"):
                    for status in data["list"]:
                        try:
                            text = status.get("text", "").strip()
                            if not text:
                                continue
                            # 清洗HTML标签
                            text = re.sub(r'<[^>]+>', '', text)
                            title = text[:80] + ("..." if len(text) > 80 else "")

                            data_id = str(status.get("id", ""))
                            url_link = f"https://xueqiu.com/{status.get('user', {}).get('screenName', '')}/{data_id}"

                            news_id = self.db.insert_news(
                                title=title,
                                url=url_link,
                                source="雪球讨论",
                                summary=text[:200],
                                published_at=datetime.now().isoformat()
                            )
                            if news_id:
                                self.db.link_news_stock(news_id, code)
                                count += 1
                        except:
                            pass
            except Exception as e:
                logger.warning(f"雪球讨论采集异常({stock['code']}): {e}")
        return count
