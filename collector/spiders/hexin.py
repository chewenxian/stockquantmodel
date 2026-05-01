"""
同花顺 (10jqka) 7x24快讯采集器
数据源：http://www.10jqka.com.cn
采集：7x24快讯、市场热点、盘中实时资讯
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class HexinCollector(BaseCollector):
    """
    同花顺7x24快讯采集器
    官网：http://www.10jqka.com.cn
    数据：7x24小时实时快讯、板块热点、资金动向
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.base_url = "http://www.10jqka.com.cn"
        self.flash_url = "http://www.10jqka.com.cn/flash/"
        self.api_url = "http://www.10jqka.com.cn/flash/api/getFlashList"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["flash_news"] = self._collect_flash_news()
        results["stock_news"] = self._collect_stock_news()
        total = sum(results.values())
        logger.info(f"[同花顺] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_flash_news(self) -> int:
        """采集同花顺7x24快讯"""
        count = 0

        headers = {
            "Referer": self.flash_url,
            "User-Agent": self._random_ua(),
            "Accept": "application/json, text/plain, */*",
        }

        # 尝试API方式采集
        try:
            params = {
                "page": 1,
                "limit": 50,
                "type": "all",
            }

            data = self.get_json(self.api_url, params=params, headers=headers)
            if data and data.get("data"):
                items = data["data"].get("list", [])
                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        content = item.get("content", "")
                        flash_id = item.get("id", "")
                        pub_time = item.get("time", item.get("createTime", ""))
                        item_type = item.get("type", "")
                        source = "同花顺7x24"

                        # 判断消息类型
                        type_tags = {
                            "1": "【重要】",
                            "2": "【快讯】",
                            "3": "【解读】",
                            "4": "【数据】",
                            "5": "【异动】",
                            "6": "【公告】",
                        }
                        prefix = type_tags.get(str(item_type), "【快讯】")

                        if not title and not content:
                            continue
                        if not title:
                            title = content[:80]

                        full_title = f"{prefix}{title}"
                        link = f"{self.flash_url}detail/{flash_id}" if flash_id else self.flash_url
                        summary = content[:300] if content else title

                        # 格式化时间
                        if pub_time:
                            try:
                                # 同花顺可能返回时间戳或字符串
                                if isinstance(pub_time, (int, float)):
                                    pub_time = datetime.fromtimestamp(
                                        pub_time / 1000 if pub_time > 1e10 else pub_time
                                    ).isoformat()
                            except:
                                pass

                        news_id = self.db.insert_news(
                            title=full_title,
                            url=link,
                            source=source,
                            summary=summary,
                            published_at=pub_time or datetime.now().isoformat(),
                        )
                        if news_id:
                            count += 1

                    except Exception as e:
                        logger.warning(f"同花顺快讯解析异常: {e}")
        except Exception as e:
            logger.warning(f"同花顺API请求异常, 尝试HTML方式: {e}")

            # 如果API失败，尝试HTML解析
            try:
                resp = self.get(self.flash_url, headers=headers)
                if resp:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    items = soup.find_all("div", class_="flash-item")
                    for item in items:
                        try:
                            title_el = item.find("div", class_="title") or item.find("h3")
                            time_el = item.find("span", class_="time") or item.find("div", class_="time")
                            content_el = item.find("div", class_="content") or item.find("p")

                            title = title_el.get_text(strip=True)[:80] if title_el else ""
                            pub_time = time_el.get_text(strip=True) if time_el else ""
                            content = content_el.get_text(strip=True) if content_el else ""

                            if not content and not title:
                                continue

                            final_title = f"【同花顺快讯】{title}" if title else f"【同花顺快讯】{content[:60]}"

                            full_link = ""
                            a_tag = item.find("a")
                            if a_tag and a_tag.get("href"):
                                href = a_tag["href"]
                                full_link = href if href.startswith("http") else f"{self.base_url}{href}"

                            news_id = self.db.insert_news(
                                title=final_title,
                                url=full_link or self.flash_url,
                                source="同花顺7x24",
                                summary=content[:300] if content else "",
                                published_at=pub_time or datetime.now().isoformat(),
                            )
                            if news_id:
                                count += 1
                        except:
                            pass
            except Exception as e2:
                logger.warning(f"同花顺HTML解析也失败: {e2}")

        return count

    def _collect_stock_news(self) -> int:
        """采集同花顺个股资讯"""
        count = 0
        stocks = self.db.load_stocks()

        headers = {
            "Referer": self.base_url,
            "User-Agent": self._random_ua(),
        }

        for stock in stocks[:20]:  # 限制最多20只
            try:
                code = stock["code"]
                # 同花顺个股资讯API
                url = f"http://basic.10jqka.com.cn/{code}/info.html"
                resp = self.get(url, headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                news_items = soup.find_all("li", class_="clearfix")
                for item in news_items[:10]:
                    try:
                        a_tag = item.find("a")
                        time_el = item.find("span", class_="date")

                        if not a_tag:
                            continue

                        title = a_tag.get_text(strip=True)
                        href = a_tag.get("href", "")
                        pub_time = time_el.get_text(strip=True) if time_el else ""

                        if not title:
                            continue

                        full_url = href if href.startswith("http") else f"http://basic.10jqka.com.cn{href}"

                        news_id = self.db.insert_news(
                            title=f"【同花顺个股】{title}",
                            url=full_url,
                            source="同花顺-个股资讯",
                            published_at=pub_time or datetime.now().isoformat(),
                        )
                        if news_id:
                            self.db.link_news_stock(news_id, code)
                            count += 1
                    except:
                        pass
            except Exception as e:
                logger.warning(f"同花顺个股资讯异常({stock['code']}): {e}")

        return count
