"""
证券时报 (STCN) 新闻采集器
数据源：http://www.stcn.com
采集：财经新闻、公司新闻、行业新闻
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class STCNCollector(BaseCollector):
    """
    证券时报新闻采集器
    官网：http://www.stcn.com
    数据：财经头条、公司新闻、行业动态
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.base_url = "http://www.stcn.com"
        self.news_url = "http://www.stcn.com/article/list.html"
        self.api_url = "http://www.stcn.com/api/article/list"
        self.kuaixun_url = "http://kuaixun.stcn.com/"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["news"] = self._collect_news()
        results["flash_news"] = self._collect_flash_news()
        total = sum(results.values())
        logger.info(f"[证券时报] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_news(self) -> int:
        """采集证券时报新闻"""
        count = 0

        # 新闻分类
        categories = [
            ("yw", "要闻"),
            ("gs", "公司"),
            ("hy", "行业"),
            ("sc", "市场"),
            ("jj", "经济"),
            ("gj", "国际"),
        ]

        headers = {
            "Referer": self.base_url,
            "User-Agent": self._random_ua(),
            "Accept": "text/html,application/json,*/*",
        }

        for cat_code, cat_name in categories:
            try:
                url = f"{self.base_url}/article/list/{cat_code}.html"
                params = {"page": 1}

                resp = self.get(url, params=params, headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                articles = soup.find_all("div", class_="news-item")
                if not articles:
                    # 尝试其他选择器
                    articles = soup.select("ul.list li a[href*='/article/']")

                if not articles:
                    articles = soup.find_all("a", href=re.compile(r"/article/\d+"))

                for article in articles:
                    try:
                        # 提取标题和链接
                        a_tag = article if article.name == "a" else article.find("a")
                        if not a_tag:
                            continue

                        title = a_tag.get("title", "") or a_tag.get_text(strip=True)
                        href = a_tag.get("href", "")

                        if not title or not href:
                            continue

                        full_url = href if href.startswith("http") else f"{self.base_url}{href}"

                        # 提取时间
                        time_el = article.find("span", class_="time") or article.find("em", class_="date")
                        pub_time = ""
                        if time_el:
                            pub_time = time_el.get_text(strip=True)

                        # 提取摘要
                        summary_el = article.find("p", class_="desc") or article.find("div", class_="summary")
                        summary = summary_el.get_text(strip=True)[:300] if summary_el else ""

                        # 存入新闻表
                        news_id = self.db.insert_news(
                            title=title.strip(),
                            url=full_url,
                            source=f"证券时报-{cat_name}",
                            summary=summary,
                            published_at=pub_time if pub_time else datetime.now().isoformat(),
                        )
                        if news_id:
                            count += 1

                    except Exception as e:
                        logger.warning(f"证券时报解析异常: {e}")

            except Exception as e:
                logger.warning(f"证券时报请求异常({cat_name}): {e}")

        return count

    def _collect_flash_news(self) -> int:
        """采集证券时报快讯"""
        count = 0

        headers = {
            "Referer": self.kuaixun_url,
            "User-Agent": self._random_ua(),
            "Accept": "text/html,text/plain,*/*",
        }

        try:
            # 快讯列表
            params = {"page": 1, "pageSize": 30}
            data = self.get_json(f"{self.kuaixun_url}api/feed/list", params, headers=headers)
            if data and data.get("data"):
                items = data["data"].get("list", [])
                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        content = item.get("content", "")
                        feed_id = item.get("id", "")
                        pub_time = item.get("publishTime", "")
                        source_name = item.get("source", "")

                        if not title and not content:
                            continue
                        if not title:
                            title = content[:80]

                        full_title = f"【证券时报快讯】{title}"
                        link = f"{self.kuaixun_url}detail/{feed_id}" if feed_id else self.kuaixun_url
                        summary = content[:300] if content else title

                        news_id = self.db.insert_news(
                            title=full_title,
                            url=link,
                            source="证券时报快讯",
                            summary=summary,
                            published_at=pub_time or datetime.now().isoformat(),
                        )
                        if news_id:
                            count += 1

                    except Exception as e:
                        logger.warning(f"证券时报快讯解析异常: {e}")

        except Exception as e:
            logger.warning(f"证券时报快讯请求异常: {e}")

        return count
