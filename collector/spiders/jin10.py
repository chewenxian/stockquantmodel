"""
金十数据 (jin10.com) 采集器
- 实时快讯（24h滚动）
- 热点头条
- 影响板块检测
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict, List
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class Jin10Collector(BaseCollector):
    """
    金十数据采集器
    官网：https://www.jin10.com
    数据：实时快讯、热点头条、市场数据
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.flash_url = "https://www.jin10.com/"
        self.detail_url = "https://flash.jin10.com/detail/"

    def collect(self) -> Dict[str, int]:
        results = {}
        results["flash_news"] = self._collect_flash_news()
        total = sum(results.values())
        logger.info(f"[金十数据] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_flash_news(self) -> int:
        """采集金十24h快讯（从HTML解析）"""
        count = 0
        headers = {
            "User-Agent": self._random_ua(),
            "Referer": "https://www.jin10.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        resp = self.get(self.flash_url, headers=headers)
        if not resp:
            logger.warning("[金十数据] 页面请求失败")
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.find_all("div", class_="jin-flash-item")
        logger.info(f"[金十数据] 发现 {len(items)} 条快讯")

        for item in items:
            try:
                # 提取ID
                item_id = ""
                parent = item.find_parent("div", id=re.compile(r"flash\d+"))
                if parent and parent.get("id"):
                    item_id = parent["id"]

                # 判断类型
                classes = item.get("class", [])
                is_important = "is-important" in classes
                is_vip = "is-vip" in classes
                is_article = "article" in classes

                # 提取标题（article类型有标题）
                title_el = item.find("b", class_="right-common-title")
                title = title_el.get_text(strip=True) if title_el else ""

                # 提取内容
                content_el = item.find("div", class_="flash-text")
                content = ""
                if content_el:
                    content = content_el.get_text(strip=True)

                # 提取时间
                time_el = item.find("div", class_="item-time")
                time_str = time_el.get_text(strip=True) if time_el else ""

                # 提取数据来源标签（如有）
                tag_el = item.find("span", class_="jin-tag")
                tag = tag_el.get_text(strip=True) if tag_el else ""

                # 构建标题
                if not title:
                    if content:
                        title = content[:60] + ("..." if len(content) > 60 else "")
                    else:
                        continue  # 跳过无内容项

                # 构建URL
                news_url = f"{self.detail_url}{item_id}" if item_id else self.flash_url

                # 拼接完整时间
                today = datetime.now().strftime("%Y-%m-%d")
                pub_time = f"{today} {time_str}" if time_str else datetime.now().isoformat()

                # 生成摘要
                summary = content[:300] if content else title
                prefix = "【金十快讯】"
                if is_important:
                    prefix = "【金十重要】"
                elif is_vip:
                    prefix = "【金十VIP】"
                elif tag:
                    prefix = f"【金十{tag}】"

                full_title = f"{prefix}{title}"

                # 存入新闻表
                news_id = self.db.insert_news(
                    title=full_title,
                    url=news_url,
                    source="金十数据",
                    summary=summary,
                    published_at=pub_time,
                )

                if news_id:
                    # 检测涉及板块并关联股票
                    sectors = self._detect_related_sectors(content + title)
                    for sector_name, stock_codes in sectors.items():
                        for code in stock_codes:
                            self.db.link_news_stock(news_id, code)

                    count += 1

            except Exception as e:
                logger.warning(f"金十快讯解析异常: {e}")

        # 采集VIP快讯（公共部分）
        vip_count = self._collect_vip_previews(soup)
        count += vip_count

        return count

    def _collect_vip_previews(self, soup) -> int:
        """采集VIP快讯的公开预览部分"""
        count = 0
        unlock_items = soup.find_all("div", class_="need-unlock")
        for item in unlock_items:
            try:
                # 从 "normal-user" 区域提取
                unlock_text = item.find("a", href=re.compile(r"javascript:void\('unlockFlash'\)"))
                if unlock_text:
                    title = unlock_text.get_text(strip=True)
                    if title:
                        news_id = self.db.insert_news(
                            title=f"【金十VIP预览】{title[:80]}",
                            url=self.flash_url,
                            source="金十数据VIP",
                            summary=f"VIP内容预览: {title[:200]}",
                            published_at=datetime.now().isoformat()
                        )
                        if news_id:
                            count += 1
            except:
                pass
        return count

    def _detect_related_sectors(self, text: str) -> Dict[str, List[str]]:
        """从文本中检测涉及的板块并关联自选股"""
        sectors = {}

        # 关键词-股票映射
        keyword_map = {
            "新能源": ["300750", "002594", "601012"],
            "新能源汽车": ["002594", "600104"],
            "光伏": ["601012", "688599"],
            "锂电池": ["300750", "002074"],
            "白酒": ["600519", "000858", "000568"],
            "金融|银行|券商": ["601318", "600036", "600030"],
            "保险": ["601318", "601601"],
            "房地产|楼市": ["000002", "001979"],
            "医药|医疗|CRO": ["603259", "300760", "600196"],
            "AI|人工智能|算力": ["002230", "688111", "603019"],
            "半导体|芯片": ["688981", "002371", "603501"],
            "家电": ["000333", "000651"],
            "消费": ["600519", "000858", "600887", "000333"],
            "安防|AI视觉": ["002415"],
            "面板|屏幕": ["000725", "002456"],
            "军工|国防": ["600760", "600893", "600185"],
            "煤炭": ["601088", "600188"],
            "有色|铝|铜": ["601600", "000630", "000060"],
            "原油|石油|能源": ["601857", "600028", "600688"],
            "黄金": ["601899", "600547", "002155"],
            "农业|猪肉|粮食": ["000895", "002714", "600598"],
        }

        text_lower = text.lower()
        for keywords, codes in keyword_map.items():
            if re.search(keywords, text, re.IGNORECASE):
                sectors[keywords] = codes

        return sectors


# 如果直接运行，测试采集
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from storage.database import Database
    db = Database("data/test.db")
    c = Jin10Collector(db)
    result = c.collect()
    print(f"采集结果: {result}")
