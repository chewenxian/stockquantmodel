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
                    # 先用标题精确匹配关联自选股
                    linked = self._link_by_stock_name(news_id, title)
                    if not linked:
                        # 标题没匹配到，再用内容检测（但已废弃，返回空）
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
        """从文本中检测涉及的板块并关联自选股
        注意: 仅匹配明确含义的关键词，避免"金融时报"误配"金融"
        """
        sectors = {}

        # 关键词-硬编码股票映射（已废弃）
        # 使用新版基于自选股+标题精确匹配
        return sectors

    def _link_by_stock_name(self, news_id: int, title: str) -> bool:
        """
        通过股票名称精确匹配标题来关联自选股
        股票名必须在标题中作为独立词组出现
        返回是否成功关联
        """
        import sqlite3
        try:
            conn = self.db._connect()
            stocks = conn.execute("SELECT code, name FROM stocks").fetchall()
            stocks = sorted(stocks, key=lambda x: -len(x[1]))

            for code, name in stocks:
                if not name or name not in title:
                    continue
                # 检查边界：确认name不是更长词组的一部分
                idx = title.index(name)
                before = title[idx-1] if idx > 0 else ""
                after = title[idx+len(name)] if idx+len(name) < len(title) else ""

                if before and self._is_cjk_or_alpha(before):
                    continue  # 前面有中文/字母，可能是更大词的一部分
                if after and self._is_cjk_or_alpha(after):
                    continue  # 后面有中文/字母，可能是更大词的一部分

                self.db.link_news_stock(news_id, code)
                return True

            self.db._close(conn)
        except Exception:
            pass
        return False

    def _is_cjk_or_alpha(self, ch: str) -> bool:
        """判断字符是否为中文或英文字母"""
        cp = ord(ch)
        return (0x4E00 <= cp <= 0x9FFF) or ('a' <= ch <= 'z') or ('A' <= ch <= 'Z')


# 如果直接运行，测试采集
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from storage.database import Database
    db = Database("data/test.db")
    c = Jin10Collector(db)
    result = c.collect()
    print(f"采集结果: {result}")
