"""
政策与宏观数据采集：影响大盘走势的政策信息
数据源：
- 财联社电报 (cls.cn)
- 华尔街见闻 (wallstreetcn.com)
- 国家统计局 (stats.gov.cn)
- 人民银行 (pbc.gov.cn)
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class PolicyCollector(BaseCollector):
    """
    政策新闻 + 宏观数据采集器
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def collect(self) -> Dict[str, int]:
        results = {}
        results["cls_news"] = self._collect_cls_news()
        results["policy_news"] = self._collect_wallstreet_news()
        total = sum(results.values())
        logger.info(f"[政策宏观] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_cls_news(self) -> int:
        """财联社电报 - 市场快讯"""
        count = 0
        url = "https://www.cls.cn/telegraph"
        headers = {
            "Referer": "https://www.cls.cn/",
            "User-Agent": self._random_ua(),
        }
        params = {
            "category": "all",
            "limit": 30,
        }
        data = self.get_json("https://www.cls.cn/api/telegraph/list", params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("roll_data", []) if "roll_data" in data.get("data", {}) else data["data"].get("list", [])
            if isinstance(items, list):
                for item in items:
                    try:
                        title = item.get("title", "").strip() or item.get("content", "")[:80]
                        link = f"https://www.cls.cn/detail/{item.get('id', '')}"
                        content = item.get("content", "")
                        ctime = item.get("ctime", "")

                        if not title:
                            continue

                        # 分类影响板块
                        related_sectors = self._detect_sectors(title + content)

                        self.db.insert_policy(
                            title=title, url=link,
                            source="财联社",
                            department="",
                            summary=content[:300],
                            publish_date=ctime,
                            related_sectors=json.dumps(related_sectors, ensure_ascii=False)
                        )
                        count += 1
                    except Exception as e:
                        logger.warning(f"财联社解析异常: {e}")
        return count

    def _collect_wallstreet_news(self) -> int:
        """华尔街见闻 - 快讯"""
        count = 0
        url = "https://api-one.wallstcn.com/apiv1/content/lives"
        params = {"channel": "global", "limit": 30}
        headers = {"Referer": "https://wallstreetcn.com/live/global"}

        data = self.get_json(url, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("items", [])
            for item in items:
                try:
                    content = item.get("content", "")
                    title = content[:80] if content else ""
                    if not title:
                        continue
                    link = f"https://wallstreetcn.com/live/global"
                    ctime = item.get("display_time", "")

                    self.db.insert_policy(
                        title=title, url=link,
                        source="华尔街见闻",
                        summary=content[:300],
                        publish_date=ctime
                    )
                    count += 1
                except:
                    pass
        return count

    def _detect_sectors(self, text: str) -> list:
        """从文本中检测涉及的市场板块"""
        sectors = []
        keywords = {
            "新能源": ["新能源", "光伏", "风电", "锂电池", "新能源车", "比亚迪", "宁德时代"],
            "半导体": ["半导体", "芯片", "集成电路", "中芯国际", "光刻"],
            "金融": ["银行", "券商", "保险", "降息", "降准", "加息", "LPR"],
            "房地产": ["房地产", "楼市", "房贷", "房企", "万科", "碧桂园"],
            "医药": ["医药", "医疗", "创新药", "CXO", "疫苗", "集采"],
            "消费": ["消费", "白酒", "食品", "零售", "免税", "贵州茅台"],
            "AI科技": ["AI", "人工智能", "大模型", "ChatGPT", "算力", "数据要素"],
            "军工": ["军工", "国防", "航天", "卫星"],
            "周期": ["煤炭", "钢铁", "有色", "化工", "石油"],
            "农业": ["农业", "种业", "猪肉", "粮食"],
        }
        text_lower = text.lower()
        for sector, kws in keywords.items():
            if any(kw.lower() in text_lower for kw in kws):
                sectors.append(sector)
        return sectors
