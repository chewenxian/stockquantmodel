"""
巨潮资讯网：公司公告（影响股价的官方信息）
- 业绩预告
- 分红送转
- 重大合同
- 资产重组
- 股东增减持
- 监管问询
"""
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class CninfoCollector(BaseCollector):
    """
    巨潮资讯网公告采集器
    官网：http://www.cninfo.com.cn
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.search_url = "http://www.cninfo.com.cn/new/disclosure/stock"

    def collect(self) -> Dict[str, int]:
        results = {}
        stocks = self.db.load_stocks()
        results["announcements"] = self._collect_announcements(stocks)
        total = sum(results.values())
        logger.info(f"[巨潮] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_announcements(self, stocks: List[Dict]) -> int:
        """采集公司公告"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        for stock in stocks:
            try:
                code = stock["code"]
                url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer": "http://www.cninfo.com.cn/new/disclosure/stock",
                    "User-Agent": self._random_ua(),
                }
                data = {
                    "pageNum": 1,
                    "pageSize": 20,
                    "column": "szse_main",
                    "tabName": "fulltext",
                    "plate": self._get_plate(code),
                    "stock": f"{code},{stock['name']}",
                    "searchkey": "",
                    "secid": "",
                    "category": "",
                    "trade": "",
                    "seDate": f"{week_ago}~{today}",
                    "sortName": "",
                    "sortType": "",
                    "isHLtitle": True,
                }

                resp = self.session.post(url, data=data, headers=headers, timeout=15)
                if resp.status_code != 200:
                    continue

                result = resp.json()
                items = result.get("announcements", [])
                for ann in items:
                    try:
                        title = ann.get("announcementTitle", "").strip()
                        # 去除HTML标签
                        title = title.replace("<b>", "").replace("</b>", "")
                        ann_id = ann.get("announcementId", "")
                        ann_type = ann.get("announcementTypeName", "")
                        pub_date = ann.get("announcementDate", "")

                        # 重大公告分类关键词
                        important_keywords = [
                            "业绩预告", "业绩修正", "分红", "送转", "停牌", "复牌",
                            "重大资产重组", "收购", "减持", "增持", "回购",
                            "立案调查", "行政处罚", "监管函", "问询函",
                            "中标", "重大合同", "战略合作", "定增", "配股",
                            "退市风险警示", "ST", "摘帽", "更名",
                            "预增", "预减", "预亏", "扭亏",
                        ]
                        is_important = any(k in title for k in important_keywords)

                        summary = ann.get("announcementSummary", "")
                        full_url = f"http://www.cninfo.com.cn/new/disclosure/detail?announcementId={ann_id}"

                        self.db.insert_announcement(
                            code=code, title=title, url=full_url,
                            announce_type=ann_type,
                            summary=f"{'【重要】' if is_important else ''}{summary[:200]}",
                            publish_date=pub_date
                        )

                        # 重要公告也存入新闻表
                        if is_important:
                            self.db.insert_news(
                                title=f"【公告】{title}",
                                url=full_url,
                                source="巨潮-公司公告",
                                summary=summary[:300],
                                published_at=pub_date
                            )
                        count += 1
                    except:
                        pass
            except Exception as e:
                logger.warning(f"巨潮公告采集异常({stock['code']}): {e}")
        return count

    def _get_plate(self, code: str) -> str:
        """根据代码判断板块"""
        if code.startswith("6"):
            return "sh"
        elif code.startswith("0") or code.startswith("3"):
            return "sz"
        elif code.startswith("8"):
            return "bj"
        return "sz"
