"""
北交所公告采集器 (BSE)
数据源：http://www.bse.cn
采集：上市公司公告、业务通知、监管动态
"""
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class BSECollector(BaseCollector):
    """
    北交所公告采集器
    官网：http://www.bse.cn
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.query_url = "http://www.bse.cn/api/info/announcement/list"
        self.list_url = "http://www.bse.cn/disclosure/announcement.html"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["announcements"] = self._collect_announcements()
        total = sum(results.values())
        logger.info(f"[北交所] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_announcements(self) -> int:
        """采集北交所公告"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            "Referer": self.list_url,
            "User-Agent": self._random_ua(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # 北交所公告分类
        categories = [
            ("", "全部公告"),
            ("01", "定期报告"),
            ("02", "业绩预告"),
            ("03", "IPO"),
            ("04", "再融资"),
            ("05", "并购重组"),
            ("06", "股权激励"),
            ("07", "分红"),
            ("08", "停复牌"),
            ("09", "减持增持"),
            ("10", "回购"),
            ("11", "风险提示"),
            ("12", "监管问询"),
        ]

        for cat_code, cat_name in categories:
            try:
                payload = {
                    "page": 1,
                    "pageSize": 30,
                    "startDate": week_ago,
                    "endDate": today,
                    "announcementType": cat_code,
                }

                resp = self.session.post(
                    self.query_url,
                    json=payload,
                    headers=headers,
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue

                data = resp.json()
                items = data.get("data", {}).get("list", [])

                for item in items:
                    try:
                        title = item.get("announcementTitle", "").strip()
                        if not title:
                            continue
                        title = re.sub(r"<[^>]+>", "", title)

                        ann_id = item.get("id", "")
                        stock_code = item.get("stockCode", "")
                        stock_name = item.get("stockName", "")
                        publish_date = item.get("publishDate", "")
                        file_url = item.get("announcementFilePath", "")

                        # 构建URL
                        full_url = ""
                        if file_url:
                            if file_url.startswith("http"):
                                full_url = file_url
                            else:
                                full_url = f"http://www.bse.cn{file_url}"
                        elif ann_id:
                            full_url = f"http://www.bse.cn/disclosure/announcement.html?id={ann_id}"

                        # 判断是否重大公告
                        important_keywords = [
                            "业绩预告", "业绩修正", "分红", "送转", "停牌", "复牌",
                            "重大资产重组", "收购", "减持", "增持", "回购",
                            "立案调查", "行政处罚", "监管函", "问询函",
                            "中标", "重大合同", "战略合作", "定增", "配股",
                            "预增", "预减", "预亏", "扭亏", "退市",
                            "风险提示", "ST",
                        ]
                        is_important = any(k in title for k in important_keywords)

                        # 北交所代码以8开头，6位数字
                        if stock_code:
                            code = stock_code.zfill(6) if len(stock_code) < 6 else stock_code
                            self.db.insert_announcement(
                                code=code,
                                title=title,
                                url=full_url,
                                announce_type=cat_name,
                                summary=f"北交所{cat_name}",
                                publish_date=publish_date,
                            )

                        # 重要公告也存入新闻表
                        if is_important:
                            self.db.insert_news(
                                title=f"【北交所公告】{title}",
                                url=full_url,
                                source=f"北交所-{cat_name}",
                                summary=f"北交所上市公司{cat_name}公告",
                                published_at=publish_date,
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"北交所公告解析异常: {e}")
            except Exception as e:
                logger.warning(f"北交所请求异常({cat_name}): {e}")

        logger.info(f"[北交所] 公告采集完成, 共 {count} 条")
        return count
