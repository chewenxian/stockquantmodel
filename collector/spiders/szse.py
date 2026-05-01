"""
深交所公告采集器 (SZSE)
数据源：http://www.szse.cn
采集：上市公司公告、监管公告
"""
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class SZSECollector(BaseCollector):
    """
    深交所公告采集器
    官网：http://www.szse.cn
    公告类型：信息披露、监管函件、业务通知
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.query_url = "http://www.szse.cn/api/disc/announcement/annList"
        self.detail_url = "http://www.szse.cn/disclosure/listed/notice/index.html"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["announcements"] = self._collect_announcements()
        total = sum(results.values())
        logger.info(f"[深交所] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_announcements(self) -> int:
        """采集深交所公告"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            "Referer": self.detail_url,
            "User-Agent": self._random_ua(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # 深交所公告类型
        category_codes = [
            ("category_bulletin", "公司公告"),
            ("category_regulation", "监管函件"),
            ("category_chip", "权益变动"),
            ("category_chg_notice", "变更通知"),
            ("category_trade_notice", "交易提示"),
            ("category_business", "业务通知"),
        ]

        for cat_key, cat_name in category_codes:
            try:
                payload = {
                    "seDate": [week_ago, today],
                    "stock": ["", "", ""],
                    "channelCode": [],
                    "pageNum": 1,
                    "pageSize": 50,
                    "categoryCode": [cat_key],
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
                items = data.get("data", [])
                if not items:
                    continue

                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        if not title:
                            continue
                        # 去除HTML标签
                        title = re.sub(r"<[^>]+>", "", title)

                        ann_id = item.get("id", "")
                        sec_code = item.get("secCode", "")
                        sec_name = item.get("secName", "")
                        publish_date = item.get("publishDate", "")
                        # adjUrl 是附件路径
                        adj_url = item.get("adjUrl", "")

                        # 构建详情URL
                        if ann_id:
                            full_url = f"http://disc.szse.cn/detail/{ann_id}"
                        elif adj_url:
                            full_url = f"http://disc.szse.cn{adj_url}"
                        else:
                            full_url = self.detail_url

                        # 判断是否重大公告
                        important_keywords = [
                            "业绩预告", "业绩修正", "分红", "送转", "停牌", "复牌",
                            "重大资产重组", "收购", "减持", "增持", "回购",
                            "立案调查", "行政处罚", "监管函", "问询函",
                            "中标", "重大合同", "战略合作", "定增", "配股",
                            "预增", "预减", "预亏", "扭亏", "退市",
                        ]
                        is_important = any(k in title for k in important_keywords)

                        # 存入公告表
                        if sec_code:
                            code = sec_code.zfill(6) if len(sec_code) < 6 else sec_code
                            self.db.insert_announcement(
                                code=code,
                                title=title,
                                url=full_url,
                                announce_type=cat_name,
                                summary=f"深交所{cat_name}",
                                publish_date=publish_date,
                            )

                        # 重要公告也存入新闻表
                        if is_important:
                            self.db.insert_news(
                                title=f"【深交所公告】{title}",
                                url=full_url,
                                source=f"深交所-{cat_name}",
                                summary=f"深交所上市公司{cat_name}公告",
                                published_at=publish_date,
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"深交所公告解析异常: {e}")
            except Exception as e:
                logger.warning(f"深交所请求异常({cat_name}): {e}")

        logger.info(f"[深交所] 公告采集完成, 共 {count} 条")
        return count
