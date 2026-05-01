"""
上交所公告采集器 (SSE)
数据源：http://www.sse.com.cn
采集：上市公司公告、监管公告
"""
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class SSECollector(BaseCollector):
    """
    上交所公告采集器
    官网：http://www.sse.com.cn
    公告类型：上市公司公告、监管公告、业务公告
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.announce_url = "http://www.sse.com.cn/disclosure/listedinfo/announcement/"
        self.query_url = "http://query.sse.com.cn/security/stock/queryBulletinStock.do"
        # 上交所公告分类代码
        self.bulletin_types = [
            ("0", "全部"),
            ("0101", "年报"),
            ("0102", "半年报"),
            ("0103", "季报"),
            ("0201", "分红"),
            ("0301", "业绩预告"),
            ("0801", "配股"),
            ("0901", "增发"),
            ("1101", "重组"),
            ("1301", "停牌"),
            ("1302", "复牌"),
            ("1401", "减持"),
            ("1402", "增持"),
            ("1501", "回购"),
            ("1705", "问询函回复"),
            ("1706", "监管函"),
        ]

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["announcements"] = self._collect_announcements()
        total = sum(results.values())
        logger.info(f"[上交所] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_announcements(self) -> int:
        """采集上交所公告"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            "Referer": self.announce_url,
            "User-Agent": self._random_ua(),
            "Accept": "application/json, text/plain, */*",
        }

        for bulletin_code, bulletin_name in self.bulletin_types:
            try:
                params = {
                    "productType": "",
                    "bulletinType": bulletin_code,
                    "pageSize": 30,
                    "pageNum": 1,
                    "stockType": "",
                    "beginDate": week_ago,
                    "endDate": today,
                    "jsonCallBack": f"jsonp{int(datetime.now().timestamp())}",
                    "_": str(int(datetime.now().timestamp() * 1000)),
                }
                resp = self.get(self.query_url, params=params, headers=headers)
                if not resp:
                    continue

                # 上交所返回 JSONP 格式，需要提取 JSON
                text = resp.text
                json_str = re.search(r"jsonp\d+\((.*)\)", text)
                if not json_str:
                    continue

                data = json.loads(json_str.group(1))
                items = data.get("result", [])

                for item in items:
                    try:
                        title = item.get("bulletinTitle", "").strip()
                        if not title:
                            continue

                        ann_id = item.get("bulletinId", "")
                        stock_code = item.get("stockCode", "")
                        pub_date = item.get("bulletinDate", "")
                        doc_url = item.get("docURL", "")
                        code_type = item.get("stockType", "")

                        # 构建完整URL
                        full_url = ""
                        if doc_url:
                            if doc_url.startswith("http"):
                                full_url = doc_url
                            else:
                                full_url = f"http://www.sse.com.cn{doc_url}"
                        elif ann_id:
                            full_url = f"http://www.sse.com.cn/disclosure/listedinfo/announcement/c/{ann_id}"

                        # 确认股票代码
                        if not stock_code and code_type:
                            stock_code = code_type

                        # 判断是否重大公告
                        important_keywords = [
                            "业绩预告", "业绩修正", "分红", "送转", "停牌", "复牌",
                            "重大资产重组", "收购", "减持", "增持", "回购",
                            "立案调查", "行政处罚", "监管函", "问询函",
                            "中标", "重大合同", "战略合作", "定增", "配股",
                            "预增", "预减", "预亏", "扭亏",
                        ]
                        is_important = any(k in title for k in important_keywords)

                        # 存入公告表
                        if stock_code:
                            # 上交所代码通常6位，以6开头
                            code = stock_code.zfill(6) if len(stock_code) < 6 else stock_code
                            self.db.insert_announcement(
                                code=code,
                                title=title,
                                url=full_url,
                                announce_type=bulletin_name,
                                summary=f"上交所-{bulletin_name}",
                                publish_date=pub_date,
                            )

                        # 重要公告也存入新闻表
                        if is_important:
                            self.db.insert_news(
                                title=f"【上交所公告】{title}",
                                url=full_url,
                                source=f"上交所-{bulletin_name}",
                                summary=f"上交所上市公司{bulletin_name}公告",
                                published_at=pub_date,
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"上交所公告解析异常: {e}")
            except Exception as e:
                logger.warning(f"上交所请求异常({bulletin_name}): {e}")

        logger.info(f"[上交所] 公告采集完成, 共 {count} 条")
        return count
