"""
巨潮资讯网：公司公告（影响股价的官方信息）
数据源：http://www.cninfo.com.cn

API说明（2026年验证）：
- 公告查询API：http://www.cninfo.com.cn/new/hisAnnouncement/query
- 必须使用 JSON POST (Content-Type: application/json)
- 使用 form-data POST 会返回 0 条结果
- stock 参数格式：code,name（如 "000001,平安银行"）
- 需要正确的 Referer + X-Requested-With 头
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
        self.search_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    @property
    def _tracker_key(self) -> str:
        return "cninfo"

    def collect(self) -> Dict[str, int]:
        results = {"error": 0}

        # 先检查增量间隔（默认30分钟，公告不需要太频繁）
        if not self.should_fetch(min_interval_minutes=30):
            info = self.get_last_fetch()
            logger.info(f"[巨潮] 跳过采集（上次 {info['last_success_at'][:19] if info else '未知'}，未到间隔）")
            if info:
                results["announcements"] = info["last_item_count"] or 0
            return results

        stocks = self.db.load_stocks()
        try:
            results["announcements"] = self._collect_announcements(stocks)
            self.mark_fetched(item_count=results["announcements"])
        except Exception as e:
            logger.error(f"[巨潮] 公告采集异常: {e}")
            results["announcements"] = 0
            self.mark_fetched(error=str(e)[:200])
        total = sum(results.values())
        logger.info(f"[巨潮] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_announcements(self, stocks: List[Dict]) -> int:
        """采集公司公告 - 增量模式，只查上次采集到现在的数据"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        # 增量查询：只查上次采集时间之后的数据
        last_info = self.get_last_fetch()
        if last_info and last_info.get("last_success_at"):
            try:
                last_dt = datetime.fromisoformat(last_info["last_success_at"])
                # 往前多查2小时，防止漏掉时间窗口边缘的公告
                start_date = (last_dt - timedelta(hours=2)).strftime("%Y-%m-%d")
            except Exception:
                start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            # 首次采集：查最近7天
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            "Content-Type": "application/json",
            "Referer": "http://www.cninfo.com.cn/new/disclosure/stock",
            "User-Agent": self._random_ua(),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://www.cninfo.com.cn",
        }

        # 先访问首页获取必要cookie
        self.get("http://www.cninfo.com.cn/new/disclosure/stock", headers=headers)

        for stock in stocks:
            try:
                code = stock["code"]
                name = stock["name"]
                plate = self._get_plate(code)

                # 使用 JSON POST - 关键！form-data POST 会返回0条
                payload = {
                    "pageNum": 1,
                    "pageSize": 20,
                    "column": "szse",
                    "tabName": "fulltext",
                    "plate": plate,
                    "stock": f"{code},{name}",
                    "searchkey": "",
                    "secid": "",
                    "category": "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_dxrl_szsh;category_rz_szsh;category_bpszc_szsh",
                    "trade": "",
                    "seDate": f"{start_date}~{today}",
                    "sortName": "",
                    "sortType": "",
                    "isHLtitle": True,
                }

                resp = self.session.post(
                    self.search_url, json=payload, headers=headers, timeout=15
                )
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
                        sec_code = ann.get("secCode", code)

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
                            code=sec_code, title=title, url=full_url,
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
                logger.warning(f"[巨潮] 公告采集异常({stock['code']}): {e}")

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
