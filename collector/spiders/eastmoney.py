"""
东方财富数据源：新闻 + 行情 + 资金流向 + 龙虎榜 + 板块
"""
import re
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dateutil import parser

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class EastMoneyCollector(BaseCollector):
    """
    东方财富多维度采集器
    数据源：https://www.eastmoney.com
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.news_api = "https://push2.eastmoney.com/api/qt/article/list"
        self.quote_api = "https://push2.eastmoney.com/api/qt/stock/get"
        self.money_flow_api = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
        self.dragon_api = "https://push2ex.eastmoney.com/getStockDragon"
        self.board_api = "https://push2.eastmoney.com/api/qt/clist/get"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        stocks = self.db.load_stocks()

        results["news"] = self._collect_news()
        results["quotes"] = self._collect_quotes(stocks)
        results["money_flow"] = self._collect_money_flow(stocks)
        results["dragon_tiger"] = self._collect_dragon_tiger()
        results["boards"] = self._collect_boards()

        total = sum(results.values())
        logger.info(f"[东方财富] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_news(self) -> int:
        """
        采集股票相关新闻
        东方财富新闻分类：
        - 1: 个股新闻
        - 2: 板块新闻
        - 8: 快讯
        """
        count = 0
        # 综合财经新闻
        categories = [
            (1, "个股"), (2, "板块"), (3, "宏观"),
            (4, "产业"), (8, "快讯"), (82, "股市热点")
        ]
        for cat_id, cat_name in categories:
            params = {
                "secids": "",
                "art_type": cat_id,
                "pageSize": 50,
                "pageNum": 1,
                "sort": "pushTime",
                "range": "day",
            }
            data = self.get_json(self.news_api, params)
            if data and data.get("data"):
                articles = data["data"].get("list", [])
                for art in articles:
                    try:
                        title = art.get("art_title", "").strip()
                        url = art.get("art_url", "")
                        summary = art.get("art_abstract", "")
                        pub_time = art.get("pushTime", "")
                        source = f"东方财富-{cat_name}"

                        if not title:
                            continue

                        # 解析发布时间
                        pub_dt = None
                        if pub_time:
                            try:
                                pub_dt = datetime.fromtimestamp(pub_time / 1000)
                            except:
                                pub_dt = datetime.now()

                        news_id = self.db.insert_news(
                            title=title, url=url, source=source,
                            summary=summary, published_at=pub_dt.isoformat() if pub_dt else None
                        )

                        # 提取关联股票
                        if news_id and "stock_codes" in art:
                            for sc in art["stock_codes"]:
                                if sc and len(sc) >= 6:
                                    self.db.link_news_stock(news_id, sc)
                        count += 1
                    except Exception as e:
                        logger.warning(f"处理东方财富新闻异常: {e}")
        return count

    def _collect_quotes(self, stocks: List[Dict]) -> int:
        """采集实时行情"""
        count = 0
        # 东方财富股票代码格式: 1.600519 (1=SH, 0=SZ)
        secids = []
        for s in stocks:
            prefix = "1." if s["market"] == "SH" else "0."
            secids.append(f"{prefix}{s['code']}")

        if not secids:
            return 0

        # 批量查询（一次最多50只）
        for i in range(0, len(secids), 50):
            batch = ",".join(secids[i:i+50])
            params = {
                "secids": batch,
                "fields": "f2,f3,f4,f5,f6,f7,f15,f16,f17,f18,f20,f21,f57",
                "invt": 2,
                "cb": "j",  # JSONP callback
            }
            data = self.get_json(self.quote_api, params)
            if data and data.get("data"):
                items = data["data"].get("list", []) if isinstance(data["data"], dict) else data["data"].get("diff", [])
                for item in items:
                    try:
                        code = str(item.get("f57", ""))
                        # 转换回纯数字代码
                        code = code.replace("1.", "").replace("0.", "")
                        price = item.get("f2", 0) or 0
                        change_pct = item.get("f3", 0) or 0
                        volume = item.get("f4", 0) or 0
                        amount = item.get("f5", 0) or 0
                        high = item.get("f15", 0) or 0
                        low = item.get("f16", 0) or 0
                        opening = item.get("f17", 0) or 0
                        turnover = item.get("f20", 0) or 0
                        pe = item.get("f21", 0) or 0

                        self.db.insert_market_snapshot(
                            code=code, price=price, change_pct=change_pct,
                            volume=volume, amount=amount,
                            high=high, low=low, open=opening,
                            turnover_rate=turnover, pe=pe
                        )
                        count += 1
                    except Exception as e:
                        logger.warning(f"行情解析异常: {e}")
        return count

    def _collect_money_flow(self, stocks: List[Dict]) -> int:
        """采集资金流向"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        headers = {"Referer": "https://data.eastmoney.com/"}

        # 全市场资金流向排行
        flow_url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 50,
            "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f62",  # 按主力净流入排序
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f12,f14,f62,f64,f66,f69,f84",
        }
        data = self.get_json(flow_url, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("diff", [])
            for item in items:
                try:
                    code = str(item.get("f12", ""))
                    name = item.get("f14", "")
                    main_net = (item.get("f62", 0) or 0) / 1e8  # 转亿
                    retail_net = (item.get("f64", 0) or 0) / 1e8
                    large_order_net = (item.get("f66", 0) or 0) / 1e8
                    total_amount = (item.get("f69", 0) or 0) / 1e8

                    if code:
                        self.db.insert_money_flow(
                            code=code, date=today,
                            main_net=main_net, retail_net=retail_net,
                            large_order_net=large_order_net,
                            total_amount=total_amount
                        )
                        count += 1
                except Exception as e:
                    logger.warning(f"资金流向解析异常: {e}")
        return count

    def _collect_dragon_tiger(self) -> int:
        """采集龙虎榜"""
        count = 0
        url = "https://push2ex.eastmoney.com/getStockDragon"
        today = datetime.now().strftime("%Y-%m-%d")

        headers = {"Referer": "https://data.eastmoney.com/"}
        params = {
            "pageSize": 30,
            "pageNum": 1,
            "sortType": "Zdf",
            "sortOrder": "desc",
            "reportDate": today,
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        }
        data = self.get_json(url, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("list", []) if isinstance(data["data"], dict) else data["data"]
            if isinstance(items, list):
                for item in items:
                    try:
                        code = str(item.get("sc", ""))
                        buy_amount = (item.get("buyAmt", 0) or 0) / 1e4
                        sell_amount = (item.get("sellAmt", 0) or 0) / 1e4
                        net_amount = (item.get("netAmt", 0) or 0) / 1e4
                        reason = item.get("lbyy", "")

                        if code:
                            self.db.insert_money_flow(
                                code=code, date=today,
                                main_net=net_amount,
                                total_amount=(buy_amount + sell_amount)
                            )
                            count += 1
                    except Exception as e:
                        logger.warning(f"龙虎榜解析异常: {e}")
        return count

    def _collect_boards(self) -> int:
        """采集板块涨跌排行"""
        count = 0
        headers = {"Referer": "https://data.eastmoney.com/bkzj/hy.html"}

        # 行业板块
        params = {
            "pn": 1, "pz": 30,
            "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2,
            "fid": "f3",
            "fs": "m:90+t:2",  # 行业板块
            "fields": "f12,f14,f2,f3,f4,f104,f105",
        }
        data = self.get_json(self.board_api, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("diff", [])
            for item in items:
                try:
                    board_name = item.get("f14", "")
                    change_pct = item.get("f3", 0) or 0
                    leader = item.get("f104", "") or ""

                    conn = self.db._connect()
                    conn.execute("""
                        INSERT INTO board_index(board_name, board_code, change_pct, leader_stocks)
                        VALUES(?, ?, ?, ?)
                    """, (board_name, str(item.get("f12", "")), change_pct, leader))
                    conn.commit()
                    conn.close()
                    count += 1
                except:
                    pass
        return count

    def collect_news_for_stock(self, code: str) -> int:
        """按个股采集相关新闻"""
        prefix = "1." if code.startswith("6") else "0."
        secid = f"{prefix}{code}"
        params = {
            "secids": secid,
            "art_type": 1,
            "pageSize": 30,
            "pageNum": 1,
            "sort": "pushTime",
            "range": "week",
        }
        count = 0
        data = self.get_json(self.news_api, params)
        if data and data.get("data"):
            articles = data["data"].get("list", [])
            for art in articles:
                try:
                    title = art.get("art_title", "").strip()
                    url = art.get("art_url", "")
                    if not title:
                        continue
                    news_id = self.db.insert_news(
                        title=title, url=url,
                        source="东方财富-个股",
                        published_at=datetime.now().isoformat()
                    )
                    if news_id:
                        self.db.link_news_stock(news_id, code)
                        count += 1
                except:
                    pass
        return count
