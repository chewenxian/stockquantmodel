"""
东方财富数据源：新闻 + 行情 + 资金流向 + 板块排行
数据源：https://www.eastmoney.com

API说明（2026年已验证）：
- 行情：push2.eastmoney.com/api/qt/ulist.np/get（批量）/ api/qt/stock/get（单只）
- 资金流向：push2.eastmoney.com/api/qt/clist/get（行情列表）
- 板块排行：push2.eastmoney.com/api/qt/clist/get（同上不同fs参数）
- 新闻：push2.eastmoney.com/api/qt/article/list → 404，改用首页HTML解析
- 龙虎榜：push2ex.eastmoney.com/getStockDragon → 404，改用data.eastmoney.com页面解析
"""
import re
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

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
        # 行情相关API（已验证可用）
        self.batch_quote_api = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        self.single_quote_api = "https://push2.eastmoney.com/api/qt/stock/get"
        self.clist_api = "https://push2.eastmoney.com/api/qt/clist/get"
        # 新闻：从东方财富首页解析
        self.homepage_url = "https://www.eastmoney.com/"
        # 龙虎榜：从数据页面解析
        self.dragon_url = "https://data.eastmoney.com/stock/tradedetail.html"

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {"error": 0}
        stocks = self.db.load_stocks()

        try:
            results["news"] = self._collect_news()
        except Exception as e:
            logger.error(f"[东方财富] 新闻采集异常: {e}")
            results["news"] = 0

        try:
            results["quotes"] = self._collect_quotes(stocks)
        except Exception as e:
            logger.error(f"[东方财富] 行情采集异常: {e}")
            results["quotes"] = 0

        try:
            results["money_flow"] = self._collect_money_flow(stocks)
        except Exception as e:
            logger.error(f"[东方财富] 资金流向采集异常: {e}")
            results["money_flow"] = 0

        try:
            results["dragon_tiger"] = self._collect_dragon_tiger()
        except Exception as e:
            logger.error(f"[东方财富] 龙虎榜采集异常: {e}")
            results["dragon_tiger"] = 0

        try:
            results["boards"] = self._collect_boards()
        except Exception as e:
            logger.error(f"[东方财富] 板块排行采集异常: {e}")
            results["boards"] = 0

        total = sum(results.values())
        logger.info(f"[东方财富] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_news(self) -> int:
        """
        从东方财富首页解析最新新闻
        首页URL: https://www.eastmoney.com/
        新闻链接格式: https://finance.eastmoney.com/a/YYYYMMDDXXXXXXXXX.html
        """
        count = 0
        headers = {
            "Referer": "https://www.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }
        resp = self.get(self.homepage_url, headers=headers)
        if not resp:
            logger.warning("[东方财富] 首页请求失败，尝试备用新闻源")
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        # 查找新闻链接 - 东方财富首页的新闻链接包含 /a/ 路径
        seen_titles = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            # 匹配新闻文章链接格式
            if re.search(r'finance\.eastmoney\.com/a/\d+', href) or re.search(r'/a/\d+', href):
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 5 or title in seen_titles:
                    continue
                seen_titles.add(title)

                # 构建完整URL
                full_url = href if href.startswith("http") else f"https:{href}" if href.startswith("//") else f"https://www.eastmoney.com{href}"

                # 尝试从URL中提取发布日期
                pub_date = ""
                date_match = re.search(r'/a/(\d{8})', href)
                if date_match:
                    d = date_match.group(1)
                    pub_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

                try:
                    news_id = self.db.insert_news(
                        title=title,
                        url=full_url,
                        source="东方财富",
                        summary="",
                        published_at=pub_date or datetime.now().isoformat(),
                    )
                    if news_id:
                        count += 1
                except Exception as e:
                    logger.warning(f"新闻入库异常: {e}")

        # 如果首页没有解析到足够的新闻，尝试搜索接口
        if count < 10:
            logger.info(f"[东方财富] 首页解析到 {count} 条，尝试搜索接口补充")
            count += self._collect_news_from_search()

        return count

    def _collect_news_from_search(self) -> int:
        """从东方财富搜索接口补充新闻"""
        count = 0
        try:
            resp = self.get(
                "https://so.eastmoney.com/news/s",
                params={"keyword": "A股", "pageindex": 1, "pagesize": 20},
                headers={"Referer": "https://www.eastmoney.com/"},
            )
            if not resp:
                return 0

            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "article" in href or "detail" in href:
                    title = a_tag.get_text(strip=True)
                    if title and len(title) > 5:
                        full_url = href if href.startswith("http") else f"https:{href}"
                        try:
                            news_id = self.db.insert_news(
                                title=title,
                                url=full_url,
                                source="东方财富",
                                published_at=datetime.now().isoformat(),
                            )
                            if news_id:
                                count += 1
                        except:
                            pass
        except Exception as e:
            logger.warning(f"[东方财富] 搜索接口异常: {e}")

        return count

    def _collect_quotes(self, stocks: List[Dict]) -> int:
        """采集实时行情 - 使用 ulist.np/get 批量接口（已验证可用）"""
        count = 0
        # 东方财富证券ID格式: 1.600519 (1=SH, 0=SZ)
        secids = []
        for s in stocks:
            prefix = "1." if s["market"] == "SH" else "0."
            secids.append(f"{prefix}{s['code']}")

        if not secids:
            return 0

        # 批量查询（ulist.np/get 支持一次查询最多50只）
        for i in range(0, len(secids), 50):
            batch = ",".join(secids[i:i+50])
            params = {
                "fltt": 2,
                "fields": "f2,f3,f4,f5,f6,f7,f15,f16,f17,f18,f20,f21,f57",
                "secids": batch,
                "invt": 2,
            }
            headers = {
                "Referer": "https://quote.eastmoney.com/",
                "User-Agent": self._random_ua(),
            }
            data = self.get_json(self.batch_quote_api, params, headers=headers)
            if data and data.get("data"):
                # ulist.np/get 返回格式: {data:{diff:[...]}}
                items = data["data"].get("diff", [])
                for item in items:
                    try:
                        code = str(item.get("f57", ""))
                        # 去除前缀 1./0.
                        code = re.sub(r'^\d+\.', '', code)
                        if not code:
                            continue

                        price = item.get("f2", 0) or 0
                        change_pct = item.get("f3", 0) or 0
                        volume = item.get("f4", 0) or 0  # 成交量(手)
                        amount = item.get("f5", 0) or 0   # 成交额
                        high = item.get("f15", 0) or 0
                        low = item.get("f16", 0) or 0
                        opening = item.get("f17", 0) or 0
                        turnover = item.get("f20", 0) or 0  # 换手率
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
        """采集资金流向排行（全市场，按主力净流入排序）"""
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

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
        data = self.get_json(self.clist_api, params, headers=headers)
        if data and data.get("data"):
            items = data["data"].get("diff", [])
            for item in items:
                try:
                    code = str(item.get("f12", ""))
                    if not code:
                        continue
                    name = item.get("f14", "")
                    main_net = (item.get("f62", 0) or 0) / 1e8  # 转亿
                    retail_net = (item.get("f64", 0) or 0) / 1e8
                    large_order_net = (item.get("f66", 0) or 0) / 1e8
                    total_amount = (item.get("f69", 0) or 0) / 1e8

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
        """
        采集龙虎榜数据
        原API: push2ex.eastmoney.com/getStockDragon → 404
        改用 data.eastmoney.com 页面解析
        """
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        try:
            # 尝试从数据页面解析龙虎榜
            url = "https://data.eastmoney.com/stock/tradedetail.html"
            resp = self.get(url, headers=headers)
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                # 查找表格数据
                table = soup.find("table", id=re.compile(r"dt|dragon|tiger"))
                if table:
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # 跳过表头
                        cols = row.find_all("td")
                        if len(cols) >= 5:
                            try:
                                code = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                                if not code:
                                    continue
                                code = re.sub(r'\D', '', code)
                                net_str = cols[4].get_text(strip=True) if len(cols) > 4 else "0"
                                net_amount = float(net_str.replace(",", "").replace("亿", "e8").replace("万", "e4")) if net_str else 0

                                self.db.insert_money_flow(
                                    code=code, date=today,
                                    main_net=net_amount,
                                    total_amount=0
                                )
                                count += 1
                            except:
                                pass
        except Exception as e:
            logger.warning(f"[东方财富] 龙虎榜页面解析异常: {e}")

        # 如果页面解析失败，尝试备用API
        if count == 0:
            try:
                # 尝试龙虎榜API - 有些变体可能有效
                data = self.get_json(
                    "https://push2ex.eastmoney.com/getStockDragon",
                    params={"pageSize": 10, "pageNum": 1, "sortType": "Zdf", "sortOrder": "desc"},
                    headers=headers,
                )
                if data and data.get("data"):
                    items = data["data"].get("list", []) if isinstance(data["data"], dict) else data["data"]
                    if isinstance(items, list):
                        for item in items:
                            try:
                                code = str(item.get("sc", ""))
                                net_amount = (item.get("netAmt", 0) or 0) / 1e4
                                if code:
                                    self.db.insert_money_flow(
                                        code=code, date=today, main_net=net_amount, total_amount=0
                                    )
                                    count += 1
                            except:
                                pass
            except Exception as e:
                logger.debug(f"[东方财富] 龙虎榜备用API也失败: {e}")

        return count

    def _collect_boards(self) -> int:
        """采集板块涨跌排行（行业板块、概念板块）"""
        count = 0
        headers = {
            "Referer": "https://data.eastmoney.com/bkzj/hy.html",
            "User-Agent": self._random_ua(),
        }

        # 行业板块 + 概念板块
        board_configs = [
            ("m:90+t:2", "行业板块"),  # 行业板块
            ("m:90+t:3", "概念板块"),  # 概念板块
        ]

        for fs, board_type in board_configs:
            params = {
                "pn": 1, "pz": 30,
                "po": 1, "np": 1,
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2,
                "fid": "f3",
                "fs": fs,
                "fields": "f12,f14,f2,f3,f4,f104,f105",
            }
            data = self.get_json(self.clist_api, params, headers=headers)
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
        """按个股采集相关新闻（从个股页面解析）"""
        count = 0
        try:
            prefix = "1." if code.startswith("6") else "0."
            # 从东方财富个股行情页面解析新闻
            url = f"https://quote.eastmoney.com/{prefix.replace('.','')}{code}.html"
            resp = self.get(url, headers={"Referer": "https://www.eastmoney.com/"})
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if re.search(r'/a/\d+', href):
                        title = a_tag.get_text(strip=True)
                        if title and len(title) > 5:
                            full_url = href if href.startswith("http") else f"https:{href}"
                            news_id = self.db.insert_news(
                                title=title, url=full_url,
                                source="东方财富-个股",
                                published_at=datetime.now().isoformat()
                            )
                            if news_id:
                                self.db.link_news_stock(news_id, code)
                                count += 1
        except Exception as e:
            logger.warning(f"[东方财富] 个股新闻采集异常({code}): {e}")

        return count
