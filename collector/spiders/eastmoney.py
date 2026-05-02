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

        # 按个股采集新闻并关联自选股
        stock_news_count = 0
        try:
            for stock in stocks[:50]:
                stock_news_count += self.collect_news_for_stock(stock["code"])
            results["stock_news"] = stock_news_count
        except Exception as e:
            logger.error(f"[东方财富] 个股新闻采集异常: {e}")
            results["stock_news"] = 0

        # 标题匹配：将通用新闻按股票名关联
        try:
            linked = self.link_unlinked_news_by_title()
            results["title_linked"] = linked
        except Exception as e:
            logger.error(f"[东方财富] 标题匹配异常: {e}")

        total = sum(v for v in results.values() if isinstance(v, int))
        logger.info(f"[东方财富] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_news(self) -> int:
        """
        从东方财富首页解析最新新闻 (v2.0 使用批量插入)

        首页URL: https://www.eastmoney.com/
        新闻链接格式: https://finance.eastmoney.com/a/YYYYMMDDXXXXXXXXX.html
        """
        headers = {
            "Referer": "https://www.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }
        resp = self.get(self.homepage_url, headers=headers)
        if not resp:
            logger.warning("[东方财富] 首页请求失败，尝试备用新闻源")
            return 0

        soup = BeautifulSoup(resp.text, "lxml")
        # 查找新闻链接 - 东方财富首页的新闻链接包含 /a/ 路径
        seen_titles = set()
        news_batch = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if re.search(r'finance\.eastmoney\.com/a/\d+', href) or re.search(r'/a/\d+', href):
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 5 or title in seen_titles:
                    continue
                seen_titles.add(title)

                full_url = href if href.startswith("http") else f"https:{href}" if href.startswith("//") else f"https://www.eastmoney.com{href}"

                pub_date = ""
                date_match = re.search(r'/a/(\d{8})', href)
                if date_match:
                    d = date_match.group(1)
                    pub_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

                news_batch.append({
                    "title": title,
                    "url": full_url,
                    "source": "东方财富",
                    "summary": "",
                    "published_at": pub_date or datetime.now().isoformat(),
                })

        count = 0
        if news_batch:
            count = self.db.batch_insert_news(news_batch)
            logger.info(f"[东方财富] 首页批量插入 {count}/{len(news_batch)} 条新闻")

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
        """采集实时行情 - 使用 ulist.np/get 批量接口，批量插入"""
        secids = []
        for s in stocks:
            prefix = "1." if s["market"] == "SH" else "0."
            secids.append(f"{prefix}{s['code']}")

        if not secids:
            return 0

        batch_snapshots = []
        headers = {
            "Referer": "https://quote.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        for i in range(0, len(secids), 50):
            batch = ",".join(secids[i:i+50])
            params = {
                "fltt": 2,
                "fields": "f2,f3,f4,f5,f6,f7,f15,f16,f17,f18,f20,f21,f57",
                "secids": batch,
                "invt": 2,
            }
            data = self.get_json(self.batch_quote_api, params, headers=headers)
            if data and data.get("data"):
                items = data["data"].get("diff", [])
                for item in items:
                    try:
                        code = str(item.get("f57", ""))
                        code = re.sub(r'^\d+\.', '', code)
                        if not code:
                            continue
                        batch_snapshots.append({
                            "stock_code": code,
                            "price": item.get("f2", 0) or 0,
                            "change_pct": item.get("f3", 0) or 0,
                            "volume": item.get("f4", 0) or 0,
                            "amount": item.get("f5", 0) or 0,
                            "high": item.get("f15", 0) or 0,
                            "low": item.get("f16", 0) or 0,
                            "open": item.get("f17", 0) or 0,
                            "turnover_rate": item.get("f20", 0) or 0,
                            "pe": item.get("f21", 0) or 0,
                        })
                    except Exception as e:
                        logger.warning(f"行情解析异常: {e}")

        count = self.db.batch_insert_market_snapshots(batch_snapshots) if batch_snapshots else 0
        logger.info(f"[东方财富] 批量插入行情 {count}/{len(batch_snapshots)} 条")
        return count

    def _collect_money_flow(self, stocks: List[Dict]) -> int:
        """采集资金流向排行（全市场，按主力净流入排序，批量插入）"""
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
        if not data or not data.get("data"):
            return 0

        items = data["data"].get("diff", [])
        batch = []
        for item in items:
            try:
                code = str(item.get("f12", ""))
                if not code:
                    continue
                batch.append({
                    "code": code,
                    "date": today,
                    "main_net": (item.get("f62", 0) or 0) / 1e8,
                    "retail_net": (item.get("f64", 0) or 0) / 1e8,
                    "large_order_net": (item.get("f66", 0) or 0) / 1e8,
                    "total_amount": (item.get("f69", 0) or 0) / 1e8,
                })
            except Exception as e:
                logger.warning(f"资金流向解析异常: {e}")

        if not batch:
            return 0
        count = self.db.batch_insert_money_flow(batch)
        logger.info(f"[东方财富] 批量插入资金流向 {count}/{len(batch)} 条")
        return count

    def _collect_dragon_tiger(self) -> int:
        """
        采集龙虎榜数据（批量插入）

        原API: push2ex.eastmoney.com/getStockDragon → 404
        改用 data.eastmoney.com 页面解析
        """
        today = datetime.now().strftime("%Y-%m-%d")
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        batch = []

        # 方案一：从数据页面解析龙虎榜
        try:
            url = "https://data.eastmoney.com/stock/tradedetail.html"
            resp = self.get(url, headers=headers)
            if resp:
                soup = BeautifulSoup(resp.text, "lxml")
                table = soup.find("table", id=re.compile(r"dt|dragon|tiger"))
                if table:
                    rows = table.find_all("tr")
                    for row in rows[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 5:
                            try:
                                code = cols[1].get_text(strip=True)
                                if not code:
                                    continue
                                code = re.sub(r'\D', '', code)
                                net_str = cols[4].get_text(strip=True)
                                net_amount = float(net_str.replace(",", "").replace("亿", "e8").replace("万", "e4")) if net_str else 0

                                batch.append({
                                    "code": code,
                                    "trade_date": today,
                                    "net_amount": net_amount,
                                })
                            except:
                                pass
        except Exception as e:
            logger.warning(f"[东方财富] 龙虎榜页面解析异常: {e}")

        # 方案二：如果页面没解析到数据，尝试备用API
        if not batch:
            try:
                data = self.get_json(
                    "https://push2ex.eastmoney.com/getStockDragon",
                    params={"pageSize": 10, "pageNum": 1, "sortType": "Zdf", "sortOrder": "desc"},
                    headers=headers,
                )
                if data and data.get("data"):
                    raw = data["data"]
                    items = raw.get("list", []) if isinstance(raw, dict) else raw
                    if isinstance(items, list):
                        for item in items:
                            try:
                                code = str(item.get("sc", ""))
                                if not code:
                                    continue
                                batch.append({
                                    "code": code,
                                    "trade_date": today,
                                    "net_amount": (item.get("netAmt", 0) or 0) / 1e4,
                                })
                            except:
                                pass
            except Exception as e:
                logger.debug(f"[东方财富] 龙虎榜备用API也失败: {e}")

        if not batch:
            return 0
        count = self.db.batch_insert_dragon_tiger(batch)
        logger.info(f"[东方财富] 批量插入龙虎榜 {count}/{len(batch)} 条")
        return count

    def _collect_boards(self) -> int:
        """采集板块涨跌排行（行业板块、概念板块，批量插入）"""
        headers = {
            "Referer": "https://data.eastmoney.com/bkzj/hy.html",
            "User-Agent": self._random_ua(),
        }

        # 行业板块 + 概念板块
        board_configs = [
            ("m:90+t:2", "行业板块"),
            ("m:90+t:3", "概念板块"),
        ]

        all_batch = []
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
                        all_batch.append({
                            "board_name": item.get("f14", ""),
                            "board_code": str(item.get("f12", "")),
                            "change_pct": item.get("f3", 0) or 0,
                            "leader_stocks": item.get("f104", "") or "",
                        })
                    except:
                        pass

        if not all_batch:
            return 0
        count = self.db.batch_insert_boards(all_batch)
        logger.info(f"[东方财富] 批量插入板块排行 {count}/{len(all_batch)} 条")
        return count

    def collect_news_for_stock(self, code: str) -> int:
        """按个股采集相关新闻（从个股页面解析，批量插入+关联）"""
        news_batch = []
        try:
            prefix = "1." if code.startswith("6") else "0."
            url = f"https://quote.eastmoney.com/{prefix.replace('.','')}{code}.html"
            resp = self.get(url, headers={"Referer": "https://www.eastmoney.com/"})
            if resp:
                soup = BeautifulSoup(resp.text, "lxml")
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if re.search(r'/a/\d+', href):
                        title = a_tag.get_text(strip=True)
                        if title and len(title) > 5:
                            full_url = href if href.startswith("http") else f"https:{href}"
                            pub_date = ""
                            date_match = re.search(r'/a/(\d{8})', href)
                            if date_match:
                                d = date_match.group(1)
                                pub_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                            news_batch.append({
                                "title": title,
                                "url": full_url,
                                "source": "东方财富-个股",
                                "summary": "",
                                "published_at": pub_date or datetime.now().isoformat(),
                            })
        except Exception as e:
            logger.warning(f"[东方财富] 个股新闻采集异常({code}): {e}")
            return 0

        if not news_batch:
            return 0

        # 1. 批量插入新闻
        self.db.batch_insert_news(news_batch)

        # 2. 批量关联：通过URL查找已插入的新闻ID并关联到个股
        urls = [item["url"] for item in news_batch if item.get("url")]
        if urls:
            conn = self.db._connect()
            try:
                placeholders = ",".join(["?"] * len(urls))
                rows = conn.execute(
                    f"SELECT id, url FROM news WHERE url IN ({placeholders})",
                    urls
                ).fetchall()
                links = [(r["id"], code, 0.0) for r in rows]
                if links:
                    self.db.batch_link_news_stocks(links)
            except Exception as e:
                logger.warning(f"[东方财富] 批量关联个股新闻异常: {e}")
            finally:
                self.db._close(conn)

        return len(news_batch)

    def link_unlinked_news_by_title(self):
        """
        通过标题中的股票名称/代码匹配，将未关联的通用新闻关联到自选股（批量关联）
        """
        conn = None
        try:
            conn = self.db._connect()
            stocks = conn.execute("SELECT code, name FROM stocks").fetchall()
            stocks_sorted = sorted(stocks, key=lambda s: -len(s["name"]))

            unlinked = conn.execute("""
                SELECT n.id, n.title FROM news n
                LEFT JOIN news_stocks ns ON n.id = ns.news_id
                WHERE ns.news_id IS NULL
                ORDER BY n.id DESC LIMIT 500
            """).fetchall()

            links = []
            for news in unlinked:
                title = news["title"] or ""
                for s in stocks_sorted:
                    if s["name"] in title:
                        links.append((news["id"], s["code"], 0.0))
                        break

            linked = 0
            if links:
                linked = self.db.batch_link_news_stocks(links)

            logger.info(f"[东方财富] 标题匹配关联 {linked} 条新闻")
            return linked
        except Exception as e:
            logger.warning(f"[东方财富] 标题匹配异常: {e}")
            return 0
        finally:
            if conn is not None:
                self.db._close(conn) 
