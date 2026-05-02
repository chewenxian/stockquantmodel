"""
新浪财经：行情快照 + 多板块新闻 + 美股数据

数据源（2026年5月验证）：
- 新闻：https://finance.sina.com.cn/{板快页面} → HTML解析文章列表
- 行情：https://hq.sinajs.cn/list=sh600519,sz000858
- 美股：https://hq.sinajs.cn/list=.DJI,.IXIC,.INX
"""
import re
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class SinaFinanceCollector(BaseCollector):
    """
    新浪财经数据源
    - 多板块新闻（财经首页/股市/基金/外汇/滚动等）
    - 实时行情 (hq.sinajs.cn)
    - 美股行情
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    @property
    def _tracker_key(self) -> str:
        return "sina_finance"

    def collect(self) -> Dict[str, int]:
        results = {}
        stocks = self.db.load_stocks()

        # 多板块新闻采集
        try:
            news_result = self._collect_news()
            if isinstance(news_result, dict):
                results["news"] = news_result.get("total", 0)
            else:
                results["news"] = news_result
        except Exception as e:
            logger.error(f"[新浪财经] 新闻采集异常: {e}")
            results["news"] = 0

        # 行情
        try:
            results["quotes"] = self._collect_quotes(stocks)
        except Exception as e:
            logger.error(f"[新浪财经] 行情采集异常: {e}")
            results["quotes"] = 0

        # 美股
        try:
            results["us_market"] = self._collect_us_market()
        except Exception as e:
            logger.error(f"[新浪财经] 美股采集异常: {e}")
            results["us_market"] = 0

        # 新闻按标题关联自选股
        try:
            linked = self._link_news_to_stocks()
            if linked:
                logger.info(f"[新浪财经] 标题匹配关联 {linked} 条新闻")
        except Exception as e:
            logger.warning(f"[新浪财经] 标题匹配异常: {e}")

        total = sum(results.values())
        logger.info(f"[新浪财经] 采集完成: {results}, 总计 {total} 条")
        return results

    # ──────────────────────────────────────────
    # 多板块新闻采集
    # ──────────────────────────────────────────

    def _collect_news(self) -> dict:
        """
        采集新浪财经各板块新闻

        板块列表：
        - 财经首页：综合财经要闻
        - 股市：A股市场新闻
        - 滚动：实时滚动新闻
        - 基金：基金相关
        - 外汇：外汇市场
        - 期货：期货市场
        """
        sections = [
            ("https://finance.sina.com.cn/", "财经首页"),
            ("https://finance.sina.com.cn/stock/", "股市"),
            ("https://finance.sina.com.cn/roll/", "滚动"),
            ("https://finance.sina.com.cn/fund/", "基金"),
            ("https://finance.sina.com.cn/forex/", "外汇"),
            ("https://finance.sina.com.cn/futures/", "期货"),
            ("https://finance.sina.com.cn/money/bond/", "债券"),
        ]

        total_count = 0
        section_counts = {}

        for url, section_name in sections:
            try:
                count = self._collect_section_news(url, section_name)
                if count:
                    section_counts[section_name] = count
                    total_count += count
                    logger.info(f"  [新浪-{section_name}] 采集 {count} 条")
            except Exception as e:
                logger.warning(f"[新浪-{section_name}] 采集异常: {e}")

        logger.info(f"[新浪财经] 新闻采集完成: {section_counts}, 总计 {total_count} 条")
        return {"total": total_count, **section_counts}

    def _collect_section_news(self, page_url: str, section_name: str) -> int:
        """采集单个板块的新闻列表（批量插入）"""
        headers = {
            "Referer": "https://finance.sina.com.cn/",
            "User-Agent": self._random_ua(),
        }

        resp = self.get(page_url, headers=headers)
        if not resp:
            return 0

        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, "lxml")

        # 提取所有文章链接
        seen_urls = set()
        news_batch = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            title = a_tag.get_text(strip=True)

            if not title or len(title) < 8:
                continue
            if not re.search(r'(finance|cj)\.sina.*shtml|doc', href):
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # 构建完整URL
            if href.startswith("//"):
                full_url = f"https:{href}"
            elif href.startswith("/"):
                full_url = f"https://finance.sina.com.cn{href}"
            elif not href.startswith("http"):
                continue
            else:
                full_url = href

            pub_date = self._extract_date_from_url(full_url)

            news_batch.append({
                "title": title,
                "url": full_url,
                "source": f"新浪-{section_name}",
                "summary": "",
                "published_at": pub_date or datetime.now().isoformat(),
            })

        if not news_batch:
            return 0
        return self.db.batch_insert_news(news_batch)

    def _extract_date_from_url(self, url: str) -> str:
        """从新浪文章URL中提取日期"""
        # 格式: .../2026-04-30/doc-xxx.shtml
        m = re.search(r'/(\d{4}-\d{2}-\d{2})/', url)
        if m:
            return m.group(1)
        # 格式: .../20260430/xxx.shtml
        m = re.search(r'/(\d{4})(\d{2})(\d{2})/', url)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return ""

    # ──────────────────────────────────────────
    # 行情采集（保留原有逻辑）
    # ──────────────────────────────────────────

    def _collect_quotes(self, stocks: List[Dict]) -> int:
        """新浪行情接口 - 支持沪深（批量插入）"""
        codes = []
        for s in stocks:
            prefix = "sh" if s["market"] == "SH" else "sz"
            codes.append(f"{prefix}{s['code']}")

        if not codes:
            return 0

        batch_snapshots = []
        for i in range(0, len(codes), 20):
            batch = ",".join(codes[i:i+20])
            url = f"https://hq.sinajs.cn/list={batch}"
            headers = {
                "Referer": "https://finance.sina.com.cn",
                "Accept": "*/*",
            }
            resp = self.get(url, headers=headers)
            if not resp:
                continue

            text = resp.text
            for line in text.strip().split("\n"):
                match = re.search(r'hq_str_(\w+)="(.+)"', line)
                if not match:
                    continue

                code = match.group(1)
                parts = match.group(2).split(",")
                if len(parts) < 30:
                    continue

                try:
                    close_price = float(parts[2]) if parts[2] else 0
                    price = float(parts[3]) if parts[3] else 0
                    high = float(parts[4]) if parts[4] else 0
                    low = float(parts[5]) if parts[5] else 0
                    open_price = float(parts[1]) if parts[1] else 0
                    volume = float(parts[8]) if parts[8] else 0
                    amount = float(parts[9]) if parts[9] else 0
                    change_pct = round((price - close_price) / close_price * 100, 2) if close_price > 0 else 0

                    batch_snapshots.append({
                        "stock_code": code,
                        "price": price,
                        "change_pct": change_pct,
                        "volume": volume,
                        "amount": amount,
                        "high": high,
                        "low": low,
                        "open": open_price,
                    })
                except (ValueError, IndexError):
                    continue

        if not batch_snapshots:
            return 0
        return self.db.batch_insert_market_snapshots(batch_snapshots)

    # ──────────────────────────────────────────
    # 美股行情（保留原有逻辑）
    # ──────────────────────────────────────────

    def _collect_us_market(self) -> int:
        """采集美股主要指数并入库"""
        count = 0
        us_codes = [".DJI", ".IXIC", ".INX"]
        url = f"https://hq.sinajs.cn/list={'%2C'.join(us_codes)}"
        headers = {"Referer": "https://finance.sina.com.cn"}

        resp = self.get(url, headers=headers)
        if not resp:
            return count

        for line in resp.text.strip().split("\n"):
            try:
                match = re.search(r'hq_str_(\w+)="(.+)"', line)
                if not match:
                    continue
                code = match.group(1)
                parts = match.group(2).split(",")
                if len(parts) < 10:
                    continue

                fields = {
                    "name": parts[0],
                    "open": float(parts[1]) if parts[1] else 0,
                    "close": float(parts[2]) if parts[2] else 0,
                    "price": float(parts[3]) if parts[3] else 0,
                    "high": float(parts[4]) if parts[4] else 0,
                    "low": float(parts[5]) if parts[5] else 0,
                }

                change_pct = 0
                if fields["close"] > 0:
                    change_pct = (fields["price"] - fields["close"]) / fields["close"] * 100

                self.db.insert_market_snapshot(
                    code=code.upper(), price=fields["price"],
                    change_pct=round(change_pct, 2),
                    volume=0, amount=0,
                    high=fields["high"], low=fields["low"],
                    open=fields["open"]
                )
                count += 1
            except Exception as e:
                logger.warning(f"美股行情解析异常 ({line[:80]}): {e}")

        return count

    def _link_news_to_stocks(self) -> int:
        """
        将未关联的新浪新闻按标题中的股票名称匹配到自选股（批量关联）
        """
        try:
            conn = self.db._connect()
            stocks = conn.execute("SELECT code, name FROM stocks").fetchall()
            stocks_sorted = sorted(stocks, key=lambda s: -len(s["name"]))

            unlinked = conn.execute("""
                SELECT n.id, n.title FROM news n
                LEFT JOIN news_stocks ns ON n.id = ns.news_id
                WHERE ns.news_id IS NULL
                AND n.source LIKE '新浪-%'
                ORDER BY n.id DESC LIMIT 300
            """).fetchall()

            links = []
            for news in unlinked:
                title = news["title"] or ""
                for s in stocks_sorted:
                    if s["name"] in title:
                        links.append((news["id"], s["code"], 0.0))
                        break

            self.db._close(conn)

            linked = 0
            if links:
                linked = self.db.batch_link_news_stocks(links)
            return linked
        except Exception as e:
            logger.warning(f"[新浪财经] 标题匹配异常: {e}")
            return 0
