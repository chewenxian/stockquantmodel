"""
东方财富股吧情绪采集器
数据源：东方财富股吧

API: https://push2ex.eastmoney.com/getStockBarChart
      https://guba.eastmoney.com/list,code.html
数据：个股讨论热度、帖子数量、看涨看跌比例（情绪反向指标）
"""
import re
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class GubaSentimentCollector(BaseCollector):
    """
    东方财富股吧情绪采集器
    采集个股讨论热度、帖子数量、看涨看跌比例
    作为市场情绪指标使用（反向指标）

    数据源：东方财富股吧
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_url = "https://push2ex.eastmoney.com/getStockBarChart"
        self.guba_url = "https://guba.eastmoney.com"

    def _ensure_table(self):
        """确保 guba_sentiment 表存在"""
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS guba_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                post_count INTEGER DEFAULT 0,
                view_count INTEGER DEFAULT 0,
                bullish_ratio REAL DEFAULT 0.0,
                bearish_ratio REAL DEFAULT 0.0,
                sentiment_score REAL DEFAULT 0.0,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def _get_secid_prefix(self, code: str) -> str:
        """获取东方财富证券ID前缀"""
        code = str(code).strip()
        if code.startswith("6"):
            return "SH"
        elif code.startswith("0") or code.startswith("3"):
            return "SZ"
        elif code.startswith("4") or code.startswith("8"):
            return "BJ"
        return "SZ"

    def _fetch_by_api(self, code: str, name: str) -> Optional[int]:
        """
        通过股吧API采集个股情绪数据
        API: https://push2ex.eastmoney.com/getStockBarChart
        """
        prefix = self._get_secid_prefix(code)
        sec_code = f"{prefix}{code}"

        params = {
            "code": sec_code,
            "page": 1,
            "size": 20,
        }
        headers = {
            "Referer": f"https://guba.eastmoney.com/list,{code}.html",
            "User-Agent": self._random_ua(),
        }

        try:
            data = self.get_json(self.api_url, params=params, headers=headers)
            if not data:
                return None

            # 解析返回数据
            raw_data = data.get("data", {})
            if not raw_data:
                return None

            # 帖子数量
            total_hits = raw_data.get("totalHits", 0) or 0
            if isinstance(total_hits, str):
                try:
                    total_hits = int(total_hits.replace(",", ""))
                except ValueError:
                    total_hits = 0

            # 阅读数
            total_read = raw_data.get("totalRead", 0) or 0
            if isinstance(total_read, str):
                try:
                    total_read = int(total_read.replace(",", ""))
                except ValueError:
                    total_read = 0

            # 总帖数 (另一种方式)
            total_bars = raw_data.get("totalBars", 0) or 0

            # 综合帖子数
            post_count = max(total_hits, total_bars)

            # 看涨/看跌比例 - 从股吧首页list页面获取更多信息
            bullish_ratio = 0.0
            bearish_ratio = 0.0

            # 计算情绪得分: 正值=看涨，负值=看跌
            today = datetime.now().strftime("%Y-%m-%d")

            conn = self.db._connect()
            existing = conn.execute(
                "SELECT id FROM guba_sentiment WHERE stock_code = ? AND trade_date = ?",
                (code, today)
            ).fetchone()

            if not existing:
                conn.execute("""
                    INSERT INTO guba_sentiment(
                        stock_code, stock_name, post_count, view_count,
                        bullish_ratio, bearish_ratio, sentiment_score, trade_date
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, name, post_count, total_read,
                      bullish_ratio, bearish_ratio, 0.0, today))
                conn.commit()
                conn.close()
                return 1

            conn.close()
            return 0

        except Exception as e:
            logger.warning(f"[股吧情绪] API采集异常 ({code}): {e}")
            return None

    def _fetch_by_html(self, code: str, name: str) -> int:
        """
        通过股吧HTML页面解析个股讨论数据
        URL: https://guba.eastmoney.com/list,code.html
        """
        url = f"{self.guba_url}/list,{code}.html"
        headers = {
            "Referer": self.guba_url,
            "User-Agent": self._random_ua(),
        }

        try:
            resp = self.get(url, headers=headers)
            if not resp:
                return 0

            soup = BeautifulSoup(resp.text, "html.parser")

            # 尝试提取帖子总数
            post_count = 0
            total_text = soup.find("span", class_="total")
            if total_text:
                text = total_text.get_text(strip=True)
                nums = re.findall(r'\d+', text)
                if nums:
                    post_count = int(''.join(nums))

            # 尝试提取阅读数
            view_count = 0
            read_text = soup.find("span", class_="read")
            if not read_text:
                # 从其它元素提取
                info_items = soup.find_all("span", class_=re.compile(r"num|count"))
                for item in info_items[:3]:
                    text = item.get_text(strip=True)
                    nums = re.findall(r'\d+', text.replace(",", ""))
                    if nums:
                        view_count = max(view_count, int(''.join(nums)))

            # 看涨看跌比例 - 从股吧讨论分类
            bullish_ratio = 0.0
            bearish_ratio = 0.0

            # 分析帖子标题中看涨/看跌关键词的比例
            title_tags = soup.find_all("a", class_="title") or soup.find_all("a", href=re.compile(r"read,.*" + code))
            total_titles = 0
            bullish_titles = 0
            bearish_titles = 0

            for a_tag in title_tags:
                title = a_tag.get_text(strip=True).lower()
                if not title:
                    continue
                total_titles += 1
                if any(kw in title for kw in ["涨停", "大涨", "利好", "看涨", "买入", "加仓", "抄底", "吃肉", "起飞"]):
                    bullish_titles += 1
                elif any(kw in title for kw in ["跌停", "大跌", "利空", "看跌", "卖出", "减仓", "跑路", "清仓", "崩盘", "割肉"]):
                    bearish_titles += 1

            if total_titles > 0:
                bullish_ratio = round(bullish_titles / total_titles * 100, 1)
                bearish_ratio = round(bearish_titles / total_titles * 100, 1)

            # 情绪得分: 正值=偏乐观, 负值=偏悲观
            sentiment_score = round(bullish_ratio - bearish_ratio, 1)

            today = datetime.now().strftime("%Y-%m-%d")

            conn = self.db._connect()
            existing = conn.execute(
                "SELECT id FROM guba_sentiment WHERE stock_code = ? AND trade_date = ?",
                (code, today)
            ).fetchone()

            if not existing:
                conn.execute("""
                    INSERT INTO guba_sentiment(
                        stock_code, stock_name, post_count, view_count,
                        bullish_ratio, bearish_ratio, sentiment_score, trade_date
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, name, post_count, view_count,
                      bullish_ratio, bearish_ratio, sentiment_score, today))
                conn.commit()
                conn.close()
                return 1

            conn.close()
            return 0

        except Exception as e:
            logger.warning(f"[股吧情绪] HTML采集异常 ({code}): {e}")
            return 0

    def collect(self) -> Dict[str, int]:
        """
        采集股吧情绪数据
        尝试API → 失败后降级到HTML解析
        Returns:
            Dict[str, int]: {"guba_sentiment": 采集数量}
        """
        self._ensure_table()
        results = {"guba_sentiment": 0}

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("[股吧情绪] 无自选股数据")
            return results

        count = 0
        for stock in stocks:
            code = stock.get("code", "")
            name = stock.get("name", "")

            try:
                # 优先用API
                api_result = self._fetch_by_api(code, name)
                if api_result is not None:
                    count += api_result
                else:
                    # API失败，降级到HTML
                    count += self._fetch_by_html(code, name)

                # 请求间隔
                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"[股吧情绪] {code} 采集异常: {e}")
                continue

        results["guba_sentiment"] = count
        logger.info(f"[股吧情绪] 采集完成，新增 {count} 条")

        return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    from storage.database import Database
    db = Database("data/stock_news.db")
    c = GubaSentimentCollector(db)
    result = c.collect()
    print(f"采集结果: {result}")
