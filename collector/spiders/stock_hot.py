"""
股票热度排行采集器
数据源：东方财富人气榜

接口：https://push2ex.eastmoney.com/getStockHotRank
参数：page=1 pagesize=30
数据：当日热门股票排行（基于搜索/浏览量）
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class StockHotCollector(BaseCollector):
    """
    股票热度排行采集器
    采集当日热门股票排行（基于搜索/浏览量）

    数据源：东方财富人气榜
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_url = "https://push2ex.eastmoney.com/getStockHotRank"

    def _ensure_table(self):
        """确保 stock_hot 表存在"""
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_hot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                hot_rank INTEGER,
                hot_score REAL,
                change_pct REAL,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def collect(self) -> Dict[str, int]:
        """采集股票热度排行数据"""
        self._ensure_table()
        results = {"stock_hot": 0}

        params = {
            "page": 1,
            "pagesize": 30,
        }
        headers = {
            "Referer": "https://guba.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        today = datetime.now().strftime("%Y-%m-%d")

        try:
            data = self.get_json(self.api_url, params=params, headers=headers)
            if not data:
                logger.warning("[股票热度] 接口请求返回空")
                return results

            raw_data = data.get("data", {})
            if not raw_data:
                logger.warning("[股票热度] 数据为空")
                return results

            items = raw_data.get("list", [])
            if not items or not isinstance(items, list):
                logger.warning("[股票热度] 列表为空")
                return results

            count = 0
            conn = self.db._connect()

            for item in items:
                try:
                    stock_code = str(item.get("sc", "") or item.get("code", "") or "")
                    stock_name = str(item.get("n", "") or item.get("name", "") or "")
                    if not stock_code:
                        continue

                    # 热度排名
                    rank = item.get("rk", 0) or 0
                    try:
                        rank = int(rank)
                    except (ValueError, TypeError):
                        rank = 0

                    # 热度值
                    hot_score = item.get("hs", 0) or 0
                    try:
                        hot_score = float(hot_score)
                    except (ValueError, TypeError):
                        hot_score = 0.0

                    # 涨跌幅
                    change_pct = item.get("pct", 0) or 0
                    try:
                        change_pct = float(change_pct)
                    except (ValueError, TypeError):
                        change_pct = 0.0

                    # 去重：同日同股不重复插入
                    existing = conn.execute(
                        "SELECT id FROM stock_hot WHERE stock_code = ? AND trade_date = ?",
                        (stock_code, today)
                    ).fetchone()

                    if not existing:
                        conn.execute("""
                            INSERT INTO stock_hot(
                                stock_code, stock_name, hot_rank,
                                hot_score, change_pct, trade_date
                            ) VALUES(?, ?, ?, ?, ?, ?)
                        """, (stock_code, stock_name, rank,
                              hot_score, change_pct, today))
                        conn.commit()
                        count += 1

                except (ValueError, TypeError) as e:
                    logger.warning(f"[股票热度] 解析异常: {e}")
                    continue

            conn.close()
            results["stock_hot"] = count
            logger.info(f"[股票热度] 采集完成，新增 {count} 条")

        except Exception as e:
            logger.error(f"[股票热度] 采集异常: {e}", exc_info=True)
            results["error"] = 0

        return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    from storage.database import Database
    db = Database("data/stock_news.db")
    c = StockHotCollector(db)
    result = c.collect()
    print(f"采集结果: {result}")
