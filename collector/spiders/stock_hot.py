"""
股票热度排行采集器
数据源：雪球热门股票榜（经 bb-browser 浏览器采集）

原东方财富 push2ex 接口（getStockHotRank）已404失效。
改用 bb-browser 调用雪球热门股票榜。
"""
import re
import logging
from datetime import datetime
from typing import Dict, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class StockHotCollector(BaseCollector):
    """
    股票热度排行采集器（bb-browser 版）

    通过 bb-browser 浏览器采集雪球热门股票榜单。
    雪球热门股票基于用户的讨论热度、搜索量和关注度计算。
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

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

    def _bb_hot_stocks(self, top_n: int = 30) -> Optional[list]:
        """通过 bb-browser 获取雪球热门股票"""
        import subprocess
        import json as _json

        try:
            result = subprocess.run(
                ["bb-browser", "site", "xueqiu/hot-stock", str(top_n), "--json"],
                capture_output=True, text=True, timeout=25,
            )
            if result.returncode != 0:
                logger.warning(f"[股票热度] bb-browser 失败: {result.stderr[:100]}")
                return None

            data = _json.loads(result.stdout.strip())
            if not data.get("success"):
                return None
            return data.get("data", {}).get("items", [])
        except Exception as e:
            logger.warning(f"[股票热度] bb-browser 异常: {e}")
            return None

    def collect(self) -> Dict[str, int]:
        """采集雪球热门股票排行"""
        self._ensure_table()
        results = {"stock_hot": 0}
        today = datetime.now().strftime("%Y-%m-%d")

        items = self._bb_hot_stocks(30)
        if not items:
            logger.warning("[股票热度] 雪球热榜返回为空")
            return results

        count = 0
        conn = self.db._connect()

        for item in items:
            try:
                symbol = item.get("symbol", "")
                name = item.get("name", "")
                if not symbol or not name:
                    continue

                # 提取纯代码
                code = symbol
                for prefix in ["SH", "SZ"]:
                    if code.startswith(prefix):
                        code = code[len(prefix):]
                        break

                rank = int(item.get("rank", 0))
                heat = int(item.get("heat", 0))
                change_pct = float(str(item.get("changePercent", "0")).replace("%", ""))

                # 去重
                existing = conn.execute(
                    "SELECT id FROM stock_hot WHERE stock_code = ? AND trade_date = ?",
                    (code, today)
                ).fetchone()

                if not existing:
                    conn.execute("""
                        INSERT INTO stock_hot(
                            stock_code, stock_name, hot_rank,
                            hot_score, change_pct, trade_date
                        ) VALUES(?, ?, ?, ?, ?, ?)
                    """, (code, name, rank, float(heat), change_pct, today))
                    count += 1

            except (ValueError, TypeError) as e:
                logger.warning(f"[股票热度] 解析异常: {e}")
                continue

        conn.commit()
        conn.close()
        results["stock_hot"] = count
        logger.info(f"[股票热度] 采集完成，新增 {count} 条（来源：雪球bb-browser）")
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
