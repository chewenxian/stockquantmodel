"""
融资融券数据采集器
数据源：东方财富数据中心

接口：https://push2.eastmoney.com/api/qt/clist/get
参数：pn=1 pz=50 po=1 np=1 fields=f12,f14,f115,f117,f119
      fs=m:0+t:6+f:!2+m:0+t:80+f:!2+m:1+t:2+f:!2+m:1+t:23+f:!2
数据：个股融资余额、融券余额、融资净买入
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict, List

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class MarginTradingCollector(BaseCollector):
    """
    融资融券数据采集器
    采集个股融资余额、融券余额、融资净买入
    数据源：东方财富
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_url = "https://push2.eastmoney.com/api/qt/clist/get"

    def _ensure_table(self):
        """确保 margin_trading 表存在"""
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS margin_trading (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                margin_balance REAL,
                short_balance REAL,
                margin_net_buy REAL,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def collect(self) -> Dict[str, int]:
        """采集融资融券数据"""
        self._ensure_table()
        results = {"margin_trading": 0}

        params = {
            "pn": 1,
            "pz": 50,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:0+t:6+f:!2+m:0+t:80+f:!2+m:1+t:2+f:!2+m:1+t:23+f:!2",
            "fields": "f12,f14,f115,f117,f119",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        }
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        today = datetime.now().strftime("%Y-%m-%d")

        try:
            data = self.get_json(self.api_url, params=params, headers=headers)
            if not data:
                logger.warning("[融资融券] 接口请求返回空")
                return results

            raw_data = data.get("data", {})
            if not raw_data:
                logger.warning("[融资融券] 数据为空")
                return results

            items = raw_data.get("diff", [])
            if not items or not isinstance(items, list):
                logger.warning("[融资融券] 列表为空")
                return results

            count = 0
            conn = self.db._connect()

            for item in items:
                try:
                    stock_code = str(item.get("f12", "") or "")
                    stock_name = str(item.get("f14", "") or "")
                    if not stock_code:
                        continue

                    # 融资余额 (单位:元 -> 亿)
                    margin_balance_raw = item.get("f115", 0) or 0
                    margin_balance = round(float(margin_balance_raw) / 1e8, 4)

                    # 融券余额 (单位:元 -> 亿)
                    short_balance_raw = item.get("f117", 0) or 0
                    short_balance = round(float(short_balance_raw) / 1e8, 4)

                    # 融资净买入 (单位:元 -> 亿)
                    margin_net_buy_raw = item.get("f119", 0) or 0
                    margin_net_buy = round(float(margin_net_buy_raw) / 1e8, 4)

                    # 去重：同日同股不重复插入
                    existing = conn.execute(
                        "SELECT id FROM margin_trading WHERE stock_code = ? AND trade_date = ?",
                        (stock_code, today)
                    ).fetchone()

                    if not existing:
                        conn.execute("""
                            INSERT INTO margin_trading(
                                stock_code, stock_name, margin_balance,
                                short_balance, margin_net_buy, trade_date
                            ) VALUES(?, ?, ?, ?, ?, ?)
                        """, (stock_code, stock_name, margin_balance,
                              short_balance, margin_net_buy, today))
                        conn.commit()
                        count += 1

                except (ValueError, TypeError) as e:
                    logger.warning(f"[融资融券] 解析异常: {e}")
                    continue

            conn.close()
            results["margin_trading"] = count
            logger.info(f"[融资融券] 采集完成，新增 {count} 条")

        except Exception as e:
            logger.error(f"[融资融券] 采集异常: {e}", exc_info=True)
            results["error"] = 0

        return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    from storage.database import Database
    db = Database("data/stock_news.db")
    c = MarginTradingCollector(db)
    result = c.collect()
    print(f"采集结果: {result}")
