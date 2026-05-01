"""
北向资金流向采集器
数据源：东方财富数据API

接口：https://push2.eastmoney.com/api/qt/kamt.kline/get
参数: klt=101, lmt=10, secid=1
数据：沪股通净流入、深股通净流入、合计净流入
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class NorthFlowCollector(BaseCollector):
    """
    北向资金流向采集器
    采集中沪深港通北向资金的净流入/流出数据
    数据源：东方财富
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_url = "https://push2.eastmoney.com/api/qt/kamt.kline/get"

    def _ensure_table(self):
        """确保 north_flow 表存在"""
        conn = self.db._connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS north_flow (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date DATE,
                    sh_net REAL,
                    sz_net REAL,
                    total_net REAL,
                    cumulative_net REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        finally:
            self.db._close(conn)

    def _parse_kline_value(self, val_str: str) -> float:
        """解析K线数值，处理各种格式"""
        if not val_str or val_str == "-":
            return 0.0
        try:
            val_str = val_str.replace(",", "").replace("亿", "").strip()
            return float(val_str)
        except (ValueError, TypeError):
            return 0.0

    def collect(self) -> Dict[str, int]:
        """采集北向资金流向数据"""
        self._ensure_table()
        results = {"north_flow": 0}

        params = {
            "klt": "101",
            "lmt": "10",
            "secid": "1",
            "fields1": "f1,f2,f3",
            "fields2": "f51,f52,f53,f54,f55",
        }
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        try:
            data = self.get_json(self.api_url, params=params, headers=headers)
            if not data:
                logger.warning("[北向资金] 接口请求返回空")
                return results

            raw_data = data.get("data", {})
            if not raw_data:
                logger.warning("[北向资金] 数据为空")
                return results

            klines = raw_data.get("klines", [])
            if not klines or not isinstance(klines, list):
                logger.warning(f"[北向资金] K线列表为空或格式异常")
                return results

            count = 0
            conn = self.db._connect()
            try:
                for line in klines:
                    try:
                        items = line.split(",")
                        if len(items) < 5:
                            continue

                        trade_date_str = items[0].strip()
                        if "-" not in trade_date_str and len(trade_date_str) == 8:
                            trade_date = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:8]}"
                        else:
                            trade_date = trade_date_str

                        sh_net = self._parse_kline_value(items[1])
                        sz_net = self._parse_kline_value(items[2])
                        total_net = self._parse_kline_value(items[3])
                        cumulative_net = self._parse_kline_value(items[4])

                        existing = conn.execute(
                            "SELECT id FROM north_flow WHERE trade_date = ?",
                            (trade_date,)
                        ).fetchone()

                        if not existing:
                            conn.execute("""
                                INSERT INTO north_flow(trade_date, sh_net, sz_net, total_net, cumulative_net)
                                VALUES(?, ?, ?, ?, ?)
                            """, (trade_date, sh_net, sz_net, total_net, cumulative_net))
                            count += 1

                    except (ValueError, IndexError) as e:
                        logger.warning(f"[北向资金] K线解析异常: {line[:50]}... {e}")
                        continue
                conn.commit()
            finally:
                self.db._close(conn)

            results["north_flow"] = count
            logger.info(f"[北向资金] 采集完成，新增 {count} 条")

        except Exception as e:
            logger.error(f"[北向资金] 采集异常: {e}", exc_info=True)
            results["error"] = 0

        return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    from storage.database import Database
    db = Database("data/stock_news.db")
    c = NorthFlowCollector(db)
    result = c.collect()
    print(f"采集结果: {result}")
