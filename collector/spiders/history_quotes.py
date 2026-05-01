"""
历史K线数据采集器
多源降级策略：新浪 → Baostock → 腾讯 → 东方财富
"""
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class HistoryQuotesCollector(BaseCollector):
    """采集A股历史日K线数据（多源降级）"""

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.sina_api = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        self.tx_api = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"

    def _prefix(self, code: str) -> str:
        return "sh" if code.startswith("6") else "sz"

    def collect(self) -> Dict[str, int]:
        """采集所有自选股历史K线"""
        stocks = self.db.load_stocks()
        result = self.collect_all_stocks(stocks, limit=500)
        return {"kline_count": result.get("kline_count", 0)}

    def collect_stock(self, code: str, market: str = None, limit: int = 500) -> int:
        """多源降级采集单只股票历史K线"""
        scode = str(code).strip()
        prefix = self._prefix(scode)

        # 1️⃣ 新浪
        c = self._sina(scode, prefix, limit)
        if c > 0: return c

        # 2️⃣ Baostock
        c = self._baostock(scode, prefix)
        if c > 0: return c

        # 3️⃣ 腾讯
        c = self._tencent(scode, prefix, limit)
        if c > 0: return c

        logger.warning(f"[历史K线] {code} 所有数据源均失败")
        return 0

    def _sina(self, code: str, prefix: str, limit: int) -> int:
        try:
            data = self.get_json(self.sina_api,
                {"symbol": f"{prefix}{code}", "datalen": limit})
            if isinstance(data, list) and len(data) > 0:
                count = 0
                for item in data:
                    d = item.get("date", "")
                    o = float(item.get("open", 0)) if item.get("open") else None
                    c = float(item.get("close", 0)) if item.get("close") else None
                    h = float(item.get("high", 0)) if item.get("high") else None
                    l = float(item.get("low", 0)) if item.get("low") else None
                    v = float(item.get("volume", 0)) if item.get("volume") else None
                    chg = round((c - o) / o * 100, 2) if o and c and o > 0 else None
                    if self.db.upsert_daily_price(code, d, o, c, h, l, v, None, chg):
                        count += 1
                logger.info(f"[历史K线/新浪] {code} {count}条")
                return count
        except Exception as e:
            logger.warning(f"[历史K线/新浪] {code} 失败: {e}")
        return 0

    def _baostock(self, code: str, prefix: str) -> int:
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != "0":
                return 0
            rs = bs.query_history_k_data_plus(f"{prefix}.{code}",
                "date,open,close,high,low,volume",
                start_date="2020-01-01", end_date="2026-12-31")
            count = 0
            while rs.next():
                row = rs.get_row_data()
                if not row or not row[0]:
                    continue
                o = float(row[1]) if row[1] else None
                c = float(row[2]) if row[2] else None
                h = float(row[3]) if row[3] else None
                l = float(row[4]) if row[4] else None
                v = float(row[5]) if row[5] else None
                chg = round((c - o) / o * 100, 2) if o and c and o > 0 else None
                if self.db.upsert_daily_price(code, row[0], o, c, h, l, v, None, chg):
                    count += 1
            bs.logout()
            logger.info(f"[历史K线/Baostock] {code} {count}条")
            return count
        except Exception as e:
            logger.warning(f"[历史K线/Baostock] {code} 失败: {e}")
        return 0

    def _tencent(self, code: str, prefix: str, limit: int) -> int:
        try:
            data = self.get_json(self.tx_api,
                {"param": f"{prefix}{code},m/day,,{limit}"})
            if not data:
                return 0
            days = data.get("data", {}).get(code, {}).get("day") or \
                   data.get("data", {}).get(f"{prefix}{code}", {}).get("day", [])
            if not isinstance(days, list) or len(days) == 0:
                return 0
            count = 0
            for item in days:
                if not isinstance(item, list) or len(item) < 6:
                    continue
                d = str(item[0])[:10]
                o = float(item[1]) if item[1] else None
                c = float(item[2]) if item[2] else None
                h = float(item[3]) if item[3] else None
                l = float(item[4]) if item[4] else None
                v = float(item[5]) if item[5] else None
                if self.db.upsert_daily_price(code, d, o, c, h, l, v, None, None):
                    count += 1
            logger.info(f"[历史K线/腾讯] {code} {count}条")
            return count
        except Exception as e:
            logger.warning(f"[历史K线/腾讯] {code} 失败: {e}")
        return 0

    def collect_all_stocks(self, stocks: List[Dict], limit: int = 500) -> Dict:
        """遍历所有股票采集历史K线"""
        success = 0
        total_kline = 0
        for i, stock in enumerate(stocks):
            code = stock.get("code", "")
            name = stock.get("name", "")
            logger.info(f"[历史K线] [{i+1}/{len(stocks)}] {name}({code})...")
            count = self.collect_stock(code, limit=limit)
            if count > 0:
                success += 1
                total_kline += count
            time.sleep(0.3)  # 防限流
        return {"success": success, "total": len(stocks), "kline_count": total_kline}
