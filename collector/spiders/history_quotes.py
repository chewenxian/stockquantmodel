"""
历史K线数据采集器
采集A股历史日K线数据（东方财富API）

API: https://push2his.eastmoney.com/api/qt/stock/kline/get
参数:
  - secid: "1.600519"（1=SH, 0=SZ）
  - fields1: "f1,f2,f3"
  - fields2: "f51,f52,f53,f54,f55,f56,f57"
  - klt: 101（日K）
  - fqt: 1（前复权）
  - end: "20500101"
  - lmt: 500（最多500条）
"""
import time
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class HistoryQuotesCollector(BaseCollector):
    """采集A股历史日K线数据（东方财富API）"""

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.kline_api = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        # 默认参数
        self.default_params = {
            "fields1": "f1,f2,f3",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
            "klt": "101",        # 日K
            "fqt": "1",          # 前复权
            "end": "20500101",
        }

    def _get_secid(self, code: str, market: str = None) -> tuple:
        """根据股票代码获取 secid (market_code, code)"""
        code_str = str(code).strip()
        
        # 市场代码转换
        mkt_map = {"SH": "1", "SZ": "0", "BJ": "0"}
        if market and market.upper() in mkt_map:
            return mkt_map[market.upper()], code_str
        
        # 自动判断
        if market:
            return market, code_str
        if code_str.startswith("6") or code_str.startswith("9"):
            return "1", code_str
        elif code_str.startswith("0") or code_str.startswith("3") or code_str.startswith("2"):
            return "0", code_str
        elif code_str.startswith("4"):
            return "0", code_str
        else:
            return "1", code_str

    def collect_stock(self, code: str, market: str = None, limit: int = 500) -> int:
        """
        采集单只股票的历史日K线数据

        Args:
            code: 股票代码
            market: 市场代码 ("1"=SH, "0"=SZ)，自动判断
            limit: 最多采集多少条（默认500）

        Returns:
            采集到的K线条数
        """
        mkt, scode = self._get_secid(code, market)
        secid = f"{mkt}.{scode}"

        params = dict(self.default_params)
        params["secid"] = secid
        params["lmt"] = str(limit)

        try:
            resp_json = self.get_json(self.kline_api, params=params)
            if not resp_json:
                logger.warning(f"[历史K线] {code} 请求返回空")
                return 0

            data = resp_json.get("data", {})
            if not data:
                logger.warning(f"[历史K线] {code} 无数据")
                return 0

            klines = data.get("klines", [])
            if not klines:
                logger.warning(f"[历史K线] {code} K线列表为空")
                return 0

            count = 0
            for line in klines:
                try:
                    items = line.split(",")
                    if len(items) < 7:
                        continue

                    trade_date = items[0].strip()
                    open_price = float(items[1]) if items[1] else None
                    close_price = float(items[2]) if items[2] else None
                    high_price = float(items[3]) if items[3] else None
                    low_price = float(items[4]) if items[4] else None
                    volume = float(items[5]) if items[5] else None
                    amount = float(items[6]) if items[6] else None

                    # 计算涨跌幅
                    change_pct = None
                    if open_price and close_price and open_price > 0:
                        change_pct = round((close_price - open_price) / open_price * 100, 2)

                    if self.db.upsert_daily_price(
                        stock_code=scode,
                        trade_date=trade_date,
                        open_price=open_price,
                        close_price=close_price,
                        high_price=high_price,
                        low_price=low_price,
                        volume=volume,
                        amount=amount,
                        change_pct=change_pct,
                    ):
                        count += 1

                except (ValueError, IndexError) as e:
                    logger.warning(f"[历史K线] {code} 解析K线行失败: {line[:30]}... {e}")
                    continue

            logger.info(f"[历史K线] {code} 采集完成，共 {count}/{len(klines)} 条")
            return count

        except Exception as e:
            logger.error(f"[历史K线] {code} 采集异常: {e}")
            return 0

    def collect_all_stocks(self, stocks: List[Dict], limit: int = 500) -> Dict[str, int]:
        """
        遍历所有股票采集历史K线

        Args:
            stocks: 股票列表，每项为 {"code": ..., "name": ..., "market": ...}
            limit: 每只股票采集条数

        Returns:
            {"success": 成功数, "total": 总数, "kline_count": K线条数}
        """
        result = {"success": 0, "failed": 0, "total": len(stocks), "kline_count": 0}

        for i, stock in enumerate(stocks):
            code = stock.get("code", "")
            name = stock.get("name", "")
            market = stock.get("market")

            logger.info(f"[历史K线] 正在采集 [{i+1}/{len(stocks)}] {name}({code})...")

            try:
                count = self.collect_stock(code, market=market, limit=limit)
                if count > 0:
                    result["success"] += 1
                    result["kline_count"] += count
                else:
                    result["failed"] += 1
            except Exception as e:
                logger.error(f"[历史K线] {code} 采集异常: {e}")
                result["failed"] += 1

            # 请求间隔，防封
            time.sleep(0.5)

        logger.info(f"[历史K线] 全部采集完成: 成功{result['success']}只, "
                    f"失败{result['failed']}只, 共{result['kline_count']}条K线")
        return result

    def collect(self) -> Dict[str, int]:
        """
        兼容调度器接口，采集所有自选股历史数据

        Returns:
            {"history_kline": 采集到的K线条数}
        """
        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("[历史K线] 无自选股数据，请先初始化股票池")
            return {"history_kline": 0, "error": "股票池为空"}

        result = self.collect_all_stocks(stocks, limit=500)
        return {"history_kline": result["kline_count"],
                "stocks_success": result["success"],
                "stocks_failed": result["failed"]}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )

    from storage.database import Database
    db = Database("data/stock_news.db")
    collector = HistoryQuotesCollector(db)

    # 测试单只股票
    count = collector.collect_stock("600519", limit=10)
    print(f"采集到 {count} 条K线")

    # 验证
    prices = db.get_price_history("600519", days=5)
    print(f"\n最近5条K线:")
    for p in prices:
        print(f"  {p['trade_date']} O:{p['open_price']} C:{p['close_price']} "
              f"H:{p['high_price']} L:{p['low_price']} V:{p['volume']}")
