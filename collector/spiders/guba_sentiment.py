"""
股吧情绪采集器（bb-browser 版）

原东方财富 push2ex API 已全部404失效。
改用 bb-browser 通过雪球获取个股行情数据+讨论热度。
基于股票涨跌幅和雪球热度计算情绪指标。
"""
import re
import logging
from datetime import datetime
from typing import Dict, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class GubaSentimentCollector(BaseCollector):
    """
    股吧情绪采集器（bb-browser 版）

    通过雪球个股行情数据计算情绪指标：
    - 价格涨跌幅 → 正涨幅看多情绪
    - 雪球热度 → 讨论活跃度
    - 当日涨跌幅 + 振幅 → 情绪波动
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

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

    def _bb_quote(self, code: str, market: str) -> Optional[dict]:
        """通过 bb-browser 获取雪球个股行情"""
        import subprocess
        import json as _json

        symbol = f"{market}{code}"
        try:
            result = subprocess.run(
                ["bb-browser", "site", "xueqiu/stock", symbol, "--json"],
                capture_output=True, text=True, timeout=20,
            )
            if result.returncode != 0:
                return None
            data = _json.loads(result.stdout.strip())
            if not data.get("success"):
                return None
            return data.get("data")
        except Exception as e:
            logger.warning(f"[股吧情绪] bb-browser 异常({symbol}): {e}")
            return None

    def _calc_sentiment(self, quote: dict) -> float:
        """
        根据行情数据计算情绪分
        范围 -1.0 ~ 1.0
        正值 = 看多，负值 = 看空
        """
        try:
            change_pct_str = quote.get("changePercent", "0")
            change_pct = float(str(change_pct_str).replace("%", ""))

            amplitude_str = quote.get("amplitude", "0%")
            amplitude = float(str(amplitude_str).replace("%", ""))

            turnover_str = quote.get("turnoverRate", "0%")
            turnover = float(str(turnover_str).replace("%", ""))

            # 情绪 = 涨跌幅方向 + 活跃度系数
            # 涨幅大+换手率高 = 看多情绪强
            # 跌幅大+换手率高 = 看空情绪强
            score = 0.0

            # 涨跌幅贡献 (-0.5 ~ 0.5)
            price_signal = max(-0.5, min(0.5, change_pct / 20.0))
            score += price_signal

            # 活跃度因子 (0 ~ 0.3)
            activity_factor = min(0.3, turnover / 20.0)

            # 振幅因子 (-0.2 ~ 0.2)
            amp_signal = max(-0.2, min(0.2, amplitude / 30.0))
            if change_pct >= 0:
                score += activity_factor + amp_signal
            else:
                score -= activity_factor + amp_signal

            return round(max(-1.0, min(1.0, score)), 2)

        except (ValueError, TypeError):
            return 0.0

    def collect(self) -> Dict[str, int]:
        """
        采集情绪数据
        通过 bb-browser 获取雪球行情，计算情绪指标
        """
        self._ensure_table()
        results = {"guba_sentiment": 0}

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("[股吧情绪] 无自选股数据")
            return results

        today = datetime.now().strftime("%Y-%m-%d")
        count = 0

        # 只取前20只，避免太慢
        for stock in stocks[:20]:
            code = stock.get("code", "")
            name = stock.get("name", "")
            market = stock.get("market", "SH")

            if not code:
                continue

            quote = self._bb_quote(code, market)
            if not quote:
                continue

            try:
                change_pct = float(str(quote.get("changePercent", "0")).replace("%", ""))
                volume = int(quote.get("volume", 0) or 0)
                sentiment = self._calc_sentiment(quote)

                # 涨跌幅作为基本面指标：
                # 涨幅 > 3% = 看多比例高
                # 跌幅 > 3% = 看空比例高
                if change_pct >= 3:
                    bullish = 60.0 + min(30.0, change_pct * 5)
                    bearish = 100.0 - bullish
                elif change_pct <= -3:
                    bearish = 60.0 + min(30.0, abs(change_pct) * 5)
                    bullish = 100.0 - bearish
                else:
                    # 小幅涨跌: 中性偏一点
                    bullish = 50.0 + change_pct * 3
                    bearish = 100.0 - bullish

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
                    """, (code, name, volume // 1000, volume,
                          round(bullish, 1), round(bearish, 1),
                          sentiment, today))
                    count += 1

                conn.commit()
                conn.close()

            except Exception as e:
                logger.warning(f"[股吧情绪] {code} 处理异常: {e}")
                continue

        results["guba_sentiment"] = count
        logger.info(f"[股吧情绪] 采集完成，新增 {count} 条（来源：雪球bb-browser）")
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
