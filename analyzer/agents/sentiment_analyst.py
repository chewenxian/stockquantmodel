"""情绪分析师 — 股吧情绪、市场情绪分析"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class SentimentAnalyst(BaseAnalyst):
    """情绪分析师：分析股吧/社交媒体情绪"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "💬 情绪分析师"

    def analyze(self, code: str, name: str, **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        # 1. 股吧情绪数据
        guba_sentiment = self._get_guba_sentiment(code)
        if guba_sentiment is not None:
            report.details["guba_sentiment"] = guba_sentiment
            report.key_findings.append(f"股吧情绪值: {guba_sentiment:.2f}")
            if guba_sentiment > 0.2:
                report.key_findings.append("股吧情绪偏乐观")
                report.sentiment += 0.15
            elif guba_sentiment < -0.2:
                report.key_findings.append("股吧情绪偏悲观")
                report.sentiment -= 0.15
            report.details["guba_sentiment"] = guba_sentiment
        else:
            report.key_findings.append("股吧情绪数据暂缺")

        # 2. 涨跌幅 + 换手等市场情绪
        market_sentiment = self._get_market_sentiment(code)
        if market_sentiment is not None:
            report.details["market_sentiment"] = market_sentiment
            change = market_sentiment.get("change_pct", 0)
            if change > 3:
                report.key_findings.append(f"近5日涨幅{change:+.1f}%，市场情绪高涨")
                report.sentiment += 0.2
            elif change < -3:
                report.key_findings.append(f"近5日跌幅{change:+.1f}%，市场情绪低落")
                report.sentiment -= 0.2
            else:
                report.key_findings.append(f"近5日涨跌幅{change:+.1f}%，市场平稳")

        # 3. 情绪综合
        report.sentiment = max(min(report.sentiment, 1.0), -1.0)
        report.confidence = min(abs(report.sentiment) * 0.5 + 0.3, 1.0)
        report.summary = self._gen_summary(report)

        return report

    def _get_guba_sentiment(self, code: str) -> float:
        """获取股吧情绪"""
        try:
            conn = self.db._connect()
            row = conn.execute(
                "SELECT sentiment_score FROM guba_sentiment WHERE stock_code = ? ORDER BY collected_at DESC LIMIT 1",
                (code,)
            ).fetchone()
            self.db._close(conn)
            return float(row[0]) if row else None
        except Exception:
            return None

    def _get_market_sentiment(self, code: str) -> Dict:
        """获取5日涨跌幅作为市场情绪参考"""
        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT close_price, change_pct, trade_date
                FROM daily_prices
                WHERE stock_code = ?
                ORDER BY trade_date DESC LIMIT 5
            """, (code,)).fetchall()
            self.db._close(conn)
            if rows and len(rows) >= 2:
                changes = [r["change_pct"] or 0 for r in rows]
                return {
                    "change_pct": sum(changes),
                    "latest_close": rows[0]["close_price"],
                    "latest_change": rows[0]["change_pct"],
                }
            return None
        except Exception:
            return None

    def _gen_summary(self, report: AnalystReport) -> str:
        s = report.sentiment
        if s > 0.3:
            return "市场情绪整体偏乐观，股吧看多情绪较强"
        elif s > 0.1:
            return "市场情绪略偏多"
        elif s < -0.3:
            return "市场情绪整体偏悲观"
        elif s < -0.1:
            return "市场情绪略偏空"
        return "市场情绪中性，无明显倾向"
