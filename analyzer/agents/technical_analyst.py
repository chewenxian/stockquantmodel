"""技术分析师 — MACD/RSI/布林带/KDJ 技术指标分析"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class TechnicalAnalyst(BaseAnalyst):
    """技术分析师：计算并解读技术指标"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "📈 技术分析师"

    def analyze(self, code: str, name: str, **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        if not self.db:
            report.summary = "数据库未连接"
            return report

        # 获取最近180天价格数据
        prices = self._get_prices(code, days=180)
        if not prices or len(prices) < 30:
            report.summary = f"{name} 价格数据不足 (仅有{len(prices) if prices else 0}天)"
            report.confidence = 0.1
            return report

        try:
            from analyzer.technical import TechnicalIndicator
            ti = TechnicalIndicator(self.db)

            close_prices = [p["close"] for p in prices]
            highs = [p["high"] for p in prices]
            lows = [p["low"] for p in prices]

            # 计算各项指标
            latest = close_prices[-1]
            ma5 = ti.calculate_ma(close_prices, 5)
            ma10 = ti.calculate_ma(close_prices, 10)
            ma20 = ti.calculate_ma(close_prices, 20)
            ma60 = ti.calculate_ma(close_prices, 60)
            rsi = ti.calculate_rsi(close_prices)
            bollinger = ti.calculate_bollinger(close_prices)
            kdj_data = ti.calculate_kdj(close_prices, highs, lows)

            # 构建报告
            report.details = {
                "latest_price": latest,
                "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
                "rsi": rsi,
                "bollinger_upper": bollinger.get("upper"),
                "bollinger_mid": bollinger.get("mid"),
                "bollinger_lower": bollinger.get("lower"),
                "kdj_k": kdj_data.get("k"), "kdj_d": kdj_data.get("d"), "kdj_j": kdj_data.get("j"),
            }

            # 趋势判断
            signals = self._get_signals(details=report.details)

            report.summary = f"最新价{latest:.2f}，{self._trend_summary(signals)}"
            report.sentiment = signals["overall_sentiment"]
            report.confidence = min(abs(report.sentiment) * 0.7 + 0.3, 1.0)
            report.key_findings = signals["findings"]
            report.risk_factors = signals["risks"]
            report.opportunities = signals["opportunities"]

        except Exception as e:
            logger.warning(f"[技术分析师] 分析异常: {e}")
            report.summary = f"技术分析异常: {e}"
            report.confidence = 0.1

        return report

    def _get_prices(self, code: str, days: int = 180) -> List[Dict]:
        try:
            conn = self.db._connect()
            since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            rows = conn.execute("""
                SELECT close_price as close, high_price as high,
                       low_price as low, trade_date
                FROM daily_prices
                WHERE stock_code = ? AND trade_date >= ?
                ORDER BY trade_date
            """, (code, since)).fetchall()
            self.db._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"[技术分析师] 获取价格失败: {e}")
            return []

    def _get_signals(self, details: Dict) -> Dict:
        findings, risks, opps = [], [], []
        score = 0

        # 均线判断
        price = details.get("latest_price", 0)
        ma5 = details.get("ma5", 0)
        ma20 = details.get("ma20", 0)
        ma60 = details.get("ma60", 0)

        if ma5 and price > ma5:
            findings.append(f"价格>{ma5:.2f}(5日均线)，短线多头")
            score += 0.15
        if ma20 and price > ma20:
            findings.append(f"价格>{ma20:.2f}(20日均线)，中线偏多")
            score += 0.15
        elif ma20 and price < ma20:
            findings.append(f"价格<{ma20:.2f}(20日均线)，中线偏空")
            score -= 0.15
        if ma60 and price > ma60:
            findings.append(f"价格>{ma60:.2f}(60日均线)，长线多头")

        # RSI判断
        rsi = details.get("rsi")
        if rsi is not None:
            if rsi > 70:
                findings.append(f"RSI={rsi:.1f}，超买区间")
                risks.append("RSI超买，可能回调")
                score -= 0.1
            elif rsi < 30:
                findings.append(f"RSI={rsi:.1f}，超卖区间")
                opps.append("RSI超卖，可能存在反弹机会")
                score += 0.1
            else:
                findings.append(f"RSI={rsi:.1f}，中性区间")

        # KDJ判断
        k = details.get("kdj_k")
        d_val = details.get("kdj_d")
        if k is not None and d_val is not None:
            if k > d_val:
                findings.append("KDJ金叉")
                score += 0.1
            elif k < d_val:
                findings.append("KDJ死叉")
                score -= 0.1

        # 布林带
        upper = details.get("bollinger_upper")
        lower = details.get("bollinger_lower")
        if upper and lower and price:
            if price >= upper:
                findings.append("价格触及布林上轨")
                risks.append("触及上轨，注意压力")
                score -= 0.05
            elif price <= lower:
                findings.append("价格触及布林下轨")
                opps.append("触及下轨，可能有支撑")
                score += 0.05

        sentiment = max(min(score, 1.0), -1.0)
        return {
            "overall_sentiment": sentiment,
            "findings": findings,
            "risks": risks,
            "opportunities": opps,
        }

    def _trend_summary(self, signals: Dict) -> str:
        s = signals["overall_sentiment"]
        if s > 0.3:
            return "技术面看多，均线多头排列"
        elif s > 0.1:
            return "技术面偏多"
        elif s < -0.3:
            return "技术面看空"
        elif s < -0.1:
            return "技术面偏空"
        return "技术面中性，趋势不明朗"
