"""
标准化 Alpha 因子库

因子定义格式:
{
    "name": "因子名称",
    "category": "动量/反转/波动率/价值/质量/情绪/资金流",
    "frequency": "daily/weekly/monthly",
    "formula": "因子计算公式",
    "description": "因子说明",
    "expected_ic": "预期IC值",
    "parameters": {...}
}

所有因子方法返回格式:
{
    "factor_name": "...",
    "value": float | None,
    "signal": "positive/negative/neutral",
    "zscore": float | None,
    "description": "...",
}
"""
import logging
import math
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# 全局标准化参数（可被外部调整）
ZSCOER_GLOBAL_MEAN: Dict[str, float] = {}
ZSCOER_GLOBAL_STD: Dict[str, float] = {}


class FactorLibrary:
    """标准化 Alpha 因子库"""

    def __init__(self, db=None):
        self.db = db

    # ══════════════════════════════════════════
    # 辅助方法
    # ══════════════════════════════════════════

    def _get_price_series(self, code: str, max_points: int = 60) -> List[float]:
        """从数据库获取收盘价序列"""
        prices: List[float] = []
        if not self.db:
            return prices
        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT price FROM market_snapshots
                WHERE stock_code = ?
                  AND price IS NOT NULL
                  AND price > 0
                ORDER BY snapshot_time ASC
                LIMIT ?
            """, (code, max_points)).fetchall()
            prices = [r["price"] for r in rows if r["price"] is not None and r["price"] > 0]
            conn.close()
        except Exception as e:
            logger.warning(f"获取 {code} 价格序列异常: {e}")
        return prices

    def _get_latest_price(self, code: str) -> Optional[float]:
        """获取最新价格"""
        if not self.db:
            return None
        try:
            snap = self.db.get_latest_market_snapshot(code)
            return snap.get("price") if snap else None
        except Exception:
            return None

    def _zscore(self, value: Optional[float], mean: float = 0.0, std: float = 1.0) -> Optional[float]:
        """计算 z-score"""
        if value is None or std <= 0:
            return None
        return round((value - mean) / std, 4)

    def _compute_returns(self, prices: List[float], periods: int = 1) -> Optional[float]:
        """计算 N 期收益率"""
        if not prices or len(prices) < periods + 1:
            return None
        latest = prices[-1]
        prev = prices[-(periods + 1)]
        if prev <= 0:
            return None
        return (latest - prev) / prev

    def _default_result(self, name: str, value: Optional[float] = None,
                        signal: str = "neutral", zscore: Optional[float] = None,
                        description: str = "") -> Dict:
        """创建标准返回结果"""
        return {
            "factor_name": name,
            "value": value,
            "signal": signal,
            "zscore": zscore,
            "description": description,
        }

    # ══════════════════════════════════════════
    # 动量因子
    # ══════════════════════════════════════════

    def momentum_1m(self, code: str) -> Dict:
        """
        一个月动量因子

        计算最近20个交易日的累计收益率。
        动量 > 5% → positive, < -5% → negative

        Returns:
            {"factor_name": "momentum_1m", "value": 0.05, "signal": "positive", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=25)
            ret = self._compute_returns(prices, 20)
            if ret is None:
                return self._default_result("momentum_1m", description="数据不足")

            z = self._zscore(ret, mean=0.0, std=0.08)
            signal = "positive" if ret > 0.05 else ("negative" if ret < -0.05 else "neutral")

            return self._default_result(
                "momentum_1m", value=round(ret, 4), signal=signal, zscore=z,
                description=f"1个月动量: {ret*100:+.2f}%",
            )
        except Exception as e:
            logger.warning(f"momentum_1m 异常 ({code}): {e}")
            return self._default_result("momentum_1m", description=f"计算异常: {e}")

    def momentum_3m(self, code: str) -> Dict:
        """
        三个月动量因子

        计算最近60个交易日的累计收益率。

        Returns:
            {"factor_name": "momentum_3m", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=65)
            ret = self._compute_returns(prices, 60)
            if ret is None:
                return self._default_result("momentum_3m", description="数据不足")

            z = self._zscore(ret, mean=0.0, std=0.15)
            signal = "positive" if ret > 0.10 else ("negative" if ret < -0.10 else "neutral")

            return self._default_result(
                "momentum_3m", value=round(ret, 4), signal=signal, zscore=z,
                description=f"3个月动量: {ret*100:+.2f}%",
            )
        except Exception as e:
            logger.warning(f"momentum_3m 异常 ({code}): {e}")
            return self._default_result("momentum_3m", description=f"计算异常: {e}")

    def momentum_6m(self, code: str) -> Dict:
        """
        六个月动量因子

        计算最近120个交易日的累计收益率。

        Returns:
            {"factor_name": "momentum_6m", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=125)
            ret = self._compute_returns(prices, 120)
            if ret is None:
                return self._default_result("momentum_6m", description="数据不足")

            z = self._zscore(ret, mean=0.0, std=0.25)
            signal = "positive" if ret > 0.15 else ("negative" if ret < -0.15 else "neutral")

            return self._default_result(
                "momentum_6m", value=round(ret, 4), signal=signal, zscore=z,
                description=f"6个月动量: {ret*100:+.2f}%",
            )
        except Exception as e:
            logger.warning(f"momentum_6m 异常 ({code}): {e}")
            return self._default_result("momentum_6m", description=f"计算异常: {e}")

    def reverse_5d(self, code: str) -> Dict:
        """
        5日反转因子

        近5日跌幅过大 → 反弹预期 → positive signal
        近5日涨幅过大 → 回调风险 → negative signal

        反转效应一般在 A 股短期明显。

        Returns:
            {"factor_name": "reverse_5d", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=10)
            ret = self._compute_returns(prices, 5)
            if ret is None:
                return self._default_result("reverse_5d", description="数据不足")

            # 反转信号：跌多了看涨，涨多了看跌
            signal = "positive" if ret < -0.05 else ("negative" if ret > 0.05 else "neutral")
            # z-score 取反（下跌的 5 日收益率应给出正的 z-score）
            z = self._zscore(-ret, mean=0.0, std=0.04)

            return self._default_result(
                "reverse_5d", value=round(ret, 4), signal=signal, zscore=z,
                description=f"5日反转: {ret*100:+.2f}%（{signal}）",
            )
        except Exception as e:
            logger.warning(f"reverse_5d 异常 ({code}): {e}")
            return self._default_result("reverse_5d", description=f"计算异常: {e}")

    def macd_factor(self, code: str) -> Dict:
        """
        MACD 信号因子

        基于收盘价计算 MACD(12,26,9)：
        - DIF = EMA12 - EMA26
        - DEA = EMA9 of DIF
        - MACD柱 = 2 * (DIF - DEA)

        MACD柱向上突破零轴 → 金叉 → positive
        MACD柱向下突破零轴 → 死叉 → negative

        Returns:
            {"factor_name": "macd_factor", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=60)
            if not prices or len(prices) < 35:
                return self._default_result("macd_factor", description="数据不足（需>35天）")

            def ema(data: List[float], period: int) -> List[float]:
                """计算指数移动平均"""
                if not data:
                    return []
                result = [data[0]]
                k = 2.0 / (period + 1)
                for v in data[1:]:
                    result.append(v * k + result[-1] * (1 - k))
                return result

            # 计算 DIF = EMA12 - EMA26
            ema12 = ema(prices, 12)
            ema26 = ema(prices, 26)
            dif = [ema12[i] - ema26[i] for i in range(len(ema26))]

            # 计算 DEA = EMA9 of DIF
            dea = ema(dif, 9)

            # 当前 MACD 柱
            macd_bar = 2 * (dif[-1] - dea[-1])
            prev_macd_bar = 2 * (dif[-2] - dea[-2]) if len(dif) > 1 and len(dea) > 1 else 0

            # 信号判断
            if macd_bar > 0 and prev_macd_bar <= 0:
                signal = "positive"  # 金叉
            elif macd_bar < 0 and prev_macd_bar >= 0:
                signal = "negative"  # 死叉
            elif macd_bar > 0:
                signal = "positive"  # 零轴上方
            else:
                signal = "negative"  # 零轴下方

            z = self._zscore(macd_bar, mean=0.0, std=abs(macd_bar) if macd_bar != 0 else 1.0)

            return self._default_result(
                "macd_factor", value=round(macd_bar, 4), signal=signal, zscore=z,
                description=f"MACD柱: {macd_bar:+.4f}（{'金叉' if signal=='positive' else '死叉' if signal=='negative' else '中性'}）",
            )
        except Exception as e:
            logger.warning(f"macd_factor 异常 ({code}): {e}")
            return self._default_result("macd_factor", description=f"计算异常: {e}")

    # ══════════════════════════════════════════
    # 波动率因子
    # ══════════════════════════════════════════

    def volatility_20d(self, code: str) -> Dict:
        """
        20日波动率因子

        计算20日年化波动率。
        波动率高 → 风险高, 波动率低 → 风险低

        Returns:
            {"factor_name": "volatility_20d", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=25)
            if not prices or len(prices) < 21:
                return self._default_result("volatility_20d", description="数据不足")

            recent = prices[-(21):]
            log_returns = []
            for i in range(1, len(recent)):
                if recent[i - 1] > 0 and recent[i] > 0:
                    log_returns.append(math.log(recent[i] / recent[i - 1]))

            if len(log_returns) < 5:
                return self._default_result("volatility_20d", description="数据不足")

            mean = sum(log_returns) / len(log_returns)
            var = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
            daily_std = math.sqrt(var)
            annual_vol = daily_std * math.sqrt(252)
            vol_val = round(annual_vol, 4)

            signal = "negative" if vol_val > 0.40 else ("positive" if vol_val < 0.20 else "neutral")
            z = self._zscore(vol_val, mean=0.30, std=0.10)

            return self._default_result(
                "volatility_20d", value=vol_val, signal=signal, zscore=z,
                description=f"20日年化波动率: {vol_val*100:.1f}%",
            )
        except Exception as e:
            logger.warning(f"volatility_20d 异常 ({code}): {e}")
            return self._default_result("volatility_20d", description=f"计算异常: {e}")

    def max_drawdown_60d(self, code: str) -> Dict:
        """
        60日最大回撤因子

        计算60个交易日的最大回撤（从峰值下跌的最大幅度）。
        回撤大 → 负面信号（波动大/趋势弱）, 回撤小 → 正面信号

        Returns:
            {"factor_name": "max_drawdown_60d", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=65)
            if not prices or len(prices) < 30:
                return self._default_result("max_drawdown_60d", description="数据不足")

            peak = prices[0]
            max_dd = 0.0
            for p in prices:
                if p > peak:
                    peak = p
                dd = (peak - p) / peak if peak > 0 else 0
                if dd > max_dd:
                    max_dd = dd

            signal = "negative" if max_dd > 0.20 else ("positive" if max_dd < 0.08 else "neutral")
            z = self._zscore(-max_dd, mean=-0.15, std=0.08)

            return self._default_result(
                "max_drawdown_60d", value=round(max_dd, 4), signal=signal, zscore=z,
                description=f"60日最大回撤: {max_dd*100:.1f}%",
            )
        except Exception as e:
            logger.warning(f"max_drawdown_60d 异常 ({code}): {e}")
            return self._default_result("max_drawdown_60d", description=f"计算异常: {e}")

    def skewness_20d(self, code: str) -> Dict:
        """
        20日偏度因子

        收益率分布的偏度。
        正偏 → 右侧尾部更长（上行风险）
        负偏 → 左侧尾部更长（下行风险）

        Returns:
            {"factor_name": "skewness_20d", ...}
        """
        try:
            prices = self._get_price_series(code, max_points=25)
            if not prices or len(prices) < 21:
                return self._default_result("skewness_20d", description="数据不足")

            recent = prices[-(21):]
            returns = []
            for i in range(1, len(recent)):
                if recent[i - 1] > 0:
                    returns.append((recent[i] - recent[i - 1]) / recent[i - 1])

            if len(returns) < 5:
                return self._default_result("skewness_20d", description="数据不足")

            n = len(returns)
            mean_r = sum(returns) / n
            variance = sum((r - mean_r) ** 2 for r in returns) / (n - 1)
            if variance <= 0:
                return self._default_result("skewness_20d", description="无波动")

            std_r = math.sqrt(variance)
            skew = sum(((r - mean_r) / std_r) ** 3 for r in returns) * n / ((n - 1) * (n - 2))

            signal = "positive" if skew > 0.5 else ("negative" if skew < -0.5 else "neutral")
            z = self._zscore(skew, mean=0.0, std=0.5)

            return self._default_result(
                "skewness_20d", value=round(skew, 4), signal=signal, zscore=z,
                description=f"20日偏度: {skew:+.4f}",
            )
        except Exception as e:
            logger.warning(f"skewness_20d 异常 ({code}): {e}")
            return self._default_result("skewness_20d", description=f"计算异常: {e}")

    # ══════════════════════════════════════════
    # 价值因子
    # ══════════════════════════════════════════

    def pe_factor(self, code: str) -> Dict:
        """
        市盈率因子

        从数据库获取最新 PE。
        低 PE → 价值低估（positive）
        高 PE → 估值偏高（negative）
        PE 为负（亏损）→ neutral

        Returns:
            {"factor_name": "pe_factor", ...}
        """
        try:
            if not self.db:
                return self._default_result("pe_factor", description="无数据库连接")

            snap = self.db.get_latest_market_snapshot(code)
            if not snap:
                return self._default_result("pe_factor", description="无行情数据")

            pe = snap.get("pe")
            if pe is None or pe <= 0:
                return self._default_result("pe_factor", value=pe, description="PE缺失或为负")

            signal = "positive" if pe < 15 else ("negative" if pe > 40 else "neutral")
            # PE z-score（相对市场均值15-20）
            z = self._zscore(pe, mean=25.0, std=15.0)

            return self._default_result(
                "pe_factor", value=round(pe, 2), signal=signal, zscore=z,
                description=f"市盈率: {pe:.1f}",
            )
        except Exception as e:
            logger.warning(f"pe_factor 异常 ({code}): {e}")
            return self._default_result("pe_factor", description=f"计算异常: {e}")

    def pb_factor(self, code: str) -> Dict:
        """
        市净率因子

        从数据库获取最新 PB。
        低 PB → 价值低估（positive）
        高 PB → 估值偏高（negative）

        Returns:
            {"factor_name": "pb_factor", ...}
        """
        try:
            if not self.db:
                return self._default_result("pb_factor", description="无数据库连接")

            snap = self.db.get_latest_market_snapshot(code)
            if not snap:
                return self._default_result("pb_factor", description="无行情数据")

            pb = snap.get("pb")
            if pb is None or pb <= 0:
                return self._default_result("pb_factor", value=pb, description="PB缺失")

            signal = "positive" if pb < 1.5 else ("negative" if pb > 5 else "neutral")
            z = self._zscore(pb, mean=3.0, std=2.0)

            return self._default_result(
                "pb_factor", value=round(pb, 2), signal=signal, zscore=z,
                description=f"市净率: {pb:.2f}",
            )
        except Exception as e:
            logger.warning(f"pb_factor 异常 ({code}): {e}")
            return self._default_result("pb_factor", description=f"计算异常: {e}")

    def turnover_factor(self, code: str) -> Dict:
        """
        换手率因子

        从数据库获取最新换手率。
        换手率过高 → 筹码松动（negative）
        换手率过低 → 流动性不足（negative）
        适中换手 → 正常交易（positive）

        Returns:
            {"factor_name": "turnover_factor", ...}
        """
        try:
            if not self.db:
                return self._default_result("turnover_factor", description="无数据库连接")

            snap = self.db.get_latest_market_snapshot(code)
            if not snap:
                return self._default_result("turnover_factor", description="无行情数据")

            tr = snap.get("turnover_rate")
            if tr is None or tr <= 0:
                return self._default_result("turnover_factor", value=tr, description="换手率缺失")

            # 换手率已为百分比（如 2.5 表示 2.5%）
            signal = "neutral"
            if tr > 10:
                signal = "negative"  # 过高
            elif tr < 0.5:
                signal = "negative"  # 过低
            elif 1 < tr < 5:
                signal = "positive"  # 适中

            z = self._zscore(tr, mean=3.0, std=2.5)

            return self._default_result(
                "turnover_factor", value=round(tr, 2), signal=signal, zscore=z,
                description=f"换手率: {tr:.2f}%",
            )
        except Exception as e:
            logger.warning(f"turnover_factor 异常 ({code}): {e}")
            return self._default_result("turnover_factor", description=f"计算异常: {e}")

    # ══════════════════════════════════════════
    # 情绪因子
    # ══════════════════════════════════════════

    def sentiment_factor(self, code: str, days: int = 5) -> Dict:
        """
        新闻情绪因子

        从数据库获取近 N 天的新闻情感分析结果。
        正面新闻多 → positive
        负面新闻多 → negative

        Args:
            code: 股票代码
            days: 统计天数

        Returns:
            {"factor_name": "sentiment_factor", ...}
        """
        try:
            if not self.db:
                return self._default_result("sentiment_factor", description="无数据库连接")

            try:
                news_list = self.db.get_stock_news_sentiment(code, days=days)
            except Exception:
                return self._default_result("sentiment_factor", description="无情感数据")

            if not news_list:
                return self._default_result("sentiment_factor", value=0.0,
                                            description=f"近{days}天无新闻")

            sentiments = [n.get("sentiment", 0) or 0 for n in news_list]
            if not sentiments:
                return self._default_result("sentiment_factor", description="情感评分缺失")

            avg_sentiment = sum(sentiments) / len(sentiments)

            # 信号
            if avg_sentiment > 0.3:
                signal = "positive"
            elif avg_sentiment < -0.3:
                signal = "negative"
            else:
                signal = "neutral"

            z = self._zscore(avg_sentiment, mean=0.0, std=0.3)

            return self._default_result(
                "sentiment_factor", value=round(avg_sentiment, 4), signal=signal, zscore=z,
                description=f"近{days}天新闻情绪: {avg_sentiment:+.4f}（{len(news_list)}篇）",
            )
        except Exception as e:
            logger.warning(f"sentiment_factor 异常 ({code}): {e}")
            return self._default_result("sentiment_factor", description=f"计算异常: {e}")

    def event_factor(self, code: str) -> Dict:
        """
        事件冲击因子

        检测近期是否有异常事件（如公告、涨跌停）。
        重大事件 → signal 调整

        Returns:
            {"factor_name": "event_factor", ...}
        """
        try:
            if not self.db:
                return self._default_result("event_factor", description="无数据库连接")

            # 获取最新行情
            snap = self.db.get_latest_market_snapshot(code)
            if not snap:
                return self._default_result("event_factor", description="无行情数据")

            change_pct = snap.get("change_pct") or 0
            price = snap.get("price") or 0

            # 检测异常涨跌
            event_level = 0
            event_desc = []

            if change_pct > 9.5:
                event_level += 2
                event_desc.append("涨停")
            elif change_pct > 7:
                event_level += 1
                event_desc.append("大涨")
            elif change_pct < -9.5:
                event_level -= 2
                event_desc.append("跌停")
            elif change_pct < -7:
                event_level -= 1
                event_desc.append("大跌")

            # 获取近期公告
            try:
                conn = self.db._connect()
                row = conn.execute("""
                    SELECT title, announce_type FROM announcements
                    WHERE stock_code = ? AND publish_date >= date('now', '-3 days', 'localtime')
                    ORDER BY publish_date DESC LIMIT 3
                """, (code,)).fetchall()
                conn.close()
                for r in row:
                    ann_type = r["announce_type"] or ""
                    if any(k in ann_type for k in ["重组", "资产", "收购", "减持", "增发"]):
                        event_level += 1 if "收购" in ann_type or "重组" in ann_type else -1
                        event_desc.append(r["title"][:20])
            except Exception:
                pass

            signal = "positive" if event_level > 0 else ("negative" if event_level < 0 else "neutral")
            z = self._zscore(event_level, mean=0.0, std=1.5)

            desc = "; ".join(event_desc) if event_desc else "无重大事件"

            return self._default_result(
                "event_factor", value=event_level, signal=signal, zscore=z,
                description=f"事件冲击: {desc}",
            )
        except Exception as e:
            logger.warning(f"event_factor 异常 ({code}): {e}")
            return self._default_result("event_factor", description=f"计算异常: {e}")

    # ══════════════════════════════════════════
    # 批量计算
    # ══════════════════════════════════════════

    def all_factors(self, code: str) -> Dict:
        """
        计算全部因子

        Args:
            code: 股票代码

        Returns:
            {
                "code": "600519",
                "factors": {
                    "momentum_1m": {...},
                    ...
                },
                "summary": {
                    "positive_count": 3,
                    "negative_count": 2,
                    "neutral_count": 4,
                    "composite_signal": "neutral",
                    "composite_score": 0.15,
                }
            }
        """
        result = {"code": code, "factors": {}, "summary": {}}

        try:
            # 计算所有因子
            factor_methods = [
                ("momentum_1m", self.momentum_1m),
                ("momentum_3m", self.momentum_3m),
                ("momentum_6m", self.momentum_6m),
                ("reverse_5d", self.reverse_5d),
                ("macd_factor", self.macd_factor),
                ("volatility_20d", self.volatility_20d),
                ("max_drawdown_60d", self.max_drawdown_60d),
                ("skewness_20d", self.skewness_20d),
                ("pe_factor", self.pe_factor),
                ("pb_factor", self.pb_factor),
                ("turnover_factor", self.turnover_factor),
                ("sentiment_factor", self.sentiment_factor),
                ("event_factor", self.event_factor),
            ]

            for name, method in factor_methods:
                try:
                    result["factors"][name] = method(code)
                except Exception as e:
                    result["factors"][name] = self._default_result(name, description=f"因子异常: {e}")

            # 汇总
            positive = 0
            negative = 0
            neutral = 0
            total_score = 0.0
            weighted_count = 0

            for fname, fdata in result["factors"].items():
                signal = fdata.get("signal", "neutral")
                if signal == "positive":
                    positive += 1
                    total_score += 1.0
                elif signal == "negative":
                    negative += 1
                    total_score -= 1.0
                else:
                    neutral += 1
                weighted_count += 1

            composite_score = total_score / max(weighted_count, 1)

            if composite_score > 0.3:
                composite_signal = "positive"
            elif composite_score < -0.3:
                composite_signal = "negative"
            else:
                composite_signal = "neutral"

            result["summary"] = {
                "positive_count": positive,
                "negative_count": negative,
                "neutral_count": neutral,
                "composite_signal": composite_signal,
                "composite_score": round(composite_score, 4),
            }

        except Exception as e:
            logger.warning(f"all_factors 异常 ({code}): {e}")

        return result

    def factor_report(self, code: str) -> str:
        """
        生成因子报告文本

        Args:
            code: 股票代码

        Returns:
            格式化文本报告
        """
        try:
            data = self.all_factors(code)
            lines = []
            name = code

            # 获取名称
            try:
                from analyzer.ner_extractor import A_SHARE_STOCKS
                name = A_SHARE_STOCKS.get(code, code)
            except Exception:
                pass

            lines.append(f"{'=' * 50}")
            lines.append(f"📊 因子分析报告: {name} ({code})")
            lines.append(f"{'=' * 50}")

            summary = data.get("summary", {})
            cs = summary.get("composite_signal", "neutral")
            cs_emoji = "🟢" if cs == "positive" else ("🔴" if cs == "negative" else "🟡")
            lines.append(f"\n综合信号: {cs_emoji} {cs.upper()}")
            lines.append(f"综合得分: {summary.get('composite_score', 0):+.4f}")
            lines.append(f"积极因子: {summary.get('positive_count', 0)} 个")
            lines.append(f"消极因子: {summary.get('negative_count', 0)} 个")
            lines.append(f"中性因子: {summary.get('neutral_count', 0)} 个")

            lines.append(f"\n{'─' * 40}")

            # 分组显示
            categories = [
                ("动量 & 反转", ["momentum_1m", "momentum_3m", "momentum_6m", "reverse_5d", "macd_factor"]),
                ("波动率", ["volatility_20d", "max_drawdown_60d", "skewness_20d"]),
                ("价值", ["pe_factor", "pb_factor", "turnover_factor"]),
                ("情绪", ["sentiment_factor", "event_factor"]),
            ]

            for cat_name, factor_names in categories:
                lines.append(f"\n  [{cat_name}]")
                for fname in factor_names:
                    fdata = data.get("factors", {}).get(fname)
                    if fdata:
                        sig = fdata.get("signal", "?")
                        sig_emoji = "🟢" if sig == "positive" else ("🔴" if sig == "negative" else "⚪")
                        desc = fdata.get("description", "")
                        lines.append(f"    {sig_emoji} {fname}: {desc}")
                    else:
                        lines.append(f"    ⚪ {fname}: 未计算")

            lines.append(f"\n{'=' * 50}")

            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"factor_report 异常 ({code}): {e}")
            return f"因子报告生成失败: {e}"


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    fl = FactorLibrary()

    # 测试全部因子（无数据库时返回默认值）
    print("===== 因子测试 (无数据库) =====")
    result = fl.all_factors("600519")
    print(f"综合信号: {result['summary']['composite_signal']}")
    print(f"因子数: {len(result['factors'])}")

    report = fl.factor_report("600519")
    print(f"\n{report}")
