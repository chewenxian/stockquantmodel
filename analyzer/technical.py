"""
技术指标计算模块
提供：RSI、均线、布林带等常用技术指标计算

数据来源：storage.database.Database → market_snapshots 表
所有指标纯 Python 实现，无外部依赖
"""
import logging
import math
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TechnicalIndicator:
    """股票技术指标计算器"""

    def __init__(self, db=None):
        self.db = db

    # ──────────────────────────────────────────
    # RSI 计算
    # ──────────────────────────────────────────

    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """
        计算 RSI (相对强弱指标)

        RSI = 100 - (100 / (1 + RS))
        RS = 平均上涨幅度 / 平均下跌幅度

        Args:
            prices: 价格序列（从旧到新）
            period: RSI 周期（默认14）

        Returns:
            RSI 值 (0~100)，数据不足时返回 None
        """
        try:
            if len(prices) < period + 1:
                return None

            # 计算每日涨跌
            changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            # 只取最近 period 个涨跌值
            recent_changes = changes[-period:]

            avg_gain = sum(c for c in recent_changes if c > 0) / period
            avg_loss = sum(abs(c) for c in recent_changes if c < 0) / period

            if avg_loss == 0:
                return 100.0  # 持续上涨

            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

            return round(rsi, 2)

        except Exception as e:
            logger.error(f"RSI 计算异常: {e}")
            return None

    def calculate_rsi_smoothed(self, prices: List[float], period: int = 14) -> Optional[float]:
        """
        平滑 RSI 计算（Wilder 平滑法）
        更常用的 RSI 实现方式

        Args:
            prices: 价格序列（从旧到新）
            period: RSI 周期（默认14）

        Returns:
            RSI 值 (0~100)
        """
        try:
            if len(prices) < period + 1:
                return None

            # 初始 SMA
            gains = []
            losses = []
            for i in range(1, period + 1):
                change = prices[i] - prices[i-1]
                gains.append(max(change, 0))
                losses.append(max(-change, 0))

            avg_gain = sum(gains) / period
            avg_loss = sum(losses) / period

            # Wilder 平滑
            for i in range(period + 1, len(prices)):
                change = prices[i] - prices[i-1]
                gain = max(change, 0)
                loss = max(-change, 0)

                avg_gain = (avg_gain * (period - 1) + gain) / period
                avg_loss = (avg_loss * (period - 1) + loss) / period

            if avg_loss == 0:
                return 100.0

            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

            return round(rsi, 2)

        except Exception as e:
            logger.error(f"平滑RSI计算异常: {e}")
            return None

    # ──────────────────────────────────────────
    # 移动均线
    # ──────────────────────────────────────────

    def calculate_ma(self, prices: List[float], period: int) -> Optional[float]:
        """
        计算移动均线（简单移动平均 SMA）

        Args:
            prices: 价格序列（从旧到新）
            period: 均线周期

        Returns:
            SMA 值
        """
        try:
            if len(prices) < period:
                return None

            recent = prices[-period:]
            ma = sum(recent) / period

            return round(ma, 2)

        except Exception as e:
            logger.error(f"均线计算异常: {e}")
            return None

    def calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        """
        计算指数移动均线（EMA）

        EMA = (价格 - 前一日EMA) * 平滑系数 + 前一日EMA
        平滑系数 = 2 / (周期 + 1)

        Args:
            prices: 价格序列（从旧到新）
            period: 均线周期

        Returns:
            最新的 EMA 值
        """
        try:
            if len(prices) < period:
                return None

            multiplier = 2.0 / (period + 1)

            # 用 SMA 作为初始 EMA
            ema = sum(prices[:period]) / period

            # 迭代计算后续 EMA
            for price in prices[period:]:
                ema = (price - ema) * multiplier + ema

            return round(ema, 2)

        except Exception as e:
            logger.error(f"EMA计算异常: {e}")
            return None

    def calculate_all_ma(self, prices: List[float]) -> Dict[str, Optional[float]]:
        """
        计算常用多周期均线

        Returns:
            {"ma5": ..., "ma10": ..., "ma20": ..., "ma30": ..., "ma60": ...}
        """
        periods = [5, 10, 20, 30, 60]
        result = {}
        for p in periods:
            try:
                result[f"ma{p}"] = self.calculate_ma(prices, p)
            except Exception:
                result[f"ma{p}"] = None
        return result

    # ──────────────────────────────────────────
    # 布林带
    # ──────────────────────────────────────────

    def calculate_bollinger(self, prices: List[float], period: int = 20) -> Dict[str, Any]:
        """
        计算布林带

        - 中轨 = SMA(period)
        - 上轨 = 中轨 + k * 标准差
        - 下轨 = 中轨 - k * 标准差
        - 带宽 = (上轨 - 下轨) / 中轨 * 100

        Args:
            prices: 价格序列（从旧到新）
            period: 布林带周期（默认20）

        Returns:
            {
                "upper": 上轨,
                "middle": 中轨,
                "lower": 下轨,
                "bandwidth": 带宽百分比,
                "position": 最新价格在布林带中的位置 (0~1),
            }
            数据不足时各字段为 None
        """
        default = {"upper": None, "middle": None, "lower": None,
                   "bandwidth": None, "position": None}

        try:
            if len(prices) < period:
                return default

            recent = prices[-period:]
            sma = sum(recent) / period

            # 计算标准差（总体标准差）
            variance = sum((p - sma) ** 2 for p in recent) / period
            std_dev = math.sqrt(variance)

            k = 2.0  # 标准倍数
            upper = sma + k * std_dev
            lower = sma - k * std_dev
            bandwidth = (upper - lower) / sma * 100 if sma != 0 else 0.0

            # 最新价格在布林带中的相对位置 (0~1)
            current_price = prices[-1]
            if upper != lower:
                position = (current_price - lower) / (upper - lower)
            else:
                position = 0.5

            return {
                "upper": round(upper, 2),
                "middle": round(sma, 2),
                "lower": round(lower, 2),
                "bandwidth": round(bandwidth, 2),
                "position": round(position, 4),
            }

        except Exception as e:
            logger.error(f"布林带计算异常: {e}")
            return default

    # ──────────────────────────────────────────
    # 辅助指标
    # ──────────────────────────────────────────

    # ──────────────────────────────────────────
    # KDJ 随机指标
    # ──────────────────────────────────────────

    def calculate_kdj(self, prices: List[float], highs: List[float], lows: List[float],
                      period: int = 9) -> Dict[str, Any]:
        """
        计算 KDJ 随机指标

        K值 = 2/3 * 前一日K值 + 1/3 * RSV
        D值 = 2/3 * 前一日D值 + 1/3 * K值
        J值 = 3 * K值 - 2 * D值

        Args:
            prices: 收盘价序列（从旧到新）
            highs: 最高价序列
            lows: 最低价序列
            period: 周期（默认9）

        Returns:
            {"k": val, "d": val, "j": val, "signal": "超买"/"超卖"/"正常"}
        """
        default = {"k": None, "d": None, "j": None, "signal": "未知"}
        try:
            n = len(prices)
            if n < period + 1 or len(highs) < n or len(lows) < n:
                return default

            # 计算 RSV 序列
            rsv_list = []
            for i in range(period - 1, n):
                sub_high = max(highs[i - period + 1:i + 1])
                sub_low = min(lows[i - period + 1:i + 1])
                if sub_high != sub_low:
                    rsv = (prices[i] - sub_low) / (sub_high - sub_low) * 100
                else:
                    rsv = 50.0
                rsv_list.append(rsv)

            # 初始化 K、D
            k = 50.0
            d = 50.0
            for rsv in rsv_list:
                k = 2.0 / 3.0 * k + 1.0 / 3.0 * rsv
                d = 2.0 / 3.0 * d + 1.0 / 3.0 * k

            j = 3.0 * k - 2.0 * d

            # 信号判断
            if k > 80 and d > 80:
                signal = "超买"
            elif k < 20 and d < 20:
                signal = "超卖"
            else:
                signal = "正常"

            return {
                "k": round(k, 2),
                "d": round(d, 2),
                "j": round(j, 2),
                "signal": signal,
            }

        except Exception as e:
            logger.error(f"KDJ计算异常: {e}")
            return default

    # ──────────────────────────────────────────
    # WR 威廉指标
    # ──────────────────────────────────────────

    def calculate_wr(self, prices: List[float], highs: List[float], lows: List[float],
                     period: int = 14) -> Optional[float]:
        """
        计算 WR 威廉指标（与KDJ互补）

        WR = (HH - Close) / (HH - LL) * -100

        Args:
            prices: 收盘价序列
            highs: 最高价序列
            lows: 最低价序列
            period: 周期（默认14）

        Returns:
            WR 值 (-100~0)
        """
        try:
            n = len(prices)
            if n < period or len(highs) < n or len(lows) < n:
                return None

            sub_prices = prices[-period:]
            sub_highs = highs[-period:]
            sub_lows = lows[-period:]

            hh = max(sub_highs)
            ll = min(sub_lows)

            if hh == ll:
                return -50.0

            close = sub_prices[-1]
            wr = (hh - close) / (hh - ll) * -100

            return round(wr, 2)

        except Exception as e:
            logger.error(f"WR计算异常: {e}")
            return None

    # ──────────────────────────────────────────
    # CCI 商品通道指标
    # ──────────────────────────────────────────

    def calculate_cci(self, prices: List[float], highs: List[float], lows: List[float],
                      period: int = 20) -> Optional[float]:
        """
        计算 CCI 商品通道指标

        CCI = (TP - SMA(TP)) / (0.015 * MD)
        TP = (High + Low + Close) / 3

        Args:
            prices: 收盘价序列
            highs: 最高价序列
            lows: 最低价序列
            period: 周期（默认20）

        Returns:
            CCI 值
        """
        try:
            n = len(prices)
            if n < period or len(highs) < n or len(lows) < n:
                return None

            # 计算典型价格 TP
            tp_list = [(highs[i] + lows[i] + prices[i]) / 3.0 for i in range(n)]

            sub_tp = tp_list[-period:]
            sma_tp = sum(sub_tp) / period

            # 平均绝对偏差 MD
            md = sum(abs(tp - sma_tp) for tp in sub_tp) / period

            if md == 0:
                return 0.0

            current_tp = tp_list[-1]
            cci = (current_tp - sma_tp) / (0.015 * md)

            return round(cci, 2)

        except Exception as e:
            logger.error(f"CCI计算异常: {e}")
            return None

    # ──────────────────────────────────────────
    # SAR 抛物线转向
    # ──────────────────────────────────────────

    def calculate_sar(self, prices: List[float], highs: List[float], lows: List[float],
                      acceleration: float = 0.02, max_acc: float = 0.2) -> Dict[str, Any]:
        """
        计算 SAR 抛物线转向指标

        Args:
            prices: 收盘价序列（从旧到新）
            highs: 最高价序列
            lows: 最低价序列
            acceleration: 加速因子初始值（默认0.02）
            max_acc: 加速因子最大值（默认0.2）

        Returns:
            {"sar": val, "trend": "上升"/"下降"}
        """
        default = {"sar": None, "trend": "未知"}
        try:
            n = len(prices)
            if n < 3 or len(highs) < n or len(lows) < n:
                return default

            # 初始判断趋势
            sar = min(lows[0], lows[1])  # 初始SAR
            upward = prices[1] >= prices[0]
            ep = max(highs[0], highs[1]) if upward else min(lows[0], lows[1])
            af = acceleration

            for i in range(2, n):
                prev_sar = sar

                if upward:
                    # 上升趋势
                    sar = prev_sar + af * (ep - prev_sar)
                    # SAR不能超过前两期最低价
                    sar = min(sar, min(lows[i - 1], lows[i - 2]))

                    if highs[i] > ep:
                        ep = highs[i]
                        af = min(af + acceleration, max_acc)

                    # 转换条件：价格跌破SAR
                    if prices[i] < sar:
                        upward = False
                        sar = ep
                        ep = min(lows[i], lows[i - 1])
                        af = acceleration
                else:
                    # 下降趋势
                    sar = prev_sar - af * (prev_sar - ep)
                    # SAR不能低于前两期最高价
                    sar = max(sar, max(highs[i - 1], highs[i - 2]))

                    if lows[i] < ep:
                        ep = lows[i]
                        af = min(af + acceleration, max_acc)

                    # 转换条件：价格突破SAR
                    if prices[i] > sar:
                        upward = True
                        sar = ep
                        ep = max(highs[i], highs[i - 1])
                        af = acceleration

            return {
                "sar": round(sar, 2),
                "trend": "上升" if upward else "下降",
            }

        except Exception as e:
            logger.error(f"SAR计算异常: {e}")
            return default

    # ──────────────────────────────────────────
    # OBV 能量潮
    # ──────────────────────────────────────────

    def calculate_obv(self, prices: List[float], volumes: List[float]) -> Dict[str, Any]:
        """
        计算 OBV 能量潮指标

        OBV 基于量价关系：价格上涨日成交量加，价格下跌日成交量减

        Args:
            prices: 收盘价序列（从旧到新）
            volumes: 成交量序列

        Returns:
            {"obv": val, "signal": "量价配合"/"量价背离"}
        """
        default = {"obv": None, "signal": "未知"}
        try:
            n = len(prices)
            if n < 3 or len(volumes) < n:
                return default

            obv = 0
            obv_series = [0]

            for i in range(1, n):
                if prices[i] > prices[i - 1]:
                    obv += volumes[i]
                elif prices[i] < prices[i - 1]:
                    obv -= volumes[i]
                # 价格持平则OBV不变
                obv_series.append(obv)

            # 判断量价关系
            # 近5日价格趋势和OBV趋势对比
            recent_prices = prices[-5:] if len(prices) >= 5 else prices
            recent_obv = obv_series[-5:] if len(obv_series) >= 5 else obv_series

            price_trend = recent_prices[-1] - recent_prices[0]
            obv_trend = recent_obv[-1] - recent_obv[0]

            if price_trend > 0 and obv_trend > 0:
                signal = "量价配合"
            elif price_trend < 0 and obv_trend < 0:
                signal = "量价配合"
            elif (price_trend > 0 and obv_trend < 0) or (price_trend < 0 and obv_trend > 0):
                signal = "量价背离"
            else:
                signal = "无明显信号"

            return {
                "obv": obv,
                "signal": signal,
            }

        except Exception as e:
            logger.error(f"OBV计算异常: {e}")
            return default

    # ──────────────────────────────────────────
    # 背离检测
    # ──────────────────────────────────────────

    def detect_divergence(self, prices: List[float], indicator_values: List[Optional[float]],
                          indicator_name: str = "") -> Dict[str, Any]:
        """
        检测顶背离 / 底背离

        顶背离：价格创新高，但指标没有同步创新高
        底背离：价格创新低，但指标没有同步创新低

        Args:
            prices: 价格序列（从旧到新）
            indicator_values: 指标值序列（对应每个价格点，可含None）
            indicator_name: 指标名称（用于日志）

        Returns:
            {"type": "顶背离"/"底背离"/None, "strength": val (0~1)}
        """
        default = {"type": None, "strength": 0.0}
        try:
            # 过滤有效数据
            valid_indices = [i for i, v in enumerate(indicator_values) if v is not None]
            if len(valid_indices) < 6:
                return default

            # 取近期的两段高点/低点判断
            recent_len = min(len(valid_indices), 30)
            valid = valid_indices[-recent_len:]

            filter_prices = [prices[i] for i in valid]
            filter_indicators = [indicator_values[i] for i in valid]

            # 找价格波段高点和低点
            price_peaks = []
            price_troughs = []
            m = len(filter_prices)

            # 简单波峰波谷检测
            for i in range(2, m - 2):
                # 波峰：比前后各两个点都高
                if (filter_prices[i] > filter_prices[i - 1] and
                        filter_prices[i] > filter_prices[i - 2] and
                        filter_prices[i] > filter_prices[i + 1] and
                        filter_prices[i] > filter_prices[i + 2]):
                    price_peaks.append((i, filter_prices[i], filter_indicators[i]))

                # 波谷：比前后各两个点都低
                if (filter_prices[i] < filter_prices[i - 1] and
                        filter_prices[i] < filter_prices[i - 2] and
                        filter_prices[i] < filter_prices[i + 1] and
                        filter_prices[i] < filter_prices[i + 2]):
                    price_troughs.append((i, filter_prices[i], filter_indicators[i]))

            # 顶背离：价格新高但指标新低
            if len(price_peaks) >= 2:
                last_two = price_peaks[-2:]
                p1_price, p1_ind = last_two[0][1], last_two[0][2]
                p2_price, p2_ind = last_two[1][1], last_two[1][2]

                if p2_price > p1_price and p2_ind < p1_ind:
                    # 强度 = (价格涨幅比例 + 指标跌幅比例) / 2
                    price_ratio = (p2_price - p1_price) / p1_price if p1_price > 0 else 0
                    ind_ratio = abs(p2_ind - p1_ind) / max(abs(p1_ind), 1)
                    strength = min((price_ratio + ind_ratio) / 2 * 10, 1.0)
                    return {"type": "顶背离", "strength": round(strength, 4)}

            # 底背离：价格新低但指标新高
            if len(price_troughs) >= 2:
                last_two = price_troughs[-2:]
                t1_price, t1_ind = last_two[0][1], last_two[0][2]
                t2_price, t2_ind = last_two[1][1], last_two[1][2]

                if t2_price < t1_price and t2_ind > t1_ind:
                    price_ratio = abs(t2_price - t1_price) / t1_price if t1_price > 0 else 0
                    ind_ratio = abs(t2_ind - t1_ind) / max(abs(t1_ind), 1)
                    strength = min((price_ratio + ind_ratio) / 2 * 10, 1.0)
                    return {"type": "底背离", "strength": round(strength, 4)}

            return default

        except Exception as e:
            logger.error(f"背离检测异常 ({indicator_name}): {e}")
            return default

    # ──────────────────────────────────────────
    # 辅助指标
    # ──────────────────────────────────────────

    def calculate_macd(self, prices: List[float]) -> Optional[Dict[str, float]]:
        """
        计算 MACD 指标

        - DIF = EMA12 - EMA26
        - DEA = EMA9(DIF)
        - MACD = (DIF - DEA) * 2

        Args:
            prices: 价格序列（从旧到新）

        Returns:
            {"dif": ..., "dea": ..., "macd": ...}
            数据不足时返回 None
        """
        try:
            if len(prices) < 35:  # 至少需要35个数据点
                return None

            ema12 = self.calculate_ema(prices, 12)
            ema26 = self.calculate_ema(prices, 26)

            if ema12 is None or ema26 is None:
                return None

            dif = ema12 - ema26

            # 计算 DIF 序列用于 DEA
            dif_values = []
            for i in range(26, len(prices)):
                sub_prices = prices[:i+1]
                e12 = self.calculate_ema(sub_prices, 12)
                e26 = self.calculate_ema(sub_prices, 26)
                if e12 is not None and e26 is not None:
                    dif_values.append(e12 - e26)

            if len(dif_values) < 9:
                return {"dif": round(dif, 4), "dea": None, "macd": None}

            dea = self.calculate_ema(dif_values, 9)
            macd = (dif - dea) * 2 if dea is not None else None

            return {
                "dif": round(dif, 4),
                "dea": round(dea, 4) if dea is not None else None,
                "macd": round(macd, 4) if macd is not None else None,
            }

        except Exception as e:
            logger.error(f"MACD计算异常: {e}")
            return None

    # ──────────────────────────────────────────
    # 综合获取所有指标（从数据库）
    # ──────────────────────────────────────────

    def get_all_indicators(self, code: str) -> Dict[str, Any]:
        """
        从数据库获取历史数据并计算所有技术指标

        从 market_snapshots 表获取收盘价序列，计算：
        - RSI(14)
        - MA5, MA10, MA20, MA30, MA60
        - 布林带（20,2）
        - MACD (12,26,9)

        Args:
            code: 股票代码

        Returns:
            {
                "code": code,
                "price": 最新价格,
                "rsi": RSI值,
                "rsi_signal": "超买"/"超卖"/"正常",
                "ma": {"ma5": ..., ...},
                "bollinger": {...},
                "macd": {...},
                "price_vs_ma": "多头排列"/"空头排列"/"震荡",
                "data_points": 使用的数据点数,
                "error": 错误信息(如有),
            }
        """
        result = {
            "code": code,
            "price": None,
            "rsi": None,
            "rsi_signal": "未知",
            "ma": {},
            "bollinger": {},
            "macd": None,
            "kdj": {},
            "wr": None,
            "cci": None,
            "sar": {},
            "obv": {},
            "divergence": {},
            "price_vs_ma": "未知",
            "data_points": 0,
        }

        try:
            series = self._get_price_series(code)
            prices = series["prices"]
            if not prices:
                logger.warning(f"获取 {code} 历史价格失败或数据不足")
                result["error"] = "数据不足"
                return result

            highs = series["highs"]
            lows = series["lows"]
            volumes = series["volumes"]

            current_price = prices[-1]
            result["price"] = current_price
            result["data_points"] = len(prices)

            # 1. RSI
            rsi = self.calculate_rsi_smoothed(prices, 14)
            result["rsi"] = rsi

            if rsi is not None:
                if rsi >= 70:
                    result["rsi_signal"] = "超买"
                elif rsi <= 30:
                    result["rsi_signal"] = "超卖"
                else:
                    result["rsi_signal"] = "正常"

            # 2. 均线
            result["ma"] = self.calculate_all_ma(prices)

            # 3. 价格与均线的关系
            result["price_vs_ma"] = self._judge_ma_trend(prices, result["ma"])

            # 4. 布林带
            result["bollinger"] = self.calculate_bollinger(prices, 20)

            # 5. MACD
            macd = self.calculate_macd(prices)
            if macd:
                result["macd"] = macd
                # MACD 金叉/死叉信号
                if macd.get("dif") is not None and macd.get("dea") is not None:
                    if macd["dif"] > macd["dea"] and macd.get("macd", 0) > 0:
                        result["macd_signal"] = "金叉"
                    elif macd["dif"] < macd["dea"] and macd.get("macd", 0) < 0:
                        result["macd_signal"] = "死叉"
                    else:
                        result["macd_signal"] = "震荡"

            # 6. KDJ
            if highs and lows:
                result["kdj"] = self.calculate_kdj(prices, highs, lows)

            # 7. WR
            if highs and lows:
                result["wr"] = self.calculate_wr(prices, highs, lows)

            # 8. CCI
            if highs and lows:
                result["cci"] = self.calculate_cci(prices, highs, lows)

            # 9. SAR
            if highs and lows:
                result["sar"] = self.calculate_sar(prices, highs, lows)

            # 10. OBV
            result["obv"] = self.calculate_obv(prices, volumes)

            # 11. 背离检测（基于RSI）
            if rsi is not None:
                # 构建RSI序列用于背离检测
                rsi_series = []
                for i in range(20, len(prices) + 1):
                    sub = prices[:i]
                    if len(sub) >= 14:
                        val = self.calculate_rsi_smoothed(sub, 14)
                        rsi_series.append(val)
                    else:
                        rsi_series.append(None)
                if len(rsi_series) == len(prices):
                    result["divergence"] = self.detect_divergence(prices, rsi_series, "RSI")

        except Exception as e:
            logger.error(f"获取 {code} 技术指标异常: {e}")
            result["error"] = str(e)

        return result

    # ──────────────────────────────────────────
    # 辅助方法
    # ──────────────────────────────────────────

    def _get_price_series(self, code: str, max_points: int = 100) -> dict:
        """
        获取股票OHLCV序列
        优先从 daily_prices 表读取，数据不足时从API拉取并存入数据库

        Args:
            code: 股票代码
            max_points: 最多获取多少条

        Returns:
            {
                "prices": 收盘价列表（从旧到新）,
                "highs": 最高价列表,
                "lows": 最低价列表,
                "volumes": 成交量列表,
            }
            数据不足时返回空列表
        """
        result = {"prices": [], "highs": [], "lows": [], "volumes": []}

        if not self.db:
            logger.warning(f"数据库未初始化，无法获取 {code} 历史价格")
            return result

        try:
            # 优先从 daily_prices 表读取
            rows = self.db.get_price_history(code, days=max_points)
            prices = [r["close_price"] for r in rows
                      if r.get("close_price") is not None and r["close_price"] > 0]
            highs = [r.get("high_price", r.get("close_price", 0)) for r in rows
                     if r.get("close_price") is not None and r["close_price"] > 0]
            lows = [r.get("low_price", r.get("close_price", 0)) for r in rows
                    if r.get("close_price") is not None and r["close_price"] > 0]
            volumes = [r.get("volume", r.get("vol", 0)) for r in rows
                       if r.get("close_price") is not None and r["close_price"] > 0]

            # 如果 daily_prices 数据不足（少于20天），回退到 market_snapshots
            if len(prices) < 20:
                logger.info(f"{code} daily_prices数据不足({len(prices)}条)，尝试从market_snapshots获取")
                conn = self.db._connect()
                rows2 = conn.execute("""
                    SELECT price FROM market_snapshots
                    WHERE stock_code = ?
                      AND price IS NOT NULL
                      AND price > 0
                    ORDER BY snapshot_time ASC
                    LIMIT ?
                """, (code, max_points)).fetchall()
                conn.close()

                prices2 = [r["price"] for r in rows2 if r["price"] is not None and r["price"] > 0]
                if len(prices2) > len(prices):
                    prices = prices2
                    highs = prices2  # fallback: use close as high/low proxy
                    lows = prices2
                    volumes = [0] * len(prices2)

            # 如果仍不足20条，尝试从API拉取历史K线
            if len(prices) < 20:
                logger.info(f"{code} 本地数据仍不足({len(prices)}条)，尝试从API拉取历史日K线")
                try:
                    from collector.spiders.history_quotes import HistoryQuotesCollector
                    collector = HistoryQuotesCollector(self.db)
                    count = collector.collect_stock(code, limit=max_points)
                    if count > 0:
                        # 重新从 daily_prices 读取
                        rows3 = self.db.get_price_history(code, days=max_points)
                        prices = [r["close_price"] for r in rows3
                                  if r.get("close_price") is not None and r["close_price"] > 0]
                        highs = [r.get("high_price", r.get("close_price", 0)) for r in rows3
                                 if r.get("close_price") is not None and r["close_price"] > 0]
                        lows = [r.get("low_price", r.get("close_price", 0)) for r in rows3
                                if r.get("close_price") is not None and r["close_price"] > 0]
                        volumes = [r.get("volume", r.get("vol", 0)) for r in rows3
                                   if r.get("close_price") is not None and r["close_price"] > 0]
                        logger.info(f"{code} 从API拉取 {count} 条K线后，现有 {len(prices)} 条")
                except ImportError:
                    logger.warning("HistoryQuotesCollector 不可用")
                except Exception as e:
                    logger.warning(f"{code} API拉取历史K线异常: {e}")

        except Exception as e:
            logger.warning(f"获取 {code} 价格序列异常: {e}")

        result["prices"] = prices
        result["highs"] = highs if len(highs) == len(prices) else prices
        result["lows"] = lows if len(lows) == len(prices) else prices
        result["volumes"] = volumes if len(volumes) == len(prices) else [0] * len(prices)
        return result

    def _judge_ma_trend(self, prices: List[float],
                        ma_dict: Dict[str, Optional[float]]) -> str:
        """
        判断均线排列状态
        """
        current_price = prices[-1] if prices else 0
        ma5 = ma_dict.get("ma5")
        ma10 = ma_dict.get("ma10")
        ma20 = ma_dict.get("ma20")

        if None in (ma5, ma10, ma20):
            return "未知"

        # 多头排列：MA5 > MA10 > MA20 且价格在MA5上方
        if ma5 > ma10 > ma20 and current_price > ma5:
            return "多头排列"

        # 空头排列：MA5 < MA10 < MA20 且价格在MA5下方
        if ma5 < ma10 < ma20 and current_price < ma5:
            return "空头排列"

        # 短期向上：价格上穿MA5
        if len(prices) >= 3:
            prev_price = prices[-2]
            if prev_price < ma5 and current_price > ma5:
                return "短期向上突破"
            if prev_price > ma5 and current_price < ma5:
                return "短期向下突破"

        return "震荡整理"


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    ti = TechnicalIndicator()

    # 模拟价格数据
    import random
    random.seed(42)
    sim_prices = [100.0]
    for i in range(1, 80):
        sim_prices.append(sim_prices[-1] * (1 + random.uniform(-0.03, 0.03)))

    print("===== 技术指标测试 =====")
    print(f"数据点数: {len(sim_prices)}")

    # RSI
    rsi = ti.calculate_rsi_smoothed(sim_prices, 14)
    print(f"\nRSI(14): {rsi}")
    if rsi:
        if rsi >= 70:
            print(f"  → 超买区域")
        elif rsi <= 30:
            print(f"  → 超卖区域")
        else:
            print(f"  → 正常区域")

    # 均线
    ma_results = ti.calculate_all_ma(sim_prices)
    print(f"\n均线:")
    for k, v in ma_results.items():
        print(f"  {k}: {v}")

    # 布林带
    bb = ti.calculate_bollinger(sim_prices, 20)
    print(f"\n布林带(20,2):")
    for k, v in bb.items():
        print(f"  {k}: {v}")

    # MACD
    macd = ti.calculate_macd(sim_prices)
    if macd:
        print(f"\nMACD:")
        for k, v in macd.items():
            print(f"  {k}: {v}")
