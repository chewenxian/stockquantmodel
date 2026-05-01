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
            "price_vs_ma": "未知",
            "data_points": 0,
        }

        try:
            prices = self._get_price_series(code)
            if not prices:
                logger.warning(f"获取 {code} 历史价格失败或数据不足")
                result["error"] = "数据不足"
                return result

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

        except Exception as e:
            logger.error(f"获取 {code} 技术指标异常: {e}")
            result["error"] = str(e)

        return result

    # ──────────────────────────────────────────
    # 辅助方法
    # ──────────────────────────────────────────

    def _get_price_series(self, code: str, max_points: int = 100) -> List[float]:
        """
        从数据库获取股票收盘价序列

        Args:
            code: 股票代码
            max_points: 最多获取多少条

        Returns:
            价格列表（从旧到新）
        """
        prices: List[float] = []

        if not self.db:
            logger.warning(f"数据库未初始化，无法获取 {code} 历史价格")
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

            # 如果数据不够，尝试从 analysis 表获取历史数据（降序取）
            if len(prices) < 20:
                rows2 = conn.execute("""
                    SELECT llm_analysis FROM analysis
                    WHERE stock_code = ?
                    ORDER BY date DESC LIMIT 1
                """, (code,)).fetchall()

            conn.close()

        except Exception as e:
            logger.warning(f"获取 {code} 价格序列异常: {e}")

        return prices

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
