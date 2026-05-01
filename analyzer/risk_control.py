"""
动态风控模块
提供：波动率计算、仓位建议、止损预警、综合风险评级

数据来源：storage.database.Database → market_snapshots 表
所有指标纯 Python 实现
"""
import logging
import math
import random
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RiskController:
    """
    动态风控控制器

    功能：
    - 历史波动率计算
    - 基于波动率的仓位建议
    - 止损检查（基于最近价格 vs 入场价）
    - 综合风险评级
    """

    def __init__(self, db=None):
        self.db = db

    # ──────────────────────────────────────────
    # 波动率计算
    # ──────────────────────────────────────────

    def calculate_volatility(self, code: str, days: int = 20) -> Optional[float]:
        """
        计算历史波动率（年化）

        波动率 = 收益率标准差 * sqrt(252)
        即：将日波动率年化（一年约252个交易日）

        Args:
            code: 股票代码
            days: 计算周期（交易日数，默认20）

        Returns:
            年化波动率（百分比，如 0.25 表示 25%）
            数据不足时返回 None
        """
        try:
            prices = self._get_price_series(code, max_points=days + 5)
            if not prices or len(prices) < days:
                logger.warning(f"{code} 历史价格不足 {days} 条")
                return None

            # 只取最近 days 个交易日数据
            recent_prices = prices[-(days + 1):]

            # 计算每日对数收益率
            log_returns = []
            for i in range(1, len(recent_prices)):
                if recent_prices[i-1] > 0 and recent_prices[i] > 0:
                    log_r = math.log(recent_prices[i] / recent_prices[i-1])
                    log_returns.append(log_r)

            if len(log_returns) < 2:
                return None

            # 计算标准差
            mean = sum(log_returns) / len(log_returns)
            variance = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
            daily_std = math.sqrt(variance)

            # 年化波动率
            annualized_vol = daily_std * math.sqrt(252)

            return round(annualized_vol, 4)

        except Exception as e:
            logger.error(f"波动率计算异常 ({code}): {e}")
            return None

    def calculate_simple_volatility(self, code: str, days: int = 20) -> Optional[float]:
        """
        简化波动率计算（基于涨跌幅绝对值）

        适合数据量少或需要快速计算时使用

        Args:
            code: 股票代码
            days: 计算周期

        Returns:
            年化波动率（百分比）
        """
        try:
            prices = self._get_price_series(code, max_points=days + 5)
            if not prices or len(prices) < days:
                return None

            recent = prices[-(days + 1):]

            # 简单涨跌幅法
            changes = []
            for i in range(1, len(recent)):
                if recent[i-1] > 0:
                    pct = abs((recent[i] - recent[i-1]) / recent[i-1])
                    changes.append(pct)

            if not changes:
                return None

            avg_daily_change = sum(changes) / len(changes)

            # 日平均涨跌幅 * sqrt(252) 作为年化波动率
            annualized_vol = avg_daily_change * math.sqrt(252)

            return round(annualized_vol, 4)

        except Exception as e:
            logger.error(f"简化波动率计算异常 ({code}): {e}")
            return None

    # ──────────────────────────────────────────
    # 仓位建议
    # ──────────────────────────────────────────

    def get_position_advice(self, volatility: Optional[float]) -> Dict:
        """
        基于波动率的仓位建议

        分类:
        - 低波动 (<20%): 正常仓位 (80-100%)
        - 中低波动 (20-25%): 偏重仓位 (60-80%)
        - 中波动 (25-35%): 半仓 (40-60%)
        - 中高波动 (35-50%): 轻仓 (20-40%)
        - 高波动 (>50%): 观望/极轻仓 (0-20%)

        Args:
            volatility: 年化波动率（小数，如 0.25）

        Returns:
            {
                "suggested_position": 0.6,        # 建议仓位比例 (0~1)
                "position_level": "半仓",          # 仓位描述
                "range": [0.4, 0.6],              # 建议仓位范围
                "risk_level": "中",                # 风险评估
                "advice": "建议半仓操作",          # 文字建议
            }
        """
        if volatility is None:
            return {
                "suggested_position": 0.5,
                "position_level": "半仓",
                "range": [0.4, 0.6],
                "risk_level": "中",
                "advice": "数据不足，建议半仓操作",
            }

        try:
            vol_pct = volatility * 100  # 转为百分比

            if vol_pct < 20:
                position = 0.9
                level = "正常仓位"
                risk = "低"
                advice = "波动率低，可正常仓位操作"

            elif vol_pct < 25:
                position = 0.7
                level = "偏重仓位"
                risk = "低"
                advice = "波动率偏低，可适当加大仓位"

            elif vol_pct < 35:
                position = 0.5
                level = "半仓"
                risk = "中"
                advice = "波动率适中，建议半仓操作"

            elif vol_pct < 50:
                position = 0.3
                level = "轻仓"
                risk = "高"
                advice = "波动率偏高，建议轻仓操作，控制风险"

            else:
                position = 0.1
                level = "观望"
                risk = "极高"
                advice = f"波动率过高 ({vol_pct:.1f}%)，建议观望或极轻仓"

            range_low = max(0, position - 0.15)
            range_high = min(1.0, position + 0.15)

            return {
                "suggested_position": round(position, 2),
                "position_level": level,
                "range": [round(range_low, 2), round(range_high, 2)],
                "risk_level": risk,
                "advice": advice,
                "volatility_pct": round(vol_pct, 1),
            }

        except Exception as e:
            logger.error(f"仓位建议计算异常: {e}")

            return {
                "suggested_position": 0.5,
                "position_level": "半仓",
                "range": [0.4, 0.6],
                "risk_level": "中",
                "advice": "仓位计算异常，建议半仓",
            }

    # ──────────────────────────────────────────
    # 止损检查
    # ──────────────────────────────────────────

    def check_stop_loss(self, code: str, entry_price: float,
                        stop_loss_pct: Optional[float] = None) -> Dict:
        """
        止损检查

        基于最近收盘价判断是否需要触发止损

        Args:
            code: 股票代码
            entry_price: 入场价格
            stop_loss_pct: 止损比例（如 0.05 表示下跌5%止损），
                          为 None 时根据波动率动态计算

        Returns:
            {
                "current_price": 最新价格,
                "entry_price": 入场价格,
                "change_pct": 盈亏比例,
                "stop_loss_price": 止损价,
                "stop_loss_pct": 止损比例,
                "triggered": False,        # 是否触发止损
                "distance_to_stop": 0.03,  # 距止损线的距离
                "status": "正常",           # 正常/接近止损/已触发
                "volatility_based": True,   # 是否是动态计算的止损价
                "advice": "...",            # 建议文字
            }
        """
        result = {
            "current_price": None,
            "entry_price": entry_price,
            "change_pct": None,
            "stop_loss_price": None,
            "stop_loss_pct": None,
            "triggered": False,
            "distance_to_stop": None,
            "status": "未知",
            "volatility_based": False,
            "advice": "数据不足，请手动判断",
        }

        try:
            # 获取最新价格
            current_price = self._get_latest_price(code)
            if current_price is None or current_price <= 0:
                return result

            result["current_price"] = current_price
            change_pct = (current_price - entry_price) / entry_price
            result["change_pct"] = round(change_pct, 4)

            # 计算止损比例
            if stop_loss_pct is not None:
                sl_pct = stop_loss_pct
                result["volatility_based"] = False
            else:
                # 基于波动率动态计算止损
                volatility = self.calculate_volatility(code, 20)
                if volatility is not None:
                    # 波动率越高，止损幅度越大（避免被正常波动洗出去）
                    sl_pct = max(volatility * 0.8, 0.03)  # 至少3%
                    result["volatility_based"] = True
                else:
                    sl_pct = 0.07  # 默认7%止损

            stop_loss_price = entry_price * (1 - sl_pct)
            result["stop_loss_price"] = round(stop_loss_price, 2)
            result["stop_loss_pct"] = round(sl_pct, 4)

            # 止损判断
            distance = (current_price - stop_loss_price) / stop_loss_price
            result["distance_to_stop"] = round(distance, 4)

            if current_price <= stop_loss_price:
                result["triggered"] = True
                result["status"] = "已触发"
                loss = (current_price - entry_price) / entry_price * 100
                result["advice"] = f"⚠️ 止损已触发！当前亏损 {loss:.1f}%，建议立即执行止损"

            elif distance < 0.05:
                result["status"] = "接近止损"
                result["advice"] = (f"⚡ 接近止损线！距止损仅 {distance*100:.1f}%，"
                                    f"建议密切监控，可考虑部分减仓")

            else:
                result["status"] = "正常"
                pnl = change_pct * 100
                result["advice"] = (f"✅ 运行正常，当前盈亏 {pnl:+.1f}%，"
                                    f"距止损线仍有 {distance*100:.1f}% 空间")

            # 盈利保护（移动止盈建议）
            if change_pct > 0.10:
                trailing_stop = current_price * (1 - sl_pct * 0.5)
                result["trailing_stop_price"] = round(trailing_stop, 2)
                if not result.get("triggered"):
                    result["advice"] += f"，建议设置移动止盈 {trailing_stop:.2f}"

        except Exception as e:
            logger.error(f"止损检查异常 ({code}): {e}")
            result["advice"] = f"止损检查异常: {e}"

        return result

    # ──────────────────────────────────────────
    # 综合风险评估
    # ──────────────────────────────────────────

    def get_risk_level(self, code: str) -> Dict:
        """
        综合风险评级

        评估维度：
        - 波动率风险（40%权重）
        - 价格位置风险（30%权重）
        - 资金流向风险（30%权重）

        Returns:
            {
                "risk_level": "中",          # 低/中/高/极高
                "risk_score": 45.0,           # 0~100 的风险评分
                "components": {               # 各维度评分
                    "volatility_risk": 30,
                    "price_risk": 50,
                    "flow_risk": 55,
                },
                "details": {                  # 详细数据
                    "volatility": 0.25,
                    "price_ma_ratio": 1.05,
                    "main_net": 1000000,
                },
                "advice": "建议半仓操作，控制风险",
            }
        """
        default = {
            "risk_level": "中",
            "risk_score": 50.0,
            "components": {},
            "details": {},
            "advice": "数据不足，风险未知",
        }

        try:
            details: Dict[str, Any] = {}
            components: Dict[str, float] = {}

            # 1. 波动率风险 (0~100)
            volatility = self.calculate_volatility(code, 20)
            details["volatility"] = volatility

            if volatility is not None:
                vol_pct = volatility * 100
                if vol_pct < 20:
                    vol_risk = 20  # 低波动
                elif vol_pct < 30:
                    vol_risk = 40
                elif vol_pct < 40:
                    vol_risk = 60
                elif vol_pct < 60:
                    vol_risk = 80
                else:
                    vol_risk = 95
                components["volatility_risk"] = vol_risk
            else:
                components["volatility_risk"] = 50

            # 2. 价格位置风险 (0~100)
            price = self._get_latest_price(code)
            if price is not None:
                # 获取最近均线
                prices = self._get_price_series(code, max_points=25)
                if len(prices) >= 10:
                    ma20 = sum(prices[-20:]) / 20 if len(prices) >= 20 else None
                    ma10 = sum(prices[-10:]) / 10 if len(prices) >= 10 else None

                    details["price_ma_ratio"] = price / ma20 if ma20 else None

                    # 价格远离均线时风险高（均值回归）
                    if ma20 and price > ma20 * 1.15:
                        price_risk = 70  # 高位偏离
                    elif ma20 and price < ma20 * 0.85:
                        price_risk = 70  # 低位偏离（下跌趋势）
                    elif ma20 and ma10:
                        # 空头排列
                        if ma10 < ma20:
                            price_risk = 55
                        else:
                            price_risk = 40
                    else:
                        price_risk = 50
                else:
                    price_risk = 50
            else:
                price_risk = 50

            components["price_risk"] = price_risk

            # 3. 资金流向风险 (0~100)
            if self.db:
                try:
                    flow = self.db.get_latest_money_flow(code)
                    if flow:
                        main_net = flow.get("main_net", 0)
                        details["main_net"] = main_net

                        if main_net > 0:
                            flow_risk = 30  # 主力流入，风险较低
                        elif main_net > -5000000:
                            flow_risk = 60  # 小量流出
                        else:
                            flow_risk = 80  # 大量流出
                    else:
                        flow_risk = 50
                except Exception:
                    flow_risk = 50
            else:
                flow_risk = 50

            components["flow_risk"] = flow_risk

            # 综合风险评分
            weights = {
                "volatility_risk": 0.40,
                "price_risk": 0.30,
                "flow_risk": 0.30,
            }

            risk_score = sum(
                components.get(k, 50) * w
                for k, w in weights.items()
            )

            # 定级
            if risk_score < 25:
                risk_level = "低"
                advice = "综合风险较低，可正常操作"
            elif risk_score < 45:
                risk_level = "低"
                advice = "风险可控，注意仓位管理"
            elif risk_score < 60:
                risk_level = "中"
                advice = "风险适中，建议半仓操作"
            elif risk_score < 75:
                risk_level = "高"
                advice = "风险较高，建议轻仓操作"
            else:
                risk_level = "极高"
                advice = "风险极高，建议观望"

            # ── 4. VaR 分析 ──
            var_result = self.calculate_var(code, days=252, confidence=0.95)
            if var_result.get("parametric_var") is not None:
                details["var_parametric"] = var_result["parametric_var"]
                details["var_historical"] = var_result["historical_var"]
                details["var_parametric_annual"] = var_result.get("parametric_var_annual")

                # VaR 因子：如果日 VaR > 3%，提高风险分
                daily_var = var_result["parametric_var"]
                if daily_var > 0.04:
                    components["var_risk"] = 80
                elif daily_var > 0.03:
                    components["var_risk"] = 60
                elif daily_var > 0.02:
                    components["var_risk"] = 40
                else:
                    components["var_risk"] = 25

            # ── 5. CVaR 分析 ──
            cvar_result = self.calculate_cvar(code, days=252, confidence=0.95)
            if cvar_result.get("cvar") is not None:
                details["cvar"] = cvar_result["cvar"]
                cvar_val = cvar_result["cvar"]
                if cvar_val > 0.06:
                    components["cvar_risk"] = 85
                elif cvar_val > 0.04:
                    components["cvar_risk"] = 65
                elif cvar_val > 0.025:
                    components["cvar_risk"] = 45
                else:
                    components["cvar_risk"] = 30

            # ── 6. GARCH 波动率预测 ──
            prices = self._get_price_series(code, max_points=126)
            if len(prices) >= 20:
                log_returns = []
                for i in range(1, len(prices)):
                    if prices[i - 1] > 0 and prices[i] > 0:
                        log_returns.append(math.log(prices[i] / prices[i - 1]))

                if len(log_returns) >= 20:
                    garch_result = self.predict_garch_volatility(log_returns, forecast_days=5)
                    if garch_result.get("current_vol") is not None:
                        details["garch_current_vol"] = garch_result["current_vol"]
                        details["garch_long_term_vol"] = garch_result["long_term_vol"]
                        details["garch_forecast"] = garch_result["forecast"]

                        # GARCH 趋势：如果预测波动率上升，增加风险分
                        if len(garch_result["forecast"]) >= 3:
                            forecast_trend = (
                                garch_result["forecast"][-1] / garch_result["forecast"][0]
                            )
                            details["garch_trend"] = round(forecast_trend, 4)
                            if forecast_trend > 1.15:
                                components["garch_trend_risk"] = 75
                            elif forecast_trend > 1.08:
                                components["garch_trend_risk"] = 55
                            else:
                                components["garch_trend_risk"] = 30

            # 综合风险评分（包含 VaR/CVaR/GARCH）
            weights = {
                "volatility_risk": 0.30,
                "price_risk": 0.20,
                "flow_risk": 0.20,
                "var_risk": 0.10,
                "cvar_risk": 0.10,
                "garch_trend_risk": 0.10,
            }

            risk_score = sum(
                components.get(k, 50) * w
                for k, w in weights.items()
            )

            # 如果波动率很高，加上额外提示
            if volatility and volatility > 0.40:
                advice += f"（波动率 {volatility*100:.1f}%，VaR 预警）"

            # 如果 VaR/CVaR 很高，附加风险提示
            if details.get("var_historical") and details["var_historical"] > 0.035:
                advice += f" 历史VaR: {details['var_historical']*100:.2f}%/日"

            return {
                "risk_level": risk_level,
                "risk_score": round(risk_score, 1),
                "components": components,
                "details": details,
                "var_analysis": var_result if var_result.get("parametric_var") else None,
                "cvar_analysis": cvar_result if cvar_result.get("cvar") else None,
                "garch_analysis": garch_result if garch_result.get("current_vol") else None,
                "advice": advice,
            }

        except Exception as e:
            logger.error(f"综合风险评估异常 ({code}): {e}")

        return default

    # ──────────────────────────────────────────
    # VaR 计算
    # ──────────────────────────────────────────

    def calculate_var(self, code: str, days: int = 252, confidence: float = 0.95) -> Dict:
        """
        计算 Value at Risk（风险价值）

        - 参数法（正态分布假设）
        - 历史模拟法

        Args:
            code: 股票代码
            days: 计算周期（交易日数）
            confidence: 置信水平，默认0.95

        Returns:
            {
                "parametric_var": 0.0321,    # 参数法VaR（百分比）
                "historical_var": 0.0285,    # 历史模拟法VaR（百分比）
                "daily_volatility": 0.025,   # 日波动率
                "confidence": 0.95,
                "days": 252,
            }
        """
        result = {
            "parametric_var": None,
            "historical_var": None,
            "daily_volatility": None,
            "confidence": confidence,
            "days": days,
        }

        try:
            prices = self._get_price_series(code, max_points=days + 10)
            if not prices or len(prices) < 10:
                logger.warning(f"{code} 价格数据不足，无法计算 VaR")
                return result

            # 计算日对数收益率
            log_returns = []
            for i in range(1, len(prices)):
                if prices[i - 1] > 0 and prices[i] > 0:
                    log_returns.append(math.log(prices[i] / prices[i - 1]))

            if len(log_returns) < 5:
                return result

            # 日波动率
            mean_r = sum(log_returns) / len(log_returns)
            variance = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
            daily_vol = math.sqrt(variance)
            result["daily_volatility"] = round(daily_vol, 6)

            # 1. 参数法 VaR（正态分布假设）
            # VaR = z_score * sigma，z(0.95)=1.645, z(0.99)=2.326
            z_scores = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}
            z = z_scores.get(confidence, 1.645)
            parametric_var = z * daily_vol
            result["parametric_var"] = round(parametric_var, 6)

            # 2. 历史模拟法 VaR
            sorted_returns = sorted(log_returns)
            n = len(sorted_returns)
            idx = int(n * (1 - confidence))
            idx = max(0, min(idx, n - 1))
            historical_var = abs(sorted_returns[idx])
            result["historical_var"] = round(historical_var, 6)

            # 年化 VaR
            result["parametric_var_annual"] = round(parametric_var * math.sqrt(252), 6)
            result["historical_var_annual"] = round(historical_var * math.sqrt(252), 6)

        except Exception as e:
            logger.error(f"VaR 计算异常 ({code}): {e}")

        return result

    def calculate_cvar(self, code: str, days: int = 252, confidence: float = 0.95) -> Dict:
        """
        计算 Conditional VaR（条件VaR / 期望尾损）

        CVaR = E[loss | loss > VaR]
        即超过 VaR 的尾部损失的平均值

        Args:
            code: 股票代码
            days: 计算周期
            confidence: 置信水平

        Returns:
            {
                "cvar": 0.045,                # CVaR（百分比）
                "var_at_confidence": 0.032,   # 对应置信水平的 VaR
                "tail_count": 10,             # 尾部分布数量
                "confidence": 0.95,
            }
        """
        result = {
            "cvar": None,
            "var_at_confidence": None,
            "tail_count": 0,
            "confidence": confidence,
        }

        try:
            prices = self._get_price_series(code, max_points=days + 10)
            if not prices or len(prices) < 10:
                logger.warning(f"{code} 价格数据不足，无法计算 CVaR")
                return result

            # 计算日收益率
            returns = []
            for i in range(1, len(prices)):
                if prices[i - 1] > 0 and prices[i] > 0:
                    r = (prices[i] - prices[i - 1]) / prices[i - 1]
                    returns.append(r)

            if len(returns) < 5:
                return result

            sorted_returns = sorted(returns)
            n = len(sorted_returns)
            idx = int(n * (1 - confidence))
            idx = max(0, min(idx, n - 1))

            var_val = abs(sorted_returns[idx])
            result["var_at_confidence"] = round(var_val, 6)

            # CVaR：所有超过 VaR 的损失的平均值
            tail_returns = sorted_returns[:idx + 1] if idx > 0 else [sorted_returns[0]]
            tail_returns = [abs(r) for r in tail_returns if r < 0]

            if tail_returns:
                cvar_val = sum(tail_returns) / len(tail_returns)
                result["cvar"] = round(cvar_val, 6)
                result["tail_count"] = len(tail_returns)

        except Exception as e:
            logger.error(f"CVaR 计算异常 ({code}): {e}")

        return result

    # ──────────────────────────────────────────
    # GARCH(1,1) 波动率预测
    # ──────────────────────────────────────────

    def predict_garch_volatility(self, daily_returns: List[float],
                                  forecast_days: int = 5) -> Dict:
        """
        GARCH(1,1) 模型预测波动率

        纯 Python 实现（无需外部库）
        GARCH(1,1): sigma_t^2 = omega + alpha * eps_{t-1}^2 + beta * sigma_{t-1}^2

        Args:
            daily_returns: 日收益率序列
            forecast_days: 预测未来天数，默认5天

        Returns:
            {
                "current_vol": 0.025,           # 当前波动率
                "forecast": [0.026, 0.027, ...], # 未来 forecast_days 天波动率
                "long_term_vol": 0.023,          # 长期均值波动率
                "alpha": 0.15,                    # GARCH alpha 参数
                "beta": 0.80,                     # GARCH beta 参数
                "omega": 0.00001,                 # GARCH omega 参数
                "converged": True,                # 是否收敛
            }
        """
        result = {
            "current_vol": None,
            "forecast": [],
            "long_term_vol": None,
            "alpha": None,
            "beta": None,
            "omega": None,
            "converged": False,
        }

        try:
            if not daily_returns or len(daily_returns) < 10:
                logger.warning("收益率数据不足，无法拟合 GARCH(1,1)")
                return result

            n = len(daily_returns)

            # 1. 初始参数估计
            # 使用样本方差作为初始长期方差
            mean_r = sum(daily_returns) / n
            sample_var = sum((r - mean_r) ** 2 for r in daily_returns) / (n - 1)
            long_term_var = sample_var

            # 2. GARCH(1,1) 参数估计（MLE 近似）
            # 典型参数范围：alpha in [0.05, 0.2], beta in [0.75, 0.95], alpha+beta < 1
            # 使用简单网格搜索
            best_ll = -float("inf")
            best_params = (0.1, 0.85, 0.00001 * sample_var)

            omega_base = 0.05 * sample_var
            for alpha in [0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20]:
                for beta in [0.75, 0.78, 0.80, 0.82, 0.85, 0.88, 0.90, 0.92, 0.95]:
                    if alpha + beta >= 1.0:
                        continue
                    omega = sample_var * (1 - alpha - beta)
                    if omega <= 0:
                        continue

                    # 计算条件方差序列
                    sigma2 = [sample_var]
                    for t in range(1, n):
                        eps2 = daily_returns[t - 1] ** 2
                        s2 = omega + alpha * eps2 + beta * sigma2[-1]
                        sigma2.append(s2)

                    # 对数似然（高斯）
                    log_likelihood = 0.0
                    for t in range(1, n):
                        if sigma2[t] > 0:
                            log_likelihood += -0.5 * (
                                math.log(2 * math.pi) + math.log(sigma2[t]) +
                                daily_returns[t] ** 2 / sigma2[t]
                            )

                    if log_likelihood > best_ll:
                        best_ll = log_likelihood
                        best_params = (alpha, beta, omega)

            alpha, beta, omega = best_params
            result["alpha"] = round(alpha, 4)
            result["beta"] = round(beta, 4)
            result["omega"] = round(omega, 8)
            result["converged"] = True

            # 3. 计算当前波动率
            sigma2 = [sample_var]
            for t in range(1, n):
                eps2 = daily_returns[t - 1] ** 2
                s2 = omega + alpha * eps2 + beta * sigma2[-1]
                sigma2.append(s2)

            current_var = sigma2[-1]
            current_vol = math.sqrt(current_var)
            result["current_vol"] = round(current_vol, 6)

            # 4. 长期均衡波动率
            long_term_var = omega / (1 - alpha - beta) if (alpha + beta) < 1 else sample_var
            result["long_term_vol"] = round(math.sqrt(long_term_var), 6)

            # 5. 预测未来波动率
            # sigma^2_{t+k} = omega + (alpha+beta) * sigma^2_{t+k-1}
            forecast_vars = [current_var]
            for k in range(1, forecast_days + 1):
                f_var = omega + (alpha + beta) * forecast_vars[-1]
                forecast_vars.append(f_var)

            result["forecast"] = [
                round(math.sqrt(v), 6) for v in forecast_vars[1:]
            ]

        except Exception as e:
            logger.error(f"GARCH 波动率预测异常: {e}")

        return result

    # ──────────────────────────────────────────
    # 压力测试
    # ──────────────────────────────────────────

    def stress_test(self, code: str) -> Dict:
        """
        压力测试

        场景:
        - 大盘跌 -5%
        - 大盘跌 -10%
        - 个股跌停 (-10%)
        - 熔断 (-7%)

        Args:
            code: 股票代码

        Returns:
            {
                "code": "600519",
                "name": "贵州茅台",
                "current_price": 1800.0,
                "scenarios": [
                    {"name": "大盘跌5%", "expected_loss_pct": -0.05, "expected_loss_price": ..., "prob": "中"},
                    ...
                ],
                "max_loss_scenario": "熔断(-7%)",
                "max_loss_pct": -0.07,
                "volatility_context": 0.25,
            }
        """
        result = {
            "code": code,
            "name": "",
            "current_price": None,
            "scenarios": [],
            "max_loss_scenario": "",
            "max_loss_pct": None,
            "volatility_context": None,
        }

        try:
            # 获取名称和当前价格
            try:
                from analyzer.ner_extractor import A_SHARE_STOCKS
                result["name"] = A_SHARE_STOCKS.get(code, "")
            except Exception:
                pass

            current_price = self._get_latest_price(code)
            result["current_price"] = current_price

            # 获取波动率作为上下文
            volatility = self.calculate_volatility(code, 60)
            result["volatility_context"] = volatility

            # 获取 Beta（简化为使用波动率对比估计）
            # 使用当前个股波动率 vs 市场平均波动率（假设20%）作为 Beta 代理
            beta_estimate = 1.0
            if volatility and volatility > 0:
                market_vol = 0.20  # 假设市场年化波动率20%
                beta_estimate = volatility / market_vol

            # 定义测试场景
            scenarios = [
                {
                    "name": "大盘跌5%",
                    "market_change": -0.05,
                    "shock_type": "系统性",
                    "prob": "中",
                    "description": "市场整体回调5%，受宏观因素驱动",
                },
                {
                    "name": "大盘跌10%",
                    "market_change": -0.10,
                    "shock_type": "系统性",
                    "prob": "低",
                    "description": "市场大幅下挫10%，金融危机级别",
                },
                {
                    "name": "熔断(-7%)",
                    "market_change": -0.07,
                    "shock_type": "系统性",
                    "prob": "低",
                    "description": "触发熔断机制，市场恐慌情绪蔓延",
                },
                {
                    "name": "个股跌停(-10%)",
                    "market_change": -0.10,
                    "shock_type": "个股",
                    "prob": "中低",
                    "description": "个股因利空消息跌停",
                },
                {
                    "name": "板块回调(-8%)",
                    "market_change": -0.08 * beta_estimate,
                    "shock_type": "板块",
                    "prob": "中",
                    "description": "所属板块因政策或行业利空回调",
                },
                {
                    "name": "流动性冲击(-15%)",
                    "market_change": -0.15,
                    "shock_type": "极端",
                    "prob": "极低",
                    "description": "极端流动性危机，连续跌停",
                },
            ]

            for sc in scenarios:
                change_pct = sc["market_change"]
                expected_loss_pct = change_pct  # 简单模型：个股跟随市场

                # 如果有个股特定场景，加大损失
                if sc["shock_type"] == "个股":
                    expected_loss_pct = -0.10  # 跌停
                elif sc["shock_type"] == "极端":
                    expected_loss_pct = -0.15  # 极端情况

                expected_loss_price = None
                if current_price and current_price > 0:
                    expected_loss_price = round(current_price * (1 + expected_loss_pct), 2)

                # 根据波动率调整
                if volatility and volatility > 0.40 and sc["prob"] != "极低":
                    sc["prob"] = {
                        "低": "中低",
                        "中低": "中",
                        "中": "中高",
                    }.get(sc["prob"], "中")
                    expected_loss_pct *= 1.2  # 高波动下损失放大

                result["scenarios"].append({
                    "name": sc["name"],
                    "expected_loss_pct": round(expected_loss_pct, 4),
                    "expected_loss_price": expected_loss_price,
                    "prob": sc["prob"],
                    "description": sc["description"],
                    "shock_type": sc["shock_type"],
                })

            # 找出最大损失场景
            max_loss = None
            max_loss_name = ""
            for sc in result["scenarios"]:
                if max_loss is None or sc["expected_loss_pct"] < max_loss:
                    max_loss = sc["expected_loss_pct"]
                    max_loss_name = sc["name"]

            result["max_loss_scenario"] = max_loss_name
            result["max_loss_pct"] = max_loss

            # 添加风险评估
            if volatility:
                vol_pct = volatility * 100
                if vol_pct < 25:
                    result["stress_risk"] = "低"
                elif vol_pct < 40:
                    result["stress_risk"] = "中"
                elif vol_pct < 60:
                    result["stress_risk"] = "高"
                else:
                    result["stress_risk"] = "极高"
            else:
                result["stress_risk"] = "未知"

        except Exception as e:
            logger.error(f"压力测试异常 ({code}): {e}")

        return result

    # ──────────────────────────────────────────
    # 辅助方法
    # ──────────────────────────────────────────

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
        except Exception as e:
            logger.warning(f"获取 {code} 价格异常: {e}")
            return None

    # ──────────────────────────────────────────
    # 止损策略建议
    # ──────────────────────────────────────────

    def suggest_stop_loss_strategy(self, code: str,
                                    entry_price: float) -> Dict:
        """
        建议止损策略

        综合波动率和价格位置，给出多种止损方案

        Args:
            code: 股票代码
            entry_price: 入场价格

        Returns:
            {
                "strategies": [
                    {
                        "name": "严格止损",
                        "stop_loss_price": 95.00,
                        "stop_loss_pct": 0.05,
                        "expected_loss": "5.0%",
                        "suitable_for": "短期交易",
                    },
                    ...
                ],
                "recommended": "严格止损",
                "volatility": 0.25,
            }
        """
        result: Dict[str, Any] = {
            "strategies": [],
            "recommended": "",
            "volatility": None,
        }

        try:
            volatility = self.calculate_volatility(code, 20)
            result["volatility"] = volatility

            if volatility is None:
                # 无波动率数据，使用固定策略
                result["strategies"] = [
                    {
                        "name": "严格止损", "stop_loss_pct": 0.05,
                        "stop_loss_price": round(entry_price * 0.95, 2),
                        "expected_loss": "5.0%", "suitable_for": "短期交易",
                    },
                    {
                        "name": "标准止损", "stop_loss_pct": 0.08,
                        "stop_loss_price": round(entry_price * 0.92, 2),
                        "expected_loss": "8.0%", "suitable_for": "波段交易",
                    },
                    {
                        "name": "宽松止损", "stop_loss_pct": 0.12,
                        "stop_loss_price": round(entry_price * 0.88, 2),
                        "expected_loss": "12.0%", "suitable_for": "趋势交易",
                    },
                ]
                result["recommended"] = "标准止损"
                return result

            vol = volatility * 100

            # 基于波动率生成策略
            strategies = []
            # 严格止损：0.5倍波动率（适合短线）
            tight_pct = max(vol * 0.5 / 100, 0.03)
            strategies.append({
                "name": "严格止损",
                "stop_loss_pct": round(tight_pct, 3),
                "stop_loss_price": round(entry_price * (1 - tight_pct), 2),
                "expected_loss": f"{tight_pct*100:.1f}%",
                "suitable_for": "短期交易",
            })

            # 标准止损：0.8倍波动率（适合波段）
            mid_pct = max(vol * 0.8 / 100, 0.05)
            strategies.append({
                "name": "标准止损",
                "stop_loss_pct": round(mid_pct, 3),
                "stop_loss_price": round(entry_price * (1 - mid_pct), 2),
                "expected_loss": f"{mid_pct*100:.1f}%",
                "suitable_for": "波段交易",
            })

            # 宽松止损：1.2倍波动率（适合趋势）
            loose_pct = max(vol * 1.2 / 100, 0.08)
            strategies.append({
                "name": "宽松止损",
                "stop_loss_pct": round(loose_pct, 3),
                "stop_loss_price": round(entry_price * (1 - loose_pct), 2),
                "expected_loss": f"{loose_pct*100:.1f}%",
                "suitable_for": "趋势交易",
            })

            # ATR止损：基于平均真实波幅
            prices = self._get_price_series(code, max_points=25)
            if len(prices) >= 15:
                atr_changes = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
                atr = sum(atr_changes[-14:]) / 14 if len(atr_changes) >= 14 else None
                if atr and atr > 0:
                    atr_stop_pct = atr * 2 / entry_price
                    strategies.append({
                        "name": "ATR止损",
                        "stop_loss_pct": round(atr_stop_pct, 3),
                        "stop_loss_price": round(entry_price * (1 - atr_stop_pct), 2),
                        "expected_loss": f"{atr_stop_pct*100:.1f}%",
                        "suitable_for": "技术交易",
                    })

            result["strategies"] = strategies
            result["recommended"] = "标准止损"

        except Exception as e:
            logger.error(f"止损策略建议异常 ({code}): {e}")

        return result


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    rc = RiskController()

    # 测试波动率
    test_prices = [100.0]
    import random
    random.seed(42)
    for i in range(1, 60):
        test_prices.append(test_prices[-1] * (1 + random.uniform(-0.04, 0.04)))

    class MockDB:
        def _connect(self):
            import sqlite3
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE market_snapshots (
                    stock_code TEXT,
                    price REAL,
                    snapshot_time DATETIME
                )
            """)
            from datetime import datetime, timedelta
            for i, p in enumerate(test_prices):
                t = (datetime.now() - timedelta(minutes=30*(len(test_prices)-i))).strftime("%Y-%m-%d %H:%M:%S")
                conn.execute(
                    "INSERT INTO market_snapshots(stock_code, price, snapshot_time) VALUES(?, ?, ?)",
                    ("000001", p, t)
                )
            conn.commit()
            return conn

        def get_latest_market_snapshot(self, code):
            return {"price": test_prices[-1]}

        def get_latest_money_flow(self, code):
            return {"main_net": 5000000, "total_amount": 500000000}

    rc.db = MockDB()

    print("===== 波动率计算 =====")
    vol = rc.calculate_volatility("000001", 20)
    print(f"年化波动率: {vol:.4f} ({vol*100:.1f}%)")
    simple_vol = rc.calculate_simple_volatility("000001", 20)
    print(f"简化波动率: {simple_vol:.4f} ({simple_vol*100:.1f}%)")

    print("\n===== 仓位建议 =====")
    for vol_test in [0.15, 0.25, 0.30, 0.40, 0.55]:
        advice = rc.get_position_advice(vol_test)
        print(f"波动率 {vol_test*100:.0f}% → {advice['position_level']} "
              f"(仓位 {advice['suggested_position']:.0%}, 范围 {advice['range'][0]:.0%}-{advice['range'][1]:.0%})")

    print("\n===== 止损检查 =====")
    result = rc.check_stop_loss("000001", entry_price=105.0)
    print(f"入场价: 105.0")
    print(f"最新价: {result['current_price']:.2f}")
    print(f"止损价: {result['stop_loss_price']:.2f}")
    print(f"盈亏: {result['change_pct']*100:+.2f}%")
    print(f"状态: {result['status']}")
    print(f"建议: {result['advice']}")

    print("\n===== 综合风险评级 =====")
    risk = rc.get_risk_level("000001")
    print(f"风险等级: {risk['risk_level']}")
    print(f"风险评分: {risk['risk_score']}")
    print(f"各维度: {risk['components']}")
    print(f"建议: {risk['advice']}")

    print("\n===== 止损策略建议 =====")
    strategies = rc.suggest_stop_loss_strategy("000001", entry_price=100.0)
    print(f"推荐: {strategies['recommended']}")
    for s in strategies['strategies']:
        print(f"  {s['name']}: 止损 {s['stop_loss_price']} ({s['expected_loss']}) - 适合{s['suitable_for']}")
