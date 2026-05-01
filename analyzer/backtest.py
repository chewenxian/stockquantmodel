"""
策略回测引擎
记录AI每天的买卖建议，在历史数据上验证有效性

功能：
- 记录AI日分析建议到 backtest_signals 表
- 基于历史行情的策略回测
- 绩效指标计算（收益率/最大回撤/胜率/夏普比率）
- 与基准对比（沪深300）
"""
import math
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 交易成本：单边万分之三（佣金+印花税简化）
TRANSACTION_COST = 0.0003

# 基准代码（沪深300）
BENCHMARK_CODE = "000300"


class BacktestEngine:
    """
    策略回测引擎

    记录AI每天的买卖建议，在历史数据上验证有效性。
    支持单只股票回测、全量回测、绩效汇总。
    """

    def __init__(self, db):
        """
        Args:
            db: Database 实例
        """
        self.db = db

    # ═══════════════════════════════════════════════
    # 信号记录
    # ═══════════════════════════════════════════════

    def record_signal(self, stock_code: str, signal_date: str,
                      suggestion: str, confidence: float = 0.0,
                      sentiment: float = 0.0) -> bool:
        """
        记录AI当天的分析建议到数据库

        Args:
            stock_code: 股票代码
            signal_date: 信号日期 (YYYY-MM-DD)
            suggestion: 建议 (买入/卖出/持有/观望/强烈买入/强烈卖出)
            confidence: 置信度 0~1
            sentiment: 综合情绪得分 -1~1

        Returns:
            bool: 是否成功
        """
        # 获取当日价格
        price = self._get_price_on_date(stock_code, signal_date)
        return self.db.record_backtest_signal(
            stock_code=stock_code,
            signal_date=signal_date,
            suggestion=suggestion,
            confidence=confidence,
            sentiment=sentiment,
            price_at_signal=price,
        )

    def record_today_signals(self) -> int:
        """
        从 analysis 表获取今天的分析结果，批量记录为回测信号

        Returns:
            int: 记录的信号数量
        """
        today = datetime.now().strftime("%Y-%m-%d")
        analyses = self.db.get_today_analysis()
        count = 0
        for a in analyses:
            ok = self.record_signal(
                stock_code=a["stock_code"],
                signal_date=today,
                suggestion=a.get("suggestion", "持有"),
                confidence=a.get("confidence", 0.0),
                sentiment=a.get("avg_sentiment", 0.0),
            )
            if ok:
                count += 1
        logger.info(f"已记录 {count} 条今日回测信号")
        return count

    # ═══════════════════════════════════════════════
    # 单只股票回测
    # ═══════════════════════════════════════════════

    def run_backtest(self, stock_code: str,
                     start_date: str = None,
                     end_date: str = None) -> Dict[str, Any]:
        """
        对某只股票跑回测

        策略规则：
        - 建议=买入/强烈买入 → 开仓（如果不在持仓中）
        - 建议=卖出/强烈卖出 → 平仓（如果在持仓中）
        - 建议=持有/观望 → 维持当前状态

        考虑交易成本（单边万分之三）
        持仓比例基于信号强度和风险等级

        Args:
            stock_code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)，默认90天前
            end_date: 结束日期 (YYYY-MM-DD)，默认今天

        Returns:
            {
                "total_return": 总收益率,
                "annual_return": 年化收益率,
                "max_drawdown": 最大回撤,
                "win_rate": 胜率,
                "trade_count": 交易次数,
                "sharpe_ratio": 夏普比率,
                "benchmark_return": 基准收益率,
                "alpha": 阿尔法,
                "trades": [...],
                "equity_curve": [...]
            }
        """
        # 默认时间范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start = datetime.now() - timedelta(days=90)
            start_date = start.strftime("%Y-%m-%d")

        # 获取回测信号
        signals = self.db.get_backtest_signals(stock_code, start_date, end_date)
        if not signals:
            logger.warning(f"{stock_code} 在 {start_date}~{end_date} 无回测信号")
            return self._default_result(stock_code, start_date, end_date)

        # 获取价格历史
        prices = self.db.get_price_history(stock_code, start_date, end_date)
        price_map = {p["trade_date"]: p["price"] for p in prices}

        # 运行模拟交易
        trades = []
        equity_curve = []
        position = 0.0  # 当前持仓比例
        cash = 1.0  # 初始资金归一化
        capital = 1.0  # 总资产（现金+持仓市值）
        prev_capital = 1.0
        max_capital = 1.0
        max_drawdown = 0.0
        trade_count = 0
        wins = 0
        daily_returns = []

        for signal in signals:
            s_date = signal["signal_date"]
            if s_date not in price_map:
                continue

            price = price_map[s_date]
            suggestion = signal.get("suggestion", "持有")
            confidence = signal.get("confidence", 0.5) or 0.5

            # 确定仓位比例
            position_ratio = self._calc_position_ratio(suggestion, confidence)

            # 执行交易
            if position_ratio > 0 and position <= 0:
                # 开仓
                cost = position_ratio * TRANSACTION_COST
                position = position_ratio - cost
                cash -= cost
                trade_count += 1
                trades.append({
                    "date": s_date,
                    "type": "买入",
                    "price": price,
                    "position": position,
                    "capital": capital,
                })
            elif position_ratio <= 0 and position > 0:
                # 平仓
                cost = position * TRANSACTION_COST
                position = 0
                cash -= cost
                trades.append({
                    "date": s_date,
                    "type": "卖出",
                    "price": price,
                    "position": 0,
                    "capital": capital,
                })

            # 计算当日资产
            if position > 0:
                capital = cash + position * (price / self._get_reference_price(s_date, price_map))
            else:
                capital = cash

            # 日收益率
            if prev_capital > 0:
                daily_return = (capital - prev_capital) / prev_capital
                daily_returns.append(daily_return)

            # 更新资金曲线和最大回撤
            equity_curve.append({
                "date": s_date,
                "capital": capital,
            })
            if capital > max_capital:
                max_capital = capital
            drawdown = (max_capital - capital) / max_capital if max_capital > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown

            prev_capital = capital

        # 统计胜率
        for i in range(1, len(trades)):
            if trades[i]["type"] == "卖出" and trades[i - 1]["type"] == "买入":
                if trades[i]["capital"] > trades[i - 1]["capital"]:
                    wins += 1
        win_rate = wins / max(trade_count, 1)

        # 计算总收益
        total_return = capital - 1.0

        # 年化收益率
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days = max((end_dt - start_dt).days, 1)
            annual_return = (1 + total_return) ** (365 / days) - 1 if total_return > -1 else -0.999
        except Exception:
            annual_return = 0.0

        # 夏普比率
        sharpe_ratio = self._calc_sharpe_ratio(daily_returns)

        # 基准对比
        benchmark_return = 0.0
        alpha = 0.0
        try:
            benchmark_data = self._get_benchmark_return(start_date, end_date)
            benchmark_return = benchmark_data.get("return", 0)
            alpha = total_return - benchmark_return
        except Exception:
            pass

        result = {
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
            "total_return": round(total_return, 4),
            "annual_return": round(annual_return, 4),
            "max_drawdown": round(max_drawdown, 4),
            "win_rate": round(win_rate, 4),
            "trade_count": trade_count,
            "sharpe_ratio": round(sharpe_ratio, 4),
            "benchmark_return": round(benchmark_return, 4),
            "alpha": round(alpha, 4),
            "trades": trades,
            "equity_curve": equity_curve,
        }

        # 保存结果到数据库
        self.db.save_backtest_result(stock_code, start_date, end_date, result)

        return result

    def _calc_position_ratio(self, suggestion: str, confidence: float) -> float:
        """
        根据建议和置信度计算持仓比例

        Args:
            suggestion: 建议
            confidence: 置信度 0~1

        Returns:
            float: 持仓比例 0~1（负数表示空仓）
        """
        suggestion = suggestion.strip() if suggestion else "持有"

        base_ratios = {
            "强烈买入": 1.0,
            "买入": 0.7,
            "关注": 0.5,
            "强烈关注": 0.8,
            "持有": 0.3,
            "观望": 0.0,
            "卖出": 0.0,
            "强烈卖出": 0.0,
            "回避": 0.0,
            "强烈回避": 0.0,
        }
        # 反向匹配（兼容中英文混用或其他变体）
        matched = False
        for key, ratio in base_ratios.items():
            if key in suggestion:
                base = ratio
                matched = True
                break
        if not matched:
            base = 0.0

        # 置信度修正
        adjusted = base * (0.5 + 0.5 * confidence)

        # 卖出类建议返回负值（空仓信号）
        sell_keywords = ["卖出", "回避"]
        for kw in sell_keywords:
            if kw in suggestion:
                return 0.0  # 空仓

        return max(0.0, min(1.0, adjusted))

    def _get_reference_price(self, signal_date: str,
                              price_map: Dict[str, float]) -> float:
        """获取开仓参考价格（持仓期间的最新价格）"""
        # 简单实现：使用信号日的价格
        return price_map.get(signal_date, 1.0)

    def _get_price_on_date(self, stock_code: str,
                            signal_date: str) -> Optional[float]:
        """获取某只股票在特定日期的价格"""
        try:
            prices = self.db.get_price_history(stock_code, signal_date, signal_date)
            if prices:
                return prices[0]["price"]
        except Exception:
            pass
        return None

    def _calc_sharpe_ratio(self, daily_returns: List[float]) -> float:
        """
        计算夏普比率

        假设无风险利率为 2%（年化），转换为日化

        Args:
            daily_returns: 日收益率列表

        Returns:
            float: 夏普比率
        """
        if len(daily_returns) < 2:
            return 0.0

        n = len(daily_returns)
        avg_return = sum(daily_returns) / n

        # 计算标准差
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / (n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0001

        # 年化无风险利率 2% → 日化
        risk_free_daily = 0.02 / 252

        # 日夏普
        daily_sharpe = (avg_return - risk_free_daily) / std if std > 0 else 0

        # 年化夏普
        return daily_sharpe * math.sqrt(252)

    def _get_benchmark_return(self, start_date: str, end_date: str) -> Dict:
        """
        获取基准指数（沪深300）在回测期间的收益率

        Returns:
            {"return": 收益率}
        """
        try:
            benchmark_prices = self.db.get_price_history(
                BENCHMARK_CODE, start_date, end_date
            )
            if benchmark_prices and len(benchmark_prices) >= 2:
                first = benchmark_prices[0]["price"]
                last = benchmark_prices[-1]["price"]
                if first and first > 0:
                    bm_return = (last - first) / first
                else:
                    bm_return = 0.0
            else:
                bm_return = 0.0
            return {"return": bm_return}
        except Exception:
            return {"return": 0.0}

    def _default_result(self, stock_code: str, start_date: str,
                        end_date: str) -> Dict:
        """数据不足时返回合理默认值"""
        return {
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
            "total_return": 0.0,
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "sharpe_ratio": 0.0,
            "benchmark_return": 0.0,
            "alpha": 0.0,
            "trades": [],
            "equity_curve": [],
        }

    # ═══════════════════════════════════════════════
    # 全量回测
    # ═══════════════════════════════════════════════

    def run_all_backtest(self, start_date: str = None,
                         end_date: str = None) -> List[Dict]:
        """
        对所有有信号的股票跑回测

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            所有股票的回测结果列表
        """
        # 获取所有有信号的股票
        signals = self.db.get_all_backtest_signals()
        stock_codes = set(s["stock_code"] for s in signals)

        results = []
        for code in sorted(stock_codes):
            try:
                logger.info(f"正在回测 {code}...")
                result = self.run_backtest(code, start_date, end_date)
                results.append(result)
            except Exception as e:
                logger.error(f"回测失败 {code}: {e}")
                results.append(self._default_result(code,
                                                     start_date or "",
                                                     end_date or ""))

        # 按总收益排序
        results.sort(key=lambda r: r.get("total_return", 0), reverse=True)
        return results

    # ═══════════════════════════════════════════════
    # 绩效汇总
    # ═══════════════════════════════════════════════

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        回测绩效汇总

        Returns:
            {
                "total_stocks": 回测股票数,
                "avg_return": 平均收益率,
                "avg_annual_return": 平均年化收益率,
                "avg_max_drawdown": 平均最大回撤,
                "avg_win_rate": 平均胜率,
                "avg_sharpe": 平均夏普比率,
                "best_stock": 最佳表现股票,
                "worst_stock": 最差表现股票,
                "positive_count": 正收益股票数,
                "positive_percent": 正收益比例,
            }
        """
        results = self.db.get_backtest_results()
        if not results:
            return {
                "total_stocks": 0,
                "avg_return": 0,
                "avg_annual_return": 0,
                "avg_max_drawdown": 0,
                "avg_win_rate": 0,
                "avg_sharpe": 0,
                "positive_count": 0,
                "positive_percent": 0,
                "best_stock": None,
                "worst_stock": None,
            }

        n = len(results)
        total_return_vals = [r.get("total_return", 0) or 0 for r in results]
        annual_vals = [r.get("annual_return", 0) or 0 for r in results]
        drawdown_vals = [r.get("max_drawdown", 0) or 0 for r in results]
        winrate_vals = [r.get("win_rate", 0) or 0 for r in results]
        sharpe_vals = [r.get("sharpe_ratio", 0) or 0 for r in results]

        positive = sum(1 for v in total_return_vals if v > 0)

        best = max(results, key=lambda r: r.get("total_return", 0))
        worst = min(results, key=lambda r: r.get("total_return", 0))

        return {
            "total_stocks": n,
            "avg_return": round(sum(total_return_vals) / n, 4),
            "avg_annual_return": round(sum(annual_vals) / n, 4),
            "avg_max_drawdown": round(sum(drawdown_vals) / n, 4),
            "avg_win_rate": round(sum(winrate_vals) / n, 4),
            "avg_sharpe": round(sum(sharpe_vals) / n, 4),
            "positive_count": positive,
            "positive_percent": round(positive / n, 4),
            "best_stock": {
                "code": best.get("stock_code"),
                "name": best.get("stock_name", ""),
                "return": best.get("total_return", 0),
            },
            "worst_stock": {
                "code": worst.get("stock_code"),
                "name": worst.get("stock_name", ""),
                "return": worst.get("total_return", 0),
            },
        }

    # ═══════════════════════════════════════════════
    # 基准对比
    # ═══════════════════════════════════════════════

    def compare_with_benchmark(self, stock_code: str,
                                start_date: str = None,
                                end_date: str = None) -> Dict[str, Any]:
        """
        与基准（沪深300）对比

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            {
                "stock_code": ...,
                "stock_return": 策略收益率,
                "benchmark_return": 基准收益率,
                "excess_return": 超额收益,
                "alpha": 阿尔法,
                "stock_equity_curve": [...],
                "benchmark_equity_curve": [...],
            }
        """
        # 跑一次回测
        result = self.run_backtest(stock_code, start_date, end_date)

        # 获取基准价格数据
        s_date = result.get("start_date", start_date)
        e_date = result.get("end_date", end_date)
        benchmark_prices = self.db.get_price_history(BENCHMARK_CODE, s_date, e_date)

        # 构建基准资金曲线
        benchmark_curve = []
        if benchmark_prices and len(benchmark_prices) >= 2:
            base_price = benchmark_prices[0]["price"]
            for bp in benchmark_prices:
                if base_price > 0:
                    norm = bp["price"] / base_price
                    benchmark_curve.append({
                        "date": bp["trade_date"],
                        "capital": norm,
                    })

        return {
            "stock_code": stock_code,
            "stock_return": result.get("total_return", 0),
            "benchmark_return": result.get("benchmark_return", 0),
            "excess_return": round(
                result.get("total_return", 0) - result.get("benchmark_return", 0), 4
            ),
            "alpha": result.get("alpha", 0),
            "stock_equity_curve": result.get("equity_curve", []),
            "benchmark_equity_curve": benchmark_curve,
        }
