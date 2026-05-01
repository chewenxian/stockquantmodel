"""
日报生成器
生成盘前早报和收盘晚报

格式：Markdown，适合推送到微信
时间：交易日 08:30（早报）/ 16:00（晚报）
"""
import logging
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    日报生成器
    生成格式化的 Market 日报
    """

    def __init__(self, stock_analyzer=None, nlp=None, db=None):
        self.analyzer = stock_analyzer
        self.nlp = nlp
        self.db = db
        self._init_components()

    def _init_components(self):
        if self.analyzer is None:
            try:
                from analyzer.stock_analyzer import StockAnalyzer
                self.analyzer = StockAnalyzer()
                # 复用 analyzer 中的组件引用
                self.nlp = self.analyzer.nlp
                self.db = self.analyzer.db
            except Exception as e:
                logger.warning(f"初始化 StockAnalyzer 失败: {e}")

        if self.nlp is None and hasattr(self, 'analyzer') and self.analyzer:
            self.nlp = self.analyzer.nlp

    # ──────────────────────────────────────────
    # 交易日判断
    # ──────────────────────────────────────────

    def is_trading_day(self, check_date: Optional[date] = None) -> bool:
        """
        判断是否为交易日（跳过周末，节假日判断需扩展）

        Args:
            check_date: 待检查日期

        Returns:
            bool: 是否为交易日
        """
        if check_date is None:
            check_date = date.today()

        # 跳过周末
        if check_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            return False

        # 简化版：中国法定节假日列表（主要节日）
        # 完整版需要接入交易日历 API
        simple_holidays = [
            # 元旦
            f"{check_date.year}-01-01",
            # 春节 (大年初一前后, 简化)
            # 清明
            f"{check_date.year}-04-05",
            # 劳动节
            f"{check_date.year}-05-01",
            # 端午
            # 中秋
            # 国庆
            f"{check_date.year}-10-01",
            f"{check_date.year}-10-02",
            f"{check_date.year}-10-03",
        ]

        date_str = check_date.isoformat()
        if date_str in simple_holidays:
            return False

        return True

    # ──────────────────────────────────────────
    # 盘前早报
    # ──────────────────────────────────────────

    def generate_morning_report(self) -> str:
        """
        生成盘前早报（交易日 08:30）

        内容：
        1. 整体市场情绪判断
        2. 重点个股盘前分析
        3. 昨日行情回顾
        4. 今日关注事件
        5. 操作策略建议

        Returns:
            Markdown 格式早报文本
        """
        today = date.today()
        yesterday = today - timedelta(days=1)

        if not self.is_trading_day(today):
            logger.info("非交易日，跳过早报生成")
            return ""

        logger.info(f"生成盘前早报 ({today.isoformat()})")

        # 获取分析结果
        if not self.analyzer:
            return self._fallback_report("morning")

        # 分析所有股票（用昨日数据）
        stock_analyses = self.analyzer.analyze_all_stocks()

        if not stock_analyses:
            return "今日暂无分析数据。"

        # 排序：按置信度降序排列
        stock_analyses.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        # 提取建议分布
        suggestions = {}
        for s in stock_analyses:
            sug = s.get("suggestion", "持有")
            suggestions[sug] = suggestions.get(sug, 0) + 1

        # 统计数据
        bullish_count = suggestions.get("强烈买入", 0) + suggestions.get("买入", 0)
        bearish_count = suggestions.get("强烈卖出", 0) + suggestions.get("卖出", 0)

        # 整体市场判断
        if bullish_count > bearish_count * 2:
            market_judgment = "🟢 偏多"
        elif bearish_count > bullish_count * 2:
            market_judgment = "🔴 偏空"
        else:
            market_judgment = "⚪ 中性"

        # 使用 LLM 生成早报
        try:
            if self.nlp:
                llm_report = self.nlp.generate_report(stock_analyses)
                if llm_report:
                    # 添加早报头信息
                    header = (
                        f"# 🌅 盘前早报\n"
                        f"**{today.strftime('%Y年%m月%d日')}  |  市场判断: {market_judgment}**\n\n"
                        f"---\n"
                    )
                    return header + llm_report
        except Exception as e:
            logger.warning(f"LLM 生成早报失败: {e}")

        return self._fallback_report("morning", stock_analyses, today)

    def _fallback_report(self, report_type: str,
                         stock_analyses: Optional[List[Dict]] = None,
                         report_date: Optional[date] = None) -> str:
        """日报生成的兜底方案"""
        if stock_analyses is None:
            stock_analyses = []
        if report_date is None:
            report_date = date.today()

        if report_type == "morning":
            title = "🌅 盘前早报"
        else:
            title = "📊 收盘晚报"

        lines = [
            f"# {title}\n",
            f"**{report_date.strftime('%Y年%m月%d日 %A')}**\n",
            "---\n",
        ]

        if not stock_analyses:
            lines.append("⚠️ 今日无分析数据\n")
            return "\n".join(lines)

        # 建议汇总
        suggestions = {}
        for s in stock_analyses:
            sug = s.get("suggestion", "持有")
            suggestions[sug] = suggestions.get(sug, 0) + 1

        lines.append("## 📊 建议汇总\n")
        suggestions_order = ["强烈买入", "买入", "持有", "观望", "卖出", "强烈卖出"]
        for sug in suggestions_order:
            count = suggestions.get(sug, 0)
            if count > 0:
                lines.append(f"- **{sug}**: {count} 只\n")
        lines.append("\n")

        # 个股分析
        lines.append("## 📰 个股分析\n")
        for sa in stock_analyses:
            name = sa.get("name", "未知")
            code = sa.get("code", "")
            sentiment = sa.get("avg_sentiment", 0)
            suggestion = sa.get("suggestion", "持有")
            confidence = sa.get("confidence", 0)

            sentiment_icon = "🟢" if sentiment > 0.3 else ("🔴" if sentiment < -0.3 else "⚪")
            lines.append(f"### {name} ({code}) {sentiment_icon}\n")
            lines.append(f"- **情绪**: {sentiment:.2f} | **建议**: {suggestion} | **置信度**: {confidence:.0%}\n")

            if sa.get("summary"):
                lines.append(f"- **分析**: {sa['summary'][:200]}\n")
            if sa.get("key_topics"):
                lines.append(f"- **热点**: {'、'.join(sa['key_topics'][:3])}\n")
            if sa.get("risk_warnings"):
                lines.append(f"- ⚠️ **风险**: {'；'.join(sa['risk_warnings'][:2])}\n")
            lines.append("\n")

        # 风险提示
        lines.append("---\n")
        lines.append("> ⚠️ **风险提示**: 以上分析基于公开信息和AI模型，仅供参考，不构成投资建议。投资有风险，入市需谨慎。\n")

        return "\n".join(lines)

    # ──────────────────────────────────────────
    # 收盘晚报
    # ──────────────────────────────────────────

    def generate_closing_report(self) -> str:
        """
        生成收盘晚报（交易日 16:00）

        内容：
        1. 今日整体市场回顾
        2. 重点个股收盘分析
        3. 资金流向分析
        4. 明日展望
        5. 操作建议汇总

        Returns:
            Markdown 格式晚报文本
        """
        today = date.today()

        if not self.is_trading_day(today):
            logger.info("非交易日，跳过晚报生成")
            return ""

        logger.info(f"生成收盘晚报 ({today.isoformat()})")

        # 获取分析结果
        if not self.analyzer:
            return self._fallback_report("closing")

        stock_analyses = self.analyzer.analyze_all_stocks()

        if not stock_analyses:
            return "今日暂无分析数据。"

        # 排序
        stock_analyses.sort(key=lambda x: abs(x.get("avg_sentiment", 0)), reverse=True)

        # 使用 LLM 生成晚报
        try:
            if self.nlp:
                llm_report = self.nlp.generate_report(stock_analyses)
                if llm_report:
                    header = (
                        f"# 📊 收盘晚报\n"
                        f"**{today.strftime('%Y年%m月%d日 %A')}**\n\n"
                        f"---\n"
                    )
                    return header + llm_report
        except Exception as e:
            logger.warning(f"LLM 生成晚报失败: {e}")

        return self._fallback_report("closing", stock_analyses, today)

    # ──────────────────────────────────────────
    # 导出
    # ──────────────────────────────────────────

    def save_report(self, content: str, report_type: str = "report",
                    output_dir: str = "output") -> Optional[str]:
        """
        保存日报到文件

        Args:
            content: 日报内容
            report_type: 报告类型 (morning/closing/report)
            output_dir: 输出目录

        Returns:
            保存的文件路径，失败返回 None
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        today = date.today().isoformat()
        type_map = {"morning": "早报", "closing": "晚报", "report": "日报"}
        type_name = type_map.get(report_type, "日报")

        filename = f"{today}_{type_name}.md"
        filepath = os.path.join(output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"报告已保存到 {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"保存报告失败: {e}")
            return None


# 测试用
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    gen = ReportGenerator()
    today = date.today()

    if gen.is_trading_day(today):
        print(f"{today} 是交易日，生成报告\n")

        report = gen.generate_morning_report()
        if report:
            print(report[:500])
            print("...\n")

        report = gen.generate_closing_report()
        if report:
            print(report[:500])
    else:
        print(f"{today} ({today.strftime('%A')}) 是非交易日")
