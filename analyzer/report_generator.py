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
    # 午间速报
    # ──────────────────────────────────────────

    def generate_midday_report(self) -> str:
        """
        生成午间速报（交易日 12:00）

        内容：
        1. 上午涨跌分布
        2. 半日热点个股
        3. 下午关注

        Returns:
            Markdown 格式午报文本
        """
        today = date.today()
        if not self.is_trading_day(today):
            logger.info("非交易日，跳过午报")
            return ""

        logger.info(f"生成午间速报 ({today.isoformat()})")

        if not self.analyzer:
            return self._fallback_report("closing")  # 复用兜底模板

        stock_analyses = self.analyzer.analyze_all_stocks()
        if not stock_analyses:
            return "暂无分析数据。"

        stock_analyses.sort(key=lambda x: abs(x.get("avg_sentiment", 0)), reverse=True)

        today_str = today.strftime("%Y年%m月%d日")
        lines = [
            f"# ☀️ 午间速报 | {today_str}\n",
        ]

        # 建议分布
        suggestions = {}
        for s in stock_analyses:
            sug = s.get("suggestion", "持有")
            suggestions[sug] = suggestions.get(sug, 0) + 1

        bullish = suggestions.get("强烈买入", 0) + suggestions.get("买入", 0) + suggestions.get("关注", 0)
        bearish = suggestions.get("强烈卖出", 0) + suggestions.get("卖出", 0) + suggestions.get("回避", 0)

        lines.append(f"📈 **看多**: {bullish}只  |  📉 **看空**: {bearish}只  |  ⚪ **中性**: {len(stock_analyses)-bullish-bearish}只\n")
        lines.append("\n")

        # 半日热点（情绪最强前5）
        top_bullish = [s for s in stock_analyses if s.get("avg_sentiment", 0) > 0.1][:5]
        if top_bullish:
            lines.append("## 🟢 半日强势股\n")
            for s in top_bullish:
                name = s.get("name", "")
                code = s.get("code", "")
                sentiment = s.get("avg_sentiment", 0)
                suggestion = s.get("suggestion", "")
                lines.append(f"- **{name}**({code}) | 情绪: {sentiment:.2f} | {suggestion}\n")
            lines.append("\n")

        # 半日弱势（情绪最弱前5）
        top_bearish = [s for s in stock_analyses if s.get("avg_sentiment", 0) < -0.1][-5:]
        top_bearish.reverse()
        if top_bearish:
            lines.append("## 🔴 半日弱势股\n")
            for s in top_bearish:
                name = s.get("name", "")
                code = s.get("code", "")
                sentiment = s.get("avg_sentiment", 0)
                suggestion = s.get("suggestion", "")
                lines.append(f"- **{name}**({code}) | 情绪: {sentiment:.2f} | {suggestion}\n")
            lines.append("\n")

        # 置信度最高
        top_confidence = sorted(stock_analyses, key=lambda x: x.get("confidence", 0), reverse=True)[:3]
        lines.append("## 🎯 高置信度信号\n")
        for s in top_confidence:
            name = s.get("name", "")
            code = s.get("code", "")
            confidence = s.get("confidence", 0)
            suggestion = s.get("suggestion", "")
            lines.append(f"- **{name}**({code}) | 置信度: {confidence:.0%} | {suggestion}\n")
        lines.append("\n---\n")
        lines.append(f"*⏰ {datetime.now().strftime('%H:%M')} · 午间速报*\n")

        return "".join(lines)

    # ──────────────────────────────────────────
    # 收盘复盘 + 次日展望
    # ──────────────────────────────────────────

    def generate_closing_with_outlook(self) -> str:
        """
        生成收盘复盘 + 次日板块/个股展望

        内容：
        1. 今日整体回顾
        2. 板块强弱分析（通过知识图谱按行业分组）
        3. 重点个股分析
        4. 次日走强/走弱预判
        5. 操作策略

        Returns:
            Markdown 格式报告
        """
        today = date.today()
        if not self.is_trading_day(today):
            logger.info("非交易日，跳过收盘复盘")
            return ""

        logger.info(f"生成收盘复盘+次日展望 ({today.isoformat()})")

        if not self.analyzer:
            return self._fallback_report("closing")

        stock_analyses = self.analyzer.analyze_all_stocks()
        if not stock_analyses:
            return "今日暂无分析数据。"

        today_str = today.strftime("%Y年%m月%d日")

        # ── 1. 按行业分组 ──
        try:
            from analyzer.knowledge_graph import FinancialKnowledgeGraph
            kg = FinancialKnowledgeGraph()
            # sectors: 从 stocks.csv 的 industry 字段读取
        except Exception:
            kg = None

        # 获取每只股票的行业（从数据库读）
        stock_industry = {}
        if self.db:
            try:
                conn = self.db._connect()
                rows = conn.execute("SELECT code, industry FROM stocks").fetchall()
                for r in rows:
                    ind = r["industry"] or ""
                    if ind:
                        stock_industry[r["code"]] = ind
                conn.close()
            except Exception:
                pass

        # 按行业分组统计
        sector_groups = {}  # sector -> {stocks, avg_sentiment, bullish_count, bearish_count}
        for s in stock_analyses:
            code = s.get("code", "")
            industry = stock_industry.get(code, "其他")
            sentiment = s.get("avg_sentiment", 0) or 0
            suggestion = s.get("suggestion", "持有")
            confidence = s.get("confidence", 0) or 0

            if industry not in sector_groups:
                sector_groups[industry] = {
                    "stocks": [],
                    "sentiments": [],
                    "bullish": 0,
                    "bearish": 0,
                    "neutral": 0,
                }
            g = sector_groups[industry]
            g["stocks"].append(s)
            g["sentiments"].append(sentiment)
            if suggestion in ("强烈买入", "买入", "关注"):
                g["bullish"] += 1
            elif suggestion in ("强烈卖出", "卖出", "回避"):
                g["bearish"] += 1
            else:
                g["neutral"] += 1

        # 计算各行业平均情绪
        sector_stats = []
        for name, g in sector_groups.items():
            avg_s = sum(g["sentiments"]) / max(len(g["sentiments"]), 1)
            total = g["bullish"] + g["bearish"] + g["neutral"]
            sector_stats.append({
                "name": name,
                "avg_sentiment": avg_s,
                "count": len(g["stocks"]),
                "bullish_ratio": g["bullish"] / max(total, 1),
                "bearish_ratio": g["bearish"] / max(total, 1),
                "stocks": g["stocks"],
            })

        # 排序：情绪从高到低
        sector_stats.sort(key=lambda x: x["avg_sentiment"], reverse=True)

        # ── 构建报告 ──
        lines = [
            f"# 📊 收盘复盘 | {today_str}\n",
            f"---\n",
        ]

        # 整体情绪
        all_sentiments = [s.get("avg_sentiment", 0) or 0 for s in stock_analyses]
        avg_all = sum(all_sentiments) / max(len(all_sentiments), 1)
        market_icon = "🟢" if avg_all > 0.1 else ("🔴" if avg_all < -0.1 else "⚪")
        total = len(stock_analyses)
        bullish_count = sum(1 for s in stock_analyses if s.get("avg_sentiment", 0) > 0.1)
        bearish_count = sum(1 for s in stock_analyses if s.get("avg_sentiment", 0) < -0.1)
        lines.append(f"**整体情绪**: {market_icon} {avg_all:.2f}  |  🟢 偏多: {bullish_count}只  |  🔴 偏空: {bearish_count}只  |  总数: {total}只\n")
        lines.append("\n")

        # ── 2. 板块强弱 ──
        strong_sectors = [s for s in sector_stats if s["avg_sentiment"] > 0.05][:5]
        weak_sectors = [s for s in sector_stats if s["avg_sentiment"] < -0.05][-5:]
        weak_sectors.reverse()

        if strong_sectors:
            lines.append("## 🟢 今日强势板块\n")
            for st in strong_sectors:
                lines.append(f"- **{st['name']}** | 情绪: {st['avg_sentiment']:.2f} | 看多率: {st['bullish_ratio']:.0%} ({st['count']}只)\n")
            lines.append("\n")

        if weak_sectors:
            lines.append("## 🔴 今日弱势板块\n")
            for st in weak_sectors:
                lines.append(f"- **{st['name']}** | 情绪: {st['avg_sentiment']:.2f} | 看空率: {st['bearish_ratio']:.0%} ({st['count']}只)\n")
            lines.append("\n")

        # ── 3. 重点个股 ──
        # 按置信度排序前5
        top_sorted = sorted(stock_analyses, key=lambda x: x.get("confidence", 0), reverse=True)[:5]
        lines.append("## 📈 重点个股\n")
        for s in top_sorted:
            name = s.get("name", "")
            code = s.get("code", "")
            sentiment = s.get("avg_sentiment", 0)
            suggestion = s.get("suggestion", "")
            confidence = s.get("confidence", 0)
            icon = "🟢" if sentiment > 0.1 else ("🔴" if sentiment < -0.1 else "⚪")
            lines.append(f"- {icon} **{name}**({code}) | 情绪: {sentiment:.2f} | {suggestion} | 置信度: {confidence:.0%}\n")
            summary = (s.get("summary") or "")[:80]
            if summary:
                lines.append(f"  └ {summary}\n")
        lines.append("\n")

        # ── 4. 次日走强/走弱预判 ──
        lines.append("## 🔮 次日展望\n")

        # 走强板块：今日强势 + 看多率高
        outlook_strong = strong_sectors[:3]
        if outlook_strong:
            lines.append("### ✅ 预计走强\n")
            for st in outlook_strong:
                # 从该板块挑置信度最高的股票
                top_stocks = sorted(st["stocks"], key=lambda x: x.get("confidence", 0), reverse=True)[:2]
                stock_strs = [f"{s.get('name','')}({s.get('code','')})" for s in top_stocks]
                lines.append(f"- **{st['name']}** → {'、'.join(stock_strs)}\n")
            lines.append("\n")

        # 走弱板块：今日弱势 + 看空率高
        outlook_weak = weak_sectors[:3]
        if outlook_weak:
            lines.append("### ⚠️ 预计走弱\n")
            for st in outlook_weak:
                top_stocks = sorted(st["stocks"], key=lambda x: x.get("sentiment", 0))[:2]
                stock_strs = [f"{s.get('name','')}({s.get('code','')})" for s in top_stocks]
                lines.append(f"- **{st['name']}** → {'、'.join(stock_strs)}\n")
            lines.append("\n")

        # 操作策略
        lines.append("### 📋 操作策略\n")
        if avg_all > 0.1:
            lines.append("市场情绪偏暖，可关注强势板块回调机会。\n")
        elif avg_all < -0.1:
            lines.append("市场情绪偏弱，建议控制仓位、观望为主。\n")
        else:
            lines.append("市场情绪中性，结构性机会为主，精选个股。\n")
        lines.append("\n---\n")
        lines.append(f"*⏰ {datetime.now().strftime('%H:%M')} · 次日展望由 AI 生成，仅供参考*\n")

        return "".join(lines)

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
