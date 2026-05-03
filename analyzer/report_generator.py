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

    def _get_market_data(self) -> dict:
        """获取市场数据（行情、板块、资金流向、新闻）"""
        data = {"quotes": [], "boards": [], "news": [], "announcements": [], "money_flow": []}
        if not self.db:
            return data
        try:
            conn = self.db._connect()
            # 今日行情
            rows = conn.execute("""
                SELECT s.code, s.name, m.price, m.change_pct, m.pe, m.pb, m.total_mv
                FROM market_snapshots m
                JOIN stocks s ON s.code = m.stock_code
                WHERE m.snapshot_time >= date('now')
                ORDER BY m.change_pct DESC
            """).fetchall()
            data["quotes"] = [dict(r) for r in rows]

            # 板块排行
            rows = conn.execute("""
                SELECT board_name, board_type, change_pct, up_count, down_count
                FROM board_rankings WHERE date = date('now')
                ORDER BY board_type, change_pct DESC
            """).fetchall()
            data["boards"] = [dict(r) for r in rows]

            # 今日新闻
            rows = conn.execute("""
                SELECT title, source FROM news
                WHERE date(collected_at) = date('now')
                ORDER BY id DESC LIMIT 20
            """).fetchall()
            data["news"] = [dict(r) for r in rows]

            # 今日公告
            rows = conn.execute("""
                SELECT stock_code, title, announce_type FROM announcements
                WHERE date(collected_at) = date('now')
                ORDER BY id DESC LIMIT 10
            """).fetchall()
            data["announcements"] = [dict(r) for r in rows]

            # 资金流向
            rows = conn.execute("""
                SELECT stock_code, main_net, retail_net, north_net
                FROM money_flow WHERE date = date('now')
                ORDER BY main_net DESC LIMIT 10
            """).fetchall()
            data["money_flow"] = [dict(r) for r in rows]

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"获取市场数据异常: {e}")
        return data

    def _build_report_prompt(self, report_type: str, stock_analyses: list, market: dict) -> str:
        """构建不同报告类型的 LLM 提示"""
        today_str = date.today().strftime('%Y年%m月%d日')

        # 行情摘要
        quotes_top = market["quotes"][:10] if market["quotes"] else []
        quotes_str = "\n".join([
            f"{q['name']}({q['code']}) 价格={q['price']} 涨跌={q['change_pct']}% PE={q['pe']}"
            for q in quotes_top
        ]) if quotes_top else "暂无"

        # 板块摘要
        boards_str = "\n".join([
            f"{b['board_name']}({b['board_type']}) {b['change_pct']}%"
            for b in market["boards"][:10]
        ]) if market["boards"] else "暂无"

        # 新闻摘要
        news_str = "\n".join([
            f"[{n['source']}] {n['title'][:60]}"
            for n in market["news"][:10]
        ]) if market["news"] else "暂无"

        # 公告摘要
        ann_str = "\n".join([
            f"{a['stock_code']}: {a['title'][:50]} [{a['announce_type']}]"
            for a in market["announcements"][:5]
        ]) if market["announcements"] else "暂无"

        # 资金流向
        flow_str = "\n".join([
            f"{f['stock_code']} 主力净流入={f['main_net']}亿"
            for f in market["money_flow"][:5]
        ]) if market["money_flow"] else "暂无"

        # 个股分析摘要
        analysis_str = "\n".join([
            f"{s.get('name','')}({s.get('code','')}) 情绪={s.get('avg_sentiment',0):.2f} 建议={s.get('suggestion','持有')} 置信度={s.get('confidence',0):.0%}"
            for s in stock_analyses[:10]
        ]) if stock_analyses else "暂无"

        if report_type == "morning":
            prompt = f"""你是一位资深的A股券商晨报分析师。请基于以下市场数据，生成{ today_str }的盘前早报。

## 隔夜消息/最新新闻
{news_str}

## 公司公告
{ann_str}

## 昨日行情回顾
{quotes_str}

## 板块表现
{boards_str}

## 资金流向
{flow_str}

## AI分析结果
{analysis_str}

请生成Markdown格式的早报，要求：
1. 📰 **隔夜要闻速览** — 最重要的隔夜消息摘要
2. 📊 **昨日复盘** — 昨日行情总览，涨跌分布
3. 🏆 **今日关注板块** — 重点关注的行业/概念板块
4. 🎯 **今日关注个股** — 结合隔夜消息和AI分析，推荐今日重点关注个股及理由
5. ⚠️ **风险提示**

风格：专业、简洁，类似券商晨报。字数控制在800字以内。"""

        elif report_type == "midday":
            prompt = f"""你是一位资深的A股交易员。请基于以下半日数据，生成{ today_str }的午间速报。

## 上午行情表现
{quotes_str}

## 上午板块排行
{boards_str}

## 资金流向
{flow_str}

## 相关新闻
{news_str}

## AI分析结果
{analysis_str}

请生成Markdown格式的午间速报，要求：
1. 📊 **上午走势总览** — 上午整体表现，涨跌家数比
2. 🟢 **上午强势板块** — 涨幅领先的板块
3. 🏅 **上午强势个股** — 表现突出的个股及原因
4. 🔮 **下午关注** — 下午可能的走势和关注点
5. ⚠️ **风险提示**

风格：简洁、直接。字数控制在600字以内。"""

        else:  # closing
            prompt = f"""你是一位资深的A股券商分析师。请基于以下全天数据，生成{ today_str }的收盘复盘+次日展望。

## 全天行情
{quotes_str}

## 板块表现
{boards_str}

## 资金流向
{flow_str}

## 今日新闻
{news_str}

## 公司公告
{ann_str}

## AI分析结果
{analysis_str}

请生成Markdown格式的收盘复盘，要求：
1. 📊 **全天走势回顾** — 今日整体表现回顾
2. 🏆 **板块强弱分析** — 今日最强/最弱板块
3. 💰 **资金动向** — 主力资金和北向资金流向
4. 📰 **重要消息** — 对市场有影响的重要新闻/公告
5. 🔮 **明日预判** — 明日走势预判和关注方向
6. 🎯 **操作策略** — 简要操作建议
7. ⚠️ **风险提示**

风格：专业、有深度、有前瞻性，类似券商研报。字数控制在1000字以内。"""

        return prompt

    def generate_morning_report(self) -> str:
        """
        生成盘前早报（交易日 08:30）
        - 隔夜消息 + 前一日走势复盘
        - 今日关注板块和个股
        """
        today = date.today()
        if not self.is_trading_day(today):
            logger.info("非交易日，跳过早报生成")
            return ""

        logger.info(f"生成盘前早报 ({today.isoformat()})")

        if not self.analyzer:
            return self._fallback_report("morning")

        stock_analyses = self.analyzer.analyze_all_stocks()
        if not stock_analyses:
            return "今日暂无分析数据。"

        stock_analyses.sort(key=lambda x: x.get("confidence", 0), reverse=True)
        market = self._get_market_data()

        try:
            if self.nlp:
                prompt = self._build_report_prompt("morning", stock_analyses, market)
                logger.info(f"[早报] 调用LLM生成...")
                content = self.nlp._call_api(
                    messages=[{"role": "user", "content": prompt}],
                    system_prompt="你是一位资深的A股券商晨报分析师，专业、客观。"
                )
                if content:
                    return f"# 🌅 盘前早报\n**{today.strftime('%Y年%m月%d日')}**\n\n---\n\n" + content
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
        - 总结一天走势
        - 预判明天走势
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

        stock_analyses.sort(key=lambda x: x.get("confidence", 0), reverse=True)
        market = self._get_market_data()

        try:
            if self.nlp:
                prompt = self._build_report_prompt("closing", stock_analyses, market)
                content = self.nlp._call_api(
                    messages=[{"role": "user", "content": prompt}],
                    system_prompt="你是一位资深的A股券商分析师，专业、有深度。"
                )
                if content:
                    return f"# 📊 收盘复盘\n**{today.strftime('%Y年%m月%d日')}**\n\n---\n\n" + content
        except Exception as e:
            logger.warning(f"LLM 生成收盘复盘失败: {e}")

        # ── LLM 失败时兜底 ──
        today_str = today.strftime("%Y年%m月%d日")
        lines = [f"# 📊 收盘复盘 | {today_str}\n---\n"]

        # 整体情绪
        all_sentiments = [s.get("avg_sentiment", 0) or 0 for s in stock_analyses]
        avg_all = sum(all_sentiments) / max(len(all_sentiments), 1)
        market_icon = "🟢" if avg_all > 0.1 else ("🔴" if avg_all < -0.1 else "⚪")
        bullish_count = sum(1 for s in stock_analyses if s.get("avg_sentiment", 0) > 0.1)
        bearish_count = sum(1 for s in stock_analyses if s.get("avg_sentiment", 0) < -0.1)
        lines.append(f"**整体情绪**: {market_icon} {avg_all:.2f}  | 🟢偏多: {bullish_count}只  🔴偏空: {bearish_count}只\n\n")

        # 板块
        boards_data = market["boards"]
        boards_up = [b for b in boards_data if b.get("change_pct", 0) > 0][:5]
        boards_down = [b for b in boards_data if b.get("change_pct", 0) < 0][-5:]
        if boards_up:
            lines.append("## 🟢 强势板块\n")
            for b in boards_up:
                lines.append(f"- {b['board_name']}({b['board_type']}) {b['change_pct']}%\n")
            lines.append("\n")
        if boards_down:
            lines.append("## 🔴 弱势板块\n")
            for b in boards_down:
                lines.append(f"- {b['board_name']}({b['board_type']}) {b['change_pct']}%\n")
            lines.append("\n")

        # 个股
        top = stock_analyses[:5]
        lines.append("## 📈 重点个股\n")
        for s in top:
            n,c,sen,sug,conf = s.get('name',''),s.get('code',''),s.get('avg_sentiment',0),s.get('suggestion',''),s.get('confidence',0)
            icon = "🟢" if sen > 0.1 else ("🔴" if sen < -0.1 else "⚪")
            lines.append(f"- {icon} {n}({c}) | 情绪:{sen:.2f} | {sug} | 置信度:{conf:.0%}\n")
        lines.append("\n---\n")
        lines.append("*⏰ 收盘复盘 | 明日预判请参考以上板块和个股的资金动向*\n")

        return "".join(lines)

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
