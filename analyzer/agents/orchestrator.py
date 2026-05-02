"""
多 Agent 分析编排器 — 全流程指挥

流程:
1. 按需实时采集（如果数据库无数据则实时拉取）
2. NewsAnalyst — 新闻分析
3. SentimentAnalyst — 情绪分析
4. TechnicalAnalyst — 技术分析
5. FundamentalsAnalyst — 基本面分析
6. Bull/Bear Researcher — 多空辩论
7. ResearchManager — 综合评级
"""
import logging
import re
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional

from .base_analyst import AnalystReport
from .news_analyst import NewsAnalyst
from .sentiment_analyst import SentimentAnalyst
from .technical_analyst import TechnicalAnalyst
from .fundamentals_analyst import FundamentalsAnalyst
from .bull_researcher import BullResearcher
from .bear_researcher import BearResearcher
from .research_manager import ResearchManager
from analyzer.analysis_memory import AnalysisMemory

logger = logging.getLogger(__name__)


class MultiAgentOrchestrator:
    """多 Agent 分析编排器（含按需实时采集）"""

    def __init__(self, db=None, nlp=None):
        self.db = db
        self.nlp = nlp
        self.memory = AnalysisMemory()

        # 初始化所有 Agent
        self.agents = {
            "news": NewsAnalyst(db, nlp),
            "sentiment": SentimentAnalyst(db, nlp),
            "technical": TechnicalAnalyst(db, nlp),
            "fundamentals": FundamentalsAnalyst(db, nlp),
        }
        self.bull = BullResearcher(db, nlp)
        self.bear = BearResearcher(db, nlp)
        self.manager = ResearchManager(db, nlp)

        # 延迟初始化各采集器（用于按需实时采集）
        self._eastmoney = None
        self._history_quotes = None
        self._guba = None

    # ═══════════════════════════════════════════════════════
    #  按需实时采集（关键新增）
    # ═══════════════════════════════════════════════════════

    def _get_eastmoney(self):
        if self._eastmoney is None and self.db:
            try:
                from collector.spiders.eastmoney import EastMoneyCollector
                self._eastmoney = EastMoneyCollector(self.db)
            except Exception as e:
                logger.warning(f"初始化EastMoney采集器失败: {e}")
        return self._eastmoney

    def _get_history_quotes(self):
        if self._history_quotes is None and self.db:
            try:
                from collector.spiders.history_quotes import HistoryQuotesCollector
                self._history_quotes = HistoryQuotesCollector(self.db)
            except Exception as e:
                logger.warning(f"初始化HistoryQuotes采集器失败: {e}")
        return self._history_quotes

    def _get_guba(self):
        if self._guba is None and self.db:
            try:
                from collector.spiders.guba_sentiment import GubaSentimentCollector
                self._guba = GubaSentimentCollector(self.db)
            except Exception as e:
                logger.warning(f"初始化Guba采集器失败: {e}")
        return self._guba

    def _has_recent_news(self, code: str, hours: int = 12) -> bool:
        """检查最近是否有该股新闻缓存"""
        if not self.db:
            return False
        try:
            conn = self.db._connect()
            row = conn.execute("""
                SELECT COUNT(*) FROM news_stocks ns
                JOIN news n ON n.id = ns.news_id
                WHERE ns.stock_code = ?
                AND n.published_at >= datetime('now', ?)
            """, (code, f'-{hours} hours')).fetchone()
            self.db._close(conn)
            count = row[0] if row else 0
            logger.debug(f"[按需采集] {code} 最近{hours}h新闻缓存: {count}条")
            return count >= 3  # 至少3条新闻才算有效
        except Exception:
            return False

    def _has_daily_price(self, code: str, days: int = 120) -> bool:
        """检查是否有足够的历史K线数据"""
        if not self.db:
            return False
        try:
            conn = self.db._connect()
            row = conn.execute("""
                SELECT COUNT(*) FROM daily_prices
                WHERE stock_code = ?
                AND trade_date >= date('now', ?)
            """, (code, f'-{days} days')).fetchone()
            self.db._close(conn)
            count = row[0] if row else 0
            logger.debug(f"[按需采集] {code} 最近{days}天K线缓存: {count}条")
            return count >= 20  # 至少20个交易日才算有效
        except Exception:
            return False

    def _has_guba_sentiment(self, code: str) -> bool:
        """检查是否有股吧情绪数据"""
        if not self.db:
            return False
        try:
            conn = self.db._connect()
            row = conn.execute("""
                SELECT COUNT(*) FROM guba_sentiment
                WHERE stock_code = ?
                AND trade_date = date('now')
            """, (code,)).fetchone()
            self.db._close(conn)
            count = row[0] if row else 0
            return count > 0
        except Exception:
            return False

    def _live_fetch_news(self, code: str, name: str) -> int:
        """
        按需实时采集个股新闻
        策略：东方财富个股页面 → 问财API
        Returns: 获取到的新闻条数
        """
        fetched = 0

        # 方案一：东方财富个股新闻
        collector = self._get_eastmoney()
        if collector:
            try:
                count = collector.collect_news_for_stock(code)
                logger.info(f"[按需采集] 东方财富个股新闻({code}): {count}条")
                fetched += count
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"[按需采集] 东方财富个股新闻失败({code}): {e}")

        # 方案二：标题匹配（将新闻库中未关联的新闻通过标题匹配到此股）
        if fetched < 3 and collector:
            try:
                linked = collector.link_unlinked_news_by_title()
                logger.info(f"[按需采集] 标题匹配({code}): 全局关联{linked}条")
            except Exception as e:
                logger.warning(f"[按需采集] 标题匹配失败: {e}")

        # 方案三：问财API补充（如果还是太少）
        if fetched < 3:
            try:
                from analyzer.stock_analyzer import StockAnalyzer
                temp = StockAnalyzer(db=self.db)
                iwencai = temp._fetch_iwencai_for_stock(code, name)
                news_list = iwencai.get("news", [])
                if news_list:
                    # 入库
                    batch = []
                    for item in news_list[:10]:
                        batch.append({
                            "title": item.get("title", ""),
                            "url": item.get("url", "") or item.get("source_url", ""),
                            "source": "问财",
                            "summary": item.get("summary", "") or item.get("content", ""),
                            "published_at": item.get("publish_date", "") or datetime.now().isoformat(),
                        })
                    if batch and self.db:
                        self.db.batch_insert_news(batch)
                        # 尝试关联（通过标题）
                        for item in batch:
                            if any(w in item["title"] for w in [name, code]):
                                conn = self.db._connect()
                                news_row = conn.execute(
                                    "SELECT id FROM news WHERE title = ?", (item["title"],)
                                ).fetchone()
                                if news_row:
                                    conn.execute(
                                        "INSERT OR IGNORE INTO news_stocks(news_id, stock_code) VALUES(?, ?)",
                                        (news_row[0], code)
                                    )
                                conn.commit()
                                self.db._close(conn)
                    fetched += len(batch)
                    logger.info(f"[按需采集] 问财新闻({code}): 补充{len(batch)}条")
            except Exception as e:
                logger.debug(f"[按需采集] 问财新闻补充失败: {e}")

        return fetched

    def _live_fetch_kline(self, code: str) -> int:
        """按需实时采集历史K线（多源降级，直接调用）"""
        # 先尝试用 collector 类
        collector = self._get_history_quotes()
        if collector:
            try:
                count = collector.collect_stock(code, limit=500)
                if count > 0:
                    logger.info(f"[按需采集] 历史K线({code}): {count}条")
                    return count
            except Exception as e:
                logger.debug(f"[按需采集] HistoryQuotesCollector失败({code}): {e}")

        # 降级：直接调用新浪K线API（scale=240 为日K线，已验证可用）
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
            resp = requests.get(url, params={"symbol": f"{prefix}{code}", "datalen": 500, "scale": 240},
                                timeout=15,
                                headers={"Referer": "https://finance.sina.com.cn"})
            if resp.status_code != 200:
                logger.warning(f"[按需采集] 新浪K线API {code} 状态码={resp.status_code}")
                return 0
            data = resp.json()
            if not isinstance(data, list) or len(data) == 0:
                logger.warning(f"[按需采集] 新浪K线API返回空({code})")
                return 0
            count = 0
            for item in data:
                d = item.get("day", "")
                o = float(item.get("open", 0)) if item.get("open") else None
                c = float(item.get("close", 0)) if item.get("close") else None
                h = float(item.get("high", 0)) if item.get("high") else None
                l = float(item.get("low", 0)) if item.get("low") else None
                v = float(item.get("volume", 0)) if item.get("volume") else None
                chg = round((c - o) / o * 100, 2) if o and c and o > 0 else None
                if self.db and self.db.upsert_daily_price(code, d, o, c, h, l, v, None, chg):
                    count += 1
            logger.info(f"[按需采集] 新浪K线({code}): {count}条")
            return count
        except Exception as e:
            logger.warning(f"[按需采集] 新浪K线降级也失败({code}): {e}")
            return 0

    def _live_fetch_guba(self, code: str, name: str) -> int:
        """按需实时采集股吧情绪（API→HTML降级）"""
        collector = self._get_guba()
        if not collector:
            return 0
        try:
            # 优先API（可能有404错误，忽略并降级）
            result = None
            try:
                result = collector._fetch_by_api(code, name)
            except Exception:
                pass
            if result is not None and result > 0:
                logger.info(f"[按需采集] 股吧情绪API({code}): 成功")
                return result
            # API失败/404，降级到HTML
            count = collector._fetch_by_html(code, name)
            logger.info(f"[按需采集] 股吧情绪HTML({code}): {count}条")
            return count
        except Exception as e:
            logger.warning(f"[按需采集] 股吧情绪失败({code}): {e}")
            return 0

    def _ensure_stock_in_db(self, code: str, name: str) -> bool:
        """确保该股在stocks表中存在（非自选股也存一份以便关联新闻）"""
        if not self.db:
            return False
        try:
            conn = self.db._connect()
            existing = conn.execute(
                "SELECT code FROM stocks WHERE code = ?", (code,)
            ).fetchone()
            if not existing:
                market = "SH" if code.startswith("6") else "BJ" if code.startswith("4") or code.startswith("8") else "SZ"
                conn.execute(
                    "INSERT OR IGNORE INTO stocks(code, name, market) VALUES(?, ?, ?)",
                    (code, name, market)
                )
                conn.commit()
                logger.info(f"[按需采集] 自动添加 {name}({code}) 到stocks表")
            self.db._close(conn)
            return True
        except Exception as e:
            logger.warning(f"[按需采集] 添加{code}到stocks表失败: {e}")
            return False

    # ═══════════════════════════════════════════════════════
    #  全流程分析（含按需采集）
    # ═══════════════════════════════════════════════════════

    def analyze(self, code: str, name: str) -> Dict:
        """
        执行全流程多 Agent 分析
        自动按需实时采集缺失数据

        Args:
            code: 股票代码
            name: 股票名称

        Returns:
            {
                "reports": {各分析师报告},
                "debate": {多空辩论},
                "final_rating": {最终评级},
                "summary": 一句话总结,
                "analysis_date": 分析时间
            }
        """
        logger.info(f"🚀 启动多Agent分析: {name}({code})")

        # ── Step 0: 按需实时采集 ──
        logger.info(f"Phase 0: 按需实时采集...")

        self._ensure_stock_in_db(code, name)

        live_feeds = {}
        if not self._has_recent_news(code):
            n = self._live_fetch_news(code, name)
            live_feeds["news"] = n
        else:
            live_feeds["news"] = -1  # 已缓存

        if not self._has_daily_price(code):
            k = self._live_fetch_kline(code)
            live_feeds["kline"] = k
        else:
            live_feeds["kline"] = -1

        if not self._has_guba_sentiment(code):
            g = self._live_fetch_guba(code, name)
            live_feeds["guba"] = g
        else:
            live_feeds["guba"] = -1

        logger.info(f"  [按需采集] 结果: "
                     f"新闻={live_feeds['news']}, "
                     f"K线={live_feeds['kline']}, "
                     f"股吧={live_feeds['guba']}")

        # 加载历史记忆
        historical_context = self.memory.get_historical_context(code)

        # === Phase 1: 分析师并行分析 ===
        logger.info(f"Phase 1: 分析师分析...")

        news_report = self.agents["news"].analyze(code, name, historical_context=historical_context)
        logger.info(f"  ✅ {news_report.analyst_name}: 情绪={news_report.sentiment:+.2f}")

        sentiment_report = self.agents["sentiment"].analyze(code, name)
        logger.info(f"  ✅ {sentiment_report.analyst_name}: 情绪={sentiment_report.sentiment:+.2f}")

        technical_report = self.agents["technical"].analyze(code, name)
        logger.info(f"  ✅ {technical_report.analyst_name}: 情绪={technical_report.sentiment:+.2f}")

        fundamentals_report = self.agents["fundamentals"].analyze(code, name)
        logger.info(f"  ✅ {fundamentals_report.analyst_name}: 情绪={fundamentals_report.sentiment:+.2f}")

        analyst_reports = [news_report, sentiment_report, technical_report, fundamentals_report]

        # === Phase 2: 多空辩论 ===
        logger.info(f"Phase 2: 多空辩论...")
        bull_report = self.bull.analyze(code, name, analyst_reports=analyst_reports)
        bear_report = self.bear.analyze(code, name, analyst_reports=analyst_reports)
        logger.info(f"  🟢 多方: 情绪={bull_report.sentiment:+.2f}(置信{bull_report.confidence:.0%})")
        logger.info(f"  🔴 空方: 情绪={bear_report.sentiment:+.2f}(置信{bear_report.confidence:.0%})")

        # === Phase 3: 研究主管综合 ===
        logger.info(f"Phase 3: 综合评级...")
        final_report = self.manager.analyze(
            code, name,
            analyst_reports=analyst_reports,
            bull_report=bull_report,
            bear_report=bear_report,
            historical_context=historical_context,
        )
        logger.info(f"  🎯 最终评级: {final_report.details.get('rating', 'N/A')} "
                    f"(综合情绪={final_report.sentiment:+.2f})")

        # 标记实时采集来源
        live_source_labels = []
        for key, val in live_feeds.items():
            if val > 0:
                live_source_labels.append(f"{key}(实时{val}条)")
            elif val == -1:
                live_source_labels.append(f"{key}(缓存)")

        result = {
            "reports": {
                "news": news_report.to_dict(),
                "sentiment": sentiment_report.to_dict(),
                "technical": technical_report.to_dict(),
                "fundamentals": fundamentals_report.to_dict(),
            },
            "debate": {
                "bull": bull_report.to_dict(),
                "bear": bear_report.to_dict(),
            },
            "final_rating": final_report.to_dict(),
            "summary": final_report.summary,
            "rating": final_report.details.get("rating", "持有"),
            "sentiment": final_report.sentiment,
            "confidence": final_report.confidence,
            "risk_factors": final_report.risk_factors,
            "opportunities": final_report.opportunities,
            "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "live_fetch": live_feeds,
            "data_source": " + ".join(live_source_labels),
        }

        # 保存到记忆
        self.memory.save_analysis(code, name, {
            "avg_sentiment": final_report.sentiment,
            "suggestion": final_report.details.get("rating", "持有"),
            "confidence": final_report.confidence,
            "risk_level": "低" if final_report.sentiment > 0.3 else ("高" if final_report.sentiment < -0.3 else "中"),
            "summary": final_report.summary,
            "analysis_date": result["analysis_date"],
        })

        return result

    def analyze_all(self) -> List[Dict]:
        """分析所有自选股"""
        if not self.db:
            return []
        stocks = self.db.load_stocks()
        results = []
        for stock in stocks:
            try:
                r = self.analyze(stock["code"], stock["name"])
                results.append(r)
                logger.info(f"  ✅ {stock['name']}({stock['code']}): {r['rating']}")
            except Exception as e:
                logger.error(f"  ❌ {stock['name']}({stock['code']}): {e}")
        return results
