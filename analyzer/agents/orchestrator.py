"""
多 Agent 分析编排器 — 全流程指挥

流程:
1. NewsAnalyst — 新闻分析
2. SentimentAnalyst — 情绪分析
3. TechnicalAnalyst — 技术分析
4. FundamentalsAnalyst — 基本面分析
5. Bull/Bear Researcher — 多空辩论
6. ResearchManager — 综合评级
"""
import logging
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
    """多 Agent 分析编排器"""

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

    def analyze(self, code: str, name: str) -> Dict:
        """
        执行全流程多 Agent 分析

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
