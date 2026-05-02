"""
多 Agent 分析架构

借鉴 TradingAgents:
- NewsAnalyst → 新闻情绪分析
- SentimentAnalyst → 社交媒体情绪分析
- TechnicalAnalyst → 技术指标分析
- FundamentalsAnalyst → 基本面分析
- Bull/Bear Researcher → 多空观点碰撞
- ResearchManager → 综合决策
"""
from .news_analyst import NewsAnalyst
from .sentiment_analyst import SentimentAnalyst
from .technical_analyst import TechnicalAnalyst
from .fundamentals_analyst import FundamentalsAnalyst
from .bull_researcher import BullResearcher
from .bear_researcher import BearResearcher
from .research_manager import ResearchManager
from .orchestrator import MultiAgentOrchestrator

__all__ = [
    "NewsAnalyst", "SentimentAnalyst", "TechnicalAnalyst",
    "FundamentalsAnalyst", "BullResearcher", "BearResearcher",
    "ResearchManager", "MultiAgentOrchestrator",
]
