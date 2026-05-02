"""分析师基类 — 通用报告格式"""
from datetime import datetime
from typing import Dict, List, Optional, Any

class AnalystReport:
    """分析师报告的数据结构"""
    
    def __init__(self, analyst_name: str, stock_code: str, stock_name: str):
        self.analyst_name = analyst_name
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M")
        self.summary = ""
        self.sentiment = 0.0          # -1 ~ 1
        self.confidence = 0.0         # 0 ~ 1
        self.key_findings: List[str] = []
        self.risk_factors: List[str] = []
        self.opportunities: List[str] = []
        self.details: Dict[str, Any] = {}

    def to_dict(self) -> Dict:
        return {
            "analyst": self.analyst_name,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "date": self.analysis_date,
            "summary": self.summary,
            "sentiment": self.sentiment,
            "confidence": self.confidence,
            "key_findings": self.key_findings,
            "risk_factors": self.risk_factors,
            "opportunities": self.opportunities,
            "details": self.details,
        }

    def __str__(self) -> str:
        sentiment_label = "看多" if self.sentiment > 0.15 else ("看空" if self.sentiment < -0.15 else "中性")
        return f"""
## {self.analyst_name} 报告 — {self.stock_name}({self.stock_code})

**情绪**: {sentiment_label} ({self.sentiment:+.2f}) | **置信度**: {self.confidence:.0%}

**摘要**: {self.summary}

**关键发现**:
{chr(10).join(f'- {f}' for f in self.key_findings)}

**风险因素**:
{chr(10).join(f'- {r}' for r in self.risk_factors)}

**机会**:
{chr(10).join(f'- {o}' for o in self.opportunities)}
"""


class BaseAnalyst:
    """分析师基类"""
    
    def __init__(self, db=None, nlp=None):
        self.db = db
        self.nlp = nlp
        self.name = self.__class__.__name__

    def analyze(self, code: str, name: str) -> AnalystReport:
        """子类实现：返回分析报告"""
        raise NotImplementedError
