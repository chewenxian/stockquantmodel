"""
分析记忆系统 — 记录每次分析结果，下次自动带上历史表现

借鉴 TradingAgents 的记忆机制：
- 每次分析结果写入 trading_memory.md
- 下次同标的分析带上前次判断和收益反思
- 跨标的经验总结注入决策上下文
"""
import json
import os
import logging
from datetime import datetime, date
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# 记忆文件路径
MEMORY_DIR = os.path.expanduser("~/.openclaw/workspace/stockquantmodel/analysis_memory")
MEMORY_FILE = os.path.join(MEMORY_DIR, "trading_memory.md")


class AnalysisMemory:
    """分析记忆管理器"""

    def __init__(self):
        os.makedirs(MEMORY_DIR, exist_ok=True)
        if not os.path.exists(MEMORY_FILE):
            self._init_memory_file()

    def _init_memory_file(self):
        with open(MEMORY_FILE, "w", encoding="utf-8") as f:
            f.write("# 交易分析与决策记忆\n\n")
            f.write(f"初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write("---\n\n")

    def save_analysis(self, code: str, name: str, analysis: Dict):
        """保存一次分析结果到记忆文件"""
        try:
            date_str = analysis.get("analysis_date", datetime.now().strftime("%Y-%m-%d"))
            sentiment = analysis.get("avg_sentiment", 0)
            suggestion = analysis.get("suggestion", "观望")
            confidence = analysis.get("confidence", 0)
            risk = analysis.get("risk_level", "中")
            summary = analysis.get("summary", "")[:200]

            entry = f"""
## {name} ({code}) — {date_str}

| 指标 | 值 |
|------|-----|
| 情绪值 | {sentiment:.2f} |
| 建议 | {suggestion} |
| 置信度 | {confidence:.0%} |
| 风险等级 | {risk} |

**分析摘要:**
{summary}

---
"""
            with open(MEMORY_FILE, "a", encoding="utf-8") as f:
                f.write(entry)
            return True
        except Exception as e:
            logger.warning(f"保存分析记忆失败: {e}")
            return False

    def get_historical_context(self, code: str, max_entries: int = 3) -> str:
        """获取某只股票的历史分析上下文"""
        try:
            if not os.path.exists(MEMORY_FILE):
                return ""

            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                content = f.read()

            # 找到该股票的最近分析记录
            sections = content.split(f"## ")
            matched = []
            for sec in sections:
                if f"({code})" in sec:
                    matched.append("## " + sec.strip())

            if not matched:
                return ""

            context = "\n\n".join(matched[-max_entries:])
            return f"【历史分析记录】\n{context}\n"
        except Exception as e:
            logger.warning(f"读取分析记忆失败: {e}")
            return ""

    def get_recent_lessons(self, limit: int = 5) -> str:
        """获取最近的跨标的经验教训"""
        try:
            if not os.path.exists(MEMORY_FILE):
                return ""

            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # 找最近的分析记录
            entries = []
            current = []
            for line in lines:
                if line.startswith("## ") and current:
                    entries.append("".join(current))
                    current = [line]
                else:
                    current.append(line)
            if current:
                entries.append("".join(current))

            if not entries:
                return ""

            recent = entries[-limit:]
            return "【近期分析回顾】\n" + "\n".join(recent) + "\n"
        except Exception as e:
            logger.warning(f"读取近期经验失败: {e}")
            return ""
