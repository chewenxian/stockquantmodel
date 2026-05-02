"""基本面分析师 — PE/PB/ROE/营收等基本面分析"""
import json
import logging
import subprocess
import os
from datetime import datetime
from typing import Dict, Optional

from .base_analyst import BaseAnalyst, AnalystReport

logger = logging.getLogger(__name__)


class FundamentalsAnalyst(BaseAnalyst):
    """基本面分析师：通过问财获取PE/PB/ROE等数据"""

    def __init__(self, db=None, nlp=None):
        super().__init__(db, nlp)
        self.name = "📊 基本面分析师"

    def analyze(self, code: str, name: str, **kwargs) -> AnalystReport:
        report = AnalystReport(self.name, code, name)

        # 通过问财获取基本面数据
        fundamentals = self._fetch_fundamentals(code, name)
        if not fundamentals:
            report.summary = f"{name} 基本面数据暂缺"
            report.confidence = 0.2
            return report

        pe = fundamentals.get("pe")
        pb = fundamentals.get("pb")
        roe = fundamentals.get("roe")
        market_cap = fundamentals.get("market_cap", 0)

        report.details = fundamentals

        # PE分析
        if pe is not None:
            report.key_findings.append(f"市盈率(PE)={pe:.2f}")
            if pe > 50:
                report.risk_factors.append(f"PE={pe:.1f}偏高，估值可能过高")
                report.sentiment -= 0.1
            elif pe < 15:
                report.opportunities.append(f"PE={pe:.1f}偏低，可能存在低估机会")
                report.sentiment += 0.1

        # PB分析
        if pb is not None:
            report.key_findings.append(f"市净率(PB)={pb:.2f}")
            if pb > 5:
                report.risk_factors.append(f"PB={pb:.1f}偏高")
            elif pb < 1:
                report.opportunities.append(f"PB={pb:.1f}<1，可能破净")
                report.sentiment -= 0.05

        # ROE分析
        if roe is not None:
            report.key_findings.append(f"净资产收益率(ROE)={roe:.1f}%")
            if roe > 15:
                report.opportunities.append(f"ROE={roe:.1f}%优秀，盈利能力强劲")
                report.sentiment += 0.15
            elif roe < 5:
                report.risk_factors.append(f"ROE={roe:.1f}%偏低，盈利能力不足")
                report.sentiment -= 0.15

        # 市值
        if market_cap:
            cap_billion = market_cap / 1e8
            if cap_billion > 1000:
                report.key_findings.append(f"市值{cap_billion:.0f}亿，超大市值蓝筹")
            elif cap_billion > 100:
                report.key_findings.append(f"市值{cap_billion:.0f}亿，中大盘股")

        report.sentiment = max(min(report.sentiment, 1.0), -1.0)
        report.confidence = min(0.5 + abs(report.sentiment) * 0.3, 1.0)
        report.summary = self._gen_summary(report)

        return report

    def _fetch_fundamentals(self, code: str, name: str) -> Optional[Dict]:
        """通过问财CLI获取基本面数据"""
        skill_dir = os.path.expanduser(
            "~/.openclaw/workspace/skills/hithink-sector-selector"
        )
        cli_script = os.path.join(skill_dir, "scripts", "cli.py")
        if not os.path.exists(cli_script):
            logger.warning("[基本面] 问财CLI未安装")
            # 从数据库已有的analysis结果取
            return self._get_db_fundamentals(code)

        try:
            query = f"{name} {code} 最新市盈率 市净率 净资产收益率 流通市值"
            result = subprocess.run(
                ["python3", cli_script, "--query", query, "--limit", "1"],
                capture_output=True, text=True, timeout=15,
                env={**os.environ}
            )
            if result.returncode != 0:
                return self._get_db_fundamentals(code)

            data = json.loads(result.stdout)
            if not data.get("success") or not data.get("datas"):
                return self._get_db_fundamentals(code)

            item = data["datas"][0]
            return {
                "pe": item.get("最新市盈率ttm"),
                "pb": item.get("最新市净率"),
                "roe": item.get("净资产收益率[20260331]"),
                "market_cap": item.get("最新a股流通市值"),
                "static_pe": item.get("最新静态市盈率"),
            }
        except Exception as e:
            logger.warning(f"[基本面] 问财查询失败: {e}")
            return self._get_db_fundamentals(code)

    def _get_db_fundamentals(self, code: str) -> Optional[Dict]:
        """从数据库已有分析结果获取基本面（回退）"""
        try:
            conn = self.db._connect()
            row = conn.execute(
                "SELECT llm_analysis FROM analysis WHERE stock_code = ? ORDER BY date DESC LIMIT 1",
                (code,)
            ).fetchone()
            self.db._close(conn)
            return {"source": "database"} if row else None
        except Exception:
            return None

    def _gen_summary(self, report: AnalystReport) -> str:
        d = report.details
        parts = []
        if d.get("pe"):
            parts.append(f"PE={d['pe']:.1f}")
        if d.get("pb"):
            parts.append(f"PB={d['pb']:.2f}")
        if d.get("roe") is not None:
            parts.append(f"ROE={d['roe']:.1f}%")
        if parts:
            return f"基本面: {' | '.join(parts)}"
        return "基本面数据有限"
