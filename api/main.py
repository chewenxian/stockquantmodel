#!/usr/bin/env python3
"""
📡 FastAPI 接口层——股票量化分析系统

提供 RESTful API 用于查询分析结果、信号、报告等。

启动方式:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
    或 python main.py api
"""
import sys
import os
import json
import logging
from datetime import datetime, date
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(
    title="股票量化分析系统 API",
    description="股票新闻情报收集分析系统的 RESTful 接口",
    version="6.0.0",
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════
# 延迟初始化（避免启动时加载所有依赖）
# ═══════════════════════════════════════════

_db = None
_analyzer = None
_report_gen = None


def _get_db():
    global _db
    if _db is None:
        from storage.database import Database
        _db = Database("data/stock_news.db")
    return _db


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        from analyzer.stock_analyzer import StockAnalyzer
        _analyzer = StockAnalyzer()
    return _analyzer


def _get_report_gen():
    global _report_gen
    if _report_gen is None:
        from analyzer.report_generator import ReportGenerator
        _report_gen = ReportGenerator()
    return _report_gen


# ═══════════════════════════════════════════
# 路由
# ═══════════════════════════════════════════


@app.get("/")
async def root():
    """系统状态"""
    db = _get_db()
    stats = db.get_stats()
    return {
        "status": "running",
        "service": "股票量化分析系统 API",
        "version": "6.0.0",
        "timestamp": datetime.now().isoformat(),
        "stats": {
            "stocks": stats.get("total_stocks", 0),
            "news": stats.get("total_news", 0),
            "today_news": stats.get("today_news", 0),
            "announcements": stats.get("total_announcements", 0),
            "analysis": stats.get("total_analysis", 0),
            "db_size_mb": stats.get("db_size_mb", 0),
        },
    }


@app.get("/analyze/{code}")
async def analyze_stock(code: str, days: int = Query(1, ge=1, le=30)):
    """个股分析"""
    analyzer = _get_analyzer()
    try:
        result = analyzer.analyze_stock(code, days=days)
        if not result or "error" in result:
            raise HTTPException(status_code=404, detail=result.get("error", "分析失败"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"分析 {code} 失败: {e}")
        raise HTTPException(status_code=500, detail=f"分析失败: {str(e)}")


@app.get("/analyze")
async def analyze_all():
    """全量分析"""
    analyzer = _get_analyzer()
    try:
        results = analyzer.analyze_all_stocks()
        return {
            "count": len(results),
            "timestamp": datetime.now().isoformat(),
            "results": results,
        }
    except Exception as e:
        logger.error(f"全量分析失败: {e}")
        raise HTTPException(status_code=500, detail=f"全量分析失败: {str(e)}")


@app.get("/signals")
async def get_signals(level: Optional[str] = Query(None, pattern="^(S|A|B|C)$")):
    """今日S/A级信号列表"""
    db = _get_db()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        analyses = db.get_today_analysis()

        # 尝试从 impact_model 或 signal_grader 获取信号级别
        signals = []
        for a in analyses:
            signal = {
                "code": a.get("stock_code", ""),
                "name": a.get("name", ""),
                "suggestion": a.get("suggestion", "持有"),
                "confidence": a.get("confidence", 0),
                "sentiment": a.get("avg_sentiment", 0),
                "risk_level": a.get("risk_level", "中"),
                "summary": (a.get("llm_analysis") or "")[:200],
            }
            # 简单信号级别估算（实际应使用 SignalGrader）
            if signal["confidence"] >= 0.8 and abs(signal["sentiment"]) >= 0.5:
                signal["signal_level"] = "S"
            elif signal["confidence"] >= 0.6 and abs(signal["sentiment"]) >= 0.3:
                signal["signal_level"] = "A"
            elif signal["confidence"] >= 0.4:
                signal["signal_level"] = "B"
            elif signal["confidence"] >= 0.2:
                signal["signal_level"] = "C"
            else:
                signal["signal_level"] = "无效"

            signals.append(signal)

        # 按级别过滤
        if level:
            signals = [s for s in signals if s["signal_level"] == level]

        # 默认只返回 S/A 级
        if level is None:
            signals = [s for s in signals if s["signal_level"] in ("S", "A")]

        return {
            "date": today,
            "count": len(signals),
            "signals": signals,
        }
    except Exception as e:
        logger.error(f"获取信号列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report")
async def get_report(type: str = Query("closing", pattern="^(closing|morning)$")):
    """今日报告文本"""
    gen = _get_report_gen()
    try:
        if type == "morning":
            report = gen.generate_morning_report()
        else:
            report = gen.generate_closing_report()

        if not report:
            raise HTTPException(status_code=404, detail="报告生成失败，可能为非交易日")
        return {
            "type": type,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "content": report,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成报告失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report/morning")
async def get_morning_report():
    """盘前早报快捷入口"""
    gen = _get_report_gen()
    try:
        report = gen.generate_morning_report()
        if not report:
            raise HTTPException(status_code=404, detail="早报生成失败，可能为非交易日")
        return {
            "type": "morning",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "content": report,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stocks")
async def get_stocks():
    """自选股列表"""
    db = _get_db()
    try:
        stocks = db.load_stocks()
        return {
            "count": len(stocks),
            "stocks": stocks,
        }
    except Exception as e:
        logger.error(f"获取自选股失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """数据库统计"""
    db = _get_db()
    try:
        stats = db.get_stats()
        return stats
    except Exception as e:
        logger.error(f"获取统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════
# 健康检查
# ═══════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# ═══════════════════════════════════════════
# 直接运行
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    print("🚀 启动 FastAPI 服务: http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
