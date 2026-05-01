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
import time
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import List, Optional, Dict as DictType
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 生命周期管理：启动时初始化全局依赖"""
    from storage.database import Database
    from analyzer.stock_analyzer import StockAnalyzer
    from analyzer.report_generator import ReportGenerator

    global _db, _analyzer, _report_gen
    _db = Database("data/stock_news.db")
    _analyzer = StockAnalyzer()
    _report_gen = ReportGenerator()
    logger.info("全局依赖初始化完成 (Database / StockAnalyzer / ReportGenerator)")
    yield
    # 可在此添加关闭清理逻辑
    logger.info("全局依赖关闭")


app = FastAPI(
    title="股票量化分析系统 API",
    description="股票新闻情报收集分析系统的 RESTful 接口",
    version="6.0.0",
    lifespan=lifespan,
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
# Pydantic 模型
# ═══════════════════════════════════════════

class BatchAnalyzeRequest(BaseModel):
    """批量分析请求模型"""
    codes: List[str]
    days: int = 1


class SearchResult(BaseModel):
    """搜索结果模型"""
    total: int
    results: List[DictType]


# ═══════════════════════════════════════════
# 简单限流（基于时间戳，每IP每分钟最多30次）
# ═══════════════════════════════════════════

_RATE_LIMIT_WINDOW = 60  # 窗口秒数
_RATE_LIMIT_MAX = 30     # 最大请求数
_rate_limit_store = defaultdict(list)  # ip -> [timestamps]


def _check_rate_limit(request: Request) -> bool:
    """
    检查IP是否超过限流阈值

    Args:
        request: FastAPI Request 对象

    Returns:
        bool: True=允许通过, False=超过限流
    """
    try:
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        window_start = now - _RATE_LIMIT_WINDOW

        # 清理过期记录
        timestamps = _rate_limit_store[client_ip]
        _rate_limit_store[client_ip] = [t for t in timestamps if t > window_start]

        # 检查是否超限
        if len(_rate_limit_store[client_ip]) >= _RATE_LIMIT_MAX:
            return False

        _rate_limit_store[client_ip].append(now)
        return True

    except Exception:
        # 限流出错时放行，避免影响正常使用
        return True


# ═══════════════════════════════════════════
# 延迟初始化（避免启动时加载所有依赖）
# ═══════════════════════════════════════════

_db = None
_analyzer = None
_report_gen = None


def _get_db():
    return _db


def _get_analyzer():
    return _analyzer


def _get_report_gen():
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
async def analyze_stock(code: str, days: int = Query(1, ge=1, le=30), request: Request = None):
    """个股分析"""
    # 简单限流
    if request and not _check_rate_limit(request):
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试（每分钟最多30次）")

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
async def analyze_all(request: Request = None):
    """全量分析"""
    # 简单限流
    if request and not _check_rate_limit(request):
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试（每分钟最多30次）")

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
# 搜索与批量分析
# ═══════════════════════════════════════════

@app.get("/search")
async def search_news(
    q: str = Query(..., description="搜索关键词"),
    limit: int = Query(10, ge=1, le=50),
    source: Optional[str] = Query(None, description="数据源过滤"),
    request: Request = None,
):
    """
    搜索新闻和公告

    支持关键词搜索、数据源过滤

    Args:
        q: 搜索关键词
        limit: 返回结果数量上限（1~50，默认10）
        source: 数据源过滤（如：东方财富、巨潮资讯等）
        request: FastAPI请求对象（用于限流）

    Returns:
        {
            "total": 总结果数,
            "results": [新闻/公告列表],
            "query": 搜索关键词,
            "source": 数据源过滤,
        }
    """
    # 简单限流
    if request and not _check_rate_limit(request):
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试（每分钟最多30次）")

    db = _get_db()
    try:
        # 搜索新闻
        news_results = db.search_news(q, limit=limit * 2)  # 多取一些以便混合
        # 搜索公告
        announcements_results = db.search_announcements(q, limit=limit * 2)

        results = []

        # 合并新闻结果
        for item in (news_results or []):
            result_item = {
                "type": "news",
                "title": item.get("title", ""),
                "summary": item.get("summary", ""),
                "source": item.get("source", ""),
                "published_at": item.get("published_at", ""),
                "stock_code": item.get("stock_code", ""),
                "url": item.get("url", ""),
                "sentiment": item.get("sentiment", 0),
            }
            # 按数据源过滤
            if source and source not in str(result_item.get("source", "")):
                continue
            results.append(result_item)

        # 合并公告结果
        for item in (announcements_results or []):
            result_item = {
                "type": "announcement",
                "title": item.get("title", ""),
                "summary": item.get("content", "")[:200],
                "source": item.get("source", "公告"),
                "published_at": item.get("published_at", ""),
                "stock_code": item.get("stock_code", ""),
                "url": item.get("url", ""),
            }
            if source and source not in str(result_item.get("source", "")):
                continue
            results.append(result_item)

        # 按时间排序（最新的在前）
        results.sort(key=lambda x: x.get("published_at", ""), reverse=True)

        # 截取限制
        results = results[:limit]

        return {
            "total": len(results),
            "results": results,
            "query": q,
            "source": source,
        }

    except Exception as e:
        logger.error(f"搜索失败 (q={q}): {e}")
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@app.post("/batch_analyze")
async def batch_analyze(request: BatchAnalyzeRequest):
    """
    批量分析多只股票

    接受股票代码列表，返回每只股票的分析结果

    Args:
        request: 批量分析请求 {"codes": ["000001", "600000"], "days": 1}

    Returns:
        {
            "count": 成功分析数量,
            "results": [分析结果列表],
            "errors": [失败列表],
        }
    """
    analyzer = _get_analyzer()
    codes = request.codes
    days = request.days

    results = []
    errors = []

    for code in codes:
        try:
            result = analyzer.analyze_stock(code, days=days)
            if result and "error" not in result:
                results.append(result)
            else:
                errors.append({
                    "code": code,
                    "error": result.get("error", "分析返回空结果") if result else "分析失败",
                })
        except Exception as e:
            logger.error(f"批量分析 {code} 失败: {e}")
            errors.append({
                "code": code,
                "error": str(e),
            })

    return {
        "count": len(results),
        "results": results,
        "errors": errors,
        "timestamp": datetime.now().isoformat(),
    }


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
