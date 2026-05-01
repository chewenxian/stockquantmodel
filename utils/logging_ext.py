"""
结构化日志 + 统一异常体系

使用方式:
    from utils.logging_ext import get_logger, StockQuantError

    logger = get_logger(__name__)
    logger.info("采集完成", extra={"source": "eastmoney", "count": 42})

异常体系:
    StockQuantError          # 基类
    ├── CollectorError       # 采集层异常
    ├── DatabaseError        # 数据库层异常
    ├── AnalyzerError        # 分析层异常
    └── APIError             # API层异常
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """JSON 日志格式化器"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # 如果有异常，附加堆栈
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)

        # 如果有 extra 字段，附加
        for key in ("source", "count", "code", "duration", "status"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        return json.dumps(log_entry, ensure_ascii=False)


# ══════════════════════════════════════════
# 统一异常体系
# ══════════════════════════════════════════


class StockQuantError(Exception):
    """
    系统统一异常基类

    Attributes:
        message: 错误描述
        code: 错误码
        details: 附加详情
    """

    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code or "UNKNOWN"
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": self.message,
            "code": self.code,
            "details": self.details,
        }


class CollectorError(StockQuantError):
    """采集层异常"""
    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code=code or "COLLECTOR_ERROR", details=details)


class DatabaseError(StockQuantError):
    """数据库层异常"""
    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code=code or "DATABASE_ERROR", details=details)


class AnalyzerError(StockQuantError):
    """分析层异常"""
    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code=code or "ANALYZER_ERROR", details=details)


class APIError(StockQuantError):
    """API层异常"""
    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code=code or "API_ERROR", details=details)


# ══════════════════════════════════════════
# 日志配置
# ══════════════════════════════════════════


def setup_json_logging(level: int = logging.INFO,
                       log_file: Optional[str] = None):
    """
    配置全局 JSON 日志

    - 控制台输出 JSON 格式
    - 可选同时写入文件

    Args:
        level: 日志级别，默认 logging.INFO
        log_file: 日志文件路径（可选）
    """
    # 移除已有 handler
    root = logging.getLogger()
    root.handlers.clear()

    # 控制台 handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(JSONFormatter())
    root.addHandler(console)

    # 文件 handler（可选）
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(JSONFormatter())
        root.addHandler(fh)

    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """获取日志器，自动适配 extra 字段"""
    return logging.getLogger(name)
