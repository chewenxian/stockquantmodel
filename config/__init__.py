"""
配置管理模块
统一从 config.yaml 读取配置，避免各模块硬编码路径
"""
import os
from typing import Any, Dict, Optional

_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    """
    加载系统配置（带缓存）

    Args:
        path: 配置文件路径，默认 config.yaml

    Returns:
        配置字典
    """
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    import yaml
    with open(path, "r", encoding="utf-8") as f:
        _CONFIG_CACHE = yaml.safe_load(f)
    return _CONFIG_CACHE


def get_db_path(config_path: str = "config.yaml") -> str:
    """
    从配置获取数据库路径

    Args:
        config_path: 配置文件路径

    Returns:
        数据库文件路径
    """
    cfg = load_config(config_path)
    return cfg.get("system", {}).get("db_path", "data/stock_news.db")


def get_data_dir(config_path: str = "config.yaml") -> str:
    """从配置获取数据目录"""
    cfg = load_config(config_path)
    return cfg.get("system", {}).get("data_dir", "data")


def get_log_dir(config_path: str = "config.yaml") -> str:
    """从配置获取日志目录"""
    cfg = load_config(config_path)
    return cfg.get("system", {}).get("log_dir", "logs")
