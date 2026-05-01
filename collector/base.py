"""
采集基类：所有爬虫的公共基础设施
"""
import time
import logging
import random
from typing import Optional, Dict, Any
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry

from utils.logging_ext import CollectorError


logger = logging.getLogger(__name__)


class BaseCollector:
    """所有数据源采集器的基类"""

    def __init__(self, proxy: Optional[Dict[str, str]] = None):
        self.session = self._create_session(proxy)
        self.headers = {
            "User-Agent": self._random_ua(),
            "Accept": "text/html,application/json,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://finance.sina.com.cn/",
        }

    def _create_session(self, proxy: Optional[Dict[str, str]] = None) -> requests.Session:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        if proxy:
            session.proxies.update(proxy)
        session.timeout = 15
        return session

    def _random_ua(self) -> str:
        agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        ]
        return random.choice(agents)

    def get(self, url: str, params: dict = None, **kwargs) -> Optional[requests.Response]:
        """带重试和延迟的安全 GET 请求"""
        for attempt in range(3):
            try:
                time.sleep(random.uniform(0.3, 1.0))  # 请求间隔，防封
                resp = self.session.get(
                    url, params=params,
                    headers={**self.headers, **kwargs.pop("headers", {})},
                    **kwargs
                )
                resp.raise_for_status()
                # 修复编码：中文网站常被错误识别为ISO-8859-1
                if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                    resp.encoding = resp.apparent_encoding or 'utf-8'
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"[{self.__class__.__name__}] GET {url} 失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                if attempt >= 2:
                    raise CollectorError(
                        f"采集请求失败: {url}",
                        details={"url": url, "attempts": 3, "error": str(e)}
                    )
        return None

    def get_json(self, url: str, params: dict = None, **kwargs) -> Optional[dict]:
        resp = self.get(url, params, **kwargs)
        if resp:
            # JSON API强制使用UTF-8
            if 'json' in resp.headers.get('content-type', '') and resp.encoding:
                if resp.encoding.lower() == 'iso-8859-1':
                    resp.encoding = 'utf-8'
            try:
                return resp.json()
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] JSON解析失败: {e}")
        return None

    def safe_text(self, resp: Optional[requests.Response]) -> str:
        return resp.text if resp else ""

    def collect(self) -> int:
        """
        子类实现：执行一次采集，返回采集数量

        Raises:
            CollectorError: 采集失败时抛出
        """
        raise NotImplementedError

    # ───────────────────────────────────
    # 增量采集辅助
    # ───────────────────────────────────

    @property
    def _tracker_key(self) -> str:
        """增量追踪的 key，子类可覆盖"""
        return self.__class__.__name__

    def should_fetch(self, min_interval_minutes: int = 15) -> bool:
        """判断是否需要执行采集"""
        if not hasattr(self, 'db') or not self.db:
            return True
        return self.db.should_fetch(self._tracker_key, min_interval_minutes)

    def mark_fetched(self, item_count: int = 0, error: str = ""):
        """记录采集结果到 tracker"""
        if hasattr(self, 'db') and self.db:
            self.db.mark_fetched(self._tracker_key, item_count, error)

    def get_last_fetch(self):
        """查询上次采集时间"""
        if hasattr(self, 'db') and self.db:
            return self.db.get_last_fetch(self._tracker_key)
        return None
