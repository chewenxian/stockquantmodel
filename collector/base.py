"""
采集基类：所有爬虫的公共基础设施
"""
import time
import logging
import random
from typing import Optional, Dict, Any, Union
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry

from utils.logging_ext import CollectorError


logger = logging.getLogger(__name__)


class BaseCollector:
    """所有数据源采集器的基类"""

    DEFAULT_TIMEOUT = 30  # 默认请求超时秒数

    # 国内金融站点直连，不走代理（代理只用于 GitHub 等海外站点）
    NO_PROXY_DOMAINS = [
        "eastmoney.com", "push2.eastmoney.com", "emdatah5.eastmoney.com",
        "quote.eastmoney.com", "data.eastmoney.com", "so.eastmoney.com",
        "sina.com.cn", "finance.sina.com.cn", "hq.sinajs.cn",
        "vip.stock.finance.sina.com.cn", "roll.finance.sina.com.cn",
        "cninfo.com.cn",
        "gtimg.cn", "qt.gtimg.cn", "web.sqt.gtimg.cn",
        "10jqka.com.cn", "hexin.com",
        "cls.cn", "wallstreetcn.com",
        "jin10.com",
        "dfcfw.com",
    ]

    def __init__(self, proxy: Optional[Dict[str, str]] = None):
        self._proxy_config = proxy
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
        timeout = kwargs.pop("timeout", self.DEFAULT_TIMEOUT)

        # 国内金融站点绕过代理直连
        bypass_proxy = False
        if self._proxy_config:
            for domain in self.NO_PROXY_DOMAINS:
                if domain in url:
                    bypass_proxy = True
                    break
        if bypass_proxy:
            kwargs["proxies"] = {"http": None, "https": None}

        for attempt in range(3):
            try:
                time.sleep(random.uniform(0.3, 1.0))
                resp = self.session.get(
                    url, params=params,
                    headers={**self.headers, **kwargs.pop("headers", {})},
                    timeout=timeout,
                    **kwargs
                )
                resp.raise_for_status()
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

    def post(self, url: str, json: dict = None, data: dict = None,
               **kwargs) -> Optional[requests.Response]:
        """带代理绕过逻辑的 POST 请求"""
        # 国内金融站点绕过代理
        if self._proxy_config:
            for domain in self.NO_PROXY_DOMAINS:
                if domain in url:
                    kwargs["proxies"] = {"http": None, "https": None}
                    break
        timeout = kwargs.pop("timeout", self.DEFAULT_TIMEOUT)
        for attempt in range(3):
            try:
                time.sleep(random.uniform(0.3, 1.0))
                resp = self.session.post(
                    url, json=json, data=data,
                    headers={**self.headers, **kwargs.pop("headers", {})},
                    timeout=timeout,
                    **kwargs
                )
                resp.raise_for_status()
                if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                    resp.encoding = 'utf-8'
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"[{self.__class__.__name__}] POST {url} 失败 (尝试 {attempt+1}/3): {e}")
                if attempt >= 2:
                    raise CollectorError(
                        f"POST 请求失败: {url}",
                        details={"url": url, "attempts": 3, "error": str(e)}
                    )
                time.sleep(2 ** attempt)
        return None

    def safe_text(self, resp: Optional[requests.Response]) -> str:
        return resp.text if resp else ""

    def collect(self) -> Union[int, Dict[str, int]]:
        """
        子类实现：执行一次采集，返回采集数量或各分类采集结果

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
