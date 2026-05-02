"""
采集基类：所有爬虫的公共基础设施

v2.0 优化：
- 移除 GET/POST 中每次请求前的固定延迟 (time.sleep 0.3~1.0s)
- 改为按域名的智能速率限制（每个域名每秒最多 N 次请求）
- 新增批处理辅助方法（批量DB插入）
- 新增增量采集接口
"""
import time
import logging
import random
from collections import defaultdict
from typing import Optional, Dict, Any, Union, List
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry

from utils.logging_ext import CollectorError


logger = logging.getLogger(__name__)

# 全局域速率限制器
_domain_rl: Dict[str, float] = defaultdict(float)


def _rate_limit(domain: str, min_interval: float = 0.15):
    """按域名限流：确保同一域名两次请求之间至少间隔 min_interval 秒"""
    last = _domain_rl[domain]
    now = time.time()
    gap = now - last
    if gap < min_interval:
        time.sleep(min_interval - gap)
    _domain_rl[domain] = time.time()


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

    def _extract_domain(self, url: str) -> str:
        """从 URL 中提取域名用于速率限制"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc or url
        except Exception:
            return url

    def get(self, url: str, params: dict = None, **kwargs) -> Optional[requests.Response]:
        """带重试和域名限流的 GET 请求 (v2.0 移除了每次请求前的固定延迟)"""
        timeout = kwargs.pop("timeout", self.DEFAULT_TIMEOUT)
        raw_headers = kwargs.pop("headers", {})

        # 域名级速率限制（默认 150ms 间隔，约 6 QPS）
        min_interval = kwargs.pop("rate_limit", 0.15)
        domain = self._extract_domain(url)
        _rate_limit(domain, min_interval)

        # 国内金融站点绕过代理直连
        bypass_proxy = False
        if self._proxy_config:
            for d in self.NO_PROXY_DOMAINS:
                if d in url:
                    bypass_proxy = True
                    break
        if bypass_proxy:
            kwargs["proxies"] = {"http": None, "https": None}

        for attempt in range(3):
            try:
                resp = self.session.get(
                    url, params=params,
                    headers={**self.headers, **raw_headers},
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
            try:
                return resp.json()
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] JSON解析失败: {e}")
        return None

    def post(self, url: str, json: dict = None, data: dict = None,
               **kwargs) -> Optional[requests.Response]:
        """带域名限流的 POST 请求"""
        timeout = kwargs.pop("timeout", self.DEFAULT_TIMEOUT)
        raw_headers = kwargs.pop("headers", {})

        min_interval = kwargs.pop("rate_limit", 0.15)
        domain = self._extract_domain(url)
        _rate_limit(domain, min_interval)

        if self._proxy_config:
            for d in self.NO_PROXY_DOMAINS:
                if d in url:
                    kwargs["proxies"] = {"http": None, "https": None}
                    break
        for attempt in range(3):
            try:
                resp = self.session.post(
                    url, json=json, data=data,
                    headers={**self.headers, **raw_headers},
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

    def extract_article(self, html: str, url: str = "") -> Dict[str, str]:
        """
        三级降级：从HTML中提取新闻正文

        1. XPath/CSS选择器精确提取
        2. newspaper3k 智能提取（降级）
        3. body纯文本截取（兜底）

        Returns:
            {"title": str, "content": str, "parse_quality": "high" / "mid" / "low"}
        """
        result = {"title": "", "content": "", "parse_quality": "low"}
        if not html:
            return result

        # 一级：精确提取（由具体spider的解析逻辑覆盖）
        # 二级：newspaper3k 智能提取
        try:
            from newspaper import Article
            article = Article(url if url else "")
            article.set_html(html)
            article.parse()
            title = (article.title or "").strip()
            text = (article.text or "").strip()
            if len(text) > 100:
                result["title"] = title
                result["content"] = text
                result["parse_quality"] = "mid"
                return result
        except ImportError:
            pass
        except Exception:
            pass

        # 三级：body纯文本截取
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")
            body = soup.find("body") or soup
            text = body.get_text(separator="\n", strip=True)
            text = "\n".join(
                line for line in text.split("\n")
                if len(line) > 20
            )
            # 取title
            title_tag = soup.find("title")
            if title_tag:
                result["title"] = title_tag.get_text(strip=True)
            if len(text) > 50:
                result["content"] = text[:5000]
                result["parse_quality"] = "low"
        except Exception:
            pass

        return result

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
