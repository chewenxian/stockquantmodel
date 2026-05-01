"""
问财 iWencai 数据源：使用 news-search + announcement-search 技能 API

⚠️ 日限额 1000 次，所以必须精打细算：
   - 不做"每只股票逐个查"，改为"按板块/行业批量查"
   - 每天只跑 2 次（09:00 + 13:00）
   - 入库时自动从标题/内容提取股票代码做关联

API 说明：
- 接口：POST /v1/comprehensive/search
- channels: news → 新闻, announcement → 公告
- 需要 IWENCAI_API_KEY 环境变量
"""
import json
import os
import re
import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set

import requests

from ..base import BaseCollector

logger = logging.getLogger(__name__)

# A股代码正则（用来从搜索结果正文里匹配股票代码）
STOCK_CODE_RE = re.compile(r'\b(6\d{5}|0\d{5}|3\d{5}|4\d{5}|8\d{5})\b')

# 新闻频道 - 按行业批量查询
BROAD_NEWS_QUERIES = [
    "A股 热点要闻",
    "A股 最新消息",
    "上市公司 业绩预告",
]

# 可选：如果 stocks.csv 里配了行业，可以自动按行业查
# 这个列表作为兜底
BACKUP_NEWS_QUERIES = [
    "股市 行情 大盘",
]

# 公告频道 - 按市场分
BROAD_ANNOUNCEMENT_QUERIES = [
    "上交所 上市公司 公告",
    "深交所 上市公司 公告",
    "北交所 上市公司 公告",
]


class IwencaiBaseCollector(BaseCollector):
    """
    问财 API 基类
    """

    API_URL = "https://openapi.iwencai.com/v1/comprehensive/search"
    SKILL_ID = None
    SKILL_VERSION = "1.0.0"
    CHANNEL = None
    DAILY_LIMIT = 1000   # 日限额

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_key = os.environ.get("IWENCAI_API_KEY", "")
        self._usage_today = None  # 懒加载
        if not self.api_key:
            logger.warning(f"[问财-{self.CHANNEL}] IWENCAI_API_KEY 未设置，无法使用")

    # ──────────────────────────────────────
    # 用量追踪
    # ──────────────────────────────────────

    @property
    def usage_key(self) -> str:
        return f"iwencai_usage_{self.CHANNEL}"

    def _get_today_usage(self) -> int:
        """获取今日已用请求数"""
        if self._usage_today is not None:
            return self._usage_today
        self._usage_today = 0
        if not self.db:
            return 0
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            info = self.db.get_last_fetch(self.usage_key)
            if info and info.get("last_success_at", "").startswith(today):
                self._usage_today = info.get("last_item_count", 0) or 0
        except Exception:
            pass
        return self._usage_today

    def _record_usage(self, count: int):
        """记录本次用量"""
        self._usage_today = self._get_today_usage() + count
        if self.db:
            self.db.mark_fetched(self.usage_key, item_count=self._usage_today)

    def _has_quota(self, needed: int = 1) -> bool:
        """检查是否有剩余配额"""
        used = self._get_today_usage()
        remaining = self.DAILY_LIMIT - used
        if remaining < needed:
            logger.warning(f"[问财-{self.CHANNEL}] 日限额已用 {used}/{self.DAILY_LIMIT}，"
                          f"不足 {needed} 次，跳过")
            return False
        return True

    # ──────────────────────────────────────
    # API 调用
    # ──────────────────────────────────────

    def _call_api(self, query: str) -> Optional[Dict]:
        """调用问财 API（每次调用消耗 1 次配额）"""
        if not self.api_key:
            return None
        if not self._has_quota(1):
            return None

        trace_id = secrets.token_hex(32)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "X-Claw-Call-Type": "normal",
            "X-Claw-Skill-Id": self.SKILL_ID,
            "X-Claw-Skill-Version": self.SKILL_VERSION,
            "X-Claw-Plugin-Id": "none",
            "X-Claw-Plugin-Version": "none",
            "X-Claw-Trace-Id": trace_id,
        }
        payload = {
            "channels": [self.CHANNEL],
            "app_id": "AIME_SKILL",
            "query": query,
        }

        for attempt in range(3):
            try:
                resp = self.session.post(
                    self.API_URL, json=payload, headers=headers, timeout=15
                )
                resp.raise_for_status()
                data = resp.json()
                code = data.get("code")
                if code and code != 0:
                    logger.warning(f"[问财] API 错误: code={code}, msg={data.get('msg','')}")
                    self._record_usage(1)
                    return None
                self._record_usage(1)
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"[问财] API 失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    import time
                    time.sleep(2 ** attempt)

        return None

    # ──────────────────────────────────────
    # 股票代码匹配（入库时做关联）
    # ──────────────────────────────────────

    def _extract_stock_codes(self, text: str) -> List[str]:
        """从文本中提取所有 A 股代码"""
        return STOCK_CODE_RE.findall(text)

    def _match_stock_codes(self, title: str, summary: str, content: str,
                            stock_pool: List[Dict]) -> List[str]:
        """
        从标题/内容中匹配自选股代码
        先尝试代码正则，再试名称匹配
        """
        text = f"{title} {summary} {content[:2000]}"
        codes_from_pattern = self._extract_stock_codes(text)

        # 从自选股 pool 里查找
        matched = []
        seen = set()

        # 1. 代码匹配
        for s in stock_pool:
            code = s.get("code", "")
            name = s.get("name", "")
            if code in codes_from_pattern and code not in seen:
                matched.append(code)
                seen.add(code)

        # 2. 名称匹配（标题中包含股票名）
        for s in stock_pool:
            name = s.get("name", "")
            code = s.get("code", "")
            if not name or code in seen:
                continue
            if name in title or name in summary:
                matched.append(code)
                seen.add(code)

        return matched

    def collect(self) -> Dict[str, int]:
        raise NotImplementedError


class IwencaiNewsCollector(IwencaiBaseCollector):
    """
    问财新闻采集器
    策略：按行业/板块批量查，入库时自动匹配股票代码
    每天约消耗：6～10 次查询
    """

    SKILL_ID = "news-search"
    SKILL_VERSION = "1.0.0"
    CHANNEL = "news"

    @property
    def _tracker_key(self) -> str:
        return "iwencai_news"

    def collect(self) -> Dict[str, int]:
        if not self.api_key:
            return {"news": 0, "skipped": "no_api_key"}

        # 每日限跑 2 次（开盘前 + 午盘），间隔至少 3 小时
        if not self.should_fetch(min_interval_minutes=180):
            info = self.get_last_fetch()
            remaining = self.DAILY_LIMIT - self._get_today_usage()
            logger.info(f"[问财-新闻] 跳过（距上次 {info['last_success_at'][:19] if info else '未知'} "
                       f"不足3h, 今日剩余 {remaining} 次）")
            return {"news": info["last_item_count"] if info else 0}

        stock_pool = self.db.load_stocks()

        # 按行业分组（如果有 industry 字段）
        industry_groups: Dict[str, List[str]] = {}
        for s in stock_pool:
            ind = s.get("industry", "").strip()
            if ind:
                industry_groups.setdefault(ind, []).append(s)

        # 构建查询列表
        queries = list(BROAD_NEWS_QUERIES)

        # 按行业查（如果有行业信息）
        for ind, stocks in industry_groups.items():
            if len(stocks) >= 2:  # 至少2只同一行业的股票才值得单独查
                queries.append(f"{ind} 板块 最新消息")

        # 最多查 8 个不同关键词（日限额 1000，够用）
        queries = queries[:8]

        logger.info(f"[问财-新闻] 批量查询 {len(queries)} 个关键词...")
        total_count = 0

        for query in queries:
            try:
                data = self._call_api(query)
                if not data:
                    continue

                items = data.get("data", [])
                if not isinstance(items, list):
                    continue

                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        if not title:
                            continue

                        url = item.get("url", "")
                        summary = (item.get("summary") or "")[:500]
                        content = (item.get("content") or "")[:2000]
                        pub_date = item.get("publish_date", "")

                        if pub_date and "T" not in pub_date and " " in pub_date:
                            pub_date = pub_date.replace(" ", "T")

                        source = item.get("source", "").strip() or "问财"

                        news_id = self.db.insert_news(
                            title=title,
                            url=url or f"iwencai-news://{abs(hash(title))}",
                            source=f"问财",
                            summary=summary,
                            content=content,
                            published_at=pub_date,
                        )

                        if news_id:
                            # 自动从标题/内容匹配自选股
                            codes = self._match_stock_codes(
                                title, summary, content, stock_pool
                            )
                            for code in codes:
                                self.db.link_news_stock(news_id, code, sentiment=0.0)
                            total_count += 1

                    except Exception as e:
                        logger.warning(f"[问财-新闻] 入库异常: {e}")

            except Exception as e:
                logger.warning(f"[问财-新闻] 查询异常({query}): {e}")

        self.mark_fetched(item_count=total_count)
        logger.info(f"[问财-新闻] 完成: {total_count} 条, "
                   f"今日查询 {len(queries)} 次, "
                   f"剩余配额 {self.DAILY_LIMIT - self._get_today_usage()}")
        return {"news": total_count, "queries_used": len(queries)}


class IwencaiAnnouncementCollector(IwencaiBaseCollector):
    """
    问财公告采集器
    策略：按交易所批量查，入库时自动匹配股票代码
    每天约消耗：3 次查询
    """

    SKILL_ID = "announcement-search"
    SKILL_VERSION = "1.0.0"
    CHANNEL = "announcement"

    @property
    def _tracker_key(self) -> str:
        return "iwencai_announcement"

    def collect(self) -> Dict[str, int]:
        if not self.api_key:
            return {"announcements": 0, "skipped": "no_api_key"}

        # 每日限跑 1 次
        if not self.should_fetch(min_interval_minutes=360):
            info = self.get_last_fetch()
            remaining = self.DAILY_LIMIT - self._get_today_usage()
            logger.info(f"[问财-公告] 跳过（距上次 {info['last_success_at'][:19] if info else '未知'} "
                       f"不足6h, 今日剩余 {remaining} 次）")
            return {"announcements": info["last_item_count"] if info else 0}

        stock_pool = self.db.load_stocks()
        queries = list(BROAD_ANNOUNCEMENT_QUERIES)

        # 如果自选股比较集中，再加一个针对性查询
        if stock_pool:
            names = [s.get("name", "") for s in stock_pool if s.get("name")]
            if len(names) >= 5:
                # 挑最多 5 只一起查
                batch = " ".join(names[:5])
                queries.append(f"{batch} 公告")
                queries = queries[:5]  # 最多 5 次

        logger.info(f"[问财-公告] 批量查询 {len(queries)} 个关键词...")
        total_count = 0

        # 公告类型关键词映射
        type_keywords = {
            "业绩": ["业绩", "预告", "快报", "年报", "季报", "半年报",
                    "财报", "营收", "净利润", "一季报"],
            "分红": ["分红", "送转", "派息", "利润分配"],
            "回购": ["回购", "增持", "减持"],
            "重组": ["重组", "收购", "并购", "定增", "资产注入"],
            "处罚": ["立案", "处罚", "监管", "问询", "警示", "调查"],
            "中标": ["中标", "合同", "订单"],
        }

        for query in queries:
            try:
                data = self._call_api(query)
                if not data:
                    continue

                items = data.get("data", [])
                if not isinstance(items, list):
                    continue

                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        if not title:
                            continue

                        url = item.get("url", "")
                        summary = (item.get("summary") or "")[:500]
                        pub_date = item.get("publish_date", "")

                        if pub_date and "T" not in pub_date and " " in pub_date:
                            pub_date = pub_date.replace(" ", "T")

                        # 判断公告类型
                        announce_type = "其他"
                        for atype, kws in type_keywords.items():
                            if any(kw in title for kw in kws):
                                announce_type = atype
                                break

                        # 匹配自选股代码
                        codes = self._match_stock_codes(
                            title, summary, "", stock_pool
                        )

                        for code in codes:
                            success = self.db.insert_announcement(
                                code=code,
                                title=title,
                                url=url or f"iwencai-ann://{code}/{abs(hash(title))}",
                                announce_type=announce_type,
                                summary=summary,
                                publish_date=pub_date,
                            )
                            if success:
                                total_count += 1

                    except Exception as e:
                        logger.warning(f"[问财-公告] 入库异常: {e}")

            except Exception as e:
                logger.warning(f"[问财-公告] 查询异常({query}): {e}")

        self.mark_fetched(item_count=total_count)
        logger.info(f"[问财-公告] 完成: {total_count} 条, "
                   f"今日查询 {len(queries)} 次, "
                   f"剩余配额 {self.DAILY_LIMIT - self._get_today_usage()}")
        return {"announcements": total_count, "queries_used": len(queries)}


__all__ = ["IwencaiNewsCollector", "IwencaiAnnouncementCollector"]
