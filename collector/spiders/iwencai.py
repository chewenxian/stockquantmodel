"""
问财 iWencai 数据源：使用 news-search + announcement-search 技能 API
数据源：https://openapi.iwencai.com

提供两个采集器：
- IwencaiNewsCollector: 财经新闻采集
- IwencaiAnnouncementCollector: 金融公告采集

API 说明：
- 接口：POST /v1/comprehensive/search
- channels: news → 新闻, announcement → 公告
- 需要 IWENCAI_API_KEY 环境变量
"""
import json
import os
import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import requests

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class IwencaiBaseCollector(BaseCollector):
    """
    问财 API 基类
    封装了公共的 API 调用逻辑
    """

    API_URL = "https://openapi.iwencai.com/v1/comprehensive/search"
    SKILL_ID = None  # 子类覆盖
    SKILL_VERSION = "1.0.0"
    CHANNEL = None  # 子类覆盖: "news" or "announcement"

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.api_key = os.environ.get("IWENCAI_API_KEY", "")
        if not self.api_key:
            logger.warning("[问财] IWENCAI_API_KEY 未设置，技能无法使用")

    def _call_api(self, query: str) -> Optional[Dict]:
        """
        调用问财 API

        Args:
            query: 搜索关键词（如 "贵州茅台 最新消息"）

        Returns:
            API 响应 JSON，失败返回 None
        """
        if not self.api_key:
            return None

        trace_id = secrets.token_hex(32)  # 64 字符
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
                # 检查是否返回有效数据
                code = data.get("code")
                if code and code != 0:
                    logger.warning(f"[问财] API 返回错误码: {code}, msg={data.get('msg','')}")
                    return None
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"[问财] API 调用失败 (尝试 {attempt+1}/3): {e}")
                if attempt < 2:
                    import time
                    time.sleep(2 ** attempt)

        return None

    def _search_stock_news(self, stock_name: str, stock_code: str,
                            keywords: str = "") -> List[Dict]:
        """
        搜索某只股票的新闻

        Args:
            stock_name: 股票名称（如 "贵州茅台"）
            stock_code: 股票代码（如 "600519"）
            keywords: 额外关键词（如 "业绩"）

        Returns:
            文章列表
        """
        # 构造查询：股票名 + 关键词
        query_parts = [stock_name]
        if keywords:
            query_parts.append(keywords)
        query = " ".join(query_parts)
        if stock_code:
            query += f" {stock_code}"

        data = self._call_api(query)
        if not data:
            return []

        items = data.get("data", [])
        if not isinstance(items, list):
            return []

        return items

    def collect(self) -> Dict[str, int]:
        """子类实现"""
        raise NotImplementedError


class IwencaiNewsCollector(IwencaiBaseCollector):
    """
    问财新闻采集器
    对每只自选股搜索最新新闻，去重后入库
    """

    SKILL_ID = "news-search"
    SKILL_VERSION = "1.0.0"
    CHANNEL = "news"

    @property
    def _tracker_key(self) -> str:
        return "iwencai_news"

    def collect(self) -> Dict[str, int]:
        if not self.api_key:
            logger.warning("[问财-新闻] API Key 未配置，跳过")
            return {"news": 0, "skipped_reason": "no_api_key"}

        # 增量检查（最小间隔15分钟）
        if not self.should_fetch(min_interval_minutes=15):
            info = self.get_last_fetch()
            logger.info(f"[问财-新闻] 跳过（上次采集 {info['last_success_at'][:19] if info else '未知'}）")
            return {"news": info["last_item_count"] if info else 0}

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("[问财-新闻] 自选股列表为空")
            return {"news": 0}

        logger.info(f"[问财-新闻] 开始采集 {len(stocks)} 只自选股的新闻...")
        total_count = 0

        for stock in stocks:
            name = stock.get("name", "")
            code = stock.get("code", "")
            if not name:
                continue

            try:
                items = self._search_stock_news(name, code)

                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        if not title:
                            continue

                        # 截取有效标题（去重由数据库 UNIQUE(url) 保证）
                        url = item.get("url", "")
                        summary = (item.get("summary") or "")[:500]
                        content = (item.get("content") or "")[:2000]
                        pub_date = item.get("publish_date", "")
                        source = item.get("source", "问财")

                        # 转 ISO 格式
                        if pub_date and "T" not in pub_date and " " in pub_date:
                            pub_date = pub_date.replace(" ", "T")

                        news_id = self.db.insert_news(
                            title=title,
                            url=url or f"iwencai://{code}/{abs(hash(title))}",
                            source="问财",
                            summary=summary,
                            content=content,
                            published_at=pub_date,
                        )

                        if news_id:
                            # 关联股票
                            self.db.link_news_stock(news_id, code, sentiment=0.0)
                            total_count += 1

                    except Exception as e:
                        logger.warning(f"[问财-新闻] 入库异常: {e}")

            except Exception as e:
                logger.warning(f"[问财-新闻] 查询异常({name}/{code}): {e}")

        self.mark_fetched(item_count=total_count)
        logger.info(f"[问财-新闻] 采集完成: {total_count} 条")
        return {"news": total_count}


class IwencaiAnnouncementCollector(IwencaiBaseCollector):
    """
    问财公告采集器
    对每只自选股搜索最新公告，去重后入库
    """

    SKILL_ID = "announcement-search"
    SKILL_VERSION = "1.0.0"
    CHANNEL = "announcement"

    @property
    def _tracker_key(self) -> str:
        return "iwencai_announcement"

    def collect(self) -> Dict[str, int]:
        if not self.api_key:
            logger.warning("[问财-公告] API Key 未配置，跳过")
            return {"announcements": 0, "skipped_reason": "no_api_key"}

        # 增量检查（公告不需要太频繁，最小间隔30分钟）
        if not self.should_fetch(min_interval_minutes=30):
            info = self.get_last_fetch()
            logger.info(f"[问财-公告] 跳过（上次采集 {info['last_success_at'][:19] if info else '未知'}）")
            return {"announcements": info["last_item_count"] if info else 0}

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("[问财-公告] 自选股列表为空")
            return {"announcements": 0}

        logger.info(f"[问财-公告] 开始采集 {len(stocks)} 只自选股的公告...")
        total_count = 0

        for stock in stocks:
            name = stock.get("name", "")
            code = stock.get("code", "")
            if not name:
                continue

            try:
                items = self._search_stock_news(name, code, keywords="公告")

                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        if not title:
                            continue

                        url = item.get("url", "")
                        summary = (item.get("summary") or "")[:500]
                        pub_date = item.get("publish_date", "")

                        # 转 ISO 格式
                        if pub_date and "T" not in pub_date and " " in pub_date:
                            pub_date = pub_date.replace(" ", "T")

                        # 判断公告类型
                        announce_type = "其他"
                        type_keywords = {
                            "业绩": ["业绩", "预告", "快报", "年报", "季报", "半年报",
                                    "财报", "营收", "净利润"],
                            "分红": ["分红", "送转", "派息", "利润分配"],
                            "回购": ["回购", "增持", "减持"],
                            "重组": ["重组", "收购", "并购", "定增", "资产注入"],
                            "处罚": ["立案", "处罚", "监管", "问询", "警示"],
                            "中标": ["中标", "合同", "订单"],
                        }
                        for atype, kws in type_keywords.items():
                            if any(kw in title for kw in kws):
                                announce_type = atype
                                break

                        success = self.db.insert_announcement(
                            code=code,
                            title=title,
                            url=url or f"iwencai://{code}/{abs(hash(title))}",
                            announce_type=announce_type,
                            summary=summary,
                            publish_date=pub_date,
                        )

                        if success:
                            total_count += 1

                    except Exception as e:
                        logger.warning(f"[问财-公告] 入库异常: {e}")

            except Exception as e:
                logger.warning(f"[问财-公告] 查询异常({name}/{code}): {e}")

        self.mark_fetched(item_count=total_count)
        logger.info(f"[问财-公告] 采集完成: {total_count} 条")
        return {"announcements": total_count}


# 快捷导入
__all__ = ["IwencaiNewsCollector", "IwencaiAnnouncementCollector"]
