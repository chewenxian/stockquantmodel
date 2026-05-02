#!/usr/bin/env python3
"""
🚨 实时事件推送引擎

重大消息秒级推送。采集到新数据后立即评估重要性，重要事件直接推送到 QQ + 飞书。

工作原理：
1. 每次采集完成后，检查新入库的公告/新闻
2. 用 EventFactorEngine 检测事件类型和影响等级
3. S/A 级事件 → 立即推送
4. 支持独立运行模式：python main.py watch（30秒轮询）

推送阈值：
- 🔴 S级（重大）：业绩预亏/立案调查/退市风险，置信度高 → 秒推
- 🔴 A级（重要）：业绩预增/增持/中标合同，置信度中高 → 秒推
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from output.notifier import Notifier

logger = logging.getLogger(__name__)


class RealtimePusher:
    """
    实时事件推送器
    
    集成到采集流程中：采集完成后调用 check_new_events() 检查是否有需要立即推送的事件。
    也可独立运行：python main.py watch → 每30秒轮询一次数据库。
    """

    # 需要秒级推送的事件类型（按严重程度排序）
    CRITICAL_EVENTS = {
        "立案调查": {"level": "S", "urgency": "immediate"},
        "退市风险": {"level": "S", "urgency": "immediate"},
        "业绩预亏": {"level": "A", "urgency": "immediate"},
        "业绩预增": {"level": "A", "urgency": "normal"},
        "高管减持": {"level": "A", "urgency": "immediate"},
        "高管增持": {"level": "A", "urgency": "normal"},
        "资产重组": {"level": "A", "urgency": "immediate"},
        "中标合同": {"level": "B", "urgency": "normal"},
        "股份回购": {"level": "B", "urgency": "normal"},
        "业绩变脸": {"level": "A", "urgency": "immediate"},       # 五粮液式事件
        "财务造假": {"level": "S", "urgency": "immediate"},
        "会计差错": {"level": "A", "urgency": "immediate"},       # 追溯调整
        "业绩修正": {"level": "A", "urgency": "immediate"},       # 业绩预告修正
        "预盈转亏": {"level": "S", "urgency": "immediate"},
    }

    PUSH_CHANNELS = ["qq", "feishu"]

    def __init__(self, db=None, notifier: Notifier = None):
        self.db = db
        self.notifier = notifier or Notifier()
        self._init_db()

        # 上次检查到的最大ID（用来判断增量）
        self._last_news_id = 0
        self._last_announcement_id = 0
        self._load_last_ids()

    def _init_db(self):
        """延迟加载数据库"""
        if self.db is None:
            try:
                from storage.database import Database
                self.db = Database("data/stock_news.db")
            except Exception as e:
                logger.warning(f"初始化数据库失败: {e}")

    def _load_last_ids(self):
        """加载上次处理到的最大ID"""
        if not self.db:
            return
        try:
            conn = self.db._connect()
            row = conn.execute("SELECT MAX(id) as max_id FROM news").fetchone()
            if row and row["max_id"] is not None:
                self._last_news_id = row["max_id"]
            row = conn.execute("SELECT MAX(id) as max_id FROM announcements").fetchone()
            if row and row["max_id"] is not None:
                self._last_announcement_id = row["max_id"]
            self.db._close(conn)
        except Exception:
            pass

    # ──────────────────────────────────────────
    # 事件检测
    # ──────────────────────────────────────────

    def _detect_events_from_text(self, title: str, content: str, source: str = "") -> List[Dict]:
        """
        从文本中检测重要事件

        Args:
            title: 标题
            content: 正文/摘要
            source: 来源

        Returns:
            检测到的事件列表
        """
        text = f"{title} {content}"
        events = []

        for event_type, config in self.CRITICAL_EVENTS.items():
            # 事件对应的关键词
            keyword_map = {
                "立案调查": ["立案", "立案调查", "被证监会立案", "涉嫌", "违法违规",
                             "被调查", "证监会调查", "被监管"],
                "退市风险": ["退市", "ST", "*ST", "风险警示", "终止上市", "暂停上市"],
                "业绩预亏": ["业绩预亏", "大幅下降", "净利润亏损", "业绩大幅下降",
                             "净利下滑", "由盈转亏", "预亏", "亏损扩大",
                             "利润急降", "营收大降"],
                "业绩预增": ["业绩预增", "大幅上升", "净利润增长", "扭亏为盈",
                             "业绩大幅提升", "利润大增", "同比大增",
                             "同比增长", "利润大涨", "业绩大涨", "超预期"],
                "高管减持": ["减持", "减持计划", "控股股东减持", "大股东减持",
                             "计划减持", "股份减持"],
                "高管增持": ["增持", "增持计划", "控股股东增持", "大股东增持",
                             "计划增持", "股份增持"],
                "资产重组": ["重组", "收购", "收购股权", "重大资产重组",
                             "借壳", "资产注入", "并购重组"],
                "中标合同": ["中标", "重大合同", "项目中标", "签订合同", "获得订单"],
                "股份回购": ["回购", "股份回购", "回购计划", "股票回购",
                             "计划回购"],
                "业绩变脸": ["业绩变脸", "业绩大变脸", "净利暴跌", "利润暴跌",
                             "业绩大跳水"],
                "财务造假": ["财务造假", "造假", "虚增利润", "虚增收入",
                             "伪造", "财务舞弊"],
                "会计差错": ["会计差错", "追溯调整", "更正公告", "差错更正",
                             "前期会计"],
                "业绩修正": ["业绩修正", "业绩预告修正", "修正公告",
                             "向下修正", "大幅向下修正"],
                "预盈转亏": ["预盈转亏", "盈利转亏", "业绩大亏"],
            }

            keywords = keyword_map.get(event_type, [])
            for kw in keywords:
                if kw in text:
                    events.append({
                        "event_type": event_type,
                        "level": config["level"],
                        "urgency": config["urgency"],
                        "matched_keyword": kw,
                    })
                    break

        return events

    def _get_stock_name(self, code: str) -> str:
        """根据股票代码获取名称"""
        if not self.db or not code:
            return code
        try:
            conn = self.db._connect()
            row = conn.execute("SELECT name FROM stocks WHERE code = ?", (code,)).fetchone()
            self.db._close(conn)
            return row["name"] if row else code
        except Exception:
            return code

    # ──────────────────────────────────────────
    # 增量检查新数据
    # ──────────────────────────────────────────

    def check_new_announcements(self) -> List[Dict]:
        """检查是否有新公告（增量）"""
        if not self.db:
            return []

        events = []
        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT id, stock_code, title, summary, announce_type, publish_date
                FROM announcements
                WHERE id > ?
                ORDER BY id
            """, (self._last_announcement_id,)).fetchall()

            for row in rows:
                ann = dict(row)
                detected = self._detect_events_from_text(
                    ann.get("title", ""),
                    f"{ann.get('summary', '')} {ann.get('announce_type', '')}",
                    source="公告"
                )
                if detected:
                    stock_name = self._get_stock_name(ann.get("stock_code", ""))
                    for evt in detected:
                        evt["stock_code"] = ann["stock_code"]
                        evt["stock_name"] = stock_name
                        evt["title"] = ann["title"]
                        evt["source"] = "公告"
                        evt["publish_date"] = ann["publish_date"]
                        evt["item_id"] = ann["id"]
                        events.append(evt)

                # 更新已处理的最大ID
                if ann["id"] > self._last_announcement_id:
                    self._last_announcement_id = ann["id"]

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"检查新公告异常: {e}")

        return events

    def check_new_news(self) -> List[Dict]:
        """检查是否有新新闻（增量）"""
        if not self.db:
            return []

        events = []
        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT n.id, n.title, n.summary, n.source, n.published_at,
                       ns.stock_code
                FROM news n
                LEFT JOIN news_stocks ns ON n.id = ns.news_id
                WHERE n.id > ?
                ORDER BY n.id
            """, (self._last_news_id,)).fetchall()

            # 按ID分组，合并同一条新闻关联的多个股票
            news_map = {}
            for row in rows:
                r = dict(row)
                nid = r["id"]
                if nid not in news_map:
                    news_map[nid] = {
                        "id": nid,
                        "title": r["title"],
                        "summary": r.get("summary", ""),
                        "source": r.get("source", ""),
                        "published_at": r["published_at"],
                        "stock_codes": set(),
                    }
                if r.get("stock_code"):
                    news_map[nid]["stock_codes"].add(r["stock_code"])

            for nid, item in news_map.items():
                detected = self._detect_events_from_text(
                    item["title"],
                    item.get("summary", ""),
                    source=item.get("source", "")
                )
                if detected:
                    for evt in detected:
                        codes = list(item["stock_codes"])
                        evt["stock_code"] = codes[0] if codes else ""
                        evt["stock_name"] = self._get_stock_name(codes[0]) if codes else ""
                        evt["title"] = item["title"]
                        evt["source"] = item.get("source", "")
                        evt["publish_date"] = item["published_at"]
                        evt["item_id"] = nid
                        evt["related_stocks"] = [
                            {"code": c, "name": self._get_stock_name(c)} for c in codes
                        ]
                        events.append(evt)

            if news_map:
                self._last_news_id = max(news_map.keys())

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"检查新新闻异常: {e}")

        return events

    # ──────────────────────────────────────────
    # 推送
    # ──────────────────────────────────────────

    def _build_alert_message(self, event: Dict) -> str:
        """
        构建推送消息文本

        Returns:
            Markdown 格式告警文本
        """
        level_icon = {"S": "🔴🔴🔴", "A": "🔴🔴", "B": "🟡"}.get(
            event.get("level", "B"), "⚪"
        )
        event_type = event.get("event_type", "未知事件")
        stock_name = event.get("stock_name", "")
        stock_code = event.get("stock_code", "")
        title = event.get("title", "")
        source = event.get("source", "系统")

        lines = [
            f"{level_icon} **{event_type}**",
            f"**{stock_name}** ({stock_code})" if stock_name else "",
            f"{title}" if title else "",
            f"来源: {source}",
        ]

        # 关联股票
        related = event.get("related_stocks", [])
        if len(related) > 1:
            extra = ", ".join([f"{r['name']}({r['code']})" for r in related[1:]])
            lines.append(f"关联: {extra}")

        # 影响判断
        if event_type in ["立案调查", "退市风险", "业绩预亏", "业绩变脸",
                          "财务造假", "会计差错", "业绩修正", "预盈转亏"]:
            lines.append("⚠️ **负面影响，建议关注风险**")
        elif event_type in ["业绩预增", "中标合同", "股份回购"]:
            lines.append("✅ 正面影响")
        elif event_type in ["高管增持"]:
            lines.append("✅ 正面信号")
        elif event_type in ["高管减持"]:
            lines.append("⚠️ 关注减持动向")
        elif event_type == "资产重组":
            lines.append("🔍 重大事项，关注后续进展")

        lines.append("")
        lines.append(f"⏰ {datetime.now().strftime('%H:%M:%S')}")
        lines.append("---")

        return "\n".join([l for l in lines if l])

    def push_event(self, event: Dict) -> Dict[str, bool]:
        """
        推送一条事件到所有通道

        Args:
            event: 事件字典

        Returns:
            {channel: success}
        """
        message = self._build_alert_message(event)
        results = self.notifier.push_report(message, channels=self.PUSH_CHANNELS)
        return results

    def process_new_items(self) -> int:
        """
        处理所有新增数据，推送重要事件

        Returns:
            推送的事件数量
        """
        all_events = []

        # 检查新公告
        ann_events = self.check_new_announcements()
        all_events.extend(ann_events)

        # 检查新新闻
        news_events = self.check_new_news()
        all_events.extend(news_events)

        if not all_events:
            return 0

        # 去重：同一事件类型+股票+标题合并
        seen = set()
        unique_events = []
        for evt in all_events:
            key = (evt.get("event_type", ""), evt.get("stock_code", ""), evt.get("item_id", 0))
            if key not in seen:
                seen.add(key)
                unique_events.append(evt)

        # 按严重程度排序：S级先推
        unique_events.sort(key=lambda e: (0 if e.get("level") == "S" else 1,
                                          0 if e.get("urgency") == "immediate" else 1))

        pushed = 0
        for evt in unique_events:
            result = self.push_event(evt)
            channels_ok = sum(1 for v in result.values() if v)
            level = evt.get("level", "?")
            logger.info(f"[实时推送] {level}级 {evt.get('event_type', '?')} "
                        f"{evt.get('stock_name', '')} → {channels_ok}/{len(result)} 通道成功")
            pushed += 1

        return pushed

    # ──────────────────────────────────────────
    # 独立轮询模式
    # ──────────────────────────────────────────

    def run_watch_loop(self, interval_seconds: int = 30):
        """
        独立轮询模式：每 N 秒检查一次新数据，重要事件秒级推送

        Args:
            interval_seconds: 轮询间隔（默认30秒）
        """
        logger.info(f"🚨 实时监控启动: 每 {interval_seconds}s 轮询一次")
        logger.info(f"   推送通道: {', '.join(self.PUSH_CHANNELS)}")
        logger.info(f"   上次处理: news_id={self._last_news_id}, "
                    f"announcement_id={self._last_announcement_id}")

        while True:
            try:
                count = self.process_new_items()
                if count > 0:
                    logger.info(f"  → 本次推送 {count} 条事件")
            except Exception as e:
                logger.error(f"轮询异常: {e}", exc_info=True)

            time.sleep(interval_seconds)


# 快捷入口
def run_watch(interval: int = 30):
    """启动实时监控"""
    pusher = RealtimePusher()
    pusher.run_watch_loop(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    run_watch()
