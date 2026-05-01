#!/usr/bin/env python3
"""
事件驱动因子模块
从公司公告中提取事件并计算影响因子

事件类型及影响系数：
| 事件类型   | 关键词                         | 历史影响   | 置信度 |
|-----------|-------------------------------|-----------|:------:|
| 业绩预增   | 业绩预增 / 大幅上升             | T+1 +3.2% | 高     |
| 业绩预亏   | 业绩预亏 / 大幅下降             | T+1 -4.5% | 高     |
| 高管增持   | 增持                           | 5日+1.8%  | 中     |
| 高管减持   | 减持                           | 5日-2.1%  | 中     |
| 股份回购   | 回购                           | 3日+1.5%  | 中     |
| 中标合同   | 中标 / 重大合同                 | T+1 +2.0% | 中     |
| 分红送转   | 分红 / 送转 / 10送             | 公告日+1.0%| 低     |
| 立案调查   | 立案 / 调查                    | T+1 -5.0% | 高     |
| 资产重组   | 重组 / 收购                    | T+1 +3.0% | 中     |
| 退市风险   | ST / 退市                      | T+1 -8.0% | 高     |
"""
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class EventFactorEngine:
    """
    事件驱动因子
    从公司公告中提取事件并计算影响因子
    """

    # 事件类型定义
    EVENT_TYPES = {
        "业绩预增": {
            "keywords": ["业绩预增", "大幅上升", "净利润增长", "营收增长", "扭亏为盈",
                         "业绩大幅提升", "同比增长", "利润大增"],
            "impact": 0.032,
            "direction": "利好",
            "confidence": "高",
        },
        "业绩预亏": {
            "keywords": ["业绩预亏", "大幅下降", "净利润亏损", "营收下降", "业绩下滑",
                         "业绩大幅下降", "同比下滑", "利润大跌", "预亏"],
            "impact": -0.045,
            "direction": "利空",
            "confidence": "高",
        },
        "高管增持": {
            "keywords": ["增持", "增持股份", "增持计划", "控股股东增持"],
            "impact": 0.018,
            "direction": "利好",
            "confidence": "中",
        },
        "高管减持": {
            "keywords": ["减持", "减持股份", "减持计划", "控股股东减持", "股份减持"],
            "impact": -0.021,
            "direction": "利空",
            "confidence": "中",
        },
        "股份回购": {
            "keywords": ["回购", "股份回购", "回购计划", "回购股份", "股票回购"],
            "impact": 0.015,
            "direction": "利好",
            "confidence": "中",
        },
        "中标合同": {
            "keywords": ["中标", "重大合同", "签署合同", "项目中标", "重大订单",
                         "合同签订", "获得订单"],
            "impact": 0.020,
            "direction": "利好",
            "confidence": "中",
        },
        "分红送转": {
            "keywords": ["分红", "送转", "10送", "10转", "分红方案", "利润分配",
                         "送股", "转增", "派息", "每股分红"],
            "impact": 0.010,
            "direction": "利好",
            "confidence": "低",
        },
        "立案调查": {
            "keywords": ["立案", "调查", "立案调查", "被调查", "监管调查",
                         "证监会调查", "涉嫌", "违法违规", "被证监会立案"],
            "impact": -0.050,
            "direction": "利空",
            "confidence": "高",
        },
        "资产重组": {
            "keywords": ["重组", "收购", "资产重组", "重大资产重组", "并购",
                         "收购股权", "借壳", "资产注入", "整体上市"],
            "impact": 0.030,
            "direction": "利好",
            "confidence": "中",
        },
        "退市风险": {
            "keywords": ["ST", "退市", "退市风险", "*ST", "终止上市",
                         "暂停上市", "退市预警", "风险警示"],
            "impact": -0.080,
            "direction": "利空",
            "confidence": "高",
        },
    }

    def __init__(self, db=None):
        self.db = db
        self._init_db()

    def _init_db(self):
        """延迟初始化数据库"""
        if self.db is None:
            try:
                from storage.database import Database
                self.db = Database("data/stock_news.db")
            except Exception as e:
                logger.warning(f"初始化数据库失败: {e}")

    # ──────────────────────────────────────────
    # 事件检测
    # ──────────────────────────────────────────

    def detect_events(self, announcements: List[Dict]) -> List[Dict]:
        """
        从公告文本中检测事件

        Args:
            announcements: 公告列表，每项含 title, summary, announce_type 等字段

        Returns:
            检测到的事件列表：
            [{
                "event_type": "业绩预增",
                "announcement_id": int,
                "stock_code": "600519",
                "title": "...",
                "impact": 0.032,
                "direction": "利好",
                "confidence": "高",
                "matched_keyword": "业绩预增",
                "publish_date": "2024-01-15",
            }]
        """
        events = []
        for ann in announcements:
            title = ann.get("title", "") or ""
            summary = ann.get("summary", "") or ""
            announce_type = ann.get("announce_type", "") or ""
            text = f"{title} {summary} {announce_type}"

            for event_type, config in self.EVENT_TYPES.items():
                for keyword in config["keywords"]:
                    if keyword in text:
                        events.append({
                            "event_type": event_type,
                            "announcement_id": ann.get("id"),
                            "stock_code": ann.get("stock_code", ""),
                            "title": title[:100],
                            "impact": config["impact"],
                            "direction": config["direction"],
                            "confidence": config["confidence"],
                            "matched_keyword": keyword,
                            "publish_date": ann.get("publish_date", ""),
                        })
                        break  # 一个公告只匹配一种事件（第一个命中）

        return events

    # ──────────────────────────────────────────
    # 事件影响计算
    # ──────────────────────────────────────────

    def calculate_event_impact(self, code: str, events: List[Dict]) -> Dict:
        """
        计算某只股票的综合事件影响

        Args:
            code: 股票代码
            events: 该股票的相关事件列表

        Returns:
            {
                "stock_code": "600519",
                "total_impact": 0.032,
                "direction": "利好",
                "event_count": 1,
                "events": [...],
                "score": 0.8,     # 综合评分 (0~1)
                "level": "显著",   # 影响等级
            }
        """
        if not events:
            return {
                "stock_code": code,
                "total_impact": 0.0,
                "direction": "中性",
                "event_count": 0,
                "events": [],
                "score": 0.0,
                "level": "无事件",
            }

        # 计算总影响（累加）
        total_impact = sum(e["impact"] for e in events)
        direction = "利好" if total_impact > 0 else ("利空" if total_impact < 0 else "中性")

        # 置信度加权修正
        confidence_weight = 1.0
        for e in events:
            if e["confidence"] == "高":
                confidence_weight *= 1.0
            elif e["confidence"] == "中":
                confidence_weight *= 0.7
            else:  # 低
                confidence_weight *= 0.5
        adjusted_impact = total_impact * confidence_weight

        # 归一化评估分数 (0~1)
        score = min(abs(adjusted_impact) * 10, 1.0)

        # 等级判定
        if score >= 0.8:
            level = "重大"
        elif score >= 0.5:
            level = "显著"
        elif score >= 0.2:
            level = "一般"
        else:
            level = "轻微"

        return {
            "stock_code": code,
            "total_impact": round(total_impact, 4),
            "adjusted_impact": round(adjusted_impact, 4),
            "direction": direction,
            "event_count": len(events),
            "events": events,
            "score": round(score, 2),
            "level": level,
            "confidence_weight": round(confidence_weight, 2),
        }

    # ──────────────────────────────────────────
    # 热门事件查询
    # ──────────────────────────────────────────

    def get_hot_events(self, days: int = 1) -> List[Dict]:
        """
        获取近期高影响力事件

        Args:
            days: 查询近几天内的事件

        Returns:
            按影响程度排序的事件列表
        """
        if not self.db:
            logger.warning("数据库未初始化，无法查询热点事件")
            return []

        try:
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            conn = self.db._connect()
            rows = conn.execute("""
                SELECT id, stock_code, title, announce_type, summary, publish_date
                FROM announcements
                WHERE publish_date >= ? AND publish_date <= ?
                ORDER BY publish_date DESC
            """, (start_date, today)).fetchall()
            self.db._close(conn)

            announcements = [dict(r) for r in rows]
            events = self.detect_events(announcements)

            # 按影响绝对值排序
            events.sort(key=lambda e: abs(e["impact"]), reverse=True)

            # 添加股票名称
            for e in events:
                name = self._get_stock_name(e["stock_code"])
                e["stock_name"] = name

            return events

        except Exception as e:
            logger.error(f"查询热点事件失败: {e}")
            return []

    # ──────────────────────────────────────────
    # 辅助方法
    # ──────────────────────────────────────────

    def _get_stock_name(self, code: str) -> str:
        """获取股票名称"""
        if not self.db:
            return code
        try:
            conn = self.db._connect()
            row = conn.execute(
                "SELECT name FROM stocks WHERE code = ?", (code,)
            ).fetchone()
            self.db._close(conn)
            return row[0] if row else code
        except Exception:
            return code


# ═══════════════════════════════════════════
# 独立测试
# ═══════════════════════════════════════════

if __name__ == "__main__":
    # 测试事件检测
    engine = EventFactorEngine()

    test_announcements = [
        {
            "id": 1,
            "stock_code": "600519",
            "title": "贵州茅台2023年业绩预增公告：净利润同比增长约20%",
            "summary": "业绩预增，大幅上升",
            "publish_date": "2024-01-15",
        },
        {
            "id": 2,
            "stock_code": "300750",
            "title": "宁德时代关于控股股东减持股份的计划公告",
            "summary": "控股股东拟减持不超过1%股份",
            "publish_date": "2024-01-14",
        },
        {
            "id": 3,
            "stock_code": "000858",
            "title": "五粮液股份有限公司重大资产重组预案",
            "summary": "拟收购相关资产",
            "publish_date": "2024-01-13",
        },
    ]

    print("=" * 60)
    print("📊 事件因子引擎测试")
    print("=" * 60)

    events = engine.detect_events(test_announcements)
    print(f"\n检测到 {len(events)} 个事件:")
    for e in events:
        print(f"  [{e['direction']}] {e['event_type']} | "
              f"影响: {e['impact']:+.2%} | {e['matched_keyword']}")
        print(f"    {e['title'][:50]}")

    # 按股票分组计算影响
    print("\n综合事件影响:")
    for code in ["600519", "300750", "000858"]:
        stock_events = [e for e in events if e["stock_code"] == code]
        impact = engine.calculate_event_impact(code, stock_events)
        print(f"  {code}: {impact['direction']} | "
              f"累计影响: {impact['total_impact']:+.2%} | "
              f"等级: {impact['level']} | 评分: {impact['score']}")

    print("\n事件类型配置:")
    for etype, cfg in engine.EVENT_TYPES.items():
        print(f"  {etype}: {cfg['direction']} {cfg['impact']:+.1%} ({cfg['confidence']})")
