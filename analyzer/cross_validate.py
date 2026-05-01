"""
跨源情报交叉验证模块

功能：
从社区/非官方来源（雪球、股吧）获取的情报 → 去官方公告库检索确认 → 打可信度标签

使用方式：
    from analyzer.cross_validate import CrossValidator

    validator = CrossValidator()
    result = validator.verify_news(news_item)
    # {'verified': True, 'confidence': 0.85, 'tag': '⚠️ 已核实', 'evidence': [...]}

设计原则：
- 所有外部调用含 try/except，不中断流程
- 全异步友好（可同步使用）
- 内存占用小，适合 2GB 服务器
"""
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# ───────────────────────────────────
# 可信度标签常量
# ───────────────────────────────────

CREDIBILITY_TAGS = {
    "confirmed": "✅ 确认",       # 官方来源直接确认
    "verified": "⚠️ 已核实",     # 权威媒体验证通过
    "pending": "📋 待核实",       # 逻辑合理但无官方背书
    "rumor": "❓ 传闻",           # 社区来源无官方背书
    "doubtful": "🚫 存疑",        # 明显虚假或负面异常
}

# 官方来源列表
OFFICIAL_SOURCES = {
    "巨潮资讯", "巨潮", "cninfo",
    "上海证券交易所", "上交所", "sse",
    "深圳证券交易所", "深交所", "szse",
    "北京证券交易所", "北交所", "bse",
    "中国人民银行", "央行", "pboc",
    "国家发改委", "发改委", "ndrc",
    "中国证监会", "证监会", "csrc",
    "国家统计局", "统计局",
    "交易所公告",
}

# 权威财经媒体（可信度较高）
AUTHORITY_MEDIA = {
    "东方财富", "东财", "eastmoney",
    "财联社", "cls",
    "华尔街见闻", "wallstreetcn",
    "证券时报", "stcn",
    "中国证券报", "中证报",
    "上海证券报",
    "第一财经",
    "经济日报", "人民日报",
    "新华网", "新华社",
    "21世纪经济报道",
    "每日经济新闻",
    "经济观察报",
    "新浪财经", "新浪",
    "腾讯证券", "腾讯财经",
    "同花顺", "10jqka",
    "金十数据", "jin10",
}

# 社区/非官方来源（需要交叉验证）
COMMUNITY_SOURCES = {
    "雪球", "xueqiu",
    "股吧", "东方财富股吧", "guba",
    "微博", "weibo",
    "知乎", "zhihu",
    "贴吧", "百度贴吧",
    "小红书",
    "抖音",
    "微信群",
    "QQ群",
    "论坛",
    "博客",
}

# A股代码正则
STOCK_CODE_PATTERN = re.compile(r'\b(?:6\d{5}|0\d{5}|3\d{5}|4\d{5}|8\d{5})\b')

# 主动词列表（用于事件类型识别）
EVENT_KEYWORDS = {
    "业绩预增": ["业绩预增", "大幅上升", "净利润增长", "营收增长", "扭亏为盈"],
    "业绩预亏": ["业绩预亏", "大幅下降", "净利润亏损", "营收下滑"],
    "高管增持": ["增持", "股东增持", "高管增持", "实际控制人增持"],
    "高管减持": ["减持", "股东减持", "高管减持", "大股东减持"],
    "股份回购": ["回购", "股份回购", "股票回购", "回购计划"],
    "分红送转": ["分红", "送转", "10送", "10转", "派息", "股息"],
    "资产重组": ["重组", "收购", "并购", "借壳", "资产注入", "定增"],
    "中标合同": ["中标", "重大合同", "订单", "签约"],
    "立案调查": ["立案", "调查", "处罚", "罚款", "监管函", "问询函"],
    "退市风险": ["ST", "退市", "暂停上市", "风险警示"],
    "新产品": ["发布", "新品", "首发", "量产", "投产"],
    "政策利好": ["政策支持", "补贴", "减税", "降准", "降息", "放水"],
    "政策利空": ["监管", "整顿", "限购", "加息", "去杠杆", "调控"],
}


class CrossValidator:
    """
    跨源情报交叉验证引擎

    从社区/非官方来源获取的情报 → 去官方公告库检索确认 → 打可信度标签

    主要流程：
    1. 提取关键词（股票代码、公司名、事件关键词）
    2. 去 announcements 表搜索匹配的官方公告
    3. 去 policies 表搜索匹配的政策信息
    4. 去 news 表搜索权威来源报道
    5. 综合分析 → 打可信度标签
    """

    def __init__(self, db=None):
        self.db = db
        self._init_db()

    def _init_db(self):
        """延迟初始化数据库"""
        if self.db is not None:
            return
        try:
            from storage.database import Database
            self.db = Database("data/stock_news.db")
        except Exception as e:
            logger.warning(f"初始化数据库失败（跨源验证将降级）: {e}")

    # ───────────────────────────────────
    # 关键词提取
    # ───────────────────────────────────

    def _extract_stock_codes(self, text: str) -> List[str]:
        """从文本中提取 A 股股票代码"""
        return list(set(STOCK_CODE_PATTERN.findall(text)))

    def _extract_event_keywords(self, text: str) -> List[str]:
        """从文本中提取事件关键词"""
        text_lower = text.lower()
        found = []
        for event_type, keywords in EVENT_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    found.append(kw)
                    break  # 每个事件类型最多匹配一个关键词
        return found

    def _extract_keywords(self, news_item: Dict) -> Dict[str, Any]:
        """
        从一条新闻中提取验证所需的所有关键词

        Args:
            news_item: {title, source, content, published_at, ...}

        Returns:
            {
                "stock_codes": ["600519"],
                "company_names": ["贵州茅台"],
                "event_keywords": ["业绩预增"],
                "search_terms": ["贵州茅台 业绩预增"],
            }
        """
        title = news_item.get("title", "")
        content = news_item.get("content", "") or news_item.get("summary", "")
        full_text = f"{title} {content}"

        # 提取股票代码
        stock_codes = self._extract_stock_codes(full_text)

        # 提取公司名（从数据库查询股票名）
        company_names = []
        if stock_codes and self.db:
            try:
                conn = self.db._connect()
                for code in stock_codes:
                    row = conn.execute(
                        "SELECT name FROM stocks WHERE code = ?",
                        (code,)
                    ).fetchone()
                    if row:
                        company_names.append(row["name"])
                self.db._close(conn)
            except Exception:
                pass

        # 如果没有代码，尝试从标题里找已知的股票名
        if not company_names and self.db:
            try:
                conn = self.db._connect()
                rows = conn.execute(
                    "SELECT code, name FROM stocks"
                ).fetchall()
                for row in rows:
                    if row["name"] in title or row["name"] in content:
                        stock_codes.append(row["code"])
                        company_names.append(row["name"])
                        break  # 只取第一个匹配的
                self.db._close(conn)
            except Exception:
                pass

        # 提取事件关键词
        event_keywords = self._extract_event_keywords(full_text)

        # 构造搜索词
        search_terms = []
        if company_names and event_keywords:
            for name in company_names:
                for kw in event_keywords:
                    search_terms.append(f"{name} {kw}")
        elif company_names:
            search_terms = [name for name in company_names]
        elif event_keywords:
            search_terms = event_keywords

        # 确保代码去重
        stock_codes = list(set(stock_codes))

        return {
            "stock_codes": stock_codes,
            "company_names": company_names,
            "event_keywords": event_keywords,
            "search_terms": search_terms,
        }

    # ───────────────────────────────────
    # 数据库搜索
    # ───────────────────────────────────

    def _search_announcements(self, stock_codes: List[str],
                               event_keywords: List[str],
                               published_at: Optional[str] = None,
                               days_window: int = 7) -> List[Dict]:
        """
        在 announcements 表搜索匹配的官方公告

        Args:
            stock_codes: 股票代码列表
            event_keywords: 事件关键词列表
            published_at: 新闻发布时间（字符串ISO）
            days_window: 搜索窗口天数（发布时间前后几天）

        Returns:
            匹配的公告列表
        """
        if not self.db or not stock_codes:
            return []

        results = []
        try:
            conn = self.db._connect()

            # 计算日期范围
            start_date = None
            end_date = None
            if published_at:
                try:
                    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                    start_date = (dt - timedelta(days=days_window)).strftime("%Y-%m-%d")
                    end_date = (dt + timedelta(days=days_window)).strftime("%Y-%m-%d")
                except Exception:
                    pass

            for code in stock_codes:
                sql = """
                    SELECT stock_code, title, announce_type, summary, publish_date
                    FROM announcements
                    WHERE stock_code = ?
                """
                params = [code]

                # 事件关键词筛选
                if event_keywords:
                    like_clauses = []
                    for kw in event_keywords:
                        like_clauses.append("(title LIKE ? OR summary LIKE ?)")
                        params.append(f"%{kw}%")
                        params.append(f"%{kw}%")
                    sql += " AND (" + " OR ".join(like_clauses) + ")"

                # 日期范围
                if start_date and end_date:
                    sql += " AND publish_date >= ? AND publish_date <= ?"
                    params.append(start_date)
                    params.append(end_date)

                sql += " ORDER BY publish_date DESC LIMIT 10"

                rows = conn.execute(sql, params).fetchall()
                for row in rows:
                    results.append({
                        "type": "announcement",
                        "stock_code": row["stock_code"],
                        "title": row["title"],
                        "announce_type": row["announce_type"],
                        "summary": row["summary"],
                        "publish_date": row["publish_date"],
                    })

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"搜索公告失败: {e}")

        return results

    def _search_policies(self, event_keywords: List[str],
                          published_at: Optional[str] = None,
                          days_window: int = 14) -> List[Dict]:
        """
        在 policies 表搜索匹配的政策信息

        Args:
            event_keywords: 事件关键词列表
            published_at: 新闻发布时间
            days_window: 搜索窗口天数

        Returns:
            匹配的政策信息列表
        """
        if not self.db or not event_keywords:
            return []

        results = []
        try:
            conn = self.db._connect()

            start_date = None
            end_date = None
            if published_at:
                try:
                    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                    start_date = (dt - timedelta(days=days_window)).strftime("%Y-%m-%d")
                    end_date = (dt + timedelta(days=days_window)).strftime("%Y-%m-%d")
                except Exception:
                    pass

            like_clauses = []
            params = []
            for kw in event_keywords:
                like_clauses.append("(title LIKE ? OR summary LIKE ? OR full_text LIKE ?)")
                params.append(f"%{kw}%")
                params.append(f"%{kw}%")
                params.append(f"%{kw}%")

            if not like_clauses:
                return results

            sql = """
                SELECT title, source, department, summary, publish_date
                FROM policies
                WHERE (""" + " OR ".join(like_clauses) + """)"""

            if start_date and end_date:
                sql += " AND publish_date >= ? AND publish_date <= ?"
                params.append(start_date)
                params.append(end_date)

            sql += " ORDER BY publish_date DESC LIMIT 10"

            rows = conn.execute(sql, params).fetchall()
            for row in rows:
                results.append({
                    "type": "policy",
                    "title": row["title"],
                    "source": row["source"],
                    "department": row["department"],
                    "summary": row["summary"],
                    "publish_date": row["publish_date"],
                })

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"搜索政策失败: {e}")

        return results

    def _search_authority_news(self, stock_codes: List[str],
                                company_names: List[str],
                                event_keywords: List[str],
                                published_at: Optional[str] = None,
                                days_window: int = 3) -> List[Dict]:
        """
        在 news 表搜索权威来源的报道

        Args:
            stock_codes: 股票代码列表
            company_names: 公司名列表
            event_keywords: 事件关键词
            published_at: 新闻发布时间
            days_window: 搜索窗口天数

        Returns:
            匹配的权威新闻列表
        """
        if not self.db:
            return []

        if not stock_codes and not company_names and not event_keywords:
            return []

        results = []
        try:
            conn = self.db._connect()

            # 构建搜索词 - 优先用代码关联的标题匹配
            search_terms = set()
            for code in stock_codes:
                # 通过 news_stocks 表关联
                rows = conn.execute("""
                    SELECT n.id, n.title, n.source, n.published_at, n.summary
                    FROM news n
                    JOIN news_stocks ns ON n.id = ns.news_id
                    WHERE ns.stock_code = ?
                    ORDER BY n.published_at DESC
                    LIMIT 20
                """, (code,)).fetchall()

                for row in rows:
                    source = (row["source"] or "").strip()
                    # 只保留权威媒体
                    if any(auth in source for auth in AUTHORITY_MEDIA):
                        results.append({
                            "type": "news",
                            "id": row["id"],
                            "title": row["title"],
                            "source": source,
                            "published_at": row["published_at"],
                            "summary": row["summary"],
                        })

            # 如果用代码关联没找到，用关键词模糊搜索权威来源
            if not results and event_keywords:
                like_clauses = []
                params = []
                for kw in event_keywords:
                    like_clauses.append("(title LIKE ? OR summary LIKE ?)")
                    params.append(f"%{kw}%")
                    params.append(f"%{kw}%")

                if like_clauses:
                    sql = """
                        SELECT id, title, source, published_at, summary
                        FROM news
                        WHERE (""" + " OR ".join(like_clauses) + """)"""

                    # 只搜索权威来源
                    auth_like = []
                    for auth in AUTHORITY_MEDIA:
                        auth_like.append("source LIKE ?")
                        params.append(f"%{auth}%")
                    sql += " AND (" + " OR ".join(auth_like) + ")"

                    # 时间窗口
                    if published_at:
                        try:
                            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                            start = (dt - timedelta(days=days_window)).strftime("%Y-%m-%d %H:%M:%S")
                            end = (dt + timedelta(days=days_window)).strftime("%Y-%m-%d %H:%M:%S")
                            sql += " AND published_at >= ? AND published_at <= ?"
                            params.append(start)
                            params.append(end)
                        except Exception:
                            pass

                    sql += " ORDER BY published_at DESC LIMIT 10"

                    rows = conn.execute(sql, params).fetchall()
                    for row in rows:
                        results.append({
                            "type": "news",
                            "id": row["id"],
                            "title": row["title"],
                            "source": row["source"],
                            "published_at": row["published_at"],
                            "summary": row["summary"],
                        })

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"搜索权威新闻失败: {e}")

        return results

    # ───────────────────────────────────
    # 可信度标签
    # ───────────────────────────────────

    def tag_credibility(self, source: str, verified: bool = False,
                        confidence: float = 0.0) -> str:
        """
        根据来源和验证结果打可信度标签

        Args:
            source: 新闻来源
            verified: 是否已验证
            confidence: 置信度 0~1

        Returns:
            可信度标签（含 emoji）
        """
        # 官方来源直接确认
        if any(official in source for official in OFFICIAL_SOURCES):
            return CREDIBILITY_TAGS["confirmed"]

        # 权威媒体验证通过
        if verified and confidence >= 0.8:
            return CREDIBILITY_TAGS["verified"]

        # 权威媒体未找到但逻辑合理
        if verified and 0.4 <= confidence < 0.8:
            return CREDIBILITY_TAGS["pending"]

        # 社区来源无官方背书
        if any(comm in source for comm in COMMUNITY_SOURCES):
            if confidence < 0.4:
                return CREDIBILITY_TAGS["rumor"]
            return CREDIBILITY_TAGS["pending"]

        # 权威媒体但验证未通过
        if any(auth in source for auth in AUTHORITY_MEDIA):
            if confidence >= 0.7:
                return CREDIBILITY_TAGS["verified"]
            return CREDIBILITY_TAGS["pending"]

        # 默认
        return CREDIBILITY_TAGS["pending"]

    # ───────────────────────────────────
    # 核验核心逻辑
    # ───────────────────────────────────

    def verify_news(self, news_item: Dict) -> Dict:
        """
        交叉验证一条新闻的可信度

        Args:
            news_item: 新闻/帖子数据
                {
                    "title": str,
                    "source": str,
                    "content": str,
                    "summary": str,
                    "published_at": str (ISO格式),
                }

        Returns:
            {
                "verified": bool,      # 是否被官方/权威证实
                "confidence": float,   # 置信度 0~1
                "tag": str,            # 可信度标签（含 emoji）
                "evidence": [          # 验证依据
                    {"type": "announcement|policy|news",
                     "source": "...", "title": "...", ...}
                ],
                "conflicting": bool,   # 是否存在矛盾信息
                "recommendation": str, # 处理建议
            }
        """
        title = news_item.get("title", "")
        source = news_item.get("source", "")
        content = news_item.get("content", "") or news_item.get("summary", "")
        published_at = news_item.get("published_at") or news_item.get("created_at")

        logger.info(f"开始交叉验证: [{source}] {title[:50]}...")

        # 1. 提取关键词
        keywords = self._extract_keywords(news_item)
        stock_codes = keywords["stock_codes"]
        company_names = keywords["company_names"]
        event_keywords = keywords["event_keywords"]

        logger.info(f"  关键词: 代码={stock_codes}, 公司={company_names}, 事件={event_keywords}")

        evidence = []
        conflicting = False

        # 2. 搜索公告（最可信的验证依据）
        announcements = self._search_announcements(
            stock_codes, event_keywords, published_at
        )
        for ann in announcements:
            evidence.append({
                "type": "announcement",
                "title": ann["title"],
                "source": "交易所公告",
                "announce_type": ann.get("announce_type", ""),
                "summary": ann.get("summary", ""),
                "publish_date": ann.get("publish_date", ""),
            })

        # 3. 搜索政策
        policies = self._search_policies(event_keywords, published_at)
        for pol in policies:
            evidence.append({
                "type": "policy",
                "title": pol["title"],
                "source": pol.get("department", pol.get("source", "")),
                "summary": pol.get("summary", ""),
                "publish_date": pol.get("publish_date", ""),
            })

        # 4. 搜索权威新闻
        auth_news = self._search_authority_news(
            stock_codes, company_names, event_keywords, published_at
        )
        for n in auth_news:
            evidence.append({
                "type": "news",
                "title": n["title"],
                "source": n.get("source", ""),
                "summary": n.get("summary", ""),
                "published_at": n.get("published_at", ""),
            })

        # 5. 综合分析
        has_announcement = any(e["type"] == "announcement" for e in evidence)
        has_policy = any(e["type"] == "policy" for e in evidence)
        has_authority_news = any(e["type"] == "news" for e in evidence)

        # 判断来源类型
        is_official = any(official in source for official in OFFICIAL_SOURCES)
        is_community = any(comm in source for comm in COMMUNITY_SOURCES)
        is_authority = any(auth in source for auth in AUTHORITY_MEDIA)

        # 计算置信度
        confidence = 0.0

        if is_official:
            # 官方来源 → 置信度 1.0
            confidence = 1.0
        elif has_announcement:
            # 有官方公告验证
            confidence = 0.85 if has_authority_news else 0.75
        elif has_policy and has_authority_news:
            # 政策 + 权威报道双重确认
            confidence = 0.80
        elif has_policy:
            # 仅有政策匹配
            confidence = 0.70
        elif has_authority_news:
            # 仅有权威媒体报道
            confidence = 0.65
        elif is_authority:
            # 权威来源自身，无其它验证
            confidence = 0.55
        elif is_community:
            # 社区来源，无任何验证
            confidence = 0.20
            # 如果有股票代码和事件关键词，稍微提升
            if stock_codes and event_keywords:
                confidence = 0.30
            elif stock_codes:
                confidence = 0.25
        else:
            # 其他来源
            confidence = 0.40

        # 6. 矛盾检测
        if has_announcement and not has_authority_news and is_community:
            # 社区消息有公告但无权威报道→可能可信，但需要关注
            pass
        if event_keywords and not has_announcement and not has_policy:
            # 有事件关键词但没有任何官方/验证
            if is_community:
                confidence = min(confidence, 0.15)
                conflicting = True

        # 置信度区间裁剪
        confidence = max(0.0, min(1.0, confidence))

        # 7. 打标签
        tag = self.tag_credibility(source, has_announcement or has_policy, confidence)

        # 8. 生成处理建议
        recommendation = self._generate_recommendation(tag, confidence, is_community)

        # 9. 汇总
        verified = confidence >= 0.7

        result = {
            "verified": verified,
            "confidence": round(confidence, 4),
            "tag": tag,
            "evidence": evidence,
            "conflicting": conflicting,
            "recommendation": recommendation,
        }

        logger.info(f"  结果: {tag} (置信度={confidence:.2f}, 验证={verified}, "
                    f"证据={len(evidence)}条, 矛盾={conflicting})")

        return result

    def _generate_recommendation(self, tag: str, confidence: float,
                                  is_community: bool) -> str:
        """根据验证结果生成处理建议"""
        if tag == CREDIBILITY_TAGS["confirmed"]:
            return "可直接采用，作为分析核心依据"
        elif tag == CREDIBILITY_TAGS["verified"]:
            return "可优先采用，结合权威来源确认"
        elif tag == CREDIBILITY_TAGS["pending"]:
            return "可作为参考，需自行核实后再做判断"
        elif tag == CREDIBILITY_TAGS["rumor"]:
            if is_community:
                return "社区传闻，不可作为投资依据，建议观望"
            return "可信度不足，建议进一步核实"
        elif tag == CREDIBILITY_TAGS["doubtful"]:
            return "信息存疑，建议排除或标记为噪音"
        return "建议谨慎对待"

    # ───────────────────────────────────
    # 批量验证
    # ───────────────────────────────────

    def batch_verify(self, news_list: List[Dict]) -> List[Dict]:
        """
        批量验证多条新闻

        Args:
            news_list: 新闻列表

        Returns:
            每条新闻添加验证字段后的列表
        """
        results = []
        for item in news_list:
            try:
                verification = self.verify_news(item)
                item["cross_verify"] = verification
                item["credibility_tag"] = verification["tag"]
                item["verified"] = 1 if verification["verified"] else 0
                item["evidence"] = json.dumps(
                    verification["evidence"], ensure_ascii=False
                )
                results.append(item)
            except Exception as e:
                logger.error(f"验证新闻失败: {e}", exc_info=True)
                item["cross_verify"] = {
                    "verified": False,
                    "confidence": 0.0,
                    "tag": CREDIBILITY_TAGS["doubtful"],
                    "evidence": [],
                    "conflicting": False,
                    "recommendation": f"验证异常: {e}",
                }
                item["credibility_tag"] = CREDIBILITY_TAGS["doubtful"]
                item["verified"] = 0
                item["evidence"] = json.dumps([])
                results.append(item)

        return results

    # ───────────────────────────────────
    # 待验证新闻查询
    # ───────────────────────────────────

    def get_unverified_alerts(self, days: int = 1) -> List[Dict]:
        """
        获取近期未经验证的传闻（需要关注的）

        从 news 表中查找社区来源、未验证、关注度高的新闻

        Args:
            days: 近几天

        Returns:
            待验证新闻列表
        """
        if not self.db:
            return []

        alerts = []
        try:
            conn = self.db._connect()
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            # 查找未经验证的社区来源新闻
            like_clauses = []
            params = [cutoff]
            for comm in COMMUNITY_SOURCES:
                like_clauses.append("source LIKE ?")
                params.append(f"%{comm}%")

            sql = """
                SELECT id, title, source, summary, published_at
                FROM news
                WHERE published_at >= ?
                  AND (""" + " OR ".join(like_clauses) + """)
                  AND (credibility_tag IS NULL OR credibility_tag = ''
                       OR credibility_tag LIKE '%待核实%' OR credibility_tag LIKE '%传闻%')
                ORDER BY published_at DESC
                LIMIT 50
            """

            rows = conn.execute(sql, params).fetchall()
            for row in rows:
                alerts.append({
                    "id": row["id"],
                    "title": row["title"],
                    "source": row["source"],
                    "summary": row.get("summary", ""),
                    "published_at": row["published_at"],
                    "type": "unverified_rumor",
                })

            self.db._close(conn)
        except Exception as e:
            logger.warning(f"查询未验证新闻失败: {e}")

        return alerts

    # ───────────────────────────────────
    # 保存验证结果到数据库
    # ───────────────────────────────────

    def save_verification(self, news_id: int, verification: Dict) -> bool:
        """
        将验证结果写入数据库 news 表

        Args:
            news_id: 新闻ID
            verification: verify_news() 返回的结果

        Returns:
            bool: 是否成功
        """
        if not self.db:
            return False

        try:
            conn = self.db._connect()
            conn.execute("""
                UPDATE news SET
                    credibility_tag = ?,
                    verified = ?,
                    evidence = ?
                WHERE id = ?
            """, (
                verification.get("tag", ""),
                1 if verification.get("verified") else 0,
                json.dumps(verification.get("evidence", []), ensure_ascii=False),
                news_id,
            ))
            conn.commit()
            self.db._close(conn)
            return True
        except Exception as e:
            logger.error(f"保存验证结果失败 (news_id={news_id}): {e}")
            return False


# ═══════════════════════════════════════════════════
# 模块测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    validator = CrossValidator()

    # 测试用例
    test_news = [
        {
            "title": "茅台2024年净利润同比增长15%，业绩超预期",
            "source": "雪球",
            "content": "贵州茅台发布业绩预告，预计2024年净利润同比增长15%",
            "published_at": "2025-01-20T10:30:00",
        },
        {
            "title": "央行宣布降准50个基点，释放长期资金约1万亿",
            "source": "财联社",
            "content": "中国人民银行决定下调金融机构存款准备金率0.5个百分点",
            "published_at": "2025-01-20T09:00:00",
        },
        {
            "title": "某游资大佬称下周A股暴跌",
            "source": "股吧",
            "content": "某知名游资在股吧发表言论认为下周大盘将暴跌",
            "published_at": "2025-01-19T15:00:00",
        },
    ]

    print("=" * 60)
    print("跨源情报交叉验证模块测试")
    print("=" * 60)

    for news in test_news:
        print(f"\n📰 [{news['source']}] {news['title']}")
        result = validator.verify_news(news)
        print(f"  标签: {result['tag']}")
        print(f"  置信度: {result['confidence']:.2f}")
        print(f"  验证结果: {'✅ 已证实' if result['verified'] else '❌ 未证实'}")
        print(f"  建议: {result['recommendation']}")
        if result['evidence']:
            print(f"  证据 ({len(result['evidence'])} 条):")
            for e in result['evidence'][:3]:
                print(f"    [{e['type']}] {e['title'][:40]}...")
        if result['conflicting']:
            print(f"  ⚠️ 存在矛盾信息")

    # 查询未验证新闻
    print(f"\n{'=' * 60}")
    print("近期未经验证的传闻:")
    alerts = validator.get_unverified_alerts(days=7)
    if alerts:
        for a in alerts[:5]:
            print(f"  📌 [{a['source']}] {a['title'][:50]}")
    else:
        print("  ✅ 暂未发现待验证传闻")
