"""
SQLite 数据库模块
存储：新闻、公告、行情、资金流向、分析结果等

v2.0 新增:
- FTS5 全文搜索
- 内容去重 (content_hash)
- 字段扩展 (category, keywords, processed)
- 数据归档
- 数据库统计
"""
import json
import logging
import sqlite3
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "data/stock_news.db"):
        self._conn = None
        self.db_path = db_path
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_tables()
        self._migrate_schema()

    def _connect(self):
        """获取数据库连接。对 :memory: 数据库只创建一个共享连接。"""
        if self.db_path == ":memory:" and self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=OFF")
        if self.db_path == ":memory:":
            self._conn = conn
        return conn

    def _close(self, conn):
        """安全关闭连接，不关闭共享的 :memory: 连接"""
        if self.db_path == ":memory:":
            return  # 共享连接，不关闭
        try:
            conn.close()
        except Exception as e:
            logger.debug("关闭数据库连接时出现异常: %s", e)

    def close(self):
        """关闭所有连接（仅用于清理）"""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as e:
                logger.debug("关闭主连接时出现异常: %s", e)
            self._conn = None

    def _init_tables(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.executescript("""
            -- 1. 股票基础信息
            CREATE TABLE IF NOT EXISTS stocks (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                market TEXT DEFAULT 'SH',
                reason TEXT,
                industry TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- 2. 财经新闻
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE,
                source TEXT,
                summary TEXT,
                content TEXT,
                published_at DATETIME,
                collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url)
            );
            CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at DESC);
            CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);



            -- 3. 新闻-股票关联 + 情感分析
            CREATE TABLE IF NOT EXISTS news_stocks (
                news_id INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                relevance REAL DEFAULT 1.0,
                sentiment REAL DEFAULT 0.0,
                PRIMARY KEY(news_id, stock_code),
                FOREIGN KEY(news_id) REFERENCES news(id) ON DELETE CASCADE,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_ns_stock ON news_stocks(stock_code);

            -- 4. 公司公告
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT UNIQUE,
                announce_type TEXT,
                summary TEXT,
                publish_date DATE,
                collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_ann_stock ON announcements(stock_code);
            CREATE INDEX IF NOT EXISTS idx_ann_date ON announcements(publish_date DESC);

            -- 5. 实时行情快照
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                price REAL,
                change_pct REAL,
                volume REAL,
                amount REAL,
                high REAL,
                low REAL,
                open REAL,
                turnover_rate REAL,
                pe REAL,
                pb REAL,
                total_mv REAL,
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_ms_stock_time ON market_snapshots(stock_code, snapshot_time DESC);

            -- 6. 资金流向
            CREATE TABLE IF NOT EXISTS money_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date DATE,
                main_net REAL,         -- 主力净流入
                retail_net REAL,       -- 散户净流入
                north_net REAL,        -- 北向净流入
                large_order_net REAL,  -- 大单净流入
                total_amount REAL,     -- 总成交额
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_mf_stock ON money_flow(stock_code, date DESC);

            -- 7. 龙虎榜
            CREATE TABLE IF NOT EXISTS dragon_tiger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                trade_date DATE,
                buy_amount REAL,
                sell_amount REAL,
                net_amount REAL,
                reason TEXT,
                top_buyers TEXT,       -- JSON
                top_sellers TEXT,      -- JSON
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_dt_date ON dragon_tiger(trade_date DESC);
            CREATE INDEX IF NOT EXISTS idx_dt_stock ON dragon_tiger(stock_code);

            -- 8. 板块行情
            CREATE TABLE IF NOT EXISTS board_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                board_name TEXT,
                board_code TEXT,
                change_pct REAL,
                leader_stocks TEXT,    -- 领涨股 JSON
                snapshot_time DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_bi_time ON board_index(snapshot_time DESC);

            -- 9. 宏观数据
            CREATE TABLE IF NOT EXISTS macro_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator TEXT,        -- e.g. CPI, PPI, PMI, GDP
                value REAL,
                unit TEXT,
                release_date DATE,
                source TEXT,
                summary TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_macro_indicator ON macro_data(indicator, release_date DESC);

            -- 10. 政策新闻
            CREATE TABLE IF NOT EXISTS policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE,
                source TEXT,
                department TEXT,       -- 发布部门
                summary TEXT,
                full_text TEXT,
                publish_date DATE,
                related_sectors TEXT,  -- 影响板块 JSON
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_pol_date ON policies(publish_date DESC);
            CREATE INDEX IF NOT EXISTS idx_pol_dept ON policies(department);

            -- 11. LLM 分析结果
            CREATE TABLE IF NOT EXISTS analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date DATE NOT NULL,
                news_count INTEGER DEFAULT 0,
                avg_sentiment REAL DEFAULT 0.0,
                sentiment_std REAL DEFAULT 0.0,
                key_topics TEXT,
                llm_analysis TEXT,
                suggestion TEXT,
                confidence REAL,
                risk_level TEXT,       -- high / medium / low
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(stock_code) REFERENCES stocks(code)
            );
            CREATE INDEX IF NOT EXISTS idx_analysis_stock ON analysis(stock_code, date DESC);

            -- 12. 采集日志
            CREATE TABLE IF NOT EXISTS collect_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                data_type TEXT,        -- news/market/flow/announcement
                item_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'success',
                error_msg TEXT,
                started_at DATETIME,
                finished_at DATETIME
            );

            -- 13. 归档数据（新闻归档）
            CREATE TABLE IF NOT EXISTS news_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                title TEXT NOT NULL,
                url TEXT,
                source TEXT,
                summary TEXT,
                content TEXT,
                category TEXT,
                keywords TEXT,
                published_at DATETIME,
                archived_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- 14. 归档数据（公告归档）
            CREATE TABLE IF NOT EXISTS announcements_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                stock_code TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                announce_type TEXT,
                summary TEXT,
                category TEXT,
                publish_date DATE,
                archived_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- 15. 回测信号记录（无外键约束：回测表独立，可引用已删除的股票）
            CREATE TABLE IF NOT EXISTS backtest_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                signal_date DATE NOT NULL,
                suggestion TEXT,
                confidence REAL,
                sentiment REAL,
                price_at_signal REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_bts_stock_date ON backtest_signals(stock_code, signal_date DESC);

            -- 16. 回测结果（无外键约束）
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                start_date DATE,
                end_date DATE,
                total_return REAL,
                annual_return REAL,
                max_drawdown REAL,
                win_rate REAL,
                trade_count INTEGER,
                sharpe_ratio REAL,
                benchmark_return REAL,
                alpha REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_btr_stock ON backtest_results(stock_code);

            -- 17. 历史日K线数据
            CREATE TABLE IF NOT EXISTS daily_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open_price REAL,
                close_price REAL,
                high_price REAL,
                low_price REAL,
                volume REAL,
                amount REAL,
                change_pct REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, trade_date)
            );
            CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(stock_code, trade_date DESC);

            -- 18. 北向资金流入
            CREATE TABLE IF NOT EXISTS north_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE,
                sh_net REAL,
                sz_net REAL,
                total_net REAL,
                cumulative_net REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_nf_date ON north_flow(trade_date DESC);

            -- 19. 融资融券
            CREATE TABLE IF NOT EXISTS margin_trading (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                margin_balance REAL,
                short_balance REAL,
                margin_net_buy REAL,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_mt_stock_date ON margin_trading(stock_code, trade_date DESC);

            -- 20. 股吧情绪
            CREATE TABLE IF NOT EXISTS guba_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                post_count INTEGER DEFAULT 0,
                view_count INTEGER DEFAULT 0,
                bullish_ratio REAL DEFAULT 0.0,
                bearish_ratio REAL DEFAULT 0.0,
                sentiment_score REAL DEFAULT 0.0,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_gs_stock_date ON guba_sentiment(stock_code, trade_date DESC);

            -- 21. 国债收益率
            CREATE TABLE IF NOT EXISTS bond_yield (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                yield_name TEXT,
                yield_value REAL,
                unit TEXT DEFAULT '%',
                trade_date DATE,
                source TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_by_name_date ON bond_yield(yield_name, trade_date DESC);

            -- 22. 股票热度排行
            CREATE TABLE IF NOT EXISTS stock_hot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                hot_rank INTEGER,
                hot_score REAL,
                change_pct REAL,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_sh_date_rank ON stock_hot(trade_date DESC, hot_rank ASC);

            -- 23. 采集增量追踪
            CREATE TABLE IF NOT EXISTS fetch_tracker (
                source_key TEXT PRIMARY KEY,
                last_success_at DATETIME,
                last_item_count INTEGER DEFAULT 0,
                last_error TEXT,
                consecutive_failures INTEGER DEFAULT 0
            );
        """)
        conn.commit()
        self._close(conn)

    def _migrate_schema(self):
        """
        增量迁移：为新版表结构添加缺失的列
        避免因 ALTER TABLE 失败导致程序崩溃
        """
        conn = self._connect()
        cur = conn.cursor()

        migrations = [
            # news 表新增列
            ("ALTER TABLE news ADD COLUMN content_hash TEXT",
             "SELECT content_hash FROM news LIMIT 1"),
            ("ALTER TABLE news ADD COLUMN category TEXT DEFAULT '其他'",
             "SELECT category FROM news LIMIT 1"),
            ("ALTER TABLE news ADD COLUMN keywords TEXT DEFAULT ''",
             "SELECT keywords FROM news LIMIT 1"),
            ("ALTER TABLE news ADD COLUMN processed INTEGER DEFAULT 0",
             "SELECT processed FROM news LIMIT 1"),

            # announcements 表新增列
            ("ALTER TABLE announcements ADD COLUMN content_hash TEXT",
             "SELECT content_hash FROM announcements LIMIT 1"),
            ("ALTER TABLE announcements ADD COLUMN category TEXT DEFAULT '其他'",
             "SELECT category FROM announcements LIMIT 1"),

            # news 表 - title_hash 去重字段（v7.1 新增）
            ("ALTER TABLE news ADD COLUMN title_hash TEXT DEFAULT ''",
             "SELECT title_hash FROM news LIMIT 1"),

            # news 表 - 可信度字段（v7.0 新增）
            ("ALTER TABLE news ADD COLUMN credibility_tag TEXT DEFAULT ''",
             "SELECT credibility_tag FROM news LIMIT 1"),
            ("ALTER TABLE news ADD COLUMN verified BOOLEAN DEFAULT 0",
             "SELECT verified FROM news LIMIT 1"),
            ("ALTER TABLE news ADD COLUMN evidence TEXT",
             "SELECT evidence FROM news LIMIT 1"),
        ]

        for alter_sql, check_sql in migrations:
            try:
                cur.execute(check_sql)
            except sqlite3.OperationalError:
                try:
                    cur.execute(alter_sql)
                except sqlite3.OperationalError:
                    pass  # 列已存在

        conn.commit()

        # 独立创建索引（不依赖列存在性检查）
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_title_hash ON news(title_hash)")
        except sqlite3.OperationalError:
            pass  # 字段或表尚未就绪

        self._close(conn)

    # ═══════════════════════════════════════════
    # FTS5 全文搜索
    # ═══════════════════════════════════════════

    def create_fts_index(self):
        """
        为 news 表创建 FTS5 虚拟表
        支持中文全文搜索（需 SQLite 启用 FTS5）

        如果已存在则不重复创建
        """
        conn = self._connect()
        cur = conn.cursor()
        try:
            # 检查 FTS5 扩展是否可用
            cur.execute("SELECT json_extract('{\"a\":1}', '$.a')")
        except sqlite3.OperationalError:
            pass  # JSON1 可用

        try:
            # 创建 FTS5 虚拟表
            cur.executescript("""
                CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
                    title, content, summary, source,
                    content=news,
                    content_rowid=id
                );

                -- 初始同步已有数据
                INSERT OR IGNORE INTO news_fts(rowid, title, content, summary, source)
                SELECT id, title, content, summary, source FROM news;
            """)
            conn.commit()

            # 创建触发器保持同步
            cur.executescript("""
                CREATE TRIGGER IF NOT EXISTS news_ai AFTER INSERT ON news BEGIN
                    INSERT INTO news_fts(rowid, title, content, summary, source)
                    VALUES (new.id, new.title, new.content, new.summary, new.source);
                END;

                CREATE TRIGGER IF NOT EXISTS news_ad AFTER DELETE ON news BEGIN
                    INSERT INTO news_fts(news_fts, rowid) VALUES ('delete', old.id);
                END;

                CREATE TRIGGER IF NOT EXISTS news_au AFTER UPDATE ON news BEGIN
                    INSERT INTO news_fts(news_fts, rowid) VALUES ('delete', old.id);
                    INSERT INTO news_fts(rowid, title, content, summary, source)
                    VALUES (new.id, new.title, new.content, new.summary, new.source);
                END;
            """)
            conn.commit()
        except sqlite3.OperationalError as e:
            logger.debug("FTS5扩展不可用（可能未启用）: %s", e)
        finally:
            self._close(conn)

    def search_news(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        FTS5 全文搜索新闻

        Args:
            keyword: 搜索关键词（支持中文）
            limit: 返回条数上限

        Returns:
            匹配的新闻列表
        """
        conn = self._connect()
        try:
            # 对中文关键词做空格分词（FTS5 默认空格分词）
            # 将中文字符之间加空格，让 FTS5 的 unicode61 分词器能识别单个汉字
            spaced_keyword = " OR ".join(keyword.strip().split())
            if not spaced_keyword:
                spaced_keyword = keyword

            rows = conn.execute("""
                SELECT n.*, rank
                FROM news_fts
                JOIN news n ON news_fts.rowid = n.id
                WHERE news_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """, (spaced_keyword, limit)).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]

        except (sqlite3.OperationalError, sqlite3.ProgrammingError):
            # FTS5 不可用或搜索失败，回退到 LIKE 查询
            self._close(conn)
            return self._search_news_fallback(keyword, limit)

    def _search_news_fallback(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        """LIKE 模糊搜索回退"""
        conn = self._connect()
        like_pattern = f"%{keyword}%"
        rows = conn.execute("""
            SELECT * FROM news
            WHERE title LIKE ? OR content LIKE ? OR summary LIKE ?
            ORDER BY published_at DESC
            LIMIT ?
        """, (like_pattern, like_pattern, like_pattern, limit)).fetchall()
        self._close(conn)
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════
    # 内容去重
    # ═══════════════════════════════════════════

    def get_by_content_hash(self, content_hash: str) -> bool:
        """
        检查内容哈希是否已存在于news或announcements表中

        Args:
            content_hash: 内容哈希值

        Returns:
            True 表示已存在（重复）
        """
        if not content_hash:
            return False
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM news WHERE content_hash = ? LIMIT 1",
            (content_hash,)
        ).fetchone()

        if row:
            self._close(conn)
            return True

        row = conn.execute(
            "SELECT 1 FROM announcements WHERE content_hash = ? LIMIT 1",
            (content_hash,)
        ).fetchone()
        self._close(conn)
        return row is not None

    # ═══════════════════════════════════════════
    # CRUD 操作（扩展版）
    # ═══════════════════════════════════════════

    def upsert_stock(self, code: str, name: str, market: str = "SH",
                     reason: str = "", industry: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO stocks(code, name, market, reason, industry)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET name=excluded.name, reason=excluded.reason
        """, (code, name, market, reason, industry))
        conn.commit()
        self._close(conn)

    def load_stocks(self):
        conn = self._connect()
        rows = conn.execute(
            "SELECT code, name, market FROM stocks"
        ).fetchall()
        self._close(conn)
        return [dict(r) for r in rows]

    @staticmethod
    def _normalize_title(title: str) -> str:
        """标题归一化：去空格、全角转半角、去标点，用于去重匹配"""
        if not title:
            return ""
        import unicodedata
        # 全角转半角
        t = unicodedata.normalize('NFKC', title)
        # 去空格 + 常见标点
        import re
        t = re.sub(r'[\s,，。！？、；：""''（）\(\)【】\[\]《》【】「」『』…—·～\t\n\r]', '', t)
        return t[:100].lower()

    def insert_news(self, title: str, url: str, source: str,
                    summary: str = "", content: str = "",
                    published_at: str = None,
                    category: str = "其他",
                    keywords: str = "",
                    content_hash: str = "") -> Optional[int]:
        """
        插入新闻（带自动去重）

        同一内容从不同来源采集时，
        - 标题归一化后相同（48小时内）→ 合并到现有记录
        - URL相同 → 跳过
        - 标题不同 → 正常插入
        """
        try:
            conn = self._connect()

            title_hash = self._normalize_title(title)

            # 1. 先检查是否已有相同标题哈希的新闻（48小时内）
            if title_hash:
                existing = conn.execute("""
                    SELECT id, source, title FROM news
                    WHERE title_hash = ?
                    AND julianday('now') - julianday(collected_at) < 2
                    ORDER BY id ASC LIMIT 1
                """, (title_hash,)).fetchone()

                if existing:
                    existing_id = existing["id"]
                    # 合并来源
                    old_sources = (existing["source"] or "").split("/")
                    if source not in old_sources:
                        new_source = f"{existing['source']}/{source}"
                        if len(title) > len(existing["title"] or ""):
                            conn.execute("UPDATE news SET source=?, title=? WHERE id=?",
                                         (new_source, title, existing_id))
                        else:
                            conn.execute("UPDATE news SET source=? WHERE id=?",
                                         (new_source, existing_id))
                        conn.commit()
                    self._close(conn)
                    return existing_id

            # 2. 新记录：正常插入（URL UNIQUE防重）
            cur = conn.execute("""
                INSERT OR IGNORE INTO news(
                    title, url, source, summary, content,
                    published_at, category, keywords, content_hash, title_hash
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (title, url, source, summary, content,
                  published_at, category, keywords, content_hash, title_hash))
            conn.commit()
            last_id = cur.lastrowid
            self._close(conn)
            return last_id
        except Exception as e:
            logger.warning("插入新闻失败: %s", e)
            return None

    def update_news_analysis(self, news_id: int, category: str = None,
                              keywords: str = None, processed: bool = True):
        """更新新闻的分析结果字段"""
        try:
            conn = self._connect()
            updates = []
            params = []
            if category is not None:
                updates.append("category = ?")
                params.append(category)
            if keywords is not None:
                # 如果keywords是列表，转为逗号分隔字符串
                if isinstance(keywords, list):
                    keywords = ",".join(keywords)
                updates.append("keywords = ?")
                params.append(keywords)
            updates.append("processed = ?")
            params.append(1 if processed else 0)
            params.append(news_id)

            conn.execute(f"""
                UPDATE news SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.warning("更新新闻分析字段失败: %s", e)
            return False

    def link_news_stock(self, news_id: int, stock_code: str,
                         sentiment: float = 0.0):
        conn = self._connect()
        conn.execute("""
            INSERT OR IGNORE INTO news_stocks(news_id, stock_code, sentiment)
            VALUES(?, ?, ?)
        """, (news_id, stock_code, sentiment))
        conn.commit()
        self._close(conn)

    def insert_market_snapshot(self, code: str, price: float, change_pct: float,
                                volume: float, amount: float, **kwargs):
        conn = self._connect()
        conn.execute("""
            INSERT INTO market_snapshots(
                stock_code, price, change_pct, volume, amount,
                high, low, open, turnover_rate, pe, pb, total_mv
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, price, change_pct, volume, amount,
              kwargs.get('high'), kwargs.get('low'), kwargs.get('open'),
              kwargs.get('turnover_rate'), kwargs.get('pe'),
              kwargs.get('pb'), kwargs.get('total_mv')))
        conn.commit()
        self._close(conn)

    def insert_announcement(self, code: str, title: str, url: str,
                             announce_type: str = "", summary: str = "",
                             publish_date: str = None,
                             category: str = "其他",
                             content_hash: str = ""):
        try:
            conn = self._connect()
            conn.execute("""
                INSERT OR IGNORE INTO announcements(
                    stock_code, title, url, announce_type,
                    summary, publish_date, category, content_hash
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, title, url, announce_type,
                  summary, publish_date, category, content_hash))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.warning("插入公告失败: %s", e)
            return False

    def insert_dragon_tiger(self, code: str, trade_date: str,
                             net_amount: float = 0, buy_amount: float = 0,
                             sell_amount: float = 0, reason: str = "",
                             top_buyers: str = "", top_sellers: str = ""):
        """插入龙虎榜数据"""
        try:
            conn = self._connect()
            conn.execute("""
                INSERT INTO dragon_tiger(
                    stock_code, trade_date, net_amount, buy_amount,
                    sell_amount, reason, top_buyers, top_sellers
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, trade_date, net_amount, buy_amount,
                  sell_amount, reason, top_buyers, top_sellers))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.warning("插入龙虎榜数据失败: %s", e)
            return False

    def insert_money_flow(self, code: str, date: str, main_net: float = 0,
                           retail_net: float = 0, north_net: float = 0,
                           large_order_net: float = 0, total_amount: float = 0):
        conn = self._connect()
        conn.execute("""
            INSERT INTO money_flow(stock_code, date, main_net, retail_net,
                                   north_net, large_order_net, total_amount)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """, (code, date, main_net, retail_net,
              north_net, large_order_net, total_amount))
        conn.commit()
        self._close(conn)

    def insert_policy(self, title: str, url: str, source: str,
                       department: str = "", summary: str = "",
                       publish_date: str = None, related_sectors: str = ""):
        try:
            conn = self._connect()
            conn.execute("""
                INSERT OR IGNORE INTO policies(
                    title, url, source, department, summary,
                    publish_date, related_sectors
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (title, url, source, department, summary,
                  publish_date, related_sectors))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            raise DatabaseError(f"插入政策新闻失败: {e}",
                                details={"title": title[:50], "source": source})

    def insert_macro(self, indicator: str, value: float, unit: str = "",
                      release_date: str = None, source: str = "",
                      summary: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO macro_data(indicator, value, unit, release_date, source, summary)
            VALUES(?, ?, ?, ?, ?, ?)
        """, (indicator, value, unit, release_date, source, summary))
        conn.commit()
        self._close(conn)

    def log_collect(self, source: str, data_type: str, count: int = 0,
                     status: str = "success", error_msg: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO collect_logs(
                source, data_type, item_count, status, error_msg,
                started_at, finished_at
            ) VALUES(?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """, (source, data_type, count, status, error_msg))
        conn.commit()
        self._close(conn)

    # ═══════════════════════════════════════════
    # 增量采集追踪
    # ═══════════════════════════════════════════

    def get_last_fetch(self, source_key: str) -> Optional[dict]:
        """
        获取某个采集器上次采集记录

        Returns:
            dict: {last_success_at, last_item_count, consecutive_failures, last_error} 或 None
        """
        conn = self._connect()
        row = conn.execute(
            "SELECT last_success_at, last_item_count, consecutive_failures, last_error "
            "FROM fetch_tracker WHERE source_key = ?",
            (source_key,)
        ).fetchone()
        self._close(conn)
        return dict(row) if row else None

    def should_fetch(self, source_key: str, min_interval_minutes: int = 15) -> bool:
        """
        判断是否需要执行采集（距离上次采集超过最小间隔）

        Args:
            source_key: 采集器标识
            min_interval_minutes: 最小间隔（分钟）

        Returns:
            True = 需要执行采集
        """
        info = self.get_last_fetch(source_key)
        if not info or not info.get("last_success_at"):
            return True  # 从未采集过
        try:
            last_dt = datetime.fromisoformat(info["last_success_at"])
            elapsed = (datetime.now() - last_dt).total_seconds() / 60
            return elapsed >= min_interval_minutes
        except Exception as e:
            logger.warning("解析上次采集时间失败: %s", e)
            return True

    def mark_fetched(self, source_key: str, item_count: int = 0, error: str = ""):
        """
        记录采集结果

        Args:
            source_key: 采集器标识
            item_count: 本次采集到的数量
            error: 错误信息（为空表示成功）
        """
        conn = self._connect()
        now = datetime.now().isoformat()
        if error:
            conn.execute("""
                INSERT INTO fetch_tracker(source_key, last_success_at, last_item_count, last_error, consecutive_failures)
                VALUES(?, ?, ?, ?, 1)
                ON CONFLICT(source_key) DO UPDATE SET
                    last_error = excluded.last_error,
                    consecutive_failures = consecutive_failures + 1
            """, (source_key, now, item_count, error[:200]))
        else:
            conn.execute("""
                INSERT INTO fetch_tracker(source_key, last_success_at, last_item_count, last_error, consecutive_failures)
                VALUES(?, ?, ?, '', 0)
                ON CONFLICT(source_key) DO UPDATE SET
                    last_success_at = excluded.last_success_at,
                    last_item_count = excluded.last_item_count,
                    last_error = '',
                    consecutive_failures = 0
            """, (source_key, now, item_count))
        conn.commit()
        self._close(conn)

    def get_fetch_health(self) -> List[Dict]:
        """
        获取所有采集器的健康状态

        Returns:
            [{source_key, last_success_at, last_item_count, last_error, consecutive_failures, status}]
        """
        conn = self._connect()
        rows = conn.execute("""
            SELECT source_key, last_success_at, last_item_count,
                   COALESCE(last_error, '') as last_error,
                   consecutive_failures
            FROM fetch_tracker
            ORDER BY source_key
        """).fetchall()
        self._close(conn)
        result = []
        for r in rows:
            d = dict(r)
            d["status"] = (
                "error" if d["consecutive_failures"] >= 3
                else "warning" if d["consecutive_failures"] >= 1
                else "ok"
            )
            result.append(d)
        return result

    # ═══════════════════════════════════════════
    # 数据归档
    # ═══════════════════════════════════════════

    def archive_old_data(self, days: int = 30) -> Dict[str, int]:
        """
        将指定天数前的旧数据归档到备份表，并从原表删除

        Args:
            days: 归档多少天前的数据（默认30天）

        Returns:
            {"news": 归档新闻数, "announcements": 归档公告数}
        """
        conn = self._connect()
        result = {"news": 0, "announcements": 0}

        try:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            # 归档新闻
            cur = conn.execute("""
                INSERT OR IGNORE INTO news_archive(
                    original_id, title, url, source, summary, content,
                    category, keywords, published_at
                )
                SELECT id, title, url, source, summary, content,
                       category, keywords, published_at
                FROM news
                WHERE published_at < ? AND published_at IS NOT NULL
            """, (cutoff,))
            archived_news = cur.rowcount
            result["news"] = archived_news if archived_news > 0 else 0

            if archived_news > 0:
                # 同步更新 FTS（必须在 DELETE 之前执行）
                old_ids = conn.execute("""
                    SELECT id FROM news
                    WHERE published_at < ? AND published_at IS NOT NULL
                """, (cutoff,)).fetchall()
                for row in old_ids:
                    conn.execute(
                        "INSERT INTO news_fts(news_fts, rowid) VALUES ('delete', ?)",
                        (row["id"],)
                    )

                # 删除已归档的旧新闻

            # 归档公告
            cur = conn.execute("""
                INSERT OR IGNORE INTO announcements_archive(
                    original_id, stock_code, title, url, announce_type,
                    summary, category, publish_date
                )
                SELECT id, stock_code, title, url, announce_type,
                       summary, category, publish_date
                FROM announcements
                WHERE publish_date < ? AND publish_date IS NOT NULL
            """, (cutoff,))
            archived_ann = cur.rowcount
            result["announcements"] = archived_ann if archived_ann > 0 else 0

            if archived_ann > 0:
                conn.execute("""
                    DELETE FROM announcements
                    WHERE publish_date < ? AND publish_date IS NOT NULL
                """, (cutoff,))

            conn.commit()
        except Exception as e:
            logger.error("数据归档失败: %s", e)
        finally:
            self._close(conn)

        return result

    # ═══════════════════════════════════════════
    # 数据库统计
    # ═══════════════════════════════════════════

    def get_stats(self) -> Dict[str, Any]:
        """
        返回数据库统计信息

        Returns:
            {
                "total_news": int,
                "today_news": int,
                "total_announcements": int,
                "total_stocks": int,
                "total_policies": int,
                "total_analysis": int,
                "processed_news": int,
                "unprocessed_news": int,
                "db_size_mb": float,
                "oldest_news_date": str or None,
                "newest_news_date": str or None,
                "categories": {"业绩": n, "重组": n, ...},
            }
        """
        stats: Dict[str, Any] = {}
        conn = self._connect()
        try:
            # 基础计数（白名单验证，防止 SQL 注入）
            _ALLOWED_TABLES = {"news", "announcements", "stocks", "policies", "analysis"}
            for name, table in [
                ("total_news", "news"),
                ("total_announcements", "announcements"),
                ("total_stocks", "stocks"),
                ("total_policies", "policies"),
                ("total_analysis", "analysis"),
            ]:
                if table not in _ALLOWED_TABLES:
                    stats[name] = 0
                    continue
                row = conn.execute(f"SELECT COUNT(*) as cnt FROM [{table}]").fetchone()
                stats[name] = row["cnt"] if row else 0

            # 处理状态
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM news WHERE processed = 1"
            ).fetchone()
            stats["processed_news"] = row["cnt"] if row else 0

            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM news WHERE processed = 0 OR processed IS NULL"
            ).fetchone()
            stats["unprocessed_news"] = row["cnt"] if row else 0

            # 今日新闻
            row = conn.execute("""
                SELECT COUNT(*) as cnt FROM news
                WHERE date(collected_at) = date('now', 'localtime')
            """).fetchone()
            stats["today_news"] = row["cnt"] if row else 0

            # 日期范围
            row = conn.execute(
                "SELECT MIN(published_at) as min_d, MAX(published_at) as max_d FROM news"
            ).fetchone()
            if row:
                stats["oldest_news_date"] = row["min_d"]
                stats["newest_news_date"] = row["max_d"]

            # 分类统计
            rows = conn.execute("""
                SELECT category, COUNT(*) as cnt FROM news
                WHERE category IS NOT NULL AND category != ''
                GROUP BY category ORDER BY cnt DESC
            """).fetchall()
            stats["categories"] = {r["category"]: r["cnt"] for r in rows}

            # 数据库文件大小
            try:
                stats["db_size_mb"] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
            except OSError:
                stats["db_size_mb"] = 0

            # 来源分布
            rows = conn.execute("""
                SELECT source, COUNT(*) as cnt FROM news
                WHERE source IS NOT NULL AND source != ''
                GROUP BY source ORDER BY cnt DESC
            """).fetchall()
            stats["sources"] = {r["source"]: r["cnt"] for r in rows}

        except Exception as e:
            logger.warning("获取数据库统计失败: %s", e)
        finally:
            self._close(conn)

        return stats

    # ═══════════════════════════════════════════
    # 旧版统计查询（兼容）
    # ═══════════════════════════════════════════

    def get_today_news_count(self) -> int:
        stats = self.get_stats()
        return stats.get("today_news", 0)

    def get_stock_news_sentiment(self, code: str, days: int = 1):
        conn = self._connect()
        rows = conn.execute("""
            SELECT n.id, n.title, n.source, n.published_at, n.summary, n.content, ns.sentiment
            FROM news_stocks ns
            JOIN news n ON ns.news_id = n.id
            WHERE ns.stock_code = ?
              AND n.published_at >= datetime('now', ? || ' days', 'localtime')
            ORDER BY n.published_at DESC
        """, (code, f'-{days}')).fetchall()
        self._close(conn)
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════
    # 分析模块专用查询方法
    # ═══════════════════════════════════════════

    # ═══════════════════════════════════════════
    # 回测相关方法
    # ═══════════════════════════════════════════

    def record_backtest_signal(self, stock_code: str, signal_date: str,
                                 suggestion: str, confidence: float = 0.0,
                                 sentiment: float = 0.0,
                                 price_at_signal: Optional[float] = None) -> bool:
        """记录回测信号到数据库"""
        try:
            conn = self._connect()
            conn.execute("""
                INSERT INTO backtest_signals(
                    stock_code, signal_date, suggestion,
                    confidence, sentiment, price_at_signal
                ) VALUES(?, ?, ?, ?, ?, ?)
            """, (stock_code, signal_date, suggestion,
                   confidence, sentiment, price_at_signal))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.warning("记录回测信号失败 (%s): %s", stock_code, e)
            return False

    def get_backtest_signals(self, stock_code: str,
                              start_date: str = None,
                              end_date: str = None) -> List[Dict]:
        """获取某只股票的回测信号"""
        try:
            conn = self._connect()
            sql = "SELECT * FROM backtest_signals WHERE stock_code = ?"
            params = [stock_code]
            if start_date:
                sql += " AND signal_date >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND signal_date <= ?"
                params.append(end_date)
            sql += " ORDER BY signal_date ASC"
            rows = conn.execute(sql, params).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取回测信号失败 (%s): %s", stock_code, e)
            return []

    def get_all_backtest_signals(self) -> List[Dict]:
        """获取所有回测信号"""
        try:
            conn = self._connect()
            rows = conn.execute("""
                SELECT bs.*, s.name as stock_name
                FROM backtest_signals bs
                LEFT JOIN stocks s ON bs.stock_code = s.code
                ORDER BY bs.signal_date DESC
            """).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取所有回测信号失败: %s", e)
            return []

    def save_backtest_result(self, stock_code: str, start_date: str,
                              end_date: str, result: Dict) -> bool:
        """保存回测结果"""
        try:
            conn = self._connect()
            # upsert
            existing = conn.execute(
                "SELECT id FROM backtest_results WHERE stock_code = ? AND start_date = ? AND end_date = ?",
                (stock_code, start_date, end_date)
            ).fetchone()
            if existing:
                conn.execute("""UPDATE backtest_results SET
                    total_return=?, annual_return=?, max_drawdown=?,
                    win_rate=?, trade_count=?, sharpe_ratio=?,
                    benchmark_return=?, alpha=?
                    WHERE id=?
                """, (
                    result.get("total_return", 0),
                    result.get("annual_return", 0),
                    result.get("max_drawdown", 0),
                    result.get("win_rate", 0),
                    result.get("trade_count", 0),
                    result.get("sharpe_ratio", 0),
                    result.get("benchmark_return", 0),
                    result.get("alpha", 0),
                    existing["id"]
                ))
            else:
                conn.execute("""INSERT INTO backtest_results(
                    stock_code, start_date, end_date, total_return,
                    annual_return, max_drawdown, win_rate, trade_count,
                    sharpe_ratio, benchmark_return, alpha
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                    stock_code, start_date, end_date,
                    result.get("total_return", 0),
                    result.get("annual_return", 0),
                    result.get("max_drawdown", 0),
                    result.get("win_rate", 0),
                    result.get("trade_count", 0),
                    result.get("sharpe_ratio", 0),
                    result.get("benchmark_return", 0),
                    result.get("alpha", 0),
                ))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.warning("保存回测结果失败 (%s): %s", stock_code, e)
            return False

    def get_backtest_results(self, stock_code: str = None) -> List[Dict]:
        """获取回测结果"""
        try:
            conn = self._connect()
            if stock_code:
                rows = conn.execute(
                    "SELECT * FROM backtest_results WHERE stock_code = ? ORDER BY created_at DESC",
                    (stock_code,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT br.*, s.name as stock_name FROM backtest_results br LEFT JOIN stocks s ON br.stock_code = s.code ORDER BY br.created_at DESC"
                ).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取回测结果失败: %s", e)
            return []

    def get_all_backtest_signals_grouped(self) -> Dict[str, List[Dict]]:
        """按股票代码分组获取所有回测信号"""
        signals = self.get_all_backtest_signals()
        grouped = {}
        for s in signals:
            code = s["stock_code"]
            if code not in grouped:
                grouped[code] = []
            grouped[code].append(s)
        return grouped

    def get_price_history(self, code: str, start_date: str = None,
                           end_date: str = None) -> List[Dict]:
        """
        获取某只股票的历史价格序列（从market_snapshots按日聚合）
        返回按日期升序排列的{date, price, change_pct, volume}列表
        """
        try:
            conn = self._connect()
            sql = """
                SELECT date(snapshot_time) as trade_date,
                       price,
                       change_pct,
                       volume,
                       amount
                FROM market_snapshots
                WHERE stock_code = ?
            """
            params = [code]
            if start_date:
                sql += " AND date(snapshot_time) >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND date(snapshot_time) <= ?"
                params.append(end_date)
            sql += """
                GROUP BY date(snapshot_time)
                ORDER BY trade_date ASC
            """
            rows = conn.execute(sql, params).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取历史价格失败 (%s): %s", code, e)
            return []

    def get_latest_market_snapshot(self, code: str) -> Optional[Dict]:
        """获取某只股票的最新行情快照（自动处理 sh/sz 前缀）"""
        try:
            conn = self._connect()
            # 尝试精确匹配和带 sh/sz 前缀匹配
            for try_code in (code, f"sh{code}", f"sz{code}", code.removeprefix("sh").removeprefix("sz")):
                row = conn.execute("""
                    SELECT price, change_pct, volume, amount, high, low,
                           open, turnover_rate, pe, pb, total_mv
                    FROM market_snapshots
                    WHERE stock_code = ?
                    ORDER BY snapshot_time DESC LIMIT 1
                """, (try_code,)).fetchone()
                if row:
                    self._close(conn)
                    return dict(row)
            self._close(conn)
            return None
        except Exception as e:
            logger.warning("获取最新行情失败 (%s): %s", code, e)
            return None

    def get_latest_board_index(self) -> Optional[List[Dict]]:
        """获取最新板块排行数据"""
        try:
            conn = self._connect()
            rows = conn.execute("""
                SELECT board_name, board_code, change_pct
                FROM board_index
                ORDER BY snapshot_time DESC
                LIMIT 30
            """,).fetchall()
            self._close(conn)
            return [dict(r) for r in rows] if rows else None
        except Exception as e:
            logger.warning("获取板块数据失败: %s", e)
            return None

    def get_latest_money_flow(self, code: str) -> Optional[Dict]:
        """获取最新资金流向（先查个股，再查板块）"""
        try:
            conn = self._connect()
            # 先按精确 stock_code 匹配
            row = conn.execute("""
                SELECT main_net, retail_net, north_net, large_order_net, total_amount
                FROM money_flow
                WHERE stock_code = ?
                ORDER BY date DESC LIMIT 1
            """, (code,)).fetchone()
            if row:
                self._close(conn)
                return dict(row)
            # 没找到个股数据，返回板块最近数据
            row = conn.execute("""
                SELECT main_net, retail_net, north_net, large_order_net, total_amount
                FROM money_flow
                ORDER BY date DESC LIMIT 1
            """).fetchone()
            self._close(conn)
            return dict(row) if row else None
        except Exception as e:
            logger.warning("获取最新资金流向失败 (%s): %s", code, e)
            return None

    def save_analysis(self, code: str, analysis: Dict) -> bool:
        """
        保存或更新分析结果

        Args:
            code: 股票代码
            analysis: 分析数据字典

        Returns:
            bool: 是否成功
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            conn = self._connect()

            existing = conn.execute(
                "SELECT id FROM analysis WHERE stock_code = ? AND date = ?",
                (code, today)
            ).fetchone()

            if existing:
                conn.execute("""
                    UPDATE analysis SET
                        news_count = ?, avg_sentiment = ?, sentiment_std = ?,
                        key_topics = ?, llm_analysis = ?,
                        suggestion = ?, confidence = ?, risk_level = ?
                    WHERE stock_code = ? AND date = ?
                """, (
                    analysis.get("news_count", 0),
                    analysis.get("avg_sentiment", 0.0),
                    analysis.get("sentiment_std", 0.0),
                    json.dumps(analysis.get("key_topics", []), ensure_ascii=False),
                    analysis.get("summary", ""),
                    analysis.get("suggestion", "持有"),
                    analysis.get("confidence", 0.0),
                    analysis.get("risk_level", "中"),
                    code, today,
                ))
            else:
                conn.execute("""
                    INSERT INTO analysis (
                        stock_code, date, news_count, avg_sentiment,
                        sentiment_std, key_topics, llm_analysis,
                        suggestion, confidence, risk_level
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code, today,
                    analysis.get("news_count", 0),
                    analysis.get("avg_sentiment", 0.0),
                    analysis.get("sentiment_std", 0.0),
                    json.dumps(analysis.get("key_topics", []), ensure_ascii=False),
                    analysis.get("summary", ""),
                    analysis.get("suggestion", "持有"),
                    analysis.get("confidence", 0.0),
                    analysis.get("risk_level", "中"),
                ))

            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.error(f"保存分析结果失败 ({code}): {e}")
            raise DatabaseError(f"保存分析结果失败 ({code}): {e}",
                                details={"code": code})

    def get_today_analysis(self) -> List[Dict]:
        """获取所有今日分析结果"""
        try:
            conn = self._connect()
            today = datetime.now().strftime("%Y-%m-%d")
            rows = conn.execute("""
                SELECT a.*, s.name, s.market
                FROM analysis a
                LEFT JOIN stocks s ON a.stock_code = s.code
                WHERE a.date = ?
                ORDER BY ABS(a.avg_sentiment) DESC
            """, (today,)).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取今日分析结果失败: %s", e)
            return []

    def get_recent_analysis(self, code: str, limit: int = 30) -> List[Dict]:
        """获取某只股票的历史分析记录"""
        try:
            conn = self._connect()
            rows = conn.execute("""
                SELECT * FROM analysis
                WHERE stock_code = ?
                ORDER BY date DESC LIMIT ?
            """, (code, limit)).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("获取历史分析记录失败: %s", e)
            return []

    # ═══════════════════════════════════════════
    # 历史日K线数据
    # ═══════════════════════════════════════════

    def upsert_daily_price(self, stock_code: str, trade_date: str,
                           open_price: float = None, close_price: float = None,
                           high_price: float = None, low_price: float = None,
                           volume: float = None, amount: float = None,
                           change_pct: float = None) -> bool:
        """插入或更新日K线数据"""
        try:
            conn = self._connect()
            conn.execute("""
                INSERT INTO daily_prices(
                    stock_code, trade_date, open_price, close_price,
                    high_price, low_price, volume, amount, change_pct
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stock_code, trade_date) DO UPDATE SET
                    open_price=excluded.open_price,
                    close_price=excluded.close_price,
                    high_price=excluded.high_price,
                    low_price=excluded.low_price,
                    volume=excluded.volume,
                    amount=excluded.amount,
                    change_pct=excluded.change_pct
            """, (stock_code, trade_date, open_price, close_price,
                   high_price, low_price, volume, amount, change_pct))
            conn.commit()
            self._close(conn)
            return True
        except Exception as e:
            logger.error(f"插入日K线失败 ({stock_code} {trade_date}): {e}")
            return False

    def batch_upsert_daily_prices(self, stock_code: str,
                                    rows: List[Dict]) -> int:
        """
        批量插入日K线数据（单连接单事务）

        Args:
            stock_code: 股票代码
            rows: [{trade_date, open_price, close_price, high_price, low_price, volume, amount, change_pct}]

        Returns:
            成功插入/更新的数量
        """
        if not rows:
            return 0
        count = 0
        try:
            conn = self._connect()
            for r in rows:
                try:
                    conn.execute("""
                        INSERT INTO daily_prices(
                            stock_code, trade_date, open_price, close_price,
                            high_price, low_price, volume, amount, change_pct
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
                            open_price=excluded.open_price,
                            close_price=excluded.close_price,
                            high_price=excluded.high_price,
                            low_price=excluded.low_price,
                            volume=excluded.volume,
                            amount=excluded.amount,
                            change_pct=excluded.change_pct
                    """, (stock_code, r["trade_date"], r.get("open_price"),
                           r.get("close_price"), r.get("high_price"),
                           r.get("low_price"), r.get("volume"),
                           r.get("amount"), r.get("change_pct")))
                    count += 1
                except Exception as e:
                    logger.debug("跳过插入单条日K线数据 (%s): %s",
                                  r.get("trade_date", "unknown"), e)
            conn.commit()
            self._close(conn)
        except Exception as e:
            logger.error(f"批量插入日K线失败 ({stock_code}): {e}")
        return count

    def get_price_history(self, code: str, days: int = 60) -> List[Dict]:
        """
        获取历史K线数据（用于技术指标和图表）

        Args:
            code: 股票代码
            days: 获取最近多少条记录

        Returns:
            按日期升序排列的K线数据列表
        """
        try:
            conn = self._connect()
            rows = conn.execute("""
                SELECT trade_date, open_price, close_price, high_price,
                       low_price, volume, amount, change_pct
                FROM daily_prices
                WHERE stock_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
            """, (code, days)).fetchall()
            self._close(conn)
            # 按日期升序排列
            result = [dict(r) for r in rows]
            result.reverse()
            return result
        except Exception as e:
            logger.error(f"获取历史K线失败 ({code}): {e}")
            return []

    def get_price_range(self, code: str, start_date: str, end_date: str) -> List[Dict]:
        """
        获取指定日期范围的价格数据

        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            按日期升序排列的K线数据列表
        """
        try:
            conn = self._connect()
            rows = conn.execute("""
                SELECT trade_date, open_price, close_price, high_price,
                       low_price, volume, amount, change_pct
                FROM daily_prices
                WHERE stock_code = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY trade_date ASC
            """, (code, start_date, end_date)).fetchall()
            self._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"获取指定范围价格失败 ({code}): {e}")
            return []

    # ═══════════════════════════════════════════
    # 批量操作方法（采集优化专用）
    # ═══════════════════════════════════════════

    def batch_insert_news(self, items: List[Dict]) -> int:
        """
        批量插入新闻（单连接单事务极大提升性能）

        Args:
            items: [{title, url, source, summary, content, published_at, category, keywords, content_hash}]

        Returns:
            成功插入数量
        """
        if not items:
            return 0
        count = 0
        conn = self._connect()
        try:
            sql = """INSERT OR IGNORE INTO news(
                title, url, source, summary, content,
                published_at, category, keywords, content_hash
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            for item in items:
                try:
                    cur = conn.execute(sql, (
                        item.get("title", ""),
                        item.get("url", ""),
                        item.get("source", ""),
                        item.get("summary", ""),
                        item.get("content", ""),
                        item.get("published_at"),
                        item.get("category", "其他"),
                        item.get("keywords", ""),
                        item.get("content_hash", ""),
                    ))
                    if cur.lastrowid:
                        count += 1
                except Exception as e:
                    logger.debug("批量插入单条新闻跳过: %s", e)
            conn.commit()
        except Exception as e:
            logger.error("批量插入新闻事务失败: %s", e)
        finally:
            self._close(conn)
        return count

    def batch_upsert_stocks(self, items: List[Dict]) -> int:
        """
        批量更新股票信息

        Args:
            items: [{code, name, market, reason, industry}]

        Returns:
            处理数量
        """
        if not items:
            return 0
        conn = self._connect()
        sql = """INSERT INTO stocks(code, name, market, reason, industry)
                 VALUES(?, ?, ?, ?, ?)
                 ON CONFLICT(code) DO UPDATE SET
                     name=excluded.name, reason=excluded.reason"""
        for item in items:
            conn.execute(sql, (
                item.get("code", ""),
                item.get("name", ""),
                item.get("market", "SH"),
                item.get("reason", ""),
                item.get("industry", ""),
            ))
        conn.commit()
        self._close(conn)
        return len(items)

    def batch_insert_market_snapshots(self, items: List[Dict]) -> int:
        """
        批量插入行情快照

        Args:
            items: [{stock_code, price, change_pct, volume, amount, high, low, open, ...}]

        Returns:
            成功插入数量
        """
        if not items:
            return 0
        count = 0
        conn = self._connect()
        try:
            sql = """INSERT INTO market_snapshots(
                stock_code, price, change_pct, volume, amount,
                high, low, open, turnover_rate, pe, pb, total_mv
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            for item in items:
                try:
                    conn.execute(sql, (
                        item.get("stock_code", ""),
                        item.get("price", 0),
                        item.get("change_pct", 0),
                        item.get("volume", 0),
                        item.get("amount", 0),
                        item.get("high"),
                        item.get("low"),
                        item.get("open"),
                        item.get("turnover_rate"),
                        item.get("pe"),
                        item.get("pb"),
                        item.get("total_mv"),
                    ))
                    count += 1
                except Exception as e:
                    logger.debug("批量插入行情单条跳过: %s", e)
            conn.commit()
        except Exception as e:
            logger.error("批量插入行情事务失败: %s", e)
        finally:
            self._close(conn)
        return count

    def batch_insert_money_flow(self, items: List[Dict]) -> int:
        """
        批量插入资金流向数据

        Args:
            items: [{code, date, main_net, retail_net, north_net, large_order_net, total_amount}]

        Returns:
            成功插入数量
        """
        if not items:
            return 0
        count = 0
        conn = self._connect()
        try:
            sql = """INSERT INTO money_flow(
                stock_code, date, main_net, retail_net,
                north_net, large_order_net, total_amount
            ) VALUES(?, ?, ?, ?, ?, ?, ?)"""
            for item in items:
                try:
                    conn.execute(sql, (
                        item.get("code", ""),
                        item.get("date", ""),
                        item.get("main_net", 0),
                        item.get("retail_net", 0),
                        item.get("north_net", 0),
                        item.get("large_order_net", 0),
                        item.get("total_amount", 0),
                    ))
                    count += 1
                except Exception as e:
                    logger.debug("批量插入资金流向单条跳过: %s", e)
            conn.commit()
        except Exception as e:
            logger.error("批量插入资金流向事务失败: %s", e)
        finally:
            self._close(conn)
        return count

    def batch_insert_dragon_tiger(self, items: List[Dict]) -> int:
        """
        批量插入龙虎榜数据

        Args:
            items: [{code, trade_date, net_amount, buy_amount, sell_amount, reason, top_buyers, top_sellers}]

        Returns:
            成功插入数量
        """
        if not items:
            return 0
        count = 0
        conn = self._connect()
        try:
            sql = """INSERT INTO dragon_tiger(
                stock_code, trade_date, net_amount, buy_amount,
                sell_amount, reason, top_buyers, top_sellers
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"""
            for item in items:
                try:
                    conn.execute(sql, (
                        item.get("code", ""),
                        item.get("trade_date", ""),
                        item.get("net_amount", 0),
                        item.get("buy_amount", 0),
                        item.get("sell_amount", 0),
                        item.get("reason", ""),
                        item.get("top_buyers", ""),
                        item.get("top_sellers", ""),
                    ))
                    count += 1
                except Exception as e:
                    logger.debug("批量插入龙虎榜单条跳过: %s", e)
            conn.commit()
        except Exception as e:
            logger.error("批量插入龙虎榜事务失败: %s", e)
        finally:
            self._close(conn)
        return count

    def batch_insert_boards(self, items: List[Dict]) -> int:
        """
        批量插入板块排行数据

        Args:
            items: [{board_name, board_code, change_pct, leader_stocks}]

        Returns:
            成功插入数量
        """
        if not items:
            return 0
        count = 0
        conn = self._connect()
        try:
            sql = """INSERT INTO board_index(
                board_name, board_code, change_pct, leader_stocks
            ) VALUES(?, ?, ?, ?)"""
            for item in items:
                try:
                    conn.execute(sql, (
                        item.get("board_name", ""),
                        item.get("board_code", ""),
                        item.get("change_pct", 0),
                        item.get("leader_stocks", ""),
                    ))
                    count += 1
                except Exception as e:
                    logger.debug("批量插入板块排行单条跳过: %s", e)
            conn.commit()
        except Exception as e:
            logger.error("批量插入板块排行事务失败: %s", e)
        finally:
            self._close(conn)
        return count

    def batch_link_news_stocks(self, links: List[Tuple[int, str, float]]) -> int:
        """
        批量关联新闻与股票

        Args:
            links: [(news_id, stock_code, sentiment), ...]

        Returns:
            成功关联数量
        """
        if not links:
            return 0
        conn = self._connect()
        try:
            conn.executemany(
                "INSERT OR IGNORE INTO news_stocks(news_id, stock_code, sentiment) VALUES(?, ?, ?)",
                links,
            )
            conn.commit()
        except Exception as e:
            logger.error("批量关联新闻-股票失败: %s", e)
        finally:
            self._close(conn)
        return len(links)
