"""
SQLite 数据库模块
存储：新闻、公告、行情、资金流向、分析结果等
"""
import sqlite3
import os
from datetime import datetime
from typing import Optional


class Database:
    def __init__(self, db_path: str = "data/stock_news.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_tables()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

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
        """)
        conn.commit()
        conn.close()

    # ---- CRUD 操作 ----

    def upsert_stock(self, code: str, name: str, market: str = "SH", reason: str = "", industry: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO stocks(code, name, market, reason, industry)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET name=excluded.name, reason=excluded.reason
        """, (code, name, market, reason, industry))
        conn.commit()
        conn.close()

    def load_stocks(self):
        conn = self._connect()
        rows = conn.execute("SELECT code, name, market FROM stocks").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def insert_news(self, title: str, url: str, source: str, summary: str = "",
                    content: str = "", published_at: str = None) -> Optional[int]:
        try:
            conn = self._connect()
            cur = conn.execute("""
                INSERT OR IGNORE INTO news(title, url, source, summary, content, published_at)
                VALUES(?, ?, ?, ?, ?, ?)
            """, (title, url, source, summary, content, published_at))
            conn.commit()
            last_id = cur.lastrowid
            conn.close()
            return last_id
        except Exception:
            return None

    def link_news_stock(self, news_id: int, stock_code: str, sentiment: float = 0.0):
        conn = self._connect()
        conn.execute("""
            INSERT OR IGNORE INTO news_stocks(news_id, stock_code, sentiment)
            VALUES(?, ?, ?)
        """, (news_id, stock_code, sentiment))
        conn.commit()
        conn.close()

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
        conn.close()

    def insert_announcement(self, code: str, title: str, url: str,
                            announce_type: str = "", summary: str = "",
                            publish_date: str = None):
        try:
            conn = self._connect()
            conn.execute("""
                INSERT OR IGNORE INTO announcements(stock_code, title, url, announce_type, summary, publish_date)
                VALUES(?, ?, ?, ?, ?, ?)
            """, (code, title, url, announce_type, summary, publish_date))
            conn.commit()
            conn.close()
            return True
        except Exception:
            return False

    def insert_money_flow(self, code: str, date: str, main_net: float = 0,
                           retail_net: float = 0, north_net: float = 0,
                           large_order_net: float = 0, total_amount: float = 0):
        conn = self._connect()
        conn.execute("""
            INSERT INTO money_flow(stock_code, date, main_net, retail_net, north_net, large_order_net, total_amount)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """, (code, date, main_net, retail_net, north_net, large_order_net, total_amount))
        conn.commit()
        conn.close()

    def insert_policy(self, title: str, url: str, source: str, department: str = "",
                       summary: str = "", publish_date: str = None, related_sectors: str = ""):
        try:
            conn = self._connect()
            conn.execute("""
                INSERT OR IGNORE INTO policies(title, url, source, department, summary, publish_date, related_sectors)
                VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (title, url, source, department, summary, publish_date, related_sectors))
            conn.commit()
            conn.close()
            return True
        except Exception:
            return False

    def insert_macro(self, indicator: str, value: float, unit: str = "",
                      release_date: str = None, source: str = "", summary: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO macro_data(indicator, value, unit, release_date, source, summary)
            VALUES(?, ?, ?, ?, ?, ?)
        """, (indicator, value, unit, release_date, source, summary))
        conn.commit()
        conn.close()

    def log_collect(self, source: str, data_type: str, count: int = 0,
                     status: str = "success", error_msg: str = ""):
        conn = self._connect()
        conn.execute("""
            INSERT INTO collect_logs(source, data_type, item_count, status, error_msg, started_at, finished_at)
            VALUES(?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """, (source, data_type, count, status, error_msg))
        conn.commit()
        conn.close()

    # ---- 统计查询 ----

    def get_today_news_count(self) -> int:
        conn = self._connect()
        row = conn.execute("""
            SELECT COUNT(*) as cnt FROM news
            WHERE date(collected_at) = date('now', 'localtime')
        """).fetchone()
        conn.close()
        return row['cnt'] if row else 0

    def get_stock_news_sentiment(self, code: str, days: int = 1):
        conn = self._connect()
        rows = conn.execute("""
            SELECT n.title, n.source, n.published_at, ns.sentiment
            FROM news_stocks ns
            JOIN news n ON ns.news_id = n.id
            WHERE ns.stock_code = ?
              AND n.published_at >= datetime('now', ? || ' days', 'localtime')
            ORDER BY n.published_at DESC
        """, (code, f'-{days}')).fetchall()
        conn.close()
        return [dict(r) for r in rows]
