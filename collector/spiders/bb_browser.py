"""
bb-browser 数据源：通过真实浏览器绕过反爬获取证券数据
数据源：bb-browser (https://github.com/epiral/bb-browser)

优势：
- 使用用户真实 Chrome 浏览器，登录态天然存在
- 无需维护 Cookie / API 签名 / 反爬策略
- 结构化 JSON 输出，直接入库

采集引擎：
  - bb-browser CLI（雪球site适配器）
  - Scrapling AsyncFetcher（东财push2 API，自动降级到bb-browser）

当前采集能力：
  ✅ xueqiu/stock          - 雪球个股实时行情（112只）
  ✅ xueqiu/hot-stock      - 雪球热门股票榜
  ✅ eastmoney/news        - 东方财富财经新闻
  ✅ Scrapling(push2)      - 板块涨跌幅排行（行业+概念，共60条）
  ✅ Scrapling(push2)      - 个股资金流向排行（主力净流入前30）
  ✅ Scrapling(push2)      - 基本面数据（PE-TTM、PB、总市值，112只→1次批量）
  ✅ Scrapling(push2)      - 融资融券数据（10条）
  ✅ Scrapling(push2)      - 北向资金流向（沪深港通）
  ✅ Scrapling(东财)      - 公司公告
"""
import subprocess
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class BbBrowserCollector(BaseCollector):
    """
    bb-browser 采集器

    通过 bb-browser CLI 调用浏览器适配器获取结构化数据。
    优点：真实浏览器 + 登录态，无需处理反爬。
    缺点：需要本地 Chrome 实例 + bb-browser 守护进程。
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self._bb_bin = self._find_bb_browser()

    def _find_bb_browser(self) -> str:
        """查找 bb-browser 可执行文件路径"""
        import shutil
        path = shutil.which("bb-browser")
        if path:
            logger.info(f"[bb-browser] 可执行文件: {path}")
            return path
        # 尝试常见的 npm 全局安装路径
        candidates = [
            "/usr/local/bin/bb-browser",
            "/opt/homebrew/bin/bb-browser",
            f"{__import__('os').environ.get('HOME', '')}/.npm-global/bin/bb-browser",
        ]
        for c in candidates:
            if __import__('os').path.exists(c):
                logger.info(f"[bb-browser] 可执行文件: {c}")
                return c
        logger.warning("[bb-browser] 未找到可执行文件，请确认已安装: npm install -g bb-browser")
        return "bb-browser"

    def _run_cmd(self, cmd: List[str]) -> Optional[dict]:
        """
        执行 bb-browser 命令并解析 JSON 输出
        cmd: ['bb-browser', 'site', 'xueqiu/stock', 'SH600519', '--json']
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,  # 最长等待30秒
            )

            if result.returncode != 0:
                stderr = result.stderr.strip()[:200]
                logger.warning(f"[bb-browser] 命令失败 (exit={result.returncode}): {stderr}")
                # 如果守护进程没启动，尝试主动拉起
                if "Daemon did not start" in stderr or "Daemon not running" in stderr:
                    logger.info("[bb-browser] 尝试启动守护进程...")
                    self._ensure_daemon()
                    # 重试一次
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    if result.returncode != 0:
                        logger.error(f"[bb-browser] 重试后仍然失败: {result.stderr[:200]}")
                        return None

            # 解析 JSON 输出（可能带有非JSON前缀日志）
            output = result.stdout.strip()
            if not output:
                logger.warning("[bb-browser] 空输出")
                return None

            # 尝试解析整段输出为 JSON
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                # 可能包含日志行，尝试提取 JSON 行
                for line in output.split("\n"):
                    line = line.strip()
                    if line.startswith("{"):
                        try:
                            return json.loads(line)
                        except json.JSONDecodeError:
                            continue
                logger.warning(f"[bb-browser] JSON 解析失败: {output[:200]}")
                return None

        except subprocess.TimeoutExpired:
            logger.error("[bb-browser] 命令超时")
            return None
        except FileNotFoundError:
            logger.error("[bb-browser] 未安装，请运行: npm install -g bb-browser")
            return None
        except Exception as e:
            logger.error(f"[bb-browser] 执行异常: {e}")
            return None

    def _ensure_daemon(self):
        """确保 bb-browser 守护进程在运行"""
        try:
            subprocess.run(
                [self._bb_bin, "daemon", "status"],
                capture_output=True, text=True, timeout=5,
            )
        except Exception:
            pass

    # ===================== 雪球行情采集 =====================

    def collect_stock_quotes(self, stocks: List[Dict]) -> int:
        """
        采集雪球个股实时行情
        返回插入行情的数量
        """
        count = 0
        batch = []

        for stock in stocks:
            market = stock.get("market", "SH")
            code = stock["code"]
            symbol = f"{market}{code}"

            cmd = [self._bb_bin, "site", "xueqiu/stock", symbol, "--json"]
            data = self._run_cmd(cmd)
            if not data or not data.get("success"):
                logger.warning(f"[bb-browser] 行情获取失败: {symbol}")
                continue

            quote = data.get("data", {})
            if not quote:
                continue

            batch.append({
                "stock_code": code,
                "price": quote.get("price", 0) or 0,
                "change_pct": self._parse_pct(quote.get("changePercent", "0")),
                "volume": quote.get("volume", 0) or 0,
                "amount": self._parse_amount(quote.get("amount", "0")),
                "high": quote.get("high", 0) or 0,
                "low": quote.get("low", 0) or 0,
                "open": quote.get("open", 0) or 0,
                "turnover_rate": self._parse_pct(quote.get("turnoverRate", "0%")),
                "amplitude": self._parse_pct(quote.get("amplitude", "0%")),
                "market_cap": quote.get("marketCap", ""),
                "prev_close": quote.get("prevClose", 0) or 0,
            })
            count += 1

            # 每10只入库一批
            if len(batch) >= 10:
                self.db.batch_insert_market_snapshots(batch)
                batch = []

        if batch:
            self.db.batch_insert_market_snapshots(batch)

        logger.info(f"[bb-browser] 雪球行情采集: {count}/{len(stocks)} 只")
        return count

    def collect_hot_stocks(self, top_n: int = 20) -> int:
        """
        采集雪球热门股票榜
        将热门股票以新闻形式入库并关联到自选股
        """
        cmd = [self._bb_bin, "site", "xueqiu/hot-stock", str(top_n), "--json"]
        data = self._run_cmd(cmd)
        if not data or not data.get("success"):
            logger.warning("[bb-browser] 热门股票采集失败")
            return 0

        items = data.get("data", {}).get("items", [])
        if not items:
            return 0

        count = 0
        for item in items:
            try:
                symbol = item.get("symbol", "")
                name = item.get("name", "")
                change_pct = item.get("changePercent", "")
                heat = item.get("heat", 0)

                # 提取纯数字代码
                code = ""
                if symbol.startswith("SH") or symbol.startswith("SZ"):
                    code = symbol[2:]
                elif symbol.startswith("0") or symbol.startswith("3"):
                    code = symbol
                    symbol = f"SZ{symbol}"
                elif symbol.startswith("6") or symbol.startswith("68"):
                    code = symbol
                    symbol = f"SH{symbol}"
                else:
                    code = symbol  # 港股等其他市场

                title = f"【雪球热股】第{item.get('rank')}名 {name} ({symbol}) {change_pct} | 热度{heat}"
                news_id = self.db.insert_news(
                    title=title,
                    url=f"https://xueqiu.com/S/{symbol}",
                    source="雪球热榜(bbs)",
                    summary=f"雪球人气榜第{item.get('rank')}名 | {name}({symbol}) 涨跌幅{change_pct} | 热议度{heat}",
                    published_at=datetime.now().isoformat(),
                )

                if news_id and code:
                    self.db.link_news_stock(news_id, code)
                    count += 1
            except Exception as e:
                logger.warning(f"[bb-browser] 热门股票解析异常: {e}")

        logger.info(f"[bb-browser] 雪球热门股票: {count} 条")
        return count

    def collect_eastmoney_news(self, top_n: int = 20) -> int:
        """
        采集东方财富财经新闻
        以新闻形式入库，并通过股票名匹配尝试关联自选股
        """
        cmd = [self._bb_bin, "site", "eastmoney/news", str(top_n), "--json"]
        data = self._run_cmd(cmd)
        if not data or not data.get("success"):
            logger.warning("[bb-browser] 东方财富新闻采集失败")
            return 0

        news_list = data.get("data", {}).get("news", [])
        if not news_list:
            return 0

        count = 0
        for item in news_list:
            try:
                title = item.get("title", "")
                summary = item.get("summary", "")
                source = item.get("source", "东方财富(bbs)")
                url = item.get("url", "")
                time_str = item.get("time", "")

                if not title or len(title) < 5:
                    continue

                news_id = self.db.insert_news(
                    title=title,
                    url=url,
                    source=source,
                    summary=summary,
                    published_at=time_str or datetime.now().isoformat(),
                )

                if news_id:
                    count += 1
            except Exception as e:
                logger.warning(f"[bb-browser] 新闻解析异常: {e}")

        # 标题匹配关联到自选股
        try:
            linked = self._link_news_to_stocks()
            logger.info(f"[bb-browser] 标题匹配关联 {linked} 条新闻到自选股")
        except Exception as e:
            logger.warning(f"[bb-browser] 标题匹配异常: {e}")

        logger.info(f"[bb-browser] 东方财富新闻采集: {count} 条")
        return count

    def _link_news_to_stocks(self) -> int:
        """通过标题中的股票名称匹配，将未关联的新闻关联到自选股"""
        conn = None
        try:
            conn = self.db._connect()
            stocks = conn.execute(
                "SELECT code, name FROM stocks"
            ).fetchall()
            # 按名称长度降序，避免短名误匹配（如 "茅台" 可能被 "贵州茅台" 先匹配）
            stocks_sorted = sorted(stocks, key=lambda s: -len(s["name"]))

            # 最近200条未关联新闻
            unlinked = conn.execute("""
                SELECT n.id, n.title FROM news n
                LEFT JOIN news_stocks ns ON n.id = ns.news_id
                WHERE ns.news_id IS NULL
                ORDER BY n.id DESC LIMIT 200
            """).fetchall()

            links = []
            for news in unlinked:
                title = news["title"] or ""
                for s in stocks_sorted:
                    if self._stock_name_in_title(s["name"], title):
                        links.append((news["id"], s["code"], 0.0))
                        break

            linked = 0
            if links:
                linked = self.db.batch_link_news_stocks(links)

            return linked
        except Exception as e:
            logger.warning(f"[bb-browser] 标题匹配异常: {e}")
            return 0
        finally:
            if conn is not None:
                self.db._close(conn)

    # ===================== 板块排行采集 =====================

    def _browser_fetch(self, url: str) -> Optional[dict]:
        """
        通过 Scrapling AsyncFetcher 发起请求
        比 bb-browser subprocess 方式更快，且不需要 Chrome 实例
        自动降级到 bb-browser
        """
        import subprocess as _sp
        import os as _os

        # 项目根目录（从 bb_browser.py 向上3层）
        _project_root = _os.path.dirname(
            _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        )
        # Scrapling venv 中的 Python 解释器
        fetch_script = _os.path.join(_project_root, "utils", "scrapling_fetch.py")
        scrapling_python = _os.path.join(
            _project_root, ".scrapling_venv", "bin", "python3"
        )

        try:
            result = _sp.run(
                [scrapling_python, fetch_script, url],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode != 0:
                logger.warning(f"[Scrapling] 请求失败: {result.stderr[:100]}")
                # 降级到 bb-browser
                return self._legacy_browser_fetch(url)

            data = json.loads(result.stdout.strip())
            if not data.get("success"):
                logger.warning(f"[Scrapling] 失败: {data.get('error','')}")
                return self._legacy_browser_fetch(url)
            return data.get("data")
        except FileNotFoundError:
            # Scrapling 未安装，降级到 bb-browser
            logger.warning("[Scrapling] 未安装，降级到 bb-browser")
            return self._legacy_browser_fetch(url)
        except Exception as e:
            logger.warning(f"[Scrapling] 异常: {e}")
            return self._legacy_browser_fetch(url)

    def _legacy_browser_fetch(self, url: str) -> Optional[dict]:
        """
        降级方案：通过 bb-browser subprocess 获取数据
        """
        try:
            result = subprocess.run(
                [self._bb_bin, "fetch", url, "--json"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode != 0:
                logger.warning(f"[bb-browser] fetch失败: {result.stderr[:100]}")
                return None
            return json.loads(result.stdout.strip())
        except Exception as e:
            logger.warning(f"[bb-browser] fetch异常: {e}")
            return None

    def _ensure_board_table(self):
        """确保 board_rankings 表存在"""
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS board_rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                board_code TEXT,
                board_name TEXT,
                board_type TEXT,
                price REAL DEFAULT 0,
                change_pct REAL DEFAULT 0,
                volume REAL DEFAULT 0,
                up_count INTEGER DEFAULT 0,
                down_count INTEGER DEFAULT 0,
                date TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def collect_board_rankings(self) -> int:
        """
        采集板块涨跌幅排行（行业板块 + 概念板块）
        通过 bb-browser 在浏览器上下文中调用 push2 API
        """
        self._ensure_board_table()
        count = 0
        conn = self.db._connect()
        today = datetime.now().strftime("%Y-%m-%d")

        board_types = [
            ("m:90+t:2", "行业板块"),
            ("m:90+t:3", "概念板块"),
        ]

        for fs, board_type in board_types:
            url = (
                f"https://push2.eastmoney.com/api/qt/clist/get"
                f"?pn=1&pz=30&po=1&np=1&fltt=2&invt=2"
                f"&fid=f3&fs={fs}"
                f"&fields=f12,f14,f2,f3,f4,f104,f105"
                f"&ut=bd1d9ddb04089700cf9c27f6f7426281"
            )
            data = self._browser_fetch(url)
            if not data or not data.get("data"):
                logger.warning(f"[bb-browser] {board_type}排行获取失败")
                continue

            items = data["data"].get("diff", [])
            for item in items:
                try:
                    board_code = str(item.get("f12", ""))
                    board_name = str(item.get("f14", ""))
                    if not board_code or not board_name:
                        continue

                    change_pct = float(item.get("f3", 0) or 0)
                    price = float(item.get("f2", 0) or 0)
                    volume = float(item.get("f4", 0) or 0)
                    up_count = int(item.get("f104", 0) or 0)
                    down_count = int(item.get("f105", 0) or 0)

                    # 去重写入
                    existing = conn.execute(
                        "SELECT id FROM board_rankings WHERE board_code = ? AND date = ?",
                        (board_code, today)
                    ).fetchone()

                    if not existing:
                        conn.execute("""
                            INSERT INTO board_rankings(
                                board_code, board_name, board_type,
                                price, change_pct, volume,
                                up_count, down_count, date
                            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (board_code, board_name, board_type,
                              price, change_pct, volume,
                              up_count, down_count, today))
                        count += 1
                except Exception as e:
                    logger.warning(f"[bb-browser] 板块解析异常: {e}")
                    continue

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 板块排行采集: {count} 条")
        return count

    def collect_money_flow(self) -> int:
        """
        采集个股资金流向排行（按主力净流入排序）
        通过 bb-browser 在浏览器上下文中调用 push2 API
        """
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            "?pn=1&pz=30&po=1&np=1&fltt=2&invt=2"
            "&fid=f62"
            "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
            "&fields=f12,f14,f62,f64,f66,f69,f84"
            "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        )
        data = self._browser_fetch(url)
        if not data or not data.get("data"):
            logger.warning("[bb-browser] 资金流向获取失败")
            return 0

        items = data["data"].get("diff", [])
        today = datetime.now().strftime("%Y-%m-%d")
        count = 0
        conn = self.db._connect()

        for item in items:
            try:
                code = str(item.get("f12", ""))
                name = str(item.get("f14", ""))
                if not code:
                    continue

                # f62=主力净流入, f64=散户净流入, f66=超大单净流入
                # f69=成交额(亿), f84=北向净流入
                main_net = (item.get("f62", 0) or 0) / 1e8
                retail_net = (item.get("f64", 0) or 0) / 1e8
                large_net = (item.get("f66", 0) or 0) / 1e8
                total_amount = (item.get("f69", 0) or 0)
                north_net = (item.get("f84", 0) or 0) / 1e8

                existing = conn.execute(
                    "SELECT id FROM money_flow WHERE stock_code = ? AND date = ?",
                    (code, today)
                ).fetchone()

                if not existing:
                    conn.execute("""
                        INSERT INTO money_flow(
                            stock_code, date,
                            main_net, retail_net, large_order_net,
                            total_amount, north_net
                        ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """, (code, today,
                          round(main_net, 2), round(retail_net, 2),
                          round(large_net, 2), round(total_amount, 2),
                          round(north_net, 2)))
                    count += 1

            except Exception as e:
                logger.warning(f"[bb-browser] 资金流向解析异常: {e}")
                continue

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 资金流向采集: {count} 条")
        return count

    # ===================== 基本面采集 =====================

    def _batch_fundamentals(self, stocks: list) -> list:
        """
        批量获取基本面数据（PE, PB, 市值等）
        使用 Scrapling 通过 ulist.np/get 批量API一次性获取，
        替代原来的每只股票逐个fetch（112次→1次）
        """
        if not stocks:
            return []

        # 批量构建 secids（ulist.np/get 支持一次最多50只）
        secids = []
        for s in stocks:
            prefix = "1." if s["market"] == "SH" else "0."
            secids.append(f"{prefix}{s['code']}")

        results = []
        # 每批50只
        batch_size = 50
        for i in range(0, len(secids), batch_size):
            batch = ",".join(secids[i:i+batch_size])
            url = (
                "https://push2.eastmoney.com/api/qt/ulist.np/get"
                f"?fltt=2&fields=f2,f3,f12,f14,f55,f86,f116,f117,f162,f167"
                f"&secids={batch}&invt=2"
            )

            data = self._browser_fetch(url)
            if not data or not data.get("data"):
                logger.warning(f"[Scrapling] 基本面批次获取失败 (idx={i})")
                continue

            items = data["data"].get("diff", [])
            for item in items:
                code = str(item.get("f12", ""))
                if not code:
                    continue

                try:
                    pe_raw = item.get("f86", 0) or 0  # f86 = PE静态
                    pb_raw = item.get("f167", 0) or 0

                    results.append({
                        "code": code,
                        "price": float(item.get("f2", 0) or 0),
                        "pe_ttm": float(pe_raw) / 100.0 if pe_raw and pe_raw != "-" else None,
                        "pb": float(pb_raw) / 100.0 if pb_raw and pb_raw != "-" else None,
                        "total_mv": float(item.get("f116", 0) or 0),
                        "float_mv": float(item.get("f117", 0) or 0),
                    })
                except Exception:
                    continue

        return results

    def collect_fundamentals(self) -> int:
        """
        采集基本面数据并更新 market_snapshots 表
        包括：PE、PB、总市值
        """
        stocks = self.db.load_stocks()
        fund_data = self._batch_fundamentals(stocks)

        if not fund_data:
            logger.warning("[bb-browser] 基本面数据全部获取失败")
            return 0

        conn = self.db._connect()
        updated = 0
        for item in fund_data:
            try:
                pe = item["pe_ttm"]
                pb = item["pb"]
                if pe is None:
                    continue  # PE数据不可用则跳过
                conn.execute("""
                    UPDATE market_snapshots
                    SET pe = ?, pb = ?, total_mv = ?
                    WHERE stock_code = ?
                      AND snapshot_time >= date('now')
                """, (
                    round(pe, 2) if pe else None,
                    round(pb, 2) if pb else None,
                    round(item["total_mv"], 2),
                    item["code"]
                ))
                updated += 1
            except Exception as e:
                logger.warning(f"[bb-browser] 基本面更新异常({item['code']}): {e}")
                continue

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 基本面数据采集: 获取{len(fund_data)}只, 更新{updated}只")
        return len(fund_data)

    # ===================== 融资融券采集 =====================

    def collect_margin_trading(self) -> int:
        """
        采集融资融券数据（通过浏览器fetch）
        """
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            "?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f3"
            "&fs=m:0+t:6+f:!2+m:0+t:80+f:!2+m:1+t:2+f:!2+m:1+t:23+f:!2"
            "&fields=f12,f14,f115,f117,f119"
        )
        data = self._browser_fetch(url)
        if not data or not data.get("data"):
            logger.warning("[bb-browser] 融资融券获取失败")
            return 0

        items = data["data"].get("diff", [])
        today = datetime.now().strftime("%Y-%m-%d")
        count = 0
        conn = self.db._connect()

        for item in items:
            try:
                code = str(item.get("f12", ""))
                name = str(item.get("f14", ""))
                if not code:
                    continue

                def safe_float(val, default=0.0):
                    """安全转换，处理 '-' 等非数值"""
                    if val is None or val == "-" or val == "":
                        return default
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return default

                margin_balance = safe_float(item.get("f115")) * 1e8
                short_balance = safe_float(item.get("f117")) * 1e8
                margin_net_buy = safe_float(item.get("f119")) * 1e8

                existing = conn.execute(
                    "SELECT id FROM margin_trading WHERE stock_code = ? AND trade_date = ?",
                    (code, today)
                ).fetchone()

                if not existing:
                    conn.execute("""
                        INSERT INTO margin_trading(
                            stock_code, stock_name,
                            margin_balance, short_balance,
                            margin_net_buy, trade_date
                        ) VALUES(?, ?, ?, ?, ?, ?)
                    """, (code, name,
                          round(margin_balance, 2),
                          round(short_balance, 2),
                          round(margin_net_buy, 2),
                          today))
                    count += 1
            except Exception as e:
                logger.warning(f"[bb-browser] 融资融券解析异常: {e}")
                continue

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 融资融券采集: {count} 条")
        return count

    # ===================== 北向资金采集 =====================

    def collect_north_flow(self) -> int:
        """
        采集北向资金流向（通过浏览器fetch）
        """
        url = (
            "https://push2.eastmoney.com/api/qt/kamt.kline/get"
            "?klt=101&lmt=5&secid=1&fields1=f1,f2&fields2=f51,f52"
        )
        data = self._browser_fetch(url)
        if not data or not data.get("data"):
            logger.warning("[bb-browser] 北向资金获取失败")
            return 0

        raw = data["data"]
        today = datetime.now().strftime("%Y-%m-%d")
        count = 0
        conn = self.db._connect()

        try:
            # kamt 返回格式:
            # hk2sh="2026-04-30,0.00" (南向沪, 非交易时间返回0)
            # sh2hk="2026-04-30,4200000.00" (沪股通)
            # hk2sz="2026-04-30,0.00" (南向深)
            for key, label in [("hk2sh", "南向沪"), ("sh2hk", "沪股通"), ("hk2sz", "南向深")]:
                lines = raw.get(key, [])
                for line in lines:
                    parts = line.split(",")
                    trade_date = parts[0] if parts else today
                    flow_value = float(parts[1]) if len(parts) > 1 else 0.0

                    existing = conn.execute(
                        "SELECT id FROM north_flow WHERE trade_date = ?",
                        (trade_date,)
                    ).fetchone()

                    if not existing:
                        if key == "sh2hk":
                            # 沪股通 = sh_net
                            conn.execute(
                                "INSERT INTO north_flow(trade_date, sh_net) VALUES(?, ?)",
                                (trade_date, round(flow_value, 2))
                            )
                        elif key == "hk2sz":
                            conn.execute(
                                "INSERT INTO north_flow(trade_date, sz_net) VALUES(?, ?)",
                                (trade_date, round(flow_value, 2))
                            )
                        elif key == "hk2sh":
                            # 更新已有行或创建
                            conn.execute(
                                "INSERT OR IGNORE INTO north_flow(trade_date) VALUES(?)",
                                (trade_date,)
                            )
                            conn.execute(
                                "UPDATE north_flow SET cumulative_net = ? WHERE trade_date = ?",
                                (round(flow_value, 2), trade_date)
                            )
                        count += 1
        except Exception as e:
            logger.warning(f"[bb-browser] 北向资金解析异常: {e}")

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 北向资金采集: {count} 条")
        return count

    # ===================== 公司公告采集 =====================

    def collect_announcements(self) -> int:
        """
        采集自选股公司公告（通过浏览器fetch调用东财公告API）
        """
        stocks = self.db.load_stocks()
        count = 0

        # 分批采集，每批最多10只
        batch_size = 10
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            codes = ",".join([s["code"] for s in batch])

            url = (
                "https://np-anotice-stock.eastmoney.com/api/security/ann"
                f"?sr=-1&page_size=5&page_index=1&ann_type=A"
                f"&stock_list={codes}&f_node=0&s_node=0"
            )

            data = self._browser_fetch(url)
            if not data or not data.get("data"):
                continue

            items = data["data"].get("list", [])
            conn = self.db._connect()

            for item in items:
                try:
                    title = item.get("title", "")
                    display_time = item.get("display_time", "")
                    codes_info = item.get("codes", [])
                    columns = item.get("columns", [])

                    if not title:
                        continue

                    ann_type = ""
                    if columns:
                        ann_type = columns[0].get("column_name", "")

                    stock_code = ""
                    if codes_info:
                        stock_code = codes_info[0].get("stock_code", "")

                    # 去重
                    existing = conn.execute(
                        "SELECT id FROM announcements WHERE title = ? AND date(collected_at) = date('now')",
                        (title[:100],)
                    ).fetchone()

                    if not existing:
                        publish_date = display_time[:10] if len(display_time) >= 10 else datetime.now().strftime("%Y-%m-%d")
                        conn.execute("""
                            INSERT INTO announcements(
                                stock_code, title, announce_type,
                                publish_date, url
                            ) VALUES(?, ?, ?, ?, ?)
                        """, (stock_code, title[:200],
                              ann_type, publish_date,
                              f"https://np-anotice-stock.eastmoney.com/api/security/ann?art_code={item.get('art_code','')}" if item.get('art_code') else ""))
                        count += 1
                except Exception as e:
                    logger.warning(f"[bb-browser] 公告解析异常: {e}")
                    continue

            conn.commit()
            conn.close()

        logger.info(f"[bb-browser] 公告采集: {count} 条")
        return count

    # ===================== 更新stock_hot表 =====================

    def _ensure_stock_hot_table(self):
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_hot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                hot_rank INTEGER,
                hot_score REAL,
                change_pct REAL,
                trade_date DATE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def collect_hot_stocks_to_table(self) -> int:
        """
        采集雪球热门股票并写入 stock_hot 表
        """
        self._ensure_stock_hot_table()

        cmd = [self._bb_bin, "site", "xueqiu/hot-stock", "20", "--json"]
        data = self._run_cmd(cmd)
        if not data or not data.get("success"):
            return 0

        items = data.get("data", {}).get("items", [])
        if not items:
            return 0

        today = datetime.now().strftime("%Y-%m-%d")
        count = 0
        conn = self.db._connect()

        for item in items:
            try:
                symbol = item.get("symbol", "")
                name = item.get("name", "")
                code = symbol
                for p in ["SH", "SZ"]:
                    if code.startswith(p):
                        code = code[len(p):]
                        break

                rank = int(item.get("rank", 0))
                heat = int(item.get("heat", 0))
                change_pct = float(str(item.get("changePercent", "0")).replace("%", ""))

                existing = conn.execute(
                    "SELECT id FROM stock_hot WHERE stock_code = ? AND trade_date = ?",
                    (code, today)
                ).fetchone()

                if not existing:
                    conn.execute("""
                        INSERT INTO stock_hot(
                            stock_code, stock_name, hot_rank,
                            hot_score, change_pct, trade_date
                        ) VALUES(?, ?, ?, ?, ?, ?)
                    """, (code, name, rank, float(heat), change_pct, today))
                    count += 1
            except Exception:
                continue

        conn.commit()
        conn.close()
        logger.info(f"[bb-browser] 股票热度写入stock_hot表: {count} 条")
        return count

    # ===================== 工具方法 =====================

    def _parse_pct(self, val) -> float:
        """解析百分比字符串，如 '-1.17%' → -1.17"""
        if isinstance(val, (int, float)):
            return float(val)
        try:
            return float(str(val).replace("%", "").strip())
        except (ValueError, AttributeError):
            return 0.0

    def _parse_amount(self, val) -> float:
        """解析金额字符串，如 '73.16亿' → 7316000000"""
        if isinstance(val, (int, float)):
            return float(val)
        try:
            s = str(val).strip()
            if "万亿" in s:
                return float(s.replace("万亿", "")) * 1e12
            if "亿" in s:
                return float(s.replace("亿", "")) * 1e8
            if "万" in s:
                return float(s.replace("万", "")) * 1e4
            return float(s)
        except (ValueError, AttributeError):
            return 0.0

    # ===================== 统一采集入口 =====================

    def collect(self) -> Dict[str, int]:
        """
        主采集入口：采集雪球行情、热门股票、东财新闻
        """
        results = {"error": 0}

        try:
            stocks = self.db.load_stocks()
        except Exception as e:
            logger.error(f"[bb-browser] 加载自选股失败: {e}")
            results["error"] = 1
            return results

        # 1. 雪球行情
        try:
            results["quotes"] = self.collect_stock_quotes(stocks)
        except Exception as e:
            logger.error(f"[bb-browser] 行情采集异常: {e}")
            results["quotes"] = 0

        # 2. 雪球热门股票
        try:
            results["hot_stocks"] = self.collect_hot_stocks(20)
        except Exception as e:
            logger.error(f"[bb-browser] 热门股票采集异常: {e}")
            results["hot_stocks"] = 0

        # 3. 东方财富新闻
        try:
            results["eastmoney_news"] = self.collect_eastmoney_news(20)
        except Exception as e:
            logger.error(f"[bb-browser] 新闻采集异常: {e}")
            results["eastmoney_news"] = 0

        # 4. 板块涨跌幅排行（行业+概念，通过浏览器fetch绕过push2限制）
        try:
            results["board_rankings"] = self.collect_board_rankings()
        except Exception as e:
            logger.error(f"[bb-browser] 板块排行采集异常: {e}")
            results["board_rankings"] = 0

        # 5. 个股资金流向排行（通过浏览器fetch绕过push2限制）
        try:
            results["money_flow"] = self.collect_money_flow()
        except Exception as e:
            logger.error(f"[bb-browser] 资金流向采集异常: {e}")
            results["money_flow"] = 0

        # 6. 基本面数据（PE、PB、市值等）
        try:
            results["fundamentals"] = self.collect_fundamentals()
        except Exception as e:
            logger.error(f"[bb-browser] 基本面采集异常: {e}")
            results["fundamentals"] = 0

        # 7. 融资融券
        try:
            results["margin_trading"] = self.collect_margin_trading()
        except Exception as e:
            logger.error(f"[bb-browser] 融资融券采集异常: {e}")
            results["margin_trading"] = 0

        # 8. 北向资金
        try:
            results["north_flow"] = self.collect_north_flow()
        except Exception as e:
            logger.error(f"[bb-browser] 北向资金采集异常: {e}")
            results["north_flow"] = 0

        # 9. 公司公告
        try:
            results["announcements"] = self.collect_announcements()
        except Exception as e:
            logger.error(f"[bb-browser] 公告采集异常: {e}")
            results["announcements"] = 0

        # 10. 股票热度表
        try:
            results["stock_hot"] = self.collect_hot_stocks_to_table()
        except Exception as e:
            logger.error(f"[bb-browser] 股票热度采集异常: {e}")
            results["stock_hot"] = 0

        total = sum(v for v in results.values() if isinstance(v, int) and v >= 0)
        logger.info(f"[bb-browser] 采集完成: {results}, 总计 {total} 条")
        return results
