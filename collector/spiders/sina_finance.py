"""
新浪财经：行情快照 + 新闻 + 板块数据
"""
import re
import json
import logging
from datetime import datetime
from typing import List, Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class SinaFinanceCollector(BaseCollector):
    """
    新浪财经数据源
    - 实时行情 (hq.sinajs.cn)
    - 财经新闻 (RSS / API)
    - 美股行情
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def collect(self) -> Dict[str, int]:
        results = {}
        stocks = self.db.load_stocks()
        results["quotes"] = self._collect_quotes(stocks)
        results["news"] = self._collect_news()
        results["us_market"] = self._collect_us_market()
        total = sum(results.values())
        logger.info(f"[新浪财经] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_quotes(self, stocks: List[Dict]) -> int:
        """新浪行情接口 - 支持沪深"""
        count = 0
        # 构建代码列表
        codes = []
        for s in stocks:
            prefix = "sh" if s["market"] == "SH" else "sz"
            codes.append(f"{prefix}{s['code']}")

        if not codes:
            return 0

        # 分批请求（新浪限制每次最多约30个代码）
        for i in range(0, len(codes), 20):
            batch = ",".join(codes[i:i+20])
            url = f"https://hq.sinajs.cn/list={batch}"
            headers = {
                "Referer": "https://finance.sina.com.cn",
                "Accept": "*/*",
            }
            resp = self.get(url, headers=headers)
            if not resp:
                continue

            text = resp.text
            for line in text.strip().split("\n"):
                try:
                    # 格式: var hq_str_sh600519="贵州茅台,1728.50,..."
                    match = re.search(r'hq_str_(\w+)="(.+)"', line)
                    if not match:
                        continue
                    code = match.group(1)
                    parts = match.group(2).split(",")
                    if len(parts) < 30:
                        continue

                    fields = {
                        "name": parts[0],
                        "open": float(parts[1]) if parts[1] else 0,
                        "close": float(parts[2]) if parts[2] else 0,
                        "price": float(parts[3]) if parts[3] else 0,
                        "high": float(parts[4]) if parts[4] else 0,
                        "low": float(parts[5]) if parts[5] else 0,
                        "volume": float(parts[8]) if parts[8] else 0,  # 手
                        "amount": float(parts[9]) if parts[9] else 0,  # 元
                    }

                    # 去 market prefix 获取纯代码
                    pure_code = code[2:] if len(code) > 2 else code
                    change_pct = 0
                    if fields["close"] > 0:
                        change_pct = (fields["price"] - fields["close"]) / fields["close"] * 100

                    self.db.insert_market_snapshot(
                        code=pure_code, price=fields["price"],
                        change_pct=round(change_pct, 2),
                        volume=fields["volume"],
                        amount=fields["amount"],
                        high=fields["high"], low=fields["low"],
                        open=fields["open"]
                    )
                    count += 1
                except Exception as e:
                    logger.warning(f"新浪行情解析异常: {e}")
        return count

    def _collect_news(self) -> int:
        """采集新浪财经滚动新闻"""
        count = 0
        # 新浪财经头条列表
        # 新浪新闻API使用lid参数（2026已验证）
        urls = [
            ("https://feed.mix.sina.com.cn/api/roll/get", "滚动新闻", {"lid": 153, "num": 30}),
            ("https://feed.mix.sina.com.cn/api/roll/get", "股市", {"lid": 155, "num": 30}),
            ("https://feed.mix.sina.com.cn/api/roll/get", "财经", {"lid": 158, "num": 30}),
            ("https://feed.mix.sina.com.cn/api/roll/get", "产经", {"lid": 161, "num": 30}),
        ]

        for url, source_name, params in urls:
            data = self.get_json(url, params)
            if data and data.get("result"):
                items = data["result"].get("data", [])
                for item in items:
                    try:
                        title = item.get("title", "").strip()
                        link = item.get("url", "") or item.get("link", "")
                        intro = item.get("intro", "")
                        ctime = item.get("ctime", "")

                        if not title:
                            continue

                        self.db.insert_news(
                            title=title, url=link,
                            source=f"新浪-{source_name}",
                            summary=intro,
                            published_at=ctime
                        )
                        count += 1
                    except Exception as e:
                        logger.warning(f"新浪新闻解析异常: {e}")
        return count

    def _collect_us_market(self) -> int:
        """采集美股主要指数并入库"""
        count = 0
        # 美股三大指数
        us_codes = [".DJI", ".IXIC", ".INX"]
        url = f"https://hq.sinajs.cn/list={'%2C'.join(us_codes)}"
        headers = {"Referer": "https://finance.sina.com.cn"}

        resp = self.get(url, headers=headers)
        if not resp:
            return count

        text = resp.text
        logger.info(f"[新浪-美股] 采集到行情数据 ({len(text)} bytes)")

        for line in text.strip().split("\n"):
            try:
                # 格式: var hq_str_dji="名称,开盘价,昨收,当前价,最高,最低,..."
                match = re.search(r'hq_str_(\w+)="(.+)"', line)
                if not match:
                    continue
                code = match.group(1)
                parts = match.group(2).split(",")
                if len(parts) < 10:
                    continue

                fields = {
                    "name": parts[0],
                    "open": float(parts[1]) if parts[1] else 0,
                    "close": float(parts[2]) if parts[2] else 0,  # 昨收
                    "price": float(parts[3]) if parts[3] else 0,
                    "high": float(parts[4]) if parts[4] else 0,
                    "low": float(parts[5]) if parts[5] else 0,
                }

                # 计算涨跌幅
                change_pct = 0
                if fields["close"] > 0:
                    change_pct = (fields["price"] - fields["close"]) / fields["close"] * 100

                self.db.insert_market_snapshot(
                    code=code.upper(), price=fields["price"],
                    change_pct=round(change_pct, 2),
                    volume=0, amount=0,
                    high=fields["high"], low=fields["low"],
                    open=fields["open"]
                )
                count += 1
            except Exception as e:
                logger.warning(f"美股行情解析异常 ({line[:80]}): {e}")

        return count
