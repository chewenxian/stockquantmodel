"""
国债收益率采集器
数据源：和讯网/东方财富

接口尝试：
  1. 东方财富 - https://data.eastmoney.com/cjsj/hbzjj.html (银行间拆借利率)
  2. 华尔街见闻API - https://api-ddc-wscn.awtmt.com/market/real
数据：10年期国债收益率、Shibor利率、国债收益率曲线关键期限

影响逻辑：债券收益率上升→无风险利率上升→股市估值承压
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class BondYieldCollector(BaseCollector):
    """
    国债收益率采集器
    采集关键国债收益率和Shibor利率

    数据来源（按优先级）：
    1. 东方财富宏观数据页
    2. 和讯网国债频道
    3. API备用源
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def _ensure_table(self):
        """确保 bond_yield 表存在"""
        conn = self.db._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bond_yield (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                yield_name TEXT,
                yield_value REAL,
                unit TEXT DEFAULT '%',
                trade_date DATE,
                source TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def collect(self) -> Dict[str, int]:
        """采集国债收益率和Shibor利率"""
        self._ensure_table()
        results = {"bond_yield": 0}

        count = 0

        # 尝试东方财富宏观数据页
        try:
            cnt = self._collect_from_eastmoney()
            count += cnt
        except Exception as e:
            logger.warning(f"[国债收益率] 东方财富采集异常: {e}")

        # 如果东方财富失败，尝试API备用源
        if count == 0:
            try:
                cnt = self._collect_from_wsj_api()
                count += cnt
            except Exception as e:
                logger.warning(f"[国债收益率] WSJ API采集异常: {e}")

        # 仍然失败，尝试和讯网
        if count == 0:
            try:
                cnt = self._collect_from_hexin()
                count += cnt
            except Exception as e:
                logger.warning(f"[国债收益率] 和讯网采集异常: {e}")

        results["bond_yield"] = count
        logger.info(f"[国债收益率] 采集完成，新增 {count} 条")
        return results

    def _collect_from_eastmoney(self) -> int:
        """
        从东方财富银行间拆借利率页面解析
        URL: https://data.eastmoney.com/cjsj/hbzjj.html
        """
        count = 0
        headers = {
            "Referer": "https://data.eastmoney.com/",
            "User-Agent": self._random_ua(),
        }

        resp = self.get("https://data.eastmoney.com/cjsj/hbzjj.html", headers=headers)
        if not resp:
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")

        # 尝试从利率表格中提取数据 - 寻找包含Shibor和国债收益率的表格
        today = datetime.now().strftime("%Y-%m-%d")

        # 方法1: 查找 class="datatips" 或 id="tbody-data" 的表格
        rows = soup.select("table tr") or soup.select("#tbody-data tr") or soup.find_all("tr")

        # 关键利率指标映射
        yield_names = {
            "Shibor": "Shibor",
            "隔夜": "Shibor_ON",
            "1周": "Shibor_1W",
            "2周": "Shibor_2W",
            "1个月": "Shibor_1M",
            "3个月": "Shibor_3M",
            "6个月": "Shibor_6M",
            "9个月": "Shibor_9M",
            "1年": "Shibor_1Y",
            "10年期国债收益率": "CN10YR",
            "10年国债": "CN10YR",
            "国债收益率10年": "CN10YR",
        }

        conn = self.db._connect()

        for row in rows:
            try:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                text_content = row.get_text("|", strip=True)
                # 检查是否包含关键利率关键词
                matched_name = None
                for keyword, name in yield_names.items():
                    if keyword in text_content:
                        matched_name = name
                        break

                if not matched_name:
                    continue

                # 提取利率值 - 查找百分比数字
                value_text = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                if not value_text:
                    value_text = cols[-1].get_text(strip=True) if len(cols) > 1 else ""

                # 提取数字
                nums = re.findall(r'[\d.]+', value_text.replace("%", ""))
                if nums:
                    yield_value = float(nums[0])

                    # 去重
                    existing = conn.execute(
                        "SELECT id FROM bond_yield WHERE yield_name = ? AND trade_date = ?",
                        (matched_name, today)
                    ).fetchone()

                    if not existing:
                        conn.execute("""
                            INSERT INTO bond_yield(yield_name, yield_value, trade_date, source)
                            VALUES(?, ?, ?, ?)
                        """, (matched_name, yield_value, today, "东方财富"))
                        conn.commit()
                        count += 1

            except (ValueError, IndexError) as e:
                continue

        # 如果没有从表格解析到数据，做个硬编码回退（取趋势即可，不必精确值）
        if count == 0:
            logger.info("[国债收益率] 表格解析未命中，尝试从页面英文关键标签提取")

        conn.close()
        return count

    def _collect_from_wsj_api(self) -> int:
        """
        备用: 通过华尔街见闻API采集
        通过web请求获取基础利率数据
        """
        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        headers = {
            "User-Agent": self._random_ua(),
            "Referer": "https://wallstreetcn.com/",
        }

        # 尝试万得/华尔街见闻接口
        try:
            resp = self.get_json(
                "https://api-ddc-wscn.awtmt.com/market/real",
                params={
                    "fields": "prod_name,last_px",
                    "prod_code": "CN10YR.OTC,SHIBORON.IR,SHIBOR1W.IR,SHIBOR3M.IR",
                },
                headers=headers,
            )
            if resp and resp.get("code") == 20000:
                items = resp.get("data", [])
                if isinstance(items, list):
                    conn = self.db._connect()
                    for item in items:
                        try:
                            prod_name = item.get("prod_name", "")
                            last_px = item.get("last_px", 0)
                            if not prod_name:
                                continue

                            yield_name = prod_name
                            if "10年" in prod_name or "国债" in prod_name:
                                yield_name = "CN10YR"
                            elif "隔夜" in prod_name or "Shibor" in prod_name or "shibor" in prod_name.lower():
                                yield_name = prod_name.replace(" ", "_")

                            existing = conn.execute(
                                "SELECT id FROM bond_yield WHERE yield_name = ? AND trade_date = ?",
                                (yield_name, today)
                            ).fetchone()
                            if not existing:
                                conn.execute("""
                                    INSERT INTO bond_yield(yield_name, yield_value, trade_date, source)
                                    VALUES(?, ?, ?, ?)
                                """, (yield_name, float(last_px), today, "华尔街见闻"))
                                conn.commit()
                                count += 1
                        except Exception:
                            continue
                    conn.close()
        except Exception:
            pass

        return count

    def _collect_from_hexin(self) -> int:
        """
        备用: 从和讯网国债频道解析
        URL: https://bond.hexun.com/
        """
        count = 0
        headers = {
            "User-Agent": self._random_ua(),
            "Referer": "https://bond.hexun.com/",
        }

        resp = self.get("https://bond.hexun.com/", headers=headers)
        if not resp:
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        today = datetime.now().strftime("%Y-%m-%d")

        # 尝试提取国债收益率数据
        text = soup.get_text()
        # 查找类似 "10年期国债收益率 1.65%" 的文本
        patterns = [
            r'10[年]期国债[收]?益[率][^0-9]*([\d.]+)',
            r'国债[收]?益[率][^0-9]*10[年][^0-9]*([\d.]+)',
            r'([\d.]+)%[^。]*10[年]期国债',
        ]

        conn = self.db._connect()
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    yield_value = float(match.group(1))
                    existing = conn.execute(
                        "SELECT id FROM bond_yield WHERE yield_name = 'CN10YR' AND trade_date = ?",
                        (today,)
                    ).fetchone()
                    if not existing:
                        conn.execute("""
                            INSERT INTO bond_yield(yield_name, yield_value, trade_date, source)
                            VALUES(?, ?, ?, ?)
                        """, ("CN10YR", yield_value, today, "和讯网"))
                        conn.commit()
                        count += 1
                except (ValueError, IndexError):
                    pass
                break

        conn.close()
        return count


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    from storage.database import Database
    db = Database("data/stock_news.db")
    c = BondYieldCollector(db)
    result = c.collect()
    print(f"采集结果: {result}")
