"""
雪球数据源：用户讨论 + 热帖 + 情绪指标
数据源：https://xueqiu.com

API说明（2026年验证）：
- 旧API（sqrank.json、timeline.json）均已404/400，需要登录cookie
- 改用页面解析方式：
  - 热门股票：https://xueqiu.com/hq 页面解析
  - 个股讨论：https://xueqiu.com/S/{code} 页面解析
  - 均使用BeautifulSoup从静态HTML提取数据
"""
import re
import json
import logging
from datetime import datetime
from typing import List, Dict
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class XueqiuCollector(BaseCollector):
    """
    雪球数据源（页面解析模式）
    - 热门股票列表（从HQ页面解析）
    - 个股讨论文本（从个股页面解析）
    - 情绪指标生成（基于讨论内容分析）
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.base_url = "https://xueqiu.com"
        self.hq_url = "https://xueqiu.com/hq"
        self.stock_url = "https://xueqiu.com/S"

    def collect(self) -> Dict[str, int]:
        results = {"error": 0}
        stocks = self.db.load_stocks()

        try:
            results["hot_stocks"] = self._collect_hot_stocks()
        except Exception as e:
            logger.error(f"[雪球] 热门股票采集异常: {e}")
            results["hot_stocks"] = 0

        try:
            results["stock_discussions"] = self._collect_stock_discussions(stocks)
        except Exception as e:
            logger.error(f"[雪球] 讨论采集异常: {e}")
            results["stock_discussions"] = 0

        total = sum(results.values())
        logger.info(f"[雪球] 采集完成: {results}, 总计 {total} 条")
        return results

    def _warmup_session(self):
        """预热Session：访问雪球首页获取基础Cookie"""
        # 已经通过雪球HQ页面访问获得了一些cookie
        pass

    def _collect_hot_stocks(self) -> int:
        """
        从雪球行情中心页面解析热门股票
        页面: https://xueqiu.com/hq
        """
        count = 0
        headers = {
            "Referer": self.base_url,
            "User-Agent": self._random_ua(),
        }

        try:
            # 先访问首页获取cookie
            self.get(self.base_url, headers=headers)
            # 再访问HQ页面
            resp = self.get(self.hq_url, headers=headers)
            if not resp:
                logger.warning("[雪球] HQ页面请求失败")
                return 0

            soup = BeautifulSoup(resp.text, "html.parser")

            # 查找页面中可能嵌入的热门股票数据
            # 1. 查找 script 中的 __INITIAL_STATE__ 或 Nuxt 数据
            scripts = soup.find_all("script")
            for script in scripts:
                content = script.string or ""
                if "hotStocks" in content or "hot_stock" in content or "list" in content:
                    # 尝试提取JSON数据
                    for pattern in [r'hotStocks\s*:\s*(\[.*?\])', r'hot_stock\s*:\s*(\[.*?\])']:
                        match = re.search(pattern, content, re.DOTALL)
                        if match:
                            try:
                                hot_data = json.loads(match.group(1))
                                for item in hot_data[:30]:
                                    code = str(item.get("stock_id", item.get("code", "")))
                                    name = item.get("name", "")
                                    change = float(item.get("percent", item.get("change", 0)) or 0)

                                    title = f"【雪球热股】{name} ({code})"
                                    self.db.insert_news(
                                        title=title,
                                        url=f"{self.stock_url}/{'SH' + code if code.startswith('6') else 'SZ' + code}",
                                        source="雪球热榜",
                                        summary=f"雪球热议度 | 涨幅 {change:.2f}%",
                                        published_at=datetime.now().isoformat()
                                    )
                                    count += 1
                            except:
                                pass

            # 2. 如果没找到JSON数据，尝试从HTML元素解析
            if count == 0:
                # 查找表格行或股票列表
                stock_items = soup.find_all("tr", class_=re.compile(r"stock|hover"))
                if not stock_items:
                    # 尝试其他选择器
                    stock_items = soup.select("table.stock-table tbody tr")

                for item in stock_items[:30]:
                    try:
                        cols = item.find_all("td")
                        if len(cols) >= 3:
                            code_el = item.find("a", href=re.compile(r"/S/"))
                            if code_el:
                                href = code_el.get("href", "")
                                code = re.sub(r'.*/(?:SH|SZ)?(\d{6}).*', r'\1', href)
                                name = code_el.get_text(strip=True)
                                if code and name:
                                    title = f"【雪球热股】{name} ({code})"
                                    self.db.insert_news(
                                        title=title,
                                        url=f"{self.base_url}/S/{code}",
                                        source="雪球热榜",
                                        published_at=datetime.now().isoformat()
                                    )
                                    count += 1
                    except:
                        pass

        except Exception as e:
            logger.warning(f"[雪球] 热门股票采集异常: {e}")

        return count

    def _collect_stock_discussions(self, stocks: List[Dict]) -> int:
        """
        从个股雪球页面解析讨论帖
        页面: https://xueqiu.com/S/{SH/SZ}{code}
        """
        count = 0

        for stock in stocks[:10]:  # 前10只
            try:
                code = stock["code"]
                market = stock["market"]
                symbol = f"{market}{code}"

                headers = {
                    "Referer": f"{self.stock_url}/{symbol}",
                    "User-Agent": self._random_ua(),
                }

                # 访问个股页面
                resp = self.get(f"{self.stock_url}/{symbol}", headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                # 尝试从 script 标签中提取讨论数据
                scripts = soup.find_all("script")
                for script in scripts:
                    content = script.string or ""

                    # 查找 __NEXT_DATA__ 或 __INITIAL_STATE__
                    for marker in ["__NEXT_DATA__", "__INITIAL_STATE__", "SNB.bootData"]:
                        idx = content.find(marker)
                        if idx >= 0:
                            # 尝试提取JSON
                            json_match = re.search(r'__NEXT_DATA__\s*type="application/json">\s*(\{.*?\})\s*</script>', str(script), re.DOTALL)
                            if json_match:
                                try:
                                    page_data = json.loads(json_match.group(1))
                                    # 提取讨论/状态列表
                                    statuses = self._extract_statuses_from_json(page_data)
                                    for status in statuses[:10]:
                                        title = status.get("text", "")[:80]
                                        if not title:
                                            continue
                                        title = re.sub(r'<[^>]+>', '', title)
                                        status_url = status.get("url", f"{self.stock_url}/{symbol}")

                                        news_id = self.db.insert_news(
                                            title=title + ("..." if len(status.get("text", "")) > 80 else ""),
                                            url=status_url,
                                            source="雪球讨论",
                                            summary=re.sub(r'<[^>]+>', '', status.get("text", ""))[:200],
                                            published_at=datetime.now().isoformat()
                                        )
                                        if news_id:
                                            self.db.link_news_stock(news_id, code)
                                            count += 1
                                except:
                                    pass

                # 如果JSON提取失败，尝试从页面内容中提取
                if count < len(stocks[:10]):  # 一个都没解析到
                    # 查找讨论区内容
                    status_elements = soup.find_all("div", class_=re.compile(r"status|timeline|content"))
                    for el in status_elements[:10]:
                        text = el.get_text(strip=True)
                        if text and len(text) > 20:
                            title = text[:80] + ("..." if len(text) > 80 else "")
                            news_id = self.db.insert_news(
                                title=title,
                                url=f"{self.stock_url}/{symbol}",
                                source="雪球讨论",
                                summary=text[:200],
                                published_at=datetime.now().isoformat()
                            )
                            if news_id:
                                self.db.link_news_stock(news_id, code)
                                count += 1

            except Exception as e:
                logger.warning(f"[雪球] 讨论采集异常({stock['code']}): {e}")

        return count

    def _extract_statuses_from_json(self, data: dict) -> list:
        """从页面嵌入的JSON中提取讨论状态"""
        statuses = []
        try:
            # 递归查找包含讨论列表的字段
            def find_list(obj, depth=0):
                if depth > 5:
                    return
                if isinstance(obj, dict):
                    for key, val in obj.items():
                        if key in ("statuses", "list", "items", "timeline", "feed"):
                            if isinstance(val, list):
                                return val
                            # val可能是对象里面有list
                            if isinstance(val, dict):
                                for k in ("list", "items", "data"):
                                    if k in val and isinstance(val[k], list):
                                        return val[k]
                        result = find_list(val, depth + 1)
                        if result:
                            return result
                return None

            found = find_list(data)
            if found:
                for item in found:
                    if isinstance(item, dict):
                        text = item.get("text", item.get("content", item.get("description", "")))
                        item_id = item.get("id", item.get("status_id", ""))
                        user = item.get("user", {})
                        username = ""
                        if isinstance(user, dict):
                            username = user.get("screenName", user.get("username", ""))

                        if text:
                            statuses.append({
                                "text": text,
                                "id": item_id,
                                "user": username,
                                "url": f"https://xueqiu.com/{username}/{item_id}" if username and item_id else "",
                            })
        except Exception:
            pass

        return statuses
