"""
政策与宏观数据采集：影响大盘走势的政策信息
数据源：
- 财联社电报 (cls.cn) - 从 telegraph 页面的 __NEXT_DATA__ 解析
- 华尔街见闻 (wallstreetcn.com) - API
- 国家统计局 (stats.gov.cn)
- 人民银行 (pbc.gov.cn)

API说明（2026年验证）：
- 财联社旧API（/api/telegraph/list, /v1/roll/get_roll_list）均无效
- 新方法：解析 https://www.cls.cn/telegraph 页面的 __NEXT_DATA__ JSON
- 华尔街见闻：api-one.wallstcn.com 仍可用
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class PolicyCollector(BaseCollector):
    """
    政策新闻 + 宏观数据采集器
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def collect(self) -> Dict[str, int]:
        results = {"error": 0}
        try:
            results["cls_news"] = self._collect_cls_news()
        except Exception as e:
            logger.error(f"[政策宏观] 财联社采集异常: {e}")
            results["cls_news"] = 0

        try:
            results["policy_news"] = self._collect_wallstreet_news()
        except Exception as e:
            logger.error(f"[政策宏观] 华尔街见闻采集异常: {e}")
            results["policy_news"] = 0

        total = sum(results.values())
        logger.info(f"[政策宏观] 采集完成: {results}, 总计 {total} 条")
        return results

    def _collect_cls_news(self) -> int:
        """
        财联社电报 - 从 telegraph 页面解析 __NEXT_DATA__
        页面：https://www.cls.cn/telegraph
        该页面是 Nuxt.js 服务端渲染，数据嵌入在 __NEXT_DATA__ script 标签中
        """
        count = 0
        headers = {
            "Referer": "https://www.cls.cn/",
            "User-Agent": self._random_ua(),
        }

        try:
            resp = self.get("https://www.cls.cn/telegraph", headers=headers)
            if not resp:
                logger.warning("[财联社] telegraph页面请求失败")
                return 0

            # 提取 __NEXT_DATA__ JSON
            next_data_match = re.search(
                r'__NEXT_DATA__"\s*type="application/json">\s*(\{.*?\})\s*</script>',
                resp.text, re.DOTALL
            )
            if not next_data_match:
                logger.warning("[财联社] 未找到__NEXT_DATA__数据")
                return 0

            page_data = json.loads(next_data_match.group(1))
            # 导航到 initialState.telegraph.telegraphList
            telegraph_list = []
            try:
                telegraph_list = page_data["props"]["isServer"]["initialState"]["telegraph"]["telegraphList"]
            except (KeyError, TypeError):
                # 尝试其他路径
                try:
                    telegraph_list = page_data["props"]["initialState"]["telegraph"]["telegraphList"]
                except (KeyError, TypeError):
                    logger.warning("[财联社] 无法解析电报列表数据")
                    return 0

            logger.info(f"[财联社] 解析到 {len(telegraph_list)} 条快讯")

            for item in telegraph_list:
                try:
                    # 提取标题
                    content = item.get("content", "") or ""
                    title = item.get("title", "") or ""

                    if not title and content:
                        # 从内容前80字截取作为标题
                        title = re.sub(r'<[^>]+>', '', content)[:80]
                    elif not title and not content:
                        continue

                    # 清洗HTML标签
                    clean_title = re.sub(r'<[^>]+>', '', title).strip()
                    clean_content = re.sub(r'<[^>]+>', '', content).strip()

                    # 快讯ID和数据URL
                    item_id = item.get("id", "")
                    link = f"https://www.cls.cn/detail/{item_id}" if item_id else "https://www.cls.cn/telegraph"

                    # 发布时间
                    ctime = item.get("ctime", item.get("createTime", ""))
                    if ctime:
                        try:
                            if isinstance(ctime, (int, float)):
                                ctime = datetime.fromtimestamp(ctime / 1000 if ctime > 1e10 else ctime).isoformat()
                        except:
                            pass

                    # 分类快讯级别
                    level = item.get("level", "")
                    reading_num = item.get("reading_num", 0)

                    if not clean_title:
                        continue

                    # 检测影响板块
                    related_sectors = self._detect_sectors(clean_title + clean_content)

                    self.db.insert_policy(
                        title=clean_title,
                        url=link,
                        source="财联社",
                        department="",
                        summary=clean_content[:300],
                        publish_date=ctime or datetime.now().isoformat(),
                        related_sectors=json.dumps(related_sectors, ensure_ascii=False)
                    )
                    count += 1

                except Exception as e:
                    logger.warning(f"[财联社] 快讯解析异常: {e}")

        except Exception as e:
            logger.warning(f"[财联社] 采集异常: {e}")

        return count

    def _collect_wallstreet_news(self) -> int:
        """
        华尔街见闻 - 快讯
        API: https://api-one.wallstcn.com/apiv1/content/lives
        """
        count = 0
        url = "https://api-one.wallstcn.com/apiv1/content/lives"
        params = {"channel": "global", "limit": 30}
        headers = {
            "Referer": "https://wallstreetcn.com/live/global",
            "User-Agent": self._random_ua(),
        }

        try:
            data = self.get_json(url, params, headers=headers)
            if data and data.get("data"):
                items = data["data"].get("items", [])
                for item in items:
                    try:
                        content = item.get("content", "")
                        title = content[:80] if content else ""
                        if not title:
                            continue
                        # 清洗HTML
                        title = re.sub(r'<[^>]+>', '', title).strip()
                        clean_content = re.sub(r'<[^>]+>', '', content).strip()
                        link = f"https://wallstreetcn.com/live/global"
                        ctime = item.get("display_time", "")

                        self.db.insert_policy(
                            title=title, url=link,
                            source="华尔街见闻",
                            summary=clean_content[:300] or title,
                            publish_date=ctime or datetime.now().isoformat(),
                        )
                        count += 1
                    except:
                        pass
        except Exception as e:
            logger.warning(f"[华尔街见闻] 采集异常: {e}")

        return count

    def _detect_sectors(self, text: str) -> list:
        """从文本中检测涉及的市场板块"""
        sectors = []
        keywords = {
            "新能源": ["新能源", "光伏", "风电", "锂电池", "新能源车", "比亚迪", "宁德时代"],
            "半导体": ["半导体", "芯片", "集成电路", "中芯国际", "光刻"],
            "金融": ["银行", "券商", "保险", "降息", "降准", "加息", "LPR"],
            "房地产": ["房地产", "楼市", "房贷", "房企", "万科", "碧桂园"],
            "医药": ["医药", "医疗", "创新药", "CXO", "疫苗", "集采"],
            "消费": ["消费", "白酒", "食品", "零售", "免税", "贵州茅台"],
            "AI科技": ["AI", "人工智能", "大模型", "ChatGPT", "算力", "数据要素"],
            "军工": ["军工", "国防", "航天", "卫星"],
            "周期": ["煤炭", "钢铁", "有色", "化工", "石油"],
            "农业": ["农业", "种业", "猪肉", "粮食"],
        }
        text_lower = text.lower()
        for sector, kws in keywords.items():
            if any(kw.lower() in text_lower for kw in kws):
                sectors.append(sector)
        return sectors
