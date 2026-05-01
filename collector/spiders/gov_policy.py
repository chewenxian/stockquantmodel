"""
官方网站政策采集器
数据源：
  - 中国人民银行 (www.pbc.gov.cn) - 货币政策、宏观数据
  - 国家发改委 (www.ndrc.gov.cn) - 产业政策、发展规划
  - 工信部 (www.miit.gov.cn) - 产业政策、行业监管
采集：政策法规、通知公告、新闻发布
"""
import re
import json
import logging
from datetime import datetime
from typing import Dict
from bs4 import BeautifulSoup

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class GovPolicyCollector(BaseCollector):
    """
    政府部门政策采集器
    覆盖：央行、发改委、工信部
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db

    def collect(self) -> Dict[str, int]:
        """执行全量采集"""
        results = {}
        results["pbc"] = self._collect_pbc()
        results["ndrc"] = self._collect_ndrc()
        results["miit"] = self._collect_miit()
        total = sum(results.values())
        logger.info(f"[政府政策] 采集完成: {results}, 总计 {total} 条")
        return results

    # ==================== 中国人民银行 ====================

    def _collect_pbc(self) -> int:
        """采集中国人民银行政策/数据/公告"""
        count = 0

        headers = {
            "Referer": "http://www.pbc.gov.cn",
            "User-Agent": self._random_ua(),
        }

        # 央行主要栏目
        sections = [
            ("zhengcehuobisi/125207_", "货币政策"),
            ("goutongjiaoliu/113456_", "沟通交流"),
            ("zhengcehuobisi/125202_", "宏观审慎"),
            ("jinrongshichangsi/", "金融市场"),
            ("tiaofasi/", "条法司"),
        ]

        for section_path, section_name in sections:
            try:
                url = f"http://www.pbc.gov.cn/{section_path}index.html"
                resp = self.get(url, headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                # 央行页面通常使用 ul>li>a 结构
                links = soup.find_all("a", href=re.compile(rf"^{section_path}\d+"))

                for a_tag in links:
                    try:
                        title = a_tag.get_text(strip=True)
                        href = a_tag.get("href", "")

                        if not title or not href:
                            continue

                        # 安全拼接URL
                        if href.startswith("http"):
                            full_url = href
                        elif href.startswith("/"):
                            full_url = f"http://www.pbc.gov.cn{href}"
                        else:
                            full_url = f"http://www.pbc.gov.cn/{href}"

                        # 提取发布时间
                        date_str = ""
                        # 尝试从URL中提取日期
                        date_match = re.search(r"/(\d{8})/", href)
                        if date_match:
                            d = date_match.group(1)
                            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

                        # 检测相关政策影响领域
                        related_sectors = self._detect_sectors(title)

                        self.db.insert_policy(
                            title=title.strip(),
                            url=full_url,
                            source="中国人民银行",
                            department="中国人民银行",
                            summary=f"中国人民银行-{section_name}政策发布",
                            publish_date=date_str,
                            related_sectors=json.dumps(related_sectors, ensure_ascii=False),
                        )

                        # 重要货币政策也纳入新闻
                        important_keywords = [
                            "降准", "降息", "加息", "LPR", "利率", "存款准备金",
                            "逆回购", "MLF", "公开市场", "再贷款", "再贴现",
                            "汇率", "跨境", "数字人民币",
                        ]
                        is_important = any(k in title for k in important_keywords)
                        if is_important:
                            self.db.insert_news(
                                title=f"【央行政策】{title.strip()}",
                                url=full_url,
                                source="中国人民银行",
                                summary=f"央行-{section_name}",
                                published_at=date_str or datetime.now().isoformat(),
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"央行政策解析异常: {e}")
            except Exception as e:
                logger.warning(f"央行政策请求异常({section_name}): {e}")

        return count

    # ==================== 国家发改委 ====================

    def _collect_ndrc(self) -> int:
        """采集国家发改委政策"""
        count = 0

        headers = {
            "Referer": "http://www.ndrc.gov.cn",
            "User-Agent": self._random_ua(),
        }

        # 发改委主要栏目
        sections = [
            ("xwzx/xwfb/", "新闻发布"),
            ("xwzx/zxdt/", "最新动态"),
            ("fzgggz/", "发展改革工作"),
            ("fggz/", "法规工作"),
            ("xxgk/zcfb/", "政策发布"),
        ]

        for section_path, section_name in sections:
            try:
                url = f"http://www.ndrc.gov.cn/{section_path}index.html"
                resp = self.get(url, headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                links = soup.find_all("a", href=re.compile(rf"{section_path}\d+"))

                for a_tag in links:
                    try:
                        title = a_tag.get_text(strip=True)
                        href = a_tag.get("href", "")

                        if not title or not href:
                            continue

                        full_url = href if href.startswith("http") else f"http://www.ndrc.gov.cn/{href.lstrip('/')}"

                        # 提取时间
                        date_str = ""
                        date_match = re.search(r"/(\d{8})/", href)
                        if date_match:
                            d = date_match.group(1)
                            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

                        related_sectors = self._detect_sectors(title)

                        self.db.insert_policy(
                            title=title.strip(),
                            url=full_url,
                            source="国家发改委",
                            department="国家发展和改革委员会",
                            summary=f"发改委-{section_name}",
                            publish_date=date_str,
                            related_sectors=json.dumps(related_sectors, ensure_ascii=False),
                        )

                        # 重要政策也纳入新闻
                        important_keywords = [
                            "规划", "产业", "投资", "新能源", "数字经济",
                            "降费", "减税", "专项债", "基建", "城镇化",
                            "一带一路", "长三角", "粤港澳",
                        ]
                        is_important = any(k in title for k in important_keywords)
                        if is_important:
                            self.db.insert_news(
                                title=f"【发改委】{title.strip()}",
                                url=full_url,
                                source="国家发改委",
                                summary=f"发改委-{section_name}",
                                published_at=date_str or datetime.now().isoformat(),
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"发改委解析异常: {e}")
            except Exception as e:
                logger.warning(f"发改委请求异常({section_name}): {e}")

        return count

    # ==================== 工信部 ====================

    def _collect_miit(self) -> int:
        """采集工信部产业政策"""
        count = 0

        headers = {
            "Referer": "http://www.miit.gov.cn",
            "User-Agent": self._random_ua(),
        }

        # 工信部主要栏目
        sections = [
            ("xwdt/gxdt/", "工信动态"),
            ("xwdt/zw/", "政务公开"),
            ("zwgk/zcjd/", "政策解读"),
            ("zwgk/zcwj/", "政策文件"),
            ("gzcy/", "公众参与"),
        ]

        for section_path, section_name in sections:
            try:
                url = f"http://www.miit.gov.cn/{section_path}index.html"
                resp = self.get(url, headers=headers)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                # 工信部使用各种选择器
                selectors = [
                    f"a[href*='/{section_path}']",
                    "ul.list li a",
                    "div.news-list a",
                    "a[href*='.html']",
                ]

                found_links = []
                for sel in selectors:
                    found_links = soup.select(sel)
                    if found_links:
                        break

                for a_tag in found_links:
                    try:
                        title = a_tag.get_text(strip=True)
                        href = a_tag.get("href", "")

                        if not title or len(title) < 5:
                            continue

                        full_url = href if href.startswith("http") else f"http://www.miit.gov.cn/{href.lstrip('/')}"

                        # 提取时间
                        date_str = ""
                        parent = a_tag.find_parent("li") or a_tag.find_parent("div")
                        if parent:
                            span = parent.find("span")
                            if span:
                                date_txt = span.get_text(strip=True)
                                # 匹配 YYYY-MM-DD
                                date_match = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})", date_txt)
                                if date_match:
                                    date_str = date_match.group(1).replace("/", "-")

                        related_sectors = self._detect_sectors(title)

                        self.db.insert_policy(
                            title=title.strip(),
                            url=full_url,
                            source="工业和信息化部",
                            department="工业和信息化部",
                            summary=f"工信部-{section_name}",
                            publish_date=date_str,
                            related_sectors=json.dumps(related_sectors, ensure_ascii=False),
                        )

                        # 重要产业政策纳入新闻
                        important_keywords = [
                            "5G", "6G", "芯片", "半导体", "集成电路",
                            "新能源汽车", "智能网联", "人工智能", "AI",
                            "工业互联网", "数字经济", "数字化",
                            "中小企业", "专精特新", "小巨人",
                            "绿色制造", "双碳", "节能",
                        ]
                        is_important = any(k in title for k in important_keywords)
                        if is_important:
                            self.db.insert_news(
                                title=f"【工信部】{title.strip()}",
                                url=full_url,
                                source="工信部",
                                summary=f"工信部-{section_name}",
                                published_at=date_str or datetime.now().isoformat(),
                            )

                        count += 1
                    except Exception as e:
                        logger.warning(f"工信部解析异常: {e}")
            except Exception as e:
                logger.warning(f"工信部请求异常({section_name}): {e}")

        return count

    def _detect_sectors(self, text: str) -> list:
        """从文本中检测涉及的经济/政策领域"""
        sectors = []
        keywords = {
            "货币政策": ["货币政策", "利率", "LPR", "降准", "降息", "公开市场", "MLF", "逆回购", "流动性"],
            "金融监管": ["金融", "银行", "保险", "券商", "资本", "证券"],
            "数字经济": ["数字", "数据", "互联网", "平台经济", "AI", "大模型"],
            "半导体/芯片": ["芯片", "半导体", "集成电路", "光刻", "EDA"],
            "新能源": ["新能源", "光伏", "风电", "储能", "电池", "双碳", "碳中和", "碳达峰"],
            "新能源汽车": ["新能源车", "电动汽车", "充电", "锂电", "氢能"],
            "房地产": ["房地产", "住房", "楼市", "保障房", "城中村"],
            "基建/投资": ["基建", "投资", "专项债", "重大工程", "新基建", "城镇化"],
            "产业升级": ["制造", "专精特新", "小巨人", "智能", "工业互联网", "数字化"],
            "科技创新": ["科技", "创新", "研发", "攻关", "核心技术", "自主可控"],
            "消费": ["消费", "促消费", "扩大内需", "内需"],
            "民生": ["民生", "就业", "社保", "医保", "教育"],
            "对外开放": ["开放", "自贸区", "一带一路", "外资", "进出口", "外贸"],
            "环保": ["环保", "绿色", "生态", "污染", "减排"],
            "通信": ["5G", "6G", "通信", "基站", "光纤"],
        }
        text_lower = text.lower()
        for sector, kws in keywords.items():
            if any(kw.lower() in text_lower for kw in kws):
                sectors.append(sector)
        return sectors
