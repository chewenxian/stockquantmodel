"""
数据清洗模块
- HTMLCleaner: 去除HTML标签、script、style、广告特征文本
- TextNormalizer: 统一全半角、去除多余空白、规范化标点
- clean_text(): 一站式清洗
- extract_clean_content(): 从HTML提取干净正文
"""

import re
from typing import Optional


# ──────────────────────────────────────────────
# 广告/噪音特征文本模式
# ──────────────────────────────────────────────
_AD_PATTERNS = [
    r"(免责声明|免责|声明)[：:].*?[。；;]",
    r"(本文来[源自]|来源[：:])\s*\S+",
    r"(责任编辑|编辑|校对|记者)[：:]\s*\S+",
    r"(风险提示|投资有风险|入市需谨慎|以上信息仅供参考)",
    r"点击.*?(阅读|查看|下载|关注)",
    r"(微信|wechat|公众号)[：:]\s*\S+",
    r"(扫码|二维码|长按识别)",
    r"(广告|推广|Sponsored)",
    r"\|.*?(分享到|收藏|打印|纠错)",
    r"(分享到微信朋友圈|转发|赞|在看)",
    r"\d+分钟前\s*$",
    r"^\s*$",  # 空行
]

# ──────────────────────────────────────────────
# HTMLCleaner
# ──────────────────────────────────────────────
class HTMLCleaner:
    """去除HTML标签、内嵌脚本、样式和广告噪音"""

    @staticmethod
    def strip_tags(html: str) -> str:
        """移除所有HTML标签"""
        try:
            # 先移除 script 和 style 块
            text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r"<noscript[^>]*>.*?</noscript>", "", text, flags=re.DOTALL | re.IGNORECASE)
            # 移除注释
            text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
            # 移除标签
            text = re.sub(r"<[^>]+>", " ", text)
            return text
        except Exception:
            return html

    @staticmethod
    def remove_ad_noise(text: str) -> str:
        """去除广告和噪音文本行"""
        try:
            for pattern in _AD_PATTERNS:
                text = re.sub(pattern, "", text, flags=re.IGNORECASE)
            return text
        except Exception:
            return text

    @staticmethod
    def decode_html_entities(text: str) -> str:
        """解码常见HTML实体"""
        try:
            entities = {
                "&amp;": "&", "&lt;": "<", "&gt;": ">",
                "&quot;": "\"", "&#39;": "'", "&#x27;": "'",
                "&#x2F;": "/", "&nbsp;": " ", "&#160;": " ",
                "&mdash;": "—", "&ndash;": "–", "&ldquo;": "“",
                "&rdquo;": "”", "&lsquo;": "‘", "&rsquo;": "’",
            }
            for entity, char in entities.items():
                text = text.replace(entity, char)
            # 处理 &#\d+; 数字实体
            text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))) if 32 <= int(m.group(1)) < 0x10FFFF else m.group(0), text)
            return text
        except Exception:
            return text


# ──────────────────────────────────────────────
# TextNormalizer
# ──────────────────────────────────────────────
class TextNormalizer:
    """文本规范化"""

    @staticmethod
    def fullwidth_to_halfwidth(text: str) -> str:
        """全角字符转半角"""
        try:
            result = []
            for c in text:
                code = ord(c)
                if 0xFF01 <= code <= 0xFF5E:
                    # 全角字母/数字/标点 → 半角
                    result.append(chr(code - 0xFEE0))
                elif code == 0x3000:
                    # 全角空格 → 半角
                    result.append(" ")
                else:
                    result.append(c)
            return "".join(result)
        except Exception:
            return text

    @staticmethod
    def collapse_whitespace(text: str) -> str:
        """折叠多余空白字符"""
        try:
            # 将各种空白字符替换为空格
            text = re.sub(r"[\r\n\t\f\v]+", " ", text)
            # 多个空格合并为一个
            text = re.sub(r" {2,}", " ", text)
            return text.strip()
        except Exception:
            return text

    @staticmethod
    def normalize_punctuation(text: str) -> str:
        """规范化标点符号"""
        try:
            replacements = {
                "，": ",", "。": ".", "；": ";",
                "：": ":", "？": "?", "！": "!",
                "（": "(", "）": ")", "【": "[",
                "】": "]", "“": "\"", "”": "\"",
                "‘": "'", "’": "'", "《": "<",
                "》": ">", "、": ",",
                "…": "...", "——": "—", "──": "—",
            }
            for full, half in replacements.items():
                text = text.replace(full, half)
            # 去除重复标点
            text = re.sub(r"([.,!?;:]){2,}", r"\1", text)
            return text
        except Exception:
            return text

    @staticmethod
    def normalize(text: str) -> str:
        """一键规范化"""
        text = TextNormalizer.fullwidth_to_halfwidth(text)
        text = TextNormalizer.collapse_whitespace(text)
        text = TextNormalizer.normalize_punctuation(text)
        return text


# ──────────────────────────────────────────────
# 一站式清洗
# ──────────────────────────────────────────────
def clean_text(text: str) -> str:
    """
    一站式清洗文本（针对纯文本）
    1. 解码HTML实体
    2. 去除HTML标签残留
    3. 去除广告噪音
    4. 规范化
    """
    try:
        cleaner = HTMLCleaner()
        normalizer = TextNormalizer()

        text = cleaner.decode_html_entities(text)
        text = cleaner.strip_tags(text)
        text = cleaner.remove_ad_noise(text)
        text = normalizer.normalize(text)

        return text
    except Exception:
        return text


def extract_clean_content(html: str) -> str:
    """
    从HTML中提取干净正文内容
    使用BeautifulSoup解析 + 规则过滤

    Args:
        html: 原始HTML字符串

    Returns:
        清洗后的纯文本正文
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        # 回退到正则方式
        return clean_text(html)

    try:
        soup = BeautifulSoup(html, "html.parser")

        # 移除噪音标签
        for tag_name in ["script", "style", "noscript", "iframe",
                          "nav", "footer", "header", "aside",
                          "form", "input", "button", "svg",
                          "meta", "link", "source", "picture"]:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # 尝试寻找正文区域（常见正文容器类名）
        content_selectors = [
            {"name": "article", "attrs": {}},
            {"name": "div", "attrs": {"class": re.compile(r"(article|content|main|post|text|detail|news)", re.I)}},
            {"name": "div", "attrs": {"id": re.compile(r"(article|content|main|post|text|detail|news)", re.I)}},
            {"name": "section", "attrs": {"class": re.compile(r"(article|content|post|text)", re.I)}},
        ]

        main_content = None
        for sel in content_selectors:
            tag = soup.find(sel["name"], sel["attrs"])
            if tag:
                main_content = tag
                break

        if main_content is None:
            main_content = soup.body if soup.body else soup

        # 提取文本
        text = main_content.get_text(separator="\n", strip=True)
        return clean_text(text)

    except Exception:
        return clean_text(html)
