"""
处理管道编排
组合: cleaner → extractor → deduplicator
"""

from typing import Dict, Any, List, Optional

from .cleaner import clean_text
from .deduplicator import (
    SimHash, hamming_distance, dedup,
    filter_stale_news, filter_offtopic,
)
from .extractor import extract_keywords, detect_stock_codes, categorize_news, extract_entities


def process_article(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理单篇文章：清洗 → 提取特征 → 去重计算

    输入 raw_data 格式:
    {
        "title": str,
        "content": str (可能是 HTML 或纯文本),
        "url": str,
        "source": str,
        "published_at": str,
        ...其他字段...
    }

    输出格式（在原数据基础上增加字段）:
    {
        ...原字段...
        "clean_title": str,
        "clean_content": str,
        "summary": str,          # 前200字摘要
        "keywords": [str],       # 关键词列表，逗号分隔存储时会转为字符串
        "category": str,         # 分类标签
        "stock_codes": [str],    # 关联股票代码
        "entities": dict,        # 提取的实体
        "content_hash": str,     # 用于去重的哈希
        "processed": bool,       # 是否已处理
    }
    """
    try:
        title = raw_data.get("title", "")
        content = raw_data.get("content", "")

        # 1. 清洗
        clean_title = clean_text(title) if title else ""
        clean_content = clean_text(content) if content else ""

        # 2. 生成摘要（前200字）
        summary = clean_content[:200] if clean_content else ""

        # 3. 提取特征
        combine_text = f"{clean_title} {clean_content}"

        # 关键词
        keywords = extract_keywords(combine_text, top=10)

        # 分类
        category = categorize_news(clean_title, clean_content)

        # 股票代码
        stock_codes = detect_stock_codes(combine_text)

        # 实体
        entities = extract_entities(combine_text)

        # 4. 生成内容哈希（用于数据库去重）
        dedup_text = f"{clean_title}|{clean_content}"[:2000]
        # 用Python内置hash（仅用于比较，不保证跨进程一致性）
        content_hash = str(abs(hash(dedup_text)))

        # 5. 构建结果
        result = dict(raw_data)
        result.update({
            "clean_title": clean_title,
            "clean_content": clean_content,
            "summary": summary,
            "keywords": keywords,
            "category": category,
            "stock_codes": stock_codes,
            "entities": entities,
            "content_hash": content_hash,
            "processed": True,
        })

        return result

    except Exception:
        # 出错时返回原始数据 + 默认处理字段
        result = dict(raw_data)
        result.setdefault("clean_title", raw_data.get("title", ""))
        result.setdefault("clean_content", raw_data.get("content", ""))
        result.setdefault("summary", "")
        result.setdefault("keywords", [])
        result.setdefault("category", "其他")
        result.setdefault("stock_codes", [])
        result.setdefault("entities", {})
        result.setdefault("content_hash", "")
        result.setdefault("processed", False)
        return result


def process_batch(
    raw_list: List[Dict[str, Any]],
    known_stocks: Optional[List[Dict[str, str]]] = None,
) -> List[Dict[str, Any]]:
    """
    批量处理新闻列表（增强版）
    流程: 清洗 → 旧闻过滤 → 去重 → 噪音过滤 → 特征提取

    Args:
        raw_list: 原始文章列表
        known_stocks: [{code, name}, ...] 用于噪音过滤

    Returns:
        处理后的文章列表（已去重+过滤）
    """
    if not raw_list:
        return []

    # 1. 清洗 + 提取（单遍扫描）
    processed = []
    for article in raw_list:
        try:
            result = process_article(article)
            processed.append(result)
        except Exception:
            processed.append(article)

    # 2. 旧闻过滤（标记 low_priority）
    processed = filter_stale_news(processed, max_age_hours=48)

    # 3. 基于SimHash去重（使用clean_content）
    if processed:
        processed = dedup(
            processed,
            threshold=3,
            text_key="clean_content",
            title_key="clean_title",
            window_size=100,
        )

    # 4. 噪音过滤（标记 offtopic）
    processed = filter_offtopic(processed, known_stocks=known_stocks)

    return processed
