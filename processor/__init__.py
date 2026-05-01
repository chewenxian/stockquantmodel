"""
股票新闻数据处理层
提供：清洗、去重、信息提取、管道编排
"""

from .cleaner import HTMLCleaner, TextNormalizer, clean_text, extract_clean_content
from .deduplicator import SimHash, hamming_distance, dedup
from .extractor import extract_keywords, detect_stock_codes, categorize_news, extract_entities
from .pipeline import process_article, process_batch

__all__ = [
    "HTMLCleaner",
    "TextNormalizer",
    "clean_text",
    "extract_clean_content",
    "SimHash",
    "hamming_distance",
    "dedup",
    "extract_keywords",
    "detect_stock_codes",
    "categorize_news",
    "extract_entities",
    "process_article",
    "process_batch",
]
