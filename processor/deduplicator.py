"""
SimHash 文本去重模块
64位指纹，支持滑动窗口去重
特征提取：4-gram + TF权重
纯 Python 实现，无外部依赖
"""

import math
from typing import List, Dict, Any


class SimHash:
    """
    SimHash 64位指纹实现
    用于文本相似度计算和去重
    """

    def __init__(self, hash_bits: int = 64):
        self.hash_bits = hash_bits

    def _hash_token(self, token: str) -> int:
        """
        对单个token计算哈希值（字符串转64位hash）
        使用改进的FNV-1a算法，确保散列均匀
        """
        h = 0xcbf29ce484222325  # FNV offset basis for 64-bit
        for c in token.encode("utf-8"):
            h ^= c
            # FNV-1a 素数
            h = (h * 0x100000001b3) & ((1 << 64) - 1)
        return h

    def _extract_grams(self, text: str) -> List[str]:
        """
        提取4-gram特征
        """
        grams = []
        for i in range(len(text) - 3):
            grams.append(text[i:i + 4])
        return grams

    def _compute_tf(self, grams: List[str]) -> Dict[str, float]:
        """
        计算每个4-gram的TF值 (词频)
        """
        total = len(grams) if grams else 1
        tf = {}
        for g in grams:
            tf[g] = tf.get(g, 0.0) + 1.0
        for k in tf:
            tf[k] = math.log(1.0 + tf[k]) / math.log(1.0 + total)
        return tf

    def fingerprint(self, text: str, grams: List[str] = None) -> int:
        """
        计算文本的64位SimHash指纹

        Args:
            text: 输入文本
            grams: 可选的预先提取的4-gram列表（避免重复计算）

        Returns:
            64位整数指纹
        """
        if grams is None:
            grams = self._extract_grams(text)

        tf = self._compute_tf(grams)

        # 初始化权重向量
        v = [0] * self.hash_bits

        for gram, weight in tf.items():
            h = self._hash_token(gram)
            for i in range(self.hash_bits):
                bit = (h >> i) & 1
                if bit:
                    v[i] += weight
                else:
                    v[i] -= weight

        # 计算最终指纹
        fingerprint = 0
        for i in range(self.hash_bits):
            if v[i] > 0:
                fingerprint |= (1 << i)

        return fingerprint

    def similarity(self, fp1: int, fp2: int) -> float:
        """
        计算两个指纹的相似度 (0~1)
        基于汉明距离
        """
        dist = hamming_distance(fp1, fp2)
        # 64位中不同bit的比例
        max_dist = self.hash_bits
        return 1.0 - (dist / max_dist)

    def extract_features(self, text: str) -> List[str]:
        """对外暴露的4-gram提取方法"""
        return self._extract_grams(text)


def hamming_distance(hash1: int, hash2: int) -> int:
    """
    计算两个64位hash的汉明距离
    """
    xor = hash1 ^ hash2
    # 计算popcount (Brian Kernighan算法)
    dist = 0
    while xor:
        xor &= xor - 1
        dist += 1
    return dist


def dedup(
    news_list: List[Dict[str, Any]],
    threshold: int = 3,
    text_key: str = "content",
    title_key: str = "title",
    window_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    滑动窗口文本去重

    使用SimHash 64位指纹，滑动窗口内两两比较汉明距离。
    阈值=3 表示允许最多3bit不同（高精度去重）

    Args:
        news_list: 新闻字典列表，每项至少包含 text_key 字段
        threshold: 汉明距离阈值，<=此值视为重复（默认3）
        text_key: 用于去重的文本字段名
        title_key: 标题字段名（辅助去重回退）
        window_size: 滑动窗口大小（控制内存占用）

    Returns:
        去重后的新闻列表（保留首次出现的版本）
    """
    if not news_list:
        return []

    hasher = SimHash()
    seen_fingerprints: List[tuple] = []  # [(fingerprint, idx)]
    result = []
    duplicates_skipped = 0

    for idx, article in enumerate(news_list):
        try:
            # 获取去重文本
            dedup_text = article.get(text_key) or article.get(title_key) or ""
            if not dedup_text:
                result.append(article)
                continue

            # 清理一下文本
            dedup_text = dedup_text.strip()[:2000]  # 限定长度

            # 计算指纹
            grams = hasher.extract_features(dedup_text)
            if len(grams) < 4:
                # 文本太短，直接用字符串比较
                is_dup = any(
                    existing_text.get(text_key) == dedup_text
                    for _, _, existing_text in seen_fingerprints[:window_size]
                )
                if not is_dup:
                    result.append(article)
                    seen_fingerprints.append((0, dedup_text, article))
                else:
                    duplicates_skipped += 1
                continue

            fp = hasher.fingerprint(dedup_text, grams)

            # 滑动窗口去重
            is_duplicate = False
            start = max(0, len(seen_fingerprints) - window_size)
            for existing_fp, _, _ in seen_fingerprints[start:]:
                if existing_fp is None:
                    continue
                if hamming_distance(fp, existing_fp) <= threshold:
                    is_duplicate = True
                    break

            if not is_duplicate:
                result.append(article)
                seen_fingerprints.append((fp, dedup_text, article))
            else:
                duplicates_skipped += 1

        except Exception:
            # 出错的保留原样
            result.append(article)

    return result


def dedup_by_content_hash(
    news_list: List[Dict[str, Any]],
    text_key: str = "content",
    title_key: str = "title",
) -> List[Dict[str, Any]]:
    """
    基于完整文本哈希的精确去重（轻量级）
    适合内存中快速去重

    Args:
        news_list: 新闻列表
        text_key: 文本字段名
        title_key: 标题字段名

    Returns:
        去重后的列表
    """
    seen_hashes: set = set()
    result = []

    for article in news_list:
        try:
            text = article.get(text_key) or article.get(title_key) or ""
            text_hash = hash(text)
            if text_hash not in seen_hashes:
                seen_hashes.add(text_hash)
                result.append(article)
        except Exception:
            result.append(article)

    return result
