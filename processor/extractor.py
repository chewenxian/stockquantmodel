"""
信息提取模块
- extract_keywords: 基于TF的关键词提取
- detect_stock_codes: 检测股票代码
- categorize_news: 新闻分类
- extract_entities: 实体提取
"""

import re
import math
from typing import List, Dict, Tuple
from collections import Counter


# ──────────────────────────────────────────────
# 停用词（基础中文 + 财经常见虚词）
# ──────────────────────────────────────────────
_STOP_WORDS = set("""
的 了 在 是 我 有 和 就 不 人 都 一 一个 上 也 很 到 说 要 去 你 会
着 没有 看 好 自己 这 他 她 它 们 那 什么 为 而 之 与 及 但 或
从 被 把 对 以 以 让 向 往 用 能 做 将 已 又 可以 还 如果 因为 所以
因此 但是 然而 虽然 不过 而且 或者 同时 此外 例如 比如 特别是 尤其
其中 以及 之间 之后 之前 目前 目前 当前 已经 正在 该 此 每 各 某
这个 那个 这些 那些 这样 那样 怎么 如何 是否 不是 没有 可能 应该
按照 通过 根据 关于 对于 除了 非常 比较 越来越 更 最
""".split())

# ──────────────────────────────────────────────
# 股票代码模式（6位数字，匹配沪深京市场）
# ──────────────────────────────────────────────
_STOCK_CODE_PATTERN = re.compile(
    r"(?<!\d)(?:6\d{5}|0\d{5}|3\d{5}|8\d{5}|4\d{5})(?!\d)"
)

# ──────────────────────────────────────────────
# 金额模式
# ──────────────────────────────────────────────
_AMOUNT_PATTERN = re.compile(
    r"(\d+[\.\,]?\d*)\s*(亿元?|万元?|元|美元|港元|欧元|日元|英镑)"
)

# ──────────────────────────────────────────────
# 百分比模式
# ──────────────────────────────────────────────
_PERCENT_PATTERN = re.compile(
    r"(\d+[\.\,]?\d*)\s*%"
)

# ──────────────────────────────────────────────
# 中文数字 + 单位
# ──────────────────────────────────────────────
_CHINESE_NUM_UNIT = re.compile(
    r"[零一二两三四五六七八九十百千万亿]+[元%%]?"
)


# ──────────────────────────────────────────────
# 股票关键词映射（用于分类和相关性）
# ──────────────────────────────────────────────
_CATEGORY_KEYWORDS = {
    "业绩": [
        "净利润", "营收", "业绩", "财报", "季报", "年报", "盈利", "亏损",
        "利润", "收入", "每股收益", "EPS", "增长", "下滑", "预增", "预减",
        "业绩预告", "业绩快报", "分红", "派息", "送转",
    ],
    "重组": [
        "重组", "并购", "收购", "合并", "借壳", "上市", "IPO", "定增",
        "增发", "配股", "资产注入", "整体上市", "股权转让", "要约收购",
        "回购", "私有化", "分拆",
    ],
    "政策": [
        "政策", "国务院", "央行", "证监会", "银保监会", "发改委",
        "财政部", "工信部", "商务部", "人民银行", "降准", "降息",
        "加息", "加准", "监管", "新规", "办法", "通知", "指导意见",
        "宏观调控", "产业政策", "减税", "降费",
    ],
    "行业": [
        "行业", "产业", "产能", "供给", "需求", "价格", "涨价", "降价",
        "产业链", "上下游", "景气", "周期", "库存", "供需", "市场",
        "竞争", "格局", "龙头", "份额",
    ],
    "市场": [
        "大盘", "指数", "上证", "深证", "创业板", "科创板", "北交所",
        "A股", "港股", "美股", "涨停", "跌停", "涨幅", "跌幅",
        "成交量", "成交额", "主力", "资金", "北向", "南向",
        "反弹", "回调", "突破", "支撑", "压力位",
    ],
}


# ──────────────────────────────────────────────
# 关键词提取
# ──────────────────────────────────────────────
def extract_keywords(text: str, top: int = 10) -> List[str]:
    """
    基于词频（TF）的关键词提取
    简单实现：2-gram + 4-gram 提取候选词，按 TF-IDF 简化排序

    Args:
        text: 输入文本
        top: 返回top N个关键词

    Returns:
        关键词列表
    """
    try:
        if not text:
            return []

        # 预处理：去除非中文/非字母数字字符
        text = text.strip()[:5000]

        # 分割为候选词（中文字符连续片段 + 英文单词）
        tokens = _tokenize_text(text)

        # 过滤停用词和短词
        filtered = []
        for token in tokens:
            token = token.strip().lower()
            if not token or len(token) < 2:
                continue
            if token in _STOP_WORDS:
                continue
            # 跳过纯数字
            if token.isdigit() and len(token) < 4:
                continue
            filtered.append(token)

        if not filtered:
            return []

        # 统计词频
        freq = Counter(filtered)

        # 按词频降序
        keywords = [w for w, _ in freq.most_common(top * 2)]

        # 双字词优先，按频率排序
        def score(kw: str) -> float:
            f = freq.get(kw, 0)
            length_bonus = 1.5 if len(kw) >= 4 else 1.0  # 长词更可能是关键词
            return f * length_bonus

        keywords.sort(key=score, reverse=True)

        return keywords[:top]

    except Exception:
        return []


def _tokenize_text(text: str) -> List[str]:
    """文本分词（简单规则，不依赖分词库）"""
    tokens = []

    # 提取中文连续片段
    chinese_parts = re.findall(r"[\u4e00-\u9fff]+", text)
    for part in chinese_parts:
        # 2-gram 滑动窗口
        for i in range(len(part) - 1):
            token = part[i:i + 2]
            if len(token) >= 2:
                tokens.append(token)
        # 4-gram 滑动窗口（长词）
        for i in range(len(part) - 3):
            token = part[i:i + 4]
            if token not in tokens:
                tokens.append(token)

    # 提取英文单词
    eng_words = re.findall(r"[a-zA-Z]+", text)
    tokens.extend(eng_words)

    # 提取字母+数字组合（如 EPS、PE、GDP）
    alnum = re.findall(r"[A-Z]{2,6}", text)
    tokens.extend(alnum)

    return tokens


# ──────────────────────────────────────────────
# 股票代码检测
# ──────────────────────────────────────────────
def detect_stock_codes(text: str) -> List[str]:
    """
    从文本中检测股票代码

    支持格式：
    - 6位数字（沪市600/601/603/605/688，深市000/001/002/003/300/301）
    - 可能带 .SH/.SZ 后缀

    Returns:
        股票代码列表（去重，保持顺序）
    """
    try:
        if not text:
            return []

        codes = []
        # 匹配 6位数字
        codes.extend(_STOCK_CODE_PATTERN.findall(text))

        # 匹配带市场后缀的
        market_codes = re.findall(
            r"(?<!\d)(6\d{5}|0\d{5}|3\d{5}|8\d{5}|4\d{5})[\.](SH|SZ|BJ)",
            text, re.IGNORECASE
        )
        for code, _ in market_codes:
            codes.append(code)

        # 去重并保持顺序
        seen = set()
        result = []
        for code in codes:
            if code not in seen:
                seen.add(code)
                result.append(code)

        return result

    except Exception:
        return []


# ──────────────────────────────────────────────
# 新闻分类
# ──────────────────────────────────────────────
def categorize_news(title: str, content: str) -> str:
    """
    对新闻进行分类

    类别: 业绩 / 重组 / 政策 / 行业 / 市场 / 其他

    Args:
        title: 新闻标题
        content: 新闻正文

    Returns:
        分类标签
    """
    try:
        combined = f"{title} {content}".lower()

        # 按类别统计命中关键词数
        scores: Dict[str, int] = {}
        for category, keywords in _CATEGORY_KEYWORDS.items():
            score = sum(
                combined.count(kw.lower())
                for kw in keywords
            )
            if score > 0:
                scores[category] = score

        if not scores:
            return "其他"

        # 特殊规则：政策类优先（因为政策影响面广）
        if scores.get("政策", 0) >= 2:
            return "政策"
        if scores.get("业绩", 0) >= 2:
            return "业绩"
        if scores.get("重组", 0) >= 2:
            return "重组"

        # 按最高分分类
        return max(scores, key=scores.get)

    except Exception:
        return "其他"


# ──────────────────────────────────────────────
# 实体提取
# ──────────────────────────────────────────────
def extract_entities(text: str) -> Dict[str, List[str]]:
    """
    从文本中提取实体

    Returns:
        {
            "companies": [...],  # 公司名
            "persons": [...],    # 人名
            "amounts": [...],    # 金额
            "percentages": [...] # 百分比
        }
    """
    result: Dict[str, List[str]] = {
        "companies": [],
        "persons": [],
        "amounts": [],
        "percentages": [],
    }

    try:
        if not text:
            return result

        # 截取前3000字符（防止长文本影响性能）
        text = text[:3000]

        # ──── 金额 ────
        amount_matches = _AMOUNT_PATTERN.findall(text)
        for value, unit in amount_matches:
            result["amounts"].append(f"{value}{unit}")

        # ──── 百分比 ────
        pct_matches = _PERCENT_PATTERN.findall(text)
        for pct in pct_matches:
            result["percentages"].append(f"{pct}%")

        # ──── 公司名 ────
        # 模式：["公司名" + "有限公司"/"集团"/"股份"/"证券"]
        company_patterns = [
            r"([\u4e00-\u9fff]{2,10}(?:有限公司|股份有限公司|集团|股份|证券|基金|银行|保险|信托|投资))",
            r"([\u4e00-\u9fff]{2,8}(?:实业|科技|技术|医药|能源|地产|建设|工程|制造|汽车))",
        ]
        seen_companies: set = set()
        for pattern in company_patterns:
            matches = re.findall(pattern, text)
            for m in matches:
                if m not in seen_companies and len(m) >= 4:
                    seen_companies.add(m)
                    result["companies"].append(m)

        # ──── 人名 ────
        # 模式：姓+名（2~4字中文人名）
        # 常见姓氏开头，后面跟1~3个中文字
        name_pattern = r"([张王李赵刘陈杨黄吴周徐孙马胡朱郭何罗高林郑梁谢唐许冯宋韩邓彭曹曾田萧潘袁蔡蒋余于杜叶程魏苏吕丁沈任姚卢姜崔钟谭陆汪范金石廖贾夏韦付方白邹孟熊秦邱江尹薛闫段雷侯龙史陶贺顾毛郝龚邵万钱严覃武戴莫孔向汤])([\u4e00-\u9fff]{1,3})"
        seen_persons: set = set()
        for m in re.finditer(name_pattern, text):
            full_name = m.group(0)
            # 过滤：如果以常见职务字样结尾则跳过
            if full_name.endswith(("公司", "集团", "股份", "证券", "银行", "基金")):
                continue
            if len(full_name) >= 2 and full_name not in seen_persons:
                seen_persons.add(full_name)
                result["persons"].append(full_name)

        return result

    except Exception:
        return result
