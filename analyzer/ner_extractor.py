"""
金融 NER 增强模块
基于词典 + 正则的命名实体识别，不依赖 NLP 库

提供：
1. 公司名识别（A股上市公司 + 模式匹配）
2. 人名识别（高管/分析师/政要）
3. 产品名识别（锂、碳酸锂、芯片、光伏等）
4. 行业板块识别
5. 综合金融实体提取
6. 股票代码/名称识别
"""
import re
import logging
from typing import Dict, List, Set, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════
# A 股上市公司名称词典（前100常见代码+名称）
# ═══════════════════════════════════════════════════
A_SHARE_STOCKS = {
    "600519": "贵州茅台", "000858": "五粮液", "300750": "宁德时代",
    "601318": "中国平安", "000333": "美的集团", "600036": "招商银行",
    "002594": "比亚迪", "688981": "中芯国际", "600900": "长江电力",
    "000001": "平安银行", "600887": "伊利股份", "601166": "兴业银行",
    "600030": "中信证券", "601398": "工商银行", "601939": "建设银行",
    "601288": "农业银行", "601988": "中国银行", "600276": "恒瑞医药",
    "300059": "东方财富", "002415": "海康威视", "000002": "万科A",
    "600585": "海螺水泥", "601012": "隆基绿能", "600809": "山西汾酒",
    "000568": "泸州老窖", "002304": "洋河股份", "000596": "古井贡酒",
    "600036": "招商银行", "601328": "交通银行", "600016": "民生银行",
    "601601": "中国太保", "601628": "中国人寿", "601336": "新华保险",
    "000651": "格力电器", "000100": "TCL科技", "002475": "立讯精密",
    "601138": "工业富联", "600690": "海尔智家", "000063": "中兴通讯",
    "300124": "汇川技术", "300274": "阳光电源", "601899": "紫金矿业",
    "600028": "中国石化", "601857": "中国石油", "600188": "兖矿能源",
    "601225": "陕西煤业", "600438": "通威股份", "002129": "TCL中环",
    "601985": "中国核电", "600905": "三峡能源", "300760": "迈瑞医疗",
    "603259": "药明康德", "300015": "爱尔眼科", "002007": "华兰生物",
    "600196": "复星医药", "000661": "长春高新", "605117": "德业股份",
    "002459": "晶澳科技", "688223": "晶科能源", "600089": "特变电工",
    "601668": "中国建筑", "600031": "三一重工", "000338": "潍柴动力",
    "600104": "上汽集团", "000625": "长安汽车", "601238": "广汽集团",
    "600941": "中国移动", "600050": "中国联通", "601728": "中国电信",
    "688111": "金山办公", "002230": "科大讯飞", "300033": "同花顺",
    "300750": "宁德时代", "002340": "格林美", "600884": "杉杉股份",
    "300014": "亿纬锂能", "688005": "容百科技", "603799": "华友钴业",
    "002460": "赣锋锂业", "002466": "天齐锂业", "600111": "北方稀土",
    "600010": "包钢股份", "600019": "宝钢股份", "000708": "中信特钢",
    "002352": "顺丰控股", "601006": "大秦铁路", "002142": "宁波银行",
    "600000": "浦发银行", "600015": "华夏银行", "601009": "南京银行",
    "601838": "成都银行", "600919": "江苏银行", "601229": "上海银行",
    "300413": "芒果超媒", "002602": "世纪华通", "300253": "卫宁健康",
    "600588": "用友网络", "002410": "广联达", "603986": "兆易创新",
    "688012": "中微公司", "688396": "华润微", "603501": "韦尔股份",
    "002049": "紫光国微", "600745": "闻泰科技", "603160": "汇顶科技",
    "688008": "澜起科技", "002371": "北方华创", "688036": "传音控股",
    "300433": "蓝思科技", "002241": "歌尔股份", "601766": "中国中车",
    "600150": "中国船舶", "600893": "航发动力", "600760": "中航沈飞",
    "002179": "中航光电", "600685": "中船防务", "600862": "中航高科",
    "002714": "牧原股份", "000895": "双汇发展", "600598": "北大荒",
    "300498": "温氏股份", "002311": "海大集团", "600309": "万华化学",
    "601225": "陕西煤业", "600346": "恒力石化", "000301": "东方盛虹",
    "600028": "中国石化", "600352": "浙江龙盛", "601233": "桐昆股份",
}

# 反向映射：名称→代码
STOCK_NAME_TO_CODE: Dict[str, str] = {v: k for k, v in A_SHARE_STOCKS.items()}

# ═══════════════════════════════════════════════════
# 行业板块映射
# ═══════════════════════════════════════════════════
INDUSTRY_KEYWORDS: Dict[str, List[str]] = {
    "白酒": ["白酒", "茅台", "五粮液", "酒", "酿酒", "白酒板块", "白酒行业"],
    "新能源": ["新能源", "锂电池", "光伏", "风电", "氢能", "储能", "新能源车"],
    "新能源汽车": ["新能源汽车", "电动汽车", "新能源车", "电动车", "动力电池", "充电桩"],
    "半导体": ["半导体", "芯片", "集成电路", "晶圆", "封测", "光刻机", "IC设计"],
    "医药": ["医药", "药品", "创新药", "仿制药", "中药", "生物医药", "医疗器械"],
    "医疗": ["医疗", "医疗器械", "医疗服务", "医疗设备", "医院", "IVD"],
    "金融": ["金融", "银行", "保险", "证券", "券商", "信托", "基金"],
    "银行": ["银行", "商业银行", "股份制银行", "城商行", "农商行"],
    "券商": ["券商", "证券", "证券公司", "经纪", "投行"],
    "保险": ["保险", "寿险", "财险", "保险公司", "再保险"],
    "房地产": ["房地产", "地产", "房产", "住宅", "商业地产", "物管"],
    "消费": ["消费", "消费品", "零售", "电商", "免税", "新零售", "食品饮料"],
    "食品饮料": ["食品", "饮料", "乳业", "调味品", "预制菜", "休闲食品"],
    "家电": ["家电", "白色家电", "黑色家电", "空调", "冰箱", "洗衣机", "小家电"],
    "煤炭": ["煤炭", "煤", "焦煤", "焦炭", "动力煤", "煤化工"],
    "石油石化": ["石油", "石化", "原油", "天然气", "炼化", "油气"],
    "有色金属": ["有色", "有色金属", "铜", "铝", "锌", "镍", "锂", "钴", "稀土"],
    "钢铁": ["钢铁", "钢材", "粗钢", "螺纹钢", "热轧", "冷轧"],
    "化工": ["化工", "化学", "化肥", "农药", "化纤", "纯碱", "PVC", "MDI"],
    "光伏": ["光伏", "太阳能", "光伏组件", "逆变器", "硅料", "硅片", "电池片"],
    "锂电池": ["锂电池", "锂电", "碳酸锂", "磷酸铁锂", "三元锂", "电解液", "隔膜"],
    "军工": ["军工", "国防", "航空", "航天", "船舶", "军工电子", "军机"],
    "人工智能": ["人工智能", "AI", "智能", "机器学习", "大模型", "算力"],
    "云计算": ["云计算", "云服务", "SaaS", "IaaS", "PaaS", "数据中心"],
    "通信": ["通信", "5G", "6G", "光通信", "光模块", "光纤", "基站"],
    "基建": ["基建", "建筑", "工程", "铁路", "公路", "桥梁", "隧道"],
    "机械": ["机械", "工程机械", "智能制造", "机器人", "自动化", "数控"],
    "电子": ["电子", "消费电子", "元器件", "面板", "LED", "传感器"],
    "新能源发电": ["光伏发电", "风电", "核电", "水电", "新能源发电", "绿色电力"],
    "传媒": ["传媒", "广告", "影视", "游戏", "出版", "互联网", "IP"],
    "教育": ["教育", "培训", "K12", "职业教育", "高等教育"],
    "农林牧渔": ["农业", "养殖", "种植", "猪肉", "鸡肉", "饲料", "种业"],
    "交通运输": ["运输", "航空", "铁路", "港口", "航运", "物流", "快递"],
    "汽车": ["汽车", "整车", "汽车零部件", "零部件", "发动机", "变速箱"],
    "环保": ["环保", "环境", "污水处理", "固废", "碳中和", "节能"],
    "纺织服装": ["纺织", "服装", "鞋帽", "家纺", "面料", "品牌服饰"],
    "轻工制造": ["轻工", "造纸", "包装", "家居", "家具", "印刷"],
    "商贸零售": ["商贸", "零售", "百货", "超市", "连锁", "购物中心"],
}

# ═══════════════════════════════════════════════════
# 产品/商品名词典
# ═══════════════════════════════════════════════════
PRODUCT_KEYWORDS: Dict[str, List[str]] = {
    "碳酸锂": ["碳酸锂", "电池级碳酸锂", "工业级碳酸锂"],
    "锂": ["锂", "锂矿", "锂资源", "锂盐", "氢氧化锂", "金属锂"],
    "钴": ["钴", "钴矿", "钴盐", "金属钴", "硫酸钴"],
    "镍": ["镍", "镍矿", "镍盐", "硫酸镍", "金属镍"],
    "铜": ["铜", "铜矿", "电解铜", "铜箔", "铜杆", "铜管"],
    "铝": ["铝", "铝矿", "电解铝", "氧化铝", "铝箔", "铝合金"],
    "稀土": ["稀土", "稀土永磁", "氧化镨钕", "镨钕", "镝", "铽"],
    "硅": ["硅", "硅料", "工业硅", "有机硅", "多晶硅", "单晶硅"],
    "光伏组件": ["光伏组件", "组件", "太阳能组件", "光伏板", "太阳能板"],
    "逆变器": ["逆变器", "光伏逆变器", "储能逆变器", "微型逆变器"],
    "锂电池": ["锂电池", "锂离子电池", "动力电池", "储能电池", "电池"],
    "芯片": ["芯片", "半导体芯片", "AI芯片", "GPU", "CPU", "存储芯片", "模拟芯片"],
    "光模块": ["光模块", "光模块", "高速光模块", "800G光模块", "400G光模块"],
    "服务器": ["服务器", "AI服务器", "算力服务器", "云服务器"],
    "猪肉": ["猪肉", "生猪", "猪", "猪肉价格", "猪周期"],
    "原油": ["原油", "石油", "WTI原油", "布伦特原油", "国际油价"],
    "煤炭": ["煤炭", "原煤", "焦煤", "动力煤", "喷吹煤"],
    "钢铁": ["钢铁", "钢材", "螺纹钢", "热轧卷板", "冷轧板"],
    "黄金": ["黄金", "国际金价", "黄金价格", "黄金ETF", "实物黄金"],
    "白酒": ["白酒", "茅台酒", "五粮液", "泸州老窖", "汾酒", "洋河", "白酒价格"],
}

# ═══════════════════════════════════════════════════
# 高管/分析师/政要人名姓氏列表
# ═══════════════════════════════════════════════════
_PERSON_SURNAMES = [
    "张", "王", "李", "赵", "刘", "陈", "杨", "黄", "吴", "周", "徐", "孙",
    "马", "胡", "朱", "郭", "何", "罗", "高", "林", "郑", "梁", "谢", "唐",
    "许", "冯", "宋", "韩", "邓", "彭", "曹", "曾", "田", "萧", "潘", "袁",
    "蔡", "蒋", "余", "于", "杜", "叶", "程", "魏", "苏", "吕", "丁", "沈",
    "任", "姚", "卢", "姜", "崔", "钟", "谭", "陆", "汪", "范", "金", "石",
    "廖", "贾", "夏", "韦", "付", "方", "白", "邹", "孟", "熊", "秦", "邱",
    "江", "尹", "薛", "闫", "段", "雷", "侯", "龙", "史", "陶", "贺", "顾",
    "毛", "郝", "龚", "邵", "万", "钱", "严", "覃", "武", "戴", "莫", "孔",
    "向", "汤", "申", "范", "纪", "项", "柯", "管",
]

# 头衔后缀（出现在人名后，辅助识别）
_TITLE_SUFFIXES = [
    "董事长", "总裁", "总经理", "CEO", "CFO", "CTO", "副总经理", "董秘",
    "财务总监", "销售总监", "技术总监", "首席经济学家", "分析师",
    "主席", "行长", "部长", "主任", "局长", "处长", "科长",
    "教授", "院长", "所长", "博士", "院士",
    "会长", "秘书长", "理事",
]

# 职位前缀（出现在人名前，辅助识别）
_TITLE_PREFIXES = [
    "董事长", "总裁", "总经理", "CEO", "CFO", "CTO",
    "创始人", "实控人", "实际控制人", "控股股东",
    "首席", "联席",
]


# ═══════════════════════════════════════════════════
# NER 函数
# ═══════════════════════════════════════════════════

def extract_company_names(text: str) -> List[str]:
    """
    从文本中识别公司名称
    基于 A 股上市公司名称词典 + 模式匹配

    Args:
        text: 输入文本

    Returns:
        公司名列表（去重，保持顺序）
    """
    if not text:
        return []

    found: List[str] = []
    seen: Set[str] = set()

    try:
        # 1. 精确匹配上市公司名称（优先匹配长名称）
        sorted_names = sorted(STOCK_NAME_TO_CODE.keys(), key=len, reverse=True)
        rest = text
        for name in sorted_names:
            if name in rest and name not in seen:
                found.append(name)
                seen.add(name)
                rest = rest.replace(name, "█" * len(name), 1)

        # 2. 模式匹配：含"有限公司"/"集团"/"股份"等关键词
        company_patterns = [
            r"([\u4e00-\u9fff]{2,10}(?:有限公司|股份有限公司|股份公司|有限公司分公司))",
            r"([\u4e00-\u9fff]{2,8}(?:集团|股份|证券|基金|银行|保险|信托|投资|实业|控股))公司?",
            r"([\u4e00-\u9fff]{2,8}(?:科技|技术|医药|能源|地产|建设|工程|制造))公司?",
        ]
        for pattern in company_patterns:
            for m in re.finditer(pattern, text):
                name = m.group(1)
                if name not in seen and len(name) >= 4:
                    found.append(name)
                    seen.add(name)

        # 3. 匹配"XX公司"（2~4字公司名）
        for m in re.finditer(r"([\u4e00-\u9fff]{2,4})公司", text):
            name = m.group(0)
            if name not in seen:
                found.append(name)
                seen.add(name)

    except Exception as e:
        logger.warning(f"公司名提取异常: {e}")

    return found


def extract_people(text: str) -> List[str]:
    """
    从文本中识别人名（高管/分析师/政要）

    匹配模式：
    1. 姓氏 + 1~2个字（标准中文姓名）
    2. 人名 + 头衔后缀
    3. 头衔前缀 + 人名

    Args:
        text: 输入文本

    Returns:
        人名列表（去重，保持顺序）
    """
    if not text:
        return []

    found: List[str] = []
    seen: Set[str] = set()

    try:
        # 模式1：姓名 + 头衔后缀（更精确）
        for m in re.finditer(
            r"([张王李赵刘陈杨黄吴周徐孙马胡朱郭何罗高林郑梁谢唐许冯宋韩邓彭曹曾田萧潘袁蔡蒋余于杜叶程魏苏吕丁沈任姚卢姜崔钟谭陆汪范金石廖贾夏韦付方白邹孟熊秦邱江尹薛闫段雷侯龙史陶贺顾毛郝龚邵万钱严覃武戴莫孔向汤])([\u4e00-\u9fff]{1,3}(?:" + "|".join(_TITLE_SUFFIXES) + r"))",
            text
        ):
            full_name = m.group(1) + m.group(2)
            # 去掉头衔后缀，只保留名字部分
            for suffix in _TITLE_SUFFIXES:
                if full_name.endswith(suffix):
                    name_part = full_name[:-len(suffix)]
                    if len(name_part) >= 2 and name_part not in seen:
                        found.append(name_part)
                        seen.add(name_part)
                    break

        # 模式2：头衔前缀 + 姓名
        for m in re.finditer(
            r"(?:(?:" + "|".join(_TITLE_PREFIXES) + r")[：: ]?([\u4e00-\u9fff]{2,4}))",
            text
        ):
            name = m.group(1)
            if name[0] in _PERSON_SURNAMES and name not in seen:
                found.append(name)
                seen.add(name)

        # 模式3：通用中文姓名（姓氏+1~2字）
        for m in re.finditer(
            r"([张王李赵刘陈杨黄吴周徐孙马胡朱郭何罗高林郑梁谢唐许冯宋韩邓彭曹曾田萧潘袁蔡蒋余于杜叶程魏苏吕丁沈任姚卢姜崔钟谭陆汪范金石廖贾夏韦付方白邹孟熊秦邱江尹薛闫段雷侯龙史陶贺顾毛郝龚邵万钱严覃武戴莫孔向汤])([\u4e00-\u9fff]{1,3})",
            text
        ):
            name = m.group(0)
            # 过滤常见误识别
            if any(kw in name for kw in ["公司", "集团", "股份", "证券", "银行", "基金"]):
                continue
            if len(name) >= 2 and len(name) <= 4 and name not in seen:
                found.append(name)
                seen.add(name)

    except Exception as e:
        logger.warning(f"人名提取异常: {e}")

    return found


def extract_products(text: str) -> List[str]:
    """
    从文本中识别产品/商品名
    如：锂、碳酸锂、芯片、光伏、猪肉、原油等

    Args:
        text: 输入文本

    Returns:
        产品名列表（去重，按匹配精度排序）
    """
    if not text:
        return []

    found: List[str] = []
    seen: Set[str] = set()

    try:
        # 按关键词长度降序匹配（优先匹配长名词）
        for product, keywords in sorted(
            PRODUCT_KEYWORDS.items(),
            key=lambda x: max(len(k) for k in x[1]),
            reverse=True
        ):
            for kw in keywords:
                if kw in text and product not in seen:
                    found.append(product)
                    seen.add(product)
                    break

        # 额外模式匹配：常见产品模式
        extra_patterns = [
            r"(\d+\.?\d*)\s*寸晶圆",
            r"([\u4e00-\u9fff]{2,6})期货",
            r"([\u4e00-\u9fff]{2,8})现货",
            r"([\u4e00-\u9fff]{2,6})价格",
        ]
        for pattern in extra_patterns:
            for m in re.finditer(pattern, text):
                # 提取关键产品名
                candidate = m.group(1) if m.lastindex else m.group(0)
                if candidate and candidate not in seen and len(candidate) <= 10:
                    # 避免误抓
                    if not any(excl in candidate for excl in ["公司", "集团"]):
                        found.append(candidate)
                        seen.add(candidate)

    except Exception as e:
        logger.warning(f"产品名提取异常: {e}")

    return found


def extract_industry_sectors(text: str) -> List[str]:
    """
    从文本中识别行业板块

    Args:
        text: 输入文本

    Returns:
        行业板块列表（去重，按匹配度排序）
    """
    if not text:
        return []

    found: List[str] = []
    seen: Set[str] = set()

    try:
        # 按关键词匹配度排序
        scored_sectors: List[tuple] = []
        for sector, keywords in INDUSTRY_KEYWORDS.items():
            score = 0
            for kw in keywords:
                if kw in text:
                    # 完整关键词匹配得分更高
                    score += text.count(kw) * (len(kw) / 2)

            if score > 0:
                scored_sectors.append((sector, score))

        # 按匹配分降序
        scored_sectors.sort(key=lambda x: x[1], reverse=True)

        for sector, _ in scored_sectors:
            found.append(sector)
            seen.add(sector)

    except Exception as e:
        logger.warning(f"行业板块提取异常: {e}")

    return found


def extract_stock_mentions(text: str) -> List[Dict[str, str]]:
    """
    从文本中识别提到的股票代码和名称

    支持格式：
    - 6位数字股票代码（600519, 000858, 300750）
    - 股票名称匹配（贵州茅台, 宁德时代）
    - 代码+市场后缀（600519.SH, 000858.SZ）

    Args:
        text: 输入文本

    Returns:
        [{"code": "600519", "name": "贵州茅台", "source": "code/namedict/name"}, ...]
    """
    if not text:
        return []

    found: List[Dict[str, str]] = []
    seen_codes: Set[str] = set()

    try:
        # 1. 精确匹配6位代码
        code_pattern = re.compile(r"(?<!\d)(6\d{5}|0\d{5}|3\d{5}|8\d{5}|4\d{5})(?!\d)")
        for m in code_pattern.finditer(text):
            code = m.group(1)
            if code not in seen_codes:
                seen_codes.add(code)
                name = A_SHARE_STOCKS.get(code, "")
                found.append({
                    "code": code,
                    "name": name,
                    "source": "code",
                })

        # 2. 匹配带市场后缀的
        for m in re.finditer(
            r"(?<!\d)(6\d{5}|0\d{5}|3\d{5}|8\d{5}|4\d{5})[\.](SH|SZ|BJ)",
            text, re.IGNORECASE
        ):
            code = m.group(1)
            if code not in seen_codes:
                seen_codes.add(code)
                name = A_SHARE_STOCKS.get(code, "")
                found.append({
                    "code": code,
                    "name": name,
                    "source": "code_with_suffix",
                })

        # 3. 匹配股票名称（词库）
        # 先匹配长名称
        sorted_names = sorted(STOCK_NAME_TO_CODE.keys(), key=len, reverse=True)
        for name in sorted_names:
            if name in text and STOCK_NAME_TO_CODE[name] not in seen_codes:
                code = STOCK_NAME_TO_CODE[name]
                seen_codes.add(code)
                found.append({
                    "code": code,
                    "name": name,
                    "source": "namedict",
                })

        # 4. 模式匹配：XX集团/XX股份 等常见公司名模式
        for m in re.finditer(r"([\u4e00-\u9fff]{2,8}(?:集团|股份|科技|医药|能源))", text):
            company_name = m.group(1)
            if company_name and company_name not in {f["name"] for f in found}:
                # 尝试在词典中查找
                for dict_name, dict_code in STOCK_NAME_TO_CODE.items():
                    if company_name in dict_name or dict_name in company_name:
                        if dict_code not in seen_codes:
                            seen_codes.add(dict_code)
                            found.append({
                                "code": dict_code,
                                "name": dict_name,
                                "source": "pattern_match",
                            })
                        break

    except Exception as e:
        logger.warning(f"股票提及提取异常: {e}")

    return found


def extract_financial_entities(text: str) -> Dict[str, List]:
    """
    综合金融实体提取
    一次性提取所有类型的金融实体

    Args:
        text: 输入文本

    Returns:
        {
            "companies": [...],
            "people": [...],
            "products": [...],
            "sectors": [...],
            "stock_mentions": [{"code": "...", "name": "...", "source": "..."}, ...],
        }
    """
    result = {
        "companies": [],
        "people": [],
        "products": [],
        "sectors": [],
        "stock_mentions": [],
    }

    try:
        if not text:
            return result

        text_sample = text[:5000]  # 防止超长文本

        result["companies"] = extract_company_names(text_sample)
        result["people"] = extract_people(text_sample)
        result["products"] = extract_products(text_sample)
        result["sectors"] = extract_industry_sectors(text_sample)
        result["stock_mentions"] = extract_stock_mentions(text_sample)

    except Exception as e:
        logger.error(f"综合金融实体提取异常: {e}")

    return result


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    test_text = """
    贵州茅台今日公告称，董事长丁雄军表示公司一季度业绩增长超预期。
    同时，碳酸锂价格持续下跌，宁德时代、比亚迪等新能源龙头企业受到影响。
    半导体板块表现抢眼，中芯国际、北方华创领涨。芯片产能紧张局面有望缓解。
    国务院出台新政策支持新能源汽车产业发展，锂矿资源概念股受到资金追捧。
    光伏产业链方面，隆基绿能、通威股份、阳光电源纷纷公告扩产计划。
    公司方面，中国平安发布年报，净利润同比增长，招商银行不良率下降。
    """

    print(f"===== 综合金融实体提取测试 =====")
    print(f"\n输入文本（部分）：\n{test_text.strip()[:200]}...\n")

    result = extract_financial_entities(test_text)

    print(f"\n🏢 公司名 ({len(result['companies'])}):")
    for c in result['companies']:
        print(f"  - {c}")

    print(f"\n👤 人名 ({len(result['people'])}):")
    for p in result['people']:
        print(f"  - {p}")

    print(f"\n📦 产品名 ({len(result['products'])}):")
    for p in result['products']:
        print(f"  - {p}")

    print(f"\n📊 行业板块 ({len(result['sectors'])}):")
    for s in result['sectors']:
        print(f"  - {s}")

    print(f"\n💹 股票提及 ({len(result['stock_mentions'])}):")
    for sm in result['stock_mentions']:
        print(f"  - {sm['code']} {sm['name']} (来源: {sm['source']})")
