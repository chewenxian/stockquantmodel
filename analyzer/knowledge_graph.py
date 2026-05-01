"""
金融知识图谱（轻量级）
用 JSON 实现，不需要图数据库

功能：
1. 产业链关系（上下游）
2. 竞品关系（同行业竞争）
3. 板块归属（股票→行业）
4. 推理新闻的间接影响
"""
import re
import logging
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class FinancialKnowledgeGraph:
    """
    金融知识图谱
    存储产业链关系、股权关联、竞品关系
    用于推理新闻的间接影响
    """

    def __init__(self):
        # 内置知识图谱数据
        # 这些数据硬编码在代码中，无需外部数据库
        self._init_industry_chains()
        self._init_competitors()
        self._init_sector_mapping()
        self._init_stock_code_index()

    # ──────────────────────────────────────────
    # 产业链关系
    # ──────────────────────────────────────────

    def _init_industry_chains(self):
        """
        产业链关系定义
        格式：{product_or_sector: {"上游": [...], "下游": [...]}}
        """
        self.industry_chains: Dict[str, Dict[str, List[str]]] = {
            # ── 锂电池产业链 ──
            "碳酸锂": {
                "上游": ["锂矿", "盐湖提锂", "锂资源"],
                "下游": ["正极材料", "锂电池", "电解液", "储能电池", "新能源汽车"],
            },
            "锂矿": {
                "上游": ["锂资源勘探", "矿山开采"],
                "下游": ["碳酸锂", "氢氧化锂", "锂盐"],
            },
            "锂电池": {
                "上游": ["碳酸锂", "氢氧化锂", "正极材料", "负极材料", "电解液", "隔膜", "锂矿"],
                "下游": ["新能源汽车", "储能", "消费电子", "电动工具"],
            },
            "正极材料": {
                "上游": ["碳酸锂", "钴", "镍", "锰", "锂矿"],
                "下游": ["锂电池", "动力电池"],
            },
            "负极材料": {
                "上游": ["石墨", "针状焦", "硅碳"],
                "下游": ["锂电池"],
            },
            "电解液": {
                "上游": ["六氟磷酸锂", "溶剂", "添加剂", "碳酸锂"],
                "下游": ["锂电池"],
            },
            "隔膜": {
                "上游": ["聚丙烯", "聚乙烯", "基膜"],
                "下游": ["锂电池"],
            },
            "新能源汽车": {
                "上游": ["锂电池", "电机", "电控", "汽车零部件", "芯片", "碳酸锂"],
                "下游": ["汽车销售", "充电桩", "电池回收"],
            },

            # ── 光伏产业链 ──
            "光伏": {
                "上游": ["硅料", "硅片", "工业硅"],
                "下游": ["光伏电站", "分布式光伏", "储能系统"],
            },
            "硅料": {
                "上游": ["工业硅", "电力"],
                "下游": ["硅片", "光伏组件"],
            },
            "硅片": {
                "上游": ["硅料"],
                "下游": ["光伏电池", "光伏组件"],
            },
            "光伏组件": {
                "上游": ["硅片", "光伏电池", "光伏玻璃", "胶膜", "背板", "边框"],
                "下游": ["光伏电站", "分布式光伏"],
            },
            "逆变器": {
                "上游": ["功率半导体", "IGBT", "电容", "电感"],
                "下游": ["光伏电站", "储能系统", "分布式光伏"],
            },

            # ── 半导体产业链 ──
            "芯片": {
                "上游": ["半导体设备", "半导体材料", "EDA软件", "晶圆"],
                "下游": ["消费电子", "汽车电子", "人工智能", "通信设备", "物联网"],
            },
            "半导体设备": {
                "上游": ["精密机械", "光学器件", "传感器", "射频器件"],
                "下游": ["芯片制造", "晶圆代工", "封测"],
            },
            "半导体材料": {
                "上游": ["硅片", "光刻胶", "电子特气", "靶材", "化学品"],
                "下游": ["芯片制造"],
            },
            "存储芯片": {
                "上游": ["半导体设备", "半导体材料", "晶圆"],
                "下游": ["消费电子", "服务器", "数据中心"],
            },

            # ── 人工智能 ──
            "人工智能": {
                "上游": ["芯片", "算力", "服务器", "数据", "云计算"],
                "下游": ["AI应用", "大模型", "自动驾驶", "智能家居", "机器人"],
            },
            "算力": {
                "上游": ["AI芯片", "GPU", "服务器", "光模块", "数据中心"],
                "下游": ["人工智能", "云计算", "大模型训练"],
            },
            "光模块": {
                "上游": ["光芯片", "电芯片", "光学器件", "PCB"],
                "下游": ["数据中心", "通信设备", "算力网络"],
            },

            # ── 消费电子 ──
            "消费电子": {
                "上游": ["芯片", "面板", "存储器", "传感器", "电池"],
                "下游": ["手机", "电脑", "可穿戴设备", "智能家居"],
            },

            # ── 医药 ──
            "创新药": {
                "上游": ["原料药", "CXO", "生物试剂", "实验设备"],
                "下游": ["医院", "药房", "医保", "患者"],
            },
            "CXO": {
                "上游": ["实验试剂", "实验动物", "设备"],
                "下游": ["创新药", "仿制药", "生物医药"],
            },
            "医疗器械": {
                "上游": ["精密制造", "芯片", "传感器", "医用材料"],
                "下游": ["医院", "诊所", "家庭医疗"],
            },
            "中药": {
                "上游": ["中药材种植", "中药材贸易"],
                "下游": ["医院", "药房", "OTC市场"],
            },

            # ── 煤炭 ──
            "煤炭": {
                "上游": ["煤矿开采", "煤炭勘探"],
                "下游": ["火电", "钢铁", "煤化工", "水泥"],
            },
            "煤化工": {
                "上游": ["煤炭"],
                "下游": ["甲醇", "尿素", "烯烃", "乙二醇"],
            },

            # ── 钢铁 ──
            "钢铁": {
                "上游": ["铁矿石", "焦煤", "焦炭", "废钢"],
                "下游": ["基建", "房地产", "汽车", "机械", "造船"],
            },

            # ── 石油石化 ──
            "原油": {
                "上游": ["石油勘探", "石油开采"],
                "下游": ["炼化", "化工", "成品油", "航空燃料"],
            },
            "炼化": {
                "上游": ["原油"],
                "下游": ["成品油", "化工品", "塑料", "化纤"],
            },

            # ── 房地产 ──
            "房地产": {
                "上游": ["水泥", "钢铁", "玻璃", "建材", "工程机械"],
                "下游": ["装修", "家电", "家居", "物业管理"],
            },
            "基建": {
                "上游": ["钢铁", "水泥", "工程机械", "沥青"],
                "下游": ["交通运输", "城市建设", "水利"],
            },

            # ── 军工 ──
            "军工": {
                "上游": ["特种材料", "精密制造", "芯片", "雷达"],
                "下游": ["国防装备", "航天", "航空"],
            },

            # ── 新能源汽车零部件 ──
            "动力电池": {
                "上游": ["锂电池", "正极材料", "负极材料", "电解液", "隔膜"],
                "下游": ["新能源汽车"],
            },
            "充电桩": {
                "上游": ["功率模块", "变压器", "电缆", "芯片"],
                "下游": ["新能源汽车", "充电运营"],
            },

            # ── 农业 ──
            "猪肉": {
                "上游": ["饲料", "玉米", "豆粕", "种猪"],
                "下游": ["屠宰", "肉制品加工", "冷链物流"],
            },
        }

    # ──────────────────────────────────────────
    # 竞品关系
    # ──────────────────────────────────────────

    def _init_competitors(self):
        """
        竞品关系（同行业竞争公司）
        格式：{code_or_name: [code_or_name, ...]}
        """
        self.competitors: Dict[str, List[str]] = {
            # 白酒
            "600519": ["000858", "000568", "002304", "600809", "000596"],
            "000858": ["600519", "000568", "002304", "600809", "000596"],
            "000568": ["600519", "000858", "002304", "600809", "000596"],
            "002304": ["600519", "000858", "000568", "600809", "000596"],
            "600809": ["600519", "000858", "000568", "002304", "000596"],

            # 新能源电池
            "300750": ["002594", "300014", "002340", "688005"],
            "002594": ["300750", "600104", "000625"],
            "300014": ["300750", "002340", "688005"],

            # 锂矿
            "002460": ["002466", "600111"],
            "002466": ["002460", "600111"],

            # 光伏
            "600438": ["688599", "601012", "002459"],
            "601012": ["600438", "688599", "002459"],
            "002459": ["600438", "601012", "688599"],
            "300274": ["605117", "688390"],

            # 芯片/半导体
            "688981": ["688012", "688396", "603986", "002049"],
            "002371": ["688012", "603986", "688396"],
            "603986": ["688981", "002049", "688008"],

            # 银行
            "600036": ["601166", "600016", "000001", "601398", "601939"],
            "601166": ["600036", "600016", "000001"],

            # 保险
            "601318": ["601601", "601628"],

            # 家电
            "000333": ["000651", "600690"],
            "000651": ["000333", "600690"],
            "600690": ["000333", "000651"],

            # 医药
            "600276": ["300760", "603259", "300015"],
            "300760": ["600276", "603259", "300015"],

            # 煤炭
            "601225": ["600188", "600546"],
            "600188": ["601225", "600546"],

            # 券商
            "600030": ["601211", "600837", "002736", "601688"],

            # 通信
            "600941": ["600050", "601728"],
            "000063": ["600941", "600050"],

            # 电动车整车
            "002594": ["600104", "000625", "601238", "601633"],
        }

    # ──────────────────────────────────────────
    # 行业板块归属
    # ──────────────────────────────────────────

    def _init_sector_mapping(self):
        """
        股票→行业板块归属映射
        格式：{code: [sector1, sector2, ...]}
        """
        self.sector_map: Dict[str, List[str]] = {
            "600519": ["白酒", "消费"],
            "000858": ["白酒", "消费"],
            "000568": ["白酒", "消费"],
            "002304": ["白酒", "消费"],
            "600809": ["白酒", "消费"],
            "000596": ["白酒", "消费"],

            "300750": ["锂电池", "新能源汽车", "新能源"],
            "002594": ["新能源汽车", "汽车", "新能源"],
            "300014": ["锂电池", "新能源"],
            "002340": ["锂电池", "有色金属", "新能源"],
            "688005": ["锂电池", "新能源"],

            "002460": ["有色金属", "锂电池", "锂矿"],
            "002466": ["有色金属", "锂电池", "锂矿"],
            "600111": ["有色金属", "稀土"],

            "600438": ["光伏", "新能源"],
            "601012": ["光伏", "新能源"],
            "002459": ["光伏", "新能源"],
            "688599": ["光伏", "新能源"],
            "300274": ["光伏", "新能源", "逆变器"],
            "605117": ["光伏", "新能源", "逆变器"],
            "688390": ["光伏", "新能源", "逆变器"],
            "600089": ["光伏", "新能源", "电力设备"],

            "688981": ["半导体", "芯片"],
            "688012": ["半导体", "半导体设备", "芯片"],
            "688396": ["半导体", "芯片"],
            "603986": ["半导体", "芯片", "存储芯片"],
            "002049": ["半导体", "芯片"],
            "688008": ["半导体", "芯片"],
            "002371": ["半导体", "半导体设备"],
            "603501": ["半导体", "芯片"],
            "603160": ["半导体", "芯片"],

            "600036": ["银行", "金融"],
            "601166": ["银行", "金融"],
            "600016": ["银行", "金融"],
            "000001": ["银行", "金融"],
            "601398": ["银行", "金融"],
            "601939": ["银行", "金融"],
            "601288": ["银行", "金融"],
            "601988": ["银行", "金融"],
            "601328": ["银行", "金融"],
            "002142": ["银行", "金融"],

            "601318": ["保险", "金融"],
            "601601": ["保险", "金融"],
            "601628": ["保险", "金融"],

            "600030": ["券商", "金融"],
            "601211": ["券商", "金融"],
            "600837": ["券商", "金融"],

            "000333": ["家电", "消费"],
            "000651": ["家电", "消费"],
            "600690": ["家电", "消费"],

            "300059": ["券商", "金融", "互联网金融"],

            "600276": ["医药", "创新药"],
            "300760": ["医疗器械", "医药"],
            "603259": ["医药", "CXO"],
            "300015": ["医药", "医疗服务"],

            "600941": ["通信", "电信"],
            "600050": ["通信", "电信"],
            "601728": ["通信", "电信"],
            "000063": ["通信", "5G"],

            "601225": ["煤炭", "能源"],
            "600188": ["煤炭", "能源"],
            "600546": ["煤炭", "能源"],

            "002415": ["电子", "安防", "人工智能"],
            "002230": ["人工智能", "AI", "科技"],
            "688111": ["人工智能", "云计算", "软件"],

            "600031": ["机械", "工程机械"],
            "000338": ["机械", "汽车零部件"],
            "600150": ["军工", "船舶"],
            "600893": ["军工", "航空"],
            "600760": ["军工", "航空"],

            "600900": ["电力", "新能源发电"],
            "601985": ["电力", "核电"],
            "600905": ["电力", "新能源发电"],

            "002714": ["农林牧渔", "猪肉"],
            "000895": ["农林牧渔", "食品饮料"],
            "300498": ["农林牧渔", "猪肉"],

            "600585": ["建材", "水泥", "基建"],
            "600019": ["钢铁", "材料"],
            "000708": ["钢铁", "材料"],

            "002352": ["交通运输", "物流", "快递"],
            "601006": ["交通运输", "铁路"],

            "600886": ["电力", "新能源发电"],
            "601668": ["基建", "建筑"],
            "600104": ["汽车", "新能源汽车"],
            "000625": ["汽车", "新能源汽车"],
            "601238": ["汽车", "新能源汽车"],
            "601899": ["有色金属", "黄金", "铜"],
            "600028": ["石油石化", "能源"],
            "601857": ["石油石化", "能源"],
            "600309": ["化工", "材料"],
            "600346": ["化工", "石油石化"],
        }

    # ──────────────────────────────────────────
    # 股票代码索引
    # ──────────────────────────────────────────

    def _init_stock_code_index(self):
        """
        建立股票代码/名称的双向索引
        """
        self._code_to_name: Dict[str, str] = {}
        self._name_to_code: Dict[str, str] = {}

        # 尝试从 ner_extractor 导入完整的股票词典
        imported = False
        try:
            import sys, os
            _kg_dir = os.path.dirname(os.path.abspath(__file__))
            _proj_dir = os.path.dirname(_kg_dir)
            if _proj_dir not in sys.path:
                sys.path.insert(0, _proj_dir)
            from analyzer.ner_extractor import A_SHARE_STOCKS, STOCK_NAME_TO_CODE
            self._code_to_name = dict(A_SHARE_STOCKS)
            self._name_to_code = dict(STOCK_NAME_TO_CODE)
            imported = True
        except Exception:
            pass

        if not imported:
            # 兜底：完整的 A 股股票列表（与 ner_extractor 保持一致）
            fallback = {
                "600519": "贵州茅台", "000858": "五粮液", "300750": "宁德时代",
                "601318": "中国平安", "000333": "美的集团", "600036": "招商银行",
                "002594": "比亚迪", "688981": "中芯国际", "600900": "长江电力",
                "000001": "平安银行", "600887": "伊利股份", "601166": "兴业银行",
                "600030": "中信证券", "601398": "工商银行", "601939": "建设银行",
                "601288": "农业银行", "601988": "中国银行", "600276": "恒瑞医药",
                "300059": "东方财富", "002415": "海康威视", "000002": "万科A",
                "600585": "海螺水泥", "601012": "隆基绿能", "600809": "山西汾酒",
                "000568": "泸州老窖", "002304": "洋河股份", "000596": "古井贡酒",
                "601328": "交通银行", "600016": "民生银行", "601601": "中国太保",
                "601628": "中国人寿", "601336": "新华保险", "000651": "格力电器",
                "000100": "TCL科技", "002475": "立讯精密", "601138": "工业富联",
                "600690": "海尔智家", "000063": "中兴通讯", "300124": "汇川技术",
                "300274": "阳光电源", "601899": "紫金矿业", "600028": "中国石化",
                "601857": "中国石油", "600188": "兖矿能源", "601225": "陕西煤业",
                "600438": "通威股份", "002129": "TCL中环", "601985": "中国核电",
                "600905": "三峡能源", "300760": "迈瑞医疗", "603259": "药明康德",
                "300015": "爱尔眼科", "600196": "复星医药",
                "000661": "长春高新", "605117": "德业股份", "002459": "晶澳科技",
                "688223": "晶科能源", "600089": "特变电工", "601668": "中国建筑",
                "600031": "三一重工", "000338": "潍柴动力", "600104": "上汽集团",
                "000625": "长安汽车", "601238": "广汽集团", "600941": "中国移动",
                "600050": "中国联通", "601728": "中国电信", "688111": "金山办公",
                "002230": "科大讯飞", "300033": "同花顺", "002340": "格林美",
                "600884": "杉杉股份", "300014": "亿纬锂能", "688005": "容百科技",
                "603799": "华友钴业", "002460": "赣锋锂业", "002466": "天齐锂业",
                "600111": "北方稀土", "600010": "包钢股份", "600019": "宝钢股份",
                "000708": "中信特钢", "002352": "顺丰控股", "601006": "大秦铁路",
                "002142": "宁波银行", "600000": "浦发银行",
                "601838": "成都银行", "600919": "江苏银行",
                "300413": "芒果超媒", "600588": "用友网络",
                "603986": "兆易创新", "688012": "中微公司", "688396": "华润微",
                "603501": "韦尔股份", "002049": "紫光国微", "600745": "闻泰科技",
                "688008": "澜起科技", "002371": "北方华创", "688036": "传音控股",
                "300433": "蓝思科技", "002241": "歌尔股份", "601766": "中国中车",
                "600150": "中国船舶", "600893": "航发动力", "600760": "中航沈飞",
                "002714": "牧原股份", "000895": "双汇发展",
                "300498": "温氏股份", "002311": "海大集团",
                "600309": "万华化学", "600346": "恒力石化",
            }
            self._code_to_name = fallback
            self._name_to_code = {v: k for k, v in fallback.items()}

    # ══════════════════════════════════════════
    # 核心方法
    # ══════════════════════════════════════════

    def _resolve_code(self, code_or_name: str) -> Optional[str]:
        """
        将股票代码或名称统一为代码

        Args:
            code_or_name: 股票代码（600519）或名称（贵州茅台）

        Returns:
            标准6位代码，或 None
        """
        if not code_or_name:
            return None

        # 如果是6位数字代码
        code = code_or_name.strip()
        if re.match(r"^\d{6}$", code) and code in self._code_to_name:
            return code

        # 如果是名称
        if code_or_name in self._name_to_code:
            return self._name_to_code[code_or_name]

        # 尝试近似匹配
        for name, c in self._name_to_code.items():
            if code_or_name in name or name in code_or_name:
                return c

        return None

    def _resolve_name(self, code_or_name: str) -> Optional[str]:
        """解析为股票名称"""
        code = self._resolve_code(code_or_name)
        if code:
            return self._code_to_name.get(code, code_or_name)
        # 已经是名称
        return code_or_name

    def get_chain(self, code_or_name: str) -> Dict[str, List[str]]:
        """
        获取某行业/产品的产业链上下游

        Args:
            code_or_name: 股票代码/名称/产品名/行业名

        Returns:
            {"上游": [...], "下游": [...]} 或空字典
        """
        try:
            # 先判断是否是股票代码
            code = self._resolve_code(code_or_name)

            # 如果是股票，获取其行业归属，再查找产业链
            if code:
                sectors = self.sector_map.get(code, [])
                for sector in sectors:
                    if sector in self.industry_chains:
                        return dict(self.industry_chains[sector])

                # 如果行业没找到，查产品名
                name = self._code_to_name.get(code, "")
                # 检查产品名
                for product, chain in self.industry_chains.items():
                    if product in name or name in product:
                        return dict(chain)

                return {}

            # 直接按产品/行业名查找
            if code_or_name in self.industry_chains:
                return dict(self.industry_chains[code_or_name])

            # 模糊匹配
            for key, chain in self.industry_chains.items():
                if key in code_or_name or code_or_name in key:
                    return dict(chain)

            return {}

        except Exception as e:
            logger.warning(f"获取产业链失败 ({code_or_name}): {e}")
            return {}

    def get_competitors(self, code_or_name: str) -> List[str]:
        """
        获取竞品公司列表（返回代码和名称）

        Args:
            code_or_name: 股票代码或名称

        Returns:
            [{"code": "...", "name": "..."}, ...]
        """
        try:
            code = self._resolve_code(code_or_name)
            if not code:
                return []

            competitor_codes = self.competitors.get(code, [])
            result = []
            for cc in competitor_codes:
                name = self._code_to_name.get(cc, "")
                result.append({
                    "code": cc,
                    "name": name,
                })

            # 如果是名称，按名称查
            if not competitor_codes:
                name = self._resolve_name(code_or_name)
                if name:
                    for dict_name, dict_code in self._name_to_code.items():
                        if name in dict_name or dict_name in name:
                            if dict_code != code:
                                cname = self._code_to_name.get(dict_code, "")
                                result.append({
                                    "code": dict_code,
                                    "name": cname,
                                })

            return result

        except Exception as e:
            logger.warning(f"获取竞品失败 ({code_or_name}): {e}")
            return []

    def get_sectors(self, code_or_name: str) -> List[str]:
        """
        获取某只股票所属的行业板块

        Args:
            code_or_name: 股票代码或名称

        Returns:
            板块名称列表
        """
        try:
            code = self._resolve_code(code_or_name)
            if code and code in self.sector_map:
                return list(self.sector_map[code])
            return []
        except Exception as e:
            logger.warning(f"获取板块失败 ({code_or_name}): {e}")
            return []

    def infer_impact(self, code: str, news_text: str) -> Dict:
        """
        推理新闻的间接影响
        如"碳酸锂价格暴跌"→锂矿企业利空, 电池企业利好

        Args:
            code: 受影响的核心股票代码
            news_text: 新闻文本

        Returns:
            {
                "direct_impact": "利好/利空/中性",
                "direct_reason": "...",
                "chain_reactions": [
                    {"target": "某公司", "relation": "下游", "impact": "利好", "reason": "..."},
                ],
                "related_stocks": [...],
                "impact_reasoning": ["推理步骤1", "推理步骤2"]
            }
        """
        result = {
            "direct_impact": "中性",
            "direct_reason": "",
            "chain_reactions": [],
            "related_stocks": [],
            "impact_reasoning": [],
        }

        try:
            code = self._resolve_code(code)
            if not code:
                return result

            name = self._code_to_name.get(code, code)
            sectors = self.get_sectors(code)
            reasoning: List[str] = []

            # 1. 查找新闻中提到的产品和行业
            mentioned_products = []
            mentioned_chains = []

            for product, chain in self.industry_chains.items():
                if product in news_text:
                    mentioned_products.append(product)
                    mentioned_chains.append((product, chain))

            # 2. 判断新闻情绪（简单规则）
            positive_signals = ["大涨", "涨价", "增长", "突破", "利好", "供不应求",
                                "扩产", "中标", "合同", "政策支持", "补贴"]
            negative_signals = ["大跌", "跌价", "暴跌", "亏损", "监管", "处罚",
                                "利空", "限产", "调查", "诉讼", "违约"]

            sentiment = 0.0
            for word in positive_signals:
                if word in news_text:
                    sentiment += 0.15
            for word in negative_signals:
                if word in news_text:
                    sentiment -= 0.15
            sentiment = max(-1.0, min(1.0, sentiment))

            # 3. 分析对本公司影响
            # 先查公司产业链
            chain = {}
            for sector in sectors:
                if sector in self.industry_chains:
                    chain = self.industry_chains[sector]
                    break

            # 如果有个股特定产品链
            for product, c in self.industry_chains.items():
                if product in name:
                    chain = c
                    break

            # 4. 推理传播路径
            if mentioned_products:
                for mp in mentioned_products:
                    # 看公司在哪个环节
                    mp_chain = self.industry_chains.get(mp, {})

                    # 公司是上游供应商
                    if mp in mp_chain.get("下游", []):
                        direction = "下游→上游"
                        if sentiment > 0:
                            result["direct_impact"] = "利好"
                            result["direct_reason"] = f"{mp}需求上升，作为上游供应商受益"
                            reasoning.append(f"{mp}需求增加 → 上游供应商{name}受益")
                        elif sentiment < 0:
                            result["direct_impact"] = "利空"
                            result["direct_reason"] = f"{mp}需求下降，作为上游供应商受损"
                            reasoning.append(f"{mp}需求减少 → 上游供应商{name}受损")

                    # 公司是下游客户
                    if mp in mp_chain.get("上游", []):
                        direction = "上游→下游"
                        if sentiment > 0:
                            # 上游涨价→下游成本上升（利空）
                            result["direct_impact"] = "利空"
                            result["direct_reason"] = f"{mp}涨价，下游成本上升"
                            reasoning.append(f"{mp}价格上涨 → 下游{name}成本上升 → 利空")
                        elif sentiment < 0:
                            # 上游跌价→下游成本下降（利好）
                            result["direct_impact"] = "利好"
                            result["direct_reason"] = f"{mp}跌价，下游成本下降"
                            reasoning.append(f"{mp}价格下跌 → 下游{name}成本下降 → 利好")

                    # 公司是竞争对手
                    if mp == name or name in mp:
                        if sentiment > 0:
                            result["direct_impact"] = "利好"
                            result["direct_reason"] = f"{mp}行业景气度上升"
                        elif sentiment < 0:
                            result["direct_impact"] = "利空"
                            result["direct_reason"] = f"{mp}行业景气度下降"

                    # 5. 连带影响（竞争关系）
                    competitors = self.get_competitors(code)
                    for comp in competitors[:5]:
                        comp_name = comp.get("name", comp["code"])
                        if sentiment > 0:
                            chain_reaction = {
                                "target": f"{comp_name}({comp['code']})",
                                "relation": "竞争",
                                "impact": "利好",
                                "reason": f"行业景气度上升，同行业公司{comp_name}受益",
                            }
                        elif sentiment < 0:
                            chain_reaction = {
                                "target": f"{comp_name}({comp['code']})",
                                "relation": "竞争",
                                "impact": "利空",
                                "reason": f"行业景气度下降，同行业公司{comp_name}受损",
                            }
                        else:
                            chain_reaction = {
                                "target": f"{comp_name}({comp['code']})",
                                "relation": "竞争",
                                "impact": "中性",
                                "reason": f"行业影响尚需进一步观察",
                            }
                        result["chain_reactions"].append(chain_reaction)

                    # 6. 产业链传导影响
                    for direction_key in ["上游", "下游"]:
                        for related in chain.get(direction_key, []):
                            # 查找相关股票
                            related_stocks = self._find_stocks_by_product(related)
                            for rs in related_stocks:
                                if rs["code"] == code:
                                    continue
                                if sentiment > 0 and direction_key == "上游":
                                    impact = "利好" if name not in chain.get("上游", []) else "利空"
                                elif sentiment > 0 and direction_key == "下游":
                                    impact = "利好" if name not in chain.get("下游", []) else "利空"
                                elif sentiment < 0 and direction_key == "上游":
                                    impact = "利空" if name not in chain.get("上游", []) else "利好"
                                elif sentiment < 0 and direction_key == "下游":
                                    impact = "利空" if name not in chain.get("下游", []) else "利好"
                                else:
                                    impact = "中性"

                                chain_reaction = {
                                    "target": f"{rs['name']}({rs['code']})",
                                    "relation": f"产业链{direction_key}",
                                    "impact": impact,
                                    "reason": f"通过{related}产业链传导",
                                }
                                result["chain_reactions"].append(chain_reaction)

            if not reasoning:
                if abs(sentiment) > 0.2:
                    direction = "利好" if sentiment > 0 else "利空"
                    result["direct_impact"] = direction
                    result["direct_reason"] = f"新闻情绪{direction}（{sentiment:.2f}）"
                    reasoning.append(f"新闻情绪偏向{direction}")
                else:
                    result["direct_impact"] = "中性"
                    reasoning.append("新闻情绪中性，无明显利好利空")

            result["impact_reasoning"] = reasoning

            # 7. 收集相关股票
            result["related_stocks"] = self.get_related_stocks(news_text)

        except Exception as e:
            logger.warning(f"推理影响失败 ({code}): {e}")

        return result

    def get_related_stocks(self, text: str) -> List[Dict]:
        """
        从新闻文本推理所有受影响股票

        Args:
            text: 新闻文本

        Returns:
            [{"code": "600519", "name": "贵州茅台", "relation": "...", "impact": "利好/利空/中性"}, ...]
        """
        try:
            from analyzer.ner_extractor import extract_stock_mentions, extract_products, extract_industry_sectors

            related = []
            seen_codes: Set[str] = set()

            # 1. 直接提到的股票
            mentions = extract_stock_mentions(text)
            for m in mentions:
                if m["code"] not in seen_codes:
                    seen_codes.add(m["code"])
                    related.append({
                        "code": m["code"],
                        "name": m["name"],
                        "relation": "直接提及",
                        "impact": "待评估",
                    })

            # 2. 通过产品推理
            products = extract_products(text)
            for product in products:
                if product in self.industry_chains:
                    stocks = self._find_stocks_by_product(product)
                    for s in stocks:
                        if s["code"] not in seen_codes:
                            seen_codes.add(s["code"])
                            related.append({
                                "code": s["code"],
                                "name": s["name"],
                                "relation": f"产品关联({product})",
                                "impact": "需进一步分析",
                            })

            # 3. 通过行业板块推理
            sectors = extract_industry_sectors(text)
            for sector in sectors:
                for code, sector_list in self.sector_map.items():
                    if sector in sector_list and code not in seen_codes:
                        seen_codes.add(code)
                        name = self._code_to_name.get(code, "")
                        related.append({
                            "code": code,
                            "name": name,
                            "relation": f"板块关联({sector})",
                            "impact": "需进一步分析",
                        })

            return related[:20]  # 最多20条

        except Exception as e:
            logger.warning(f"获取相关股票失败: {e}")
            return []

    def _find_stocks_by_product(self, product: str) -> List[Dict]:
        """通过产品名查找相关股票"""
        results = []
        seen: Set[str] = set()

        for code, sectors in self.sector_map.items():
            for sector in sectors:
                if product in sector or sector in product:
                    if code not in seen:
                        seen.add(code)
                        name = self._code_to_name.get(code, "")
                        results.append({"code": code, "name": name})
                    break

        # 如果通过板块没找到，通过股票名称关键词匹配
        if not results:
            for code, name in self._code_to_name.items():
                if product in name or name in product:
                    if code not in seen:
                        seen.add(code)
                        results.append({"code": code, "name": name})

        return results


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    kg = FinancialKnowledgeGraph()

    # 测试获取产业链
    print("===== 产业链 =====")
    chain = kg.get_chain("碳酸锂")
    print(f"碳酸锂 上游: {chain.get('上游', [])}")
    print(f"碳酸锂 下游: {chain.get('下游', [])}")

    chain2 = kg.get_chain("宁德时代")
    print(f"\n宁德时代 产业链: {chain2}")

    # 测试竞品
    print("\n===== 竞品 =====")
    comps = kg.get_competitors("600519")
    print(f"贵州茅台 竞品: {[c['name'] for c in comps]}")

    # 测试板块归属
    print("\n===== 板块归属 =====")
    sectors = kg.get_sectors("300750")
    print(f"宁德时代 板块: {sectors}")

    # 测试推理影响
    print("\n===== 影响推理 =====")
    news = "碳酸锂价格暴跌，锂电池成本有望大幅下降"
    impact = kg.infer_impact("300750", news)
    print(f"直接冲击: {impact['direct_impact']} - {impact['direct_reason']}")
    print(f"推理链: {impact['impact_reasoning']}")
    print(f"连锁反应: {len(impact['chain_reactions'])} 条")
    for cr in impact['chain_reactions'][:3]:
        print(f"  → {cr['target']}: {cr['impact']} ({cr['reason']})")
