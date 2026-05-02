# 📈 股票新闻情报收集分析系统

## 项目概述

自动化的股票新闻情报系统，每天定时从多个财经源收集新闻，进行 NLP 分析和情绪分析，最终生成个股操作建议。

**v8.0 核心升级：Multi-Agent 分析系统** — 通过 6 个专业 Agent（新闻/情绪/技术/基本面/多方/空方）对任意A股执行全维度实时分析。非自选股自动按需拉取缺失数据，不再依赖定时采集缓存。

特征：
- 🎯 **任意个股秒分析** — 输入股票代码即可全维度分析，非自选股自动实时采集新闻/K线/股吧情绪
- 🤖 **6 Agent 协作** — 四分析师并行采集 → 多空辩论 → 研究主管综合评级
- 📡 **按需实时采集** — 分析前智能检测缓存，缺失数据自动从新浪/东方财富/问财实时拉取
- 📊 **多模态输出** — 技术指标（均线/RSI/布林带/KDJ）+ 基本面（PE/PB/ROE）+ 新闻情绪 + 多空辩论

---

## 一、系统架构

```
┌──────────────────────────────────────┐
│  🔄 每日定时采集 (09:00/13:00/15:30)  │  ← launchd / cron
│  └─ 新闻 / 行情 / 公告 / 资金 / 板块   │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│  ① 采集层                             │
│  - 东方财富 / 新浪 / 巨潮 / 雪球 / 问财  │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│  ② 数据存储层                          │  ← SQLite
│  - news / stocks / daily_prices / ...  │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│  🆕 ③ Multi-Agent 分析层 (v8.0)       │  ← 按需触发
│  ┌──────────────┐  ┌──────────────┐   │
│  │ Phase 0:     │  │ 📰 新闻分析师  │   │
│  │ 按需实时采集   │  │ 💬 情绪分析师  │   │
│  │              │  │ 📈 技术分析师  │   │
│  │ → 新闻/K线    │  │ 📊 基本面分析师│   │
│  │ → 股吧/问财   │  └──────┬───────┘   │
│  └──────────────┘         │           │
│                     ┌──────▼───────┐   │
│                     │ Phase 2:     │   │
│                     │ 🟢多🔴空辩论 │   │
│                     └──────┬───────┘   │
│                     ┌──────▼───────┐   │
│                     │ Phase 3:     │   │
│                     │ 🎯 研究主管   │   │
│                     │ 综合评级     │   │
│                     └──────────────┘   │
└──────────────┬───────────────────────┘
               │
┌──────────────▼───────────────────────┐
│  ④ 输出层                             │
│  - 综合评级 / 多空辩论 / 风险因素       │
│  - 推送到微信 + QQ + 飞书             │
└──────────────────────────────────────┘
```

---

## 二、技术栈

| 组件 | 技术选型 | 说明 |
|------|---------|------|
| 语言 | Python 3.11+ | 生态丰富，爬虫/分析库齐全 |
| 采集 | `requests` + `BeautifulSoup` / `feedparser` | 轻量爬虫，无需浏览器 |
| 数据库 | SQLite (`sqlite3`) | 零配置，单文件，够用 |
| 分析 | DeepSeek API (LLM) | 文本摘要 + 情绪分析 + 建议生成 |
| 定时 | `launchd` (macOS) 或 `cron` | 系统级定时任务 |
| 推送到微信 | OpenClaw 微信通道 | 分析结果自动推送给你 |

---

## 三、数据库设计 (SQLite)

```sql
-- 股票基础信息
CREATE TABLE stocks (
    code TEXT PRIMARY KEY,        -- 股票代码 e.g. 600519
    name TEXT NOT NULL,           -- 股票名称 e.g. 贵州茅台
    market TEXT DEFAULT 'SH'      -- SH / SZ / BJ
);

-- 新闻原始数据
CREATE TABLE news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    url TEXT UNIQUE,
    source TEXT,                  -- 东财/新浪/雪球
    summary TEXT,
    content TEXT,
    published_at DATETIME,
    collected_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 新闻-股票关联
CREATE TABLE news_stocks (
    news_id INTEGER,
    stock_code TEXT,
    relevance REAL,               -- 关联度 0~1
    sentiment REAL,               -- 情绪值 -1~1
    FOREIGN KEY (news_id) REFERENCES news(id),
    FOREIGN KEY (stock_code) REFERENCES stocks(code)
);

-- 分析结果
CREATE TABLE analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT,
    date DATE,
    news_count INTEGER,
    avg_sentiment REAL,
    key_topics TEXT,              -- JSON 关键主题
    llm_summary TEXT,             -- LLM 生成摘要
    suggestion TEXT,               -- 建议: 买入/持有/观望/卖出
    confidence REAL,              -- 置信度 0~1
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_code) REFERENCES stocks(code)
);
```

---

## 四、模块划分

### 1️⃣ 采集模块 `collector/`
- `scheduler.py` — 定时调度入口，管理所有采集器
- `base.py` — 采集基类（请求重试、代理、UA轮换）
- `spiders/eastmoney.py` — 东方财富（新闻/行情/资金流向/龙虎榜/板块）
- `spiders/sina_finance.py` — 新浪财经（行情/新闻/美股）
- `spiders/xueqiu.py` — 雪球（热榜/讨论/情绪）
- `spiders/cninfo.py` — 巨潮资讯网（公司公告）
- `spiders/policy_collector.py` — 财联社/华尔街见闻（政策快讯）

### 2️⃣ 存储模块 `storage/`
- `database.py` — SQLite 建表+CRUD
- `models.py` — 数据模型

### 3️⃣ 分析模块 `analyzer/`
- `nlp_analyzer.py` — LLM 调用 DeepSeek API
- `sentiment.py` — 情感评分
- `advisor.py` — 综合生成操作建议

### 4️⃣ 输出模块 `output/`
- `report.py` — 生成日报
- `notifier.py` — 推送到微信

### 5️⃣ 数据处理层 `processor/` (v2.0 新增)
- `cleaner.py` — HTML清洗、文本规范化、广告噪音去除
- `deduplicator.py` — SimHash 64位指纹去重，滑动窗口O(n)复杂度
- `extractor.py` — 关键词提取、股票代码检测、新闻分类、实体提取
- `pipeline.py` — 管道编排：clean→dedup→extract 一站式处理

### 6️⃣ 智能分析层 `analyzer/` (v3.0 新增)
- `nlp_analyzer.py` — DeepSeek API 集成，新闻情绪分析、日报生成、交易建议
- `sentiment.py` — 综合情绪评分、趋势分析、异常舆情检测
- `advisor.py` — 交易建议生成器，多维置信度计算
- `stock_analyzer.py` — 个股分析入口，数据整合+LLM分析+建议生成
- `report_generator.py` — 盘前早报/收盘晚报生成
- `config.yaml` — 分析模块配置

### 7️⃣ AI 分析增强层 (v4.0 新增)
- `ner_extractor.py` — 金融NER增强，基于词典+正则提取公司名/人名/产品/行业板块/股票代码
- `knowledge_graph.py` — 轻量金融知识图谱，内置产业链关系/竞品关系/板块归属，推理新闻间接影响
- `impact_model.py` — 情报影响评估模型，综合情感+板块热度+新闻数量+历史波动率计算影响因子
- `advisor.py` (增强版) — 整合知识图谱推理结果，增加逻辑推演链输出
- `stock_analyzer.py` (增强版) — 集成NER+KG+Impact评估全流程，输出推理链

### 8️⃣ 配置
- `config.yaml` — 股票池、采集频率、模型参数、分析配置
- `stocks.csv` — 自选股列表

---

## 五、运行流程

### 定时采集 (每天 9:00 / 13:00 / 15:30)
```
1. 读取自选股列表
2. 遍历各财经源抓取相关新闻
3. 数据清洗（去除HTML标签、广告噪音、规范化文本）
4. SimHash 去重（避免重复存储相似内容）
5. 特征提取（关键词、股票代码、分类、实体）
6. 存入 SQLite（含 FTS5 全文索引）
7. 调用 DeepSeek API 做摘要 + 情绪分析
```

### 定时分析 (每天 16:00 收盘后)
```
1. 读取当天采集的所有新闻
2. 按股票维度汇总情绪评分
3. LLM 生成每只股的综合分析
4. 给出操作建议（买入/持有/观望/卖出）
```

### 推送 (每天 16:30)
```
1. 生成日报文本
2. 通过 OpenClaw 微信通道推送给你
```

### 数据归档 (每天凌晨)
```
1. 自动归档30天前的旧新闻
2. 释放主表空间
3. FTS 索引同步更新
```

---

## 六、自选股票池（初始建议）

| 代码 | 名称 | 关注理由 |
|------|------|---------|
| 600519 | 贵州茅台 | 消费龙头 |
| 000858 | 五粮液 | 白酒 |
| 300750 | 宁德时代 | 新能源 |
| 601318 | 中国平安 | 金融 |
| 000333 | 美的集团 | 家电 |
| 600036 | 招商银行 | 银行 |
| 002594 | 比亚迪 | 新能源车 |
| 688981 | 中芯国际 | 半导体 |

*（可根据你的持仓和偏好调整）*

---

## 七、项目目录结构

```
stockquantmodel/
├── README.md              ← 项目说明（本文件）
├── requirements.txt       ← Python 依赖
├── config.yaml            ← 配置
├── stocks.csv             ← 自选股列表
├── main.py                ← 入口
├── data/
│   └── stock_news.db      ← SQLite 数据库
├── collector/
│   ├── __init__.py
│   ├── scheduler.py       ← 调度器
│   ├── base.py             ← 采集基类
│   └── spiders/
│       ├── __init__.py
│       ├── eastmoney.py    ← 东方财富(新闻/行情/资金/龙虎榜)
│       ├── sina_finance.py ← 新浪财经(行情/新闻/美股)
│       ├── xueqiu.py       ← 雪球(热榜/讨论/情绪)
│       ├── cninfo.py       ← 巨潮资讯(公司公告)
│       └── policy_collector.py ← 财联社/华尔街(政策快讯)
├── processor/              ← (v2.0 新增) 数据处理层
│   ├── __init__.py
│   ├── cleaner.py          ← HTML清洗/文本规范化
│   ├── deduplicator.py     ← SimHash去重
│   ├── extractor.py        ← 关键词/股票代码/分类/实体提取
│   └── pipeline.py         ← 管道编排
├── storage/
│   ├── __init__.py
│   └── database.py         ← SQLite 14表 + FTS5全文搜索
├── analyzer/
│   └── __init__.py
└── output/
    └── __init__.py
```

---

## 数据源全景图

| 数据维度 | 来源 | 采集内容 | 影响股价逻辑 |
|---------|------|---------|-------------|
| 📰 财经新闻 | 东方财富、新浪 | 个股/板块/宏观新闻 | 新闻情绪影响短期波动 |
| 📢 公司公告 | 巨潮资讯网 | 业绩预告/分红/重组/减持 | 公告是股价直接催化剂 |
| 📊 实时行情 | 东方财富、新浪 | 价格/涨跌幅/成交量/换手率 | 技术面分析基础 |
| 💰 资金流向 | 东方财富 | 主力净流入/散户/大单 | 主力资金预示方向 |
| 🐉 龙虎榜 | 东方财富 | 涨停分析/游资动向 | 游资偏好反映热点 |
| 📋 板块排行 | 东方财富 | 行业板块涨跌/领涨股 | 板块效应联动个股 |
| 💬 社交媒体 | 雪球 | 个股讨论/热榜/情绪 | 散户情绪是反向指标 |
| 🏛️ 政策快讯 | 财联社/华尔街 | 宏观/行业政策/监管 | 政策改变行业预期 |
| 📈 宏观数据 | 国家统计局 | CPI/PPI/PMI/GDP (待接入) | 宏观定方向 |
| 💹 国际市场 | 新浪 | 美股三大指数 | 外盘情绪影响A股 |

---

---

## 八、数据处理层说明（v2.0）

### 清洗模块 `processor/cleaner.py`

| 组件 | 功能 |
|------|------|
| `HTMLCleaner.strip_tags()` | 去除HTML标签、script、style、注释 |
| `HTMLCleaner.remove_ad_noise()` | 去广告/免责声明/二维码/分享等噪音 |
| `HTMLCleaner.decode_html_entities()` | 解码 `&amp;`、`&#39;` 等实体 |
| `TextNormalizer` | 全半角转换、空白折叠、标点规范化 |
| `clean_text(text)` | 一站式清洗（实体解码→去标签→去噪音→规范化） |
| `extract_clean_content(html)` | 从HTML提取干净正文（利用BeautifulSoup定位正文容器） |

### 去重模块 `processor/deduplicator.py`

- **SimHash 64位指纹**：基于4-gram + TF权重，纯Python实现
- `hamming_distance(hash1, hash2)`：计算汉明距离
- `dedup(news_list, threshold=3)`：滑动窗口去重（O(n)复杂度）
  - `threshold=3`：允许最多3bit差异视为重复
  - `window_size=100`：滑动窗口控制内存
- 文本太短时自动回退到精确字符串比较

### 提取模块 `processor/extractor.py`

| 函数 | 功能 |
|------|------|
| `extract_keywords(text, top=10)` | 基于TF的关键词提取（2-gram + 4-gram） |
| `detect_stock_codes(text)` | 检测沪深京股票代码（6位数字） |
| `categorize_news(title, content)` | 分类：业绩/重组/政策/行业/市场/其他 |
| `extract_entities(text)` | 提取公司名/人名/金额/百分比 |

### 管道模块 `processor/pipeline.py`

```python
# 单篇处理
result = process_article(raw_article)
# result 新增字段: clean_title, clean_content, keywords, category,
#                   stock_codes, entities, content_hash, processed

# 批量处理（自动去重）
results = process_batch(raw_list)
```

### 数据库升级 `storage/database.py` (v2.0)

**新增功能：**
- **FTS5 全文搜索**：`create_fts_index()` + `search_news(keyword, limit=20)`
- **内容去重**：`get_by_content_hash(content_hash)`，`news` 和 `announcements` 表新增 `content_hash` 列
- **字段扩展**：`news` 表新增 `category`、`keywords`、`processed`；`announcements` 表新增 `category`、`content_hash`
- **数据归档**：`archive_old_data(days=30)` → 旧数据移至 `news_archive` / `announcements_archive` 备份表
- **数据库统计**：`get_stats()` → 返回表大小、分类分布、来源分布、处理进度等完整统计
- **增量迁移**：`_migrate_schema()` 自动为新版字段做 ALTER TABLE

**使用示例：**
```python
from storage.database import Database
from processor.pipeline import process_batch

db = Database("data/stock_news.db")

# 创建FTS索引
db.create_fts_index()

# 处理并存储
processed = process_batch(news_list)
for article in processed:
    if not db.get_by_content_hash(article["content_hash"]):
        db.insert_news(
            title=article["clean_title"],
            url=article["url"],
            source=article["source"],
            content=article["clean_content"],
            category=article["category"],
            keywords=",".join(article["keywords"]),
            content_hash=article["content_hash"]
        )

# 搜索
hits = db.search_news("贵州茅台")

# 查看统计
stats = db.get_stats()
print(f"数据库大小: {stats['db_size_mb']}MB")
print(f"新闻总数: {stats['total_news']}")
print(f"分类分布: {stats['categories']}")

# 归档30天前数据
result = db.archive_old_data(days=30)
print(f"已归档: {result}")
```

### 技术要求
- 所有处理函数含 try/except，不中断流程
- SimHash 纯 Python 实现，无外部依赖
- 低内存占用，适合 2GB 服务器
- utf-8 编码，正确处理中文
- 每个模块可独立测试

---

## 九、智能分析层说明（v3.0）

### 分析流程

```
┌───────────────┐
│  读取数据      │  ← 从数据库读取新闻/公告/行情/资金
└───────┬───────┘
        │
┌───────▼───────┐
│  情绪分析      │  ← SentimentAnalyzer (规则+统计)
│  - 文本情绪评分  │
│  - 综合多维情绪  │
│  - 异常检测     │
└───────┬───────┘
        │
┌───────▼───────┐
│  NLP 分析      │  ← NLPAnalyzer (DeepSeek API)
│  - 新闻摘要     │
│  - 情绪评分     │
│  - 关键事件提取  │
└───────┬───────┘
        │
┌───────▼───────┐
│  建议生成      │  ← TradingAdvisor
│  - 置信度计算   │
│  - 建议等级     │
│  - 风险评估    │
└───────┬───────┘
        │
┌───────▼───────┐
│  结果入库      │  → analysis 表
│  生成日报      │  → ReportGenerator (Markdown)
└───────────────┘
```

### 模块说明

#### `analyzer/nlp_analyzer.py` — DeepSeek API 集成

| 方法 | 功能 |
|------|------|
| `analyze_news(news_list)` | 对一批新闻做摘要+情绪分析，返回结构化JSON |
| `generate_report(stock_results)` | 生成日报文本（Markdown） |
| `get_trading_advice(sentiment, market)` | 基于多维数据生成交易建议 |

**特性：**
- API Key 从 `DEEPSEEK_API_KEY` 环境变量读取
- 自动重试3次，超时30秒
- 专业的 prompt engineering，中文输出
- LLM 不可用时自动降级到规则兜底

#### `analyzer/sentiment.py` — 综合情绪分析

| 方法 | 功能 |
|------|------|
| `calculate_sentiment_score(news)` | 综合多维度情绪评分（来源权重+时效权重） |
| `get_sentiment_trend(news_by_day)` | 情绪趋势分析 |
| `detect_anomaly(news_list)` | 检测异常舆情（突发利空/利好） |

**情绪词典：** 内置 A 股常用正面/负面词库，含增强词和减弱词处理

#### `analyzer/advisor.py` — 交易建议生成器

| 方法 | 功能 |
|------|------|
| `generate_advice(stock_analysis)` | 生成个股买卖建议 |
| `calculate_confidence(sentiment, count, market)` | 多维置信度计算 |

**建议等级：** 强烈买入 > 买入 > 持有 > 观望 > 卖出 > 强烈卖出
**风险等级：** 高 / 中 / 低

#### `analyzer/stock_analyzer.py` — 个股分析入口

| 方法 | 功能 |
|------|------|
| `analyze_stock(code, days)` | 对单只股票完整分析 |
| `analyze_all_stocks()` | 分析所有自选股 |

**流程：** 读取数据 → 情绪分析 → LLM 分析 → 生成建议 → 写入 analysis 表

#### `analyzer/report_generator.py` — 日报生成

| 方法 | 功能 |
|------|------|
| `generate_morning_report()` | 盘前早报（08:30） |
| `generate_closing_report()` | 收盘晚报（16:00） |

**格式：** Markdown，含市场判断、个股分析、风险提示、操作建议
**交易日判断：** 自动跳过周末和法定节假日

### 使用方式

```bash
# 环境变量设置
# 在 .zshrc 或运行前设置
export DEEPSEEK_API_KEY="your_api_key_here"

# 分析所有自选股
python main.py analyze

# 分析指定股票
python main.py analyze 600519
python main.py analyze 600519 3  # 分析近3天数据

# 生成收盘晚报
python main.py report

# 生成盘前早报
python main.py report morning
```

### 数据库升级 `storage/database.py` (v3.0)

**新增方法：**
- `get_latest_market_snapshot(code)` — 获取最新行情
- `get_latest_money_flow(code)` — 获取最新资金流向
- `save_analysis(code, data)` — 保存/更新分析结果（upsert）
- `get_today_analysis()` — 获取今日所有分析结果
- `get_recent_analysis(code, limit)` — 获取历史分析记录

**analysis 表现有字段：**
- `stock_code`, `date`, `news_count`, `avg_sentiment`, `sentiment_std`
- `key_topics` (JSON), `llm_analysis`, `suggestion`
- `confidence` (0~1), `risk_level` (high/medium/low)

---

## 项目状态

✅ 采集模块已完成（多数据源全面采集）
✅ 数据处理层已完成（清洗/去重/提取/管道编排）v2.0
✅ 分析模块已完成（NLP分析/情绪分析/交易建议/日报生成）v3.0
⏳ 推送模块（待开发）

---

## 十、AI 分析增强层说明（v4.0）

### 增强分析流程

```
┌─────────────────┐
│  读取数据         │  ← 从数据库读取新闻/公告/行情/资金
└────────┬────────┘
         │
┌────────▼────────┐
│  NER 实体提取    │  ← ner_extractor.py (词典+正则)
│  - 公司名识别    │
│  - 产品/商品识别  │
│  - 行业板块识别   │
│  - 股票代码识别   │
└────────┬────────┘
         │
┌────────▼────────┐
│  知识图谱推理     │  ← knowledge_graph.py (JSON图谱)
│  - 产业链上下游   │
│  - 竞品关系      │
│  - 间接影响推理   │
│  - 连锁反应分析  │
└────────┬────────┘
         │
┌────────▼────────┐
│  情绪分析         │  ← SentimentAnalyzer (规则+统计)
│  + NLP 分析      │  ← NLPAnalyzer (DeepSeek API)
└────────┬────────┘
         │
┌────────▼────────┐
│  影响评估模型     │  ← impact_model.py
│  - 多维度评分    │
│  - 等级判定      │
│  - 历史对比      │
└────────┬────────┘
         │
┌────────▼────────┐
│  增强建议生成     │  ← advisor.py (增强版)
│  - 推理链输出    │
│  - 关键因子提取   │
│  - 风险评估      │
│  结果入库+日报    │
└─────────────────┘
```

### 模块说明

#### `analyzer/ner_extractor.py` — 金融 NER 增强（v4.0 新增）

基于词典匹配 + 正则的命名实体识别，不依赖 NLP 库。

| 函数 | 功能 |
|------|------|
| `extract_company_names(text)` | 识别公司名（A股上市公司名称词典 + 模式匹配） |
| `extract_people(text)` | 识别人名（高管/分析师/政要） |
| `extract_products(text)` | 识别产品名（锂、碳酸锂、芯片、光伏等） |
| `extract_industry_sectors(text)` | 识别行业板块（30+行业板块关键词） |
| `extract_stock_mentions(text)` | 从文本中识别股票代码和名称 |
| `extract_financial_entities(text)` | 综合金融实体提取（一次性提取所有类型） |

**内置数据：** A 股前 100 常见股票代码和名称、30+ 行业板块关键词映射、20+ 产品/商品名词典

#### `analyzer/knowledge_graph.py` — 金融知识图谱（v4.0 新增）

轻量级知识图谱，JSON 实现，无需图数据库。

| 方法 | 功能 |
|------|------|
| `get_chain(code_or_name)` | 获取产业链上下游关系 |
| `get_competitors(code_or_name)` | 获取同行业竞品公司 |
| `get_sectors(code_or_name)` | 获取股票所属行业板块 |
| `infer_impact(code, news_text)` | 推理新闻的间接影响（含连锁反应） |
| `get_related_stocks(text)` | 从新闻文本推理所有受影响股票 |

**内置图谱数据：**
- **产业链关系：** 锂电池、光伏、半导体、AI、医药、煤炭等 30+ 行业链，100+ 上下游关系
- **竞品关系：** 白酒、电池、光伏、芯片、银行、保险等 20+ 行业竞争图，50+ 竞品对
- **板块归属：** 100+ 只股票到行业板块的映射

**推理示例：**
- 输入： "碳酸锂价格暴跌" + 股票代码 "300750"（宁德时代）
- 输出： 直接利好（成本下降），连锁反应：锂矿企业利空、电池/新能源车企业利好

#### `analyzer/impact_model.py` — 情报影响评估模型（v4.0 新增）

综合多维度数据计算影响因子。

| 方法 | 功能 |
|------|------|
| `calculate_impact_factor(stock_code, sentiment, news_count, ...)` | 综合评估影响因子 |
| `is_significant(impact_factor)` | 判断影响是否显著 |
| `summarize(impact_factor)` | 生成影响评估摘要 |
| `compare_stocks(analyses)` | 多只股票对比评估 |

**影响等级：** 重大利好（≥0.8）> 利好（≥0.4）> 中性 > 利空（≤-0.4）> 重大利空（≤-0.8）

**评估维度：**
1. 情感维度（40%权重）：情绪得分 + 新闻数量置信度修正
2. 板块热度（20%权重）：行业板块人气热度
3. 新闻数量（15%权重）：关注度因子
4. 历史波动率（10%权重）：高波动环境下情绪影响放大
5. 异常舆情修正：突发利好/利空修正
6. 知识图谱推理修正：产业链传导 + 竞品联动

#### `analyzer/advisor.py` — 增强建议生成器（v4.0 升级）

整合知识图谱推理结果到建议中，增加逻辑推演链输出。

**建议输出格式（增强版）：**
```json
{
  "suggestion": "强烈关注",
  "confidence": 0.85,
  "reasoning": [
    "碳酸锂价格暴跌 → 电池成本降低",
    "公司产能扩张中 → 受益于成本下降",
    "板块热度上升 → 资金关注度高"
  ],
  "risk_level": "低",
  "key_factors": ["成本下降", "产能扩张", "板块热度"],
  "impact_evaluation": { "impact_score": 0.75, "level": "利好" },
  "kg_reasoning": { "direct_impact": "利好", "chain_reactions": [...] }
}
```

**建议等级：** 强烈关注 > 关注 > 持有 > 观望 > 回避 > 强烈回避

#### `analyzer/stock_analyzer.py` — 个股分析入口（v4.0 升级）

在分析流程中集成 NER、知识图谱、影响评估模型，增加推理链输出。

| 方法 | 功能 |
|------|------|
| `analyze_stock(code, days)` | 对单只股票完整分析（含推理链） |
| `analyze_all_stocks()` | 分析所有自选股（增强版） |
| `compare_stocks_impact()` | 多只股票影响对比 |

**分析结果新增字段：**
- `ner_entities` — 提取的金融实体
- `kg_reasoning` — 知识图谱推理结果
- `impact_evaluation` — 影响评估结果
- `reasoning_chain` — 推理链列表
- `key_factors` — 关键影响因子
- `chain_reactions` — 连锁反应列表
- `impact_level` / `impact_score` — 影响等级和分数

### 使用方式

```python
from analyzer.stock_analyzer import StockAnalyzer

analyzer = StockAnalyzer()

# 增强分析一只股票
result = analyzer.analyze_stock("300750", days=3)
print(result["reasoning_chain"])  # 查看推理链
print(result["key_factors"])      # 查看关键因子

# 所有股票对比
comparison = analyzer.compare_stocks_impact()
```

### 技术要求
- 所有模块纯 Python 实现，不依赖外部图数据库
- 知识图谱数据内嵌在代码中（覆盖30+行业链，100+关系）
- NER 使用词典匹配 + 正则，不依赖 NLP 库
- 全 try/except 保护，不中断主流程
- 每个模块可独立测试（`python analyzer/xxx.py`）

---

## 十一、技术指标 + 信号分级 + 动态风控说明（v5.0 新增）

将技术面分析（RSI/均线/布林带/MACD）与基本面新闻情绪深度整合，实现信号分级过滤和动态仓位管理。

### 增强分析流程

```
┌──────────────────────┐
│  读取数据              │  ← 新闻/行情/资金/板块
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│  技术指标计算          │  ← technical.py
│  - RSI(14)            │
│  - 均线(5/10/20/30/60)│
│  - 布林带(20,2)       │
│  - MACD(12,26,9)     │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│  信号分级与过滤        │  ← signal_grader.py
│  - 多因子共振评分      │
│  - S/A/B/C/无效 五级  │
│  - 冲突检测           │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│  动态风控              │  ← risk_control.py
│  - 波动率计算          │
│  - 仓位建议            │
│  - 止损预警            │
│  - 综合风险评级        │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│  impact_model.py 升级  │
│  (集成 technical_score │
│   + signal_level 字段)  │
└──────────────────────┘
```

### 模块说明

#### `analyzer/technical.py` — 技术指标计算（v5.0 新增）

纯 Python 实现的技术指标计算器，从 `market_snapshots` 表获取历史收盘价序列。

| 方法 | 功能 |
|------|------|
| `calculate_rsi(prices, period=14)` | RSI计算（标准法） |
| `calculate_rsi_smoothed(prices, period=14)` | RSI计算（Wilder平滑法） |
| `calculate_ma(prices, period)` | 简单移动均线 SMA |
| `calculate_ema(prices, period)` | 指数移动均线 EMA |
| `calculate_all_ma(prices)` | 多周期均线（MA5/10/20/30/60） |
| `calculate_bollinger(prices, period=20)` | 布林带（上轨/中轨/下轨/带宽/位置） |
| `calculate_macd(prices)` | MACD计算（DIF/DEA/MACD柱） |
| `get_all_indicators(code)` | 从数据库自动获取并计算所有指标 |

**输出示例：**
```python
ti = TechnicalIndicator(db)
result = ti.get_all_indicators("300750")
# {
#   "code": "300750",
#   "price": 245.50,
#   "rsi": 32.15,
#   "rsi_signal": "超卖",
#   "ma": {"ma5": 240.0, "ma10": 238.5, ...},
#   "price_vs_ma": "短期向上突破",
#   "bollinger": {"upper": 260, "middle": 240, "lower": 220, "position": 0.15},
#   "macd": {"dif": 0.5, "dea": 0.2, "macd": 0.6},
#   "macd_signal": "金叉",
# }
```

#### `analyzer/signal_grader.py` — 信号分级系统（v5.0 新增）

基于多因子共振的信号过滤与分级引擎。

| 方法 | 功能 |
|------|------|
| `grade_signal(stock_code, news_sentiment, tech_data, money_flow, sector_heat)` | 综合评估信号等级 |
| `is_signal(grade_result)` | 是否有有效信号 |
| `is_strong_signal(grade_result)` | 是否为强信号（S/A级） |
| `summarize(grade_result)` | 生成信号摘要文本 |

**信号级别定义：**

| 级别 | 条件 | 含义 |
|------|------|------|
| 🔴🔴 S级 | 高分(≥4.0) + 多因子共振 + 方向一致 | 强烈信号，多重因子确认 |
| 🔴 A级 | 高分(≥2.5) + 2个以上活跃因子 | 强信号，主力因子支持 |
| 🟡 B级 | 中等(≥1.0) + 单一因子明确 | 中等信号，可参考 |
| 🟢 C级 | 微弱(≥0.3) | 弱信号，需谨慎 |
| ⚪ 无效 | 无明显信号 | 不构成交易依据 |

**分级因子：**
1. 新闻情绪（30%权重）
2. 技术指标（30%权重）— RSI超买超卖 + 均线排列 + MACD金叉死叉 + 布林带位置
3. 资金流向（25%权重）— 主力净流入比例
4. 板块热度（15%权重）

**冲突检测：**
- 情绪与技术面冲突 → 信号减半
- 情绪与资金流向冲突 → 生成预警
- RSI超买 + 利好情绪 → 追涨风险警告
- MACD死叉 + 利好情绪 → 短期风险警告

#### `analyzer/risk_control.py` — 动态风控（v5.0 新增）

波动率计算、仓位建议、止损预警、综合风险评级。

| 方法 | 功能 |
|------|------|
| `calculate_volatility(code, days=20)` | 年化历史波动率（对数收益率法） |
| `calculate_simple_volatility(code, days=20)` | 简化波动率（涨跌幅绝对值法） |
| `get_position_advice(volatility)` | 基于波动率的仓位建议 |
| `check_stop_loss(code, entry_price)` | 止损检查（含动态止损价） |
| `get_risk_level(code)` | 综合风险评级（0~100分） |
| `suggest_stop_loss_strategy(code, entry_price)` | 止损策略建议（严格/标准/宽松/ATR） |

**仓位建议规则：**

| 波动率 | 仓位 | 风险等级 |
|--------|------|---------|
| < 20% | 正常仓位 (80-100%) | 低 |
| 20-25% | 偏重仓位 (60-80%) | 低 |
| 25-35% | 半仓 (40-60%) | 中 |
| 35-50% | 轻仓 (20-40%) | 高 |
| > 50% | 观望 (0-20%) | 极高 |

**止损策略：**
- 严格止损：0.5倍波动率（短线）
- 标准止损：0.8倍波动率（波段）
- 宽松止损：1.2倍波动率（趋势）
- ATR止损：2倍平均真实波幅（技术交易）

**综合风险评级维度：**
| 维度 | 权重 | 数据来源 |
|------|------|---------|
| 波动率风险 | 40% | 年化波动率 |
| 价格位置风险 | 30% | 价格与均线偏离度 |
| 资金流向风险 | 30% | 主力净流入 |

#### `analyzer/impact_model.py` — v5.0 升级说明

1. **新增 `tech_data` 参数**：接受 `TechnicalIndicator.get_all_indicators()` 输出
2. **新增 `technical_score` 维度**：纳入综合评分（20%权重）
3. **新增 `signal_level` 输出字段**：S/A/B/C/无效 信号分级
4. **权重调整**：情感 (30%) > 技术 (20%) > 板块 (15%) > 数量(10%) > 波动(10%)
5. **`is_significant()` 增强**：同时判断信号级别

**使用示例：**
```python
from analyzer.technical import TechnicalIndicator
from analyzer.signal_grader import SignalGrader
from analyzer.risk_control import RiskController
from analyzer.impact_model import ImpactModel

# 1. 获取技术指标
ti = TechnicalIndicator(db)
tech = ti.get_all_indicators("300750")
print(f"RSI: {tech['rsi']}, MACD: {tech['macd_signal']}")

# 2. 信号分级
grader = SignalGrader(db)
signal = grader.grade_signal(
    stock_code="300750",
    news_sentiment=0.72,
    tech_data=tech,
    money_flow={"main_net": 50000000, "total_amount": 500000000},
    sector_heat=0.75,
)
print(f"信号级别: {signal['level']}")
print(f"理由: {signal['reasons']}")
print(f"警告: {signal['warnings']}")

# 3. 动态风控
rc = RiskController(db)
vol = rc.calculate_volatility("300750")
position = rc.get_position_advice(vol)
print(f"建议仓位: {position['position_level']}")

stop_loss = rc.check_stop_loss("300750", entry_price=245.0)
print(f"止损状态: {stop_loss['status']}")

risk = rc.get_risk_level("300750")
print(f"风险等级: {risk['risk_level']}")

# 4. 升级后的影响评估
model = ImpactModel(db)
impact = model.calculate_impact_factor(
    stock_code="300750",
    sentiment=0.72,
    news_count=15,
    tech_data=tech,
    sector_heat=0.7,
)
print(f"影响分: {impact['impact_score']}, 信号级: {impact['signal_level']}")
```

### 独立测试

```bash
# 测试技术指标
python analyzer/technical.py

# 测试信号分级
python analyzer/signal_grader.py

# 测试动态风控
python analyzer/risk_control.py

# 测试升级后的影响评估
python analyzer/impact_model.py
```

---


## 十二、回测模块说明（v5.0）

### `analyzer/backtest.py` — 策略回测引擎

| 方法 | 功能 |
|------|------|
| `record_signal(code, date, suggestion, confidence, sentiment)` | 记录AI分析建议到回测信号表 |
| `record_today_signals()` | 从当天 analysis 表批量记录信号 |
| `run_backtest(code, start_date, end_date)` | 单只股票回测，返回绩效指标 |
| `run_all_backtest()` | 全量股票回测 |
| `get_performance_summary()` | 绩效汇总 |
| `compare_with_benchmark(code)` | 与沪深300对比 |

**回测规则：**
- 建议=买入/强烈买入 → 开仓
- 建议=卖出/强烈卖出 → 平仓
- 建议=持有/观望 → 维持
- 交易成本：单边万分之三
- 持仓比例：基于信号强度和置信度

**绩效指标：** 总收益率 / 年化收益率 / 最大回撤 / 胜率 / 夏普比率 / 阿尔法

### `dashboard/app.py` — Streamlit 可视化看板

四页面应用：

| 页面 | 功能 |
|------|------|
| 市场总览 | 情绪仪表盘、数据源分布饼图、热点新闻、自选股影响一览 |
| 个股深度分析 | K线(均线/布林带)、情绪趋势图、AI报告、推理链 |
| 回测报告 | 收益曲线、绩效卡片、逐笔交易、基准对比 |
| 系统管理 | 数据库统计、数据源状态、手动触发采集/分析 |

**启动：**
```bash
cd stockquantmodel
streamlit run dashboard/app.py
```

**依赖：** `pip install streamlit plotly pandas`

### 数据库升级 `storage/database.py` (v5.0)

**新增表：**
- `backtest_signals` — 回测信号记录（stock_code, signal_date, suggestion, ...）
- `backtest_results` — 回测结果存储（total_return, sharpe_ratio, ...）

**新增方法：**
- `record_backtest_signal()` / `get_backtest_signals()` / `get_all_backtest_signals()`
- `save_backtest_result()` / `get_backtest_results()`
- `get_price_history()` — 按日聚合行情数据

---

---

## 十三、API 服务 + 事件因子 + 推送模块说明（v6.0 新增）

### 13.1 API 接口层 `api/main.py`

基于 FastAPI 的 RESTful 接口，支持查询分析结果、信号、报告等。

| 接口 | 方法 | 功能 |
|------|------|------|
| `/` | GET | 系统状态 |
| `/health` | GET | 健康检查 |
| `/analyze/{code}` | GET | 个股分析（`?days=N` 指定天数）|
| `/analyze` | GET | 全量分析 |
| `/signals` | GET | 今日信号列表（`?level=S` 指定级别）|
| `/report` | GET | 今日报告（`?type=closing|morning`）|
| `/report/morning` | GET | 盘前早报 |
| `/stocks` | GET | 自选股列表 |
| `/stats` | GET | 数据库统计 |

**启动方式：**
```bash
# 命令行
python main.py api        # 默认端口 8000
python main.py api 8888   # 自定义端口

# 或直接 uvicorn
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

**API 文档：** 启动后访问 `http://localhost:8000/docs` 自动生成 Swagger UI。

### 13.2 事件驱动因子 `analyzer/event_factors.py`

从公司公告中提取事件并计算影响因子：

| 事件类型 | 关键词 | 历史影响 | 置信度 |
|---------|--------|---------|:------:|
| 业绩预增 | 业绩预增/大幅上升 | T+1 +3.2% | 高 |
| 业绩预亏 | 业绩预亏/大幅下降 | T+1 -4.5% | 高 |
| 高管增持 | 增持 | 5日累计+1.8% | 中 |
| 高管减持 | 减持 | 5日累计-2.1% | 中 |
| 股份回购 | 回购 | 3日累计+1.5% | 中 |
| 中标合同 | 中标/重大合同 | T+1 +2.0% | 中 |
| 分红送转 | 分红/送转/10送 | 公告日+1.0% | 低 |
| 立案调查 | 立案/调查 | T+1 -5.0% | 高 |
| 资产重组 | 重组/收购 | T+1 +3.0% | 中 |
| 退市风险 | ST/退市 | T+1 -8.0% | 高 |

**方法：**
- `detect_events(announcements)` — 从公告文本检测事件
- `calculate_event_impact(code, events)` — 计算综合事件影响
- `get_hot_events(days=1)` — 获取近期高影响力事件
- 数据来源：`announcements` 表（title / announce_type / summary / publish_date）

**事件影响计算逻辑：**
1. 遍历公告文本匹配关键词 → 识别事件类型
2. 累加事件影响系数 → 得到总影响
3. 置信度加权修正（高 ×1.0 / 中 ×0.7 / 低 ×0.5）
4. 归一化评分 + 等级判定（重大/显著/一般/轻微）

### 13.3 推送模块 `output/notifier.py`

#### 支持通道

| 通道 | 方法 | 配置 |
|------|------|------|
| 微信（OpenClaw） | `push_wechat()` | stdout print，OpenClaw 自动转发 |
| Telegram Bot | `push_telegram()` | 环境变量 TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID |
| 企业微信机器人 | `push_wechat_work()` | 环境变量 WECHAT_WORK_WEBHOOK |
| 钉钉机器人 | `push_dingtalk()` | 环境变量 DINGTALK_WEBHOOK |

#### 通知模板

| 模板 | 方法 | 用途 |
|------|------|------|
| 盘前早报 | `morning_report_template(data)` | 每日 08:30 推送 |
| 收盘晚报 | `closing_report_template(data)` | 每日 16:00 推送 |
| 信号告警 | `signal_alert_template(signal)` | S/A 级信号实时推送 |
| 风控告警 | `risk_alert_template(risk)` | 高风险预警实时推送 |

#### 推送触发

```python
from output.notifier import Notifier

# 自动分发到多个通道
Notifier.push_report(report_text, channels=["wechat", "telegram"])

# 或手动推送
Notifier.push_wechat(markdown_text)
```

**命令触发：**
```bash
python main.py notify           # 推送收盘晚报
python main.py notify morning   # 推送盘前早报
python main.py notify signals   # 推送今日 S/A 级信号
```

Report 命令自动推送：`python main.py report` 或 `python main.py report morning` 生成报告后自动调用 notifier 推送到微信。

### 13.4 目录结构（v6.0 完整版）

```
stockquantmodel/
├── api/
│   ├── __init__.py
│   └── main.py              ← FastAPI 接口层（v6.0 新增）
├── analyzer/
│   ├── event_factors.py     ← 事件驱动因子（v6.0 新增）
│   ├── stock_analyzer.py
│   ├── report_generator.py
│   ├── signal_grader.py
│   ├── ... (其他分析模块)
├── output/
│   ├── __init__.py
│   └── notifier.py          ← 多渠道推送模块（v6.0 新增）
├── main.py                  ← 入口（新增 api / notify 命令）
└── requirements.txt         ← 新增 fastapi / uvicorn
```

---

## 十四、Multi-Agent 分析系统（v8.0 新增）

从 v8.0 起引入多 Agent 协作架构：任意个股分析时，系统自动判断数据库缓存，缺失数据实时从新浪/东方财富/问财拉取，然后由 6 个专业 Agent 协作完成全维度分析。

### 分析流程

```
Phase 0: 按需实时采集
  ├→ 新闻: 东方财富个股页 → 问财补充 (12h内≥3条跳过)
  ├→ K线: 新浪 Scale=240 API (120天≥20条跳过)
  └→ 股吧: 东方财富 API → HTML降级 (当天已采跳过)
          ↓
Phase 1: 四分析师并行
  ├→ 📰 NewsAnalyst     — 新闻情绪 + 关键事件
  ├→ 💬 SentimentAnalyst — 股吧情绪 + 市场情绪
  ├→ 📈 TechnicalAnalyst — 均线/RSI/布林带/KDJ
  └→ 📊 FundamentalsAnalyst — PE/PB/ROE/市值 (问财实时)
          ↓
Phase 2: 多空辩论
  ├→ 🟢 BullResearcher   — 寻找利好、上涨逻辑
  └→ 🔴 BearResearcher   — 识别风险、利空因素
          ↓
Phase 3: 🎯 ResearchManager — 加权汇总 → 最终评级
```

### 使用方式

```bash
cd ~/.openclaw/workspace/stockquantmodel && source ~/.zshrc
python3 -c "
from storage.database import Database
from analyzer.agents.orchestrator import MultiAgentOrchestrator
db = Database('data/stock_news.db')
result = MultiAgentOrchestrator(db=db).analyze('300428', '立中集团')
print(f'评级: {result[\"rating\"]}, 情绪: {result[\"sentiment\"]:+.2f}')
"
```

###### 关键代码文件

| 文件 | 职责 |
|------|------|
| `analyzer/agents/orchestrator.py` | 编排器：Phase 0~3 全流程控制 + 按需实时采集 |
| `analyzer/agents/news_analyst.py` | 新闻分析师：LLM 分析新闻情绪 |
| `analyzer/agents/technical_analyst.py` | 技术分析师：TechnicalIndicator 计算指标 |
| `analyzer/agents/fundamentals_analyst.py` | 基本面分析师：通过问财CLI获取 PE/PB/ROE |
| `analyzer/agents/sentiment_analyst.py` | 情绪分析师：股吧情绪 + 市场情绪 |
| `analyzer/agents/bull_researcher.py` | 多方研究员：寻找利好信号 |
| `analyzer/agents/bear_researcher.py` | 空方研究员：识别风险 |
| `analyzer/agents/research_manager.py` | 研究主管：加权综合评级 |

### 按需采集策略

| 数据 | 源 | 缓存过期 |
|------|------|---------|
| 📰 新闻 | 东方财富个股页 → 问财API | 12h内≥3条跳过 |
| 📈 K线 | 新浪 scale=240 (日K) | 120天≥20条跳过 |
| 💬 股吧 | 东方财富API → HTML降级 | 当天已采集跳过 |
| 📊 基本面 | 问财CLI | 每次实时 |

非自选股自动注册到 `stocks` 表。

### 日志

```
Phase 0: 按需实时采集...     → 检查缓存 + 实时拉取
Phase 1: 分析师分析...       → 四分析师并行
Phase 2: 多空辩论...         → Bull + Bear
Phase 3: 综合评级...         → ResearchManager
[按需采集] 新闻(...): 10条    → 实时采集结果
```

### 输出示例

```json
{
  "rating": "增持",
  "sentiment": 0.167,
  "confidence": 0.75,
  "summary": "综合评级: **增持** (📰=+0.54, 💬=+0.00, 📈=+0.40, 📊=-0.15)",
  "data_source": "news(实时10条) + kline(实时500条) + guba(缓存)"
}
```

---

## 十五、跨源情报交叉验证（v7.0 新增）

从社区/非官方来源（雪球、股吧）获取的情报 → 去官方公告库检索确认 → 打可信度标签。解决社区消息真假难辨的问题，过滤传闻噪音，提升分析质量。

### 核心问题

| 问题 | 来源 | 后果 |
|------|------|------|
| 雪球/股吧传闻真假难辨 | 社区用户 | 错误信息影响判断 |
| 情绪容易被操纵 | 虚假消息 | 情绪分析失真 |
| 利好利空消息需要官方确认 | 舆论 | 无法判断可信度 |
| 同类新闻可能不同结论 | 多渠道 | 结论矛盾 |

### 交叉验证流程

```
┌──────────────────────────┐
│  社区/非官方新闻（输入）    │  ← 雪球、股吧、社交媒体
└──────────┬───────────────┘
           │
┌──────────▼───────────────┐
│  关键词提取               │
│  - 股票代码               │
│  - 公司名                 │
│  - 事件关键词（业绩/减持/回购等） │
└──────────┬───────────────┘
           │
┌──────────▼───────────────┐
│  交叉检索                  │
│  ┌─────────────────┐     │
│  │ announcements 表  │ ← 交易所官方公告
│  │ policies 表       │ ← 央行/发改委/工信部政策
│  │ news 表            │ ← 东方财富/财联社等权威媒体
│  └─────────────────┘     │
└──────────┬───────────────┘
           │
┌──────────▼───────────────┐
│  可信度评分                │
│  - 有官方公告确认 → 高置信度 │
│  - 有政策匹配 → 中高置信度   │
│  - 有权威媒体报道 → 中等置信度 │
│  - 无任何验证 → 低置信度     │
│  - 互相矛盾 → 存疑         │
└──────────┬───────────────┘
           │
┌──────────▼───────────────┐
│  打标签 + 降低情绪权重       │
│  ✅ 确认   ⚠️ 已核实         │
│  📋 待核实  ❓ 传闻          │
│  🚫 存疑                     │
└──────────────────────────────┘
```

### `analyzer/cross_validate.py` — 跨源验证引擎

| 方法 | 功能 |
|------|------|
| `verify_news(news_item)` | 单条新闻交叉验证，返回可信度结果 |
| `tag_credibility(source, verified, confidence)` | 打可信度标签（含 emoji） |
| `batch_verify(news_list)` | 批量验证多条新闻 |
| `get_unverified_alerts(days=1)` | 获取近期未经验证的传闻 |
| `save_verification(news_id, result)` | 验证结果写入数据库 |

### 可信度标签体系

| 标签 | 含义 | 来源类型 | 置信度 |
|------|------|---------|:------:|
| ✅ 确认 | 官方来源直接确认 | 巨潮/上交所/深交所/央行/发改委 | 1.0 |
| ⚠️ 已核实 | 权威媒体验证通过 | 东方财富/财联社/华尔街见闻 | ≥0.8 |
| 📋 待核实 | 权威媒体未找到但逻辑合理 | 多方来源 | 0.4~0.7 |
| ❓ 传闻 | 社区来源无官方背书 | 雪球/股吧/微博 | <0.4 |
| 🚫 存疑 | 明显虚假或负面异常 | 任何来源 | <0.2 |

### 置信度计算规则

| 条件 | 置信度 |
|------|:------:|
| 官方来源 | 1.0 |
| 有官方公告验证 + 权威报道 | 0.85 |
| 有官方公告验证 | 0.75 |
| 政策匹配 + 权威报道 | 0.80 |
| 仅有政策匹配 | 0.70 |
| 仅有权威媒体报道 | 0.65 |
| 权威媒体自身，无其他验证 | 0.55 |
| 社区来源 + 股票代码 + 事件关键词 | 0.30 |
| 社区来源 + 仅有股票代码 | 0.25 |
| 社区来源，无任何验证 | 0.20 |

### 验证结果格式

```python
{
    "verified": True,          # 是否被官方/权威证实
    "confidence": 0.75,        # 置信度 0~1
    "tag": "⚠️ 已核实",        # 可信度标签（含 emoji）
    "evidence": [              # 验证依据列表
        {
            "type": "announcement",  # announcement / policy / news
            "title": "贵州茅台2024年业绩预告",
            "source": "交易所公告",
            "publish_date": "2025-01-20",
        },
        ...
    ],
    "conflicting": False,     # 是否存在矛盾信息
    "recommendation": "可优先采用，结合权威来源确认",
}
```

### `analyzer/stock_analyzer.py` — v7.0 升级

在分析流程中新增：

1. **调用 `cross_validate.verify_news()`** — 每轮分析前对新闻做交叉验证
2. **分析报告中增加 `info_credibility` 字段** — 展示"信息可信度"统计
3. **情绪权重修正** — 低可信度新闻降低其情绪权重

```python
# 情绪修正公式
credibility_weight = 0.5 + 0.5 * (已验证新闻数 / 总新闻数)
修正后情绪 = 原始情绪 × credibility_weight
```

**分析结果新增字段：**
- `credibility` — 完整可信度信息
- `info_credibility` — 概览统计数据
  - `verified_news_count` — 已验证新闻数
  - `total_news` — 总新闻数
  - `verified_ratio` — 验证通过比例
  - `confidence_weight` — 情绪修正权重

### `storage/database.py` — v7.0 数据库迁移

news 表新增字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `credibility_tag` | TEXT DEFAULT '' | 可信度标签（含 emoji） |
| `verified` | BOOLEAN DEFAULT 0 | 是否已验证通过 |
| `evidence` | TEXT | 验证依据（JSON 数组） |

```python
# 增量迁移（自动执行）
def _migrate_credibility(self):
    try:
        conn = self._connect()
        conn.executescript("""
            ALTER TABLE news ADD COLUMN credibility_tag TEXT DEFAULT '';
            ALTER TABLE news ADD COLUMN verified BOOLEAN DEFAULT 0;
            ALTER TABLE news ADD COLUMN evidence TEXT;
        """)
        conn.commit()
    except:
        pass  # 字段可能已存在
    finally:
        conn.close()
```

### 使用方式

```bash
# 验证最近 50 条新闻
python main.py verify

# 验证最近 100 条新闻
python main.py verify 100

# 验证并保存结果到数据库
python main.py verify 100 --save
```

**输出示例：**
```
🔍 开始交叉验证最近 50 条新闻...
📊 共 50 条新闻
  ✅ 贵州茅台2024年净利润同比增长15%... [雪球    ] 置信度=0.85 分类=⚠️ 已核实 证据=2条
  ✅ 央行宣布降准50个基点...         [财联社  ] 置信度=1.00 分类=✅ 确认 证据=1条
  ❓ 某游资大佬称下周A股暴跌...       [股吧    ] 置信度=0.20 分类=❓ 传闻 证据=0条

📊 交叉验证总结（共 50 条）：
  ✅ 确认: 5 条 (10.0%)
  ⚠️ 已核实: 12 条 (24.0%)
  📋 待核实: 15 条 (30.0%)
  ❓ 传闻: 16 条 (32.0%)
  🚫 存疑: 2 条 (4.0%)

  ✅ 已验证: 17/50 (34.0%)
  ❌ 传闻/存疑: 18/50
  📄 总证据引用: 45 条

⚠️ 发现 16 条未经验证的传闻，建议关注
```

### 集成到分析流程

```python
from analyzer.stock_analyzer import StockAnalyzer

analyzer = StockAnalyzer()

# 分析时会自动交叉验证每条新闻
result = analyzer.analyze_stock("300750", days=3)

# 查看信息可信度
print(result["info_credibility"])
# {'verified_news_count': 8, 'total_news': 12,
#  'verified_ratio': 0.6667, 'confidence_weight': 0.8333}
```

### 独立测试

```bash
python analyzer/cross_validate.py
```

### 技术要求

- 所有数据库查询含 try/except，不中断流程
- 纯 Python 实现，无外部依赖
- 全异步友好（可同步使用）
- 内存占用小，适合 2GB 服务器

---

## 项目状态

✅ 采集模块已完成（多数据源全面采集）
✅ 数据处理层已完成（清洗/去重/提取/管道编排）v2.0
✅ 分析模块已完成（NLP分析/情绪分析/交易建议/日报生成）v3.0
✅ AI 分析增强层已完成（NER/知识图谱/影响评估/推理链）v4.0
✅ 回测模块已完成（回测引擎 + Streamlit可视化看板）v5.0
✅ 技术指标 + 信号分级 + 动态风控已完成（技术面融合）v5.0
✅ API 接口层 + 事件因子 + 推送模块已完成 v6.0
✅ 跨源情报交叉验证已完成（过滤传闻噪音）v7.0
✅ Multi-Agent 分析系统已完成（按需实时采集 + 6 Agent 全维分析）v8.0
✅ 个股分析技能已完成（OpenClaw 技能注册，任意个股自动触发）v8.0
