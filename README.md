# 📈 股票新闻情报收集分析系统

## 项目概述

自动化的股票新闻情报系统，每天定时从多个财经源收集新闻，进行 NLP 分析和情绪分析，最终生成个股操作建议。

---

## 一、系统架构

```
┌──────────────────┐
│   🔄 每日定时触发  │  ← cron / launchd
└──────┬───────────┘
       │
┌──────▼───────────┐
│  ① 新闻采集层      │  ← 爬虫 / API
│  - 东方财富        │
│  - 新浪财经        │
│  - 雪球           │
│  - 同花顺         │
└──────┬───────────┘
       │
┌──────▼───────────┐
│  ② 数据存储层      │  ← SQLite
│  - 新闻原始表      │
│  - 股票基础信息表   │
│  - 分析结果表      │
│  - 操作建议表      │
└──────┬───────────┘
       │
┌──────▼───────────┐
│  ③ 智能分析层      │  ← DeepSeek API
│  - 新闻摘要        │
│  - 情感评分        │
│  - 相关股票关联     │
│  - 影响程度评估     │
└──────┬───────────┘
       │
┌──────▼───────────┐
│  ④ 输出层         │
│  - 每日早报        │
│  - 个股操作建议     │
│  - 风险预警        │
│  - 推送到微信       │
└──────────────────┘
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

### 7️⃣ 配置
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
