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

### 5️⃣ 配置
- `config.yaml` — 股票池、采集频率、模型参数
- `stocks.csv` — 自选股列表

---

## 五、运行流程

### 定时采集 (每天 9:00 / 13:00 / 15:30)
```
1. 读取自选股列表
2. 遍历各财经源抓取相关新闻
3. 去重、清洗、存入 SQLite
4. 调用 DeepSeek API 做摘要 + 情绪分析
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
├── storage/
│   ├── __init__.py
│   └── database.py         ← SQLite 12表
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

## 项目状态

✅ 采集模块已完成（多数据源全面采集）
⏳ 分析模块（待开发）
⏳ 推送模块（待开发）

> 下一步：开始开发 DeepSeek API 智能分析模块？
