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
- `spiders/eastmoney.py` — 东方财富新闻
- `spiders/sina.py` — 新浪财经
- `spiders/xueqiu.py` — 雪球
- `scheduler.py` — 定时调度入口

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
│   ├── scheduler.py
│   ├── base.py
│   └── spiders/
│       ├── __init__.py
│       ├── eastmoney.py
│       ├── sina.py
│       └── xueqiu.py
├── storage/
│   ├── __init__.py
│   ├── database.py
│   └── models.py
├── analyzer/
│   ├── __init__.py
│   ├── nlp_analyzer.py
│   ├── sentiment.py
│   └── advisor.py
└── output/
    ├── __init__.py
    ├── report.py
    └── notifier.py
```

---

## 八、后续可扩展

- [ ] Web 界面展示（Flask/Streamlit）
- [ ] 接入更多数据源（公告/研报/龙虎榜）
- [ ] K线技术指标结合
- [ ] 历史回测
- [ ] 多账户推送

---

> **项目状态** 📌 方案设计阶段
> **开始开发**？跟我说一声，从采集模块开始写！
