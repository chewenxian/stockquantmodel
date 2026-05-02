"""
DeepSeek API 集成模块
提供新闻分析、日报生成、交易建议等 LLM 能力

依赖:
- requests (>=2.31.0)
- 环境变量 DEEPSEEK_API_KEY
"""
import os
import json
import time
import logging
from typing import Dict, List, Optional, Any

import requests
import yaml

logger = logging.getLogger(__name__)


class NLPAnalyzer:
    """
    NLP 分析器：通过 DeepSeek API 进行新闻分析、日报生成、交易建议
    """

    def __init__(self, config_path: str = None):
        # 加载配置
        self.config = self._load_config(config_path)

        provider_cfg = self.config.get("provider", "deepseek")
        self.api_base = self.config.get("api_base", "https://api.deepseek.com")
        self.model = self.config.get("model", "deepseek-chat")

        # API key 从环境变量读取
        env_key_map = {
            "deepseek": "DEEPSEEK_API_KEY",
            "openai": "OPENAI_API_KEY",
        }
        key_name = env_key_map.get(provider_cfg, "DEEPSEEK_API_KEY")
        self.api_key = os.environ.get(key_name, "")

        if not self.api_key:
            logger.warning(f"⚠️ API key 未设置，请在环境变量中配置 {key_name}")

        # LLM 参数
        llm_cfg = self.config.get("llm", {})
        self.max_tokens = llm_cfg.get("max_tokens", 2048)
        self.temperature = llm_cfg.get("temperature", 0.3)
        self.timeout = llm_cfg.get("timeout", 30)
        self.retry_times = llm_cfg.get("retry_times", 3)
        self.retry_delay = llm_cfg.get("retry_delay", 2)

        logger.info(f"NLPAnalyzer 初始化: provider={provider_cfg}, model={self.model}, api_base={self.api_base}")

        # 请求会话
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

    def _load_config(self, config_path: Optional[str] = None) -> dict:
        """加载 analyzer 配置"""
        paths_to_try = [
            config_path,
            os.path.join(os.path.dirname(__file__), "config.yaml"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "analyzer", "config.yaml"),
            "analyzer/config.yaml",
        ]
        for path in paths_to_try:
            if path and os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        return yaml.safe_load(f)
                except Exception as e:
                    logger.warning(f"加载配置失败 {path}: {e}")
        logger.warning("未找到 analyzer 配置文件，使用默认配置")
        return {
            "provider": "deepseek",
            "model": "deepseek-chat",
            "api_base": "https://api.deepseek.com",
            "llm": {"max_tokens": 2048, "temperature": 0.3, "timeout": 30,
                     "retry_times": 3, "retry_delay": 2},
            "sentiment": {"window_days": 5, "anomaly_threshold": 0.6,
                          "min_news_for_analysis": 1},
            "advice": {"min_confidence": 0.3,
                       "strong_buy_threshold": 0.7, "buy_threshold": 0.3,
                       "sell_threshold": -0.3, "strong_sell_threshold": -0.7},
        }

    def _call_api(self, messages: List[Dict], system_prompt: str = None) -> Optional[str]:
        """
        调用 DeepSeek API (chat/completions)
        自动重试 + 超时处理

        Args:
            messages: 对话消息列表
            system_prompt: 系统提示词（可选）

        Returns:
            模型回复文本，失败返回 None
        """
        if not self.api_key:
            logger.error("API key 未配置，无法调用 LLM")
            return None

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        payload = {
            "model": self.model,
            "messages": full_messages,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stream": False,
        }

        url = f"{self.api_base.rstrip('/')}/v1/chat/completions"

        last_error = None
        for attempt in range(1, self.retry_times + 1):
            try:
                logger.debug(f"LLM 调用第 {attempt} 次尝试...")
                resp = self._session.post(
                    url,
                    json=payload,
                    timeout=self.timeout,
                )

                if resp.status_code == 200:
                    result = resp.json()
                    content = result["choices"][0]["message"]["content"]
                    logger.debug(f"LLM 调用成功，返回 {len(content)} 字符")
                    return content

                elif resp.status_code == 401:
                    logger.error("API key 认证失败，请检查 DEEPSEEK_API_KEY")
                    return None

                elif resp.status_code == 429:
                    # 限流，等待后重试
                    retry_after = int(resp.headers.get("Retry-After", self.retry_delay))
                    logger.warning(f"API 限流，等待 {retry_after}s 后重试")
                    time.sleep(retry_after)
                    continue

                elif resp.status_code >= 500:
                    logger.warning(f"API 服务端错误 ({resp.status_code})，等待重试...")
                    if attempt < self.retry_times:
                        time.sleep(self.retry_delay * attempt)
                    continue

                else:
                    logger.error(f"API 返回异常状态码: {resp.status_code}, body: {resp.text[:200]}")
                    return None

            except requests.Timeout:
                last_error = "API 请求超时"
                logger.warning(f"第 {attempt} 次调用超时 ({self.timeout}s)")
                if attempt < self.retry_times:
                    time.sleep(self.retry_delay * attempt)

            except requests.ConnectionError as e:
                last_error = f"网络连接错误: {e}"
                logger.warning(f"第 {attempt} 次调用连接失败")
                if attempt < self.retry_times:
                    time.sleep(self.retry_delay * attempt)

            except Exception as e:
                last_error = str(e)
                logger.error(f"API 调用异常: {e}", exc_info=True)
                return None

        logger.error(f"LLM 调用失败（已重试 {self.retry_times} 次）: {last_error}")
        return None

    # ──────────────────────────────────────────
    # Prompt 模板
    # ──────────────────────────────────────────

    @property
    def _news_analysis_system_prompt(self) -> str:
        """新闻分析系统提示词（增强版：板块归因 + 影响时效 + 主题聚类）"""
        return """你是一位专业的A股市场分析师，拥有10年以上的行业经验。
你的任务是分析一组财经新闻，输出结构化的多维分析结果。

## 工作要求
1. 只分析事实，不主观臆断
2. 对每条新闻给出客观的情绪评分
3. 标注涉及的具体板块（申万行业分类）
4. 判断影响的时效长短
5. 识别新闻间的主题关联
6. 输出格式严格为 JSON，使用中文

## 评分标准

### 情绪评分 (-1.0 ~ 1.0)
- 正数 = 利好（业绩增长、重大合同、政策利好、行业涨价、中标、回购等）
- 负数 = 利空（业绩亏损、减持、立案调查、行业利空、诉讼、退市风险等）
- 接近0 = 中性（常规公告、行业动态、例行报道）

### 影响程度 (1~5)
- 1 = 轻微影响（常规新闻、行业日常）
- 2 = 一般影响（中小级别合同、人事变动）
- 3 = 中等影响（业绩预增/预亏、中标重大合同）
- 4 = 重大影响（业绩变脸、重组、立案调查、行业政策重大变化）
- 5 = 决定性影响（退市风险、财务造假、控制权变更、行业颠覆性政策）

### 影响时效
- "短期" = 当日或1-2个交易日内反映
- "中期" = 1-4周内逐步消化
- "长期" = 影响一个季度以上

### 板块归属（申万行业分类）
从标准申万一级/二级行业中选取最相关的一个，如：
- 电子/半导体、电力设备/锂电池、食品饮料/白酒
- 医药生物/创新药、计算机/AI、汽车/新能源汽车
- 机械设备、基础化工、有色金属、国防军工、银行、非银金融
- 房地产、建筑装饰、公用事业、交通运输、通信、传媒等

如果涉及多个板块，选取最主要的1-2个。"""

    @property
    def _news_analysis_output_schema(self) -> str:
        """增强版输出格式定义"""
        return """输出格式（严格的 JSON，不要其他文字）：
{
    "summary": "整体分析摘要（50-100字，包含情绪判断和核心逻辑）",
    "avg_sentiment": 0.0,
    "sentiment_label": "偏多/偏空/中性",
    "primary_sectors": ["电子/半导体", "电力设备"],
    "dominant_theme": "业绩驱动/政策利好/行业涨价/减持利空/题材炒作/中性",
    "impact_timing": "短期/中期/长期",
    "key_topics": ["事件1", "事件2"],
    "items": [
        {
            "title": "新闻标题",
            "sentiment": 0.5,
            "impact": 3,
            "sector": "板块名称",
            "timing": "短期",
            "reasoning": "判断理由（20字内）"
        }
    ],
    "risk_warnings": ["风险1"],
    "opportunities": ["机会1"],
    "cross_correlation": "各新闻间关联分析（30字），如：'业绩预告与中标公告相互印证，强化利好信号'"
}"""

    @property
    def _report_system_prompt(self) -> str:
        """日报生成的系统提示词"""
        return """你是一位专业的A股市场分析师，负责生成每日分析报告。
报告需要专业、简洁、有洞察力，风格接近券商晨报。

要求：
1. 先给出整体市场情绪判断（偏多/偏空/中性）
2. 对每只重点股票给出简要分析
3. 指出关键风险和机会
4. 给出明确的交易建议
5. 使用中文，Markdown 格式
6. 报告力求精炼，不要冗长啰嗦"""

    @property
    def _advice_system_prompt(self) -> str:
        """交易建议生成的系统提示词"""
        return """你是一位资深股票交易顾问，基于多维数据给出交易建议。

分析维度：
1. 新闻情绪：近期相关新闻的正负面情绪
2. 市场数据：价格走势、成交量、换手率
3. 资金流向：主力资金、北向资金动向
4. 市场环境：整体市场氛围

## 建议等级
- **重点关注**：利好信号明确，多个维度共振，适合积极关注或建仓
- **谨慎持有**：中性偏多或中性，当前可持有但不宜加仓，等待进一步信号
- **规避**：利空因素明显，风险上升，建议减仓或规避

## 输出格式（严格的 JSON）
{
    "suggestion": "重点关注/谨慎持有/规避",
    "reason": "建议理由（30-50字）",
    "risk_level": "高/中/低",
    "confidence": 0.8,
    "operation": "具体操作建议",
    "key_driver": "核心驱动因素（如：业绩增长/政策利好/行业下行等）",
    "stop_loss": "止损建议",
    "target": "目标位（如适用）"
}"""

    # ──────────────────────────────────────────
    # 核心分析方法
    # ──────────────────────────────────────────

    def analyze_news(self, news_list: List[Dict],
                      stock_code: str = "", stock_name: str = "",
                      historical_context: str = "") -> Dict[str, Any]:
        """
        对一批新闻做摘要 + 情绪分析（增强版，支持历史记忆注入）

        Args:
            news_list: 新闻列表
            stock_code: 股票代码（用于加载历史记忆）
            stock_name: 股票名称
            historical_context: 历史分析记忆文本

        Returns:
            {
                "summary": str,                      # 整体摘要
                "avg_sentiment": float,               # 平均情绪 -1~1
                "sentiment_label": str,               # 偏多/偏空/中性
                "primary_sectors": List[str],         # 涉及板块（申万行业）
                "dominant_theme": str,               # 主导主题类型
                "impact_timing": str,                # 影响时效
                "key_topics": List[str],              # 关键事件/主题
                "items": [                            # 逐条分析
                    {
                        "title": str,
                        "sentiment": float,
                        "impact": int (1-5),
                        "sector": str,               # 本条涉及板块
                        "timing": str,               # 影响时效
                        "reasoning": str
                    }
                ],
                "cross_correlation": str,            # 新闻间关联分析
                "risk_warnings": List[str],           # 风险提示
                "opportunities": List[str]            # 机会提示
            }
        """
        if not news_list:
            logger.warning("新闻列表为空，跳过分析")
            return {
                "summary": "无相关新闻",
                "avg_sentiment": 0.0,
                "sentiment_label": "中性",
                "key_topics": [],
                "items": [],
                "risk_warnings": [],
                "opportunities": []
            }

        # 检查新闻是否包含有效的标题/内容
        valid_news = [n for n in news_list if n.get("title") or n.get("content")]
        if not valid_news:
            logger.warning("新闻列表中的条目均无有效标题/内容")
            news_list = []

        # 构建新闻文本（取标题+摘要，控制token用量）
        news_texts = []
        for i, news in enumerate(news_list):
            title = news.get("title", "").strip()
            summary = news.get("summary", news.get("content", "")).strip()
            source = news.get("source", "未知来源")
            published = news.get("published_at", "")

            # 截断过长内容
            if len(summary) > 300:
                summary = summary[:300] + "..."

            news_texts.append(
                f"[新闻{i+1}]\n"
                f"标题: {title}\n"
                f"来源: {source}\n"
                f"时间: {published}\n"
                f"内容: {summary}\n"
            )

        # 构建历史上下文段
        history_block = ""
        if historical_context:
            history_block = f"""
【历史分析参考】
{historical_context}
注意：以上是历史分析记录，请对比当前新闻判断趋势变化。
"""

        user_prompt = f"""请分析以下 {len(news_list)} 条财经新闻，输出严格 JSON 格式的分析结果。
{history_block}
新闻列表：
---
{chr(10).join(news_texts)}
---

{self._news_analysis_output_schema}"""

        content = self._call_api(
            messages=[{"role": "user", "content": user_prompt}],
            system_prompt=self._news_analysis_system_prompt
        )

        if not content:
            logger.warning("LLM 分析失败，使用规则兜底")
            return self._rule_based_fallback(news_list)

        # 提取 JSON
        try:
            # 尝试直接解析
            result = json.loads(content)
        except json.JSONDecodeError:
            # 尝试从 markdown 代码块中提取
            import re
            json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', content, re.DOTALL)
            if json_match:
                try:
                    result = json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    result = self._rule_based_fallback(news_list)
            else:
                # 尝试找花括号包裹的内容
                brace_match = re.search(r'\{.*\}', content, re.DOTALL)
                if brace_match:
                    try:
                        result = json.loads(brace_match.group(0))
                    except json.JSONDecodeError:
                        result = self._rule_based_fallback(news_list)
                else:
                    result = self._rule_based_fallback(news_list)

        # 确保关键字段存在（含增强版字段）
        result.setdefault("summary", "AI分析完成")
        result.setdefault("avg_sentiment", 0.0)
        result.setdefault("sentiment_label", "中性")
        result.setdefault("primary_sectors", [])
        result.setdefault("dominant_theme", "中性")
        result.setdefault("impact_timing", "短期")
        result.setdefault("key_topics", [])
        result.setdefault("items", [])
        result.setdefault("cross_correlation", "")
        result.setdefault("risk_warnings", [])
        result.setdefault("opportunities", [])

        return result

    def _rule_based_fallback(self, news_list: List[Dict]) -> Dict[str, Any]:
        """基于规则的兜底分析（LLM 不可用时）"""
        items = []
        sentiment_sum = 0.0
        keywords = set()

        # 基础情绪词典
        positive_words = ["增长", "大涨", "突破", "利好", "盈利", "分红", "中标",
                          "合同", "扩产", "创新高", "买入", "增持", "回购", "预增"]
        negative_words = ["下跌", "亏损", "减持", "立案", "处罚", "利空", "风险",
                          "下调", "降级", "违约", "诉讼", "ST", "退市", "预亏", "下滑"]

        for news in news_list:
            title = news.get("title", "")
            content_text = news.get("summary", "") + " " + news.get("content", "")
            text = title + " " + content_text
            text_lower = text.lower()

            score = 0.0
            for word in positive_words:
                if word in text:
                    score += 0.2
            for word in negative_words:
                if word in text:
                    score -= 0.2
            score = max(-1.0, min(1.0, score))

            # 提取关键词（简单取标题前3个词）
            for word in title.split():
                if len(word) >= 2:
                    keywords.add(word)

            items.append({
                "title": title,
                "sentiment": round(score, 2),
                "impact": 2 if abs(score) > 0.3 else 1,
                "sector": "",
                "timing": "短期",
                "reasoning": "基于规则判断" if abs(score) > 0.1 else "无明显情绪"
            })
            sentiment_sum += score

        avg_sentiment = sentiment_sum / len(news_list) if news_list else 0.0

        if avg_sentiment > 0.3:
            label = "偏多"
        elif avg_sentiment < -0.3:
            label = "偏空"
        else:
            label = "中性"

        return {
            "summary": f"分析了{len(news_list)}条相关新闻，整体情绪{label}",
            "avg_sentiment": round(avg_sentiment, 2),
            "sentiment_label": label,
            "primary_sectors": [],
            "dominant_theme": label,
            "impact_timing": "短期",
            "key_topics": list(keywords)[:10],
            "items": items,
            "cross_correlation": "",
            "risk_warnings": ["注意市场风险"] if avg_sentiment < 0 else [],
            "opportunities": ["关注市场机会"] if avg_sentiment > 0 else []
        }

    def generate_report(self, stock_analysis_results: List[Dict]) -> str:
        """
        基于个股分析结果生成日报文本

        Args:
            stock_analysis_results: 个股分析结果列表

        Returns:
            Markdown 格式日报文本
        """
        if not stock_analysis_results:
            return "今日无分析数据。"

        # 构建个股摘要
        stock_summaries = []
        for sa in stock_analysis_results:
            code = sa.get("code", "")
            name = sa.get("name", "")
            sentiment = sa.get("avg_sentiment", 0)
            suggestion = sa.get("suggestion", "持有")
            confidence = sa.get("confidence", 0)

            sentiment_str = "🟢" if sentiment > 0.3 else ("🔴" if sentiment < -0.3 else "⚪")
            stock_summaries.append(
                f"### {name} ({code}) {sentiment_str}\n"
                f"- **情绪评分**: {sentiment:.2f}\n"
                f"- **建议**: {suggestion} (置信度 {confidence:.0%})\n"
                f"- **新闻摘要**: {sa.get('summary', '无')}\n"
                f"- **风险提示**: {sa.get('risk_warnings', '无')}\n"
            )

        user_prompt = f"""请根据以下 {len(stock_analysis_results)} 只股票的分析结果，生成一份专业的每日投资报告。

个股分析数据：
---
{chr(10).join(stock_summaries)}
---

请生成 Markdown 格式的日报，需要包含：
1. 📊 市场情绪总览（今日整体判断）
2. 📰 重点个股分析（每只股的要点和分析逻辑）
3. ⚠️ 风险提示
4. 💡 操作建议

报告风格：专业、简洁、有洞察力，类似券商晨报。"""

        content = self._call_api(
            messages=[{"role": "user", "content": user_prompt}],
            system_prompt=self._report_system_prompt
        )

        if not content:
            return self._generate_report_fallback(stock_analysis_results)

        return content

    def _generate_report_fallback(self, stock_analysis_results: List[Dict]) -> str:
        """日报生成的兜底方案"""
        lines = ["# 📈 每日投资分析报告\n"]
        lines.append(f"**生成时间**: {time.strftime('%Y-%m-%d %H:%M')}\n")
        lines.append("---\n")

        for sa in stock_analysis_results:
            name = sa.get("name", "未知")
            code = sa.get("code", "")
            sentiment = sa.get("avg_sentiment", 0)
            suggestion = sa.get("suggestion", "持有")
            confidence = sa.get("confidence", 0)

            sentiment_icon = "🟢" if sentiment > 0.3 else ("🔴" if sentiment < -0.3 else "⚪")
            lines.append(f"## {name} ({code}) {sentiment_icon}\n")
            lines.append(f"- **情绪**: {sentiment:.2f} | **建议**: {suggestion} | **置信度**: {confidence:.0%}\n")

            if sa.get("summary"):
                lines.append(f"- **分析**: {sa['summary']}\n")
            if sa.get("key_topics"):
                lines.append(f"- **关键主题**: {'、'.join(sa['key_topics'][:5])}\n")
            if sa.get("risk_warnings"):
                lines.append(f"- ⚠️ **风险**: {sa['risk_warnings'][:3]}\n")
            lines.append("\n")

        return "\n".join(lines)

    def get_trading_advice(self, sentiment_data: Dict,
                           market_data: Dict) -> str:
        """
        基于情绪数据和市场数据生成交易建议

        Args:
            sentiment_data: 情绪数据
            market_data: 市场数据（价格、成交量等）

        Returns:
            交易建议文本（JSON格式字符串）
        """
        user_prompt = f"""请根据以下多维数据生成交易建议。

情绪数据：
- 平均情绪: {sentiment_data.get('avg_sentiment', 0)}
- 情绪标签: {sentiment_data.get('sentiment_label', '中性')}
- 新闻数量: {sentiment_data.get('news_count', 0)}
- 关键主题: {sentiment_data.get('key_topics', [])}

市场数据：
- 当前价格: {market_data.get('price', 'N/A')}
- 涨跌幅: {market_data.get('change_pct', 'N/A')}%
- 成交量: {market_data.get('volume', 'N/A')}
- 换手率: {market_data.get('turnover_rate', 'N/A')}%
- 主力资金净流入: {market_data.get('main_net', 'N/A')}

请以严格的 JSON 格式输出：
{{
    "suggestion": "强烈买入/买入/持有/观望/卖出/强烈卖出",
    "reason": "建议理由（30-50字）",
    "risk_level": "高/中/低",
    "confidence": 0.8,
    "operation": "具体操作建议",
    "stop_loss": "止损建议",
    "target": "目标位（如适用）"
}}"""

        content = self._call_api(
            messages=[{"role": "user", "content": user_prompt}],
            system_prompt=self._advice_system_prompt
        )

        if not content:
            return json.dumps({
                "suggestion": "持有",
                "reason": "无法获取AI分析，建议保持当前仓位",
                "risk_level": "中",
                "confidence": 0.5,
                "operation": "暂时观望",
                "stop_loss": "根据个人风险承受能力设定",
                "target": "待定"
            }, ensure_ascii=False)

        return content


# 测试用
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analyzer = NLPAnalyzer()

    # 测试新闻分析
    test_news = [
        {"title": "贵州茅台2024年净利润同比增长15%", "summary": "业绩稳健增长，超出市场预期",
         "source": "东方财富", "published_at": "2025-01-15"},
        {"title": "茅台酒出厂价上调20%", "summary": "提价将显著增厚公司利润",
         "source": "新浪财经", "published_at": "2025-01-15"},
    ]
    result = analyzer.analyze_news(test_news)
    print(json.dumps(result, ensure_ascii=False, indent=2))
