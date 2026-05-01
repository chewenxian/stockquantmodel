"""
个股分析入口模块
对单只或多只股票进行完整分析

流程：
1. 从数据库读取新闻、公告、行情、资金数据
2. 调用 NLP 分析器做情绪分析
3. 调用建议生成器生成建议
4. 结果写入分析表
"""
import logging
import json
from datetime import datetime, date
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class StockAnalyzer:
    """
    个股分析入口
    整合 NLP 分析、情绪分析、建议生成的完整流程
    """

    def __init__(self, db=None, nlp_analyzer=None, sentiment_analyzer=None,
                 trading_advisor=None):
        self.db = db
        self.nlp = nlp_analyzer
        self.sentiment = sentiment_analyzer
        self.advisor = trading_advisor
        self._init_components()

    def _init_components(self):
        """延迟初始化各组件"""
        if self.db is None:
            try:
                from storage.database import Database
                config = self._load_config()
                db_path = config.get("system", {}).get("db_path", "data/stock_news.db")
                self.db = Database(db_path)
            except Exception as e:
                logger.error(f"初始化数据库失败: {e}")

        if self.nlp is None:
            try:
                from analyzer.nlp_analyzer import NLPAnalyzer
                self.nlp = NLPAnalyzer()
            except Exception as e:
                logger.warning(f"初始化 NLP 分析器失败: {e}")

        if self.sentiment is None:
            try:
                from analyzer.sentiment import SentimentAnalyzer
                self.sentiment = SentimentAnalyzer()
            except Exception as e:
                logger.warning(f"初始化情绪分析器失败: {e}")

        if self.advisor is None:
            try:
                from analyzer.advisor import TradingAdvisor
                self.advisor = TradingAdvisor()
            except Exception as e:
                logger.warning(f"初始化建议生成器失败: {e}")

    def _load_config(self) -> dict:
        try:
            with open("config.yaml", "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            return {}

    # ──────────────────────────────────────────
    # 数据读取
    # ──────────────────────────────────────────

    def _get_news_for_stock(self, code: str, days: int = 1) -> List[Dict]:
        """获取某只股票的近期新闻"""
        if not self.db:
            return []
        try:
            return self.db.get_stock_news_sentiment(code, days=days)
        except Exception as e:
            logger.error(f"读取 {code} 新闻失败: {e}")
            return []

    def _get_market_data(self, code: str) -> Optional[Dict]:
        """获取某只股票的最新行情"""
        if not self.db:
            return None
        try:
            conn = self.db._connect()
            row = conn.execute("""
                SELECT price, change_pct, volume, amount, high, low,
                       open, turnover_rate, pe, pb, total_mv
                FROM market_snapshots
                WHERE stock_code = ?
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (code,)).fetchone()
            self.db._close(conn)
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"读取 {code} 行情失败: {e}")
            return None

    def _get_money_flow(self, code: str) -> Optional[Dict]:
        """获取某只股票的最新资金流向"""
        if not self.db:
            return None
        try:
            conn = self.db._connect()
            row = conn.execute("""
                SELECT main_net, retail_net, north_net, large_order_net, total_amount
                FROM money_flow
                WHERE stock_code = ?
                ORDER BY date DESC
                LIMIT 1
            """, (code,)).fetchone()
            self.db._close(conn)
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"读取 {code} 资金流向失败: {e}")
            return None

    def _get_stock_info(self, code: str) -> Optional[Dict]:
        """获取股票基本信息"""
        if not self.db:
            return None
        try:
            conn = self.db._connect()
            row = conn.execute(
                "SELECT code, name, market, industry FROM stocks WHERE code = ?",
                (code,)
            ).fetchone()
            self.db._close(conn)
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"读取 {code} 基本信息失败: {e}")
            return None

    # ──────────────────────────────────────────
    # 核心分析
    # ──────────────────────────────────────────

    def analyze_stock(self, code: str, days: int = 1) -> Dict:
        """
        对单只股票进行完整分析

        Args:
            code: 股票代码
            days: 分析近几天的数据

        Returns:
            {
                "code": "600519",
                "name": "贵州茅台",
                "avg_sentiment": 0.45,
                "sentiment_std": 0.12,
                "news_count": 10,
                "summary": "...",
                "key_topics": [...],
                "risk_warnings": [...],
                "anomaly": {...},
                "market_data": {...},
                "suggestion": "买入",
                "confidence": 0.75,
                "risk_level": "低",
                "reason": "..."
            }
        """
        logger.info(f"开始分析 {code} (近{days}天)")

        # 1. 获取股票信息
        stock_info = self._get_stock_info(code) or {"code": code, "name": code, "market": ""}

        # 2. 获取相关数据
        news_items = self._get_news_for_stock(code, days=days)
        market_data = self._get_market_data(code)
        money_flow = self._get_money_flow(code)

        # 合并行情和资金数据
        full_market = {}
        if market_data:
            full_market.update(market_data)
        if money_flow:
            full_market.update(money_flow)

        logger.info(f"{code}: 获取到 {len(news_items)} 条新闻, 行情数据: {'有' if market_data else '无'}")

        # 3. 情绪分析
        avg_sentiment = 0.0
        sentiment_std = 0.0
        summary = "无相关新闻"
        key_topics = []
        risk_warnings = []
        anomaly = {"is_anomaly": False, "type": "none", "severity": "low"}

        if news_items:
            # 规则情绪分析
            if self.sentiment:
                avg_sentiment = self.sentiment.calculate_sentiment_score(news_items)
                anomaly = self.sentiment.detect_anomaly(news_items)

                # 计算情绪标准差
                sentiments = [self.sentiment.analyze_text_sentiment(
                    n.get("title", ""), n.get("summary", ""))
                    for n in news_items]
                if sentiments:
                    mean = sum(sentiments) / len(sentiments)
                    variance = sum((s - mean) ** 2 for s in sentiments) / len(sentiments)
                    sentiment_std = round(variance ** 0.5, 4)

            # LLM 分析
            if self.nlp:
                llm_result = self.nlp.analyze_news(news_items)
                summary = llm_result.get("summary", summary)
                key_topics = llm_result.get("key_topics", [])
                risk_warnings = llm_result.get("risk_warnings", [])
                # 优先使用 LLM 的情绪评分
                if "avg_sentiment" in llm_result:
                    avg_sentiment = llm_result["avg_sentiment"]

        # 4. 生成交易建议
        analysis_data = {
            "code": code,
            "name": stock_info.get("name", code),
            "avg_sentiment": avg_sentiment,
            "news_count": len(news_items),
            "key_topics": key_topics,
            "risk_warnings": risk_warnings,
            "anomaly": anomaly,
            "market_data": full_market,
        }

        advice = {}
        if self.advisor:
            advice = self.advisor.generate_advice(analysis_data)

        # 5. 组装结果
        result = {
            "code": code,
            "name": stock_info.get("name", code),
            "avg_sentiment": avg_sentiment,
            "sentiment_std": sentiment_std,
            "news_count": len(news_items),
            "summary": summary,
            "key_topics": key_topics[:5],
            "risk_warnings": risk_warnings[:3],
            "anomaly": anomaly,
            "market_data": full_market,
            # 建议字段
            "suggestion": advice.get("suggestion", "持有"),
            "suggestion_reason": advice.get("reason", ""),
            "confidence": advice.get("confidence", 0.0),
            "risk_level": advice.get("risk_level", "中"),
        }

        # 6. 写入数据库
        self._save_analysis(result)

        logger.info(f"{code} 分析完成: 情绪={avg_sentiment:.2f}, 建议={result['suggestion']}")
        return result

    def _save_analysis(self, result: Dict):
        """将分析结果写入数据库"""
        if not self.db:
            return
        try:
            today = date.today().isoformat()
            conn = self.db._connect()

            # 检查是否已有今日分析
            existing = conn.execute(
                "SELECT id FROM analysis WHERE stock_code = ? AND date = ?",
                (result["code"], today)
            ).fetchone()

            if existing:
                # 更新
                conn.execute("""
                    UPDATE analysis SET
                        news_count = ?, avg_sentiment = ?, sentiment_std = ?,
                        key_topics = ?, llm_analysis = ?,
                        suggestion = ?, confidence = ?, risk_level = ?
                    WHERE stock_code = ? AND date = ?
                """, (
                    result["news_count"],
                    result["avg_sentiment"],
                    result.get("sentiment_std", 0),
                    json.dumps(result["key_topics"], ensure_ascii=False),
                    result.get("summary", ""),
                    result["suggestion"],
                    result["confidence"],
                    result["risk_level"],
                    result["code"],
                    today,
                ))
            else:
                # 插入
                conn.execute("""
                    INSERT INTO analysis (
                        stock_code, date, news_count, avg_sentiment,
                        sentiment_std, key_topics, llm_analysis,
                        suggestion, confidence, risk_level
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result["code"],
                    today,
                    result["news_count"],
                    result["avg_sentiment"],
                    result.get("sentiment_std", 0),
                    json.dumps(result["key_topics"], ensure_ascii=False),
                    result.get("summary", ""),
                    result["suggestion"],
                    result["confidence"],
                    result["risk_level"],
                ))

            conn.commit()
            self.db._close(conn)
        except Exception as e:
            logger.error(f"保存分析结果失败 ({result['code']}): {e}")

    def analyze_all_stocks(self) -> List[Dict]:
        """
        分析所有自选股

        Returns:
            所有股票的分析结果列表
        """
        if not self.db:
            logger.error("数据库未初始化，无法分析")
            return []

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("自选股列表为空")
            return []

        logger.info(f"开始分析所有自选股，共 {len(stocks)} 只")

        results = []
        for stock in stocks:
            try:
                code = stock["code"]
                result = self.analyze_stock(code, days=1)
                results.append(result)
            except Exception as e:
                logger.error(f"分析 {stock.get('code', '?')} 异常: {e}", exc_info=True)
                results.append({
                    "code": stock.get("code", "?"),
                    "name": stock.get("name", "未知"),
                    "avg_sentiment": 0.0,
                    "news_count": 0,
                    "summary": f"分析失败: {e}",
                    "key_topics": [],
                    "risk_warnings": ["分析异常"],
                    "suggestion": "持有",
                    "confidence": 0.0,
                    "risk_level": "中",
                })

        logger.info(f"全部分析完成，成功 {len(results)}/{len(stocks)}")
        return results

    # ──────────────────────────────────────────
    # 数据库查询
    # ──────────────────────────────────────────

    def get_analysis_history(self, code: str, days: int = 30) -> List[Dict]:
        """获取历史分析记录"""
        if not self.db:
            return []
        try:
            conn = self.db._connect()
            rows = conn.execute("""
                SELECT * FROM analysis
                WHERE stock_code = ?
                ORDER BY date DESC
                LIMIT ?
            """, (code, days)).fetchall()
            self.db._close(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"查询分析历史失败: {e}")
            return []


# 测试用
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    analyzer = StockAnalyzer()
    results = analyzer.analyze_all_stocks()

    print(f"\n📊 分析结果 ({len(results)} 只股票):")
    for r in results:
        print(f"\n{'='*50}")
        print(f"{r['name']} ({r['code']})")
        print(f"  情绪: {r['avg_sentiment']:.2f} | 建议: {r['suggestion']}")
        print(f"  置信度: {r['confidence']:.0%} | 风险: {r['risk_level']}")
        print(f"  摘要: {r['summary'][:100]}")
