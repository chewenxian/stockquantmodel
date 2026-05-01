"""
个股分析入口模块（增强版）
对单只或多只股票进行完整分析

流程：
1. 从数据库读取新闻、公告、行情、资金数据
2. 调用 NER 提取金融实体
3. 调用知识图谱推理间接影响
4. 调用 NLP 分析器做情绪分析
5. 调用影响评估模型
6. 调用建议生成器生成增强建议（含推理链）
7. 结果写入分析表

v7.0 新增:
- 跨源情报交叉验证: 调用 cross_validate.verify_news()
- 分析报告中增加 "信息可信度" 字段
- 对低可信度的新闻降低其情绪权重
"""
import logging
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class StockAnalyzer:
    """
    个股分析入口（增强版）
    整合 NER、知识图谱、影响评估、NLP、建议生成的完整流程
    """

    def __init__(self, db=None, nlp_analyzer=None, sentiment_analyzer=None,
                 trading_advisor=None, knowledge_graph=None, impact_model=None):
        self.db = db
        self.nlp = nlp_analyzer
        self.sentiment = sentiment_analyzer
        self.advisor = trading_advisor
        self.kg = knowledge_graph
        self.impact_model = impact_model
        self._init_components()

    def _init_components(self):
        """延迟初始化各组件"""
        import yaml
        config = self._load_config()

        if self.db is None:
            try:
                from storage.database import Database
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

        if self.kg is None:
            try:
                from analyzer.knowledge_graph import FinancialKnowledgeGraph
                self.kg = FinancialKnowledgeGraph()
            except Exception as e:
                logger.warning(f"初始化知识图谱失败: {e}")

        if self.impact_model is None:
            try:
                from analyzer.impact_model import ImpactModel
                self.impact_model = ImpactModel(db=self.db)
            except Exception as e:
                logger.warning(f"初始化影响评估模型失败: {e}")

        if self.advisor is None:
            try:
                from analyzer.advisor import TradingAdvisor
                self.advisor = TradingAdvisor(knowledge_graph=self.kg,
                                               impact_model=self.impact_model)
            except Exception as e:
                logger.warning(f"初始化建议生成器失败: {e}")

        # 交叉验证引擎（v7.0）
        self.cross_validator = None

    def _init_cross_validator(self):
        """延迟初始化交叉验证器（v7.0）"""
        if self.cross_validator is not None:
            return self.cross_validator
        try:
            from analyzer.cross_validate import CrossValidator
            self.cross_validator = CrossValidator(db=self.db)
            logger.info("交叉验证引擎已初始化")
        except Exception as e:
            logger.warning(f"初始化交叉验证引擎失败: {e}")
            self.cross_validator = None
        return self.cross_validator

    def _load_config(self) -> dict:
        import yaml
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
            news = self.db.get_stock_news_sentiment(code, days=days)
            if news:
                return news
        except Exception as e:
            logger.warning(f"读取 {code} 新闻关联失败: {e}")

        # Fallback: 按股票名称关键词搜索
        try:
            conn = self.db._connect()
            name_row = conn.execute(
                "SELECT name FROM stocks WHERE code=?", (code,)
            ).fetchone()
            if name_row and name_row[0]:
                keyword = name_row[0][:4]
                rows = conn.execute(
                    """SELECT id, title, source, published_at, 0.0 as sentiment
                       FROM news WHERE title LIKE ?
                       ORDER BY published_at DESC LIMIT 20""",
                    (f"%{keyword}%",)
                ).fetchall()
                conn.close()
                if rows:
                    logger.info(f"{code}: 关键词[{keyword}]搜索到 {len(rows)} 条新闻")
                    return [dict(r) for r in rows]
            conn.close()
        except Exception:
            pass
        return []

    def _get_market_data(self, code: str) -> Optional[Dict]:
        """获取某只股票的最新行情（优先实时API）"""
        try:
            from collector.spiders.eastmoney import EastMoneyCollector
            temp_c = EastMoneyCollector(None)
            prefix = "1." if code.startswith("6") else "0."
            data = temp_c.get_json(
                "https://push2.eastmoney.com/api/qt/ulist.np/get",
                {"secids": prefix + code, "fields": "f2,f3,f4,f5,f6,f7,f15,f16,f17,f18,f20,f21,f57", "fltt": 2, "invt": 2}
            )
            if data and data.get("data") and data["data"].get("diff"):
                d = data["data"]["diff"][0]
                return {
                    "price": d.get("f2", 0) or 0,
                    "change_pct": d.get("f3", 0) or 0,
                    "volume": d.get("f4", 0) or 0,
                    "amount": d.get("f5", 0) or 0,
                    "high": d.get("f15", 0) or 0,
                    "low": d.get("f16", 0) or 0,
                    "open": d.get("f17", 0) or 0,
                    "turnover_rate": d.get("f20", 0) or 0,
                    "pe": d.get("f21", 0) or 0,
                    "source": "realtime"
                }
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"实时行情API失败: {e}")

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
    # 核心分析（增强版）
    # ──────────────────────────────────────────

    def analyze_stock(self, code: str, days: int = 1) -> Dict:
        """
        对单只股票进行完整分析（增强版）

        流程：
        1. 获取股票信息、新闻、行情、资金数据
        2. NER 实体提取（公司名/产品/行业）
        3. 知识图谱推理间接影响
        4. 情绪分析（规则+LLM）
        5. 影响评估模型计算影响因子
        6. 增强建议生成（含推理链）

        Args:
            code: 股票代码
            days: 分析近几天的数据

        Returns (增强格式):
            {
                "code": "300750",
                "name": "宁德时代",
                "avg_sentiment": 0.72,
                ...
                "ner_entities": {提取出的金融实体},
                "kg_reasoning": {知识图谱推理结果},
                "impact_evaluation": {影响评估结果},
                "suggestion": "强烈关注",
                "confidence": 0.85,
                "reasoning": ["推理1", "推理2", ...],
                "key_factors": ["关键因子1", "关键因子2"],
                ...
            }
        """
        logger.info(f"开始增强分析 {code} (近{days}天)")

        # 1. 获取基础信息
        stock_info = self._get_stock_info(code) or {"code": code, "name": code, "market": ""}
        name = stock_info.get("name", code)

        # 2. 获取相关数据
        news_items = self._get_news_for_stock(code, days=days)
        market_data = self._get_market_data(code)
        money_flow = self._get_money_flow(code)

        # 合并行情和资金数据
        full_market: Dict = {}
        if market_data:
            full_market.update(market_data)
        if money_flow:
            full_market.update(money_flow)

        logger.info(f"{code}: {len(news_items)} 条新闻, 行情: {'有' if market_data else '无'}")

        # 3. NER 实体提取
        ner_entities: Dict = {}
        news_context = ""
        try:
            from analyzer.ner_extractor import extract_financial_entities
            # 拼接新闻上下文用于NER
            news_context = " ".join([
                n.get("title", "") for n in news_items[:10]
            ])
            if news_context:
                ner_entities = extract_financial_entities(news_context)
                logger.info(f"{code}: NER提取到 {len(ner_entities.get('sectors', []))} 个行业, "
                           f"{len(ner_entities.get('products', []))} 个产品, "
                           f"{len(ner_entities.get('stock_mentions', []))} 只相关股票")
        except Exception as e:
            logger.warning(f"{code}: NER提取异常: {e}")

        # 4. 知识图谱推理
        kg_result: Dict = {}
        if self.kg and news_context:
            try:
                kg_result = self.kg.infer_impact(code, news_context)
                logger.info(f"{code}: 知识图谱推理: {kg_result.get('direct_impact', 'N/A')}, "
                           f"{len(kg_result.get('chain_reactions', []))} 条连锁反应")
            except Exception as e:
                logger.warning(f"{code}: 知识图谱推理异常: {e}")

        # 5. 交叉验证新闻可信度（v7.0）
        credibility_info = {
            "verified_news_count": 0,
            "credibility_tags": {},
            "confidence_weight": 1.0,
        }
        validator = self._init_cross_validator()
        if validator and news_items:
            try:
                for n in news_items:
                    if n.get("credibility_tag"):
                        tag = n["credibility_tag"]
                        verified = n.get("verified", 0)
                    else:
                        verify_result = validator.verify_news(n)
                        tag = verify_result["tag"]
                        verified = 1 if verify_result["verified"] else 0
                        if n.get("id") and isinstance(n["id"], int):
                            try:
                                validator.save_verification(n["id"], verify_result)
                            except Exception:
                                pass

                    credibility_info["credibility_tags"][str(n.get("id", "?"))] = {
                        "tag": tag,
                        "verified": verified,
                        "confidence": n.get("cross_verify", {}).get("confidence", 0.0),
                    }
                    if verified:
                        credibility_info["verified_news_count"] += 1

                total = len(news_items)
                verified_ratio = credibility_info["verified_news_count"] / max(total, 1)
                credibility_info["confidence_weight"] = round(0.5 + 0.5 * verified_ratio, 4)

                logger.info(f"{code}: 交叉验证完成 - "
                            f"已验证 {credibility_info['verified_news_count']}/{total}, "
                            f"权重={credibility_info['confidence_weight']}")
            except Exception as e:
                logger.warning(f"{code}: 交叉验证异常: {e}")

        # 6. 情绪分析（受可信度权重修正）
        avg_sentiment = 0.0
        sentiment_std = 0.0
        summary = "无相关新闻"
        key_topics: List[str] = []
        risk_warnings: List[str] = []
        anomaly: Dict = {"is_anomaly": False, "type": "none", "severity": "low"}

        if news_items:
            if self.sentiment:
                try:
                    raw_sentiment = self.sentiment.calculate_sentiment_score(news_items)
                    credibility_weight = credibility_info.get("confidence_weight", 1.0)
                    avg_sentiment = raw_sentiment * credibility_weight
                    anomaly = self.sentiment.detect_anomaly(news_items)

                    sentiments = [
                        self.sentiment.analyze_text_sentiment(
                            n.get("title", ""), n.get("summary", "")
                        ) for n in news_items
                    ]
                    if sentiments:
                        mean_val = sum(sentiments) / len(sentiments)
                        variance = sum((s - mean_val) ** 2 for s in sentiments) / len(sentiments)
                        sentiment_std = round(variance ** 0.5, 4)

                    logger.info(f"{code}: 情绪={raw_sentiment:.2f} × 可信度权重={credibility_weight} = {avg_sentiment:.2f}")
                except Exception as e:
                    logger.warning(f"{code}: 规则情绪分析异常: {e}")

            if self.nlp:
                try:
                    llm_result = self.nlp.analyze_news(news_items)
                    summary = llm_result.get("summary", summary)
                    key_topics = llm_result.get("key_topics", [])
                    risk_warnings = llm_result.get("risk_warnings", [])
                    if "avg_sentiment" in llm_result:
                        avg_sentiment = llm_result["avg_sentiment"] * credibility_info.get("confidence_weight", 1.0)
                except Exception as e:
                    logger.warning(f"{code}: NLP分析异常: {e}")

        # 如果知识图谱推理了直接冲击，影响情绪
        kg_direct = kg_result.get("direct_impact", "")
        if kg_direct in ("重大利好", "利好") and avg_sentiment < 0.4:
            avg_sentiment = max(avg_sentiment, 0.4)
            logger.info(f"{code}: 知识图谱修正情绪为正面 (原:{avg_sentiment:.2f})")
        elif kg_direct in ("重大利空", "利空") and avg_sentiment > -0.4:
            avg_sentiment = min(avg_sentiment, -0.4)
            logger.info(f"{code}: 知识图谱修正情绪为负面 (原:{avg_sentiment:.2f})")

        # 7. 影响评估
        impact_evaluation: Dict = {}
        if self.impact_model:
            try:
                impact_evaluation = self.impact_model.calculate_impact_factor(
                    stock_code=code,
                    sentiment=avg_sentiment,
                    news_count=len(news_items),
                    market_data=full_market,
                    anomaly_data=anomaly,
                    knowledge_graph_impact=kg_result,
                )
                logger.info(f"{code}: 影响评估得分: {impact_evaluation.get('impact_score', 0):.2f}, "
                           f"等级: {impact_evaluation.get('level', 'N/A')}")
            except Exception as e:
                logger.warning(f"{code}: 影响评估异常: {e}")

        # 8. 增强建议生成（含可信度信息）
        analysis_data = {
            "code": code,
            "name": name,
            "avg_sentiment": avg_sentiment,
            "news_count": len(news_items),
            "key_topics": key_topics,
            "risk_warnings": risk_warnings,
            "anomaly": anomaly,
            "market_data": full_market,
            "impact_evaluation": impact_evaluation,
            "kg_reasoning": kg_result,
            "credibility": credibility_info,
        }

        advice: Dict = {}
        if self.advisor:
            try:
                advice = self.advisor.generate_advice(analysis_data)
            except Exception as e:
                logger.warning(f"{code}: 建议生成异常: {e}")
                advice = {
                    "suggestion": "持有",
                    "confidence": 0.3,
                    "reasoning": [f"生成失败: {e}"],
                    "risk_level": "中",
                    "key_factors": [],
                }

        # 9. 构建增强格式的结果（含可信度）
        info_credibility = {
            "verified_news_count": credibility_info["verified_news_count"],
            "total_news": len(news_items),
            "verified_ratio": round(
                credibility_info["verified_news_count"] / max(len(news_items), 1), 4
            ),
            "confidence_weight": credibility_info["confidence_weight"],
        }

        result = {
            "code": code,
            "name": name,
            "avg_sentiment": round(avg_sentiment, 4),
            "sentiment_std": sentiment_std,
            "news_count": len(news_items),
            "summary": summary,
            "key_topics": key_topics[:5],
            "risk_warnings": risk_warnings[:3],
            "anomaly": anomaly,
            "market_data": full_market,
            # 增强分析字段
            "ner_entities": ner_entities,
            "kg_reasoning": kg_result,
            "impact_evaluation": impact_evaluation,
            "related_stocks": kg_result.get("related_stocks", []),
            "chain_reactions": kg_result.get("chain_reactions", []),
            # 增强建议字段
            "suggestion": advice.get("suggestion", "持有"),
            "suggestion_reason": "; ".join(advice.get("reasoning", [])),
            "reasoning_chain": advice.get("reasoning", []),
            "key_factors": advice.get("key_factors", []),
            "confidence": advice.get("confidence", 0.0),
            "risk_level": advice.get("risk_level", "中"),
            "impact_level": impact_evaluation.get("level", "中性"),
            "impact_score": impact_evaluation.get("impact_score", 0.0),
            # 可信度字段（v7.0）
            "credibility": credibility_info,
            "info_credibility": info_credibility,
        }

        # 10. 写入数据库
        self._save_analysis(result)

        logger.info(f"{code}: 增强分析完成: "
                    f"情绪={avg_sentiment:.2f}, "
                    f"可信度={info_credibility['verified_ratio']:.0%}, "
                    f"推理链={len(result['reasoning_chain'])}步, "
                    f"建议={result['suggestion']}")
        return result

    def _save_analysis(self, result: Dict):
        """将增强分析结果写入数据库（新增 reasoning_chain 等字段）"""
        if not self.db:
            return
        try:
            today = date.today().isoformat()
            conn = self.db._connect()

            existing = conn.execute(
                "SELECT id FROM analysis WHERE stock_code = ? AND date = ?",
                (result["code"], today)
            ).fetchone()

            # 增强的分析数据（含推理链）
            analysis_json = json.dumps({
                "reasoning_chain": result.get("reasoning_chain", []),
                "key_factors": result.get("key_factors", []),
                "kg_direct_impact": result.get("kg_reasoning", {}).get("direct_impact", ""),
                "impact_level": result.get("impact_level", "中性"),
                "impact_score": result.get("impact_score", 0.0),
                "ner_companies": result.get("ner_entities", {}).get("companies", []),
                "ner_products": result.get("ner_entities", {}).get("products", []),
                "ner_sectors": result.get("ner_entities", {}).get("sectors", []),
                "ner_stocks": result.get("ner_entities", {}).get("stock_mentions", []),
                "chain_reactions": result.get("chain_reactions", []),
            }, ensure_ascii=False)

            if existing:
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
                    f"{result.get('summary', '')}\n\n[增强分析]\n{analysis_json}",
                    result["suggestion"],
                    result["confidence"],
                    result["risk_level"],
                    result["code"],
                    today,
                ))
            else:
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
                    f"{result.get('summary', '')}\n\n[增强分析]\n{analysis_json}",
                    result["suggestion"],
                    result["confidence"],
                    result["risk_level"],
                ))

            conn.commit()
            self.db._close(conn)
        except Exception as e:
            logger.error(f"保存分析结果失败 ({result['code']}): {e}")

    # ──────────────────────────────────────────
    # 批量分析
    # ──────────────────────────────────────────

    def analyze_all_stocks(self) -> List[Dict]:
        """分析所有自选股（增强版）"""
        if not self.db:
            logger.error("数据库未初始化，无法分析")
            return []

        stocks = self.db.load_stocks()
        if not stocks:
            logger.warning("自选股列表为空")
            return []

        logger.info(f"开始增强分析所有自选股，共 {len(stocks)} 只")

        results = []
        for stock in stocks:
            try:
                code = stock["code"]
                result = self.analyze_stock(code, days=1)
                results.append(result)
            except Exception as e:
                logger.error(f"增强分析 {stock.get('code', '?')} 异常: {e}", exc_info=True)
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
                    "reasoning_chain": ["分析流程异常"],
                    "key_factors": [],
                    "ner_entities": {},
                    "kg_reasoning": {},
                    "impact_evaluation": {},
                    "chain_reactions": [],
                })

        logger.info(f"全部增强分析完成，成功 {len(results)}/{len(stocks)}")
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

    def compare_stocks_impact(self) -> List[Dict]:
        """
        多只股票影响对比

        Returns:
            按影响分绝对值排序的对比列表
        """
        if not self.impact_model:
            logger.warning("影响评估模型未初始化")
            return []

        try:
            analyses = self.analyze_all_stocks()
            return self.impact_model.compare_stocks(analyses)
        except Exception as e:
            logger.error(f"影响对比分析失败: {e}")
            return []


# ═══════════════════════════════════════════════════
# 测试
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    analyzer = StockAnalyzer()
    results = analyzer.analyze_all_stocks()

    print(f"\n📊 增强分析结果 ({len(results)} 只股票):")
    for r in results:
        print(f"\n{'='*60}")
        print(f"{r['name']} ({r['code']})")
        print(f"  情绪: {r['avg_sentiment']:.2f}")
        print(f"  建议: {r['suggestion']} (置信度: {r['confidence']:.0%}, 风险: {r['risk_level']})")
        print(f"  影响等级: {r['impact_level']} (分: {r['impact_score']:.2f})")
        print(f"  推理链:")
        for step in r['reasoning_chain'][:5]:
            print(f"    → {step}")
        if r['key_factors']:
            print(f"  关键因子: {', '.join(r['key_factors'][:4])}")
        if r.get('chain_reactions'):
            print(f"  连锁反应: {len(r['chain_reactions'])} 条")
