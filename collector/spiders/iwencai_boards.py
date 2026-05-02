"""
同花顺问财板块数据采集器（替代被封锁的 push2 API）

使用 hithink-sector-selector 技能查询：
- 行业板块涨跌幅排行
- 概念板块涨跌幅排行
- 行业板块资金流向
- 北向资金流向
- 板块估值数据

数据源：同花顺 i问财 OpenAPI（需 IWENCAI_API_KEY 环境变量）
"""
import json
import logging
import subprocess
import os
from datetime import datetime
from typing import Dict, List, Optional

from ..base import BaseCollector

logger = logging.getLogger(__name__)


class IwencaiBoardCollector(BaseCollector):
    """
    同花顺问财板块采集器
    
    依赖：
    - iwencai-skillhub-cli 已安装
    - hithink-sector-selector 技能已安装
    - IWENCAI_API_KEY 环境变量
    """

    def __init__(self, db, proxy=None):
        super().__init__(proxy)
        self.db = db
        self.skill_dir = os.path.expanduser(
            "~/.openclaw/workspace/skills/hithink-sector-selector"
        )
        self.cli_script = os.path.join(self.skill_dir, "scripts", "cli.py")

    @property
    def _tracker_key(self) -> str:
        return "iwencai_boards"

    def collect(self) -> Dict[str, int]:
        """执行全量板块数据采集"""
        results = {}

        # 1. 行业板块涨跌幅
        try:
            results["industry_up"] = self._query_boards("今日涨幅最大的30个行业板块", "industry")
        except Exception as e:
            logger.error(f"[问财板块] 行业涨幅采集异常: {e}")
            results["industry_up"] = 0

        # 2. 行业板块跌幅
        try:
            results["industry_down"] = self._query_boards("今日跌幅最大的30个行业板块", "industry")
        except Exception as e:
            logger.error(f"[问财板块] 行业跌幅采集异常: {e}")
            results["industry_down"] = 0

        # 3. 概念板块涨幅
        try:
            results["concept_up"] = self._query_boards("今日涨幅最大的30个概念板块", "concept")
        except Exception as e:
            logger.error(f"[问财板块] 概念涨幅采集异常: {e}")
            results["concept_up"] = 0

        # 4. 主力资金净流入行业板块
        try:
            results["fund_inflow"] = self._query_money_flow("当日主力资金净流入前20的行业板块")
        except Exception as e:
            logger.error(f"[问财板块] 资金流入采集异常: {e}")
            results["fund_inflow"] = 0

        # 5. 主力资金净流出行业板块
        try:
            results["fund_outflow"] = self._query_money_flow("当日主力资金净流出前20的行业板块")
        except Exception as e:
            logger.error(f"[问财板块] 资金流出采集异常: {e}")
            results["fund_outflow"] = 0

        total = sum(v for v in results.values() if isinstance(v, int))
        logger.info(f"[问财板块] 采集完成: {results}, 总计 {total} 条")
        return results

    def _run_query(self, query_text: str, limit: int = 30) -> List[Dict]:
        """执行问财查询"""
        try:
            result = subprocess.run(
                ["python3", self.cli_script,
                 "--query", query_text,
                 "--limit", str(limit)],
                capture_output=True, text=True, timeout=30,
                env={**os.environ}
            )
            if result.returncode != 0:
                logger.warning(f"[问财板块] CLI 异常: {result.stderr[:200]}")
                return []

            data = json.loads(result.stdout)
            if data.get("success") and data.get("datas"):
                return data["datas"]
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"[问财板块] JSON解析失败: {e}")
            return []
        except subprocess.TimeoutExpired:
            logger.warning(f"[问财板块] 查询超时: {query_text[:30]}")
            return []
        except Exception as e:
            logger.warning(f"[问财板块] 查询异常: {e}")
            return []

    def _query_boards(self, query_text: str, board_type: str) -> int:
        """
        查询板块涨跌幅并存入 board_index

        Args:
            query_text: 问财查询语句
            board_type: industry / concept / area

        Returns:
            存入条数
        """
        items = self._run_query(query_text)
        if not items:
            return 0

        count = 0
        now = datetime.now().isoformat()
        for item in items:
            try:
                board_code = str(item.get("指数代码", ""))
                board_name = str(item.get("指数简称", ""))
                change_pct = item.get("最新涨跌幅:前复权")
                if change_pct is None:
                    change_pct = item.get("涨跌幅", 0)
                change_pct = float(change_pct) if change_pct else 0

                leader = str(item.get("成分领域", ""))

                conn = self.db._connect()
                conn.execute("""
                    INSERT INTO board_index(
                        board_name, board_code, change_pct, leader_stocks, snapshot_time
                    ) VALUES(?, ?, ?, ?, ?)
                """, (board_name, board_code, change_pct, leader, now))
                conn.commit()
                self.db._close(conn)
                count += 1
            except Exception as e:
                logger.debug(f"[问财板块] 入库异常: {e}")

        logger.info(f"[问财板块] {query_text[:20]} → 入库 {count} 条")
        return count

    def _query_money_flow(self, query_text: str) -> int:
        """
        查询板块资金流向并存入 money_flow

        Args:
            query_text: 问财查询语句

        Returns:
            存入条数
        """
        items = self._run_query(query_text)
        if not items:
            return 0

        count = 0
        today = datetime.now().strftime("%Y-%m-%d")
        for item in items:
            try:
                board_code = str(item.get("指数代码", ""))
                board_name = str(item.get("指数简称", ""))

                # 资金流向可能有多种字段名（问财 API 字段名带日期后缀如 [20260430]）
                main_net = self._extract_money_field(item, "资金净流入额")
                if main_net == 0:
                    main_net = self._extract_money_field(item, "主力净买入额")
                north_net = self._extract_money_field(item, "北向资金净流入")
                if north_net == 0:
                    north_net = self._extract_money_field(item, "净买入额合计值")
                retail_net = self._extract_money_field(item, "散户资金净流入")
                total_amount = self._extract_money_field(item, "成交额")

                conn = self.db._connect()
                conn.execute("""
                    INSERT INTO money_flow(
                        stock_code, date, main_net, retail_net,
                        north_net, total_amount, snapshot_time
                    ) VALUES(?, ?, ?, ?, ?, ?, datetime('now'))
                """, (board_code, today, main_net, retail_net,
                      north_net, total_amount))
                conn.commit()
                self.db._close(conn)
                count += 1
            except Exception as e:
                logger.debug(f"[问财板块] 资金流入库异常: {e}")

        logger.info(f"[问财板块] {query_text[:20]} → 入库 {count} 条")
        return count

    @staticmethod
    def _extract_money_field(item: Dict, field_prefix: str) -> float:
        """从问财返回数据中提取资金字段（字段名可能带日期后缀）"""
        for key, val in item.items():
            if key.startswith(field_prefix):
                try:
                    return float(val) if val else 0
                except (ValueError, TypeError):
                    return 0
        return 0
