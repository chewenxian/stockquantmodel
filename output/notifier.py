#!/usr/bin/env python3
"""
📣 多渠道推送模块

支持通道：
- 微信（OpenClaw 微信通道 → print 输出，由 OpenClaw 转发）
- Telegram Bot
- QQ（go-cqhttp OneBot API）
- 飞书机器人 Webhook
- 企业微信机器人 Webhook
- 钉钉机器人 Webhook

推送时机：
- 盘前 08:30 → 早报
- 收盘 16:00 → 晚报
- 实时 → S/A 级信号告警
- 实时 → 风控告警
"""
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Notifier:
    """
    多渠道推送：微信/Telegram/QQ/飞书/钉钉
    """

    # ══════════════════════════════════════════
    # 微信通道（OpenClaw 转发）
    # ══════════════════════════════════════════

    # ══════════════════════════════════════════
    # QQ 通道（go-cqhttp OneBot API）
    # ══════════════════════════════════════════

    @staticmethod
    def push_qq(markdown_text: str, qq_api_url: str = None,
                 qq_target: str = None, qq_target_type: str = "group") -> bool:
        """
        推送到 QQ（通过 go-cqhttp OneBot v11 HTTP API）

        Args:
            markdown_text: 消息文本
            qq_api_url: go-cqhttp HTTP 地址，默认从环境变量 QQ_API_URL（如 http://127.0.0.1:5700）
            qq_target: 目标群号或QQ号，默认从环境变量 QQ_TARGET
            qq_target_type: group（群）或 private（私聊）

        Returns:
            bool: 是否成功
        """
        api_url = qq_api_url or os.environ.get("QQ_API_URL", "")
        target = qq_target or os.environ.get("QQ_TARGET", "")

        if not api_url or not target:
            logger.warning("QQ 未配置（缺少 QQ_API_URL 或 QQ_TARGET）")
            return False

        try:
            import requests
            # 消息分段处理（QQ 消息有长度限制）
            max_len = 4000
            if len(markdown_text) > max_len:
                segments = [markdown_text[i:i+max_len] for i in range(0, len(markdown_text), max_len)]
            else:
                segments = [markdown_text]

            success = True
            for seg in segments:
                # 简化 markdown 为纯文本（go-cqhttp 不支持完整 markdown）
                # 保留粗体和基本符号
                clean_text = seg.replace("**", "").replace("#", "").replace("*", "·")
                payload = {
                    "message_type": qq_target_type,
                    "message": clean_text[:4096],
                }
                if qq_target_type == "group":
                    payload["group_id"] = int(target)
                else:
                    payload["user_id"] = int(target)

                resp = requests.post(
                    f"{api_url.rstrip('/')}/send_msg",
                    json=payload, timeout=5
                )
                if resp.status_code != 200:
                    logger.error(f"QQ 推送失败: {resp.status_code} {resp.text[:200]}")
                    success = False
                else:
                    ret = resp.json()
                    if ret.get("status") != "ok":
                        logger.warning(f"QQ 推送返回异常: {ret}")
                        success = False

            if success:
                logger.info(f"QQ 推送成功 ({len(segments)} 段, {qq_target_type}:{target})")
            return success

        except Exception as e:
            logger.error(f"QQ 推送异常: {e}")
            return False

    # ══════════════════════════════════════════
    # 飞书机器人 Webhook
    # ══════════════════════════════════════════

    @staticmethod
    def push_feishu(markdown_text: str, webhook_url: str = None) -> bool:
        """
        推送到飞书群机器人 Webhook

        Args:
            markdown_text: Markdown 格式消息
            webhook_url: Webhook URL，默认从环境变量 FEISHU_WEBHOOK 读取

        Returns:
            bool: 是否成功
        """
        webhook_url = webhook_url or os.environ.get("FEISHU_WEBHOOK", "")

        if not webhook_url:
            logger.warning("飞书未配置（缺少 FEISHU_WEBHOOK）")
            return False

        try:
            import requests
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": "📡 股票量化情报"},
                        "template": "blue",
                    },
                    "elements": [
                        {"tag": "markdown", "content": markdown_text},
                        {"tag": "hr"},
                        {
                            "tag": "note",
                            "elements": [{
                                "tag": "plain_text",
                                "content": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            }]
                        }
                    ]
                }
            }
            resp = requests.post(webhook_url, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("code") == 0:
                    logger.info("飞书推送成功")
                    return True
            logger.error(f"飞书推送失败: {resp.status_code} {resp.text[:200]}")
            return False

        except Exception as e:
            logger.error(f"飞书推送异常: {e}")
            return False

    @staticmethod
    def push_wechat(markdown_text: str) -> bool:
        """
        推送到微信（通过 OpenClaw 微信通道）

        OpenClaw 会捕获 stdout 中的特定格式文本并转发到微信。
        此处直接 print，由 OpenClaw 代理转发。

        Args:
            markdown_text: Markdown 格式文本

        Returns:
            bool: 是否成功（print 通常不会失败）
        """
        try:
            print(f"\n📣 [微信推送] {datetime.now().strftime('%H:%M:%S')}")
            print(markdown_text)
            logger.info("微信推送完成（已输出到 stdout，由 OpenClaw 转发）")
            return True
        except Exception as e:
            logger.error(f"微信推送失败: {e}")
            return False

    # ══════════════════════════════════════════
    # Telegram Bot 通道
    # ══════════════════════════════════════════

    @staticmethod
    def push_telegram(markdown_text: str, bot_token: str = None,
                       chat_id: str = None) -> bool:
        """
        推送到 Telegram Bot

        Args:
            markdown_text: 消息文本（支持 Markdown）
            bot_token: Bot Token，默认从环境变量 TELEGRAM_BOT_TOKEN 读取
            chat_id: 聊天 ID，默认从环境变量 TELEGRAM_CHAT_ID 读取

        Returns:
            bool: 是否成功
        """
        bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")

        if not bot_token or not chat_id:
            logger.warning("Telegram 未配置（缺少 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID）")
            return False

        try:
            import requests
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": markdown_text,
                "parse_mode": "MarkdownV2",
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("Telegram 推送成功")
                return True
            else:
                logger.error(f"Telegram 推送失败: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            logger.error(f"Telegram 推送异常: {e}")
            return False

    # ══════════════════════════════════════════
    # 企业微信机器人 Webhook
    # ══════════════════════════════════════════

    @staticmethod
    def push_wechat_work(markdown_text: str, webhook_url: str = None) -> bool:
        """
        推送到企业微信机器人 Webhook

        Args:
            markdown_text: Markdown 格式消息
            webhook_url: Webhook URL，默认从环境变量 WECHAT_WORK_WEBHOOK 读取

        Returns:
            bool: 是否成功
        """
        webhook_url = webhook_url or os.environ.get("WECHAT_WORK_WEBHOOK", "")

        if not webhook_url:
            logger.warning("企业微信未配置（缺少 WECHAT_WORK_WEBHOOK）")
            return False

        try:
            import requests
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": markdown_text,
                },
            }
            resp = requests.post(webhook_url, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("errcode") == 0:
                    logger.info("企业微信推送成功")
                    return True

            logger.error(f"企业微信推送失败: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"企业微信推送异常: {e}")
            return False

    # ══════════════════════════════════════════
    # 钉钉机器人 Webhook
    # ══════════════════════════════════════════

    @staticmethod
    def push_dingtalk(markdown_text: str, webhook_url: str = None) -> bool:
        """
        推送到钉钉机器人 Webhook

        Args:
            markdown_text: Markdown 格式消息
            webhook_url: Webhook URL，默认从环境变量 DINGTALK_WEBHOOK 读取

        Returns:
            bool: 是否成功
        """
        webhook_url = webhook_url or os.environ.get("DINGTALK_WEBHOOK", "")

        if not webhook_url:
            logger.warning("钉钉未配置（缺少 DINGTALK_WEBHOOK）")
            return False

        try:
            import requests
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "股票量化分析通知",
                    "text": markdown_text,
                },
            }
            resp = requests.post(webhook_url, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("errcode") == 0:
                    logger.info("钉钉推送成功")
                    return True

            logger.error(f"钉钉推送失败: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"钉钉推送异常: {e}")
            return False

    # ══════════════════════════════════════════
    # 自动分发
    # ══════════════════════════════════════════

    @staticmethod
    def push_report(report_text: str, channels: List[str] = None) -> Dict[str, bool]:
        """
        将报告自动分发到指定通道

        Args:
            report_text: 报告 Markdown 文本
            channels: 通道列表，默认 ["wechat"]
                      可选: wechat, telegram, qq, feishu, wechat_work, dingtalk

        Returns:
            {channel_name: success_bool}
        """
        if channels is None:
            channels = ["wechat"]

        results = {}
        for channel in channels:
            channel = channel.strip().lower()
            if channel == "wechat":
                results["wechat"] = Notifier.push_wechat(report_text)
            elif channel == "telegram":
                results["telegram"] = Notifier.push_telegram(report_text)
            elif channel == "qq":
                results["qq"] = Notifier.push_qq(report_text)
            elif channel == "feishu":
                results["feishu"] = Notifier.push_feishu(report_text)
            elif channel == "wechat_work":
                results["wechat_work"] = Notifier.push_wechat_work(report_text)
            elif channel == "dingtalk":
                results["dingtalk"] = Notifier.push_dingtalk(report_text)
            else:
                logger.warning(f"未知推送通道: {channel}")
                results[channel] = False

        success_count = sum(1 for v in results.values() if v)
        logger.info(f"推送完成: {success_count}/{len(results)} 通道成功")
        return results

    # ══════════════════════════════════════════
    # 通知模板
    # ══════════════════════════════════════════

    @staticmethod
    def morning_report_template(data: Dict) -> str:
        """
        盘前早报 Markdown 模板

        Args:
            data: {
                "date": "2024-01-15",
                "market_summary": "...",
                "signals": [...],
                "hot_events": [...],
                "risk_warnings": [...],
            }

        Returns:
            Markdown 格式盘前早报文本
        """
        date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        lines = [
            f"# 🌅 盘前早报 | {date_str}",
            "",
        ]

        # 市场概况
        market_summary = data.get("market_summary", "数据获取中…")
        lines.append(f"## 📊 市场概况")
        lines.append(market_summary)
        lines.append("")

        # 今日信号
        signals = data.get("signals", [])
        if signals:
            lines.append("## 🔴 今日信号")
            for s in signals:
                level_icon = {"S": "🔴🔴", "A": "🔴", "B": "🟡", "C": "🟢"}.get(
                    s.get("level", "C"), "⚪"
                )
                lines.append(
                    f"- {level_icon} **{s.get('name', '')} ({s.get('code', '')})** "
                    f"| 建议: {s.get('suggestion', '持有')} "
                    f"| 置信度: {s.get('confidence', 0):.0%}"
                )
            lines.append("")

        # 热点事件
        hot_events = data.get("hot_events", [])
        if hot_events:
            lines.append("## 📢 热点事件")
            for e in hot_events[:5]:
                direction_icon = "📈" if e.get("direction") == "利好" else "📉"
                lines.append(
                    f"- {direction_icon} **{e.get('stock_name', e.get('stock_code', ''))}** "
                    f"| {e.get('event_type', '')} "
                    f"| 影响: {e.get('impact', 0):+.2%}"
                )
            lines.append("")

        # 风险提示
        warnings = data.get("risk_warnings", [])
        if warnings:
            lines.append("## ⚠️ 风险提示")
            for w in warnings:
                lines.append(f"- {w}")
            lines.append("")

        lines.append("---")
        lines.append("*📡 由股票量化分析系统自动生成*")

        return "\n".join(lines)

    @staticmethod
    def closing_report_template(data: Dict) -> str:
        """
        收盘晚报 Markdown 模板

        Args:
            data: {
                "date": "2024-01-15",
                "market_summary": "...",
                "top_stocks": [...],
                "signals": [...],
                "today_events": [...],
                "risk_warnings": [...],
            }

        Returns:
            Markdown 格式收盘晚报文本
        """
        date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        lines = [
            f"# 📊 收盘晚报 | {date_str}",
            "",
        ]

        # 大盘概览
        market_summary = data.get("market_summary", "数据获取中…")
        lines.append(f"## 📈 大盘概览")
        lines.append(market_summary)
        lines.append("")

        # 今日信号
        signals = data.get("signals", [])
        if signals:
            lines.append("## 🔴 今日操作信号")
            for s in signals:
                level_icon = {"S": "🔴🔴", "A": "🔴", "B": "🟡", "C": "🟢"}.get(
                    s.get("level", "C"), "⚪"
                )
                lines.append(
                    f"- {level_icon} **{s.get('name', '')} ({s.get('code', '')})** "
                    f"| 建议: {s.get('suggestion', '持有')} "
                    f"| 情绪: {s.get('sentiment', 0):.2f} "
                    f"| 变动: {s.get('change_pct', 0):+.2f}%"
                )
            lines.append("")

        # 个股表现
        top_stocks = data.get("top_stocks", [])
        if top_stocks:
            lines.append("## 🏆 个股表现")
            for s in top_stocks:
                lines.append(
                    f"- **{s.get('name', '')} ({s.get('code', '')})** "
                    f"| {s.get('change_pct', 0):+.2f}% "
                    f"| 建议: {s.get('suggestion', '持有')}"
                )
            lines.append("")

        # 今日事件
        today_events = data.get("today_events", [])
        if today_events:
            lines.append("## 📢 今日重要事件")
            for e in today_events[:5]:
                direction_icon = "📈" if e.get("direction") == "利好" else "📉"
                lines.append(
                    f"- {direction_icon} **{e.get('stock_name', e.get('stock_code', ''))}** "
                    f"| {e.get('event_type', e.get('title', ''))}"
                )
            lines.append("")

        # 风控告警
        warnings = data.get("risk_warnings", [])
        if warnings:
            lines.append("## ⚠️ 风控告警")
            for w in warnings:
                lines.append(f"- {w}")
            lines.append("")

        lines.append("---")
        lines.append("*📡 由股票量化分析系统自动生成*")

        return "\n".join(lines)

    @staticmethod
    def signal_alert_template(signal: Dict) -> str:
        """
        信号告警模板

        Args:
            signal: {
                "code": "300750",
                "name": "宁德时代",
                "level": "S",
                "suggestion": "关注",
                "confidence": 0.85,
                "sentiment": 0.72,
                "reasons": [...],
                "price": 245.5,
                "change_pct": 3.2,
            }

        Returns:
            Markdown 格式信号告警文本
        """
        level_icon = {"S": "🔴🔴 S级", "A": "🔴 A级", "B": "🟡 B级", "C": "🟢 C级"}.get(
            signal.get("level", "C"), "⚪ 无效"
        )
        lines = [
            f"## 🚨 {level_icon} 信号告警",
            "",
            f"**{signal.get('name', '')} ({signal.get('code', '')})**",
            "",
            f"- 建议: **{signal.get('suggestion', '持有')}**",
            f"- 置信度: {signal.get('confidence', 0):.0%}",
            f"- 情绪: {signal.get('sentiment', 0):+.2f}",
        ]

        price = signal.get("price")
        change_pct = signal.get("change_pct")
        if price is not None:
            lines.append(f"- 最新价: {price:.2f}")
        if change_pct is not None:
            lines.append(f"- 涨跌幅: {change_pct:+.2f}%")

        reasons = signal.get("reasons", [])
        if reasons:
            lines.append("")
            lines.append("**信号理由:**")
            for r in reasons:
                lines.append(f"- {r}")

        lines.append("")
        lines.append(f"*⏰ {datetime.now().strftime('%H:%M:%S')}*")

        return "\n".join(lines)

    @staticmethod
    def risk_alert_template(risk: Dict) -> str:
        """
        风控告警模板

        Args:
            risk: {
                "code": "002594",
                "name": "比亚迪",
                "risk_level": "高",
                "volatility": 0.45,
                "position_advice": "轻仓",
                "stop_loss": 210.5,
                "warnings": ["波动率过高", "RSI 超买"],
            }

        Returns:
            Markdown 格式风控告警文本
        """
        level_icon = {"高": "🔴", "中": "🟡", "低": "🟢"}.get(
            risk.get("risk_level", "中"), "⚪"
        )
        lines = [
            f"## ⚠️ {level_icon} 风控告警 | {risk.get('name', '')} ({risk.get('code', '')})",
            "",
            f"- 风险等级: **{risk.get('risk_level', '中')}**",
        ]

        vol = risk.get("volatility")
        if vol is not None:
            lines.append(f"- 波动率: {vol:.1%}")

        position = risk.get("position_advice")
        if position:
            lines.append(f"- 建议仓位: **{position}**")

        stop_loss = risk.get("stop_loss")
        if stop_loss is not None:
            lines.append(f"- 止损价: {stop_loss:.2f}")

        entry = risk.get("entry_price")
        if entry is not None:
            lines.append(f"- 入场价: {entry:.2f}")

        warnings = risk.get("warnings", [])
        if warnings:
            lines.append("")
            lines.append("**风险因子:**")
            for w in warnings:
                lines.append(f"- {w}")

        lines.append("")
        lines.append(f"*⏰ {datetime.now().strftime('%H:%M:%S')}*")

        return "\n".join(lines)


# ═══════════════════════════════════════════
# 独立测试
# ═══════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("📣 推送模块测试")
    print("=" * 60)

    # 测试模板
    print("\n--- 盘前早报模板 ---")
    morning_data = {
        "date": "2024-01-15",
        "market_summary": "上证指数 **2890.45** (-0.42%)\n深证成指 **9054.08** (-0.80%)\n创业板指 **1775.58** (-1.76%)",
        "signals": [
            {"code": "300750", "name": "宁德时代", "level": "S",
             "suggestion": "关注", "confidence": 0.85},
            {"code": "600519", "name": "贵州茅台", "level": "A",
             "suggestion": "持有", "confidence": 0.72},
        ],
        "hot_events": [
            {"stock_code": "300750", "stock_name": "宁德时代",
             "event_type": "中标合同", "impact": 0.02, "direction": "利好"},
        ],
        "risk_warnings": ["大盘整体情绪偏弱，注意控制仓位"],
    }
    print(Notifier.morning_report_template(morning_data))

    print("\n--- 信号告警模板 ---")
    signal_data = {
        "code": "300750",
        "name": "宁德时代",
        "level": "S",
        "suggestion": "强烈关注",
        "confidence": 0.85,
        "sentiment": 0.72,
        "price": 245.5,
        "change_pct": 3.2,
        "reasons": [
            "碳酸锂价格暴跌 → 电池成本降低",
            "公司产能扩张中 → 受益于成本下降",
            "板块热度上升 → 资金关注度高",
        ],
    }
    print(Notifier.signal_alert_template(signal_data))

    print("\n--- 风控告警模板 ---")
    risk_data = {
        "code": "002594",
        "name": "比亚迪",
        "risk_level": "高",
        "volatility": 0.45,
        "position_advice": "轻仓",
        "stop_loss": 210.5,
        "entry_price": 235.0,
        "warnings": ["年化波动率达45%，显著高于市场平均"],
    }
    print(Notifier.risk_alert_template(risk_data))

    # 测试微信推送
    print("\n--- 微信推送测试 ---")
    Notifier.push_wechat(Notifier.morning_report_template(morning_data))

    print("\n✅ 推送模块测试完成")
