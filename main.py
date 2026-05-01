#!/usr/bin/env python3
"""
📈 股票新闻情报收集分析系统 - 主入口

使用方式:
    python main.py collect       # 执行一次全量采集
    python main.py quick         # 快速采集（仅新闻+行情）
    python main.py init          # 初始化数据库和股票池
    python main.py schedule      # 启动定时任务模式
    python main.py stats         # 查看采集统计
    python main.py analyze       # 分析所有自选股
    python main.py analyze 600519  # 分析指定股票
    python main.py analyze 600519 3  # 分析指定股票（近3天）
    python main.py report        # 生成收盘晚报
    python main.py report morning  # 生成盘前早报
"""
import sys
import os
import logging

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/stock_quant.log", encoding="utf-8"),
        ]
    )


def cmd_init():
    """初始化：加载自选股列表到数据库"""
    from storage.database import Database
    import csv

    db = Database("data/stock_news.db")
    count = 0

    if not os.path.exists("stocks.csv"):
        print("❌ stocks.csv 不存在，请先创建")
        return

    with open("stocks.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            db.upsert_stock(
                code=row["code"].strip(),
                name=row["name"].strip(),
                market=row.get("market", "SH").strip(),
                reason=row.get("reason", "").strip(),
                industry=row.get("industry", "").strip(),
            )
            count += 1

    print(f"✅ 已初始化 {count} 只自选股到数据库")


def cmd_collect():
    """执行一次全量采集"""
    from collector.scheduler import CollectScheduler

    print("🔄 开始全量数据采集...")
    scheduler = CollectScheduler()
    results = scheduler.collect_all()

    print(f"\n📊 采集结果汇总:")
    for name, res in results.items():
        if isinstance(res, dict):
            items = ", ".join(f"{k}={v}" for k, v in res.items())
            print(f"  ✅ {name}: {items}")
        else:
            print(f"  ❌ {name}: {res}")


def cmd_quick():
    """快速采集"""
    from collector.scheduler import CollectScheduler

    print("🔄 快速采集（新闻+行情+资金流向）...")
    scheduler = CollectScheduler()
    results = scheduler.quick_collect()
    print(f"✅ 快速采集完成: {results}")


def cmd_stats():
    """查看统计"""
    from collector.scheduler import CollectScheduler

    scheduler = CollectScheduler()
    stats = scheduler.get_stats()
    print(f"\n📊 系统状态:")
    print(f"  自选股: {stats['stock_count']} 只")
    print(f"  今日新闻: {stats['total_news']} 条")
    print(f"  数据源: {', '.join(stats['collectors'])}")


def cmd_analyze():
    """执行个股分析"""
    from analyzer.stock_analyzer import StockAnalyzer

    code = sys.argv[2] if len(sys.argv) > 2 else None
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    analyzer = StockAnalyzer()

    if code:
        print(f"🔄 开始分析 {code} (近{days}天)...")
        result = analyzer.analyze_stock(code, days=days)
        print(f"\n📊 {result['name']} ({result['code']})")
        print(f"  情绪: {result['avg_sentiment']:.2f}")
        print(f"  建议: {result['suggestion']} (置信度 {result['confidence']:.0%})")
        print(f"  风险: {result['risk_level']}")
        print(f"  分析: {result['summary'][:200]}")
    else:
        print("🔄 开始分析所有自选股...")
        results = analyzer.analyze_all_stocks()
        print(f"\n📊 分析完成: {len(results)} 只股票")
        for r in results:
            print(f"  {r['name']} ({r['code']}): 情绪={r['avg_sentiment']:.2f}, "
                  f"建议={r['suggestion']}, 置信度={r['confidence']:.0%}")


def cmd_report():
    """生成日报"""
    from analyzer.report_generator import ReportGenerator

    report_type = sys.argv[2] if len(sys.argv) > 2 else "closing"

    gen = ReportGenerator()

    if not gen.is_trading_day():
        print("⚠️ 非交易日，跳过报告生成")
        return

    if report_type == "morning":
        print("🌅 生成盘前早报...")
        report = gen.generate_morning_report()
    elif report_type == "closing":
        print("📊 生成收盘晚报...")
        report = gen.generate_closing_report()
    else:
        print(f"未知报告类型: {report_type}，使用收盘晚报")
        report = gen.generate_closing_report()

    if report:
        filepath = gen.save_report(report, report_type)
        print(f"✅ 报告已生成")
        if filepath:
            print(f"📁 已保存到: {filepath}")
        print(f"\n{report[:500]}..." if len(report) > 500 else f"\n{report}")
    else:
        print("❌ 报告生成失败")


def cmd_schedule():
    """定时采集模式"""
    import schedule as sch
    from collector.scheduler import CollectScheduler

    scheduler = CollectScheduler()
    intervals = scheduler.config.get("collector", {}).get("intervals", {})

    print("⏰ 启动定时采集模式")
    print(f"  新闻: 每 {intervals.get('news', 30)} 分钟")
    print(f"  行情: 每 {intervals.get('market', 15)} 分钟")
    print(f"  公告: 每 {intervals.get('announcement', 60)} 分钟")

    # 启动快速采集
    sch.every(intervals.get("news", 30)).minutes.do(scheduler.quick_collect)

    # 注册收盘后的定时分析（交易日 15:30）
    sch.every().monday.at("15:30").do(scheduler.collect_all)
    sch.every().tuesday.at("15:30").do(scheduler.collect_all)
    sch.every().wednesday.at("15:30").do(scheduler.collect_all)
    sch.every().thursday.at("15:30").do(scheduler.collect_all)
    sch.every().friday.at("15:30").do(scheduler.collect_all)

    # 开盘前初始化
    sch.every().monday.at("09:00").do(cmd_init)
    sch.every().tuesday.at("09:00").do(cmd_init)
    sch.every().wednesday.at("09:00").do(cmd_init)
    sch.every().thursday.at("09:00").do(cmd_init)
    sch.every().friday.at("09:00").do(cmd_init)

    print("✅ 定时任务已注册，按 Ctrl+C 退出")

    try:
        while True:
            sch.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n👋 定时采集已停止")


if __name__ == "__main__":
    import time
    setup_logging()

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    commands = {
        "init": cmd_init,
        "collect": cmd_collect,
        "quick": cmd_quick,
        "stats": cmd_stats,
        "schedule": cmd_schedule,
        "analyze": cmd_analyze,
        "report": cmd_report,
    }

    if command in commands:
        commands[command]()
    else:
        print(f"未知命令: {command}")
        print(__doc__)
