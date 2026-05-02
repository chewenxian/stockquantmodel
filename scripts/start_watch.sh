#!/bin/bash
# 启动实时事件监控守护进程
# 重大消息秒级推送至 QQ + 飞书

source ~/.zshrc
cd ~/.openclaw/workspace/stockquantmodel
mkdir -p logs

# 杀死已有实例
PID_FILE="/tmp/stock_watch.pid"
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    kill "$OLD_PID" 2>/dev/null
    sleep 1
fi

nohup python3 main.py watch 10 >> logs/watch_daemon.log 2>&1 &
echo $! > "$PID_FILE"
echo "✅ 实时监控已启动 (PID: $(cat $PID_FILE))"
