#!/bin/bash

LOG_DIR=~/perf_logs
mkdir -p $LOG_DIR

# 清空旧日志（truncate，不删除文件）
: > "$LOG_DIR/cpu.log"
: > "$LOG_DIR/mem.log"
: > "$LOG_DIR/io.log"

pkill -f mpstat
pkill -f cpu.log

# CPU
#nohup bash -c '
#while true; do
#  echo -n "$(date +%s) " >> '"$LOG_DIR"'/cpu.log
#  mpstat 1 1 | awk "/all/ {print 100-$NF}" >> '"$LOG_DIR"'/cpu.log
#done
#' >/dev/null 2>&1 &
# CPU
nohup bash -c '
while true; do
  ts=$(date +%s)
  mpstat 1 1 | awk -v ts="$ts" "/^Average:/ {print ts, 100-\$NF}" >> "'"$LOG_DIR"'/cpu.log"
done
' >/dev/null 2>&1 &


echo $! > $LOG_DIR/cpu.pid

# Memory
nohup bash -c '
while true; do
  echo -n "$(date +%s) " >> '"$LOG_DIR"'/mem.log
  free -m | awk "/Mem:/ {print \$3, \$7}" >> '"$LOG_DIR"'/mem.log
  sleep 1
done
' >/dev/null 2>&1 &

echo $! > $LOG_DIR/mem.pid

# IO
nohup bash -c '
iostat -dx 1 >> '"$LOG_DIR"'/io.log
' >/dev/null 2>&1 &

echo $! > $LOG_DIR/io.pid

echo "Started local performance collection"

