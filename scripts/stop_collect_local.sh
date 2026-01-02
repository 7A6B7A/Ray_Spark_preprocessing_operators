#!/bin/bash

LOG_DIR=~/perf_logs

for f in cpu.pid mem.pid io.pid; do
  if [ -f $LOG_DIR/$f ]; then
    kill $(cat $LOG_DIR/$f) 2>/dev/null
    rm $LOG_DIR/$f
  fi
done

echo "Stopped local performance collection"

