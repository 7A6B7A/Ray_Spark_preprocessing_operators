#!/bin/bash

WORKERS=("worker1" "worker2" "worker3")

echo "Starting collection on master..."
./start_collect_local.sh

for w in "${WORKERS[@]}"; do
  echo "Starting collection on $w ..."
  ssh $w "mkdir -p ~/perf_logs"
  scp start_collect_local.sh stop_collect_local.sh $w:~/
  ssh $w "chmod +x ~/start_collect_local.sh ~/stop_collect_local.sh"
  ssh $w "~/start_collect_local.sh"
done

echo "All nodes started collecting performance data"

