#!/bin/bash

WORKERS=("worker1" "worker2" "worker3")
DEST=~/data
mkdir -p $DEST

echo "Stopping collection on master..."
./stop_collect_local.sh
mkdir -p $DEST/master
cp -r ~/perf_logs $DEST/master/

for w in "${WORKERS[@]}"; do
  echo "Stopping collection on $w ..."
  ssh $w "~/stop_collect_local.sh"
  mkdir -p $DEST/$w
  scp -r $w:~/perf_logs $DEST/$w/
done

echo "All data collected into $DEST/"

