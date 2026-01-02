#!/usr/bin/env bash
set -euo pipefail

# === Ray Runner Configuration (Cluster Already Started) ===
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PYTHON_BIN="${PYTHON_BIN:-/root/miniconda3/bin/python3}"
PYTHON_BIN="/root/miniconda3/envs/ray_env/bin/python3"

# 数据集配置
DATA_DIR="${DATA_DIR:-/root/bigdata/dataset}"
DATASETS=(
  # "quantile_subset_500kb.csv"
  # "quantile_subset_300mb.csv"
  "yellow_tripdata_2015-01_quantile.csv"
)

# 节点配置 (用于监控，不用于启动集群)
MASTER_NAME="${MASTER_NAME:-master}"
WORKERS=(worker1 worker2 worker3)

RUNS_DIR="${RUNS_DIR:-$DIR/runs}"
REMOTE_BASE="/tmp/ray_onehot_exp" # 监控日志在远程节点的存放路径
INTERVAL=1

# Ray Data 参数
BATCH_SIZE=4096

log() { echo "[$(date '+%F %T')] $*"; }
ssh_do() { ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$1" "${@:2}"; }
scp_to() { scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "$1" "$2:$3"; }
scp_from() { scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "$1:$2" "$3"; }

# --- Monitor Helpers (已恢复完整逻辑) ---

start_monitor_local() {
  local run_id="$1" run_dir="$2"
  local local_dir="$REMOTE_BASE/$run_id"
  mkdir -p "$local_dir"
  nohup "$PYTHON_BIN" "$DIR/node_monitor.py" \
    --interval "$INTERVAL" --out "$local_dir/metrics.csv" --tag "$run_id" \
    --stop-file "$local_dir/STOP" --hostname "$MASTER_NAME" \
    >"$local_dir/monitor.log" 2>&1 &
  echo $! >"$local_dir/monitor.pid"
  mkdir -p "$run_dir/metrics"
}

start_monitor_remote() {
  local host="$1" run_id="$2"
  ssh_do "$host" "mkdir -p '$REMOTE_BASE/$run_id'"
  # 分发监控脚本到 Worker
  scp_to "$DIR/node_monitor.py" "$host" "$REMOTE_BASE/$run_id/node_monitor.py"
  ssh_do "$host" "nohup '$PYTHON_BIN' '$REMOTE_BASE/$run_id/node_monitor.py' \
    --interval '$INTERVAL' --out '$REMOTE_BASE/$run_id/metrics.csv' \
    --tag '$run_id' --stop-file '$REMOTE_BASE/$run_id/STOP' --hostname '$host' \
     >'$REMOTE_BASE/$run_id/monitor.log' 2>&1 & echo \$! >'$REMOTE_BASE/$run_id/monitor.pid'"
}

stop_monitor_local() {
  local run_id="$1"
  local local_dir="$REMOTE_BASE/$run_id"
  mkdir -p "$local_dir" && touch "$local_dir/STOP" || true
  if [[ -f "$local_dir/monitor.pid" ]]; then
    local pid; pid="$(cat "$local_dir/monitor.pid" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]]; then kill "$pid" 2>/dev/null || true; fi
  fi
}

stop_monitor_remote() {
  local host="$1" run_id="$2"
  ssh_do "$host" "mkdir -p '$REMOTE_BASE/$run_id' && touch '$REMOTE_BASE/$run_id/STOP'" || true
  ssh_do "$host" "if [[ -f '$REMOTE_BASE/$run_id/monitor.pid' ]]; then kill \$(cat '$REMOTE_BASE/$run_id/monitor.pid') 2>/dev/null || true; fi" || true
}

collect_monitor_local() {
  local run_id="$1" run_dir="$2"
  local local_dir="$REMOTE_BASE/$run_id"
  [[ -f "$local_dir/metrics.csv" ]] && cp -f "$local_dir/metrics.csv" "$run_dir/metrics/metrics_${MASTER_NAME}.csv"
  [[ -f "$local_dir/monitor.log" ]] && cp -f "$local_dir/monitor.log" "$run_dir/metrics/monitor_${MASTER_NAME}.log"
}

collect_monitor_remote() {
  local host="$1" run_id="$2" run_dir="$3"
  mkdir -p "$run_dir/metrics"
  scp_from "$host" "$REMOTE_BASE/$run_id/metrics.csv" "$run_dir/metrics/metrics_${host}.csv" || true
  scp_from "$host" "$REMOTE_BASE/$run_id/monitor.log" "$run_dir/metrics/monitor_${host}.log" || true
}

# --- Execution ---

run_one_dataset() {
  local dataset_file="$1"
  local dataset_path="$DATA_DIR/$dataset_file"
  local ts="$(date '+%Y%m%d_%H%M%S')"
  local run_id="ray_${dataset_file%.*}_$ts"
  local run_dir="$RUNS_DIR/$run_id"
  
  mkdir -p "$run_dir"
  log "=== RUN $run_id (Ray) ==="

  # 1. Start Monitors
  log "Starting monitors..."
  start_monitor_local "$run_id" "$run_dir"
  for w in "${WORKERS[@]}"; do start_monitor_remote "$w" "$run_id"; done
  
  # Ensure cleanup on return
  local cleaned=0
  cleanup() {
    if [[ "$cleaned" == "1" ]]; then return; fi
    cleaned=1
    log "Stopping monitors..."
    stop_monitor_local "$run_id"
    for w in "${WORKERS[@]}"; do stop_monitor_remote "$w" "$run_id"; done
    sleep 1 || true
    log "Collecting metrics..."
    collect_monitor_local "$run_id" "$run_dir"
    for w in "${WORKERS[@]}"; do collect_monitor_remote "$w" "$run_id" "$run_dir"; done
  }
  trap cleanup RETURN

  # 2. Submit Ray Job
  # 注意：这里使用的是 ray_onehot_dynamic.py (两阶段安全版)
  # 如果你的脚本名叫 ray_onehot.py，请修改这里
  local py_script="$DIR/ray_onehot.py" 
  
  if [[ ! -f "$py_script" ]]; then
      # 容错：如果找不到 dynamic 版，尝试找普通版
      py_script="$DIR/ray_onehot.py"
  fi
  
  log "Running Ray Python Job: $py_script"
  
  set +e
  "$PYTHON_BIN" "$py_script" \
      --input "$dataset_path" \
      --run-id "$run_id" \
      --batch-size "$BATCH_SIZE" \
      > "$run_dir/driver.log" 2>&1
  local rc=$?
  set -e
  
  if [[ "$rc" != "0" ]]; then
      log "Ray job failed (rc=$rc). See $run_dir/driver.log"
  else
      log "Ray job finished successfully."
  fi

  # 3. Stop Monitors & Collect (Triggered by cleanup/trap)
  cleanup
  trap - RETURN
  
  # 4. Summarize
  log "Summarizing metrics..."
  "$PYTHON_BIN" "$DIR/summarize_metrics.py" \
    --log "$run_dir/driver.log" \
    --metrics "$run_dir/metrics" \
    --out "$run_dir/summary.json" \
    --print \
    | tee "$run_dir/summary.txt" >/dev/null
}

main() {
  mkdir -p "$RUNS_DIR"
  
  # 检查 Python 脚本是否存在
  if [[ ! -f "$DIR/ray_onehot_dynamic.py" && ! -f "$DIR/ray_onehot.py" ]]; then
      echo "ERROR: Could not find ray_onehot_dynamic.py or ray_onehot.py in $DIR"
      exit 1
  fi

  # 假设集群已启动，直接运行任务
  log "Assuming Ray cluster is already running on $MASTER_NAME (auto connect)."
  
  for d in "${DATASETS[@]}"; do
    run_one_dataset "$d"
  done
  
  log "All datasets processed."
}

main "$@"