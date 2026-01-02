#!/usr/bin/env bash
set -euo pipefail

# One-click runner: 3 datasets x 1 OneHotEncoder job each, with 4-node monitoring.
# Assumptions:
# - Run on master node.
# - SSH works: worker1/worker2/worker3.
# - Spark/Python env already configured.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PYTHON_BIN="${PYTHON_BIN:-/root/miniconda3/bin/python3}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"

# Ensure PySpark driver/executor use the same Python minor version.
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYTHON_BIN}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PYTHON_BIN}"

# spark-submit args (can still be overridden via env vars)
SPARK_MASTER_OPT="${SPARK_MASTER_OPT:---master spark://172.25.128.53:7077}"
SPARK_EXECUTOR_MEMORY_OPT="${SPARK_EXECUTOR_MEMORY_OPT:---executor-memory 2048M}"
SPARK_TOTAL_EXECUTOR_CORES_OPT="${SPARK_TOTAL_EXECUTOR_CORES_OPT:---total-executor-cores 16}"
SPARK_EXTRA_CONF_OPT="${SPARK_EXTRA_CONF_OPT:-}"
SPARK_PYSPARK_CONF_OPT="${SPARK_PYSPARK_CONF_OPT:---conf spark.pyspark.driver.python=$PYSPARK_DRIVER_PYTHON --conf spark.pyspark.python=$PYSPARK_PYTHON}"
SPARK_DEPLOY_MODE_OPT="${SPARK_DEPLOY_MODE_OPT:-}" # e.g. "--deploy-mode client"
SPARK_SUBMIT_EXTRA_OPTS="${SPARK_SUBMIT_EXTRA_OPTS:-}" # optional extra args

DATA_DIR="${DATA_DIR:-/root/bigdata/dataset}"
DATASETS=(
  # "quantile_subset_500kb.csv"
  # "quantile_subset_300mb.csv"
  "yellow_tripdata_2015-01_quantile.csv"
)

MASTER_NAME="${MASTER_NAME:-master}"
WORKERS=(worker1 worker2 worker3)

REMOTE_BASE="${REMOTE_BASE:-/tmp/spark_onehot_exp}"
INTERVAL="${INTERVAL:-1}"

# OneHotEncoder options (Modified from StandardScaler)
# Set DROP_LAST=1 to enable --drop-last behavior
DROP_LAST="${DROP_LAST:-0}"
REPARTITION="${REPARTITION:-0}"  # 0 means no repartition
CACHE="${CACHE:-0}"              # 1 to cache features/encoded

RUNS_DIR="${RUNS_DIR:-$DIR/runs}"

log() { echo "[$(date '+%F %T')] $*"; }

die() { echo "ERROR: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"
}

ssh_do() {
  local host="$1"; shift
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$host" "$@"
}

scp_to() {
  local src="$1" host="$2" dst="$3"
  scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "$src" "$host:$dst"
}

scp_from() {
  local host="$1" src="$2" dst="$3"
  scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "$host:$src" "$dst"
}

start_monitor_local() {
  local run_id="$1" run_dir="$2"
  local local_dir="$REMOTE_BASE/$run_id"
  mkdir -p "$local_dir"
  nohup "$PYTHON_BIN" "$DIR/node_monitor.py" \
    --interval "$INTERVAL" \
    --out "$local_dir/metrics.csv" \
    --tag "$run_id" \
    --stop-file "$local_dir/STOP" \
    --hostname "$MASTER_NAME" \
    >"$local_dir/monitor.log" 2>&1 &
  echo $! >"$local_dir/monitor.pid"

  # Also stage a copy into run_dir early (in case of crash later)
  mkdir -p "$run_dir/metrics"
}

start_monitor_remote() {
  local host="$1" run_id="$2"
  ssh_do "$host" "mkdir -p '$REMOTE_BASE/$run_id'"
  scp_to "$DIR/node_monitor.py" "$host" "$REMOTE_BASE/$run_id/node_monitor.py"
  ssh_do "$host" "nohup '$PYTHON_BIN' '$REMOTE_BASE/$run_id/node_monitor.py' \
    --interval '$INTERVAL' \
    --out '$REMOTE_BASE/$run_id/metrics.csv' \
    --tag '$run_id' \
    --stop-file '$REMOTE_BASE/$run_id/STOP' \
    --hostname '$host' \
     >'$REMOTE_BASE/$run_id/monitor.log' 2>&1 & echo \$! >'$REMOTE_BASE/$run_id/monitor.pid'"
}

stop_monitor_local() {
  local run_id="$1"
  local local_dir="$REMOTE_BASE/$run_id"
  mkdir -p "$local_dir"
  touch "$local_dir/STOP" || true
  if [[ -f "$local_dir/monitor.pid" ]]; then
    local pid
    pid="$(cat "$local_dir/monitor.pid" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
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
  if [[ -f "$local_dir/metrics.csv" ]]; then
    cp -f "$local_dir/metrics.csv" "$run_dir/metrics/metrics_${MASTER_NAME}.csv"
  fi
  if [[ -f "$local_dir/monitor.log" ]]; then
    cp -f "$local_dir/monitor.log" "$run_dir/metrics/monitor_${MASTER_NAME}.log"
  fi
}

collect_monitor_remote() {
  local host="$1" run_id="$2" run_dir="$3"
  mkdir -p "$run_dir/metrics"
  scp_from "$host" "$REMOTE_BASE/$run_id/metrics.csv" "$run_dir/metrics/metrics_${host}.csv" || true
  scp_from "$host" "$REMOTE_BASE/$run_id/monitor.log" "$run_dir/metrics/monitor_${host}.log" || true
}

run_one_dataset() {
  local dataset_file="$1"
  local dataset_path="$DATA_DIR/$dataset_file"
  [[ -f "$dataset_path" ]] || die "Dataset not found: $dataset_path"

  local ts run_id run_dir
  ts="$(date '+%Y%m%d_%H%M%S')"
  run_id="${dataset_file%.*}_$ts"
  run_dir="$RUNS_DIR/$run_id"

  mkdir -p "$run_dir"
  log "=== RUN $run_id ==="

  # Start monitors
  log "Starting monitors (master + 3 workers)"
  start_monitor_local "$run_id" "$run_dir"
  for w in "${WORKERS[@]}"; do
    start_monitor_remote "$w" "$run_id"
  done

  # Ensure cleanup on exit of this function
  local cleaned=0
  cleanup() {
    if [[ "$cleaned" == "1" ]]; then return; fi
    cleaned=1
    log "Stopping monitors"
    stop_monitor_local "$run_id"
    for w in "${WORKERS[@]}"; do
      stop_monitor_remote "$w" "$run_id"
    done

    # Give monitors a moment to flush
    sleep 1 || true

    log "Collecting metrics CSVs"
    collect_monitor_local "$run_id" "$run_dir"
    for w in "${WORKERS[@]}"; do
      collect_monitor_remote "$w" "$run_id" "$run_dir"
    done
  }
  trap cleanup RETURN

  # Spark submit
  log "Submitting Spark job: $dataset_path"
  local job_args=("--input" "$dataset_path" "--run-id" "$run_id")
  
  # --- CHANGED: Adapted for OneHotEncoder parameters ---
  if [[ "$DROP_LAST" == "1" ]]; then job_args+=("--drop-last"); fi
  if [[ "$REPARTITION" != "0" ]]; then job_args+=("--repartition" "$REPARTITION"); fi
  if [[ "$CACHE" == "1" ]]; then job_args+=("--cache"); fi
  # -----------------------------------------------------

  # shellcheck disable=SC2086
  set +e
  "$SPARK_SUBMIT_BIN" \
    $SPARK_MASTER_OPT \
    $SPARK_EXECUTOR_MEMORY_OPT \
    $SPARK_TOTAL_EXECUTOR_CORES_OPT \
    $SPARK_EXTRA_CONF_OPT \
    $SPARK_PYSPARK_CONF_OPT \
    $SPARK_DEPLOY_MODE_OPT \
    $SPARK_SUBMIT_EXTRA_OPTS \
    "$DIR/spark_onehot.py" \
    "${job_args[@]}" \
    >"$run_dir/driver.log" 2>&1
  local rc=$?
  set -e

  if [[ "$rc" != "0" ]]; then
    log "Spark job failed (rc=$rc). See: $run_dir/driver.log"
  else
    log "Spark job finished"
  fi

  # Stop & collect monitors BEFORE summarizing (otherwise metrics files aren't present yet).
  cleanup

  # Summarize
  log "Summarizing metrics"
  "$PYTHON_BIN" "$DIR/summarize_metrics.py" \
    --log "$run_dir/driver.log" \
    --metrics "$run_dir/metrics" \
    --out "$run_dir/summary.json" \
    --print \
    | tee "$run_dir/summary.txt" >/dev/null

  # Cleanup already executed; remove trap.
  trap - RETURN

  return "$rc"
}

main() {
  require_cmd ssh
  require_cmd scp
  require_cmd "$PYTHON_BIN"
  require_cmd "$SPARK_SUBMIT_BIN"

  mkdir -p "$RUNS_DIR"

  log "Datasets dir: $DATA_DIR"
  log "Runs dir: $RUNS_DIR"
  log "Workers: ${WORKERS[*]}"

  local failures=0
  for d in "${DATASETS[@]}"; do
    if ! run_one_dataset "$d"; then
      failures=$((failures + 1))
    fi
  done

  log "All done. failures=$failures"
  log "Outputs under: $RUNS_DIR"

  if [[ "$failures" != "0" ]]; then
    exit 2
  fi
}

main "$@"