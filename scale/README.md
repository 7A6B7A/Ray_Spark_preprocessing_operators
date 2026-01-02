# Spark StandardScaler Experiments (4-node metrics)

Runs 3 experiments (one per dataset) with a Spark MLlib `StandardScaler` pipeline and collects per-node system metrics (master + 3 workers):

- Job execution time (s)
- CPU utilization (avg / peak)
- Memory used (avg / peak)
- Disk I/O read/write (KB/s avg / peak)
- Partition load balance stats (before/after scaling)

## Files

- `spark_standard_scaler_job.py`: Spark job (CSV -> VectorAssembler -> StandardScaler -> action) prints a `__RESULT_JSON__...` line.
- `node_monitor.py`: Lightweight Linux `/proc` sampler (CPU/mem/diskstats) -> CSV.
- `summarize_metrics.py`: Aggregates 4 node CSVs + parses Spark result JSON -> `summary.json` + console table.
- `run.sh`: One-click end-to-end runner for 3 datasets.

## Run

On master:

```bash
cd /root/spark_code_2
bash run.sh
```

Outputs:

- `runs/<run_id>/driver.log`
- `runs/<run_id>/metrics/metrics_<node>.csv`
- `runs/<run_id>/summary.json`
- `runs/<run_id>/summary.txt`

## Extra: time-only experiment (subset_300mb)

`run.sh` will also run an extra experiment (no node monitoring; time only) on `subset_300mb.csv` with 3/2/1 machines and write:

- `runs/time_only_subset_300mb_<ts>/time_only.csv`

Disable it:

```bash
export RUN_TIME_ONLY_EXP=0
bash run.sh
```

## Common overrides

Set these env vars before running `run.sh`:

- `DATA_DIR` (default `/root/bigdata/dataset`)
- `PYTHON_BIN` (default `/root/miniconda3/bin/python3`)
- `PYSPARK_DRIVER_PYTHON` / `PYSPARK_PYTHON` (default to `PYTHON_BIN`)
- `SPARK_SUBMIT_BIN` (default `spark-submit`)
- `SPARK_MASTER_OPT` (example: `--master spark://master:7077`)
- `SPARK_DEPLOY_MODE_OPT` (example: `--deploy-mode client`)
- `SPARK_SUBMIT_EXTRA_OPTS` (example: `--conf spark.executor.memory=4g`)
- `WITH_STD` (default `1`), `WITH_MEAN` (default `0`)
- `REPARTITION` (default `0`), `CACHE` (default `0`)
- `INTERVAL` sampling seconds (default `1`)

Example:

```bash
export SPARK_MASTER_OPT="--master spark://master:7077"
export SPARK_SUBMIT_EXTRA_OPTS="--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
bash run.sh
```
