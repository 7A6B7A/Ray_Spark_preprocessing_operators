#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import time
from typing import Dict, List, Tuple

from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_NUMERIC_COLS = [
    "passenger_count",
    "trip_distance",
    "fare_amount",
]


def compute_partition_balance(df, col_name: str, topn: int = 10) -> Dict:
    rdd = df.select(col_name).rdd

    def part_count(it):
        c = 0
        for _ in it:
            c += 1
        yield c

    counts: List[int] = rdd.mapPartitions(part_count).collect()
    n = len(counts)
    if n == 0:
        return {
            "num_partitions": 0,
            "counts": [],
            "min": 0,
            "max": 0,
            "mean": 0.0,
            "std": 0.0,
            "cv": 0.0,
            "max_over_min": None,
            "top_partitions": [],
        }

    mn = min(counts)
    mx = max(counts)
    mean = sum(counts) / float(n)
    var = sum((x - mean) ** 2 for x in counts) / float(n)
    std = var ** 0.5
    cv = (std / mean) if mean > 0 else 0.0
    max_over_min = (mx / mn) if mn > 0 else None

    indexed = list(enumerate(counts))
    indexed.sort(key=lambda x: x[1], reverse=True)
    top_partitions = [{"partition": i, "rows": c} for i, c in indexed[:topn]]

    return {
        "num_partitions": n,
        "min": int(mn),
        "max": int(mx),
        "mean": float(mean),
        "std": float(std),
        "cv": float(cv),
        "max_over_min": float(max_over_min) if max_over_min is not None else None,
        "top_partitions": top_partitions,
    }


def parse_args():
    p = argparse.ArgumentParser(description="StandardScaler experiment job")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--run-id", default="", help="Run id string")
    p.add_argument("--with-mean", action="store_true", help="StandardScaler(withMean=True)")
    p.add_argument("--with-std", action="store_true", help="StandardScaler(withStd=True); default False")
    p.add_argument("--numeric-cols", default=",".join(DEFAULT_NUMERIC_COLS), help="Comma-separated numeric columns")
    p.add_argument("--repartition", type=int, default=0, help="If >0, repartition before fit/transform")
    p.add_argument("--topn", type=int, default=10, help="Top N partitions to report")
    p.add_argument("--cache", action="store_true", help="Cache features/scaled before action")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    spark = (
        SparkSession.builder.appName(f"StandardScalerExp{('-' + args.run_id) if args.run_id else ''}")
        .getOrCreate()
    )

    input_path = args.input
    numeric_cols = [c.strip() for c in args.numeric_cols.split(",") if c.strip()]

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Ensure numeric columns exist and are doubles.
    existing = set(df.columns)
    cols_used: List[str] = [c for c in numeric_cols if c in existing]
    if not cols_used:
        raise RuntimeError(f"No numeric cols found in input. Wanted: {numeric_cols}. Got: {df.columns}")

    dfn = df
    for c in cols_used:
        dfn = dfn.withColumn(c, F.col(c).cast("double"))
    dfn = dfn.fillna(0.0, subset=cols_used)

    assembler = VectorAssembler(inputCols=cols_used, outputCol="features")
    features = assembler.transform(dfn).select("features")

    if args.repartition and args.repartition > 0:
        features = features.repartition(args.repartition)

    if args.cache:
        features = features.cache()
        features.count()

    # Partition balance before.
    balance_before = compute_partition_balance(features, "features", topn=args.topn)

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaledFeatures",
        withMean=bool(args.with_mean),
        withStd=bool(args.with_std),
    )

    t0 = time.time()
    model = scaler.fit(features)
    scaled = model.transform(features).select("scaledFeatures")

    if args.cache:
        scaled = scaled.cache()

    # Action to materialize.
    nrows = scaled.count()
    t1 = time.time()

    balance_after = compute_partition_balance(scaled, "scaledFeatures", topn=args.topn)

    result = {
        "run_id": args.run_id,
        "input": input_path,
        "input_size_bytes": os.path.getsize(input_path) if os.path.exists(input_path) else None,
        "rows": int(nrows),
        "standard_scaler": {
            "withMean": bool(args.with_mean),
            "withStd": bool(args.with_std),
            "numeric_cols": cols_used,
            "repartition": int(args.repartition),
            "cache": bool(args.cache),
        },
        "timing": {
            "job_seconds": float(t1 - t0),
        },
        "partition_balance": {
            "before": balance_before,
            "after": balance_after,
        },
    }

    # One-line marker for easy parsing.
    print("__RESULT_JSON__" + json.dumps(result, ensure_ascii=False))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
