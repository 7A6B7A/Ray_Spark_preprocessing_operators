#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import time
from typing import Dict, List

from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

DEFAULT_TARGET_COLS = [
    "passenger_count",
    "trip_distance",
    "fare_amount"
]

def compute_partition_balance(df, col_name: str = None, topn: int = 10) -> Dict:
    rdd = df.rdd
    def part_count(it):
        c = 0
        for _ in it:
            c += 1
        yield c

    counts: List[int] = rdd.mapPartitions(part_count).collect()
    n = len(counts)
    if n == 0:
        return {"num_partitions": 0, "counts": [], "min": 0, "max": 0, "mean": 0.0, "std": 0.0, "cv": 0.0}

    mn = min(counts)
    mx = max(counts)
    mean = sum(counts) / float(n)
    var = sum((x - mean) ** 2 for x in counts) / float(n)
    std = var ** 0.5
    cv = (std / mean) if mean > 0 else 0.0
    
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
        "top_partitions": top_partitions,
    }

def parse_args():
    p = argparse.ArgumentParser(description="OneHotEncoder Numeric Optimized Job")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--run-id", default="", help="Run id string")
    p.add_argument("--drop-last", action="store_true", help="OneHotEncoder(dropLast=True)")
    p.add_argument("--target-cols", default=",".join(DEFAULT_TARGET_COLS), help="Comma-separated columns")
    p.add_argument("--repartition", type=int, default=0, help="If >0, repartition before fit/transform")
    p.add_argument("--topn", type=int, default=10, help="Top N partitions to report")
    p.add_argument("--cache", action="store_true", help="Cache features/encoded before action")
    return p.parse_args()

def main() -> int:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"OHE_Numeric_{args.run_id}")
        .getOrCreate()
    )

    input_path = args.input
    target_cols = [c.strip() for c in args.target_cols.split(",") if c.strip()]

    # 1. 读取时不推断 Schema，最快速度读入
    df = spark.read.option("header", True).option("inferSchema", False).csv(input_path)

    existing = set(df.columns)
    cols_used: List[str] = [c for c in target_cols if c in existing]
    
    if not cols_used:
        raise RuntimeError(f"No target cols found. Wanted: {target_cols}")

    # 2. 关键步骤：直接转换为整数 (Integer)
    # OneHotEncoder 只能处理 Numeric 类型。因为你已经是 1-10 了，直接转 Int 即可。
    dfn = df
    for c in cols_used:
        dfn = dfn.withColumn(c, F.col(c).cast("int"))

    # 填充空值为 0 (0 不会影响 1-10 的编码，只是占据 index 0，它是安全的)
    dfn = dfn.fillna(0, subset=cols_used)

    if args.repartition > 0:
        dfn = dfn.repartition(args.repartition)

    if args.cache:
        # 使用安全的缓存级别
        dfn = dfn.persist(StorageLevel.MEMORY_AND_DISK)
        dfn.count()

    balance_before = compute_partition_balance(dfn, topn=args.topn)

    # 3. 直接定义 OneHotEncoder
    # 输入列就是原列名，输出列加个后缀
    # 注意：不需要 StringIndexer 了！
    vec_cols = [c + "_vec" for c in cols_used]
    
    encoder = OneHotEncoder(
        inputCols=cols_used,   # 直接吃整数列
        outputCols=vec_cols,
        dropLast=bool(args.drop_last)
    )

    # 依然没有 VectorAssembler
    pipeline = Pipeline(stages=[encoder])

    t0 = time.time()
    
    # fit() 依然需要，它要扫描一遍数据确定每列的最大值 (Max Index)
    # 但这比 StringIndexer 构建字典快得多，也省内存得多
    model = pipeline.fit(dfn)
    encoded = model.transform(dfn)

    # 仅保留结果列
    # final_df = encoded.select(*vec_cols)

    # if args.cache:
        # final_df = final_df.persist(StorageLevel.MEMORY_AND_DISK)

    # nrows = final_df.count()
    t1 = time.time()

    balance_after = compute_partition_balance(final_df, topn=args.topn)

    result = {
        "run_id": args.run_id,
        "rows": int(nrows),
        "one_hot_encoder": {
            "mode": "NUMERIC_DIRECT_OPTIMIZED", # 标记这是直接数字编码模式
            "dropLast": bool(args.drop_last),
            "target_cols": cols_used,
            "repartition": int(args.repartition),
            "string_indexer_removed": True,
            "vector_assembler_removed": True
        },
        "timing": {
            "job_seconds": float(t1 - t0),
        },
        "partition_balance": {
            "before": balance_before,
            "after": balance_after,
        },
    }

    print("__RESULT_JSON__" + json.dumps(result, ensure_ascii=False))

    spark.stop()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())