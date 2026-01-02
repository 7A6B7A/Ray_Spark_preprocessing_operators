#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import time
import os
import ray
import numpy as np
import pandas as pd

# 默认配置
DEFAULT_TARGET_COLS = [
    "passenger_count",
    "trip_distance",
    "fare_amount"
]
MAX_CATEGORY_VALUE = 20  # 安全上限：假设最大值不超过20（即使你只有10）

def parse_args():
    p = argparse.ArgumentParser(description="Ray Data OneHot Logic")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--run-id", default="", help="Run id string")
    p.add_argument("--target-cols", default=",".join(DEFAULT_TARGET_COLS))
    # Ray 特有参数
    p.add_argument("--batch-size", type=int, default=4096, help="Batch size for processing")
    p.add_argument("--parallelism", type=int, default=-1, help="Number of blocks/tasks")
    return p.parse_args()

def one_hot_transform(batch: pd.DataFrame, target_cols: list) -> pd.DataFrame:
    """
    核心转换逻辑：无状态，纯 NumPy 操作。
    输入：Pandas DataFrame (一个 Batch)
    输出：包含 OneHot 向量列的 DataFrame
    """
    # 1. 填充空值 & 类型转换
    # 这里的操作只发生在当前 Batch，内存极度安全
    for col in target_cols:
        if col not in batch.columns:
            continue
            
        # 转换为 Int，填充 0
        # fillna(0) 是安全的，因为 0 在 OneHot 里的含义是 index=0 的位置
        vals = batch[col].fillna(0).astype(int).to_numpy()
        
        # 2. 截断保护 (防止脏数据导致 index out of bounds)
        # 如果数据里突然出现 999，我们把它 clamp 到 0 (或者 MAX)
        vals = np.clip(vals, 0, MAX_CATEGORY_VALUE)
        
        # 3. NumPy 极速 OneHot (Dense)
        # 生成形状为 (batch_size, MAX_CATEGORY_VALUE + 1)
        # np.eye 返回的是 float，内存也是连续的
        oh_matrix = np.eye(MAX_CATEGORY_VALUE + 1)[vals]
        
        # 4. 将结果存回 DataFrame
        # Ray Data 支持列里存 numpy array / tensor
        batch[f"{col}_vec"] = list(oh_matrix)
        
        # (可选) 为了省内存，可以 drop 掉原始列
        # del batch[col] 
        
    return batch

def main():
    args = parse_args()
    
    # 1. 连接 Ray 集群
    # address="auto" 会自动寻找本地的 Ray 节点 (由 run.sh 启动)
    ray.init(address="auto", ignore_reinit_error=True)
    
    target_cols = [c.strip() for c in args.target_cols.split(",") if c.strip()]
    
    t0 = time.time()
    
    # 2. 读取数据 (Lazy)
    # Ray 会自动按文件块划分 Partition
    ds = ray.data.read_csv(args.input)
    
    if args.parallelism > 0:
        ds = ds.repartition(args.parallelism)
        
    # 3. 应用转换 (Streaming)
    # map_batches 是 Ray 的核心。它不会把数据全拉到内存，而是流式通过。
    transformed_ds = ds.map_batches(
        one_hot_transform,
        fn_kwargs={"target_cols": target_cols},
        batch_format="pandas",
        batch_size=args.batch_size
    )
    
    # 4. 触发执行 (Action)
    # 不同于 Spark 的 count() 可能会优化掉 map，
    # 我们这里显式强制物化并计数，模拟真实的工作流。
    # Ray Data 2.x+ 默认是 Lazy 的。
    nrows = transformed_ds.count()
    
    t1 = time.time()
    
    # 5. 简单的统计 (可选，为了证明转换成功)
    # 取第一行看看结构
    # sample_row = transformed_ds.take(1)[0]
    # print("Sample output vec shape:", len(sample_row[f"{target_cols[0]}_vec"]))

    result = {
        "run_id": args.run_id,
        "rows": nrows,
        "framework": "ray_data",
        "strategy": "stateless_numeric_map",
        "config": {
            "batch_size": args.batch_size,
            "target_cols": target_cols
        },
        "timing": {
            "job_seconds": float(t1 - t0),
            "throughput_rows_sec": nrows / (t1 - t0) if t1 > t0 else 0
        }
    }
    
    print("__RESULT_JSON__" + json.dumps(result, ensure_ascii=False))
    
    ray.shutdown()

if __name__ == "__main__":
    main()