import ray
import ray.data as rd

CSV_PATH = "/root/bigdata/dataset/subset_300mb.csv"

# 需要标准化的列
NUM_COLS = ["trip_distance", "passenger_count", "fare_amount"]

def main():
    # ----------------------
    # 1. 连接 Ray 集群
    # ----------------------
    ray.init(address="auto")
    print("Connected to Ray cluster.")

    # ----------------------
    # 2. 读取 CSV（自动分块）
    # ----------------------
    ds = rd.read_csv(CSV_PATH)

    # ----------------------
    # 3. 计算全局 mean / std
    # ----------------------
    stats = {}
    for col in NUM_COLS:
        mean = ds.mean(col)
        std = ds.std(col)
        stats[col] = (mean, std)
        print(f"{col}: mean={mean}, std={std}")

    # ----------------------
    # 4. 标准化（行级 map）
    # ----------------------
    def standard_scale(df):
        for col, (mean, std) in stats.items():
            df[col] = (df[col] - mean) / std
        return df

    ds_scaled = ds.map_batches(
        standard_scale,
        batch_format="pandas"
    )

    # ----------------------
    # 5. 查看样例
    # ----------------------
    print("\nSample rows after scaling:")
    for row in ds_scaled.take(5):
        print(row)

    # ----------------------
    # 6. 验证标准化结果
    # ----------------------
    print("\nCheck scaled dataset:")
    for col in NUM_COLS:
        print(
            f"{col}: mean={ds_scaled.mean(col):.4f}, "
            f"std={ds_scaled.std(col):.4f}"
        )

    # ----------------------
    # 7. 关闭 Ray
    # ----------------------
    ray.shutdown()
    print("Ray shutdown complete.")

if __name__ == "__main__":
    main()

