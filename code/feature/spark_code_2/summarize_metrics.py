#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import math
import os
import re
import statistics
from typing import Dict, List, Optional, Tuple


def parse_result_json_from_log(log_path: str) -> Dict:
    if not os.path.exists(log_path):
        return {}
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("__RESULT_JSON__"):
                payload = line[len("__RESULT_JSON__") :]
                try:
                    return json.loads(payload)
                except json.JSONDecodeError:
                    return {}
    return {}


def read_metrics_csv(path: str) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append(
                    {
                        "cpu_util_pct": float(r.get("cpu_util_pct") or 0.0),
                        "mem_used_mb": float(r.get("mem_used_mb") or 0.0),
                        "disk_read_kbps": float(r.get("disk_read_kbps") or 0.0),
                        "disk_write_kbps": float(r.get("disk_write_kbps") or 0.0),
                    }
                )
            except ValueError:
                continue
    return rows


def summarize_series(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"avg": 0.0, "peak": 0.0}
    return {"avg": float(statistics.fmean(values)), "peak": float(max(values))}


def summarize_node(metrics_rows: List[Dict[str, float]]) -> Dict:
    cpu = summarize_series([r["cpu_util_pct"] for r in metrics_rows])
    mem = summarize_series([r["mem_used_mb"] for r in metrics_rows])
    rd = summarize_series([r["disk_read_kbps"] for r in metrics_rows])
    wr = summarize_series([r["disk_write_kbps"] for r in metrics_rows])
    return {
        "cpu_util_pct": cpu,
        "mem_used_mb": mem,
        "disk_read_kbps": rd,
        "disk_write_kbps": wr,
        "samples": len(metrics_rows),
    }


def avg_across_nodes(nodes: Dict[str, Dict]) -> Dict:
    # Average of per-node averages, and average of per-node peaks.
    def collect(path: Tuple[str, str]) -> List[float]:
        a, b = path
        out: List[float] = []
        for _, v in nodes.items():
            out.append(float(v.get(a, {}).get(b, 0.0)))
        return out

    def mk(a: str) -> Dict:
        avgs = collect((a, "avg"))
        peaks = collect((a, "peak"))
        return {
            "avg_of_nodes": float(statistics.fmean(avgs)) if avgs else 0.0,
            "peak_of_nodes_avg": float(statistics.fmean(peaks)) if peaks else 0.0,
            "max_peak": float(max(peaks)) if peaks else 0.0,
        }

    return {
        "cpu_util_pct": mk("cpu_util_pct"),
        "mem_used_mb": mk("mem_used_mb"),
        "disk_read_kbps": mk("disk_read_kbps"),
        "disk_write_kbps": mk("disk_write_kbps"),
        "num_nodes": len(nodes),
    }


def print_table(summary: Dict):
    nodes = summary.get("nodes", {})
    header = [
        "node",
        "cpu_avg%",
        "cpu_peak%",
        "mem_avgMB",
        "mem_peakMB",
        "rd_avgKB/s",
        "rd_peakKB/s",
        "wr_avgKB/s",
        "wr_peakKB/s",
        "samples",
    ]
    print("\t".join(header))
    for node, s in nodes.items():
        print(
            "\t".join(
                [
                    node,
                    f"{s['cpu_util_pct']['avg']:.2f}",
                    f"{s['cpu_util_pct']['peak']:.2f}",
                    f"{s['mem_used_mb']['avg']:.0f}",
                    f"{s['mem_used_mb']['peak']:.0f}",
                    f"{s['disk_read_kbps']['avg']:.1f}",
                    f"{s['disk_read_kbps']['peak']:.1f}",
                    f"{s['disk_write_kbps']['avg']:.1f}",
                    f"{s['disk_write_kbps']['peak']:.1f}",
                    str(s.get("samples", 0)),
                ]
            )
        )

    overall = summary.get("overall_avg", {})
    if overall:
        print("\nOVERALL(avg across nodes):")
        print(
            "cpu_avg%="
            f"{overall['cpu_util_pct']['avg_of_nodes']:.2f} "
            "cpu_peak_avg%="
            f"{overall['cpu_util_pct']['peak_of_nodes_avg']:.2f} "
            "cpu_max_peak%="
            f"{overall['cpu_util_pct']['max_peak']:.2f}"
        )
        print(
            "mem_avgMB="
            f"{overall['mem_used_mb']['avg_of_nodes']:.0f} "
            "mem_peak_avgMB="
            f"{overall['mem_used_mb']['peak_of_nodes_avg']:.0f} "
            "mem_max_peakMB="
            f"{overall['mem_used_mb']['max_peak']:.0f}"
        )
        print(
            "rd_avgKB/s="
            f"{overall['disk_read_kbps']['avg_of_nodes']:.1f} "
            "rd_peak_avgKB/s="
            f"{overall['disk_read_kbps']['peak_of_nodes_avg']:.1f} "
            "rd_max_peakKB/s="
            f"{overall['disk_read_kbps']['max_peak']:.1f}"
        )
        print(
            "wr_avgKB/s="
            f"{overall['disk_write_kbps']['avg_of_nodes']:.1f} "
            "wr_peak_avgKB/s="
            f"{overall['disk_write_kbps']['peak_of_nodes_avg']:.1f} "
            "wr_max_peakKB/s="
            f"{overall['disk_write_kbps']['max_peak']:.1f}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize node monitor CSVs and Spark job output")
    ap.add_argument("--log", required=True, help="Spark driver log file path")
    ap.add_argument("--metrics", required=True, help="Directory containing metrics_*.csv")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--print", action="store_true", help="Print tab-separated table")
    args = ap.parse_args()

    spark_result = parse_result_json_from_log(args.log)

    nodes: Dict[str, Dict] = {}
    if os.path.isdir(args.metrics):
        for name in sorted(os.listdir(args.metrics)):
            if not name.startswith("metrics_") or not name.endswith(".csv"):
                continue
            node = name[len("metrics_") : -len(".csv")]
            rows = read_metrics_csv(os.path.join(args.metrics, name))
            nodes[node] = summarize_node(rows)

    summary = {
        "spark": spark_result,
        "nodes": nodes,
        "overall_avg": avg_across_nodes(nodes) if nodes else {},
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if args.print:
        print_table(summary)
        if spark_result.get("timing"):
            print("\nSPARK:")
            print(f"job_seconds={spark_result['timing'].get('job_seconds')}")
        if spark_result.get("partition_balance"):
            b = spark_result["partition_balance"]
            print("partition_balance(before):", b.get("before"))
            print("partition_balance(after):", b.get("after"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
