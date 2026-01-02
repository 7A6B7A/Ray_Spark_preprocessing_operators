#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class CpuTimes:
    total: int
    idle: int


def _read_first_line(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.readline().strip()


def read_cpu_times() -> CpuTimes:
    # /proc/stat: cpu  user nice system idle iowait irq softirq steal guest guest_nice
    line = _read_first_line("/proc/stat")
    parts = line.split()
    if len(parts) < 5 or parts[0] != "cpu":
        raise RuntimeError(f"Unexpected /proc/stat format: {line}")
    values = [int(x) for x in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)  # idle + iowait
    total = sum(values)
    return CpuTimes(total=total, idle=idle)


def cpu_util_percent(prev: CpuTimes, cur: CpuTimes) -> float:
    total_delta = cur.total - prev.total
    idle_delta = cur.idle - prev.idle
    if total_delta <= 0:
        return 0.0
    util = 100.0 * (1.0 - (idle_delta / total_delta))
    if util < 0:
        return 0.0
    if util > 100:
        return 100.0
    return util


def read_mem_used_mb() -> Tuple[float, float]:
    # Prefer MemAvailable if present.
    meminfo: Dict[str, int] = {}
    with open("/proc/meminfo", "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.split(":")
            if len(parts) != 2:
                continue
            key = parts[0].strip()
            rest = parts[1].strip().split()
            if not rest:
                continue
            try:
                meminfo[key] = int(rest[0])  # kB
            except ValueError:
                continue

    total_kb = meminfo.get("MemTotal")
    avail_kb = meminfo.get("MemAvailable")
    if total_kb is None:
        raise RuntimeError("MemTotal missing in /proc/meminfo")

    if avail_kb is not None:
        used_kb = max(0, total_kb - avail_kb)
    else:
        # Fallback: used = total - free - buffers - cached
        free_kb = meminfo.get("MemFree", 0)
        buffers_kb = meminfo.get("Buffers", 0)
        cached_kb = meminfo.get("Cached", 0)
        used_kb = max(0, total_kb - free_kb - buffers_kb - cached_kb)

    return used_kb / 1024.0, total_kb / 1024.0


def list_block_devices() -> List[str]:
    # Read /proc/diskstats and pick likely “real” disks.
    devs: List[str] = []
    with open("/proc/diskstats", "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            name = parts[2]
            # Skip partitions (sda1, nvme0n1p1) by ignoring names that end with digits after base?
            # We instead whitelist base disk patterns.
            if name.startswith("loop") or name.startswith("ram"):
                continue
            if name.startswith("sd") and name[-1].isalpha():
                devs.append(name)
            elif name.startswith("vd") and name[-1].isalpha():
                devs.append(name)
            elif name.startswith("xvd") and name[-1].isalpha():
                devs.append(name)
            elif name.startswith("nvme") and "p" not in name:
                # nvme0n1 is disk; nvme0n1p1 is partition
                devs.append(name)
    # De-dup while keeping order
    seen = set()
    out: List[str] = []
    for d in devs:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def read_diskstats_bytes(devices: List[str]) -> Tuple[int, int]:
    # Return total read_bytes, write_bytes across selected devices.
    # diskstats sectors are 512-byte sectors.
    read_sectors = 0
    write_sectors = 0
    if not devices:
        return 0, 0
    devset = set(devices)
    with open("/proc/diskstats", "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            name = parts[2]
            if name not in devset:
                continue
            # Fields: ... reads completed, reads merged, sectors read, time reading,
            # writes completed, writes merged, sectors written, time writing, ...
            try:
                read_sectors += int(parts[5])
                write_sectors += int(parts[9])
            except ValueError:
                continue
    return read_sectors * 512, write_sectors * 512


_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True


def main() -> int:
    parser = argparse.ArgumentParser(description="Node monitor: sample CPU/mem/disk I/O once per interval and write CSV")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval seconds (default: 1)")
    parser.add_argument("--out", required=True, help="Output CSV path")
    parser.add_argument("--tag", default="", help="Tag string to include in each row")
    parser.add_argument("--stop-file", default="", help="If set, stop when this file exists")
    parser.add_argument("--hostname", default="", help="Override hostname field")
    parser.add_argument("--devices", default="", help="Comma-separated block devices to sum, default: auto-detect")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    hostname = args.hostname or os.uname().nodename
    devices = [d for d in args.devices.split(",") if d] if args.devices else list_block_devices()

    sys.stderr.write(
        f"node_monitor starting: hostname={hostname} interval={args.interval}s out={args.out} "
        f"tag={args.tag} devices={','.join(devices) if devices else '(none)'}\n"
    )
    sys.stderr.flush()

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    prev_cpu = read_cpu_times()
    prev_rb, prev_wb = read_diskstats_bytes(devices)
    prev_t = time.time()

    fieldnames = [
        "ts",
        "hostname",
        "tag",
        "cpu_util_pct",
        "mem_used_mb",
        "mem_total_mb",
        "disk_read_kbps",
        "disk_write_kbps",
        "devices",
    ]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        # initial sleep to get deltas
        while True:
            if _stop:
                break
            if args.stop_file and os.path.exists(args.stop_file):
                break
            time.sleep(max(0.05, args.interval))

            cur_t = time.time()
            cur_cpu = read_cpu_times()
            cur_rb, cur_wb = read_diskstats_bytes(devices)

            dt = max(1e-6, cur_t - prev_t)
            cpu_pct = cpu_util_percent(prev_cpu, cur_cpu)
            mem_used_mb, mem_total_mb = read_mem_used_mb()
            read_kbps = (cur_rb - prev_rb) / 1024.0 / dt
            write_kbps = (cur_wb - prev_wb) / 1024.0 / dt

            w.writerow(
                {
                    "ts": f"{cur_t:.3f}",
                    "hostname": hostname,
                    "tag": args.tag,
                    "cpu_util_pct": f"{cpu_pct:.3f}",
                    "mem_used_mb": f"{mem_used_mb:.3f}",
                    "mem_total_mb": f"{mem_total_mb:.3f}",
                    "disk_read_kbps": f"{read_kbps:.3f}",
                    "disk_write_kbps": f"{write_kbps:.3f}",
                    "devices": ";".join(devices),
                }
            )
            f.flush()

            prev_t = cur_t
            prev_cpu = cur_cpu
            prev_rb, prev_wb = cur_rb, cur_wb

            sys.stderr.write("node_monitor stopping\n")
            sys.stderr.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
