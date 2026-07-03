#!/usr/bin/env python3
"""快速对比若干桶的 avg_weighted_wait，用于 variant 筛选。"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from evaluate_metrics import compute_metrics
from evaluate import parse_instance, parse_output, validate_records

BUCKETS = [
    ("official", "case*.in", "../02-数据集"),
    ("long_jobs", "synth*_long_jobs.in", "../02-数据集/synthetic"),
    ("single_server", "synth*_single_server.in", "../02-数据集/synthetic"),
    ("burst_t0", "synth*_burst_t0.in", "../02-数据集/synthetic"),
    ("mem_bound", "synth*_mem_bound.in", "../02-数据集/synthetic"),
    ("locked", "case08[1-9].in", "../02-数据集"),
]


def eval_bucket(exe: Path, pattern: str, dataset: Path, timeout: float) -> tuple[int, int, float]:
    import glob

    files = sorted(glob.glob(str(dataset / pattern)))
    if pattern == "locked":
        files += sorted(glob.glob(str(dataset / "case09*.in")))
        files += sorted(glob.glob(str(dataset / "case100.in")))
    v = n = 0
    ww = 0.0
    for f in files:
        n += 1
        proc = subprocess.run(
            [str(exe)],
            input=Path(f).read_bytes(),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0:
            continue
        ok, recs, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        if not ok:
            continue
        servers, jobs = parse_instance(Path(f))
        ok2, _ = validate_records(recs, servers, jobs)
        if not ok2:
            continue
        w, _, _ = compute_metrics(recs, servers, jobs)
        v += 1
        ww += w
    return v, n, ww / v if v else float("inf")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--exe", required=True)
    p.add_argument("--label", default="")
    p.add_argument("--timeout", type=float, default=10.0)
    args = p.parse_args()
    exe = Path(args.exe)
    root = Path(__file__).resolve().parent.parent
    print(f"=== {args.label or exe.name} ===")
    total = 0.0
    cnt = 0
    for name, pat, ds in BUCKETS:
        v, n, avg = eval_bucket(exe, pat, root / ds, args.timeout)
        print(f"  {name:14s} {v}/{n}  ww={avg:.0f}")
        if v:
            total += avg
            cnt += 1
    if cnt:
        print(f"  {'MEAN6':14s}      ww={total/cnt:.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
