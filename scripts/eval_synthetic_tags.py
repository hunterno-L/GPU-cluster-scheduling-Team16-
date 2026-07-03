#!/usr/bin/env python3
"""按合成集场景标签汇总评测。"""
from __future__ import annotations

import argparse
import glob
import re
import subprocess
import time
from collections import defaultdict
from pathlib import Path

from evaluate import parse_instance, parse_output, validate_records
from evaluate_metrics import compute_metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", default="build/execname_new.exe")
    parser.add_argument("--synthetic", default="../02-数据集/synthetic")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    exe = Path(args.exe)
    syn_dir = Path(args.synthetic)
    buckets: dict = defaultdict(lambda: {"v": 0, "n": 0, "ww": 0.0, "rt": 0.0})

    for case_file in sorted(glob.glob(str(syn_dir / "synth*.in"))):
        name = Path(case_file).name
        m = re.search(r"synth\d+_(.+)\.in", name)
        tag = m.group(1) if m else "other"
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(
                [str(exe)],
                input=Path(case_file).read_bytes(),
                capture_output=True,
                timeout=args.timeout,
                check=False,
            )
            rt = time.perf_counter() - t0
        except subprocess.TimeoutExpired:
            rt = args.timeout
            proc = None

        buckets[tag]["n"] += 1
        buckets[tag]["rt"] = max(buckets[tag]["rt"], rt)
        if proc is None or proc.returncode != 0:
            continue

        ok_fmt, records, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        if not ok_fmt:
            continue
        servers, jobs = parse_instance(Path(case_file))
        ok_val, _ = validate_records(records, servers, jobs)
        if not ok_val:
            continue
        ww, _, _ = compute_metrics(records, servers, jobs)
        buckets[tag]["v"] += 1
        buckets[tag]["ww"] += ww

    rows = []
    for tag, d in buckets.items():
        avg = d["ww"] / d["v"] if d["v"] else float("inf")
        rows.append((avg, d["v"], d["n"], d["rt"], tag))
    rows.sort(reverse=True)

    print(f"{'tag':22s} {'valid':>9s} {'avg_ww':>16s} {'max_rt':>8s}")
    for avg, v, n, rt, tag in rows:
        print(f"{tag:22s} {v:3d}/{n:<3d} {avg:16.0f} {rt:7.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
