#!/usr/bin/env python3
"""按规模分桶 + 调参集/锁定集/合成集对比评测。"""
from __future__ import annotations

import argparse
import glob
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Tuple

from evaluate import parse_instance, parse_output, validate_records
from evaluate_metrics import compute_metrics


def bucket(job_count: int) -> str:
    if job_count <= 30:
        return "small"
    if job_count <= 100:
        return "medium"
    if job_count <= 500:
        return "large"
    return "xlarge"


def eval_case_list(exe: Path, case_files: List[str], timeout: float) -> Tuple[dict, List[dict], dict]:
    totals = {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0, "max_rt": 0.0}
    buckets: Dict[str, dict] = {}
    rows: List[dict] = []

    for case_file in case_files:
        name = Path(case_file).name
        servers, jobs = parse_instance(Path(case_file))
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(
                [str(exe)],
                input=Path(case_file).read_bytes(),
                capture_output=True,
                timeout=timeout,
                check=False,
            )
            runtime = time.perf_counter() - t0
        except subprocess.TimeoutExpired:
            runtime = timeout
            proc = None

        totals["total"] += 1
        totals["max_rt"] = max(totals["max_rt"], runtime)

        if proc is None or proc.returncode != 0:
            rows.append({"case": name, "valid": 0, "bucket": bucket(len(jobs)), "runtime": runtime})
            continue

        ok_fmt, records, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        ok_val, _ = validate_records(records, servers, jobs) if ok_fmt else (False, "")
        if not ok_fmt or not ok_val:
            rows.append({"case": name, "valid": 0, "bucket": bucket(len(jobs)), "runtime": runtime})
            continue

        ww, vi, mk = compute_metrics(records, servers, jobs)
        totals["valid"] += 1
        totals["ww"] += ww
        totals["vram"] += vi
        totals["mk"] += mk

        b = bucket(len(jobs))
        buckets.setdefault(b, {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0})
        buckets[b]["total"] += 1
        buckets[b]["valid"] += 1
        buckets[b]["ww"] += ww
        buckets[b]["vram"] += vi
        buckets[b]["mk"] += mk

        rows.append({"case": name, "valid": 1, "bucket": b, "ww": ww, "vram": vi, "mk": mk, "runtime": runtime})

    return totals, rows, buckets


def eval_dataset(exe: Path, dataset: Path, timeout: float, pattern: str) -> Tuple[dict, List[dict], dict]:
    cases = sorted(glob.glob(str(dataset / pattern)))
    return eval_case_list(exe, cases, timeout)


def summarize(label: str, totals: dict, buckets: dict) -> None:
    n = totals["total"]
    v = totals["valid"]
    print(f"\n=== {label} ===")
    print(f"valid_rate: {v}/{n}")
    if v:
        print(f"avg_weighted_wait: {totals['ww']/v:.2f}")
        print(f"avg_vram_idle: {totals['vram']/v:.2f}")
        print(f"avg_makespan: {totals['mk']/v:.2f}")
    print(f"max_runtime_s: {totals['max_rt']:.3f}")
    for b in ("small", "medium", "large", "xlarge"):
        if b not in buckets:
            continue
        bb = buckets[b]
        if bb["valid"]:
            print(
                f"  [{b}] {bb['valid']}/{bb['total']} "
                f"ww={bb['ww']/bb['valid']:.1f} vram={bb['vram']/bb['valid']:.2f} mk={bb['mk']/bb['valid']:.1f}"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", default="build/execname.exe")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--official", default="../02-数据集")
    parser.add_argument("--synthetic", default="../02-数据集/synthetic")
    parser.add_argument("--locked-from", type=int, default=81)
    args = parser.parse_args()

    exe = Path(args.exe)
    official_dir = Path(args.official)
    syn_dir = Path(args.synthetic)

    all_off = sorted(glob.glob(str(official_dir / "case*.in")))
    tune = [f for f in all_off if int(Path(f).stem.replace("case", "")) < args.locked_from]
    locked = [f for f in all_off if int(Path(f).stem.replace("case", "")) >= args.locked_from]

    empty = {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0, "max_rt": 0.0}
    tune_totals, _, tune_buckets = eval_case_list(exe, tune, args.timeout) if tune else (empty, [], {})
    locked_totals, _, locked_buckets = eval_case_list(exe, locked, args.timeout) if locked else (empty, [], {})
    off_totals, _, off_buckets = eval_case_list(exe, all_off, args.timeout)
    syn_totals, syn_rows, syn_buckets = eval_dataset(exe, syn_dir, args.timeout, "synth*.in")

    summarize(f"Tune (case001-{args.locked_from - 1:03d})", tune_totals, tune_buckets)
    summarize(f"Locked (case{args.locked_from:03d}-100)", locked_totals, locked_buckets)
    summarize("Official (all)", off_totals, off_buckets)
    summarize("Synthetic", syn_totals, syn_buckets)

    invalid = [r["case"] for r in syn_rows if not r["valid"]]
    if invalid:
        print(f"\nInvalid synthetic cases ({len(invalid)}): {', '.join(invalid[:10])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
