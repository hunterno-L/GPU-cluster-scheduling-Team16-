#!/usr/bin/env python3
"""改前/改后回归对比：调参集、锁定集、合成集、分桶。"""
from __future__ import annotations

import argparse
import csv
import glob
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from evaluate import parse_instance, parse_output, validate_records
from evaluate_metrics import compute_metrics


def bucket(job_count: int) -> str:
    if job_count <= 30:
        return "small"
    if job_count <= 100:
        return "medium"
    return "large"


def case_number(name: str) -> Optional[int]:
    m = re.search(r"(?:case|synth)(\d+)", name)
    return int(m.group(1)) if m else None


def eval_cases(exe: Path, case_files: List[str], timeout: float) -> Tuple[dict, Dict[str, dict]]:
    totals = {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0, "max_rt": 0.0}
    buckets: Dict[str, dict] = {}

    for i, case_file in enumerate(case_files):
        if i and i % 25 == 0:
            print(f"  ... {i}/{len(case_files)}", flush=True)
        path = Path(case_file)
        servers, jobs = parse_instance(path)
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
        b = bucket(len(jobs))
        buckets.setdefault(b, {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0})
        buckets[b]["total"] += 1

        if proc is None or proc.returncode != 0:
            continue

        ok_fmt, records, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        ok_val, _ = validate_records(records, servers, jobs) if ok_fmt else (False, "")
        if not ok_fmt or not ok_val:
            continue

        ww, vi, mk = compute_metrics(records, servers, jobs)
        totals["valid"] += 1
        totals["ww"] += ww
        totals["vram"] += vi
        totals["mk"] += mk
        buckets[b]["valid"] += 1
        buckets[b]["ww"] += ww
        buckets[b]["vram"] += vi
        buckets[b]["mk"] += mk

    return totals, buckets


def avg(totals: dict) -> dict:
    v = totals["valid"]
    if v == 0:
        return {"ww": 0.0, "vram": 0.0, "mk": 0.0}
    return {"ww": totals["ww"] / v, "vram": totals["vram"] / v, "mk": totals["mk"] / v}


def collect_splits(official_dir: Path, synthetic_dir: Path, locked_from: int) -> Dict[str, List[str]]:
    official = sorted(glob.glob(str(official_dir / "case*.in")))
    synthetic = sorted(glob.glob(str(synthetic_dir / "synth*.in")))

    tune, locked = [], []
    for f in official:
        n = case_number(Path(f).name)
        if n is not None and n >= locked_from:
            locked.append(f)
        else:
            tune.append(f)

    return {
        "tune": tune,
        "locked": locked,
        "official_all": official,
        "synthetic": synthetic,
    }


def run_split(exe: Path, files: List[str], timeout: float) -> Tuple[dict, Dict[str, dict]]:
    if not files:
        return {"valid": 0, "total": 0, "ww": 0.0, "vram": 0.0, "mk": 0.0, "max_rt": 0.0}, {}
    return eval_cases(exe, files, timeout)


def fmt_delta(new: float, old: float, lower_better: bool = True) -> str:
    if old == 0:
        return "n/a"
    d = new - old
    pct = 100.0 * d / old
    if abs(d) < 1e-9:
        return "0.00 (0.00%) [same]"
    good = (d < 0) if lower_better else (d > 0)
    sign = "+" if d > 0 else ""
    tag = "better" if good else "worse"
    return f"{sign}{d:.2f} ({sign}{pct:.2f}%) [{tag}]"


def print_report(label: str, cur: dict, base: Optional[dict]) -> None:
    a = avg(cur)
    print(f"\n[{label}] valid {cur['valid']}/{cur['total']}  max_rt={cur['max_rt']:.3f}s")
    print(f"  ww={a['ww']:.2f}  vram={a['vram']:.2f}  mk={a['mk']:.1f}")
    if base is not None:
        b = avg(base)
        print(f"  Δww   {fmt_delta(a['ww'], b['ww'])}")
        print(f"  Δvram {fmt_delta(a['vram'], b['vram'])}")
        print(f"  Δmk   {fmt_delta(a['mk'], b['mk'])}")


def main() -> int:
    parser = argparse.ArgumentParser(description="调度器改前/改后回归对比")
    parser.add_argument("--exe", default="build/execname.exe")
    parser.add_argument("--baseline-exe", default=None, help="可选：改前可执行文件")
    parser.add_argument("--official", default="../02-数据集")
    parser.add_argument("--synthetic", default="../02-数据集/synthetic")
    parser.add_argument("--locked-from", type=int, default=81)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--csv", default=None, help="输出 CSV 路径")
    parser.add_argument("--skip-synthetic", action="store_true")
    args = parser.parse_args()

    exe = Path(args.exe)
    base_exe = Path(args.baseline_exe) if args.baseline_exe else None
    splits = collect_splits(Path(args.official), Path(args.synthetic), args.locked_from)

    split_names = ["tune", "locked", "official_all"]
    if not args.skip_synthetic:
        split_names.append("synthetic")

    print("=== Regression Compare ===")
    print(f"current:  {exe}")
    if base_exe:
        print(f"baseline: {base_exe}")

    results: dict = {}
    rows_for_csv = []
    for split in split_names:
        files = splits[split]
        print(f"Evaluating {split} ({len(files)} cases)...", flush=True)
        cur_totals, _ = run_split(exe, files, args.timeout)
        results[("cur", split)] = (cur_totals, {})
        base_totals = None
        if base_exe and base_exe.exists():
            base_totals, _ = run_split(base_exe, files, args.timeout)
            results[("base", split)] = (base_totals, {})
        print_report(split, cur_totals, base_totals)
        rows_for_csv.append({
            "split": split,
            "valid": f"{cur_totals['valid']}/{cur_totals['total']}",
            **{f"cur_{k}": v for k, v in avg(cur_totals).items()},
            "max_rt": cur_totals["max_rt"],
        })

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_for_csv[0].keys()))
            w.writeheader()
            w.writerows(rows_for_csv)
        print(f"\nCSV -> {out.resolve()}")

    # 通过准则提示
    cur_locked, _ = results[("cur", "locked")]
    if cur_locked["valid"] < cur_locked["total"]:
        print("\n[WARN] locked set not fully valid — consider reverting")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
