#!/usr/bin/env python3
"""一键回归：调参集 / 锁定集 / 官方全量 / 合成集。"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", default=str(root / "build" / "execname.exe"))
    parser.add_argument("--baseline-exe", default=None)
    parser.add_argument("--official", default=str(root.parent / "02-数据集"))
    parser.add_argument("--synthetic", default=str(root.parent / "02-数据集" / "synthetic"))
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--skip-synthetic", action="store_true")
    parser.add_argument("--csv", default=str(root / "results" / "regression_latest.csv"))
    args = parser.parse_args()

    cmd = [
        sys.executable,
        str(root / "scripts" / "regression_compare.py"),
        "--exe", args.exe,
        "--official", args.official,
        "--synthetic", args.synthetic,
        "--timeout", str(args.timeout),
        "--csv", args.csv,
    ]
    if args.baseline_exe:
        cmd.extend(["--baseline-exe", args.baseline_exe])
    if args.skip_synthetic:
        cmd.append("--skip-synthetic")

    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
