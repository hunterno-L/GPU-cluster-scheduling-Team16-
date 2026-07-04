#!/usr/bin/env python3
"""Build and per-case compare four scheduler variants."""
from __future__ import annotations

import csv
import glob
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
VARIANTS = ROOT / "variants"
BUILD = ROOT / "build"
DATASET = ROOT.parent / "02-数据集"
SHARED = ["parser.cpp", "machine_state.cpp", "machine_state.h", "output.cpp", "models.h", "parser.h", "output.h"]

VARIANT_NAMES = ["schedule4.0", "develop", "end", "lab4.0"]


def compile_variant(name: str) -> Path:
    exe = BUILD / f"execname_{name.replace('.', '_')}.exe"
    staging = BUILD / f"staging_{name.replace('.', '_')}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    for f in SHARED:
        shutil.copy2(SRC / f, staging / f)
    vdir = VARIANTS / name / "src"
    main_src = vdir / "main.cpp"
    shutil.copy2(main_src if main_src.exists() else SRC / "main.cpp", staging / "main.cpp")
    shutil.copy2(vdir / "scheduler.cpp", staging / "scheduler.cpp")
    shutil.copy2(vdir / "scheduler.h", staging / "scheduler.h")
    cmd = [
        "g++", "-std=c++17", "-O2",
        str(staging / "main.cpp"),
        str(staging / "parser.cpp"),
        str(staging / "machine_state.cpp"),
        str(staging / "scheduler.cpp"),
        str(staging / "output.cpp"),
        "-o", str(exe),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        print(f"COMPILE FAIL {name}:\n{r.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return exe


def eval_variant(name: str, exe: Path, timeout: float) -> dict[str, dict]:
    from evaluate import Job, Server, parse_instance, parse_output, validate_records

    rows: dict[str, dict] = {}
    cases = sorted(glob.glob(str(DATASET / "case*.in")))
    for case_file in cases:
        case = Path(case_file).name
        servers, jobs = parse_instance(Path(case_file))
        t0 = time.time()
        try:
            proc = subprocess.run(
                [str(exe)],
                input=Path(case_file).read_bytes(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
            runtime = time.time() - t0
            ok_fmt, records, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
            ok = ok_fmt and proc.returncode == 0
            if ok:
                ok, _ = validate_records(records, servers, jobs)
            ww = None
            if ok:
                ww = 0.0
                for jid, sid, st, gpu_used, ft in records:
                    job = jobs[jid]
                    ww += (st - job.release_time) * job.weight
            rows[case] = {"valid": int(ok), "ww": ww, "runtime": runtime}
        except subprocess.TimeoutExpired:
            rows[case] = {"valid": 0, "ww": None, "runtime": timeout}
        print(f"{name}\t{case}\t{rows[case]['valid']}\t{rows[case]['ww']}\t{rows[case]['runtime']:.3f}")
    return rows


def main() -> int:
    timeout = 15.0
    all_rows: dict[str, dict[str, dict]] = {}
    for name in VARIANT_NAMES:
        print(f"=== building {name} ===", flush=True)
        exe = compile_variant(name)
        print(f"=== eval {name} ===", flush=True)
        all_rows[name] = eval_variant(name, exe, timeout)

    cases = sorted(all_rows[VARIANT_NAMES[0]].keys())
    out_csv = ROOT / "results" / "four_variant_compare.csv"
    out_csv.parent.mkdir(exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        header = ["case"]
        for n in VARIANT_NAMES:
            header += [f"{n}_valid", f"{n}_ww", f"{n}_rt"]
        header += ["best_variant", "best_ww", "gain_vs_schedule4"]
        w.writerow(header)
        totals = {n: 0.0 for n in VARIANT_NAMES}
        wins = {n: 0 for n in VARIANT_NAMES}
        for case in cases:
            row = [case]
            best_n = None
            best_ww = None
            s4_ww = all_rows["schedule4.0"][case]["ww"]
            for n in VARIANT_NAMES:
                d = all_rows[n][case]
                row += [d["valid"], d["ww"], f"{d['runtime']:.3f}"]
                if d["valid"] and d["ww"] is not None:
                    totals[n] += d["ww"]
                    if best_ww is None or d["ww"] < best_ww:
                        best_ww = d["ww"]
                        best_n = n
            gain = (s4_ww - best_ww) if (s4_ww is not None and best_ww is not None) else ""
            if best_n:
                wins[best_n] += 1
            row += [best_n or "", best_ww if best_ww is not None else "", gain]
            w.writerow(row)

    print("\n=== SUMMARY ===")
    for n in VARIANT_NAMES:
        print(f"{n}: total_ww={totals[n]:.0f} wins={wins[n]}")
    print(f"CSV: {out_csv}")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT / "scripts"))
    raise SystemExit(main())
