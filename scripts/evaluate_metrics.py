#!/usr/bin/env python3
"""按题目评估函数统计：合法率、加权等待、显存空闲、完工时间。"""
from __future__ import annotations

import argparse
import glob
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Tuple

from evaluate import Job, Server, parse_instance, parse_output, validate_records


def compute_metrics(
    records: List[Tuple[int, int, int, int, int]],
    servers: Dict[int, Server],
    jobs: Dict[int, Job],
) -> Tuple[float, float, float]:
    if not records:
        return 0.0, 0.0, 0.0

    weighted_wait = 0.0
    makespan = 0
    t0 = min(j.release_time for j in jobs.values())

    for jid, sid, st, gpu_used, ft in records:
        job = jobs[jid]
        weighted_wait += (st - job.release_time) * job.weight
        makespan = max(makespan, ft)

    horizon = max(1, makespan - t0)

    # 事件扫描计算显存空闲积分，避免时间段爆炸
    events: List[Tuple[int, int, int, int]] = []  # time, type(+1/-1), sid, vram
    for jid, sid, st, _, ft in records:
        events.append((st, 1, sid, jobs[jid].gpu_memory))
        events.append((ft, -1, sid, jobs[jid].gpu_memory))
    events.sort(key=lambda x: (x[0], x[1]))

    capacity = {sid: srv.gpu_count * srv.gpu_memory for sid, srv in servers.items()}
    used = {sid: 0 for sid in servers}
    idle_integral = 0.0
    prev_t = t0
    idx = 0
    while idx < len(events):
        t = events[idx][0]
        if t > prev_t:
            seg_idle = sum(max(0, capacity[sid] - used[sid]) for sid in servers)
            idle_integral += seg_idle * (t - prev_t)
            prev_t = t
        while idx < len(events) and events[idx][0] == t:
            _, typ, sid, vram = events[idx]
            used[sid] += typ * vram
            idx += 1
    if makespan > prev_t:
        seg_idle = sum(max(0, capacity[sid] - used[sid]) for sid in servers)
        idle_integral += seg_idle * (makespan - prev_t)

    vram_idle = idle_integral / horizon
    return weighted_wait, vram_idle, float(makespan)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", default="build/execname.exe")
    parser.add_argument("--dataset", default="../02-数据集")
    parser.add_argument("--pattern", default="case*.in")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    exe = Path(args.exe)
    cases = sorted(glob.glob(str(Path(args.dataset) / args.pattern)))
    valid = 0
    total_ww = total_vram = total_mk = 0.0
    max_runtime = 0.0

    print("case,valid,weighted_wait,vram_idle,makespan,runtime_s")
    for case_file in cases:
        name = Path(case_file).name
        servers, jobs = parse_instance(Path(case_file))
        t0 = time.time()
        proc = subprocess.run(
            [str(exe)],
            input=Path(case_file).read_bytes(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=args.timeout,
            check=False,
        )
        runtime = time.time() - t0
        max_runtime = max(max_runtime, runtime)

        ok_fmt, records, _ = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        ok = ok_fmt and proc.returncode == 0
        if ok:
            ok, _ = validate_records(records, servers, jobs)
        if ok:
            valid += 1
            ww, vi, mk = compute_metrics(records, servers, jobs)
            total_ww += ww
            total_vram += vi
            total_mk += mk
            print(f"{name},1,{ww:.0f},{vi:.2f},{mk:.0f},{runtime:.3f}")
        else:
            print(f"{name},0,,,,{runtime:.3f}")

    n = len(cases)
    print()
    print(f"valid_rate: {valid}/{n}")
    if valid:
        print(f"avg_weighted_wait: {total_ww/valid:.2f}")
        print(f"avg_vram_idle: {total_vram/valid:.2f}")
        print(f"avg_makespan: {total_mk/valid:.2f}")
    print(f"max_runtime_s: {max_runtime:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
