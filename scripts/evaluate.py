#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass
class Server:
    gpu_count: int
    gpu_memory: int
    cpu_cores: int
    memory: int


@dataclass
class Job:
    release_time: int
    duration: int
    min_gpu: int
    gpu_memory: int
    cpu_cores: int
    memory: int
    weight: int


def parse_instance(path: Path) -> Tuple[Dict[int, Server], Dict[int, Job]]:
    toks = path.read_text(encoding="utf-8").split()
    it = iter(toks)
    server_count = int(next(it))
    job_count = int(next(it))

    servers: Dict[int, Server] = {}
    for sid in range(1, server_count + 1):
        servers[sid] = Server(
            gpu_count=int(next(it)),
            gpu_memory=int(next(it)),
            cpu_cores=int(next(it)),
            memory=int(next(it)),
        )

    jobs: Dict[int, Job] = {}
    for jid in range(1, job_count + 1):
        jobs[jid] = Job(
            release_time=int(next(it)),
            duration=int(next(it)),
            min_gpu=int(next(it)),
            gpu_memory=int(next(it)),
            cpu_cores=int(next(it)),
            memory=int(next(it)),
            weight=int(next(it)),
        )
    return servers, jobs


def validate_records(
    records: List[Tuple[int, int, int, int, int]],
    servers: Dict[int, Server],
    jobs: Dict[int, Job],
) -> Tuple[bool, str]:
    if len(records) != len(jobs):
        return False, f"record_count={len(records)}/{len(jobs)}"

    seen = set()
    timeline = {sid: [] for sid in servers.keys()}
    for jid, sid, st, gpu_used, ft in records:
        if jid in seen:
            return False, f"duplicate_job={jid}"
        if jid not in jobs:
            return False, f"unknown_job={jid}"
        if sid not in servers:
            return False, f"unknown_server={sid}"
        seen.add(jid)

        job = jobs[jid]
        server = servers[sid]
        if st < job.release_time:
            return False, f"early_start_job={jid}"
        if ft != st + job.duration:
            return False, f"bad_finish_job={jid}"
        if gpu_used < job.min_gpu or gpu_used > server.gpu_count:
            return False, f"bad_gpu_job={jid}"
        if job.cpu_cores > server.cpu_cores or job.memory > server.memory:
            return False, f"cpu_mem_over_job={jid}"

        # 与调度器一致：server.gpu_memory 表示单卡显存
        mem_per_gpu = max(1, server.gpu_memory)
        if job.gpu_memory > mem_per_gpu * gpu_used:
            return False, f"vram_over_job={jid}"

        timeline[sid].append((st, 1, gpu_used, job.cpu_cores, job.memory))
        timeline[sid].append((ft, -1, gpu_used, job.cpu_cores, job.memory))

    for sid, events in timeline.items():
        s = servers[sid]
        used_gpu = used_cpu = used_mem = 0
        # finish first at same timestamp
        events.sort(key=lambda x: (x[0], x[1]))
        for _, typ, g, c, m in events:
            if typ == -1:
                used_gpu -= g
                used_cpu -= c
                used_mem -= m
            else:
                used_gpu += g
                used_cpu += c
                used_mem += m
            if (
                used_gpu < 0
                or used_cpu < 0
                or used_mem < 0
                or used_gpu > s.gpu_count
                or used_cpu > s.cpu_cores
                or used_mem > s.memory
            ):
                return False, f"resource_conflict_server={sid}"

    return True, "ok"


def parse_output(stdout: str) -> Tuple[bool, List[Tuple[int, int, int, int, int]], str]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    records = []
    for line in lines:
        parts = line.split()
        if len(parts) != 5:
            return False, [], f"bad_line={line}"
        try:
            records.append(tuple(map(int, parts)))
        except ValueError:
            return False, [], f"non_int_line={line}"
    return True, records, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate scheduler on all dataset cases.")
    parser.add_argument("--exe", default="build/execname.exe")
    parser.add_argument("--dataset", default="../02-数据集")
    parser.add_argument("--timeout", type=float, default=5.0, help="Per-case timeout seconds")
    args = parser.parse_args()

    exe_path = Path(args.exe)
    if not exe_path.exists():
        print(f"[ERROR] executable not found: {exe_path}")
        return 2

    case_files = sorted(glob.glob(str(Path(args.dataset) / "case*.in")))
    if not case_files:
        print(f"[ERROR] no case files found under: {args.dataset}")
        return 2

    total = len(case_files)
    valid = 0
    timed_out = 0
    runtime_err = 0
    format_err = 0
    constraint_err = 0

    rows = []
    t_all_start = time.time()

    for case_file in case_files:
        case_name = Path(case_file).name
        case_path = Path(case_file)
        servers, jobs = parse_instance(case_path)
        inp = case_path.read_bytes()

        start = time.time()
        try:
            proc = subprocess.run(
                [str(exe_path)],
                input=inp,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=args.timeout,
                check=False,
            )
            elapsed = time.time() - start
        except subprocess.TimeoutExpired:
            timed_out += 1
            rows.append((case_name, len(jobs), 0, "TIMEOUT", f">{args.timeout}s"))
            continue

        if proc.returncode != 0:
            runtime_err += 1
            rows.append((case_name, len(jobs), 0, "RUNTIME_ERR", f"code={proc.returncode}"))
            continue

        ok_fmt, records, fmt_msg = parse_output(proc.stdout.decode("utf-8", errors="ignore"))
        if not ok_fmt:
            format_err += 1
            rows.append((case_name, len(jobs), 0, "FORMAT_ERR", fmt_msg))
            continue

        ok_cons, cons_msg = validate_records(records, servers, jobs)
        if ok_cons:
            valid += 1
            rows.append((case_name, len(jobs), len(records), "VALID", f"{elapsed:.3f}s"))
        else:
            constraint_err += 1
            rows.append((case_name, len(jobs), len(records), "CONSTRAINT_ERR", cons_msg))

    t_all = time.time() - t_all_start

    print("=== Evaluation Summary ===")
    print(f"total_cases: {total}")
    print(f"valid_cases: {valid}")
    print(f"timeout_cases: {timed_out}")
    print(f"runtime_err_cases: {runtime_err}")
    print(f"format_err_cases: {format_err}")
    print(f"constraint_err_cases: {constraint_err}")
    print(f"elapsed_total: {t_all:.3f}s")
    print()
    print("case,input_jobs,output_records,status,detail")
    for row in rows:
        print(",".join(map(str, row)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

