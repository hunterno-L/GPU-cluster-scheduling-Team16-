#!/usr/bin/env python3
"""Validate a scheduler result and report its raw evaluation components."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path


def read_instance(path: Path):
    values = list(map(int, path.read_text(encoding="utf-8").split()))
    cursor = 0
    machine_count, job_count = values[cursor : cursor + 2]
    cursor += 2

    servers = {}
    for server_id in range(1, machine_count + 1):
        gpu_count, gpu_memory, cpu_cores, memory = values[cursor : cursor + 4]
        cursor += 4
        servers[server_id] = (gpu_count, gpu_memory, cpu_cores, memory)

    jobs = {}
    for job_id in range(1, job_count + 1):
        release, duration, min_gpu, gpu_memory, cpu_cores, memory, weight = values[cursor : cursor + 7]
        cursor += 7
        jobs[job_id] = (release, duration, min_gpu, gpu_memory, cpu_cores, memory, weight)

    return servers, jobs


def read_schedule(path: Path):
    lines = [line for line in path.read_text(encoding="ascii").splitlines() if line.strip()]
    records = {}
    for line_number, line in enumerate(lines, start=1):
        fields = line.split()
        if len(fields) != 5:
            raise ValueError(f"line {line_number}: expected 5 fields, got {len(fields)}")
        job_id, server_id, start, gpu_used, finish = map(int, fields)
        if job_id in records:
            raise ValueError(f"line {line_number}: duplicate job id {job_id}")
        records[job_id] = (server_id, start, gpu_used, finish)
    return records


def validate(servers, jobs, records):
    missing = sorted(set(jobs) - set(records))
    unexpected = sorted(set(records) - set(jobs))
    if missing or unexpected:
        raise ValueError(f"missing jobs={missing[:5]}, unexpected jobs={unexpected[:5]}")

    events = defaultdict(list)
    weighted_wait = 0
    workload = 0
    horizon = 0

    for job_id, (server_id, start, gpu_used, finish) in records.items():
        if server_id not in servers:
            raise ValueError(f"job {job_id}: invalid server {server_id}")

        release, duration, min_gpu, gpu_memory, cpu_cores, memory, weight = jobs[job_id]
        server_gpu, server_gpu_memory, server_cpu, server_memory = servers[server_id]
        if start < release:
            raise ValueError(f"job {job_id}: starts before release")
        if finish != start + duration:
            raise ValueError(f"job {job_id}: finish time is inconsistent")
        if not (min_gpu <= gpu_used <= server_gpu):
            raise ValueError(f"job {job_id}: invalid GPU allocation")
        if gpu_memory > gpu_used * server_gpu_memory:
            raise ValueError(f"job {job_id}: insufficient GPU memory")
        if cpu_cores > server_cpu or memory > server_memory:
            raise ValueError(f"job {job_id}: server capacity is insufficient")

        events[server_id].append((start, 1, gpu_used, cpu_cores, memory, job_id))
        events[server_id].append((finish, 0, -gpu_used, -cpu_cores, -memory, job_id))
        weighted_wait += weight * (start - release)
        workload += gpu_memory * duration
        horizon = max(horizon, finish)

    for server_id, server_events in events.items():
        gpu_limit, _, cpu_limit, memory_limit = servers[server_id]
        gpu = cpu = memory = 0
        for _, event_type, gpu_delta, cpu_delta, memory_delta, job_id in sorted(server_events):
            gpu += gpu_delta
            cpu += cpu_delta
            memory += memory_delta
            if gpu > gpu_limit or cpu > cpu_limit or memory > memory_limit:
                raise ValueError(f"job {job_id}: concurrent resources exceed server {server_id}")

    total_gpu_memory = sum(gpu_count * gpu_memory for gpu_count, gpu_memory, _, _ in servers.values())
    average_idle_memory = total_gpu_memory - workload / horizon
    return weighted_wait, average_idle_memory, horizon


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    components = validate(*read_instance(args.input), read_schedule(args.output))
    print("valid")
    print(f"weighted_wait={components[0]}")
    print(f"average_idle_gpu_memory={components[1]:.6f}")
    print(f"makespan={components[2]}")


if __name__ == "__main__":
    main()
