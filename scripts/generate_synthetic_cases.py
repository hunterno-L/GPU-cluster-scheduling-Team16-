#!/usr/bin/env python3
"""生成合成极端测试集，格式与 02-数据集/case*.in 完全一致。"""
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List, Sequence, Tuple


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


def required_gpu(job: Job, server: Server) -> int:
    mpg = max(1, server.gpu_memory)
    g_mem = (job.gpu_memory + mpg - 1) // mpg
    return max(job.min_gpu, g_mem)


def can_ever_run(job: Job, server: Server) -> bool:
    if job.min_gpu > server.gpu_count:
        return False
    if job.cpu_cores > server.cpu_cores:
        return False
    if job.memory > server.memory:
        return False
    return required_gpu(job, server) <= server.gpu_count


def job_feasible_on_cluster(job: Job, servers: Sequence[Server]) -> bool:
    return any(can_ever_run(job, s) for s in servers)


def clamp_job_to_cluster(job: Job, servers: Sequence[Server], rng: random.Random) -> Job:
    """保证每个任务至少能在一台机器上运行。"""
    if job_feasible_on_cluster(job, servers):
        return job

    viable = [s for s in servers if job.min_gpu <= s.gpu_count]
    if not viable:
        s = min(servers, key=lambda x: x.gpu_count)
        job = Job(job.release_time, job.duration, min(job.min_gpu, s.gpu_count),
                  job.gpu_memory, job.cpu_cores, job.memory, job.weight)

    viable = [s for s in servers if job.min_gpu <= s.gpu_count]
    if not viable:
        return job

    s = rng.choice(viable)
    mpg = max(1, s.gpu_memory)
    max_vram = s.gpu_count * mpg
    job = Job(
        job.release_time,
        job.duration,
        min(job.min_gpu, s.gpu_count),
        min(job.gpu_memory, max_vram),
        min(job.cpu_cores, s.cpu_cores),
        min(job.memory, s.memory),
        job.weight,
    )
    return job


def write_case(path: Path, servers: Sequence[Server], jobs: Sequence[Job]) -> None:
    lines = [f"{len(servers)} {len(jobs)}"]
    for s in servers:
        lines.append(f"{s.gpu_count} {s.gpu_memory} {s.cpu_cores} {s.memory}")
    for j in jobs:
        lines.append(
            f"{j.release_time} {j.duration} {j.min_gpu} {j.gpu_memory} "
            f"{j.cpu_cores} {j.memory} {j.weight}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def rand_job(rng: random.Random, servers: Sequence[Server], **overrides) -> Job:
    s = rng.choice(servers)
    mpg = max(1, s.gpu_memory)
    max_g = rng.randint(1, s.gpu_count)
    min_gpu = overrides.get("min_gpu", rng.randint(1, min(4, s.gpu_count)))
    min_gpu = min(min_gpu, max_g)
    gpu_memory = overrides.get(
        "gpu_memory",
        rng.randint(max(1, mpg // 2), max(mpg, max_g * mpg)),
    )
    job = Job(
        release_time=overrides.get("release_time", rng.randint(0, 2500)),
        duration=overrides.get("duration", rng.randint(10, 400)),
        min_gpu=min_gpu,
        gpu_memory=gpu_memory,
        cpu_cores=overrides.get("cpu_cores", rng.randint(1, max(1, s.cpu_cores // 2))),
        memory=overrides.get("memory", rng.randint(1, max(1, s.memory // 2))),
        weight=overrides.get("weight", rng.randint(1, 20)),
    )
    return clamp_job_to_cluster(job, servers, rng)


def make_servers(rng: random.Random, count: int, profile: str) -> List[Server]:
    profiles: dict[str, Callable[[], Server]] = {
        "small": lambda: Server(rng.randint(2, 6), 32, rng.randint(24, 64), rng.randint(256, 512)),
        "medium": lambda: Server(rng.randint(3, 8), rng.choice([40, 48]), rng.randint(48, 96), rng.randint(384, 800)),
        "large": lambda: Server(rng.randint(4, 8), rng.choice([64, 80]), rng.randint(64, 108), rng.randint(512, 1024)),
        "wide": lambda: Server(rng.randint(6, 10), rng.choice([32, 40, 48]), rng.randint(72, 108), rng.randint(736, 1024)),
        "tiny_gpu": lambda: Server(rng.randint(1, 3), 24, rng.randint(20, 40), rng.randint(256, 400)),
        "fat_mem": lambda: Server(rng.randint(2, 5), 48, rng.randint(32, 64), rng.randint(900, 1200)),
    }
    fn = profiles.get(profile, profiles["medium"])
    return [fn() for _ in range(count)]


def scenario_burst_t0(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 6), "medium")
    jobs = [rand_job(rng, servers, release_time=0) for _ in range(n_jobs)]
    return servers, jobs


def scenario_staggered(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 8), "wide")
    jobs = []
    t = 0
    for _ in range(n_jobs):
        jobs.append(rand_job(rng, servers, release_time=t))
        t += rng.randint(5, 120)
    return servers, jobs


def scenario_scarce_80gb(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = [Server(rng.randint(4, 8), 80, rng.randint(72, 96), rng.randint(512, 800))]
    for _ in range(rng.randint(3, 8)):
        servers.append(Server(rng.randint(4, 8), rng.choice([32, 40, 48]), rng.randint(48, 88), rng.randint(384, 800)))
    jobs = []
    for i in range(n_jobs):
        if i % 3 == 0:
            jobs.append(rand_job(rng, servers, release_time=rng.randint(0, 500),
                                 gpu_memory=rng.randint(60, 240), min_gpu=rng.randint(1, 3)))
        else:
            jobs.append(rand_job(rng, servers, gpu_memory=rng.randint(8, 48)))
    return servers, jobs


def scenario_high_min_gpu(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 5), "wide")
    jobs = [rand_job(rng, servers, min_gpu=rng.randint(3, min(8, max(s.gpu_count for s in servers))))
            for _ in range(n_jobs)]
    return servers, jobs


def scenario_tight_vram(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 7), "medium")
    jobs = []
    for _ in range(n_jobs):
        s = rng.choice(servers)
        g = rng.randint(1, s.gpu_count)
        mpg = max(1, s.gpu_memory)
        vram = g * mpg if rng.random() < 0.7 else g * mpg - rng.randint(0, mpg // 4)
        vram = max(1, vram)
        jobs.append(rand_job(rng, servers, min_gpu=g, gpu_memory=vram))
    return servers, jobs


def scenario_cpu_bound(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 6), "medium")
    jobs = []
    for _ in range(n_jobs):
        s = rng.choice(servers)
        jobs.append(rand_job(rng, servers, cpu_cores=rng.randint(max(1, s.cpu_cores * 2 // 3), s.cpu_cores)))
    return servers, jobs


def scenario_mem_bound(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 6), "fat_mem")
    jobs = []
    for _ in range(n_jobs):
        s = rng.choice(servers)
        jobs.append(rand_job(rng, servers, memory=rng.randint(max(1, s.memory * 2 // 3), s.memory)))
    return servers, jobs


def scenario_weight_spike(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 7), "medium")
    jobs = []
    for i in range(n_jobs):
        w = 50 if i % 7 == 0 else rng.randint(1, 10)
        jobs.append(rand_job(rng, servers, weight=w))
    return servers, jobs


def scenario_long_jobs(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 5), "large")
    jobs = [rand_job(rng, servers, duration=rng.randint(500, 2000)) for _ in range(n_jobs)]
    return servers, jobs


def scenario_short_burst(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(4, 10), "wide")
    jobs = [rand_job(rng, servers, duration=rng.randint(5, 30), release_time=rng.randint(0, 200))
            for _ in range(n_jobs)]
    return servers, jobs


def scenario_single_server(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = [Server(rng.randint(6, 10), rng.choice([40, 48, 64]), rng.randint(64, 96), rng.randint(512, 1024))]
    jobs = [rand_job(rng, servers) for _ in range(n_jobs)]
    return servers, jobs


def scenario_many_servers(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(15, 30), "medium")
    jobs = [rand_job(rng, servers) for _ in range(max(5, n_jobs // 3))]
    return servers, jobs


def scenario_bimodal_vram(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(4, 8), "medium")
    jobs = []
    for i in range(n_jobs):
        if i % 2 == 0:
            jobs.append(rand_job(rng, servers, gpu_memory=rng.randint(4, 24), min_gpu=1))
        else:
            s = max(servers, key=lambda x: x.gpu_memory)
            g = rng.randint(2, s.gpu_count)
            jobs.append(rand_job(rng, servers, gpu_memory=rng.randint(s.gpu_memory, g * s.gpu_memory), min_gpu=g))
    return servers, jobs


def scenario_late_arrival(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 7), "medium")
    base = rng.randint(1500, 3000)
    jobs = [rand_job(rng, servers, release_time=base + rng.randint(0, 400)) for _ in range(n_jobs)]
    return servers, jobs


def scenario_homogeneous_32(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = [Server(rng.randint(4, 8), 32, rng.randint(48, 84), rng.randint(384, 768)) for _ in range(rng.randint(4, 10))]
    jobs = [rand_job(rng, servers, gpu_memory=rng.randint(16, 96)) for _ in range(n_jobs)]
    return servers, jobs


def scenario_parallel_stress(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(5, 12), "wide")
    jobs = [rand_job(rng, servers, release_time=0, min_gpu=1, duration=rng.randint(50, 200))
            for _ in range(n_jobs)]
    return servers, jobs


def scenario_hetero_extreme(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    specs = [(2, 24, 28, 320), (4, 32, 48, 448), (6, 48, 80, 656), (8, 64, 96, 784), (3, 80, 88, 992)]
    servers = [Server(*rng.choice(specs)) for _ in range(rng.randint(6, 12))]
    jobs = [rand_job(rng, servers) for _ in range(n_jobs)]
    return servers, jobs


def scenario_duplicate_jobs(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 6), "medium")
    templates = [rand_job(rng, servers) for _ in range(max(3, n_jobs // 10))]
    jobs = []
    for i in range(n_jobs):
        t = templates[i % len(templates)]
        jobs.append(Job(rng.randint(0, 800), t.duration, t.min_gpu, t.gpu_memory,
                        t.cpu_cores, t.memory, t.weight))
    return servers, jobs


def scenario_tiny_cluster(rng: random.Random, _: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(1, 2), "tiny_gpu")
    jobs = [rand_job(rng, servers) for _ in range(rng.randint(3, 8))]
    return servers, jobs


def scenario_min_gpu_one(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(4, 10), "medium")
    jobs = [rand_job(rng, servers, min_gpu=1) for _ in range(n_jobs)]
    return servers, jobs


def scenario_wave_release(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(3, 8), "wide")
    jobs = []
    wave = 0
    for i in range(n_jobs):
        jobs.append(rand_job(rng, servers, release_time=wave * 200 + rng.randint(0, 30)))
        if i % 5 == 4:
            wave += 1
    return servers, jobs


def scenario_vram_oversubscribe(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(2, 5), "medium")
    cap = sum(s.gpu_count * s.gpu_memory for s in servers)
    jobs = []
    for i in range(n_jobs):
        target = rng.randint(max(1, cap // max(1, n_jobs // 2)), max(2, cap // max(1, n_jobs // 4)))
        jobs.append(rand_job(rng, servers, gpu_memory=target, release_time=rng.randint(0, 1000)))
    return servers, jobs


def scenario_megascale_burst(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(12, 25), "wide")
    n = max(n_jobs, 400)
    jobs = [rand_job(rng, servers, release_time=rng.randint(0, 50), duration=rng.randint(20, 200))
            for _ in range(n)]
    return servers, jobs


def scenario_megascale_staggered(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = make_servers(rng, rng.randint(10, 20), "wide")
    n = max(n_jobs, 350)
    jobs = []
    t = 0
    for _ in range(n):
        jobs.append(rand_job(rng, servers, release_time=t))
        t += rng.randint(1, 40)
    return servers, jobs


def scenario_megascale_scarce80(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    servers = [Server(rng.randint(6, 10), 80, rng.randint(80, 108), rng.randint(600, 1000))]
    for _ in range(rng.randint(8, 16)):
        servers.append(Server(rng.randint(4, 8), rng.choice([32, 40, 48]), rng.randint(48, 96), rng.randint(400, 900)))
    n = max(n_jobs, 300)
    jobs = []
    for i in range(n):
        if i % 4 == 0:
            jobs.append(rand_job(rng, servers, gpu_memory=rng.randint(48, 200), min_gpu=rng.randint(1, 4)))
        else:
            jobs.append(rand_job(rng, servers, gpu_memory=rng.randint(8, 48)))
    return servers, jobs


def scenario_megascale_hetero(rng: random.Random, n_jobs: int) -> Tuple[List[Server], List[Job]]:
    specs = [(4, 24, 32, 320), (6, 32, 64, 512), (8, 48, 88, 720), (8, 64, 96, 900), (4, 80, 100, 1000)]
    servers = [Server(*rng.choice(specs)) for _ in range(rng.randint(15, 28))]
    n = max(n_jobs, 400)
    jobs = [rand_job(rng, servers) for _ in range(n)]
    return servers, jobs


SCENARIOS: List[Tuple[str, Callable[[random.Random, int], Tuple[List[Server], List[Job]]]]] = [
    ("burst_t0", scenario_burst_t0),
    ("staggered", scenario_staggered),
    ("scarce_80gb", scenario_scarce_80gb),
    ("high_min_gpu", scenario_high_min_gpu),
    ("tight_vram", scenario_tight_vram),
    ("cpu_bound", scenario_cpu_bound),
    ("mem_bound", scenario_mem_bound),
    ("weight_spike", scenario_weight_spike),
    ("long_jobs", scenario_long_jobs),
    ("short_burst", scenario_short_burst),
    ("single_server", scenario_single_server),
    ("many_servers", scenario_many_servers),
    ("bimodal_vram", scenario_bimodal_vram),
    ("late_arrival", scenario_late_arrival),
    ("homogeneous_32", scenario_homogeneous_32),
    ("parallel_stress", scenario_parallel_stress),
    ("hetero_extreme", scenario_hetero_extreme),
    ("duplicate_jobs", scenario_duplicate_jobs),
    ("tiny_cluster", scenario_tiny_cluster),
    ("min_gpu_one", scenario_min_gpu_one),
    ("wave_release", scenario_wave_release),
    ("vram_oversubscribe", scenario_vram_oversubscribe),
    ("megascale_burst", scenario_megascale_burst),
    ("megascale_staggered", scenario_megascale_staggered),
    ("megascale_scarce80", scenario_megascale_scarce80),
    ("megascale_hetero", scenario_megascale_hetero),
]

# 固定极端手工样例（可复现）
HANDCRAFTED: List[Tuple[str, List[Server], List[Job]]] = [
    (
        "hand_parallel_t0",
        [Server(1, 80, 32, 128), Server(1, 80, 32, 128)],
        [
            Job(0, 10, 1, 40, 1, 1, 1),
            Job(0, 10, 1, 40, 1, 1, 1),
            Job(0, 10, 1, 40, 1, 1, 1),
            Job(0, 10, 1, 40, 1, 1, 1),
        ],
    ),
    (
        "hand_one_scarce_80",
        [
            Server(8, 32, 64, 512),
            Server(8, 32, 64, 512),
            Server(4, 80, 96, 800),
        ],
        [
            Job(0, 100, 1, 72, 8, 64, 20),
            Job(0, 100, 1, 16, 4, 32, 5),
            Job(0, 100, 1, 16, 4, 32, 5),
            Job(0, 100, 2, 120, 16, 128, 18),
            Job(0, 100, 1, 64, 8, 64, 15),
        ],
    ),
    (
        "hand_chain_release",
        [
            Server(4, 48, 64, 512),
            Server(4, 48, 64, 512),
        ],
        [
            Job(0, 50, 1, 40, 4, 64, 10),
            Job(50, 50, 1, 40, 4, 64, 10),
            Job(100, 50, 1, 40, 4, 64, 10),
            Job(150, 50, 1, 40, 4, 64, 10),
        ],
    ),
    (
        "hand_multi_gpu_only",
        [Server(8, 40, 96, 800), Server(8, 40, 96, 800)],
        [
            Job(0, 200, 4, 120, 32, 256, 12),
            Job(0, 200, 4, 120, 32, 256, 12),
            Job(0, 150, 6, 200, 48, 400, 15),
        ],
    ),
    (
        "hand_cpu_bottleneck",
        [Server(8, 48, 32, 512)],
        [Job(0, 80, 1, 32, 28, 64, i + 1) for i in range(6)],
    ),
]


def job_count_for_index(idx: int) -> int:
    if idx <= 20:
        return 15 + (idx % 10) * 3
    if idx <= 60:
        return 30 + (idx % 15) * 5
    if idx <= 100:
        return 80 + (idx % 20) * 8
    if idx <= 350:
        return min(280, 100 + (idx % 25) * 12)
    if idx <= 700:
        return 350 + (idx % 35) * 18
    return min(1500, 600 + (idx % 60) * 25)


def generate_all(out_dir: Path, count: int, seed: int) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    for i, (name, servers, jobs) in enumerate(HANDCRAFTED, start=1):
        path = out_dir / f"synth{i:05d}_{name}.in"
        write_case(path, servers, jobs)
        paths.append(path)

    base = len(HANDCRAFTED)
    for i in range(base + 1, count + 1):
        rng = random.Random(seed + i * 10007)
        scen_name, scen_fn = SCENARIOS[(i - base - 1) % len(SCENARIOS)]
        variant = (i - base - 1) // len(SCENARIOS)
        n_jobs = job_count_for_index(i) + variant * 7
        servers, jobs = scen_fn(rng, n_jobs)
        path = out_dir / f"synth{i:05d}_{scen_name}.in"
        write_case(path, servers, jobs)
        paths.append(path)

    return paths


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="../02-数据集/synthetic")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--clean", action="store_true", help="生成前删除目录内旧 synth*.in")
    args = parser.parse_args()

    out_dir = Path(args.out)
    if args.clean and out_dir.exists():
        for old in out_dir.glob("synth*.in"):
            old.unlink()
    paths = generate_all(out_dir, args.count, args.seed)
    print(f"generated {len(paths)} cases -> {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
