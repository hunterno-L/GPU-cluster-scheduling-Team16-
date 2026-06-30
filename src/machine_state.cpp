#include "machine_state.h"

#include <algorithm>
#include <climits>

using namespace std;

MachineState::MachineState(ServerSpec server) : spec(server) {
    remaining_gpu = spec.gpu_count;
    remaining_cpu = spec.cpu_cores;
    remaining_memory = spec.memory;
}

int MachineState::requiredGpuCount(const Job &job) const {
    int gpu_for_memory = (job.gpu_memory + spec.gpu_memory - 1) / spec.gpu_memory;
    return max(job.min_gpu, gpu_for_memory);
}

bool MachineState::canEverRun(const Job &job, int gpu_used) const {
    return gpu_used <= spec.gpu_count &&
           job.cpu_cores <= spec.cpu_cores &&
           job.memory <= spec.memory;
}

bool MachineState::canStart(const Job &job, int gpu_used) const {
    return gpu_used <= remaining_gpu &&
           job.cpu_cores <= remaining_cpu &&
           job.memory <= remaining_memory;
}

pair<ScheduleRecord, RunningJob> MachineState::startJob(const Job &job, long long current_time, int gpu_used) {
    long long finish_time = current_time + job.duration;

    remaining_gpu -= gpu_used;
    remaining_cpu -= job.cpu_cores;
    remaining_memory -= job.memory;

    RunningJob running_job{
        job.job_id,
        spec.server_id,
        finish_time,
        gpu_used,
        job.cpu_cores,
        job.memory,
    };
    running_jobs.push_back(running_job);

    ScheduleRecord record{
        job.job_id,
        spec.server_id,
        current_time,
        gpu_used,
        finish_time,
    };

    return {record, running_job};
}

void MachineState::releaseJob(const RunningJob &running_job) {
    remaining_gpu += running_job.gpu_used;
    remaining_cpu += running_job.cpu_used;
    remaining_memory += running_job.memory_used;

    vector<RunningJob> remaining;
    for (size_t i = 0; i < running_jobs.size(); ++i) {
        if (running_jobs[i].job_id != running_job.job_id) {
            remaining.push_back(running_jobs[i]);
        }
    }
    running_jobs = remaining;
}

double MachineState::placementSlack(const Job &job, int gpu_used, double gpu_emphasis) const {
    double gs = spec.gpu_count > 0 ? (double)(remaining_gpu - gpu_used) / spec.gpu_count : 0.0;
    double cs = spec.cpu_cores > 0 ? (double)(remaining_cpu - job.cpu_cores) / spec.cpu_cores : 0.0;
    double ms = spec.memory > 0 ? (double)(remaining_memory - job.memory) / spec.memory : 0.0;
    gpu_emphasis = max(0.2, min(0.65, gpu_emphasis));
    double other = (1.0 - gpu_emphasis) * 0.5;
    return gpu_emphasis * gs + other * cs + other * ms;
}

long long MachineState::earliestFeasibleStart(const Job &job, int gpu_used,
                                              long long current_time) const {
    long long t0 = max(current_time, (long long)job.release_time);
    if (gpu_used <= remaining_gpu && job.cpu_cores <= remaining_cpu &&
        job.memory <= remaining_memory) {
        return t0;
    }

    vector<long long> events;
    events.reserve(running_jobs.size());
    for (const auto &rj : running_jobs) {
        if (rj.finish_time > current_time) events.push_back(rj.finish_time);
    }
    sort(events.begin(), events.end());
    events.erase(unique(events.begin(), events.end()), events.end());

    int sim_gpu = remaining_gpu;
    int sim_cpu = remaining_cpu;
    int sim_mem = remaining_memory;

    for (long long ft : events) {
        for (const auto &rj : running_jobs) {
            if (rj.finish_time == ft) {
                sim_gpu += rj.gpu_used;
                sim_cpu += rj.cpu_used;
                sim_mem += rj.memory_used;
            }
        }
        long long t = max(ft, (long long)job.release_time);
        if (gpu_used <= sim_gpu && job.cpu_cores <= sim_cpu && job.memory <= sim_mem) {
            return t;
        }
    }
    return LLONG_MAX / 4;
}

long long MachineState::totalRemainingGpuWork(long long current_time) const {
    long long work = 0;
    for (const auto &rj : running_jobs) {
        if (rj.finish_time > current_time) {
            work += (rj.finish_time - current_time) * rj.gpu_used;
        }
    }
    return work;
}

