#include "scheduler.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <climits>
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <functional>
#include <limits>
#include <unordered_set>

using namespace std;

bool compareServerById(const ServerSpec &a, const ServerSpec &b) {
    return a.server_id < b.server_id;
}

bool compareJobByRelease(const Job &a, const Job &b) {
    if (a.release_time != b.release_time) return a.release_time < b.release_time;
    if (a.duration != b.duration) return a.duration < b.duration;
    return a.job_id < b.job_id;
}

bool FinishEvent::operator>(const FinishEvent &other) const {
    if (finish_time != other.finish_time) return finish_time > other.finish_time;
    if (server_id != other.server_id) return server_id > other.server_id;
    return job_id > other.job_id;
}

GreedyScheduler::GreedyScheduler(vector<ServerSpec> input_servers, vector<Job> input_jobs)
    : servers(std::move(input_servers)), jobs(std::move(input_jobs)) {
    sort(servers.begin(), servers.end(), compareServerById);
    sort(jobs.begin(), jobs.end(), compareJobByRelease);

    for (const auto &server : servers) {
        machines.emplace_back(server);
    }
    for (int index = 0; index < static_cast<int>(machines.size()); ++index) {
        machine_index_by_id[machines[index].spec.server_id] = index;
    }

    buildFeasibleMachines();
    for (const auto &job : jobs) {
        job_by_id[job.job_id] = &job;
    }
    computeInstanceProfile();
}

void GreedyScheduler::computeInstanceProfile() {
    long long total_cap = 0;
    long long total_job_vram = 0;
    int total_gpu = 0;
    int scarce_gpu = 0;
    double sum_min_gpu = 0.0;
    double sum_duration = 0.0;
    double sum_cpu = 0.0;
    double sum_mem = 0.0;
    int min_vram = INT_MAX;
    int max_vram = 0;
    int total_server_cpu = 0;
    int total_server_mem = 0;

    for (const auto &s : servers) {
        total_cap += (long long)s.gpu_count * s.gpu_memory;
        total_gpu += s.gpu_count;
        total_server_cpu += s.cpu_cores;
        total_server_mem += s.memory;
        if (s.gpu_memory >= 80) scarce_gpu += s.gpu_count;
        min_vram = min(min_vram, s.gpu_memory);
        max_vram = max(max_vram, s.gpu_memory);
    }
    for (const auto &j : jobs) {
        total_job_vram += j.gpu_memory;
        sum_min_gpu += j.min_gpu;
        sum_duration += j.duration;
        sum_cpu += j.cpu_cores;
        sum_mem += j.memory;
    }

    profile.vram_pressure = (total_cap > 0) ? (double)total_job_vram / total_cap : 0.0;
    profile.scarce_80_ratio = (total_gpu > 0) ? (double)scarce_gpu / total_gpu : 0.0;
    profile.avg_min_gpu = jobs.empty() ? 0.0 : sum_min_gpu / jobs.size();
    profile.avg_duration = jobs.empty() ? 0.0 : sum_duration / jobs.size();
    profile.hetero_ratio = (min_vram > 0) ? (double)max_vram / min_vram : 1.0;
    profile.gpu_demand_ratio = (total_gpu > 0 && !jobs.empty())
        ? sum_min_gpu / (double)total_gpu : 0.0;
    profile.cpu_demand_ratio = (total_server_cpu > 0 && !jobs.empty())
        ? sum_cpu / (double)total_server_cpu : 0.0;
    profile.mem_demand_ratio = (total_server_mem > 0 && !jobs.empty())
        ? sum_mem / (double)total_server_mem : 0.0;

    double sum_feasible = 0.0;
    for (const auto &job : jobs) {
        auto it = feasible_machines.find(job.job_id);
        sum_feasible += (it != feasible_machines.end()) ? (double)it->second.size() : 0.0;
    }
    profile.avg_feasible = jobs.empty() ? 0.0 : sum_feasible / jobs.size();

    if (jobs.empty()) {
        profile.burst_t0_ratio = 0.0;
        return;
    }
    long long t0 = jobs.front().release_time;
    int burst = 0;
    for (const auto &j : jobs) {
        if (j.release_time <= t0) ++burst;
    }
    profile.burst_t0_ratio = (double)burst / jobs.size();

    int long_cnt = 0;
    for (const auto &j : jobs) {
        if (j.duration > profile.avg_duration * 1.25) ++long_cnt;
    }
    profile.long_job_ratio = (double)long_cnt / jobs.size();

    long long span = jobs.back().release_time - jobs.front().release_time;
    profile.time_horizon = max(1LL, span + (long long)max(1.0, profile.avg_duration));
    profile.release_spread_ratio = 1.0 - profile.burst_t0_ratio;
}

bool GreedyScheduler::isSingleServer() const {
    return machines.size() == 1;
}

bool GreedyScheduler::isLargeInstance() const {
    return jobs.size() > 1200;
}

bool GreedyScheduler::isMegascaleInstance() const {
    return jobs.size() > 2000;
}

bool GreedyScheduler::isNarrowCluster() const {
    return machines.size() <= 2;
}

bool GreedyScheduler::shouldDrainFullPending() const {
    if (jobs.size() <= 200) return true;
    if (isSingleServer() && jobs.size() <= 220) return true;
    if (!isSingleServer() && profile.long_job_ratio > 0.42 && jobs.size() <= 300) return true;
    if (!isSingleServer() && profile.burst_t0_ratio > 0.55 && jobs.size() <= 380) return true;
    if ((profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) &&
        jobs.size() <= 280) {
        return true;
    }
    return false;
}

int GreedyScheduler::jobFeasibleCount(int job_id) const {
    auto it = feasible_machines.find(job_id);
    return (it != feasible_machines.end()) ? static_cast<int>(it->second.size()) : 0;
}

// ---------- 成员 A：多策略任务排序 ----------
int GreedyScheduler::resolveOrderVariant(int strategy_seed) const {
    static const int normal[] = {0, 0, 1, 2, 0, 3, 1, 0, 2, 3, 1, 0, 0, 2, 1, 3};
    static const int burst[]  = {0, 1, 0, 3, 1, 0, 3, 1, 0, 1, 3, 0, 1, 0, 3, 1};
    static const int vram[]   = {3, 0, 3, 1, 0, 3, 2, 0, 3, 1, 0, 2, 3, 0, 1, 3};

    static const int long_job[] = {2, 2, 3, 2, 0, 2, 3, 2, 2, 3, 0, 2, 2, 1, 2, 3};
    static const int single[] = {3, 1, 3, 2, 3, 0, 1, 3, 2, 1, 3, 0, 3, 1, 2, 3};
    static const int spread[] = {3, 0, 3, 1, 0, 3, 2, 0, 3, 1, 3, 0, 2, 3, 0, 1};
    static const int resource[] = {3, 1, 3, 0, 3, 1, 0, 3, 3, 1, 0, 3, 1, 3, 0, 1};
    static const int high_gpu[] = {3, 2, 3, 1, 3, 0, 2, 3, 3, 2, 1, 3, 2, 3, 0, 1};

    const int idx = strategy_seed % 16;
    if (jobs.size() > 500) {
        if (profile.long_job_ratio > 0.38 && jobs.size() <= 900) {
            return long_job[idx];
        }
        if (profile.avg_feasible > 0.0 && profile.avg_feasible < 12.0 && (strategy_seed % 5) == 2) {
            return 3;
        }
        if (profile.burst_t0_ratio > 0.55 && (strategy_seed % 4) == 1) return 1;
        return 0;
    }
    if (isSingleServer()) return single[idx];
    if (profile.avg_min_gpu > 2.6) return high_gpu[idx];
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) return resource[idx];
    if (profile.long_job_ratio > 0.38) return long_job[idx];
    if (profile.release_spread_ratio > 0.55) return spread[idx];
    if (profile.burst_t0_ratio > 0.45) return burst[idx];
    if (profile.vram_pressure > 0.55) return vram[idx];
    return normal[idx];
}

double GreedyScheduler::jobOrderPriority(const Job &job, int order_variant,
                                         long long current_time) const {
    double p = 0.0;
    switch (order_variant % 4) {
        case 1:
            p = static_cast<double>(job.weight);
            break;
        case 2:
            p = static_cast<double>(job.weight) / sqrt(max(1.0, static_cast<double>(job.duration)));
            break;
        case 3: {
            int fc = max(1, jobFeasibleCount(job.job_id));
            p = (static_cast<double>(job.weight) / max(1, job.duration)) * (1.0 + 0.18 / fc);
            break;
        }
        default:
            p = static_cast<double>(job.weight) / max(1, job.duration);
            break;
    }

    long long waited = max(0LL, current_time - job.release_time);
    double wait_ratio = (double)waited / max(1LL, profile.time_horizon);
    double wait_boost = 0.18;
    if (profile.burst_t0_ratio > 0.45) wait_boost = 0.24;
    if (profile.long_job_ratio > 0.40) wait_boost = 0.22;
    if (isSingleServer()) wait_boost = max(wait_boost, 0.26);
    if (jobs.size() > 500) wait_boost *= 0.55;
    p *= (1.0 + wait_boost * min(1.0, wait_ratio));

    if (isSingleServer() && job.weight > 1) {
        p *= (1.0 + 0.08 * min(1.0, wait_ratio) * min(2.0, job.weight / 40.0));
    }

    if (profile.avg_duration > 0.0 && job.duration > profile.avg_duration * 1.20) {
        double long_factor = min(2.0, job.duration / profile.avg_duration);
        double long_boost = profile.long_job_ratio > 0.38 ? 0.16 : 0.12;
        p *= (1.0 + long_boost * (long_factor - 1.0));
    }
    if (isSingleServer() && order_variant % 4 == 3) {
        p *= (1.0 + 0.08 / max(1, jobFeasibleCount(job.job_id)));
    }

    return p;
}

bool GreedyScheduler::jobLessUrgent(const Job &a, const Job &b, int order_variant,
                                    long long current_time) const {
    double pa = jobOrderPriority(a, order_variant, current_time);
    double pb = jobOrderPriority(b, order_variant, current_time);
    if (fabs(pa - pb) > 1e-9) return pa < pb;
    return a.job_id > b.job_id;
}

bool GreedyScheduler::placementTieBreakPrefer(const Job &a, const Job &b) const {
    if (isSingleServer()) {
        if (a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
        if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
        return a.job_id < b.job_id;
    }
    const bool constrained_first = isNarrowCluster() || profile.avg_feasible < 10.0 ||
        (profile.avg_feasible < 14.0 && profile.burst_t0_ratio > 0.38) ||
        profile.cpu_demand_ratio > 0.58 || profile.mem_demand_ratio > 0.58;
    if (constrained_first) {
        int fa = jobFeasibleCount(a.job_id);
        int fb = jobFeasibleCount(b.job_id);
        if (fa != fb) return fa < fb;
    }
    if (profile.long_job_ratio > 0.35 && a.duration != b.duration) return a.duration > b.duration;
    if (profile.long_job_ratio > 0.42) {
        long long ia = (long long)a.weight * a.duration;
        long long ib = (long long)b.weight * b.duration;
        if (ia != ib) return ia > ib;
    }
    if (profile.cpu_demand_ratio > 0.55 && a.cpu_cores != b.cpu_cores) return a.cpu_cores > b.cpu_cores;
    if (profile.mem_demand_ratio > 0.55 && a.memory != b.memory) return a.memory > b.memory;
    if (profile.avg_min_gpu > 2.5 && a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
    if (profile.vram_pressure > 0.55 && a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
    if (profile.burst_t0_ratio > 0.40 && a.weight != b.weight) return a.weight > b.weight;
    if (profile.hetero_ratio > 2.0 && a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
    if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
    if (a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
    return a.job_id < b.job_id;
}

bool GreedyScheduler::jobMoreUrgentFirst(const Job &a, const Job &b, int order_variant,
                                       long long current_time) const {
    double pa = jobOrderPriority(a, order_variant, current_time);
    double pb = jobOrderPriority(b, order_variant, current_time);
    if (fabs(pa - pb) > 1e-9) return pa > pb;
    return placementTieBreakPrefer(a, b);
}

bool GreedyScheduler::shouldOrderPolish() const {
    if (instanceDifficulty() < 0.36) return false;
    if (isSingleServer()) return jobs.size() <= 200;
    if (isNarrowCluster()) return jobs.size() <= 250;
    if (profile.long_job_ratio > 0.38) return jobs.size() <= 160;
    if (profile.burst_t0_ratio > 0.55) return jobs.size() <= 180;
    if (profile.cpu_demand_ratio > 0.62 || profile.mem_demand_ratio > 0.62) {
        return jobs.size() <= 160;
    }
    return false;
}

bool GreedyScheduler::shouldSingleServerRefine() const {
    return isSingleServer() && jobs.size() > 20 && jobs.size() <= 400;
}

GreedyScheduler::Solution GreedyScheduler::polishOrderVariants(const Solution &best,
                                                               int placement_seed_hint) {
    Solution result = best;
    if (!isScheduleValid(best)) return result;

    int extra_orders[4];
    int n_orders = 2;
    if (isSingleServer()) {
        extra_orders[0] = 1; extra_orders[1] = 3;
        n_orders = (jobs.size() <= 120) ? 3 : 2;
        if (n_orders == 3) extra_orders[2] = 2;
    } else if (profile.long_job_ratio > 0.38) {
        extra_orders[0] = 2; extra_orders[1] = 1; n_orders = 2;
    } else if (profile.cpu_demand_ratio > 0.62 || profile.mem_demand_ratio > 0.62) {
        extra_orders[0] = 3; extra_orders[1] = 1; n_orders = 2;
    } else {
        extra_orders[0] = 1; extra_orders[1] = 3;
    }

    for (int oi = 0; oi < n_orders; ++oi) {
        int ov = extra_orders[oi];
        Solution alt = generateGreedySolutionWithStrategy(placement_seed_hint + ov * 4, ov);
        if (!isScheduleValid(alt)) continue;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(alt);
            if (isBetterOfficialSolution(alt, result)) result = alt;
        } else {
            computeMetrics(alt);
            if (isBetterSolution(alt, result)) result = alt;
        }
    }
    return result;
}

// ---------- 成员 B：实例自适应放置 ----------

double GreedyScheduler::instanceDifficulty() const {
    double d = 0.30 * profile.burst_t0_ratio + 0.35 * profile.vram_pressure;
    if (profile.hetero_ratio > 2.0) {
        d += 0.12 * min(1.0, (profile.hetero_ratio - 2.0) / 3.0);
    }
    if (profile.scarce_80_ratio < 0.30 && profile.hetero_ratio > 1.5) {
        d += 0.10;
    }
    if (profile.gpu_demand_ratio > 0.65) {
        d += 0.15 * min(1.0, (profile.gpu_demand_ratio - 0.65) / 0.35);
    }
    if (profile.avg_feasible > 0.0 && profile.avg_feasible < 8.0) {
        d += 0.08;
    }
    if (profile.cpu_demand_ratio > 0.60) {
        d += 0.06 * min(1.0, (profile.cpu_demand_ratio - 0.60) / 0.40);
    }
    if (profile.mem_demand_ratio > 0.60) {
        d += 0.06 * min(1.0, (profile.mem_demand_ratio - 0.60) / 0.40);
    }
    if (profile.long_job_ratio > 0.40) {
        d += 0.08 * min(1.0, (profile.long_job_ratio - 0.40) / 0.50);
    }
    if (isSingleServer()) d += 0.10;
    return min(1.0, d);
}

int GreedyScheduler::adaptiveStrategyCount() const {
    int base = 4;
    if (jobs.size() <= 20) base = 8;
    else if (jobs.size() <= 100) base = 16;
    else if (jobs.size() <= 500) base = 10;
    else if (isMegascaleInstance()) base = 4;
    else if (isLargeInstance()) base = 5;
    else base = 6;

    int bonus = 0;
    if (isMegascaleInstance()) {
        bonus = 0;
    } else if (jobs.size() > 500) {
        bonus = static_cast<int>(instanceDifficulty() * 2.0 + 0.5);
        bonus = min(bonus, 1);
    } else if (isSingleServer() || profile.long_job_ratio > 0.40) {
        bonus = 1;
    } else if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60 ||
               profile.avg_min_gpu > 2.6) {
        bonus = 1;
    }

    int cap = (jobs.size() <= 100) ? 18 : (jobs.size() <= 500) ? 11
            : isMegascaleInstance() ? 5 : isLargeInstance() ? 6 : 7;
    if (isSingleServer()) {
        if (jobs.size() > 500) {
            bonus = 0;
            cap = 5;
        } else if (jobs.size() > 250) {
            cap = min(cap, 8);
        } else if (jobs.size() <= 500) {
            cap = min(cap + 1, 12);
        }
    }
    int count = min(cap, base + bonus);
    return count;
}

int GreedyScheduler::adaptivePendingCap(int queue_size) const {
    if (queue_size <= 0) return 0;
    if (isSingleServer() && jobs.size() <= 220) return queue_size;
    if (jobs.size() <= 150) return queue_size;

    int cap = 50;
    if (isMegascaleInstance()) cap = 32;
    else if (isLargeInstance()) cap = 38;
    else if (jobs.size() > 500) cap = 44;
    else if (jobs.size() > 200) cap = 68;

    if (!isMegascaleInstance()) {
        if (profile.burst_t0_ratio > 0.45) cap += 6;
        if (profile.burst_t0_ratio > 0.60) cap += 3;
        if (profile.vram_pressure > 0.55) cap += 4;
        if (profile.long_job_ratio > 0.40) cap += 5;
        if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) cap += 4;
        if (!isSingleServer() && profile.long_job_ratio > 0.42 && jobs.size() > 200 && jobs.size() <= 500) {
            cap += 4;
        }
        if (profile.avg_feasible > 0.0 && profile.avg_feasible < 6.0) cap += 4;
    } else if (profile.vram_pressure > 0.55) {
        cap += 2;
    }
    cap = min(cap, isMegascaleInstance() ? 36 : 80);

    return min(queue_size, cap);
}

int GreedyScheduler::resolvePlacementMode(int strategy_seed) const {
    if (jobs.size() <= 80 && instanceDifficulty() < 0.32) {
        return strategy_seed % 4;
    }
    static const int modes_normal[] = {0, 1, 2, 3};
    static const int modes_burst[] = {0, 2, 0, 3};
    static const int modes_vram[] = {3, 1, 3, 0};
    static const int modes_single[] = {3, 1, 3, 1};
    static const int modes_long[] = {1, 3, 1, 2};
    static const int modes_resource[] = {1, 3, 1, 2};
    if (isSingleServer()) return modes_single[strategy_seed % 4];
    if (isNarrowCluster()) return modes_vram[strategy_seed % 4];
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) {
        return modes_resource[strategy_seed % 4];
    }
    if (profile.long_job_ratio > 0.42) return modes_long[strategy_seed % 4];
    const int *table = modes_normal;
    if (profile.burst_t0_ratio > 0.45) table = modes_burst;
    else if (profile.vram_pressure > 0.55) table = modes_vram;
    return table[strategy_seed % 4];
}

bool GreedyScheduler::shouldUseRuntimePareto() const {
    return jobs.size() <= 500;
}

bool GreedyScheduler::shouldLightRefine() const {
    if (jobs.size() > 100 && jobs.size() <= 165) return true;
    if (isSingleServer() && jobs.size() > 100 && jobs.size() <= 180) return true;
    return false;
}

bool GreedyScheduler::shouldLongJobRefine() const {
    if (isSingleServer() || jobs.size() <= 60 || jobs.size() > 400) return false;
    return profile.long_job_ratio > 0.42;
}

bool GreedyScheduler::shouldFastRefine() const {
    if (jobs.size() <= 250 || jobs.size() > 420) return false;
    if (profile.burst_t0_ratio > 0.55 && jobs.size() > 300) return false;
    if (instanceDifficulty() > 0.40) return true;
    if (profile.release_spread_ratio > 0.50 && profile.long_job_ratio > 0.30) return true;
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) return true;
    if (profile.burst_t0_ratio > 0.55 && profile.avg_feasible < 12.0) return true;
    return false;
}

bool GreedyScheduler::shouldUseAssignmentReplayLight() const {
    if (isSingleServer() || isMegascaleInstance()) return false;
    if (jobs.size() < 170 || jobs.size() > 1200) return false;
    if (profile.cpu_demand_ratio > 0.58 || profile.mem_demand_ratio > 0.58) return false;
    return profile.long_job_ratio > 0.38;
}

bool GreedyScheduler::shouldUseLongJobDedicatedPath() const {
    if (isSingleServer() || isMegascaleInstance()) return false;
    if (jobs.size() < 80 || jobs.size() > 1200) return false;
    if (profile.cpu_demand_ratio > 0.58 || profile.mem_demand_ratio > 0.58) return false;
    return profile.long_job_ratio > 0.38;
}

bool GreedyScheduler::shouldUseAssignmentFirstMainPath() const {
    return shouldUseLongJobDedicatedPath();
}

bool GreedyScheduler::shouldUseSingleServerDedicatedPath() const {
    return isSingleServer() && jobs.size() > 40 && jobs.size() <= 350;
}

bool GreedyScheduler::shouldUseMemBoundDedicatedPath() const {
    if (isSingleServer() || isMegascaleInstance()) return false;
    if (jobs.size() < 80 || jobs.size() > 500) return false;
    if (profile.long_job_ratio > 0.42) return false;
    return profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60;
}

bool GreedyScheduler::shouldUseMemBoundMainPath() const {
    if (isSingleServer() || isMegascaleInstance()) return false;
    if (jobs.size() < 80 || jobs.size() > 400) return false;
    if (profile.long_job_ratio > 0.42) return false;
    return profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60;
}

bool GreedyScheduler::shouldUseMegascaleDedicatedPath() const {
    return isMegascaleInstance() && jobs.size() <= 5000;
}

double GreedyScheduler::criticalRatioPriority(const Job &job,
                                              const vector<MachineState> &sim_machines,
                                              long long current_time) const {
    long long est = minEarliestStartForJob(job, sim_machines, current_time);
    if (est >= LLONG_MAX / 8) return -1e18;
    double slack = (double)max(1LL, est - current_time);
    return (double)job.weight / slack;
}

GreedyScheduler::Solution GreedyScheduler::generateCriticalRatioSolution(int strategy_seed) {
    static const int orders[] = {2, 2, 3, 2};
    const int order_variant = orders[strategy_seed % 4];

    generation_critical_ratio_ = true;
    generation_placement_override_ = 1;
    Solution sol = generateGreedySolutionWithStrategy(512 + strategy_seed * 11, order_variant);
    generation_critical_ratio_ = false;
    generation_placement_override_ = -1;
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generatePlacementFirstSolution(int strategy_seed) {
    generation_placement_first_ = true;
    generation_placement_override_ = 1;
    Solution sol = generateGreedySolutionWithStrategy(640 + strategy_seed * 9, 2);
    generation_placement_first_ = false;
    generation_placement_override_ = -1;
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generateListSchedulingSolution(int strategy_seed) {
    Solution sol;
    if (jobs.empty()) return sol;

    vector<int> rank(jobs.size() + 1, jobs.size());
    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const int mode = strategy_seed % 3;
    sort(order.begin(), order.end(), [this, mode](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (mode == 1) {
            double pa = static_cast<double>(a.weight) /
                sqrt(max(1.0, static_cast<double>(a.duration)));
            double pb = static_cast<double>(b.weight) /
                sqrt(max(1.0, static_cast<double>(b.duration)));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else if (mode == 2) {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        } else {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    });
    for (int r = 0; r < (int)order.size(); ++r) {
        rank[jobs[order[r]].job_id] = r;
    }

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<Job> pool;

    const int placement_mode = 1;
    const int sort_strategy = strategy_seed % 4;

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pool.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        sort(pool.begin(), pool.end(), [&](const Job &a, const Job &b) {
            if (rank[a.job_id] != rank[b.job_id]) return rank[a.job_id] < rank[b.job_id];
            return a.job_id < b.job_id;
        });

        vector<double> reservation_scores = buildReservationScores(pool);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : pool) {
            PlacementPick pick = chooseBestPlacement(
                job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
            if (pick.machine_index < 0 ||
                !sim_machines[pick.machine_index].canStart(job, pick.gpu_used)) {
                deferred.push_back(job);
                continue;
            }
            long long start_t = sim_machines[pick.machine_index].earliestFeasibleStart(
                job, pick.gpu_used, current_time);
            auto result = sim_machines[pick.machine_index].startJob(
                job, start_t, pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        pool = std::move(deferred);
        if (!pool.empty()) {
            sort(pool.begin(), pool.end(),
                 [this, &sim_machines, &current_time, &rank](const Job &a, const Job &b) {
                long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                if (ea != eb) return ea < eb;
                if (rank[a.job_id] != rank[b.job_id]) return rank[a.job_id] < rank[b.job_id];
                return a.job_id < b.job_id;
            });
        }

        if (scheduled_one && !pool.empty()) continue;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generateListSchedulingSolutionFast(int strategy_seed) {
    Solution sol;
    if (jobs.empty()) return sol;

    vector<int> rank(jobs.size() + 1, jobs.size());
    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const int mode = strategy_seed % 3;
    sort(order.begin(), order.end(), [this, mode](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (mode == 2) {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        } else if (mode == 1) {
            double pa = static_cast<double>(a.weight) /
                sqrt(max(1.0, static_cast<double>(a.duration)));
            double pb = static_cast<double>(b.weight) /
                sqrt(max(1.0, static_cast<double>(b.duration)));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    });
    for (int r = 0; r < (int)order.size(); ++r) {
        rank[jobs[order[r]].job_id] = r;
    }

    const int pool_cap = (jobs.size() > 2500) ? 10 : (jobs.size() > 2000) ? 14 : (isMegascaleInstance() ? 32 : 48);
    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<Job> pool;
    int mega_spin_rounds = 0;

    const int placement_mode = 1;
    const int sort_strategy = strategy_seed % 4;
    const bool mega_fast = isMegascaleInstance() && jobs.size() > 2000;
    auto rank_cmp = [&](const Job &a, const Job &b) {
        if (rank[a.job_id] != rank[b.job_id]) return rank[a.job_id] < rank[b.job_id];
        return a.job_id < b.job_id;
    };

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pool.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        vector<Job> to_try;
        vector<Job> held_back;
        if ((int)pool.size() <= pool_cap) {
            sort(pool.begin(), pool.end(), rank_cmp);
            to_try = std::move(pool);
        } else {
            nth_element(pool.begin(), pool.begin() + pool_cap, pool.end(), rank_cmp);
            to_try.assign(pool.begin(), pool.begin() + pool_cap);
            held_back.assign(pool.begin() + pool_cap, pool.end());
            sort(to_try.begin(), to_try.end(), rank_cmp);
        }
        pool.clear();

        vector<double> reservation_scores = buildReservationScores(to_try);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : to_try) {
            PlacementPick pick = chooseBestPlacement(
                job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
            if (pick.machine_index < 0 ||
                !sim_machines[pick.machine_index].canStart(job, pick.gpu_used)) {
                deferred.push_back(job);
                continue;
            }
            long long start_t = sim_machines[pick.machine_index].earliestFeasibleStart(
                job, pick.gpu_used, current_time);
            auto result = sim_machines[pick.machine_index].startJob(
                job, start_t, pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        deferred.insert(deferred.end(), held_back.begin(), held_back.end());
        pool = std::move(deferred);
        if (!pool.empty()) {
            if (mega_fast) {
                sort(pool.begin(), pool.end(), rank_cmp);
            } else {
                vector<long long> est_cache(pool.size());
                for (size_t i = 0; i < pool.size(); ++i) {
                    est_cache[i] = minEarliestStartForJob(pool[i], sim_machines, current_time);
                }
                vector<int> perm(pool.size());
                for (size_t i = 0; i < perm.size(); ++i) perm[i] = (int)i;
                sort(perm.begin(), perm.end(), [&](int ia, int ib) {
                    if (est_cache[ia] != est_cache[ib]) return est_cache[ia] < est_cache[ib];
                    if (rank[pool[ia].job_id] != rank[pool[ib].job_id]) {
                        return rank[pool[ia].job_id] < rank[pool[ib].job_id];
                    }
                    return pool[ia].job_id < pool[ib].job_id;
                });
                vector<Job> sorted;
                sorted.reserve(pool.size());
                for (int idx : perm) sorted.push_back(pool[idx]);
                pool = std::move(sorted);
            }
        }

        if (scheduled_one && !pool.empty()) {
            if (mega_fast) {
                const int spin_cap = (jobs.size() > 2500) ? 2 : 3;
                if (++mega_spin_rounds < spin_cap) continue;
                mega_spin_rounds = 0;
            } else {
                continue;
            }
        } else {
            mega_spin_rounds = 0;
        }

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleCriticalRatioList(int strategy_seed) {
    Solution sol;
    if (jobs.empty()) return sol;

    const int pool_cap = (jobs.size() > 3500) ? 8
        : (jobs.size() > 2500) ? 10 : (jobs.size() > 2000) ? 14 : 24;
    const int prefilter = min((int)jobs.size(), pool_cap * 3);

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<Job> pool;
    int spin_rounds = 0;

    const int placement_mode = 1;
    const int sort_strategy = strategy_seed % 4;

    auto impact_cmp = [](const Job &a, const Job &b) {
        long long wa = (long long)a.weight * a.duration;
        long long wb = (long long)b.weight * b.duration;
        if (wa != wb) return wa > wb;
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    };

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pool.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        vector<Job> candidates = pool;
        if ((int)candidates.size() > prefilter) {
            nth_element(candidates.begin(), candidates.begin() + prefilter,
                        candidates.end(), impact_cmp);
            candidates.resize(prefilter);
        }

        vector<double> cr_scores(candidates.size());
        for (size_t i = 0; i < candidates.size(); ++i) {
            cr_scores[i] = criticalRatioPriority(candidates[i], sim_machines, current_time);
        }

        vector<int> perm(candidates.size());
        for (size_t i = 0; i < perm.size(); ++i) perm[i] = (int)i;
        sort(perm.begin(), perm.end(), [&](int ia, int ib) {
            if (fabs(cr_scores[ia] - cr_scores[ib]) > 1e-9) return cr_scores[ia] > cr_scores[ib];
            return impact_cmp(candidates[ia], candidates[ib]);
        });

        vector<Job> to_try;
        vector<Job> held_back;
        if ((int)candidates.size() <= pool_cap) {
            for (int idx : perm) to_try.push_back(candidates[idx]);
        } else {
            for (int i = 0; i < pool_cap; ++i) to_try.push_back(candidates[perm[i]]);
        }

        unordered_set<int> try_ids;
        for (const auto &j : to_try) try_ids.insert(j.job_id);
        for (const auto &j : pool) {
            if (!try_ids.count(j.job_id)) held_back.push_back(j);
        }
        pool.clear();

        vector<double> reservation_scores = buildReservationScores(to_try);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : to_try) {
            PlacementPick pick = chooseBestPlacement(
                job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
            if (pick.machine_index < 0 ||
                !sim_machines[pick.machine_index].canStart(job, pick.gpu_used)) {
                deferred.push_back(job);
                continue;
            }
            long long start_t = sim_machines[pick.machine_index].earliestFeasibleStart(
                job, pick.gpu_used, current_time);
            auto result = sim_machines[pick.machine_index].startJob(
                job, start_t, pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        deferred.insert(deferred.end(), held_back.begin(), held_back.end());
        pool = std::move(deferred);
        if (!pool.empty()) sort(pool.begin(), pool.end(), impact_cmp);

        if (scheduled_one && !pool.empty()) {
            const int spin_cap = (jobs.size() > 3000) ? 2 : 3;
            if (++spin_rounds < spin_cap) continue;
            spin_rounds = 0;
        } else {
            spin_rounds = 0;
        }

        vector<long long> time_candidates;
        if (next_job_index < (int)jobs.size())
            time_candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            time_candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : time_candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

vector<long long> GreedyScheduler::buildReleaseWaveQuantiles(int buckets) const {
    vector<long long> releases;
    releases.reserve(jobs.size());
    for (const auto &job : jobs) releases.push_back(job.release_time);
    if (releases.empty()) return {};
    sort(releases.begin(), releases.end());
    releases.erase(unique(releases.begin(), releases.end()), releases.end());
    if ((int)releases.size() <= 1 || buckets <= 1) return releases;

    vector<long long> quantiles;
    quantiles.reserve(buckets - 1);
    for (int b = 1; b < buckets; ++b) {
        const size_t idx = min(releases.size() - 1,
            (releases.size() * (size_t)b) / (size_t)buckets);
        quantiles.push_back(releases[idx]);
    }
    return quantiles;
}

int GreedyScheduler::releaseWaveBucket(long long release_time,
                                       const vector<long long> &quantiles) const {
    int bucket = 0;
    for (long long q : quantiles) {
        if (release_time > q) ++bucket;
        else break;
    }
    return bucket;
}

bool GreedyScheduler::jobCanUsePlacement(int job_id, int server_id, int gpu_used) const {
    auto fm = feasible_machines.find(job_id);
    if (fm == feasible_machines.end()) return false;
    for (const auto &entry : fm->second) {
        if (entry.first < 0 || entry.first >= (int)machines.size()) continue;
        if (machines[entry.first].spec.server_id == server_id &&
            entry.second == gpu_used) {
            return true;
        }
    }
    return false;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleCoreAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    vector<long long> impacts;
    impacts.reserve(jobs.size());
    for (const auto &job : jobs) {
        impacts.push_back((long long)job.weight * job.duration);
    }
    const size_t mid = impacts.size() / 2;
    nth_element(impacts.begin(), impacts.begin() + mid, impacts.end());
    const long long impact_threshold = impacts[mid];

    const double burst_w = profile.burst_t0_ratio;
    const double spread_w = profile.release_spread_ratio;
    const vector<long long> wave_quantiles = buildReleaseWaveQuantiles(
        burst_w > 0.45 ? 5 : (spread_w > 0.50 ? 4 : 3));
    const bool wave_first = burst_w > 0.28 || spread_w > 0.42;
    const int sort_mode = strategy_seed % 3;

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [this, sort_mode, wave_first, burst_w,
            &wave_quantiles](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (wave_first && !wave_quantiles.empty()) {
            int ba = releaseWaveBucket(a.release_time, wave_quantiles);
            int bb = releaseWaveBucket(b.release_time, wave_quantiles);
            if (ba != bb) return ba < bb;
        } else if (wave_first && a.release_time != b.release_time) {
            return a.release_time < b.release_time;
        }
        if (sort_mode == 1) {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        } else if (sort_mode == 2) {
            double pa = (double)a.weight / max(1.0, sqrt((double)a.duration));
            double pb = (double)b.weight / max(1.0, sqrt((double)b.duration));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        }
        if (!wave_first && a.release_time != b.release_time) {
            return a.release_time < b.release_time;
        }
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double avg_dur = max(1.0, profile.avg_duration);
    const double wait_boost = min(1.0, 0.52 + 0.24 * burst_w + 0.10 * spread_w);
    const double load_coef = 0.30 + 0.20 * burst_w;
    const double spread_coef = 0.14 + 0.26 * burst_w;

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        const long long job_impact = (long long)job.weight * job.duration;
        const bool wait_primary = job_impact >= impact_threshold;
        const bool spread_job = wait_primary && burst_w > 0.25;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long max_load = 1;
        for (long long w : projected_gpu_work) max_load = max(max_load, w);

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                const double wait_cost =
                    (double)job.weight * max(0LL, est - job.release_time);
                const double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                const double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                const double impact_w = min(1.0, (double)job_impact /
                    max(1.0, avg_dur * avg_dur * 40.0));

                double cost;
                if (wait_primary) {
                    cost = wait_cost + 0.06 * pc * norm + 0.04 * load * impact_w * norm;
                } else {
                    cost = pc + wait_boost * 0.38 * wait_cost / max(1.0, norm) +
                           load_coef * load * impact_w;
                }
                if (spread_job) {
                    double rel_load = (double)projected_gpu_work[mi] / (double)max_load;
                    cost += spread_coef * rel_load * rel_load * impact_w * norm;
                }

                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        long long start_t = sim_machines[best_pick.machine_index].earliestFeasibleStart(
            job, best_pick.gpu_used, job_time);
        auto result = sim_machines[best_pick.machine_index].startJob(
            job, start_t, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runMegascaleAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleWaveDispatchScheduler(
    int policy_seed) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    const double burst_w = profile.burst_t0_ratio;
    const double spread_w = profile.release_spread_ratio;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double load_coef = 0.05 + 0.12 * burst_w;
    const double spread_coef = 0.08 + 0.20 * burst_w;
    const double wait_scale = (policy_seed % 3 == 0) ? 1.0
        : (policy_seed % 3 == 1) ? 0.92 : 1.08;
    const int prefilter_k = (jobs.size() > 4200) ? 14
        : (jobs.size() > 3000) ? 20 : 28;

    vector<long long> machine_gpu_work(machines.size(), 0);
    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<Job> pool;

    auto remove_from_pool = [&](int job_id) {
        for (size_t i = 0; i < pool.size(); ++i) {
            if (pool[i].job_id == job_id) {
                pool[i] = pool.back();
                pool.pop_back();
                return;
            }
        }
    };

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        while (!pool.empty()) {
            vector<int> cand_idx(pool.size());
            for (int i = 0; i < (int)pool.size(); ++i) cand_idx[i] = i;
            if ((int)cand_idx.size() > prefilter_k) {
                nth_element(cand_idx.begin(), cand_idx.begin() + prefilter_k, cand_idx.end(),
                    [this, &pool, current_time](int ia, int ib) {
                        const Job &a = pool[ia];
                        const Job &b = pool[ib];
                        long long wa = (long long)a.weight * a.duration;
                        long long wb = (long long)b.weight * b.duration;
                        if (wa != wb) return wa > wb;
                        long long da = (long long)a.weight *
                            max(0LL, current_time - a.release_time);
                        long long db = (long long)b.weight *
                            max(0LL, current_time - b.release_time);
                        if (da != db) return da > db;
                        return a.job_id < b.job_id;
                    });
                cand_idx.resize(prefilter_k);
            }

            int best_pi = -1;
            double best_urgency = -1.0;
            for (int pi : cand_idx) {
                const Job &job = pool[pi];
                auto fm = feasible_machines.find(job.job_id);
                if (fm == feasible_machines.end()) continue;
                bool any = false;
                for (const auto &entry : fm->second) {
                    if (sim_machines[entry.first].canStart(job, entry.second)) {
                        any = true;
                        break;
                    }
                }
                if (!any) continue;

                double rank_score = -1.0;
                if (spread_w > 0.38 || (policy_seed % 3) == 2) {
                    rank_score = criticalRatioPriority(job, sim_machines, current_time);
                    if (rank_score < -1e17) continue;
                } else {
                    long long est = minEarliestStartForJob(job, sim_machines, current_time);
                    if (est >= LLONG_MAX / 8) continue;
                    rank_score = (double)job.weight * max(0LL, est - job.release_time) +
                        0.001 * (double)job.weight * job.duration;
                }

                if (rank_score > best_urgency + 1e-9 ||
                    (fabs(rank_score - best_urgency) <= 1e-9 &&
                        (best_pi < 0 || job.job_id < pool[best_pi].job_id))) {
                    best_urgency = rank_score;
                    best_pi = pi;
                }
            }
            if (best_pi < 0) break;

            const Job &job = pool[best_pi];
            auto fm = feasible_machines.find(job.job_id);
            if (fm == feasible_machines.end()) {
                remove_from_pool(job.job_id);
                continue;
            }

            long long max_load = 1;
            for (long long w : machine_gpu_work) max_load = max(max_load, w);

            PlacementPick best_pick = {-1, -1};
            double best_cost = 1e18;
            long long best_start = LLONG_MAX;
            vector<double> empty_res(machines.size(), 0.0);
            const long long impact = (long long)job.weight * job.duration;

            for (const auto &entry : fm->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canStart(job, gpu)) continue;
                long long est = sim_machines[mi].earliestFeasibleStart(
                    job, gpu, current_time);
                if (est > LLONG_MAX / 8) continue;

                const long long wait = (long long)job.weight *
                    max(0LL, est - job.release_time);
                double cost = wait_scale * (double)wait;

                if (burst_w > 0.22) {
                    double load = (double)machine_gpu_work[mi] /
                        max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                    cost += load_coef * load * min(1.0,
                        (double)impact / max(1.0, norm * profile.avg_duration));
                }
                if (burst_w > 0.28) {
                    double rel = (double)machine_gpu_work[mi] / (double)max_load;
                    cost += spread_coef * rel * rel * min(1.0,
                        (double)impact / max(1.0, profile.avg_duration * profile.avg_duration * 20.0));
                }
                if (spread_w > 0.42) {
                    cost += 0.025 * placementCost(job, mi, gpu, sim_machines, est,
                        empty_res) * norm;
                }
                if (job.weight * job.duration >= (long long)profile.avg_duration * job.weight * 2) {
                    double load = (double)machine_gpu_work[mi] /
                        max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                    cost += 0.04 * load * (double)job.weight;
                }

                if (cost < best_cost - 1e-9 ||
                    (fabs(cost - best_cost) <= 1e-9 && est < best_start)) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }

            if (best_pick.machine_index < 0) break;
            auto result = sim_machines[best_pick.machine_index].startJob(
                job, best_start, best_pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                result.second.job_id, result.second});
            machine_gpu_work[best_pick.machine_index] +=
                (long long)job.duration * best_pick.gpu_used;
            remove_from_pool(job.job_id);
        }

        if ((int)records.size() >= (int)jobs.size()) break;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleUnifiedSolution() {
    Solution best;
    bool has_best = false;
    auto consider = [&](Solution sol) {
        if (sol.records.size() != jobs.size()) return;
        computeMetrics(sol);
        if (!has_best) {
            best = sol;
            has_best = true;
            return;
        }
        if (isBetterSolution(sol, best)) best = sol;
    };

    consider(generateMegascaleDedicatedSolution());

    if (!has_best) {
        Solution fb = generateGreedySolutionWithStrategy(42, 2);
        if (fb.records.size() == jobs.size()) consider(fb);
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleWaveAssignReplay(
    int policy_seed) {
    Solution wave = generateMegascaleWaveDispatchScheduler(policy_seed);
    if (wave.records.size() != jobs.size()) return wave;

    Solution best = wave;
    computeMetrics(best);

    static const int modes[] = {4, 0, 2};
    const int n = (jobs.size() > 4200) ? 1 : 2;
    for (int i = 0; i < n; ++i) {
        Solution alt = wave;
        replayMegascaleAssignmentList(alt, modes[i]);
        if (alt.records.size() != jobs.size()) continue;
        computeMetrics(alt);
        if (isBetterSolution(alt, best)) best = alt;
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleIntegratedTimeline(
    int policy_seed) {
    Solution timeline = generateMegascaleWaveDispatchScheduler(policy_seed);
    if (timeline.records.size() != jobs.size()) return timeline;
    return runMegascaleAssignmentFirstPipeline(timeline, 1);
}

GreedyScheduler::Solution GreedyScheduler::refineMegascaleReplayBeamSearch(
    const Solution &initial, int budget_ms) {
    if (!isMegascaleInstance() || budget_ms <= 0) return initial;
    if (initial.records.size() != jobs.size() || !isScheduleValid(initial)) return initial;
    if (jobs.size() < 2800 || jobs.size() > 3800) return initial;

    const auto deadline = chrono::steady_clock::now() +
        chrono::milliseconds(budget_ms);

    Solution best = initial;
    computeMetrics(best);

    struct JobMove {
        size_t rec_idx = 0;
        int server_id = -1;
        int gpu = -1;
    };

    vector<size_t> job_order(initial.records.size());
    for (size_t i = 0; i < job_order.size(); ++i) job_order[i] = i;
    sort(job_order.begin(), job_order.end(), [this, &initial](size_t a, size_t b) {
        const Job *ja = job_by_id.count(initial.records[a].job_id)
            ? job_by_id.at(initial.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(initial.records[b].job_id)
            ? job_by_id.at(initial.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight *
            max(0LL, initial.records[a].start_time - ja->release_time);
        long long wb = (long long)jb->weight *
            max(0LL, initial.records[b].start_time - jb->release_time);
        if (wa != wb) return wa > wb;
        long long ia = (long long)ja->weight * ja->duration;
        long long ib = (long long)jb->weight * jb->duration;
        if (ia != ib) return ia > ib;
        return ja->job_id < jb->job_id;
    });

    const int top_jobs = (jobs.size() > 3200) ? 4 : 5;
    const int beam_width = (jobs.size() > 3200) ? 3 : 4;
    const int depth = (jobs.size() > 3200) ? 2 : 3;
    const int replay_mode = 4;

    if ((int)job_order.size() > top_jobs) job_order.resize(top_jobs);

    vector<vector<JobMove>> layers;
    layers.reserve(job_order.size());
    for (size_t idx : job_order) {
        if (chrono::steady_clock::now() >= deadline) break;
        const int job_id = initial.records[idx].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;

        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        vector<JobMove> moves;
        JobMove cur;
        cur.rec_idx = idx;
        cur.server_id = initial.records[idx].server_id;
        cur.gpu = initial.records[idx].gpu_used;
        moves.push_back(cur);

        struct ScoredMove {
            JobMove move;
            double ww = 1e18;
        };
        vector<ScoredMove> scored;
        scored.reserve(fm->second.size());

        for (const auto &entry : fm->second) {
            if (entry.first < 0 || entry.first >= (int)machines.size()) continue;
            if (machines[entry.first].spec.server_id == cur.server_id &&
                entry.second == cur.gpu) {
                continue;
            }
            if (!jobCanUsePlacement(job_id, machines[entry.first].spec.server_id,
                                   entry.second)) {
                continue;
            }
            ScoredMove sm;
            sm.move.rec_idx = idx;
            sm.move.server_id = machines[entry.first].spec.server_id;
            sm.move.gpu = entry.second;

            Solution trial = initial;
            trial.records[idx].server_id = sm.move.server_id;
            trial.records[idx].gpu_used = sm.move.gpu;
            replayMegascaleAssignmentList(trial, replay_mode);
            if (trial.records.size() != jobs.size()) continue;
            computeMetrics(trial);
            sm.ww = trial.weighted_waiting;
            scored.push_back(sm);
            if (isBetterSolution(trial, best)) best = trial;
        }

        sort(scored.begin(), scored.end(), [](const ScoredMove &a, const ScoredMove &b) {
            return a.ww < b.ww;
        });
        const int alt_cap = (jobs.size() > 3200) ? 2 : 3;
        for (int i = 0; i < min(alt_cap, (int)scored.size()); ++i) {
            moves.push_back(scored[i].move);
        }
        if (moves.size() > 1) layers.push_back(std::move(moves));
    }

    if (layers.empty()) return best;

    vector<Solution> beam;
    beam.push_back(initial);
    computeMetrics(beam[0]);

    const int max_depth = min(depth, (int)layers.size());
    for (int d = 0; d < max_depth; ++d) {
        if (chrono::steady_clock::now() >= deadline) break;
        vector<Solution> next_beam;
        next_beam.reserve(beam.size() * layers[d].size());

        for (const Solution &state : beam) {
            if (chrono::steady_clock::now() >= deadline) break;
            for (const JobMove &mv : layers[d]) {
                Solution trial = state;
                trial.records[mv.rec_idx].server_id = mv.server_id;
                trial.records[mv.rec_idx].gpu_used = mv.gpu;
                replayMegascaleAssignmentList(trial, replay_mode);
                if (trial.records.size() != jobs.size()) continue;
                computeMetrics(trial);
                if (isBetterSolution(trial, best)) best = trial;
                next_beam.push_back(trial);
            }
        }
        if (next_beam.empty()) break;

        sort(next_beam.begin(), next_beam.end(), [this](const Solution &a, const Solution &b) {
            Solution ca = a;
            Solution cb = b;
            computeMetrics(ca);
            computeMetrics(cb);
            return ca.weighted_waiting < cb.weighted_waiting;
        });
        if ((int)next_beam.size() > beam_width) next_beam.resize(beam_width);
        beam = std::move(next_beam);
    }

    return best;
}

void GreedyScheduler::polishMegascaleLight(Solution &best) {
    if (!isMegascaleInstance() || !isScheduleValid(best) || best.records.empty()) return;

    Solution result = best;
    computeMetrics(result);

    static const int modes[] = {4, 0};
    const int n = (jobs.size() > 4200) ? 1 : 2;
    for (int i = 0; i < n; ++i) {
        Solution alt = best;
        replayMegascaleAssignmentList(alt, modes[i]);
        if (alt.records.size() != jobs.size()) continue;
        computeMetrics(alt);
        if (isBetterSolution(alt, result)) result = alt;
    }
    best = result;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleBalancedAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty()) return sol;

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const bool alt_sort = (strategy_seed % 2) == 1;
    sort(order.begin(), order.end(), [this, alt_sort](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (alt_sort) {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double avg_dur = max(1.0, profile.avg_duration);

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                double cost = pc;
                double wait = (double)(est - job.release_time) / norm;
                cost += 0.45 * wait * min(1.0, (double)job.weight);
                double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                cost += 0.38 * load * min(1.0, (double)job.weight * job.duration /
                    max(1.0, avg_dur * avg_dur * 40.0));
                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleAdaptiveWaitAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    vector<long long> impacts;
    impacts.reserve(jobs.size());
    for (const auto &job : jobs) {
        impacts.push_back((long long)job.weight * job.duration);
    }
    const size_t mid = impacts.size() / 2;
    nth_element(impacts.begin(), impacts.begin() + mid, impacts.end());
    const long long impact_threshold = impacts[mid];

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const int sort_mode = strategy_seed % 3;
    sort(order.begin(), order.end(), [this, sort_mode](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (sort_mode == 1) {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        } else if (sort_mode == 2) {
            double pa = (double)a.weight / max(1.0, sqrt((double)a.duration));
            double pb = (double)b.weight / max(1.0, sqrt((double)b.duration));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double avg_dur = max(1.0, profile.avg_duration);
    const double wait_boost = min(1.0, 0.55 + 0.25 * profile.burst_t0_ratio);

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        const long long job_impact = (long long)job.weight * job.duration;
        const bool wait_primary = job_impact >= impact_threshold;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                const double wait_cost =
                    (double)job.weight * max(0LL, est - job.release_time);
                const double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                const double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                const double impact_w = min(1.0, (double)job_impact /
                    max(1.0, avg_dur * avg_dur * 40.0));

                double cost;
                if (wait_primary) {
                    cost = wait_cost + 0.07 * pc * norm + 0.05 * load * impact_w * norm;
                } else {
                    cost = pc + wait_boost * 0.40 * wait_cost / max(1.0, norm) +
                           0.34 * load * impact_w;
                }

                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runMegascaleAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleProfileAwareAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    vector<long long> impacts;
    impacts.reserve(jobs.size());
    for (const auto &job : jobs) {
        impacts.push_back((long long)job.weight * job.duration);
    }
    const size_t mid = impacts.size() / 2;
    nth_element(impacts.begin(), impacts.begin() + mid, impacts.end());
    const long long impact_threshold = impacts[mid];

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;

    const double burst_w = profile.burst_t0_ratio;
    const double spread_w = profile.release_spread_ratio;
    const vector<long long> wave_quantiles = buildReleaseWaveQuantiles(
        burst_w > 0.45 ? 5 : (spread_w > 0.50 ? 4 : 3));
    const bool wave_release_first = burst_w > 0.28 || spread_w > 0.42;
    const bool spread_heavy = spread_w > 0.48;
    const int sort_mode = strategy_seed % 3;

    sort(order.begin(), order.end(), [this, sort_mode, wave_release_first, spread_heavy,
            &wave_quantiles](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (wave_release_first && !wave_quantiles.empty()) {
            int ba = releaseWaveBucket(a.release_time, wave_quantiles);
            int bb = releaseWaveBucket(b.release_time, wave_quantiles);
            if (ba != bb) return ba < bb;
        } else if (wave_release_first) {
            if (a.release_time != b.release_time) return a.release_time < b.release_time;
        }
        if (sort_mode == 1 || spread_heavy) {
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.duration != b.duration) return a.duration < b.duration;
        } else if (sort_mode == 2) {
            double pa = (double)a.weight / max(1.0, sqrt((double)a.duration));
            double pb = (double)b.weight / max(1.0, sqrt((double)b.duration));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        }
        if (!wave_release_first && a.release_time != b.release_time) {
            return a.release_time < b.release_time;
        }
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double avg_dur = max(1.0, profile.avg_duration);
    const double wait_coef = 0.36 + 0.28 * burst_w + 0.14 * spread_w;
    const double load_coef = 0.26 + 0.22 * burst_w;
    const double spread_coef = 0.12 + 0.28 * burst_w;
    const double wait_primary_boost = min(1.0, 0.50 + 0.22 * burst_w);

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        const long long job_impact = (long long)job.weight * job.duration;
        const bool wait_primary = job_impact >= impact_threshold;
        const bool spread_job = wait_primary && burst_w > 0.25;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        long long max_load = 1;
        for (long long w : projected_gpu_work) max_load = max(max_load, w);

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                const double wait_cost =
                    (double)job.weight * max(0LL, est - job.release_time);
                const double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                const double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                const double impact_w = min(1.0, (double)job_impact /
                    max(1.0, avg_dur * avg_dur * 40.0));

                double cost;
                if (wait_primary) {
                    cost = wait_cost + 0.06 * pc * norm + 0.04 * load * impact_w * norm;
                } else {
                    cost = pc + wait_coef * wait_cost / max(1.0, norm) +
                           load_coef * load * impact_w;
                }
                if (spread_job) {
                    double rel_load = (double)projected_gpu_work[mi] / (double)max_load;
                    cost += spread_coef * rel_load * rel_load * impact_w * norm;
                }
                if (!wait_primary && spread_heavy) {
                    cost += wait_primary_boost * 0.18 * wait_cost / max(1.0, norm);
                }

                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runMegascaleAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleBurstWaveAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;
    if (profile.burst_t0_ratio < 0.35) return sol;

    vector<long long> impacts;
    impacts.reserve(jobs.size());
    for (const auto &job : jobs) {
        impacts.push_back((long long)job.weight * job.duration);
    }
    const size_t mid = impacts.size() / 2;
    nth_element(impacts.begin(), impacts.begin() + mid, impacts.end());
    const long long impact_threshold = impacts[mid];

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const bool weight_first = (strategy_seed % 2) == 0;
    sort(order.begin(), order.end(), [this, weight_first](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        if (weight_first) {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        } else if (a.weight != b.weight) {
            return a.weight > b.weight;
        }
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);
    const double avg_dur = max(1.0, profile.avg_duration);
    const double burst_w = profile.burst_t0_ratio;
    const double wait_coef = 0.38 + 0.40 * burst_w;
    const double spread_coef = 0.30 + 0.25 * burst_w;

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        const long long job_impact = (long long)job.weight * job.duration;
        const bool spread_job = job_impact >= impact_threshold;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        long long max_load = 1;
        for (long long w : projected_gpu_work) max_load = max(max_load, w);

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                double cost = pc;
                const double wait = (double)(est - job.release_time) / norm;
                cost += wait_coef * wait * min(1.0, (double)job.weight);
                double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                cost += 0.32 * load * min(1.0, (double)job_impact /
                    max(1.0, avg_dur * avg_dur * 40.0));
                if (spread_job) {
                    double rel_load = (double)projected_gpu_work[mi] / (double)max_load;
                    cost += spread_coef * rel_load * min(1.0, (double)job.weight);
                }
                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runMegascaleAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::mergeMegascaleDualAssignment(
    const Solution &balanced_src, const Solution &adaptive_src) {
    if (balanced_src.records.size() != jobs.size() ||
        adaptive_src.records.size() != jobs.size()) {
        return pickBetterSolution(balanced_src, adaptive_src);
    }

    Solution trial = balanced_src;
    unordered_map<int, pair<int, int>> adaptive_assign;
    for (const auto &rec : adaptive_src.records) {
        adaptive_assign[rec.job_id] = {rec.server_id, rec.gpu_used};
    }

    vector<int> impact_ids;
    impact_ids.reserve(trial.records.size());
    for (const auto &rec : trial.records) impact_ids.push_back(rec.job_id);
    const int top_k = max(8, (int)(jobs.size() * 0.015));
    sort(impact_ids.begin(), impact_ids.end(), [this](int a, int b) {
        const Job *ja = job_by_id.count(a) ? job_by_id.at(a) : nullptr;
        const Job *jb = job_by_id.count(b) ? job_by_id.at(b) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight * ja->duration;
        long long wb = (long long)jb->weight * jb->duration;
        if (wa != wb) return wa > wb;
        return ja->job_id < jb->job_id;
    });
    if ((int)impact_ids.size() > top_k) impact_ids.resize(top_k);

    unordered_map<int, size_t> record_index;
    for (size_t i = 0; i < trial.records.size(); ++i) {
        record_index[trial.records[i].job_id] = i;
    }

    for (int job_id : impact_ids) {
        auto adp_it = adaptive_assign.find(job_id);
        auto idx_it = record_index.find(job_id);
        if (adp_it == adaptive_assign.end() || idx_it == record_index.end()) continue;

        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;

        const auto &cur = trial.records[idx_it->second];
        if (cur.server_id == adp_it->second.first &&
            cur.gpu_used == adp_it->second.second) {
            continue;
        }

        auto cur_mach = machine_index_by_id.find(cur.server_id);
        auto adp_mach = machine_index_by_id.find(adp_it->second.first);
        if (cur_mach == machine_index_by_id.end() ||
            adp_mach == machine_index_by_id.end()) {
            continue;
        }

        long long cur_est = machines[cur_mach->second].earliestFeasibleStart(
            *job, cur.gpu_used, job->release_time);
        long long adp_est = machines[adp_mach->second].earliestFeasibleStart(
            *job, adp_it->second.second, job->release_time);

        long long cur_wait = (long long)job->weight * max(0LL, cur_est - job->release_time);
        long long adp_wait = (long long)job->weight * max(0LL, adp_est - job->release_time);
        if (adp_wait < cur_wait) {
            trial.records[idx_it->second].server_id = adp_it->second.first;
            trial.records[idx_it->second].gpu_used = adp_it->second.second;
        }
    }

    replayMegascaleAssignmentList(trial, (jobs.size() > 4200) ? 0 : 4);
    if (trial.records.size() != jobs.size()) return balanced_src;

    computeMetrics(trial);
    Solution best = balanced_src;
    computeMetrics(best);
    if (isBetterSolution(trial, best)) best = trial;

    Solution adaptive_copy = adaptive_src;
    computeMetrics(adaptive_copy);
    if (isBetterSolution(adaptive_copy, best)) best = adaptive_copy;
    return best;
}

GreedyScheduler::PlacementPick GreedyScheduler::chooseWaitOptPlacement(
    const Job &job,
    const vector<MachineState> &sim_machines,
    long long current_time,
    const vector<double> &reservation_scores) const {
    PlacementPick best = {-1, -1};
    double best_cost = 1e18;
    long long best_start = LLONG_MAX;

    auto entries_it = feasible_machines.find(job.job_id);
    if (entries_it == feasible_machines.end()) return best;

    const double norm = max(1.0, (double)profile.time_horizon);
    for (const auto &entry : entries_it->second) {
        int mi = entry.first;
        int gpu = entry.second;
        if (!sim_machines[mi].canEverRun(job, gpu)) continue;

        long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, current_time);
        if (est > LLONG_MAX / 8) continue;

        const double wait_cost =
            (double)job.weight * max(0LL, est - job.release_time);
        const double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
        const double load =
            (double)sim_machines[mi].totalRemainingGpuWork(current_time) /
            max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
        const double impact_w = min(1.0, (double)job.weight * job.duration /
            max(1.0, profile.avg_duration * profile.avg_duration * 40.0));
        const double cost = wait_cost + 0.08 * pc * norm + 0.06 * load * impact_w * norm;

        if (cost < best_cost - 1e-9 ||
            (fabs(cost - best_cost) <= 1e-9 && est < best_start)) {
            best_cost = cost;
            best = {mi, gpu};
            best_start = est;
        }
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleWaitOptAssignment(
    int strategy_seed, int max_replays) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const int sort_mode = strategy_seed % 3;
    sort(order.begin(), order.end(), [this, sort_mode](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (sort_mode == 0) {
            if (a.release_time != b.release_time) return a.release_time < b.release_time;
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        } else if (sort_mode == 1) {
            double pa = (double)a.weight / max(1.0, sqrt((double)a.duration));
            double pb = (double)b.weight / max(1.0, sqrt((double)b.duration));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
            if (a.release_time != b.release_time) return a.release_time < b.release_time;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
            if (a.release_time != b.release_time) return a.release_time < b.release_time;
        }
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = max((long long)job.release_time, (long long)jobs.front().release_time);
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                const double wait_cost =
                    (double)job.weight * max(0LL, est - job.release_time);
                const double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                const double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                const double impact_w = min(1.0, (double)job.weight * job.duration /
                    max(1.0, profile.avg_duration * profile.avg_duration * 40.0));
                const double cost = wait_cost + 0.08 * pc * norm + 0.06 * load * impact_w * norm;

                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;
    return runAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleOnlineWaitList(int strategy_seed) {
    Solution sol;
    if (jobs.empty() || !isMegascaleInstance()) return sol;

    const int pool_cap = (jobs.size() > 4500) ? 8
        : (jobs.size() > 2800) ? 10 : (jobs.size() > 2000) ? 14 : 24;
    const int prefilter = min((int)jobs.size(), pool_cap * 3);
    const int variant = strategy_seed % 3;

    auto pending_priority = [this, variant](
            const Job &a, const Job &b,
            const vector<MachineState> &sim_machines, long long current_time) {
        if (variant == 1) {
            long long ea = minEarliestStartForJob(a, sim_machines, current_time);
            long long eb = minEarliestStartForJob(b, sim_machines, current_time);
            long long da = (long long)a.weight *
                max(0LL, ea - max(current_time, (long long)a.release_time));
            long long db = (long long)b.weight *
                max(0LL, eb - max(current_time, (long long)b.release_time));
            if (da != db) return da > db;
        } else if (variant == 2) {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        } else {
            double pa = criticalRatioPriority(a, sim_machines, current_time);
            double pb = criticalRatioPriority(b, sim_machines, current_time);
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    };

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<Job> pool;
    int spin_rounds = 0;
    const int spin_cap = (jobs.size() > 3500) ? 2 : 3;

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pool.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        vector<Job> candidates = pool;
        if ((int)candidates.size() > prefilter) {
            nth_element(candidates.begin(), candidates.begin() + prefilter, candidates.end(),
                        [&](const Job &a, const Job &b) {
                return pending_priority(a, b, sim_machines, current_time);
            });
            candidates.resize(prefilter);
        }

        vector<int> perm(candidates.size());
        for (size_t i = 0; i < perm.size(); ++i) perm[i] = (int)i;
        sort(perm.begin(), perm.end(), [&](int ia, int ib) {
            return pending_priority(candidates[ia], candidates[ib], sim_machines, current_time);
        });

        vector<Job> to_try;
        vector<Job> held_back;
        unordered_set<int> try_ids;
        const int try_n = min((int)candidates.size(), pool_cap);
        for (int i = 0; i < try_n; ++i) {
            to_try.push_back(candidates[perm[i]]);
            try_ids.insert(candidates[perm[i]].job_id);
        }
        for (const auto &j : pool) {
            if (!try_ids.count(j.job_id)) held_back.push_back(j);
        }
        pool.clear();

        vector<double> reservation_scores = buildReservationScores(to_try);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : to_try) {
            PlacementPick pick = chooseWaitOptPlacement(
                job, sim_machines, current_time, reservation_scores);
            if (pick.machine_index < 0) {
                deferred.push_back(job);
                continue;
            }
            long long start_t = sim_machines[pick.machine_index].earliestFeasibleStart(
                job, pick.gpu_used, current_time);
            if (start_t > LLONG_MAX / 8) {
                deferred.push_back(job);
                continue;
            }
            auto result = sim_machines[pick.machine_index].startJob(
                job, start_t, pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        deferred.insert(deferred.end(), held_back.begin(), held_back.end());
        pool = std::move(deferred);

        if (scheduled_one && !pool.empty()) {
            if (++spin_rounds < spin_cap) continue;
            spin_rounds = 0;
        } else {
            spin_rounds = 0;
        }

        vector<long long> time_candidates;
        if (next_job_index < (int)jobs.size())
            time_candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            time_candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : time_candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::runMegascaleWaitHybridPipeline(int strategy_seed) {
    Solution online = generateMegascaleOnlineWaitList(strategy_seed);
    if (online.records.size() != jobs.size()) return online;

    Solution best = online;
    computeMetrics(best);

    static const int replay_modes[] = {0, 4};
    const int n = (jobs.size() > 4500) ? 0 : (strategy_seed % 2 == 0 ? 1 : 2);
    for (int i = 0; i < n; ++i) {
        Solution replayed = online;
        replayMegascaleAssignmentList(replayed, replay_modes[i]);
        if (replayed.records.size() != jobs.size()) continue;
        computeMetrics(replayed);
        if (isBetterSolution(replayed, best)) best = replayed;
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::polishListSchedulingPipeline(
    const Solution &list_source, int max_replays) {
    if (!isScheduleValid(list_source) || list_source.records.empty()) {
        return list_source;
    }
    return runAssignmentFirstPipeline(list_source, max_replays);
}

void GreedyScheduler::polishLargeInstanceReplay(Solution &best) {
    if (!isScheduleValid(best) || best.records.empty()) return;
    if (jobs.size() <= 900 || jobs.size() > 2000) return;
    if (isMegascaleInstance()) return;

    Solution result = best;
    if (jobs.size() <= 2000) computeOfficialMetrics(result);
    else computeMetrics(result);

    static const int variants[] = {2, 0};
    int n = (jobs.size() > 1800) ? 1 : 2;
    for (int i = 0; i < n; ++i) {
        Solution alt = best;
        replayScheduleWithOrder(alt, variants[i]);
        if (!isScheduleValid(alt)) continue;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(alt);
            if (isBetterOfficialSolution(alt, result)) result = alt;
        } else {
            computeMetrics(alt);
            if (isBetterSolution(alt, result)) result = alt;
        }
    }
    best = result;
}

void GreedyScheduler::polishMegascaleInstanceReplay(Solution &best) {
    if (!isMegascaleInstance() || jobs.size() > 5000) return;
    if (!isScheduleValid(best) || best.records.empty()) return;

    Solution result = best;
    computeMetrics(result);

    static const int modes[] = {0, 2, 1};
    const int n = (jobs.size() > 4800) ? 1
        : (jobs.size() > 4000) ? 1
        : (jobs.size() > 3000) ? 2 : 3;
    for (int i = 0; i < n; ++i) {
        Solution alt = best;
        replayMegascaleAssignmentList(alt, modes[i]);
        if (alt.records.size() != jobs.size()) continue;
        computeMetrics(alt);
        if (isBetterSolution(alt, result)) result = alt;
    }
    if (jobs.size() >= 3000 && jobs.size() <= 4200 &&
        (profile.burst_t0_ratio > 0.32 || profile.release_spread_ratio > 0.48)) {
        Solution alt = best;
        replayMegascaleAssignmentList(alt, 4);
        if (alt.records.size() == jobs.size()) {
            computeMetrics(alt);
            if (isBetterSolution(alt, result)) result = alt;
        }
    }
    static const int wspt_variants[] = {2, 0};
    const int wn = (jobs.size() > 3000) ? 1 : 2;
    for (int i = 0; i < wn; ++i) {
        Solution alt = best;
        replayScheduleWithOrder(alt, wspt_variants[i]);
        if (!isScheduleValid(alt)) continue;
        computeMetrics(alt);
        if (isBetterSolution(alt, result)) result = alt;
    }
    best = result;
}

GreedyScheduler::Solution GreedyScheduler::megascaleJointLNS(const Solution &initial, int max_iters) {
    if (!isMegascaleInstance() || jobs.size() > 4000 || max_iters <= 0) return initial;
    if (!isScheduleValid(initial) || initial.records.empty()) return initial;

    Solution best = initial;
    computeMetrics(best);

    vector<int> impact;
    impact.reserve(best.records.size());
    for (const auto &rec : best.records) impact.push_back(rec.job_id);
    sort(impact.begin(), impact.end(), [this](int a, int b) {
        const Job *ja = job_by_id.count(a) ? job_by_id.at(a) : nullptr;
        const Job *jb = job_by_id.count(b) ? job_by_id.at(b) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight * ja->duration;
        long long wb = (long long)jb->weight * jb->duration;
        if (wa != wb) return wa > wb;
        return ja->job_id < jb->job_id;
    });

    const double scale = (jobs.size() > 3200) ? 0.55 : 1.0;
    const int pool = min((int)impact.size(),
        max(5, (int)(jobs.size() * 0.035 * scale)));
    const int destroy_n = min(5, max(2, (int)(jobs.size() * 0.012 * scale)));
    srand(41u + (unsigned)jobs.size() + (unsigned)max_iters);

    for (int iter = 0; iter < max_iters; ++iter) {
        unordered_set<int> flex;
        for (int i = 0; i < destroy_n; ++i) {
            flex.insert(impact[rand() % pool]);
        }
        Solution trial = rebuildWithFlexibleJobs(best, flex);
        if (!isScheduleValid(trial) || trial.records.size() != jobs.size()) continue;
        computeMetrics(trial);
        if (isBetterSolution(trial, best)) best = trial;
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::megascaleLightAssignmentRefine(
    const Solution &initial, int max_tries) {
    if (!isMegascaleInstance() || max_tries <= 0) return initial;
    if (initial.records.size() != jobs.size()) return initial;

    Solution best = initial;
    computeMetrics(best);

    vector<size_t> order(best.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [this, &best](size_t a, size_t b) {
        const Job *ja = job_by_id.count(best.records[a].job_id)
            ? job_by_id.at(best.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(best.records[b].job_id)
            ? job_by_id.at(best.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight * ja->duration;
        long long wb = (long long)jb->weight * jb->duration;
        if (wa != wb) return wa > wb;
        return ja->job_id < jb->job_id;
    });

    int tried = 0;
    for (size_t idx : order) {
        if (tried >= max_tries) break;
        const int job_id = best.records[idx].job_id;
        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        const auto &cur = best.records[idx];
        bool improved = false;
        const int opt_cap = (jobs.size() > 4000) ? 2 : 3;
        int opt_tried = 0;
        for (const auto &entry : fm->second) {
            if (opt_tried >= opt_cap) break;
            ++opt_tried;
            if (entry.second == cur.gpu_used &&
                machines[entry.first].spec.server_id == cur.server_id) {
                continue;
            }
            Solution alt = best;
            alt.records[idx].server_id = machines[entry.first].spec.server_id;
            alt.records[idx].gpu_used = entry.second;
            replayMegascaleAssignmentList(alt, 0);
            if (alt.records.size() != jobs.size()) continue;
            computeMetrics(alt);
            if (isBetterSolution(alt, best)) {
                best = alt;
                improved = true;
                break;
            }
        }
        if (improved || fm->second.size() > 1) ++tried;
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::refineMegascaleWaitContributors(
    const Solution &initial, int max_tries) {
    if (!isMegascaleInstance() || max_tries <= 0) return initial;
    if (initial.records.size() != jobs.size()) return initial;

    Solution trial = initial;
    computeMetrics(trial);

    vector<size_t> order(trial.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [this, &trial](size_t a, size_t b) {
        const Job *ja = job_by_id.count(trial.records[a].job_id)
            ? job_by_id.at(trial.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(trial.records[b].job_id)
            ? job_by_id.at(trial.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight *
            max(0LL, trial.records[a].start_time - ja->release_time);
        long long wb = (long long)jb->weight *
            max(0LL, trial.records[b].start_time - jb->release_time);
        if (wa != wb) return wa > wb;
        long long ia = (long long)ja->weight * ja->duration;
        long long ib = (long long)jb->weight * jb->duration;
        if (ia != ib) return ia > ib;
        return ja->job_id < jb->job_id;
    });

    const int patch_cap = min(max_tries, (jobs.size() > 4200) ? 2 : 3);
    const int opt_cap = 2;
    int patches = 0;

    for (size_t idx : order) {
        if (patches >= patch_cap) break;
        const int job_id = trial.records[idx].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;
        const long long wait_contrib = (long long)job->weight *
            max(0LL, trial.records[idx].start_time - job->release_time);
        if (wait_contrib <= 0) continue;

        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        const auto &cur = trial.records[idx];
        int opt_tried = 0;
        long long best_wait = wait_contrib;
        int best_mi = -1;
        int best_gpu = -1;

        for (const auto &entry : fm->second) {
            if (opt_tried >= opt_cap) break;
            ++opt_tried;
            if (entry.second == cur.gpu_used &&
                machines[entry.first].spec.server_id == cur.server_id) {
                continue;
            }
            auto mach_it = machine_index_by_id.find(machines[entry.first].spec.server_id);
            if (mach_it == machine_index_by_id.end()) continue;
            const MachineState &ms = machines[mach_it->second];
            if (!ms.canEverRun(*job, entry.second)) continue;
            long long est = ms.earliestFeasibleStart(*job, entry.second, job->release_time);
            long long alt_wait = (long long)job->weight * max(0LL, est - job->release_time);
            if (alt_wait < best_wait) {
                best_wait = alt_wait;
                best_mi = entry.first;
                best_gpu = entry.second;
            }
        }

        if (best_mi >= 0) {
            trial.records[idx].server_id = machines[best_mi].spec.server_id;
            trial.records[idx].gpu_used = best_gpu;
            ++patches;
        }
    }

    if (patches == 0) return initial;

    replayMegascaleAssignmentList(trial, (jobs.size() > 4500) ? 0 : 4);
    if (trial.records.size() != jobs.size()) return initial;
    computeMetrics(trial);
    return isBetterSolution(trial, initial) ? trial : initial;
}

GreedyScheduler::Solution GreedyScheduler::refineMegascaleSimPlacementSearch(
    const Solution &initial, int max_tries) {
    if (!isMegascaleInstance() || max_tries <= 0) return initial;
    if (initial.records.size() != jobs.size()) return initial;

    Solution best = initial;
    computeMetrics(best);

    vector<size_t> order(best.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [this, &best](size_t a, size_t b) {
        const Job *ja = job_by_id.count(best.records[a].job_id)
            ? job_by_id.at(best.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(best.records[b].job_id)
            ? job_by_id.at(best.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight *
            max(0LL, best.records[a].start_time - ja->release_time);
        long long wb = (long long)jb->weight *
            max(0LL, best.records[b].start_time - jb->release_time);
        if (wa != wb) return wa > wb;
        long long ia = (long long)ja->weight * ja->duration;
        long long ib = (long long)jb->weight * jb->duration;
        if (ia != ib) return ia > ib;
        return ja->job_id < jb->job_id;
    });

    const int replay_mode = (jobs.size() > 4000) ? 5 : 4;
    const int opts_per_job = (jobs.size() > 3800) ? 2 : 3;
    int tried = 0;

    for (size_t idx : order) {
        if (tried >= max_tries) break;

        const int job_id = best.records[idx].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;

        const long long wait_contrib = (long long)job->weight *
            max(0LL, best.records[idx].start_time - job->release_time);
        const long long job_impact = (long long)job->weight * job->duration;
        long long impact_cut = (long long)profile.avg_duration * job->weight;
        if (jobs.size() > 0) {
            vector<long long> impacts;
            impacts.reserve(jobs.size());
            for (const auto &j : jobs) {
                impacts.push_back((long long)j.weight * j.duration);
            }
            nth_element(impacts.begin(),
                impacts.begin() + impacts.size() / 2, impacts.end());
            impact_cut = impacts[impacts.size() / 2];
        }
        if (wait_contrib <= 0 && job_impact < impact_cut) continue;

        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        const auto &cur = best.records[idx];
        struct OptEntry {
            int mi = -1;
            int gpu = -1;
            long long static_wait = LLONG_MAX;
        };
        vector<OptEntry> opts;
        opts.reserve(fm->second.size());

        for (const auto &entry : fm->second) {
            if (entry.first < 0 || entry.first >= (int)machines.size()) continue;
            const MachineState &ms = machines[entry.first];
            if (!ms.canEverRun(*job, entry.second)) continue;
            if (entry.second == cur.gpu_used &&
                ms.spec.server_id == cur.server_id) {
                continue;
            }
            long long est = ms.earliestFeasibleStart(*job, entry.second, job->release_time);
            OptEntry oe;
            oe.mi = entry.first;
            oe.gpu = entry.second;
            oe.static_wait = (long long)job->weight * max(0LL, est - job->release_time);
            opts.push_back(oe);
        }
        if (opts.empty()) continue;

        sort(opts.begin(), opts.end(), [](const OptEntry &a, const OptEntry &b) {
            return a.static_wait < b.static_wait;
        });
        if ((int)opts.size() > opts_per_job) opts.resize(opts_per_job);

        bool improved = false;
        for (const auto &oe : opts) {
            Solution alt = best;
            alt.records[idx].server_id = machines[oe.mi].spec.server_id;
            alt.records[idx].gpu_used = oe.gpu;
            replayMegascaleAssignmentList(alt, replay_mode);
            if (alt.records.size() != jobs.size()) continue;
            computeMetrics(alt);
            if (isBetterSolution(alt, best)) {
                best = alt;
                improved = true;
                break;
            }
        }
        if (improved || !opts.empty()) ++tried;
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::refineMegascalePairSwapSearch(
    const Solution &initial, int max_pair_tries) {
    if (!isMegascaleInstance() || max_pair_tries <= 0) return initial;
    if (initial.records.size() != jobs.size()) return initial;

    Solution best = initial;
    computeMetrics(best);

    struct SwapCandidate {
        size_t idx = 0;
        long long wait_contrib = 0;
        long long impact = 0;
    };
    vector<SwapCandidate> pool;
    pool.reserve(best.records.size());
    for (size_t i = 0; i < best.records.size(); ++i) {
        const int job_id = best.records[i].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;
        SwapCandidate sc;
        sc.idx = i;
        sc.wait_contrib = (long long)job->weight *
            max(0LL, best.records[i].start_time - job->release_time);
        sc.impact = (long long)job->weight * job->duration;
        pool.push_back(sc);
    }
    sort(pool.begin(), pool.end(), [](const SwapCandidate &a, const SwapCandidate &b) {
        if (a.wait_contrib != b.wait_contrib) return a.wait_contrib > b.wait_contrib;
        if (a.impact != b.impact) return a.impact > b.impact;
        return a.idx < b.idx;
    });

    const int pool_size = min((int)pool.size(),
        max(12, (int)(jobs.size() * 0.006 + 8.0 * profile.burst_t0_ratio)));
    if ((int)pool.size() > pool_size) pool.resize(pool_size);

    const int replay_mode = (jobs.size() > 4200) ? 5 : 4;
    int tried = 0;

    for (size_t pi = 0; pi < pool.size() && tried < max_pair_tries; ++pi) {
        for (size_t pj = pi + 1; pj < pool.size() && tried < max_pair_tries; ++pj) {
            const size_t ia = pool[pi].idx;
            const size_t ib = pool[pj].idx;
            const auto &rec_a = best.records[ia];
            const auto &rec_b = best.records[ib];
            if (rec_a.server_id == rec_b.server_id && rec_a.gpu_used == rec_b.gpu_used) {
                continue;
            }
            if (!jobCanUsePlacement(rec_a.job_id, rec_b.server_id, rec_b.gpu_used) ||
                !jobCanUsePlacement(rec_b.job_id, rec_a.server_id, rec_a.gpu_used)) {
                continue;
            }

            ++tried;
            Solution alt = best;
            alt.records[ia].server_id = rec_b.server_id;
            alt.records[ia].gpu_used = rec_b.gpu_used;
            alt.records[ib].server_id = rec_a.server_id;
            alt.records[ib].gpu_used = rec_a.gpu_used;
            replayMegascaleAssignmentList(alt, replay_mode);
            if (alt.records.size() != jobs.size()) continue;
            computeMetrics(alt);
            if (isBetterSolution(alt, best)) best = alt;
        }
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::refineMegascaleTopJobSubproblem(
    const Solution &initial, int budget_ms) {
    if (!isMegascaleInstance() || budget_ms <= 0) return initial;
    if (initial.records.size() != jobs.size()) return initial;
    if (jobs.size() < 2800 || jobs.size() > 4000) return initial;

    const auto deadline = chrono::steady_clock::now() +
        chrono::milliseconds(min(budget_ms, jobs.size() > 3700 ? 700 : 1200));

    struct PlacementOpt {
        int server_id = -1;
        int gpu = -1;
        long long static_wait = LLONG_MAX;
    };
    struct CriticalJob {
        size_t record_idx = 0;
        int job_id = -1;
        vector<PlacementOpt> options;
    };

    vector<CriticalJob> critical;
    critical.reserve(32);
    const int top_k = (jobs.size() > 3700) ? 6 : (jobs.size() > 3400) ? 10 : 12;
    const int opts_per_job = (jobs.size() > 3600) ? 2 : 3;

    vector<size_t> order(initial.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [this, &initial](size_t a, size_t b) {
        const Job *ja = job_by_id.count(initial.records[a].job_id)
            ? job_by_id.at(initial.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(initial.records[b].job_id)
            ? job_by_id.at(initial.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long ia = (long long)ja->weight * ja->duration;
        long long ib = (long long)jb->weight * jb->duration;
        if (ia != ib) return ia > ib;
        return ja->job_id < jb->job_id;
    });

    for (size_t idx : order) {
        if ((int)critical.size() >= top_k) break;
        const int job_id = initial.records[idx].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;

        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        CriticalJob cj;
        cj.record_idx = idx;
        cj.job_id = job_id;
        vector<PlacementOpt> raw;
        raw.reserve(fm->second.size());

        for (const auto &entry : fm->second) {
            if (entry.first < 0 || entry.first >= (int)machines.size()) continue;
            const MachineState &ms = machines[entry.first];
            if (!ms.canEverRun(*job, entry.second)) continue;
            long long est = ms.earliestFeasibleStart(*job, entry.second, job->release_time);
            if (est > LLONG_MAX / 8) continue;
            PlacementOpt opt;
            opt.server_id = ms.spec.server_id;
            opt.gpu = entry.second;
            opt.static_wait = (long long)job->weight * max(0LL, est - job->release_time);
            raw.push_back(opt);
        }
        if (raw.empty()) continue;

        sort(raw.begin(), raw.end(), [](const PlacementOpt &a, const PlacementOpt &b) {
            if (a.static_wait != b.static_wait) return a.static_wait < b.static_wait;
            if (a.server_id != b.server_id) return a.server_id < b.server_id;
            return a.gpu < b.gpu;
        });
        if ((int)raw.size() > opts_per_job) raw.resize(opts_per_job);
        cj.options = std::move(raw);
        critical.push_back(std::move(cj));
    }

    if (critical.size() < 3) return initial;

    long long best_static = LLONG_MAX;
    vector<int> best_choice(critical.size(), 0);
    vector<int> cur_choice(critical.size(), 0);
    vector<long long> min_tail(critical.size(), 0);
    for (int i = (int)critical.size() - 1; i >= 0; --i) {
        min_tail[i] = critical[i].options[0].static_wait;
        if (i + 1 < (int)critical.size()) min_tail[i] += min_tail[i + 1];
    }

    function<void(int, long long)> dfs = [&](int depth, long long partial) {
        if (chrono::steady_clock::now() >= deadline) return;
        if (partial >= best_static) return;
        if (depth == (int)critical.size()) {
            best_static = partial;
            best_choice = cur_choice;
            return;
        }
        const long long tail_lb = min_tail[depth];
        if (partial + tail_lb >= best_static) return;
        for (int oi = 0; oi < (int)critical[depth].options.size(); ++oi) {
            cur_choice[depth] = oi;
            dfs(depth + 1, partial + critical[depth].options[oi].static_wait);
        }
    };
    dfs(0, 0);

    if (best_static >= LLONG_MAX / 4) return initial;

    struct Candidate {
        vector<int> choice;
        long long static_cost = 0;
    };
    vector<Candidate> candidates;
    candidates.push_back({best_choice, best_static});

    long long second_best = LLONG_MAX;
    vector<int> second_choice;
    function<void(int, long long)> dfs_second = [&](int depth, long long partial) {
        if (chrono::steady_clock::now() >= deadline) return;
        if (partial >= second_best || partial >= best_static) return;
        if (depth == (int)critical.size()) {
            bool same = true;
            for (size_t i = 0; i < critical.size(); ++i) {
                if (cur_choice[i] != best_choice[i]) { same = false; break; }
            }
            if (same) return;
            second_best = partial;
            second_choice = cur_choice;
            return;
        }
        const long long tail_lb = min_tail[depth];
        if (partial + tail_lb >= second_best || partial + tail_lb >= best_static) return;
        for (int oi = 0; oi < (int)critical[depth].options.size(); ++oi) {
            cur_choice[depth] = oi;
            dfs_second(depth + 1, partial + critical[depth].options[oi].static_wait);
        }
    };
    dfs_second(0, 0);
    if (second_best < LLONG_MAX / 4 && candidates.size() < 2 &&
        chrono::steady_clock::now() < deadline) {
        candidates.push_back({second_choice, second_best});
    }

    Solution best = initial;
    computeMetrics(best);
    const int replay_mode = (jobs.size() > 4200) ? 5 : 4;

    for (const auto &cand : candidates) {
        if (chrono::steady_clock::now() >= deadline) break;
        Solution trial = initial;
        for (size_t i = 0; i < critical.size(); ++i) {
            const auto &opt = critical[i].options[cand.choice[i]];
            trial.records[critical[i].record_idx].server_id = opt.server_id;
            trial.records[critical[i].record_idx].gpu_used = opt.gpu;
        }
        replayMegascaleAssignmentList(trial, replay_mode);
        if (trial.records.size() != jobs.size()) continue;
        computeMetrics(trial);
        if (isBetterSolution(trial, best)) best = trial;
    }

    return best;
}

bool GreedyScheduler::megascaleAnytimeExpired(
    chrono::steady_clock::time_point deadline) const {
    return chrono::steady_clock::now() >= deadline;
}

bool GreedyScheduler::megascaleTryBatchWaitPatch(
    Solution &best, int replay_mode, const vector<size_t> &order,
    size_t &cursor, int max_patches) {
    if (order.empty() || max_patches <= 0) return false;

    Solution trial = best;
    const int opt_cap = 2;
    int patches = 0;

    for (int attempt = 0; attempt < max_patches && cursor < order.size(); ++attempt) {
        const size_t idx = order[cursor++];
        const int job_id = trial.records[idx].job_id;
        const Job *job = job_by_id.count(job_id) ? job_by_id.at(job_id) : nullptr;
        if (!job) continue;

        const long long wait_contrib = (long long)job->weight *
            max(0LL, trial.records[idx].start_time - job->release_time);
        if (wait_contrib <= 0) continue;

        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        const auto &cur = trial.records[idx];
        int opt_tried = 0;
        long long best_wait = wait_contrib;
        int best_mi = -1;
        int best_gpu = -1;

        for (const auto &entry : fm->second) {
            if (opt_tried >= opt_cap) break;
            ++opt_tried;
            if (entry.second == cur.gpu_used &&
                machines[entry.first].spec.server_id == cur.server_id) {
                continue;
            }
            auto mach_it = machine_index_by_id.find(machines[entry.first].spec.server_id);
            if (mach_it == machine_index_by_id.end()) continue;
            const MachineState &ms = machines[mach_it->second];
            if (!ms.canEverRun(*job, entry.second)) continue;
            long long est = ms.earliestFeasibleStart(*job, entry.second, job->release_time);
            long long alt_wait = (long long)job->weight * max(0LL, est - job->release_time);
            if (alt_wait < best_wait) {
                best_wait = alt_wait;
                best_mi = entry.first;
                best_gpu = entry.second;
            }
        }

        if (best_mi >= 0) {
            trial.records[idx].server_id = machines[best_mi].spec.server_id;
            trial.records[idx].gpu_used = best_gpu;
            ++patches;
        }
    }

    if (patches == 0) return false;

    replayMegascaleAssignmentList(trial, replay_mode);
    if (trial.records.size() != jobs.size()) return false;
    computeMetrics(trial);
    if (isBetterSolution(trial, best)) {
        best = trial;
        return true;
    }
    return false;
}

GreedyScheduler::Solution GreedyScheduler::megascaleAnytimeImprove(
    Solution best, chrono::steady_clock::time_point deadline) {
    if (!isMegascaleInstance() || !isScheduleValid(best)) return best;

    const auto now = chrono::steady_clock::now();
    if (now >= deadline) return best;
    const int remaining_ms = (int)chrono::duration_cast<chrono::milliseconds>(
        deadline - now).count();
    if (remaining_ms < 900) return best;

    computeMetrics(best);

    if (!megascaleAnytimeExpired(deadline) && jobs.size() > 2800 && remaining_ms >= 1400) {
        Solution alt = best;
        replayMegascaleAssignmentList(alt, 5);
        if (alt.records.size() == jobs.size()) {
            computeMetrics(alt);
            if (isBetterSolution(alt, best)) best = alt;
        }
    }

    if (remaining_ms < 1800 || jobs.size() > 4800) return best;

    vector<size_t> order(best.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    const int top_k = min((int)order.size(),
        max(16, (int)(jobs.size() * 0.012)));
    sort(order.begin(), order.end(), [this, &best](size_t a, size_t b) {
        const Job *ja = job_by_id.count(best.records[a].job_id)
            ? job_by_id.at(best.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(best.records[b].job_id)
            ? job_by_id.at(best.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight *
            max(0LL, best.records[a].start_time - ja->release_time);
        long long wb = (long long)jb->weight *
            max(0LL, best.records[b].start_time - jb->release_time);
        if (wa != wb) return wa > wb;
        long long ia = (long long)ja->weight * ja->duration;
        long long ib = (long long)jb->weight * jb->duration;
        if (ia != ib) return ia > ib;
        return ja->job_id < jb->job_id;
    });
    if ((int)order.size() > top_k) order.resize(top_k);

    const int replay_mode = (jobs.size() > 4200) ? 5 : 4;
    const int patch_per_round = (jobs.size() > 4000) ? 2 : 3;
    const int max_batch_rounds = remaining_ms >= 5000 ? 3 : (remaining_ms >= 3000 ? 2 : 1);
    size_t cursor = 0;
    int stale_rounds = 0;
    int batch_rounds = 0;
    const int stale_cap = 2;

    while (!megascaleAnytimeExpired(deadline) && stale_rounds < stale_cap &&
           cursor < order.size() && batch_rounds < max_batch_rounds) {
        if (megascaleTryBatchWaitPatch(best, replay_mode, order, cursor, patch_per_round)) {
            stale_rounds = 0;
            ++batch_rounds;
        } else {
            ++stale_rounds;
        }
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::pickBetterSolution(const Solution &a,
                                                              const Solution &b) const {
    const bool a_ok = isScheduleValid(a) && a.records.size() == jobs.size();
    const bool b_ok = isScheduleValid(b) && b.records.size() == jobs.size();
    if (!a_ok && !b_ok) return Solution{};
    if (!a_ok) return b;
    if (!b_ok) return a;

    Solution ca = a;
    Solution cb = b;
    if (jobs.size() <= 2000) {
        computeOfficialMetrics(ca);
        computeOfficialMetrics(cb);
        return isBetterOfficialSolution(ca, cb) ? ca : cb;
    }
    computeMetrics(ca);
    computeMetrics(cb);
    return isBetterSolution(ca, cb) ? ca : cb;
}

GreedyScheduler::Solution GreedyScheduler::generateLongJobDedicatedSolution() {
    Solution best;
    bool has_best = false;

    auto consider = [&](Solution sol) {
        if (!isScheduleValid(sol) || sol.records.size() != jobs.size()) return;
        if (!has_best) {
            best = sol;
            has_best = true;
            return;
        }
        best = pickBetterSolution(sol, best);
        has_best = isScheduleValid(best) && best.records.size() == jobs.size();
    };

    auto consider_greedy_pipeline = [&](int seed, int order_variant, int max_replays) {
        Solution greedy = generateGreedySolutionWithStrategy(seed, order_variant);
        consider(greedy);
        if (isScheduleValid(greedy) && greedy.records.size() == jobs.size()) {
            consider(runAssignmentFirstPipeline(greedy, max_replays));
        }
    };

    auto consider_list = [&](int seed, int max_replays) {
        Solution list_sol = generateListSchedulingSolution(seed);
        consider(list_sol);
        if (isScheduleValid(list_sol) && list_sol.records.size() == jobs.size()) {
            consider(polishListSchedulingPipeline(list_sol, max_replays));
        }
    };

    const int replay_mid = (jobs.size() > 420) ? 2 : -1;
    const int replay_lite = 1;

    if (jobs.size() < 170) {
        consider_list(0, replay_lite);
        consider(generateCriticalRatioSolution(0));
        consider_greedy_pipeline(42, 2, replay_mid);
    } else if (jobs.size() <= 420) {
        consider_list(2, replay_lite);
        consider_list(1, replay_lite);
        consider(generateStaticAssignmentSolution(880, 0));
        consider(generateStaticAssignmentSolution(881, 0));
        consider(generatePlacementFirstSolution(0));
        consider(generateCriticalRatioSolution(0));
        consider_greedy_pipeline(42, 2, replay_mid);
    } else if (jobs.size() <= 650) {
        consider_list(2, replay_lite);
        consider_list(1, replay_lite);
        consider(generateStaticAssignmentSolution(880, 0));
        consider(generateStaticAssignmentSolution(881, replay_lite));
        consider_greedy_pipeline(42, 2, replay_lite);
    } else if (jobs.size() <= 900) {
        consider_list(2, replay_lite);
        consider(generateStaticAssignmentSolution(881, replay_lite));
        consider_greedy_pipeline(42, 2, replay_lite);
    } else if (jobs.size() <= 1200) {
        consider_list(2, replay_lite);
        consider_greedy_pipeline(42, 2, replay_lite);
        consider_greedy_pipeline(17, 0, replay_lite);
    } else {
        consider_greedy_pipeline(42, 2, replay_lite);
        consider_greedy_pipeline(17, 0, replay_lite);
    }

    if (has_best) {
        consider(polishAssignmentReplayLight(best));
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateSingleServerDedicatedSolution() {
    Solution best;
    bool has_best = false;
    auto consider = [&](Solution sol) {
        if (!isScheduleValid(sol) || sol.records.size() != jobs.size()) return;
        if (!has_best) {
            best = sol;
            has_best = true;
            return;
        }
        best = pickBetterSolution(sol, best);
    };
    for (int d = 0; d < 2; ++d) consider(generateDedicatedSingleServerSolution(d));
    for (int s = 0; s < 3; ++s) consider(generateSingleServerShelfSolution(s));
    if (has_best && jobs.size() <= 220) {
        consider(polishAssignmentReplayLight(best));
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateMemBoundDedicatedSolution() {
    Solution best;
    bool has_best = false;
    auto consider = [&](Solution sol) {
        if (!isScheduleValid(sol) || sol.records.size() != jobs.size()) return;
        if (!has_best) {
            best = sol;
            has_best = true;
            return;
        }
        best = pickBetterSolution(sol, best);
    };

    generation_placement_override_ = 1;
    consider(generateGreedySolutionWithStrategy(56, 3));
    Solution greedy = generateGreedySolutionWithStrategy(42, 3);
    consider(greedy);
    if (isScheduleValid(greedy) && greedy.records.size() == jobs.size()) {
        consider(runAssignmentFirstPipeline(greedy, 1));
    }
    generation_placement_override_ = -1;

    if (has_best) consider(polishAssignmentReplayLight(best));
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateMegascaleDedicatedSolution() {
    Solution best;
    bool has_best = false;
    auto consider = [&](Solution sol) {
        if (sol.records.size() != jobs.size()) return;
        if (!has_best) {
            best = sol;
            has_best = true;
            return;
        }
        best = pickBetterSolution(sol, best);
    };
    auto consider_list = [&](int seed, int max_replays) {
        Solution list = generateListSchedulingSolutionFast(seed);
        if (list.records.size() != jobs.size()) return;
        consider(list);
        if (max_replays > 0) consider(polishListSchedulingPipeline(list, max_replays));
    };

    auto consider_wait = [&](int seed, int max_replays) {
        Solution w = generateMegascaleWaitOptAssignment(seed, max_replays);
        if (w.records.size() == jobs.size()) consider(w);
    };

    const int static_replay = (jobs.size() > 3200) ? 0 : 1;
    if (jobs.size() <= 4000) {
        consider(generateStaticAssignmentSolution(881, static_replay));
    }
    if (jobs.size() >= 2001 && jobs.size() <= 2800) {
        consider_wait(884, 1);
        consider_wait(885, 0);
        consider(generateMegascaleBalancedAssignment(882, 1));
        consider(generateMegascaleBalancedAssignment(883, 1));
        consider_list(2, 1);
    } else if (jobs.size() <= 4500) {
        consider(generateStaticAssignmentSolution(880, static_replay));
        consider(generateMegascaleBalancedAssignment(882, 1));
        if (jobs.size() >= 3200 && jobs.size() <= 4200) {
            Solution adp = generateMegascaleAdaptiveWaitAssignment(887, 0);
            if (adp.records.size() == jobs.size()) consider(adp);
            if (jobs.size() <= 3800 && profile.burst_t0_ratio > 0.28) {
                Solution bal = generateMegascaleBalancedAssignment(882, 0);
                if (bal.records.size() == jobs.size()) {
                    consider(mergeMegascaleDualAssignment(bal, adp));
                }
            }
            if (profile.burst_t0_ratio > 0.28 && profile.release_spread_ratio > 0.40) {
                consider(generateMegascaleProfileAwareAssignment(888, 0));
            }
        }
    } else if (jobs.size() >= 4600) {
        consider(generateStaticAssignmentSolution(880, 0));
    } else {
        consider(generateMegascaleBalancedAssignment(882, 1));
        consider(generateStaticAssignmentSolution(880, 0));
        consider(generateMegascaleAdaptiveWaitAssignment(887, 0));
        consider(generateMegascaleBalancedAssignment(883, 0));
        consider(generateMegascaleCoreAssignment(884, 0));
        if (profile.burst_t0_ratio > 0.22 || profile.release_spread_ratio > 0.38) {
            consider(generateMegascaleWaveAssignReplay(880));
            consider(generateMegascaleIntegratedTimeline(881));
        }
    }
    if (jobs.size() <= 5200) {
        consider(generateGreedySolutionWithStrategy(42, 2));
    }
    if (jobs.size() <= 3200) {
        consider(generateGreedySolutionWithStrategy(17, 0));
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::lightweightLNS(const Solution &initial, int max_iters) {
    if (jobs.size() > 500 || !isScheduleValid(initial) || initial.records.empty()) {
        return initial;
    }

    Solution best = initial;
    computeOfficialMetrics(best);

    vector<size_t> indices(best.records.size());
    for (size_t i = 0; i < indices.size(); ++i) indices[i] = i;
    sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        const Job *ja = job_by_id.count(best.records[a].job_id)
            ? job_by_id.at(best.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(best.records[b].job_id)
            ? job_by_id.at(best.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight * ja->duration;
        long long wb = (long long)jb->weight * jb->duration;
        if (wa != wb) return wa > wb;
        return ja->job_id < jb->job_id;
    });

    const int top_k = min((int)indices.size(), max(3, (int)(jobs.size() * 0.12)));
    srand(17u + (unsigned)jobs.size());

    for (int iter = 0; iter < max_iters; ++iter) {
        const size_t idx = indices[rand() % top_k];
        const int job_id = best.records[idx].job_id;
        auto fm = feasible_machines.find(job_id);
        if (fm == feasible_machines.end() || fm->second.size() <= 1) continue;

        const auto &opts = fm->second;
        const int start = rand() % (int)opts.size();
        const int tries = min(3, (int)opts.size());
        for (int t = 0; t < tries; ++t) {
            const auto &choice = opts[(start + t) % opts.size()];
            Solution alt = best;
            alt.records[idx].server_id = machines[choice.first].spec.server_id;
            alt.records[idx].gpu_used = choice.second;
            replayScheduleWithOrder(alt, 2);
            if (!isScheduleValid(alt)) continue;
            computeOfficialMetrics(alt);
            if (isBetterOfficialSolution(alt, best)) {
                best = alt;
                break;
            }
        }
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::rebuildWithFlexibleJobs(
    const Solution &base, const unordered_set<int> &flexible_jobs) const {
    Solution sol;
    if (!isScheduleValid(base) || flexible_jobs.empty()) return base;

    unordered_map<int, pair<int, int>> assignment;
    for (const auto &rec : base.records) {
        if (flexible_jobs.count(rec.job_id)) continue;
        assignment[rec.job_id] = {rec.server_id, rec.gpu_used};
    }

    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;

    int next_job_index = 0;
    long long current_time = jobs.front().release_time;
    vector<Job> pending;

    const int placement_mode = 1;
    const int sort_strategy = 2;

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pending.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pending.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        bool any_flex = false;
        for (const auto &job : pending) {
            if (flexible_jobs.count(job.job_id)) {
                any_flex = true;
                break;
            }
        }

        sort(pending.begin(), pending.end(),
             [this, &sim_machines, &current_time, &flexible_jobs, any_flex](const Job &a, const Job &b) {
            if (any_flex) {
                if (jobs.size() > 2000) {
                    long long wa = (long long)a.weight * a.duration;
                    long long wb = (long long)b.weight * b.duration;
                    if (wa != wb) return wa > wb;
                    if (a.release_time != b.release_time) return a.release_time < b.release_time;
                    return a.job_id < b.job_id;
                }
                double pa = criticalRatioPriority(a, sim_machines, current_time);
                double pb = criticalRatioPriority(b, sim_machines, current_time);
                if (fabs(pa - pb) > 1e-9) return pa > pb;
            }
            return jobMoreUrgentFirst(a, b, 2, current_time);
        });

        vector<double> reservation_scores = buildReservationScores(pending);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : pending) {
            int machine_index = -1;
            int gpu_used = -1;

            if (flexible_jobs.count(job.job_id)) {
                PlacementPick pick = chooseBestPlacement(
                    job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
                if (pick.machine_index < 0) {
                    deferred.push_back(job);
                    continue;
                }
                machine_index = pick.machine_index;
                gpu_used = pick.gpu_used;
            } else {
                auto assign_it = assignment.find(job.job_id);
                if (assign_it == assignment.end()) {
                    deferred.push_back(job);
                    continue;
                }
                auto mach_it = machine_index_by_id.find(assign_it->second.first);
                if (mach_it == machine_index_by_id.end()) {
                    deferred.push_back(job);
                    continue;
                }
                machine_index = mach_it->second;
                gpu_used = assign_it->second.second;
            }

            if (!sim_machines[machine_index].canStart(job, gpu_used)) {
                deferred.push_back(job);
                continue;
            }

            long long start_t = sim_machines[machine_index].earliestFeasibleStart(
                job, gpu_used, current_time);
            auto result = sim_machines[machine_index].startJob(job, start_t, gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        pending = std::move(deferred);

        if (scheduled_one && !pending.empty()) continue;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::jointLNS(const Solution &initial, int max_iters) {
    if (jobs.size() > 800 || !isScheduleValid(initial) || initial.records.empty()) {
        return initial;
    }

    Solution best = initial;
    computeOfficialMetrics(best);

    vector<int> impact;
    impact.reserve(best.records.size());
    for (const auto &rec : best.records) impact.push_back(rec.job_id);
    sort(impact.begin(), impact.end(), [this](int a, int b) {
        const Job *ja = job_by_id.count(a) ? job_by_id.at(a) : nullptr;
        const Job *jb = job_by_id.count(b) ? job_by_id.at(b) : nullptr;
        if (!ja || !jb) return a < b;
        long long wa = (long long)ja->weight * ja->duration;
        long long wb = (long long)jb->weight * jb->duration;
        if (wa != wb) return wa > wb;
        return ja->job_id < jb->job_id;
    });

    const int pool = min((int)impact.size(), max(4, (int)(jobs.size() * 0.15)));
    const int destroy_n = min(8, max(2, (int)(jobs.size() * 0.06)));
    srand(23u + (unsigned)jobs.size());

    for (int iter = 0; iter < max_iters; ++iter) {
        unordered_set<int> flex;
        for (int i = 0; i < destroy_n; ++i) {
            flex.insert(impact[rand() % pool]);
        }
        Solution trial = rebuildWithFlexibleJobs(best, flex);
        if (!isScheduleValid(trial) || trial.records.size() != jobs.size()) continue;
        computeOfficialMetrics(trial);
        if (isBetterOfficialSolution(trial, best)) best = trial;
    }
    return best;
}

vector<ScheduleRecord> GreedyScheduler::schedule() {
    if (jobs.empty()) return {};

    if (shouldUseSingleServerDedicatedPath() && !shouldUseAssignmentFirstMainPath()) {
        Solution best = generateSingleServerDedicatedSolution();
        if (!isScheduleValid(best) || best.records.size() != jobs.size()) {
            best = generateGreedySolutionWithStrategy(0);
        } else {
            Solution fb = generateDedicatedSingleServerSolution(1);
            if (isScheduleValid(fb) && fb.records.size() == jobs.size()) {
                best = pickBetterSolution(fb, best);
            }
        }
        if (shouldSingleServerRefine() && isScheduleValid(best)) {
            computeOfficialMetrics(best);
            Solution refined = jobs.size() <= 180
                ? refinePlacement(best, 1) : fastRefinePlacement(best);
            if (isBetterOfficialSolution(refined, best)) best = refined;
        }
        return best.records;
    }

    if (shouldUseMemBoundMainPath() && !shouldUseAssignmentFirstMainPath()) {
        Solution best = generateMemBoundDedicatedSolution();
        if (!isScheduleValid(best) || best.records.size() != jobs.size()) {
            best = generateGreedySolutionWithStrategy(42, 3);
        } else {
            Solution fb = generateGreedySolutionWithStrategy(56, 3);
            if (isScheduleValid(fb) && fb.records.size() == jobs.size()) {
                best = pickBetterSolution(fb, best);
            }
        }
        if (jobs.size() <= 300) {
            Solution jlns = jointLNS(best, 5);
            if (isScheduleValid(jlns) && jlns.records.size() == jobs.size()) {
                computeOfficialMetrics(jlns);
                computeOfficialMetrics(best);
                if (isBetterOfficialSolution(jlns, best)) best = jlns;
            }
        }
        return best.records;
    }

    int num_strategies = adaptiveStrategyCount();
    const int alns_min_jobs = 60;
    Solution runner_up;
    Solution third_place;
    Solution initial;

    if (shouldUseMegascaleDedicatedPath()) {
        Solution dedicated = generateMegascaleDedicatedSolution();
        const int greedy_n = (jobs.size() >= 4600) ? 1
            : (jobs.size() >= 3200) ? 2
            : (jobs.size() >= 2800) ? 1 : min(3, num_strategies);
        Solution greedy = generateMultiStrategySolution(greedy_n, &runner_up, &third_place);
        initial = pickBetterSolution(dedicated, greedy);
        if (initial.records.size() != jobs.size()) {
            initial = greedy.records.size() == jobs.size() ? greedy : dedicated;
        }
    } else if (shouldUseAssignmentFirstMainPath()) {
        Solution dedicated = generateLongJobDedicatedSolution();
        int greedy_n = min(num_strategies, jobs.size() > 650 ? 2 : 3);
        Solution greedy = generateMultiStrategySolution(greedy_n, &runner_up, &third_place);
        initial = pickBetterSolution(dedicated, greedy);
        if (!isScheduleValid(initial) || initial.records.size() != jobs.size()) {
            initial = isScheduleValid(greedy) ? greedy : dedicated;
        }
    } else {
        initial = generateMultiStrategySolution(num_strategies, &runner_up, &third_place);
        if (jobs.size() <= 2000 && initial.records.size() == jobs.size()) {
            vector<Solution> pool = {initial};
            if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
                pool.push_back(runner_up);
            }
            if (pool.size() > 1) initial = pickCompositeBalancedBest(pool);
        }
    }

    Solution best = initial;
    const bool use_dedicated = shouldUseAssignmentFirstMainPath();

    if (shouldUseMegascaleDedicatedPath() && best.records.size() == jobs.size()) {
        computeMetrics(best);
        if (jobs.size() <= 4000) {
            polishMegascaleInstanceReplay(best);
        } else if (jobs.size() <= 4500) {
            polishMegascaleLight(best);
        }
        if (jobs.size() >= 3000 && jobs.size() <= 3800 &&
            profile.burst_t0_ratio > 0.25) {
            Solution beamed = refineMegascaleReplayBeamSearch(best, 500);
            if (beamed.records.size() == jobs.size() && isScheduleValid(beamed)) {
                computeMetrics(beamed);
                if (isBetterSolution(beamed, best)) best = beamed;
            }
        }
        if (jobs.size() <= 3200 && isScheduleValid(runner_up) &&
            runner_up.records.size() == jobs.size()) {
            Solution merged = pickBetterSolution(best, runner_up);
            if (isScheduleValid(merged) && merged.records.size() == jobs.size()) {
                best = merged;
            }
        }
        return best.records;
    }

    if (jobs.size() <= 100 && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = refinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;

        Solution refined_runner;
        if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
            refined_runner = refinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }

        if (isScheduleValid(third_place) && third_place.records.size() == jobs.size()) {
            Solution refined_third = refinePlacement(third_place, 1);
            if (isBetterOfficialSolution(refined_third, best)) best = refined_third;
        }

        if (jobs.size() >= alns_min_jobs) {
            Solution alns = placementALNS(best);
            if (isBetterOfficialSolution(alns, best)) best = alns;
            Solution polished = refinePlacement(best, 1);
            if (isBetterOfficialSolution(polished, best)) best = polished;
        }
        if (shouldOrderPolish()) {
            Solution order_polished = polishOrderVariants(best, 0);
            computeOfficialMetrics(order_polished);
            if (isBetterOfficialSolution(order_polished, best)) best = order_polished;
        }
        computeOfficialMetrics(best);
    } else if (shouldLightRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
            Solution refined_runner = fastRefinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
    } else if (shouldSingleServerRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = jobs.size() <= 180
            ? refinePlacement(best, 1) : fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (jobs.size() <= 220 && isScheduleValid(runner_up) &&
            runner_up.records.size() == jobs.size()) {
            Solution refined_runner = jobs.size() <= 180
                ? refinePlacement(runner_up, 1) : fastRefinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
    } else if (!use_dedicated && shouldLongJobRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (jobs.size() <= 280 && isScheduleValid(runner_up) &&
            runner_up.records.size() == jobs.size()) {
            Solution refined_runner = fastRefinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
        if (jobs.size() <= 360) {
            Solution assign_refined = lightLongJobAssignmentRefine(best);
            if (isBetterOfficialSolution(assign_refined, best)) best = assign_refined;
        }
    } else if (shouldFastRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (jobs.size() <= 300 && isScheduleValid(runner_up) &&
            runner_up.records.size() == jobs.size()) {
            Solution refined_runner = fastRefinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
        if (profile.cpu_demand_ratio > 0.62 || profile.mem_demand_ratio > 0.62) {
            Solution assign_refined = lightLongJobAssignmentRefine(best);
            if (isBetterOfficialSolution(assign_refined, best)) best = assign_refined;
        }
    }

    if (shouldOrderPolish() && isScheduleValid(best) && jobs.size() > 100) {
        computeOfficialMetrics(best);
        Solution order_polished = polishOrderVariants(best, 1);
        if (isBetterOfficialSolution(order_polished, best)) best = order_polished;
    }

    if (!use_dedicated && shouldUseAssignmentReplayLight() && isScheduleValid(best)) {
        computeOfficialMetrics(best);
        Solution replay_polished = polishAssignmentReplayLight(best);
        if (isBetterOfficialSolution(replay_polished, best)) best = replay_polished;
        if (jobs.size() >= 300 && isScheduleValid(runner_up) &&
            runner_up.records.size() == jobs.size()) {
            Solution replay_runner = polishAssignmentReplayLight(runner_up);
            if (isBetterOfficialSolution(replay_runner, best)) best = replay_runner;
        }
    } else if (shouldAssignmentReplayPolish() && isScheduleValid(best)) {
        Solution replay_polished = polishAssignmentReplay(best);
        computeMetrics(replay_polished);
        computeMetrics(best);
        if (isBetterSolution(replay_polished, best)) best = replay_polished;
        else if (jobs.size() <= 2000) {
            computeOfficialMetrics(replay_polished);
            computeOfficialMetrics(best);
            if (isBetterOfficialSolution(replay_polished, best)) best = replay_polished;
        }
    }

    // 贪心已产出全量合法解时跳过退火
    bool need_refine = !isScheduleValid(initial) || initial.records.size() < jobs.size();
    if (jobs.size() > 1 && jobs.size() <= 100 && need_refine) {
        Solution optimized = adaptiveSimulatedAnnealing(initial);
        if (isBetterSolution(optimized, best)) {
            best = optimized;
        }
    }

    if (!use_dedicated && jobs.size() > 900 && jobs.size() <= 2000 && isScheduleValid(best)) {
        polishLargeInstanceReplay(best);
    }

    if (jobs.size() >= 80 && jobs.size() <= 120 && isScheduleValid(best)) {
        Solution lns = lightweightLNS(best, 10);
        if (isScheduleValid(lns) && lns.records.size() == jobs.size()) {
            computeOfficialMetrics(lns);
            computeOfficialMetrics(best);
            if (isBetterOfficialSolution(lns, best)) best = lns;
        }
    }

    return best.records;
}

bool GreedyScheduler::isScheduleValid(const Solution &sol) const {
    if (sol.records.size() != jobs.size()) return false;

    unordered_map<int, const Job *> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    unordered_map<int, vector<tuple<long long, long long, int, int, int>>> timeline;
    for (const auto &rec : sol.records) {
        auto job_it = job_map.find(rec.job_id);
        auto mach_it = machine_index_by_id.find(rec.server_id);
        if (job_it == job_map.end() || mach_it == machine_index_by_id.end()) return false;

        const Job *job = job_it->second;
        const ServerSpec &spec = machines[mach_it->second].spec;
        if (rec.start_time < job->release_time) return false;
        if (rec.finish_time != rec.start_time + job->duration) return false;
        if (rec.gpu_used < job->min_gpu || rec.gpu_used > spec.gpu_count) return false;
        if (job->cpu_cores > spec.cpu_cores || job->memory > spec.memory) return false;

        int mem_per_gpu = max(1, spec.gpu_memory);
        if (job->gpu_memory > mem_per_gpu * rec.gpu_used) return false;

        timeline[rec.server_id].push_back(
            {rec.start_time, rec.finish_time, rec.gpu_used, job->cpu_cores, job->memory});
    }

    for (const auto &entry : timeline) {
        const auto &events = entry.second;
        int server_index = machine_index_by_id.at(entry.first);
        const ServerSpec &spec = machines[server_index].spec;

        vector<long long> points;
        for (const auto &ev : events) {
            points.push_back(get<0>(ev));
            points.push_back(get<1>(ev));
        }
        sort(points.begin(), points.end());
        points.erase(unique(points.begin(), points.end()), points.end());

        for (long long t : points) {
            int used_gpu = 0, used_cpu = 0, used_mem = 0;
            for (const auto &ev : events) {
                long long st = get<0>(ev), ft = get<1>(ev);
                if (st <= t && t < ft) {
                    used_gpu += get<2>(ev);
                    used_cpu += get<3>(ev);
                    used_mem += get<4>(ev);
                }
            }
            if (used_gpu > spec.gpu_count || used_cpu > spec.cpu_cores || used_mem > spec.memory) {
                return false;
            }
        }
    }

    return true;
}

bool GreedyScheduler::isBetterSolution(const Solution &candidate, const Solution &current) const {
    if (candidate.records.empty()) return false;
    if (current.records.empty()) return true;
    if (candidate.records.size() != current.records.size()) {
        return candidate.records.size() > current.records.size();
    }
    // 贪心满规模解构造过程已保证约束；跳过昂贵全量时间线校验
    if (isMegascaleInstance() &&
        candidate.records.size() == jobs.size() &&
        current.records.size() == jobs.size()) {
        return candidate.score < current.score;
    }
    if (!isScheduleValid(candidate)) return false;
    if (!isScheduleValid(current)) return true;
    return candidate.score < current.score;
}

GreedyScheduler::Solution GreedyScheduler::generateMultiStrategySolution(int num_strategies,
                                                                         Solution *runner_up,
                                                                         Solution *third_place) {
    Solution best_sol;
    Solution second_sol;
    Solution third_sol;

    int stale_rounds = 0;
    const bool long_job_medium = !isSingleServer() && profile.long_job_ratio > 0.40 &&
                                 jobs.size() > 260 && jobs.size() <= 500;
    const int early_stop_after = isMegascaleInstance() ? 2
                               : (jobs.size() > 2500) ? 3
                               : (isLargeInstance() ? 3 : (long_job_medium ? 2 : 99));
    const int min_runs = isMegascaleInstance() ? min(3, num_strategies)
                       : (jobs.size() > 2500) ? min(4, num_strategies)
                       : (isLargeInstance() ? min(4, num_strategies)
                          : (long_job_medium ? min(5, num_strategies) : num_strategies));

    if (shouldUseMemBoundDedicatedPath()) {
        Solution sol = generateMemBoundDedicatedSolution();
        if (jobs.size() <= 2000) computeOfficialMetrics(sol);
        else computeMetrics(sol);
        if (isBetterSolution(sol, best_sol)) {
            third_sol = second_sol;
            second_sol = best_sol;
            best_sol = sol;
            stale_rounds = 0;
        } else if (isBetterSolution(sol, second_sol)) {
            third_sol = second_sol;
            second_sol = sol;
        }
    }

    if (isSingleServer()) {
        const int dedicated_n = min(2, num_strategies);
        for (int d = 0; d < dedicated_n; ++d) {
            Solution sol = generateDedicatedSingleServerSolution(d);
            if (jobs.size() <= 2000) computeOfficialMetrics(sol);
            else computeMetrics(sol);

            if (isBetterSolution(sol, best_sol)) {
                third_sol = second_sol;
                second_sol = best_sol;
                best_sol = sol;
                stale_rounds = 0;
            } else if (isBetterSolution(sol, second_sol)) {
                third_sol = second_sol;
                second_sol = sol;
                stale_rounds++;
            } else if (third_place && isBetterSolution(sol, third_sol)) {
                third_sol = sol;
                stale_rounds++;
            } else {
                stale_rounds++;
            }
        }
        for (int s = 0; s < 3; ++s) {
            Solution sol = generateSingleServerShelfSolution(s);
            if (jobs.size() <= 2000) computeOfficialMetrics(sol);
            else computeMetrics(sol);

            if (isBetterSolution(sol, best_sol)) {
                third_sol = second_sol;
                second_sol = best_sol;
                best_sol = sol;
                stale_rounds = 0;
            } else if (isBetterSolution(sol, second_sol)) {
                third_sol = second_sol;
                second_sol = sol;
                stale_rounds++;
            } else if (third_place && isBetterSolution(sol, third_sol)) {
                third_sol = sol;
                stale_rounds++;
            } else {
                stale_rounds++;
            }
        }
    }

    for (int s = 0; s < num_strategies; ++s) {
        Solution sol = generateGreedySolutionWithStrategy(s);
        if (jobs.size() <= 2000) computeOfficialMetrics(sol);
        else computeMetrics(sol);

        if (isBetterSolution(sol, best_sol)) {
            third_sol = second_sol;
            second_sol = best_sol;
            best_sol = sol;
            stale_rounds = 0;
        } else if (isBetterSolution(sol, second_sol)) {
            third_sol = second_sol;
            second_sol = sol;
            stale_rounds++;
        } else if (third_place && isBetterSolution(sol, third_sol)) {
            third_sol = sol;
            stale_rounds++;
        } else {
            stale_rounds++;
        }

        if (s + 1 >= min_runs && stale_rounds >= early_stop_after) break;
    }

    addQueueStrategyCandidates(best_sol, second_sol, third_place);

    if (!isMegascaleInstance() && jobs.size() <= 2000) {
        vector<Solution> pool;
        pool.push_back(best_sol);
        if (second_sol.records.size() == jobs.size()) pool.push_back(second_sol);
        if (third_place && third_place->records.size() == jobs.size()) pool.push_back(*third_place);
        if (pool.size() > 1) best_sol = pickCompositeBalancedBest(pool);
    }

    if (runner_up) *runner_up = second_sol;
    if (third_place) *third_place = third_sol;
    return best_sol;
}

GreedyScheduler::Solution GreedyScheduler::generateQueueCandidate(
    double duration_exp, double age_w, double flex_w, double slack_w, double waste_w,
    double mem_trade_ratio, double cost_premium_ratio, bool use_backfill) const {
    Solution sol;
    if (jobs.empty()) return sol;

    struct ReadyItem {
        int job_index = 0;
        double priority = 0.0;
        int feasible_count = 0;
        int duration = 0;
        int job_id = 0;
        long long release_time = 0;
        long long waiting_time = 0;
    };

    struct ReadyCompare {
        double age_w = 0.0;
        bool operator()(const ReadyItem &left, const ReadyItem &right) const {
            double lp = left.priority;
            double rp = right.priority;
            if (age_w > 0.0) {
                lp *= 1.0 + age_w * log(1.0 + max(0LL, left.waiting_time));
                rp *= 1.0 + age_w * log(1.0 + max(0LL, right.waiting_time));
            }
            if (fabs(lp - rp) > 1e-9) return lp < rp;
            if (left.feasible_count != right.feasible_count) {
                return left.feasible_count > right.feasible_count;
            }
            if (left.duration != right.duration) return left.duration < right.duration;
            return left.job_id > right.job_id;
        }
    };

    vector<MachineState> sim_machines = machines;
    vector<int> machine_flex(machines.size(), 0);
    for (const auto &entry : feasible_machines) {
        unordered_set<int> seen;
        for (const auto &choice : entry.second) {
            if (seen.insert(choice.first).second && choice.first >= 0 &&
                choice.first < (int)machine_flex.size()) {
                ++machine_flex[choice.first];
            }
        }
    }

    auto attempt_limit = [this](int ready_count) {
        if (ready_count <= 0) return 0;
        return min(ready_count, min(2048, max(256, (int)machines.size() * 8)));
    };

    auto try_start = [&](const Job &job, long long current_time,
                         ScheduleRecord &record, RunningJob &running_job) {
        auto it = feasible_machines.find(job.job_id);
        if (it == feasible_machines.end() || it->second.empty()) return false;

        int best_machine = -1;
        int best_gpu = 0;
        double best_cost = numeric_limits<double>::infinity();
        double best_waste = numeric_limits<double>::infinity();

        auto placement_cost = [&](int machine_index, int gpu_used, double &waste_ratio) {
            const MachineState &machine = sim_machines[machine_index];
            int total_mem = max(1, gpu_used) * max(1, machine.spec.gpu_memory);
            waste_ratio = (double)(total_mem - job.gpu_memory) / max(1, total_mem);
            double flexibility = jobs.empty() ? 0.0 :
                (double)machine_flex[machine_index] / (double)jobs.size();
            double slack = machine.placementSlack(job, gpu_used);
            return flex_w * flexibility + slack_w * slack + waste_w * waste_ratio;
        };

        for (const auto &choice : it->second) {
            int machine_index = choice.first;
            int gpu_used = choice.second;
            if (!sim_machines[machine_index].canStart(job, gpu_used)) continue;

            double waste_ratio = 0.0;
            double cost = placement_cost(machine_index, gpu_used, waste_ratio);
            if (cost < best_cost - 1e-12 ||
                (fabs(cost - best_cost) <= 1e-12 &&
                 (waste_ratio < best_waste - 1e-12 ||
                  (fabs(waste_ratio - best_waste) <= 1e-12 &&
                   (best_machine < 0 ||
                    sim_machines[machine_index].spec.server_id <
                        sim_machines[best_machine].spec.server_id))))) {
                best_cost = cost;
                best_waste = waste_ratio;
                best_machine = machine_index;
                best_gpu = gpu_used;
            }
        }

        if (best_machine < 0) return false;

        if (mem_trade_ratio < 1e50) {
            int alt_machine = -1;
            int alt_gpu = 0;
            double alt_waste = numeric_limits<double>::infinity();
            for (const auto &choice : it->second) {
                int machine_index = choice.first;
                int gpu_used = choice.second;
                if (machine_index == best_machine ||
                    !sim_machines[machine_index].canStart(job, gpu_used)) {
                    continue;
                }
                double waste_ratio = 0.0;
                double cost = placement_cost(machine_index, gpu_used, waste_ratio);
                if (waste_ratio < best_waste * mem_trade_ratio - 1e-12 &&
                    cost <= best_cost * cost_premium_ratio + 1e-12 &&
                    waste_ratio < alt_waste - 1e-12) {
                    alt_machine = machine_index;
                    alt_gpu = gpu_used;
                    alt_waste = waste_ratio;
                }
            }
            if (alt_machine >= 0) {
                best_machine = alt_machine;
                best_gpu = alt_gpu;
            }
        }

        auto started = sim_machines[best_machine].startJob(job, current_time, best_gpu);
        record = started.first;
        running_job = started.second;
        return true;
    };

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    priority_queue<ReadyItem, vector<ReadyItem>, ReadyCompare> pending_jobs(ReadyCompare{age_w});
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            const Job &job = jobs[next_job_index];
            auto feasible_it = feasible_machines.find(job.job_id);
            int feasible_count = feasible_it != feasible_machines.end()
                ? (int)feasible_it->second.size() : 0;
            double priority = 1000000.0 * (double)job.weight /
                pow((double)max(1, job.duration), duration_exp);
            pending_jobs.push(ReadyItem{next_job_index, priority, feasible_count, job.duration,
                                        job.job_id, job.release_time, 0});
            ++next_job_index;
        }

        const int limit = attempt_limit((int)pending_jobs.size());
        vector<ReadyItem> deferred;
        deferred.reserve(limit);
        bool scheduled_one = false;

        for (int attempt = 0; attempt < limit && !pending_jobs.empty(); ++attempt) {
            ReadyItem ready = pending_jobs.top();
            pending_jobs.pop();
            const Job &job = jobs[ready.job_index];
            if (records.count(job.job_id)) continue;

            ScheduleRecord record;
            RunningJob running_job;
            if (try_start(job, current_time, record, running_job)) {
                records[job.job_id] = record;
                running_heap.push(FinishEvent{running_job.finish_time, running_job.server_id,
                                              running_job.job_id, running_job});
                scheduled_one = true;
            } else {
                deferred.push_back(ready);
            }
        }

        if (use_backfill && !deferred.empty()) {
            vector<ReadyItem> still_deferred;
            still_deferred.reserve(deferred.size());
            for (const ReadyItem &ready : deferred) {
                const Job &job = jobs[ready.job_index];
                if (records.count(job.job_id)) continue;
                ScheduleRecord record;
                RunningJob running_job;
                if (try_start(job, current_time, record, running_job)) {
                    records[job.job_id] = record;
                    running_heap.push(FinishEvent{running_job.finish_time, running_job.server_id,
                                                  running_job.job_id, running_job});
                    scheduled_one = true;
                } else {
                    still_deferred.push_back(ready);
                }
            }
            deferred = std::move(still_deferred);
        }

        for (ReadyItem ready : deferred) {
            ready.waiting_time = current_time - ready.release_time;
            pending_jobs.push(ready);
        }

        if ((int)records.size() == (int)jobs.size()) break;
        if (scheduled_one) continue;

        long long next_time = -1;
        if (next_job_index < (int)jobs.size() &&
            jobs[next_job_index].release_time > current_time) {
            next_time = jobs[next_job_index].release_time;
        }
        if (!running_heap.empty() && running_heap.top().finish_time > current_time &&
            (next_time == -1 || running_heap.top().finish_time < next_time)) {
            next_time = running_heap.top().finish_time;
        }
        if (next_time == -1) break;
        current_time = next_time;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::pickCompositeBalancedBest(
    vector<Solution> candidates) const {
    vector<Solution> valid;
    valid.reserve(candidates.size());
    for (auto sol : candidates) {
        if (sol.records.size() != jobs.size()) continue;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(sol);
            if (!isScheduleValid(sol)) continue;
        } else {
            computeMetrics(sol);
        }
        valid.push_back(std::move(sol));
    }
    if (valid.empty()) return candidates.empty() ? Solution{} : candidates.front();
    if (valid.size() == 1) return valid.front();

    const double w_wait = 1.25;
    const double w_mem = 1.0;
    const double w_fin = 1.0;
    double min_wait = numeric_limits<double>::infinity();
    double max_wait = -numeric_limits<double>::infinity();
    double min_mem = numeric_limits<double>::infinity();
    double max_mem = -numeric_limits<double>::infinity();
    double min_fin = numeric_limits<double>::infinity();
    double max_fin = -numeric_limits<double>::infinity();

    for (const auto &sol : valid) {
        min_wait = min(min_wait, sol.weighted_waiting);
        max_wait = max(max_wait, sol.weighted_waiting);
        min_mem = min(min_mem, sol.vram_idle);
        max_mem = max(max_mem, sol.vram_idle);
        min_fin = min(min_fin, sol.makespan);
        max_fin = max(max_fin, sol.makespan);
    }

    auto normalized = [](double value, double low, double high) {
        return high > low ? (value - low) / (high - low) : 0.0;
    };

    int best_idx = 0;
    double best_score = numeric_limits<double>::infinity();
    for (int i = 0; i < (int)valid.size(); ++i) {
        const auto &sol = valid[i];
        double score =
            w_wait * normalized(sol.weighted_waiting, min_wait, max_wait) +
            w_mem * normalized(sol.vram_idle, min_mem, max_mem) +
            w_fin * normalized(sol.makespan, min_fin, max_fin);
        if (score < best_score - 1e-12) {
            best_score = score;
            best_idx = i;
        }
    }
    return valid[best_idx];
}

void GreedyScheduler::addQueueStrategyCandidates(Solution &best_sol, Solution &second_sol,
                                                 Solution *third_sol) {
    if (jobs.size() > 4700 || profile.vram_pressure >= 40.0) return;
    if (isMegascaleInstance() && jobs.size() > 2000) return;

    auto ingest = [&](Solution sol) {
        if (sol.records.size() != jobs.size()) return;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(sol);
            if (!isScheduleValid(sol)) return;
        } else {
            computeMetrics(sol);
        }
        if (isBetterSolution(sol, best_sol)) {
            if (third_sol) *third_sol = second_sol;
            second_sol = best_sol;
            best_sol = std::move(sol);
        } else if (isBetterSolution(sol, second_sol)) {
            if (third_sol) *third_sol = second_sol;
            second_sol = std::move(sol);
        } else if (third_sol && isBetterSolution(sol, *third_sol)) {
            *third_sol = std::move(sol);
        }
    };

    if (jobs.size() <= 2000) {
        ingest(generateQueueCandidate(1.0, 0.0, 2.0, 0.60, 0.25, 1e100, 1.0, false));
        ingest(generateQueueCandidate(1.0, 0.0, 2.0, 0.00, 0.25, 1.00, 2.00, true));
        ingest(generateQueueCandidate(1.0, 0.4, 2.0, 0.00, 0.25, 1.00, 1.50, true));
        ingest(generateQueueCandidate(1.0, 0.2, 2.0, 0.20, 0.25, 1.00, 1.50, true));
        ingest(generateQueueCandidate(0.9, 0.3, 2.0, 0.00, 0.25, 1.00, 1.50, true));
    } else if (jobs.size() < 4700) {
        ingest(generateQueueCandidate(1.0, 0.4, 2.0, 0.00, 0.25, 1.00, 1.50, true));
        if (jobs.size() < 3000 || profile.long_job_ratio < 0.30) {
            ingest(generateQueueCandidate(1.0, 0.0, 2.0, 0.00, 0.25, 1.00, 2.00, true));
        }
    }
}

GreedyScheduler::Solution GreedyScheduler::generateGreedySolutionWithStrategy(int strategy_seed,
                                                                              int order_variant_override) {
    Solution sol;
    if (jobs.empty()) return sol;

    cached_global_load_time = -1;
    srand(static_cast<unsigned>(strategy_seed * 7919u + 17u));

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;

    const int order_variant = (order_variant_override >= 0)
        ? order_variant_override : resolveOrderVariant(strategy_seed);
    PendingJobComparator pq_cmp{this, order_variant, &current_time};
    priority_queue<Job, vector<Job>, PendingJobComparator> pending_jobs(pq_cmp);
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;

    int sort_strategy = (strategy_seed / 4) % 4;
    const int placement_mode = generation_placement_override_ >= 0
        ? generation_placement_override_
        : resolvePlacementMode(strategy_seed);

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pending_jobs.push(jobs[next_job_index]);
            ++next_job_index;
        }

        vector<Job> pending_list;
        bool pending_urgency_sorted = false;
        int max_evaluate = adaptivePendingCap(static_cast<int>(pending_jobs.size()));

        if (shouldDrainFullPending()) {
            while (!pending_jobs.empty()) {
                pending_list.push_back(pending_jobs.top());
                pending_jobs.pop();
            }
        } else if (jobs.size() > 500) {
            if (profile.long_job_ratio > 0.38 && jobs.size() <= 900) {
                vector<Job> all_pending;
                all_pending.reserve(pending_jobs.size());
                while (!pending_jobs.empty()) {
                    all_pending.push_back(pending_jobs.top());
                    pending_jobs.pop();
                }

                const int n = static_cast<int>(all_pending.size());
                const int take = min(max_evaluate, n);
                if (n > take) {
                    auto urgent_cmp = [this, order_variant, &current_time](const Job &a, const Job &b) {
                        return jobMoreUrgentFirst(a, b, order_variant, current_time);
                    };
                    nth_element(all_pending.begin(), all_pending.begin() + take, all_pending.end(), urgent_cmp);
                    sort(all_pending.begin(), all_pending.begin() + take, urgent_cmp);
                    pending_list.reserve(take);
                    for (int i = 0; i < take; ++i) pending_list.push_back(all_pending[i]);
                    for (int i = take; i < n; ++i) pending_jobs.push(all_pending[i]);
                    pending_urgency_sorted = true;
                } else {
                    pending_list = std::move(all_pending);
                }
            } else {
                int pulled = 0;
                int skip = (strategy_seed % 3) * min(5, max(0, static_cast<int>(pending_jobs.size()) / 25));
                while (!pending_jobs.empty() && skip-- > 0) {
                    Job rotated = pending_jobs.top();
                    pending_jobs.pop();
                    pending_jobs.push(rotated);
                }
                while (!pending_jobs.empty() && pulled < max_evaluate) {
                    pending_list.push_back(pending_jobs.top());
                    pending_jobs.pop();
                    ++pulled;
                }
            }
        } else {
            vector<Job> all_pending;
            all_pending.reserve(pending_jobs.size());
            while (!pending_jobs.empty()) {
                all_pending.push_back(pending_jobs.top());
                pending_jobs.pop();
            }

            const int n = static_cast<int>(all_pending.size());
            const int take = min(max_evaluate, n);
            pending_list.reserve(take);
            if (n > take) {
                auto urgent_cmp = [this, order_variant, &current_time](const Job &a, const Job &b) {
                    return jobMoreUrgentFirst(a, b, order_variant, current_time);
                };
                nth_element(all_pending.begin(), all_pending.begin() + take, all_pending.end(), urgent_cmp);
                sort(all_pending.begin(), all_pending.begin() + take, urgent_cmp);
                for (int i = 0; i < take; ++i) pending_list.push_back(all_pending[i]);
                for (int i = take; i < n; ++i) pending_jobs.push(all_pending[i]);
                pending_urgency_sorted = true;
            } else {
                pending_list = std::move(all_pending);
            }
        }

        if (pending_list.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);

            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        vector<double> reservation_scores = buildReservationScores(pending_list);

        // 成员 A 主序 + 成员 B 同优先级装箱 tie-break
        if (generation_critical_ratio_) {
            sort(pending_list.begin(), pending_list.end(),
                 [this, &sim_machines, &current_time, order_variant](const Job &a, const Job &b) {
                double pa = criticalRatioPriority(a, sim_machines, current_time);
                double pb = criticalRatioPriority(b, sim_machines, current_time);
                if (fabs(pa - pb) > 1e-9) return pa > pb;
                return jobMoreUrgentFirst(a, b, order_variant, current_time);
            });
        } else if (generation_placement_first_) {
            sort(pending_list.begin(), pending_list.end(), [](const Job &a, const Job &b) {
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
                if (a.release_time != b.release_time) return a.release_time < b.release_time;
                return a.job_id < b.job_id;
            });
        } else if (!pending_urgency_sorted) {
            sort(pending_list.begin(), pending_list.end(),
                 [this, order_variant, &current_time](const Job &a, const Job &b) {
                return jobMoreUrgentFirst(a, b, order_variant, current_time);
            });
        }

        if (generation_single_shelf_variant_ >= 0 && isSingleServer()) {
            const int sv = generation_single_shelf_variant_;
            sort(pending_list.begin(), pending_list.end(), [sv](const Job &a, const Job &b) {
                if (sv == 0) {
                    if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
                    if (a.duration != b.duration) return a.duration > b.duration;
                    return a.weight > b.weight;
                }
                if (sv == 1) {
                    double pa = (double)a.weight / max(1, a.duration);
                    double pb = (double)b.weight / max(1, b.duration);
                    if (fabs(pa - pb) > 1e-9) return pa > pb;
                    return a.gpu_memory > b.gpu_memory;
                }
                if (a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
                if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
                return a.duration > b.duration;
            });
        }

        bool scheduled_one = false;
        vector<Job> deferred;

        for (const auto &job : pending_list) {
            PlacementPick pick = chooseBestPlacement(
                job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
            if (pick.machine_index < 0) {
                deferred.push_back(job);
                continue;
            }
            if (!sim_machines[pick.machine_index].canStart(job, pick.gpu_used)) {
                deferred.push_back(job);
                continue;
            }

            auto result = sim_machines[pick.machine_index].startJob(job, current_time, pick.gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                        result.second.job_id, result.second});
            scheduled_one = true;
        }

        if (!deferred.empty()) {
            if (!isSingleServer() && !isMegascaleInstance() && profile.long_job_ratio > 0.40 &&
                jobs.size() >= 120 && jobs.size() <= 900 &&
                profile.cpu_demand_ratio <= 0.58 && profile.mem_demand_ratio <= 0.58) {
                sort(deferred.begin(), deferred.end(),
                     [this, &sim_machines, &current_time](const Job &a, const Job &b) {
                    long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                    long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                    if (ea != eb) return ea < eb;
                    int fa = jobFeasibleCount(a.job_id);
                    int fb = jobFeasibleCount(b.job_id);
                    if (fa != fb) return fa < fb;
                    return a.job_id < b.job_id;
                });
            } else {
                sort(deferred.begin(), deferred.end(), [this](const Job &a, const Job &b) {
                    return jobFeasibleCount(a.job_id) < jobFeasibleCount(b.job_id);
                });
            }
        }
        for (const auto &job : deferred) {
            auto it = feasible_machines.find(job.job_id);
            if (it != feasible_machines.end() && !it->second.empty()) {
                pending_jobs.push(job);
            }
        }

        if ((int)records.size() == (int)jobs.size()) break;

        // 当前时刻如果刚成功放置了一个任务，继续尝试填满其余空闲资源，
        // 不要立刻跳到未来时刻，否则会漏掉本可同时启动的任务。
        if (scheduled_one) {
            continue;
        }

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);

        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }

    return sol;
}

GreedyScheduler::Solution GreedyScheduler::generateDedicatedSingleServerSolution(int strategy_seed) {
    static const int orders[] = {3, 1, 3, 2, 0, 1, 2, 3};
    const int order_variant = orders[strategy_seed % 8];
    const int placement_mode = (strategy_seed % 3 == 0) ? 3 : 1;

    generation_placement_override_ = placement_mode;
    generation_single_tight_gpu_ = true;
    generation_single_shelf_variant_ = -1;
    Solution sol = generateGreedySolutionWithStrategy(48 + strategy_seed * 7, order_variant);
    generation_placement_override_ = -1;
    generation_single_tight_gpu_ = false;
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::runAssignmentFirstPipeline(
    const Solution &assignment_source, int max_replays) {
    if (isMegascaleInstance() && jobs.size() > 2000) {
        return runMegascaleAssignmentFirstPipeline(assignment_source, max_replays);
    }
    Solution best = assignment_source;
    if (!isScheduleValid(assignment_source) || assignment_source.records.empty()) {
        return best;
    }

    static const int variants[] = {2, 0, 3, 1};
    int n = 4;
    if (jobs.size() > 500) n = 3;
    if (jobs.size() > 800) n = 2;
    if (max_replays > 0) n = min(n, max_replays);

    if (jobs.size() <= 2000) computeOfficialMetrics(best);
    else computeMetrics(best);

    for (int i = 0; i < n; ++i) {
        Solution alt = assignment_source;
        replayScheduleWithOrder(alt, variants[i]);
        if (!isScheduleValid(alt)) continue;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(alt);
            if (isBetterOfficialSolution(alt, best)) {
                best = alt;
                continue;
            }
        }
        computeMetrics(alt);
        if (isBetterSolution(alt, best)) best = alt;
    }
    return best;
}

void GreedyScheduler::replayMegascaleAssignmentList(Solution &sol, int sort_mode) {
    if (sol.records.empty()) return;

    unordered_map<int, pair<int, int>> assignment;
    for (const auto &rec : sol.records) {
        assignment[rec.job_id] = {rec.server_id, rec.gpu_used};
    }

    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;

    int next_job_index = 0;
    long long current_time = jobs.front().release_time;
    vector<Job> pool;
    int spin_rounds = 0;
    const int spin_cap = (jobs.size() > 3500) ? 2 : 3;

    auto sort_pool = [&](vector<Job> &pending) {
        sort(pending.begin(), pending.end(),
             [this, sort_mode, &sim_machines, &current_time](const Job &a, const Job &b) {
            if (sort_mode == 0) {
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
            } else if (sort_mode == 1) {
                if (a.weight != b.weight) return a.weight > b.weight;
                if (a.duration != b.duration) return a.duration < b.duration;
            } else if (sort_mode == 2) {
                return jobMoreUrgentFirst(a, b, 2, current_time);
            } else if (sort_mode == 4) {
                long long ra = max(current_time, (long long)a.release_time);
                long long rb = max(current_time, (long long)b.release_time);
                long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                long long da = (long long)a.weight * max(0LL, ea - ra);
                long long db = (long long)b.weight * max(0LL, eb - rb);
                if (da != db) return da > db;
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
            } else if (sort_mode == 5) {
                long long ra = max(current_time, (long long)a.release_time);
                long long rb = max(current_time, (long long)b.release_time);
                long long da = (long long)a.weight * max(0LL, ra - current_time);
                long long db = (long long)b.weight * max(0LL, rb - current_time);
                if (da != db) return da > db;
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
            } else {
                long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                if (ea != eb) return ea < eb;
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
            }
            if (a.release_time != b.release_time) return a.release_time < b.release_time;
            return a.job_id < b.job_id;
        });
    };

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pool.push_back(jobs[next_job_index]);
            ++next_job_index;
        }

        if (pool.empty()) {
            vector<long long> candidates;
            if (next_job_index < (int)jobs.size())
                candidates.push_back(jobs[next_job_index].release_time);
            if (!running_heap.empty())
                candidates.push_back(running_heap.top().finish_time);
            long long next_t = -1;
            for (long long c : candidates)
                if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
            if (next_t == -1) break;
            current_time = next_t;
            continue;
        }

        const int prefilter = (jobs.size() > 2800) ? 48 : 96;
        if ((int)pool.size() > prefilter) {
            nth_element(pool.begin(), pool.begin() + prefilter, pool.end(),
                        [this, &sim_machines, &current_time](const Job &a, const Job &b) {
                long long ra = max(current_time, (long long)a.release_time);
                long long rb = max(current_time, (long long)b.release_time);
                long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                long long da = (long long)a.weight * max(0LL, ea - ra);
                long long db = (long long)b.weight * max(0LL, eb - rb);
                if (da != db) return da > db;
                long long wa = (long long)a.weight * a.duration;
                long long wb = (long long)b.weight * b.duration;
                if (wa != wb) return wa > wb;
                return a.job_id < b.job_id;
            });
            pool.resize(prefilter);
        }

        sort_pool(pool);
        vector<Job> deferred;
        bool scheduled_one = false;

        for (const auto &job : pool) {
            auto assign_it = assignment.find(job.job_id);
            if (assign_it == assignment.end()) {
                deferred.push_back(job);
                continue;
            }
            auto mach_it = machine_index_by_id.find(assign_it->second.first);
            if (mach_it == machine_index_by_id.end()) {
                deferred.push_back(job);
                continue;
            }
            int machine_index = mach_it->second;
            int gpu_used = assign_it->second.second;
            if (!sim_machines[machine_index].canStart(job, gpu_used)) {
                deferred.push_back(job);
                continue;
            }
            long long start_t = sim_machines[machine_index].earliestFeasibleStart(
                job, gpu_used, current_time);
            auto result = sim_machines[machine_index].startJob(job, start_t, gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                         result.second.job_id, result.second});
            scheduled_one = true;
        }
        pool = std::move(deferred);

        if (scheduled_one && !pool.empty()) {
            if (++spin_rounds < spin_cap) continue;
            spin_rounds = 0;
        } else {
            spin_rounds = 0;
        }

        if ((int)records.size() == (int)jobs.size()) break;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);
        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.clear();
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
}

GreedyScheduler::Solution GreedyScheduler::runMegascaleAssignmentFirstPipeline(
    const Solution &assignment_source, int max_replays) {
    Solution best = assignment_source;
    if (assignment_source.records.size() != jobs.size()) return best;

    computeMetrics(best);
    static const int modes[] = {0, 2, 1};
    int n = 3;
    if (jobs.size() > 3500) n = 2;
    else if (jobs.size() <= 2800 && max_replays >= 3) n = 4;
    if (max_replays > 0) n = min(n, max_replays);

    for (int i = 0; i < n; ++i) {
        Solution alt = assignment_source;
        const int mode = (i < 3) ? modes[i] : 1;
        replayMegascaleAssignmentList(alt, mode);
        if (alt.records.size() != jobs.size()) continue;
        computeMetrics(alt);
        if (isBetterSolution(alt, best)) best = alt;
    }
    return best;
}

GreedyScheduler::Solution GreedyScheduler::generateStaticAssignmentSolution(int strategy_seed,
                                                                            int max_replays) {
    Solution sol;
    if (jobs.empty()) return sol;

    vector<int> order(jobs.size());
    for (int i = 0; i < (int)jobs.size(); ++i) order[i] = i;
    const bool alt_sort = (strategy_seed % 2) == 1;
    sort(order.begin(), order.end(), [this, alt_sort](int ia, int ib) {
        const Job &a = jobs[ia];
        const Job &b = jobs[ib];
        if (alt_sort) {
            double pa = static_cast<double>(a.weight) /
                sqrt(max(1.0, static_cast<double>(a.duration)));
            double pb = static_cast<double>(b.weight) /
                sqrt(max(1.0, static_cast<double>(b.duration)));
            if (fabs(pa - pb) > 1e-9) return pa > pb;
        } else {
            long long wa = (long long)a.weight * a.duration;
            long long wb = (long long)b.weight * b.duration;
            if (wa != wb) return wa > wb;
        }
        if (a.release_time != b.release_time) return a.release_time < b.release_time;
        return a.job_id < b.job_id;
    });

    vector<long long> projected_gpu_work(machines.size(), 0);
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;
    const double norm = max(1.0, (double)profile.time_horizon);

    for (int oi : order) {
        const Job &job = jobs[oi];
        const long long job_time = job.release_time;
        vector<double> reservation_scores(machines.size(), 0.0);

        double best_cost = 1e18;
        PlacementPick best_pick = {-1, -1};
        long long best_start = job_time;

        auto entries_it = feasible_machines.find(job.job_id);
        if (entries_it != feasible_machines.end()) {
            for (const auto &entry : entries_it->second) {
                int mi = entry.first;
                int gpu = entry.second;
                if (!sim_machines[mi].canEverRun(job, gpu)) continue;

                long long est = sim_machines[mi].earliestFeasibleStart(job, gpu, job_time);
                if (est > LLONG_MAX / 8) continue;

                double pc = placementCost(job, mi, gpu, sim_machines, est, reservation_scores);
                double cost = pc;
                double wait = (double)(est - job.release_time) / norm;
                const double wait_coef = isMegascaleInstance() ? 0.32 : 0.20;
                const double load_coef = isMegascaleInstance() ? 0.26 : 0.22;
                cost += wait_coef * wait * min(1.0, (double)job.weight);
                double load = (double)projected_gpu_work[mi] /
                    max(1.0, norm * max(1, sim_machines[mi].spec.gpu_count));
                cost += load_coef * load * min(1.0, (double)job.weight * job.duration /
                    max(1.0, profile.avg_duration * profile.avg_duration * 50.0));
                if (cost < best_cost - 1e-9) {
                    best_cost = cost;
                    best_pick = {mi, gpu};
                    best_start = est;
                }
            }
        }

        if (best_pick.machine_index < 0) continue;

        auto result = sim_machines[best_pick.machine_index].startJob(
            job, best_start, best_pick.gpu_used);
        records[job.job_id] = result.first;
        projected_gpu_work[best_pick.machine_index] +=
            (long long)job.duration * best_pick.gpu_used;
    }

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
    if (sol.records.size() != jobs.size()) return sol;
    if (max_replays == 0) return sol;

    return runAssignmentFirstPipeline(sol, max_replays);
}

GreedyScheduler::Solution GreedyScheduler::polishAssignmentReplayLight(const Solution &best) {
    Solution result = best;
    if (!isScheduleValid(best) || best.records.empty()) return result;

    static const int variants[] = {2, 0, 3, 1};
    int n = 4;
    if (jobs.size() > 320) n = 2;
    if (jobs.size() > 450 && profile.long_job_ratio <= 0.42) n = 1;
    if (profile.long_job_ratio > 0.40 && jobs.size() > 450 && jobs.size() <= 900) n = 2;

    if (jobs.size() <= 2000) computeOfficialMetrics(result);
    else computeMetrics(result);

    for (int i = 0; i < n; ++i) {
        Solution alt = best;
        replayScheduleWithOrder(alt, variants[i]);
        if (!isScheduleValid(alt)) continue;
        if (jobs.size() <= 2000) {
            computeOfficialMetrics(alt);
            if (isBetterOfficialSolution(alt, result)) {
                result = alt;
                continue;
            }
        }
        computeMetrics(alt);
        if (isBetterSolution(alt, result)) result = alt;
    }
    return result;
}

GreedyScheduler::Solution GreedyScheduler::generateSingleServerShelfSolution(int strategy_seed) {
    static const int orders[] = {0, 1, 2, 3};
    const int order_variant = orders[strategy_seed % 4];

    generation_placement_override_ = 1;
    generation_single_tight_gpu_ = true;
    generation_single_shelf_variant_ = strategy_seed % 3;
    Solution sol = generateGreedySolutionWithStrategy(96 + strategy_seed * 11, order_variant);
    generation_placement_override_ = -1;
    generation_single_tight_gpu_ = false;
    generation_single_shelf_variant_ = -1;
    return sol;
}

GreedyScheduler::Solution GreedyScheduler::refinePlacement(const Solution &initial_solution,
                                                           int max_passes_override) {
    Solution best = initial_solution;
    if (!isScheduleValid(best) || best.records.empty()) return best;

    computeOfficialMetrics(best);
    int max_passes = (jobs.size() >= 30 && jobs.size() <= 100) ? 3 : 1;
    if (max_passes_override > 0) max_passes = max_passes_override;

    const size_t refine_try_cap = (profile.vram_pressure > 0.55) ? 12 : 10;

    for (int pass = 0; pass < max_passes; ++pass) {
        bool improved = false;
        vector<size_t> order(best.records.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;

        sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            const Job *ja = job_by_id.count(best.records[a].job_id) ? job_by_id.at(best.records[a].job_id) : nullptr;
            const Job *jb = job_by_id.count(best.records[b].job_id) ? job_by_id.at(best.records[b].job_id) : nullptr;
            if (!ja || !jb) return a < b;
            if (ja->gpu_memory != jb->gpu_memory) return ja->gpu_memory > jb->gpu_memory;
            if (pass % 3 == 1) return ja->weight > jb->weight;
            if (pass % 3 == 2) return ja->duration > jb->duration;
            return a < b;
        });

        for (size_t idx : order) {
            int job_id = best.records[idx].job_id;
            auto job_it = job_by_id.find(job_id);
            if (job_it == job_by_id.end()) continue;
            const Job *job = job_it->second;

            vector<pair<int, int>> candidates = paretoPlacementOptions(*job);
            sort(candidates.begin(), candidates.end(), [&](const pair<int, int> &a, const pair<int, int> &b) {
                int waste_a = a.second * max(1, machines[a.first].spec.gpu_memory) - job->gpu_memory;
                int waste_b = b.second * max(1, machines[b.first].spec.gpu_memory) - job->gpu_memory;
                if (waste_a != waste_b) return waste_a < waste_b;
                return machines[a.first].spec.gpu_memory < machines[b.first].spec.gpu_memory;
            });

            const size_t max_try = min(candidates.size(), refine_try_cap);
            for (size_t ci = 0; ci < max_try; ++ci) {
                const auto &entry = candidates[ci];
                if (machines[entry.first].spec.server_id == best.records[idx].server_id &&
                    entry.second == best.records[idx].gpu_used) {
                    continue;
                }

                Solution candidate = best;
                candidate.records[idx].server_id = machines[entry.first].spec.server_id;
                candidate.records[idx].gpu_used = entry.second;
                replayScheduleForRefine(candidate);
                if (!isScheduleValid(candidate)) continue;
                computeOfficialMetrics(candidate);
                if (isBetterOfficialSolution(candidate, best)) {
                    best = candidate;
                    improved = true;
                }
            }
        }
        if (!improved) break;
    }

    const size_t max_swap = min((size_t)18, best.records.size());
    for (size_t i = 0; i < max_swap; ++i) {
        for (size_t j = i + 1; j < min(i + 10, best.records.size()); ++j) {
            Solution candidate = best;
            swap(candidate.records[i].server_id, candidate.records[j].server_id);
            swap(candidate.records[i].gpu_used, candidate.records[j].gpu_used);
            replayScheduleForRefine(candidate);
            if (!isScheduleValid(candidate)) continue;
            computeOfficialMetrics(candidate);
            if (isBetterOfficialSolution(candidate, best)) {
                best = candidate;
            }
        }
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::fastRefinePlacement(const Solution &initial_solution) {
    Solution best = initial_solution;
    if (!isScheduleValid(best) || best.records.empty()) return best;

    computeOfficialMetrics(best);

    vector<size_t> order(best.records.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        const Job *ja = job_by_id.count(best.records[a].job_id) ? job_by_id.at(best.records[a].job_id) : nullptr;
        const Job *jb = job_by_id.count(best.records[b].job_id) ? job_by_id.at(best.records[b].job_id) : nullptr;
        if (!ja || !jb) return a < b;
        if (profile.long_job_ratio > 0.40) {
            long long wa = static_cast<long long>(ja->weight) *
                           max(0LL, best.records[a].start_time - ja->release_time);
            long long wb = static_cast<long long>(jb->weight) *
                           max(0LL, best.records[b].start_time - jb->release_time);
            if (wa != wb) return wa > wb;
            if (ja->duration != jb->duration) return ja->duration > jb->duration;
        }
        long long pa = (long long)ja->weight * ja->duration;
        long long pb = (long long)jb->weight * jb->duration;
        if (pa != pb) return pa > pb;
        return ja->gpu_memory > jb->gpu_memory;
    });

    size_t top_k = min(order.size(),
        isSingleServer()
            ? max((size_t)14, (size_t)(order.size() * (jobs.size() > 150 ? 0.085 : 0.07)))
            : max((size_t)12, (size_t)(order.size() * (0.06 + 0.05 * instanceDifficulty()))));
    if (!isSingleServer() && profile.long_job_ratio > 0.45) {
        double frac = (jobs.size() > 300) ? 0.08 : 0.10;
        top_k = min(order.size(), max(top_k, (size_t)(order.size() * frac)));
        if (jobs.size() > 280) top_k = min(top_k, (size_t)24);
    }
    const size_t max_try = isSingleServer() ? 10
        : (isNarrowCluster() ? 8 : ((profile.vram_pressure > 0.55) ? 6 : 5));
    size_t try_cap = max_try;
    size_t eval_cap = top_k;
    if (!isSingleServer() && profile.burst_t0_ratio > 0.55 && jobs.size() > 260) {
        eval_cap = min(eval_cap, (size_t)14);
        try_cap = min(try_cap, (size_t)4);
    }

    for (size_t oi = 0; oi < eval_cap; ++oi) {
        size_t idx = order[oi];
        int job_id = best.records[idx].job_id;
        auto job_it = job_by_id.find(job_id);
        if (job_it == job_by_id.end()) continue;
        const Job *job = job_it->second;

        vector<pair<int, int>> candidates = paretoPlacementOptions(*job);
        sort(candidates.begin(), candidates.end(), [&](const pair<int, int> &a, const pair<int, int> &b) {
            int waste_a = a.second * max(1, machines[a.first].spec.gpu_memory) - job->gpu_memory;
            int waste_b = b.second * max(1, machines[b.first].spec.gpu_memory) - job->gpu_memory;
            if (waste_a != waste_b) return waste_a < waste_b;
            return machines[a.first].spec.gpu_memory < machines[b.first].spec.gpu_memory;
        });

        const size_t try_n = min(candidates.size(), try_cap);
        for (size_t ci = 0; ci < try_n; ++ci) {
            const auto &entry = candidates[ci];
            if (machines[entry.first].spec.server_id == best.records[idx].server_id &&
                entry.second == best.records[idx].gpu_used) {
                continue;
            }

            Solution candidate = best;
            candidate.records[idx].server_id = machines[entry.first].spec.server_id;
            candidate.records[idx].gpu_used = entry.second;
            replayScheduleForRefine(candidate);
            if (!isScheduleValid(candidate)) continue;
            computeMetrics(candidate);
            if (isBetterSolution(candidate, best)) {
                best = candidate;
            }
        }

        if (isSingleServer()) {
            int sid = best.records[idx].server_id;
            auto mach_it = machine_index_by_id.find(sid);
            if (mach_it != machine_index_by_id.end()) {
                const MachineState &ms = machines[mach_it->second];
                int min_g = ms.requiredGpuCount(*job);
                if (best.records[idx].gpu_used > min_g) {
                    Solution candidate = best;
                    candidate.records[idx].gpu_used = min_g;
                    replayScheduleForRefine(candidate);
                    if (isScheduleValid(candidate)) {
                        computeOfficialMetrics(candidate);
                        if (isBetterOfficialSolution(candidate, best)) best = candidate;
                    }
                }
            }
        }
    }

    const size_t swap_n = min((size_t)5, order.size());
    for (size_t i = 0; i + 1 < swap_n; ++i) {
        Solution candidate = best;
        size_t a = order[i], b = order[i + 1];
        swap(candidate.records[a].server_id, candidate.records[b].server_id);
        swap(candidate.records[a].gpu_used, candidate.records[b].gpu_used);
        replayScheduleForRefine(candidate);
        if (!isScheduleValid(candidate)) continue;
        computeOfficialMetrics(candidate);
        if (isBetterOfficialSolution(candidate, best)) best = candidate;
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::lightLongJobAssignmentRefine(const Solution &initial_solution) {
    Solution best = initial_solution;
    if (!isScheduleValid(best) || best.records.empty()) return best;
    if (isSingleServer()) return best;
    const bool resource_profile = profile.cpu_demand_ratio > 0.62 ||
                                  profile.mem_demand_ratio > 0.62;
    if (!(profile.long_job_ratio > 0.45 || resource_profile)) return best;
    if (jobs.size() < 120) return best;
    if (profile.long_job_ratio > 0.45 && jobs.size() > 450) return best;
    if (resource_profile && profile.long_job_ratio <= 0.45 && jobs.size() > 400) return best;

    computeOfficialMetrics(best);

    struct CandIdx {
        long long key;
        int idx;
    };
    vector<CandIdx> critical;
    critical.reserve(best.records.size());
    for (int i = 0; i < (int)best.records.size(); ++i) {
        int jid = best.records[i].job_id;
        auto it = job_by_id.find(jid);
        if (it == job_by_id.end()) continue;
        const Job *j = it->second;
        long long wait = max(0LL, best.records[i].start_time - j->release_time);
        long long key = (long long)j->weight * wait + (long long)(0.35 * j->weight) * j->duration;
        if (resource_profile) {
            key += (long long)(0.15 * j->weight) * j->memory +
                   (long long)(0.10 * j->weight) * j->cpu_cores;
        }
        critical.push_back({key, i});
    }
    sort(critical.begin(), critical.end(), [](const CandIdx &a, const CandIdx &b) {
        if (a.key != b.key) return a.key > b.key;
        return a.idx < b.idx;
    });

    const int K = min((int)critical.size(), max(10, (int)(jobs.size() * 0.03)));
    const int move_try_per_job = 4;
    const int reassign_try_per_job = 2;

    int eval_budget = resource_profile ? min(36, K * (move_try_per_job + reassign_try_per_job))
                                       : min(30, K * (move_try_per_job + reassign_try_per_job));
    for (int t = 0; t < K && eval_budget > 0; ++t) {
        int idx = critical[t].idx;
        int jid = best.records[idx].job_id;

        // 1) Reassign GPU count (and possibly server) a couple times
        for (int r = 0; r < reassign_try_per_job && eval_budget > 0; ++r) {
            Solution cand = neighborhoodReassign(best, idx);
            --eval_budget;
            if (!isScheduleValid(cand)) continue;
            computeOfficialMetrics(cand);
            if (isBetterOfficialSolution(cand, best)) best = cand;
        }

        // 2) Move to a few alternative feasible servers
        auto fit = feasible_machines.find(jid);
        if (fit == feasible_machines.end() || fit->second.empty()) continue;

        // Prefer servers with smaller gpu memory (tighter packing) for long-jobs, but keep it cheap.
        vector<pair<int, int>> options = fit->second;
        const bool resource_profile = profile.cpu_demand_ratio > 0.62 ||
                                      profile.mem_demand_ratio > 0.62;
        sort(options.begin(), options.end(), [&](const pair<int, int> &a, const pair<int, int> &b) {
            int ma = a.first, mb = b.first;
            if (resource_profile && profile.long_job_ratio < 0.40) {
                if (profile.cpu_demand_ratio > 0.58 &&
                    machines[ma].spec.cpu_cores != machines[mb].spec.cpu_cores) {
                    return machines[ma].spec.cpu_cores > machines[mb].spec.cpu_cores;
                }
                if (profile.mem_demand_ratio > 0.58 &&
                    machines[ma].spec.memory != machines[mb].spec.memory) {
                    return machines[ma].spec.memory > machines[mb].spec.memory;
                }
            }
            int va = machines[ma].spec.gpu_memory;
            int vb = machines[mb].spec.gpu_memory;
            if (va != vb) return va < vb;
            return machines[ma].spec.server_id < machines[mb].spec.server_id;
        });

        int tried = 0;
        for (const auto &op : options) {
            if (eval_budget <= 0) break;
            int new_srv = machines[op.first].spec.server_id;
            if (new_srv == best.records[idx].server_id) continue;

            Solution cand = neighborhoodMove(best, idx, new_srv);
            --eval_budget;
            if (!isScheduleValid(cand)) continue;
            computeOfficialMetrics(cand);
            if (isBetterOfficialSolution(cand, best)) best = cand;

            if (++tried >= move_try_per_job) break;
        }
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::placementALNS(const Solution &initial_solution) {
    if (!isScheduleValid(initial_solution) || initial_solution.records.empty()) {
        return initial_solution;
    }

    Solution current = initial_solution;
    Solution best = initial_solution;
    computeOfficialMetrics(current);
    computeOfficialMetrics(best);

    const int n = static_cast<int>(jobs.size());
    const int max_iter = (n <= 100) ? 220 : 120;
    int no_improve = 0;
    const int stall_limit = max(40, n + 10);

    srand(static_cast<unsigned>(n * 2654435761u + 101u));

    for (int iter = 0; iter < max_iter; ++iter) {
        Solution neighbor = current;
        if (neighbor.records.empty()) break;

        const int op = rand() % 100;
        const int idx = rand() % static_cast<int>(neighbor.records.size());

        if (op < 52) {
            int job_id = neighbor.records[idx].job_id;
            auto it = feasible_machines.find(job_id);
            if (it == feasible_machines.end() || it->second.empty()) continue;
            const auto &choices = it->second;
            size_t pick = rand() % choices.size();
            auto job_it = job_by_id.find(job_id);
            if (job_it != job_by_id.end()) {
                const Job *job = job_it->second;
                vector<size_t> ranked(choices.size());
                for (size_t k = 0; k < choices.size(); ++k) ranked[k] = k;
                sort(ranked.begin(), ranked.end(), [&](size_t a, size_t b) {
                    int mpg_a = max(1, machines[choices[a].first].spec.gpu_memory);
                    int mpg_b = max(1, machines[choices[b].first].spec.gpu_memory);
                    return (choices[a].second * mpg_a - job->gpu_memory) <
                           (choices[b].second * mpg_b - job->gpu_memory);
                });
                int top_k = min(4, static_cast<int>(ranked.size()));
                pick = ranked[rand() % top_k];
            }
            const auto &choice = choices[pick];
            neighbor.records[idx].server_id = machines[choice.first].spec.server_id;
            neighbor.records[idx].gpu_used = choice.second;
        } else {
            int jdx = rand() % static_cast<int>(neighbor.records.size());
            if (idx == jdx) continue;
            swap(neighbor.records[idx].server_id, neighbor.records[jdx].server_id);
            swap(neighbor.records[idx].gpu_used, neighbor.records[jdx].gpu_used);
        }

        replaySchedule(neighbor);
        if (!isScheduleValid(neighbor)) continue;
        computeOfficialMetrics(neighbor);

        if (!isBetterOfficialSolution(neighbor, current)) {
            ++no_improve;
            if (no_improve >= stall_limit) break;
            continue;
        }

        current = neighbor;
        no_improve = 0;
        if (isBetterOfficialSolution(neighbor, best)) best = neighbor;
    }

    return best;
}

double GreedyScheduler::reservationContribution(const Job &job) const {
    auto it = feasible_machines.find(job.job_id);
    if (it == feasible_machines.end() || it->second.empty()) return 0.0;
    double priority = static_cast<double>(job.weight) / max(1, job.duration);
    if (profile.vram_pressure > 0.5) {
        priority *= 1.0 + 0.20 * min(1.0, (double)job.gpu_memory / 128.0);
    }
    if (profile.cpu_demand_ratio > 0.58) {
        priority *= 1.0 + 0.12 * min(1.0, job.cpu_cores / 32.0);
    }
    if (profile.mem_demand_ratio > 0.58) {
        priority *= 1.0 + 0.10 * min(1.0, job.memory / 256.0);
    }
    if (profile.avg_min_gpu > 2.5) {
        priority *= 1.0 + 0.08 * min(1.0, (job.min_gpu - 1) / 4.0);
    }
    double feasible_count = static_cast<double>(it->second.size());
    return (1.0 + priority) / max(1.0, feasible_count * feasible_count);
}

vector<double> GreedyScheduler::buildReservationScores(const vector<Job> &pending_jobs) const {
    vector<double> scores(machines.size(), 0.0);
    for (const auto &job : pending_jobs) {
        double contribution = reservationContribution(job);
        auto it = feasible_machines.find(job.job_id);
        if (it == feasible_machines.end()) continue;
        for (const auto &entry : it->second) {
            scores[entry.first] += contribution;
        }
    }
    return scores;
}

bool GreedyScheduler::isBetterOfficialSolution(const Solution &candidate, const Solution &current) const {
    if (!isScheduleValid(candidate)) return false;
    if (!isScheduleValid(current)) return true;

    const double wait_tol = max(1.0, current.weighted_waiting) * 0.003;
    if (candidate.weighted_waiting + wait_tol < current.weighted_waiting) return true;
    if (candidate.weighted_waiting > current.weighted_waiting + wait_tol) return false;

    if (fabs(candidate.vram_idle - current.vram_idle) > 1e-6) {
        return candidate.vram_idle < current.vram_idle;
    }
    if (candidate.score + 1e-9 < current.score) return true;
    if (candidate.score > current.score + 1e-9) return false;
    return candidate.makespan < current.makespan;
}

void GreedyScheduler::computeOfficialMetrics(Solution &sol) const {
    computeMetrics(sol);

    if (sol.records.empty()) return;

    unordered_map<int, const Job *> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    long long makespan = static_cast<long long>(sol.makespan);
    long long t0 = jobs.front().release_time;

    struct VramEvent {
        long long time;
        int server_index;
        int delta;
        bool operator<(const VramEvent &other) const {
            if (time != other.time) return time < other.time;
            return delta < other.delta;
        }
    };
    vector<VramEvent> events;
    events.reserve(sol.records.size() * 2);
    for (const auto &rec : sol.records) {
        auto job_it = job_map.find(rec.job_id);
        auto mach_it = machine_index_by_id.find(rec.server_id);
        if (job_it == job_map.end() || mach_it == machine_index_by_id.end()) continue;
        int sid = mach_it->second;
        int vram = job_it->second->gpu_memory;
        events.push_back({rec.start_time, sid, +vram});
        events.push_back({rec.finish_time, sid, -vram});
    }
    sort(events.begin(), events.end());

    vector<long long> capacity(machines.size());
    vector<long long> used(machines.size(), 0);
    for (size_t i = 0; i < machines.size(); ++i) {
        capacity[i] = (long long)machines[i].spec.gpu_count * machines[i].spec.gpu_memory;
    }

    long long prev_t = t0;
    long long idle_integral = 0;
    size_t idx = 0;
    while (idx < events.size()) {
        long long t = events[idx].time;
        if (t > prev_t) {
            long long seg_idle = 0;
            for (size_t i = 0; i < machines.size(); ++i) {
                seg_idle += max(0LL, capacity[i] - used[i]);
            }
            idle_integral += seg_idle * (t - prev_t);
            prev_t = t;
        }
        while (idx < events.size() && events[idx].time == t) {
            used[events[idx].server_index] += events[idx].delta;
            ++idx;
        }
    }
    if (makespan > prev_t) {
        long long seg_idle = 0;
        for (size_t i = 0; i < machines.size(); ++i) {
            seg_idle += max(0LL, capacity[i] - used[i]);
        }
        idle_integral += seg_idle * (makespan - prev_t);
    }

    long long horizon = max(1LL, makespan - t0);
    sol.vram_idle = (double)idle_integral / (double)horizon;

    double ww_norm = sol.weighted_waiting / 1000000.0;
    double vram_norm = sol.vram_idle / 100000.0;
    double mk_norm = (sol.makespan - t0) / 100000.0;
    sol.score = ww_norm + vram_norm + mk_norm;
}

void GreedyScheduler::computeMetrics(Solution &sol) const {
    if (sol.records.empty()) {
        sol.score = sol.weighted_waiting = sol.makespan = sol.vram_idle = sol.gpu_utilization = 0;
        return;
    }

    unordered_map<int, const Job *> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    long long total_weighted_wait = 0;
    long long makespan = 0;
    long long total_gpu_slots_used = 0;

    for (const auto &rec : sol.records) {
        auto it = job_map.find(rec.job_id);
        if (it == job_map.end()) continue;
        const Job *job = it->second;

        long long waiting = rec.start_time - job->release_time;
        total_weighted_wait += waiting * job->weight;
        makespan = max(makespan, rec.finish_time);
        total_gpu_slots_used += (long long)rec.gpu_used * job->duration;
    }

    long long t0 = jobs.front().release_time;
    long long horizon = max(1LL, makespan - t0);
    long long total_gpu_slots = 0;
    for (const auto &srv : servers) {
        total_gpu_slots += (long long)srv.gpu_count * horizon;
    }

    // 显存浪费积分（轻量近似，用于多策略比较）
    long long vram_waste_integral = 0;
    for (const auto &rec : sol.records) {
        auto job_it = job_map.find(rec.job_id);
        auto mach_it = machine_index_by_id.find(rec.server_id);
        if (job_it == job_map.end() || mach_it == machine_index_by_id.end()) continue;
        const Job *job = job_it->second;
        int vg = machines[mach_it->second].spec.gpu_memory;
        long long allocated = (long long)rec.gpu_used * vg;
        long long waste = max(0LL, allocated - job->gpu_memory);
        vram_waste_integral += waste * job->duration;
    }

    sol.weighted_waiting = (double)total_weighted_wait;
    sol.makespan = (double)makespan;
    sol.vram_idle = (double)vram_waste_integral / (double)horizon;
    sol.gpu_utilization = (total_gpu_slots > 0) ? (double)total_gpu_slots_used / total_gpu_slots : 0;

    // 与官方三项指标对齐（均越小越好）
    double ww_norm = sol.weighted_waiting / 1000000.0;
    double vram_norm = sol.vram_idle / 100000.0;
    double mk_norm = (sol.makespan - t0) / 100000.0;

    sol.score = ww_norm + vram_norm + mk_norm;
}

GreedyScheduler::Solution GreedyScheduler::adaptiveSimulatedAnnealing(const Solution &initial) {
    Solution current = initial;
    Solution best = initial;

    // 根据问题规模调整初始温度和冷却参数
    double temperature = (jobs.size() <= 50) ? 1000.0 : (jobs.size() <= 200) ? 2000.0 : 5000.0;
    double initial_temp = temperature;
    double cooling_rate = (jobs.size() <= 20) ? 0.997 : (jobs.size() <= 100) ? 0.998 : 0.999;

    int accept_count = 0, reject_count = 0;
    int no_improve_count = 0;
    int max_no_improve = (jobs.size() <= 50) ? (50 + jobs.size() * 2) : (80 + jobs.size() / 2);
    int stagnation_threshold = (jobs.size() <= 50) ? 20 : 35;
    int reheats = 0;
    const int max_reheats = (jobs.size() <= 50) ? 2 : 2;
    int max_iterations = (jobs.size() <= 20) ? (300 + jobs.size() * 20)
                         : (jobs.size() <= 50) ? (800 + jobs.size() * 40)
                         : (jobs.size() <= 120) ? (600 + jobs.size() * 20)
                         : (500 + jobs.size() * 8);

    srand(static_cast<unsigned>(time(nullptr)));

    for (int iter = 0; iter < max_iterations; ++iter) {
        Solution neighbor = getBestNeighbor(current);
        double delta = neighbor.score - current.score;

        bool accept = false;
        if (delta < 0) {
            accept = true;
        } else {
            double acceptance_prob = exp(-delta / temperature);
            if ((double)rand() / RAND_MAX < acceptance_prob) {
                accept = true;
            }
        }

        if (accept) {
            current = neighbor;
            accept_count++;
            if (isBetterSolution(neighbor, best)) {
                best = neighbor;
                no_improve_count = 0;
            }
        } else {
            reject_count++;
        }

        int total_moves = accept_count + reject_count;
        if (total_moves > 0) {
            double acceptance_rate = (double)accept_count / total_moves;
            // 自适应调整冷却速度
            if (acceptance_rate < 0.35) {
                temperature *= (cooling_rate > 0.99) ? 0.9995 : cooling_rate;
            } else if (acceptance_rate > 0.5) {
                temperature *= 0.999;
            } else {
                temperature *= cooling_rate;
            }

            if (total_moves >= 100) {
                accept_count = reject_count = 0;
            }
        }

        no_improve_count++;
        if (no_improve_count >= stagnation_threshold) {
            if (reheats < max_reheats && temperature < initial_temp * 0.1) {
                temperature = initial_temp * 0.5;
                reheats++;
                no_improve_count = 0;
            } else if (no_improve_count >= max_no_improve) {
                break;
            }
        }

        if (temperature < 0.1) temperature = 0.1;
    }

    return best;
}

GreedyScheduler::Solution GreedyScheduler::getBestNeighbor(const Solution &sol) {
    Solution best_neighbor = sol;

    // 动态调整邻域搜索范围
    size_t max_swap = min((size_t)(6 + jobs.size() / 50), sol.records.size());
    size_t max_move = min((size_t)(10 + jobs.size() / 30), sol.records.size());
    size_t max_reassign = min((size_t)(6 + jobs.size() / 50), sol.records.size());

    // Swap邻域：交换两个job的服务器分配
    for (size_t i = 0; i < max_swap; ++i) {
        for (size_t j = i + 1; j < min(max_swap + 6, sol.records.size()); ++j) {
            Solution n = neighborhoodSwap(sol, i, j);
            if (isBetterSolution(n, best_neighbor)) best_neighbor = n;
        }
    }

    // Move邻域：将job移动到不同服务器
    for (size_t i = 0; i < max_move; ++i) {
        int job_id = sol.records[i].job_id;
        auto it = feasible_machines.find(job_id);
        if (it == feasible_machines.end()) continue;

        for (const auto &e : it->second) {
            int new_srv = machines[e.first].spec.server_id;
            if (new_srv != sol.records[i].server_id) {
                Solution n = neighborhoodMove(sol, i, new_srv);
                if (isBetterSolution(n, best_neighbor)) best_neighbor = n;
            }
        }
    }

    // Reassign邻域：重新分配GPU数量
    for (size_t i = 0; i < max_reassign; ++i) {
        Solution n = neighborhoodReassign(sol, i);
        if (isBetterSolution(n, best_neighbor)) best_neighbor = n;
    }

    // Destroy-Repair 邻域曾导致丢任务与资源冲突，已禁用
    return best_neighbor;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodSwap(const Solution &sol, int idx1, int idx2) {
    Solution n = sol;
    if (idx1 >= (int)sol.records.size() || idx2 >= (int)sol.records.size()) return n;

    // 交换服务器分配后，必须重新计算每个job在新服务器上的GPU需求
    int job1_id = n.records[idx1].job_id;
    int job2_id = n.records[idx2].job_id;
    int new_srv1 = n.records[idx2].server_id;
    int new_srv2 = n.records[idx1].server_id;

    auto it1 = feasible_machines.find(job1_id);
    auto it2 = feasible_machines.find(job2_id);

    // 尝试为job1找到新服务器上的可行GPU数量
    int new_gpu1 = -1;
    if (it1 != feasible_machines.end()) {
        for (const auto &e : it1->second) {
            if (machines[e.first].spec.server_id == new_srv1) {
                new_gpu1 = e.second;
                break;
            }
        }
    }

    // 尝试为job2找到新服务器上的可行GPU数量
    int new_gpu2 = -1;
    if (it2 != feasible_machines.end()) {
        for (const auto &e : it2->second) {
            if (machines[e.first].spec.server_id == new_srv2) {
                new_gpu2 = e.second;
                break;
            }
        }
    }

    // 如果找不到可行配置，使用原始值（verifyAndFix会处理）
    if (new_gpu1 == -1) new_gpu1 = n.records[idx1].gpu_used;
    if (new_gpu2 == -1) new_gpu2 = n.records[idx2].gpu_used;

    swap(n.records[idx1].server_id, n.records[idx2].server_id);
    n.records[idx1].gpu_used = new_gpu1;
    n.records[idx2].gpu_used = new_gpu2;
    replaySchedule(n);
    if (!isScheduleValid(n)) return sol;
    computeMetrics(n);
    return n;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodMove(const Solution &sol, int idx, int new_server) {
    Solution n = sol;
    if (idx >= (int)sol.records.size()) return n;

    int job_id = n.records[idx].job_id;
    auto it = feasible_machines.find(job_id);

    // 移动到新server时，从feasible_machines中查找该server的GPU配置
    int new_gpu = -1;
    if (it != feasible_machines.end()) {
        for (const auto &entry : it->second) {
            if (machines[entry.first].spec.server_id == new_server) {
                new_gpu = entry.second;
                break;
            }
        }
    }

    // 如果没找到精确匹配，使用新server的requiredGpuCount重新计算
    if (new_gpu == -1) {
        auto job_it = find_if(jobs.begin(), jobs.end(),
                              [job_id](const Job &j) { return j.job_id == job_id; });
        if (job_it != jobs.end()) {
            for (int mi = 0; mi < (int)machines.size(); ++mi) {
                if (machines[mi].spec.server_id == new_server) {
                    new_gpu = machines[mi].requiredGpuCount(*job_it);
                    break;
                }
            }
        }
    }

    n.records[idx].server_id = new_server;
    if (new_gpu != -1) n.records[idx].gpu_used = new_gpu;
    replaySchedule(n);
    if (!isScheduleValid(n)) return sol;
    computeMetrics(n);
    return n;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodReassign(const Solution &sol, int idx) {
    Solution n = sol;
    if (idx >= (int)sol.records.size()) return n;

    int job_id = n.records[idx].job_id;
    auto it = feasible_machines.find(job_id);
    if (it != feasible_machines.end() && !it->second.empty()) {
        int choice = rand() % it->second.size();
        n.records[idx].server_id = machines[it->second[choice].first].spec.server_id;
        n.records[idx].gpu_used = it->second[choice].second;
    }
    replaySchedule(n);
    if (!isScheduleValid(n)) return sol;
    computeMetrics(n);
    return n;
}

void GreedyScheduler::fillReplayOrderVariants(vector<int> &out) const {
    out.clear();
    out.push_back(0);
    if (isSingleServer()) {
        out.push_back(1);
        out.push_back(3);
        if (jobs.size() <= 180) out.push_back(2);
        return;
    }
    if (profile.long_job_ratio > 0.38) {
        out.push_back(2);
        if (profile.long_job_ratio <= 0.42) out.push_back(1);
        return;
    }
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) {
        out.push_back(3);
        out.push_back(1);
        return;
    }
    if (profile.burst_t0_ratio > 0.45) {
        out.push_back(1);
        out.push_back(3);
        return;
    }
    if (profile.vram_pressure > 0.55) {
        out.push_back(3);
        return;
    }
    out.push_back(1);
}

bool GreedyScheduler::shouldDualReplayInRefine() const {
    return false;
}

bool GreedyScheduler::shouldAssignmentReplayPolish() const {
    return false;
}

void GreedyScheduler::replayScheduleForRefine(Solution &sol) {
    replaySchedule(sol);
}

void GreedyScheduler::replayScheduleAdaptive(Solution &sol) {
    if (!shouldDualReplayInRefine()) {
        replaySchedule(sol);
        return;
    }

    Solution wspt = sol;
    replaySchedule(wspt);
    if (!isScheduleValid(wspt)) {
        replaySchedule(sol);
        return;
    }

    int alt = 1;
    if (isSingleServer()) alt = 1;
    else if (profile.long_job_ratio > 0.42) alt = 2;
    else if (profile.burst_t0_ratio > 0.45) alt = 1;
    else if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) alt = 3;
    else if (profile.vram_pressure > 0.55) alt = 3;

    Solution alt_sol = sol;
    replayScheduleWithOrder(alt_sol, alt);
    if (!isScheduleValid(alt_sol)) {
        sol = wspt;
        return;
    }

    if (jobs.size() <= 2000) {
        computeMetrics(wspt);
        computeMetrics(alt_sol);
        sol = isBetterSolution(alt_sol, wspt) ? alt_sol : wspt;
    } else {
        computeMetrics(wspt);
        computeMetrics(alt_sol);
        sol = isBetterSolution(alt_sol, wspt) ? alt_sol : wspt;
    }
}

GreedyScheduler::Solution GreedyScheduler::polishAssignmentReplay(const Solution &best) {
    Solution result = best;
    if (!isScheduleValid(best) || best.records.empty()) return result;

    vector<int> variants;
    fillReplayOrderVariants(variants);
    if (jobs.size() > 500) {
        variants.resize(min(variants.size(), (size_t)3));
    }

    bool has_best = false;
    for (int ov : variants) {
        Solution alt = best;
        replayScheduleWithOrder(alt, ov);
        if (!isScheduleValid(alt)) continue;
        computeMetrics(alt);
        if (!has_best) {
            result = alt;
            has_best = true;
            continue;
        }
        computeMetrics(result);
        if (isBetterSolution(alt, result)) result = alt;
    }
    return result;
}

void GreedyScheduler::replaySchedule(Solution &sol) {
    unordered_map<int, const Job *> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    unordered_map<int, pair<int, int>> assignment;
    for (const auto &rec : sol.records) {
        assignment[rec.job_id] = {rec.server_id, rec.gpu_used};
    }

    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;

    priority_queue<Job, vector<Job>, UrgencyComparator> pending_jobs;
    int next_job_index = 0;
    long long current_time = jobs.front().release_time;

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pending_jobs.push(jobs[next_job_index]);
            ++next_job_index;
        }

        bool scheduled_one = false;
        vector<Job> deferred;
        while (!pending_jobs.empty()) {
            Job job = pending_jobs.top();
            pending_jobs.pop();

            auto assign_it = assignment.find(job.job_id);
            if (assign_it == assignment.end()) {
                deferred.push_back(job);
                continue;
            }

            auto mach_it = machine_index_by_id.find(assign_it->second.first);
            if (mach_it == machine_index_by_id.end()) {
                deferred.push_back(job);
                continue;
            }

            int machine_index = mach_it->second;
            int gpu_used = assign_it->second.second;
            if (!sim_machines[machine_index].canStart(job, gpu_used)) {
                deferred.push_back(job);
                continue;
            }

            auto result = sim_machines[machine_index].startJob(job, current_time, gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                          result.second.job_id, result.second});
            scheduled_one = true;
        }
        for (const auto &job : deferred) pending_jobs.push(job);

        if ((int)records.size() == (int)jobs.size()) break;
        if (scheduled_one) continue;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);

        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.clear();
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
}

long long GreedyScheduler::minEarliestStartForJob(const Job &job,
                                                  const vector<MachineState> &sim_machines,
                                                  long long current_time) const {
    long long best = LLONG_MAX / 4;
    auto it = feasible_machines.find(job.job_id);
    if (it == feasible_machines.end()) return best;
    for (const auto &entry : it->second) {
        if (entry.first < 0 || entry.first >= (int)sim_machines.size()) continue;
        int gpu = entry.second;
        if (generation_single_tight_gpu_ && isSingleServer()) {
            gpu = sim_machines[entry.first].requiredGpuCount(job);
        }
        long long est = sim_machines[entry.first].earliestFeasibleStart(job, gpu, current_time);
        if (est < best) best = est;
    }
    return best;
}

void GreedyScheduler::replayForRefine(Solution &sol) {
    if (profile.long_job_ratio > 0.42) {
        replayScheduleWithOrder(sol, 0);
        return;
    }
    int ov = 0;
    if (isSingleServer()) ov = 3;
    else if (profile.burst_t0_ratio > 0.45) ov = 1;
    else if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) ov = 3;
    else ov = resolveOrderVariant(0);
    replayScheduleWithOrder(sol, ov);
}

void GreedyScheduler::replayScheduleWithOrder(Solution &sol, int order_variant) {
    unordered_map<int, const Job *> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    unordered_map<int, pair<int, int>> assignment;
    for (const auto &rec : sol.records) {
        assignment[rec.job_id] = {rec.server_id, rec.gpu_used};
    }

    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;
    unordered_map<int, ScheduleRecord> records;

    int next_job_index = 0;
    long long current_time = jobs.front().release_time;
    priority_queue<Job, vector<Job>, PendingJobComparator> pending_jobs(
        PendingJobComparator{this, order_variant, &current_time});

    while ((int)records.size() < (int)jobs.size()) {
        while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
            FinishEvent event = running_heap.top();
            running_heap.pop();
            auto it = machine_index_by_id.find(event.server_id);
            if (it != machine_index_by_id.end()) {
                sim_machines[it->second].releaseJob(event.running_job);
            }
        }

        while (next_job_index < (int)jobs.size() &&
               jobs[next_job_index].release_time <= current_time) {
            pending_jobs.push(jobs[next_job_index]);
            ++next_job_index;
        }

        bool scheduled_one = false;
        vector<Job> deferred;
        while (!pending_jobs.empty()) {
            Job job = pending_jobs.top();
            pending_jobs.pop();

            auto assign_it = assignment.find(job.job_id);
            if (assign_it == assignment.end()) {
                deferred.push_back(job);
                continue;
            }

            auto mach_it = machine_index_by_id.find(assign_it->second.first);
            if (mach_it == machine_index_by_id.end()) {
                deferred.push_back(job);
                continue;
            }

            int machine_index = mach_it->second;
            int gpu_used = assign_it->second.second;
            if (!sim_machines[machine_index].canStart(job, gpu_used)) {
                deferred.push_back(job);
                continue;
            }

            auto result = sim_machines[machine_index].startJob(job, current_time, gpu_used);
            records[job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                          result.second.job_id, result.second});
            scheduled_one = true;
        }
        for (const auto &job : deferred) pending_jobs.push(job);

        if ((int)records.size() == (int)jobs.size()) break;
        if (scheduled_one) continue;

        vector<long long> candidates;
        if (next_job_index < (int)jobs.size())
            candidates.push_back(jobs[next_job_index].release_time);
        if (!running_heap.empty())
            candidates.push_back(running_heap.top().finish_time);

        long long next_t = -1;
        for (long long c : candidates)
            if (c > current_time && (next_t == -1 || c < next_t)) next_t = c;
        if (next_t == -1) break;
        current_time = next_t;
    }

    sol.records.clear();
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }
}

void GreedyScheduler::updateGlobalLoadCache(long long current_time,
                                            const vector<MachineState> &sim_machines) const {
    if (cached_global_load_time == current_time) return;
    cached_global_load_time = current_time;

    double total_gpu = 0, total_used = 0;
    cached_machine_loads.resize(sim_machines.size());
    for (size_t i = 0; i < sim_machines.size(); ++i) {
        const auto &spec = sim_machines[i].spec;
        int used_gpu = spec.gpu_count - sim_machines[i].getRemainingGPU();
        cached_machine_loads[i] = (spec.gpu_count > 0) ? used_gpu / (double)spec.gpu_count : 0.0;
        total_gpu += spec.gpu_count;
        total_used += used_gpu;
    }
    cached_avg_gpu_load = (total_gpu > 0) ? total_used / total_gpu : 0;
}

double GreedyScheduler::estStartPlacementAdjust(const Job &job, int machine_index, int gpu_used,
                                                const vector<MachineState> &sim_machines,
                                                long long current_time, bool as_cost) const {
    if (isSingleServer() || isMegascaleInstance()) return 0.0;

    const auto &machine = sim_machines[machine_index];
    long long est = machine.earliestFeasibleStart(job, gpu_used, current_time);
    if (est > LLONG_MAX / 8) return as_cost ? 1e6 : -1e6;

    double delay = (double)max(0LL, est - current_time);
    double norm = max(1.0, (double)profile.time_horizon);
    double impact = (double)job.weight * delay / norm;

    long long gpu_work = machine.totalRemainingGpuWork(current_time);
    double congest = (double)gpu_work /
        max(1.0, norm * max(1, machine.spec.gpu_count));
    double impact_w = min(1.0, (double)job.weight * job.duration /
        max(1.0, profile.avg_duration * 80.0));

    double coef = 0.12;
    if (profile.burst_t0_ratio > 0.55) coef += 0.03;
    else if (profile.burst_t0_ratio > 0.45) coef += 0.04;
    if (profile.long_job_ratio > 0.38) coef += 0.04;
    if (profile.long_job_ratio > 0.42 && jobs.size() >= 120 && jobs.size() <= 900) coef += 0.03;

    double adj = coef * impact + 0.06 * min(1.0, congest) * impact_w;
    return as_cost ? adj : -adj;
}

GreedyScheduler::PlacementPick GreedyScheduler::chooseBestPlacement(
    const Job &job,
    const vector<MachineState> &sim_machines,
    long long current_time,
    const vector<double> &reservation_scores,
    int placement_mode,
    int sort_strategy) const {
    PlacementPick best;
    double best_score = -1e18;
    double best_cost = 1e18;
    int best_vram_waste = INT_MAX;
    long long best_est_start = LLONG_MAX;

    auto entries_it = feasible_machines.find(job.job_id);
    if (entries_it == feasible_machines.end() || entries_it->second.empty()) {
        return best;
    }

    auto consider = [&](int machine_index, int gpu_used) {
        if (!sim_machines[machine_index].canStart(job, gpu_used)) return;

        long long est_start = max(current_time, (long long)job.release_time);
        if (!isMegascaleInstance()) {
            est_start = sim_machines[machine_index].earliestFeasibleStart(
                job, gpu_used, current_time);
            if (est_start > LLONG_MAX / 8) return;
        }

        int mpg = max(1, sim_machines[machine_index].spec.gpu_memory);
        int vram_waste = gpu_used * mpg - job.gpu_memory;
        const double gpu_emphasis = min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0)));

        if (placement_mode == 1 || placement_mode == 3) {
            double cost = placement_mode == 1
                ? placementCost(job, machine_index, gpu_used, sim_machines, current_time, reservation_scores)
                : ((double)vram_waste / max(1, job.gpu_memory)) +
                  0.18 * sim_machines[machine_index].placementSlack(job, gpu_used, gpu_emphasis);
            cost += estStartPlacementAdjust(job, machine_index, gpu_used, sim_machines, current_time, true);
            if (vram_waste == 0) cost -= 0.05;
            if (isSingleServer()) {
                int min_g = sim_machines[machine_index].requiredGpuCount(job);
                cost += 0.04 * (gpu_used - min_g);
            }
            if (sort_strategy > 0) {
                cost += (rand() % 1000) / 1000.0 * 0.04 * sort_strategy;
            }
            if (cost < best_cost - 1e-9 ||
                (fabs(cost - best_cost) <= 1e-9 &&
                 (est_start < best_est_start ||
                  (est_start == best_est_start &&
                   (vram_waste < best_vram_waste ||
                    (vram_waste == best_vram_waste && gpu_used < best.gpu_used)))))) {
                best_cost = cost;
                best.machine_index = machine_index;
                best.gpu_used = gpu_used;
                best_vram_waste = vram_waste;
                best_est_start = est_start;
            }
            return;
        }

        double score = placementScore(job, machine_index, gpu_used, sim_machines,
                                      current_time, reservation_scores);
        score += estStartPlacementAdjust(job, machine_index, gpu_used, sim_machines, current_time, false);
        if (placement_mode == 2) {
            score += 0.10 * sim_machines[machine_index].placementSlack(
                job, gpu_used, min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0))));
        }
        if (sort_strategy > 0) {
            score += (rand() % 1000) / 1000.0 * 0.06 * sort_strategy;
        }
        if (vram_waste == 0) score += 0.06;
        if (isSingleServer()) {
            int min_g = sim_machines[machine_index].requiredGpuCount(job);
            score -= 0.05 * (gpu_used - min_g);
            score += 0.06 * (static_cast<double>(job.weight) / max(1, job.duration));
        }
        if (profile.avg_min_gpu > 2.5 && !isSingleServer()) {
            int min_g = sim_machines[machine_index].requiredGpuCount(job);
            score -= 0.04 * (gpu_used - min_g);
        }

        if (score > best_score + 1e-9 ||
            (fabs(score - best_score) <= 1e-9 &&
             (est_start < best_est_start ||
              (est_start == best_est_start &&
               (vram_waste < best_vram_waste ||
                (vram_waste == best_vram_waste && gpu_used < best.gpu_used)))))) {
            best_score = score;
            best.machine_index = machine_index;
            best.gpu_used = gpu_used;
            best_vram_waste = vram_waste;
            best_est_start = est_start;
        }
    };

    if (shouldUseRuntimePareto()) {
        unordered_set<int> machine_indices;
        for (const auto &entry : entries_it->second) machine_indices.insert(entry.first);

        for (int mi : machine_indices) {
            const auto &ms = sim_machines[mi];
            int min_g = ms.requiredGpuCount(job);
            int max_g = min(ms.spec.gpu_count, ms.getRemainingGPU());
            if (generation_single_tight_gpu_ && isSingleServer()) {
                if (ms.canEverRun(job, min_g)) consider(mi, min_g);
                continue;
            }
            int prev_waste = INT_MAX;
            int mpg = max(1, ms.spec.gpu_memory);
            for (int g = min_g; g <= max_g; ++g) {
                if (!ms.canEverRun(job, g)) continue;
                int waste = g * mpg - job.gpu_memory;
                if (waste < prev_waste) {
                    consider(mi, g);
                    prev_waste = waste;
                }
            }
        }
    } else {
        for (const auto &entry : entries_it->second) {
            const auto &ms = sim_machines[entry.first];
            if (generation_single_tight_gpu_ && isSingleServer()) {
                int min_g = ms.requiredGpuCount(job);
                if (ms.canStart(job, min_g)) consider(entry.first, min_g);
                continue;
            }
            if (ms.canStart(job, entry.second)) {
                consider(entry.first, entry.second);
                continue;
            }
            int min_g = ms.requiredGpuCount(job);
            int max_g = min(ms.spec.gpu_count, ms.getRemainingGPU());
            int mpg = max(1, ms.spec.gpu_memory);
            int prev_waste = INT_MAX;
            for (int g = min_g; g <= max_g; ++g) {
                if (!ms.canEverRun(job, g)) continue;
                int waste = g * mpg - job.gpu_memory;
                if (waste < prev_waste) {
                    consider(entry.first, g);
                    prev_waste = waste;
                }
            }
        }
    }

    return best;
}

double GreedyScheduler::placementCost(const Job &job, int machine_index, int gpu_used,
                                      const vector<MachineState> &sim_machines,
                                      long long current_time,
                                      const vector<double> &reservation_scores) const {
    const auto &machine = sim_machines[machine_index];
    const auto &spec = machine.spec;
    int memory_per_gpu = max(1, spec.gpu_memory);
    double vram_waste = (double)(gpu_used * memory_per_gpu - job.gpu_memory) /
                        max(1.0, (double)gpu_used * memory_per_gpu);
    const double gpu_emphasis = min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0)));
    double slack = machine.placementSlack(job, gpu_used, gpu_emphasis);

    double reservation = 0.0;
    if (!reservation_scores.empty() && machine_index < (int)reservation_scores.size()) {
        double own = reservationContribution(job);
        double adjusted = max(0.0, reservation_scores[machine_index] - own);
        double max_res = 0.0;
        for (double s : reservation_scores) max_res = max(max_res, s);
        reservation = (max_res > 0.0) ? adjusted / max_res : 0.0;
    }

    double scarcity = 0.0;
    if (spec.gpu_memory >= 80 && job.gpu_memory < memory_per_gpu) {
        scarcity = 0.20 + 0.10 * (1.0 - profile.scarce_80_ratio);
    }

    const double vram_w = min(1.0, profile.vram_pressure);
    double vram_cost_w = 0.30 + 0.06 * vram_w;
    if (isNarrowCluster()) vram_cost_w += 0.08;
    double duration_w = isSingleServer() ? 0.20 : 0.08;
    if (isSingleServer() && profile.avg_duration > 0.0 &&
        job.duration > profile.avg_duration * 1.15) {
        duration_w += 0.06;
    }
    double long_load_cost = 0.0;
    if (!isSingleServer() && profile.long_job_ratio > 0.40 && profile.avg_duration > 0.0 &&
        job.duration > profile.avg_duration * 1.15) {
        double load = 1.0 - (double)machine.getRemainingGPU() / max(1, spec.gpu_count);
        double scale = min(1.4, job.duration / profile.avg_duration) - 1.0;
        long_load_cost = 0.12 * load * max(0.15, scale);
    }
    double resource_cost = 0.0;
    int post_cpu = machine.getRemainingCPU() - job.cpu_cores;
    int post_mem = machine.getRemainingMemory() - job.memory;
    if (profile.cpu_demand_ratio > 0.58) {
        double cpu_rem = (double)post_cpu / max(1, spec.cpu_cores);
        if (cpu_rem < 0.10) resource_cost += 0.10 * (0.10 - cpu_rem) / 0.10;
    }
    if (profile.mem_demand_ratio > 0.58) {
        double mem_rem = (double)post_mem / max(1, spec.memory);
        if (mem_rem < 0.10) resource_cost += 0.10 * (0.10 - mem_rem) / 0.10;
    }
    return (0.32 - 0.06 * vram_w) * slack + vram_cost_w * vram_waste +
           0.24 * reservation + duration_w * ((double)job.duration / 5000.0) + scarcity +
           long_load_cost + resource_cost;
}

double GreedyScheduler::placementScore(const Job &job, int machine_index, int gpu_used,
                                       const vector<MachineState> &sim_machines,
                                       long long current_time,
                                       const vector<double> &reservation_scores) const {
    const auto &spec = sim_machines[machine_index].spec;
    int rem_gpu = sim_machines[machine_index].getRemainingGPU();
    int rem_cpu = sim_machines[machine_index].getRemainingCPU();
    int rem_mem = sim_machines[machine_index].getRemainingMemory();

    double gpu_ratio = (double)rem_gpu / spec.gpu_count;
    double cpu_ratio = (double)rem_cpu / spec.cpu_cores;
    double mem_ratio = (double)rem_mem / spec.memory;

    double vram_score = 0.0;
    // 计算每个GPU的可用显存
    int memory_per_gpu = spec.gpu_memory;
    if (memory_per_gpu <= 0) memory_per_gpu = 1;
    int total_available = memory_per_gpu * gpu_used;
    int total_job_vram = job.gpu_memory;

    if (total_job_vram <= total_available) {
        // 显存足够时，优先使用显存更紧张的GPU配置（提高利用率）
        double usage_ratio = (double)total_job_vram / total_available;
        vram_score = 0.3 * usage_ratio;  // 利用率越高分数越高
        // 对于大显存job，放到高显存服务器上更合适
        if (job.gpu_memory >= memory_per_gpu * 2 && memory_per_gpu >= 40) {
            vram_score += 0.15;  // 大job在大显存GPU上获得额外奖励
        }
    } else {
        vram_score = -0.3;  // 显存不足，严厉惩罚
    }

    // 改进的碎片化评分：考虑放置后的整体资源利用率
    double frag_score = 0.0;
    int post_gpu = rem_gpu - gpu_used;
    int post_cpu = rem_cpu - job.cpu_cores;
    int post_mem = rem_mem - job.memory;

    if (post_gpu > 0) {
        // 鼓励留下足够后续job运行的GPU，但不要过度碎片
        if (post_gpu >= job.min_gpu) frag_score += 0.15 * (double)post_gpu / spec.gpu_count;
        else if (post_gpu >= 1) frag_score -= 0.2;
        // 避免只留1-2个GPU的情况
        if (post_gpu == 1 || post_gpu == 2) frag_score -= 0.15;
    }
    if (post_cpu > 0) {
        double r = (double)post_cpu / spec.cpu_cores;
        if (r >= 0.25) frag_score += 0.1;
        else if (r < 0.1) frag_score -= 0.1;
        // 避免留下太少CPU
        if (r < 0.05 && post_cpu < 4) frag_score -= 0.1;
    }
    if (post_mem > 0) {
        double r = (double)post_mem / spec.memory;
        if (r >= 0.2) frag_score += 0.1;
        else if (r < 0.05) frag_score -= 0.1;
    }
    if (profile.cpu_demand_ratio > 0.58 && post_cpu > 0) {
        double r = (double)post_cpu / spec.cpu_cores;
        if (r < 0.08) frag_score -= 0.12 * (0.08 - r) / 0.08;
    }
    if (profile.mem_demand_ratio > 0.58 && post_mem > 0) {
        double r = (double)post_mem / spec.memory;
        if (r < 0.08) frag_score -= 0.12 * (0.08 - r) / 0.08;
    }

    // 负载均衡奖励：避免让某台机器过载
    double load = 1.0 - gpu_ratio;
    double load_score = 0.0;
    if (!isSingleServer()) {
        if (load > 0.85) load_score -= 0.3 * (load - 0.85) / 0.15;
        else if (load < 0.3) load_score += 0.15 * (0.3 - load) / 0.3;
    }

    if (profile.avg_duration > 0.0 && job.duration > profile.avg_duration * 1.4 && load > 0.55) {
        double long_penalty = 0.14;
        if (profile.long_job_ratio > 0.42) long_penalty = 0.18;
        load_score -= long_penalty * (load - 0.55) *
                      min(1.5, job.duration / max(1.0, profile.avg_duration));
    }

    double scarcity = 0.0;
    if (spec.gpu_memory >= 80) {
        // 稀缺高显存服务器：大任务奖励，小任务惩罚（保护 80GB 资源）
        if (job.gpu_memory >= memory_per_gpu) scarcity += 0.12;
        else scarcity -= 0.25;
        if (profile.scarce_80_ratio < 0.25 && job.gpu_memory < memory_per_gpu) {
            scarcity -= 0.06;
        }
    } else if (spec.gpu_memory <= 40 && job.gpu_memory <= memory_per_gpu) {
        scarcity += 0.08;  // 小任务优先用小显存服务器
    }
    if (profile.vram_pressure > 0.65 && job.gpu_memory <= memory_per_gpu * gpu_used) {
        vram_score += 0.04;
    }

    // 显存浪费惩罚：分配显存越紧越好（对应官方 VRAM 空闲指标）
    double vram_waste = (double)(total_available - total_job_vram) / max(1, total_available);
    vram_score += 0.22 * (1.0 - vram_waste);
    if (isNarrowCluster()) {
        vram_score += 0.10 * (1.0 - vram_waste);
    }
    if (isSingleServer()) {
        vram_score += 0.14 * (1.0 - vram_waste);
    }

    // 全局负载均衡评分 - 使用缓存避免重复计算
    double global_balance_bonus = 0.0;
    if (!isSingleServer()) {
        updateGlobalLoadCache(current_time, sim_machines);
        if (machine_index < (int)cached_machine_loads.size()) {
            double current_load = cached_machine_loads[machine_index];
            if (current_load < cached_avg_gpu_load) {
                global_balance_bonus = 0.1 * (cached_avg_gpu_load - current_load);
            }
            double impact = (double)job.weight * job.duration;
            double impact_denom = max(1.0, profile.avg_duration * 80.0);
            double impact_w = min(1.0, impact / impact_denom);
            global_balance_bonus += 0.12 * impact_w * max(0.0, cached_avg_gpu_load - current_load);
            if (profile.long_job_ratio < 0.38) {
                long long waited = max(0LL, current_time - job.release_time);
                double wait_frac = min(1.0, (double)waited / max(1LL, profile.time_horizon));
                double wait_impact = (double)job.weight * wait_frac;
                global_balance_bonus += 0.08 * (wait_impact / max(1.0, 50.0)) *
                                        max(0.0, cached_avg_gpu_load - current_load);
            }
            if (profile.burst_t0_ratio > 0.50 && profile.long_job_ratio < 0.35) {
                long long waited = max(0LL, current_time - job.release_time);
                double wait_frac = min(1.0, (double)waited / max(1LL, profile.time_horizon));
                double wait_impact = (double)job.weight * wait_frac;
                global_balance_bonus += 0.07 * (wait_impact / max(1.0, 50.0)) *
                                      max(0.0, cached_avg_gpu_load - current_load);
            }
            if (profile.long_job_ratio > 0.35 && profile.avg_duration > 0.0 &&
                job.duration > profile.avg_duration * 1.2) {
                double long_w = min(1.5, job.duration / profile.avg_duration) - 1.0;
                double long_coef = (profile.long_job_ratio > 0.42) ? 0.17 : 0.14;
                global_balance_bonus += long_coef * long_w * max(0.0, cached_avg_gpu_load - current_load);
            }
            if (profile.long_job_ratio > 0.45 && current_load < 0.08) {
                global_balance_bonus += 0.10 * min(1.2, job.duration / max(1.0, profile.avg_duration));
            }
        }
    }

    double reservation_penalty = 0.0;
    if (!reservation_scores.empty() && machine_index < (int)reservation_scores.size()) {
        double own = reservationContribution(job);
        double adjusted = max(0.0, reservation_scores[machine_index] - own);
        double max_res = 0.0;
        for (double s : reservation_scores) max_res = max(max_res, s);
        if (max_res > 0.0) reservation_penalty = 0.12 * (adjusted / max_res);
    }

    double slack_bonus = 0.08 * sim_machines[machine_index].placementSlack(
        job, gpu_used, min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0))));

    const double burst_w = profile.burst_t0_ratio;
    const double vram_w = min(1.0, profile.vram_pressure);
    global_balance_bonus *= (1.0 + 0.30 * burst_w);
    vram_score *= (1.0 + 0.12 * vram_w);
    reservation_penalty *= (1.0 + 0.18 * vram_w);

    double gw = 0.34, cw = 0.33, mw = 0.33;
    if (profile.cpu_demand_ratio > 0.52) {
        cw = 0.42;
        gw = 0.38;
        mw = 0.20;
    }
    if (profile.mem_demand_ratio > 0.52) {
        mw = 0.42;
        gw = 0.38;
        cw = 0.20;
    }
    double resource_fit = exp(gw * log(max(1e-6, gpu_ratio)) + cw * log(max(1e-6, cpu_ratio)) +
                          mw * log(max(1e-6, mem_ratio)));

    return resource_fit + vram_score + frag_score + load_score +
           scarcity + global_balance_bonus + slack_bonus - reservation_penalty;
}

void GreedyScheduler::buildFeasibleMachines() {
    for (const auto &job : jobs) {
        vector<pair<int, int>> entries = paretoPlacementOptions(job);
        if (entries.empty()) {
            feasible_machines[job.job_id] = {};
            continue;
        }
        feasible_machines[job.job_id] = entries;
    }
}

vector<pair<int, int>> GreedyScheduler::paretoPlacementOptions(const Job &job) const {
    vector<pair<int, int>> entries;
    for (int i = 0; i < (int)machines.size(); ++i) {
        int min_gpu = machines[i].requiredGpuCount(job);
        int memory_per_gpu = max(1, machines[i].spec.gpu_memory);
        int prev_waste = INT_MAX;
        for (int g = min_gpu; g <= machines[i].spec.gpu_count; ++g) {
            if (!machines[i].canEverRun(job, g)) continue;
            int waste = g * memory_per_gpu - job.gpu_memory;
            if (waste < prev_waste) {
                entries.push_back({i, g});
                prev_waste = waste;
            }
        }
    }
    return entries;
}
