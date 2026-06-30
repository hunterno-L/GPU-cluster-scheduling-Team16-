#include "scheduler.h"

#include <algorithm>
#include <cmath>
#include <climits>
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <iostream>
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
    if (instanceDifficulty() > 0.40) return true;
    if (profile.release_spread_ratio > 0.50 && profile.long_job_ratio > 0.30) return true;
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) return true;
    if (profile.burst_t0_ratio > 0.55 && profile.avg_feasible < 12.0) return true;
    return false;
}

vector<ScheduleRecord> GreedyScheduler::schedule() {
    if (jobs.empty()) return {};

    int num_strategies = adaptiveStrategyCount();

    const int alns_min_jobs = 60;
    Solution runner_up;
    Solution third_place;
    Solution initial = generateMultiStrategySolution(num_strategies, &runner_up, &third_place);
    Solution best = initial;

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
        Solution refined = refinePlacement(best, 1);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
            Solution refined_runner = refinePlacement(runner_up, 1);
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
    } else if (shouldLongJobRefine() && isScheduleValid(initial)) {
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

    if (shouldAssignmentReplayPolish() && isScheduleValid(best)) {
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

    if (runner_up) *runner_up = second_sol;
    if (third_place) *third_place = third_sol;
    return best_sol;
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
        if (!pending_urgency_sorted) {
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
            if (!isMegascaleInstance() && !isLargeInstance() && jobs.size() <= 420 &&
                (profile.burst_t0_ratio > 0.45 ||
                 (profile.long_job_ratio > 0.38 && profile.burst_t0_ratio > 0.30))) {
                sort(deferred.begin(), deferred.end(),
                     [this, order_variant, &sim_machines, &current_time](const Job &a, const Job &b) {
                    long long ea = minEarliestStartForJob(a, sim_machines, current_time);
                    long long eb = minEarliestStartForJob(b, sim_machines, current_time);
                    if (ea != eb) return ea < eb;
                    return jobMoreUrgentFirst(a, b, order_variant, current_time);
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

    for (size_t oi = 0; oi < top_k; ++oi) {
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

        const size_t try_n = min(candidates.size(), max_try);
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
            computeOfficialMetrics(candidate);
            if (isBetterOfficialSolution(candidate, best)) {
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
    if (shouldDualReplayInRefine()) replayScheduleAdaptive(n);
    else replaySchedule(n);
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
        out.push_back(0);
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
    if (jobs.size() > 650) return false;
    if (isSingleServer()) return jobs.size() <= 320;
    if (!isSingleServer() && profile.long_job_ratio > 0.42) return jobs.size() <= 420;
    if (profile.burst_t0_ratio > 0.45) return jobs.size() <= 420;
    if (profile.cpu_demand_ratio > 0.60 || profile.mem_demand_ratio > 0.60) {
        return jobs.size() <= 360;
    }
    return jobs.size() <= 220 && instanceDifficulty() > 0.38;
}

bool GreedyScheduler::shouldAssignmentReplayPolish() const {
    if (jobs.size() < 40 || jobs.size() > 3500) return false;
    if (isMegascaleInstance()) return false;
    if (!isSingleServer() && profile.long_job_ratio > 0.42) {
        return jobs.size() <= 420;
    }
    return isSingleServer() || instanceDifficulty() >= 0.30;
}

void GreedyScheduler::replayScheduleForRefine(Solution &sol) {
    if (shouldDualReplayInRefine()) replayScheduleAdaptive(sol);
    else replayForRefine(sol);
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
        computeOfficialMetrics(wspt);
        computeOfficialMetrics(alt_sol);
        sol = isBetterOfficialSolution(alt_sol, wspt) ? alt_sol : wspt;
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
