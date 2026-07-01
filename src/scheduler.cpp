#include "scheduler.h"

#include <algorithm>
#include <cmath>
#include <climits>
#include <limits>
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <unordered_set>

using namespace std;

#ifndef CANDIDATE_WAIT_WEIGHT
#define CANDIDATE_WAIT_WEIGHT 1.25
#endif
#ifndef CANDIDATE_MEMORY_WEIGHT
#define CANDIDATE_MEMORY_WEIGHT 1.0
#endif
#ifndef CANDIDATE_FINISH_WEIGHT
#define CANDIDATE_FINISH_WEIGHT 1.0
#endif

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

GreedyScheduler::GreedyScheduler(vector<ServerSpec> input_servers, vector<Job> input_jobs,
                                 bool input_backfill)
    : servers(std::move(input_servers)), jobs(std::move(input_jobs)), backfill_enabled(input_backfill) {
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
}

bool GreedyScheduler::isNarrowCluster() const {
    return machines.size() <= 2;
}

int GreedyScheduler::jobFeasibleCount(int job_id) const {
    auto it = feasible_machines.find(job_id);
    return (it != feasible_machines.end()) ? static_cast<int>(it->second.size()) : 0;
}

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
    return min(1.0, d);
}

int GreedyScheduler::adaptiveStrategyCount() const {
    int base = 4;
    if (jobs.size() <= 20) base = 8;
    else if (jobs.size() <= 100) base = 16;
    else if (jobs.size() <= 500) base = 10;
    else if (jobs.size() <= 3000) base = 6;

    int bonus = 0;
    if (jobs.size() > 500) {
        bonus = static_cast<int>(instanceDifficulty() * 2.0 + 0.5);
        bonus = min(bonus, 1);
    }

    int cap = (jobs.size() <= 100) ? 18 : (jobs.size() <= 500) ? 10 : 7;
    return min(cap, base + bonus);
}

int GreedyScheduler::adaptivePendingCap(int queue_size) const {
    if (queue_size <= 0) return 0;
    if (jobs.size() <= 150) return queue_size;

    int cap = 50;
    if (jobs.size() > 500) cap = 44;
    else if (jobs.size() > 200) cap = 68;

    if (profile.burst_t0_ratio > 0.45) cap += 6;
    if (profile.vram_pressure > 0.55) cap += 4;
    if (profile.avg_feasible > 0.0 && profile.avg_feasible < 6.0) cap += 4;
    cap = min(cap, 80);

    return min(queue_size, cap);
}

int GreedyScheduler::resolvePlacementMode(int strategy_seed) const {
#if defined(PLACEMENT_P0)
    return 1;
#elif defined(PLACEMENT_P1)
    return 0;
#elif defined(PLACEMENT_P2)
    return 3;
#elif defined(PLACEMENT_P3)
    return 1;
#elif defined(PLACEMENT_P4)
    return 1;
#elif defined(PLACEMENT_P5)
    return 3;
#endif
    if (jobs.size() <= 80 && instanceDifficulty() < 0.32) {
        return strategy_seed % 4;
    }
    static const int modes_normal[] = {0, 1, 2, 3};
    static const int modes_burst[] = {0, 2, 0, 3};
    static const int modes_vram[] = {3, 1, 3, 0};
    if (isNarrowCluster()) return modes_vram[strategy_seed % 4];
    const int *table = modes_normal;
    if (profile.burst_t0_ratio > 0.45) table = modes_burst;
    else if (profile.vram_pressure > 0.55) table = modes_vram;
    return table[strategy_seed % 4];
}

bool GreedyScheduler::shouldUseRuntimePareto() const {
    return jobs.size() <= 500;
}

bool GreedyScheduler::shouldLightRefine() const {
    return jobs.size() > 100 && jobs.size() <= 165;
}

bool GreedyScheduler::shouldFastRefine() const {
    if (jobs.size() <= 250 || jobs.size() > 380) return false;
    return instanceDifficulty() > 0.40;
}

bool GreedyScheduler::shouldLongJobRefine() const {
    return jobs.size() > 80 && jobs.size() <= 420 && profile.long_job_ratio > 0.45;
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
        computeOfficialMetrics(best);
    } else if (shouldLightRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = refinePlacement(best, 1);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
            Solution refined_runner = refinePlacement(runner_up, 1);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
    } else if (shouldLongJobRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
    } else if (shouldFastRefine() && isScheduleValid(initial)) {
        computeOfficialMetrics(best);
        Solution refined = fastRefinePlacement(best);
        if (isBetterOfficialSolution(refined, best)) best = refined;
        if (isScheduleValid(runner_up) && runner_up.records.size() == jobs.size()) {
            Solution refined_runner = fastRefinePlacement(runner_up);
            if (isBetterOfficialSolution(refined_runner, best)) best = refined_runner;
        }
    }

    // 贪心已产出全量合法解时跳过退火，避免 replay 扰动合法解
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
    if (!isScheduleValid(candidate)) return false;
    if (!isScheduleValid(current)) return true;
    return candidate.score < current.score;
}

GreedyScheduler::Solution GreedyScheduler::generateMultiStrategySolution(int num_strategies,
                                                                         Solution *runner_up,
                                                                         Solution *third_place) {
    struct RankedCandidate {
        Solution solution;
        bool valid = false;
        double balanced_score = 0.0;
    };

    vector<RankedCandidate> candidates;
    candidates.reserve(num_strategies);

    for (int s = 0; s < num_strategies; ++s) {
        Solution sol = generateGreedySolutionWithStrategy(s);
        if (jobs.size() <= 2000) computeOfficialMetrics(sol);
        else computeMetrics(sol);

        bool valid = sol.records.size() == jobs.size();
        if (valid && jobs.size() <= 2000) valid = isScheduleValid(sol);
        candidates.push_back(RankedCandidate{std::move(sol), valid, 0.0});
    }

    if (candidates.empty()) return Solution{};

    double min_wait = numeric_limits<double>::infinity();
    double max_wait = -numeric_limits<double>::infinity();
    double min_memory = numeric_limits<double>::infinity();
    double max_memory = -numeric_limits<double>::infinity();
    double min_finish = numeric_limits<double>::infinity();
    double max_finish = -numeric_limits<double>::infinity();

    for (const auto &candidate : candidates) {
        if (!candidate.valid) continue;
        min_wait = min(min_wait, candidate.solution.weighted_waiting);
        max_wait = max(max_wait, candidate.solution.weighted_waiting);
        min_memory = min(min_memory, candidate.solution.vram_idle);
        max_memory = max(max_memory, candidate.solution.vram_idle);
        min_finish = min(min_finish, candidate.solution.makespan);
        max_finish = max(max_finish, candidate.solution.makespan);
    }

    auto normalized = [](double value, double low, double high) {
        return high > low ? (value - low) / (high - low) : 0.0;
    };
    for (auto &candidate : candidates) {
        if (!candidate.valid) {
            candidate.balanced_score = numeric_limits<double>::infinity();
            continue;
        }
        candidate.balanced_score =
            CANDIDATE_WAIT_WEIGHT * normalized(candidate.solution.weighted_waiting, min_wait, max_wait) +
            CANDIDATE_MEMORY_WEIGHT * normalized(candidate.solution.vram_idle, min_memory, max_memory) +
            CANDIDATE_FINISH_WEIGHT * normalized(candidate.solution.makespan, min_finish, max_finish);
    }

    stable_sort(candidates.begin(), candidates.end(), [](const RankedCandidate &a,
                                                          const RankedCandidate &b) {
        if (a.valid != b.valid) return a.valid > b.valid;
        if (a.solution.records.size() != b.solution.records.size()) {
            return a.solution.records.size() > b.solution.records.size();
        }
        if (a.balanced_score != b.balanced_score) {
            return a.balanced_score < b.balanced_score;
        }
        return a.solution.score < b.solution.score;
    });

    if (runner_up && candidates.size() > 1) *runner_up = candidates[1].solution;
    if (third_place && candidates.size() > 2) *third_place = candidates[2].solution;
    return candidates.front().solution;
}

GreedyScheduler::Solution GreedyScheduler::generateGreedySolutionWithStrategy(int strategy_seed) {
    Solution sol;
    if (jobs.empty()) return sol;

    cached_global_load_time = -1;
    srand(static_cast<unsigned>(strategy_seed * 7919u + 17u));

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;

    priority_queue<Job, vector<Job>, UrgencyComparator> pending_jobs;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;

    int sort_strategy = (strategy_seed / 4) % 4;
    const int placement_mode = resolvePlacementMode(strategy_seed);

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
        int max_evaluate = adaptivePendingCap(static_cast<int>(pending_jobs.size()));

        if (jobs.size() <= 200) {
            while (!pending_jobs.empty()) {
                pending_list.push_back(pending_jobs.top());
                pending_jobs.pop();
            }
        } else if (jobs.size() > 500) {
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
        } else {
            vector<Job> all_pending;
            all_pending.reserve(pending_jobs.size());
            while (!pending_jobs.empty()) {
                all_pending.push_back(pending_jobs.top());
                pending_jobs.pop();
            }

            const int n = static_cast<int>(all_pending.size());
            const int take = min(max_evaluate, n);
            const int start = (n > take) ? ((strategy_seed * 7 + static_cast<int>(current_time % 97)) % n) : 0;
            pending_list.reserve(take);
            vector<bool> picked(n, false);
            for (int i = 0; i < take; ++i) {
                int idx = (start + i) % n;
                pending_list.push_back(all_pending[idx]);
                picked[idx] = true;
            }
            for (int i = 0; i < n; ++i) {
                if (!picked[i]) pending_jobs.push(all_pending[i]);
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

        // 成员 B：顺序尊重 A 的 UrgencyComparator；同优先级下大显存任务优先放置（装箱启发）
        const double burst_ratio = profile.burst_t0_ratio;
        const double vram_p = profile.vram_pressure;
        const double hetero = profile.hetero_ratio;
        const double long_ratio = profile.long_job_ratio;
        const bool narrow = isNarrowCluster();
        const bool constrained_first = narrow || profile.avg_feasible < 10.0 ||
            (profile.avg_feasible < 14.0 && profile.burst_t0_ratio > 0.38);
#if defined(DISPATCH_D1)
        // Age-aware dispatch: 0.40*wspt + 0.40*age
        sort(pending_list.begin(), pending_list.end(),
             [this, current_time, constrained_first](const Job &a, const Job &b) {
            double wspt_a = (double)a.weight / a.duration;
            double wspt_b = (double)b.weight / b.duration;
            double age_a = (double)(current_time - a.release_time);
            double age_b = (double)(current_time - b.release_time);
            double u_a = 0.40 * wspt_a + 0.40 * age_a;
            double u_b = 0.40 * wspt_b + 0.40 * age_b;
            if (u_a != u_b) return u_a > u_b;
            if (constrained_first) {
                int fa = jobFeasibleCount(a.job_id);
                int fb = jobFeasibleCount(b.job_id);
                if (fa != fb) return fa < fb;
            }
            return a.job_id < b.job_id;
        });
#elif defined(DISPATCH_D3)
        // WSPT variant with sqrt(duration)
        sort(pending_list.begin(), pending_list.end(),
             [this, constrained_first](const Job &a, const Job &b) {
            double wsa = 1000000.0 * (double)a.weight / sqrt((double)a.duration);
            double wsb = 1000000.0 * (double)b.weight / sqrt((double)b.duration);
            if (wsa != wsb) return wsa > wsb;
            if (constrained_first) {
                int fa = jobFeasibleCount(a.job_id);
                int fb = jobFeasibleCount(b.job_id);
                if (fa != fb) return fa < fb;
            }
            return a.job_id < b.job_id;
        });
#elif defined(DISPATCH_D4)
        // opt1.0-style: WSPT → constrained → SHORT duration first (SPT minimizes E_wait)
        sort(pending_list.begin(), pending_list.end(),
             [this, constrained_first](const Job &a, const Job &b) {
            UrgencyComparator cmp;
            if (cmp(a, b)) return false;
            if (cmp(b, a)) return true;
            if (constrained_first) {
                int fa = jobFeasibleCount(a.job_id);
                int fb = jobFeasibleCount(b.job_id);
                if (fa != fb) return fa < fb;
            }
            if (a.duration != b.duration) return a.duration < b.duration;
            if (a.weight != b.weight) return a.weight > b.weight;
            if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
            if (a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
            return a.job_id < b.job_id;
        });
#else
        // Default: member-b's original sort comparator (DISPATCH_D0 / DISPATCH_D2 fall through)
        sort(pending_list.begin(), pending_list.end(),
             [this, burst_ratio, vram_p, hetero, long_ratio, constrained_first](const Job &a, const Job &b) {
            UrgencyComparator cmp;
            if (cmp(a, b)) return false;
            if (cmp(b, a)) return true;
            if (constrained_first) {
                int fa = jobFeasibleCount(a.job_id);
                int fb = jobFeasibleCount(b.job_id);
                if (fa != fb) return fa < fb;
            }
            if (long_ratio > 0.35 && a.duration != b.duration) return a.duration > b.duration;
            if (profile.cpu_demand_ratio > 0.62 && a.cpu_cores != b.cpu_cores) {
                return a.cpu_cores > b.cpu_cores;
            }
            if (profile.mem_demand_ratio > 0.62 && a.memory != b.memory) {
                return a.memory > b.memory;
            }
            if (vram_p > 0.55 && a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
            if (burst_ratio > 0.40 && a.weight != b.weight) return a.weight > b.weight;
            if (hetero > 2.0 && a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
            if (a.gpu_memory != b.gpu_memory) return a.gpu_memory > b.gpu_memory;
            if (a.min_gpu != b.min_gpu) return a.min_gpu > b.min_gpu;
            return a.job_id < b.job_id;
        });
#endif

        bool scheduled_one = false;
        vector<Job> deferred;

        for (const auto &job : pending_list) {
            PlacementPick pick = chooseBestPlacement(
                job, sim_machines, current_time, reservation_scores, placement_mode, sort_strategy);
            if (pick.machine_index < 0) {
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
            sort(deferred.begin(), deferred.end(), [this](const Job &a, const Job &b) {
                return jobFeasibleCount(a.job_id) < jobFeasibleCount(b.job_id);
            });
        }
        // Backfill (from test/lab2.0): retry deferred jobs after main dispatch
        if (backfill_enabled && !deferred.empty()) {
            vector<Job> still_deferred;
            still_deferred.reserve(deferred.size());
            for (const Job &job : deferred) {
                PlacementPick pick = chooseBestPlacement(
                    job, sim_machines, current_time, reservation_scores,
                    placement_mode, sort_strategy);
                if (pick.machine_index >= 0) {
                    auto result = sim_machines[pick.machine_index].startJob(
                        job, current_time, pick.gpu_used);
                    records[job.job_id] = result.first;
                    running_heap.push(FinishEvent{result.second.finish_time,
                        result.second.server_id, result.second.job_id,
                        result.second});
                } else {
                    still_deferred.push_back(job);
                }
            }
            deferred = move(still_deferred);
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
                replaySchedule(candidate);
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
            replaySchedule(candidate);
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
        if (profile.long_job_ratio > 0.40 && ja->duration != jb->duration) {
            return ja->duration > jb->duration;
        }
        long long pa = (long long)ja->weight * ja->duration;
        long long pb = (long long)jb->weight * jb->duration;
        if (pa != pb) return pa > pb;
        return ja->gpu_memory > jb->gpu_memory;
    });

    const size_t top_k = min(order.size(),
        max((size_t)12, (size_t)(order.size() * (0.06 + 0.05 * instanceDifficulty()))));
    const size_t max_try = isNarrowCluster() ? 8 : ((profile.vram_pressure > 0.55) ? 6 : 5);

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
            replaySchedule(candidate);
            if (!isScheduleValid(candidate)) continue;
            computeOfficialMetrics(candidate);
            if (isBetterOfficialSolution(candidate, best)) {
                best = candidate;
            }
        }
    }

    const size_t swap_n = min((size_t)5, order.size());
    for (size_t i = 0; i + 1 < swap_n; ++i) {
        Solution candidate = best;
        size_t a = order[i], b = order[i + 1];
        swap(candidate.records[a].server_id, candidate.records[b].server_id);
        swap(candidate.records[a].gpu_used, candidate.records[b].gpu_used);
        replaySchedule(candidate);
        if (!isScheduleValid(candidate)) continue;
        computeOfficialMetrics(candidate);
        if (isBetterOfficialSolution(candidate, best)) best = candidate;
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
    if (candidate.score + 1e-9 < current.score) return true;
    if (candidate.score > current.score + 1e-9) return false;
    if (candidate.weighted_waiting != current.weighted_waiting) {
        return candidate.weighted_waiting < current.weighted_waiting;
    }
    if (fabs(candidate.vram_idle - current.vram_idle) > 1e-6) {
        return candidate.vram_idle < current.vram_idle;
    }
    return candidate.makespan < current.makespan;
}

void GreedyScheduler::computeOfficialMetrics(Solution &sol) const {
    computeMetrics(sol);
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

    // 修正规则允许统一权重 rho_i = 1/N：
    // E_memory = sum_i rho_i * (u_i * VG_{s_i} - v_i)
    long long total_vram_waste = 0;
    for (const auto &rec : sol.records) {
        auto job_it = job_map.find(rec.job_id);
        auto mach_it = machine_index_by_id.find(rec.server_id);
        if (job_it == job_map.end() || mach_it == machine_index_by_id.end()) continue;
        const Job *job = job_it->second;
        int vg = machines[mach_it->second].spec.gpu_memory;
        long long allocated = (long long)rec.gpu_used * vg;
        long long waste = max(0LL, allocated - job->gpu_memory);
        total_vram_waste += waste;
    }

    sol.weighted_waiting = (double)total_weighted_wait;
    sol.makespan = (double)makespan;
    sol.vram_idle = jobs.empty() ? 0.0 : (double)total_vram_waste / (double)jobs.size();
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

    auto entries_it = feasible_machines.find(job.job_id);
    if (entries_it == feasible_machines.end() || entries_it->second.empty()) {
        return best;
    }

    auto consider = [&](int machine_index, int gpu_used) {
        if (!sim_machines[machine_index].canStart(job, gpu_used)) return;

        int mpg = max(1, sim_machines[machine_index].spec.gpu_memory);
        int vram_waste = gpu_used * mpg - job.gpu_memory;
        const double gpu_emphasis = min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0)));

        if (placement_mode == 1 || placement_mode == 3) {
            double cost = placement_mode == 1
                ? placementCost(job, machine_index, gpu_used, sim_machines, current_time, reservation_scores)
                : ((double)vram_waste / max(1, job.gpu_memory)) +
                  0.18 * sim_machines[machine_index].placementSlack(job, gpu_used, gpu_emphasis);
            if (vram_waste == 0) cost -= 0.05;
            if (sort_strategy > 0) {
                cost += (rand() % 1000) / 1000.0 * 0.04 * sort_strategy;
            }
            if (cost < best_cost - 1e-9 ||
                (fabs(cost - best_cost) <= 1e-9 &&
                 (vram_waste < best_vram_waste ||
                  (vram_waste == best_vram_waste && gpu_used < best.gpu_used)))) {
                best_cost = cost;
                best.machine_index = machine_index;
                best.gpu_used = gpu_used;
                best_vram_waste = vram_waste;
            }
            return;
        }

        double score = placementScore(job, machine_index, gpu_used, sim_machines,
                                      current_time, reservation_scores);
        if (placement_mode == 2) {
            score += 0.10 * sim_machines[machine_index].placementSlack(
                job, gpu_used, min(0.58, max(0.34, 0.34 + 0.08 * max(0.0, profile.avg_min_gpu - 1.0))));
        }
        if (sort_strategy > 0) {
            score += (rand() % 1000) / 1000.0 * 0.06 * sort_strategy;
        }
        if (vram_waste == 0) score += 0.06;

        if (score > best_score + 1e-9 ||
            (fabs(score - best_score) <= 1e-9 &&
             (vram_waste < best_vram_waste ||
              (vram_waste == best_vram_waste && gpu_used < best.gpu_used)))) {
            best_score = score;
            best.machine_index = machine_index;
            best.gpu_used = gpu_used;
            best_vram_waste = vram_waste;
        }
    };

    if (shouldUseRuntimePareto()) {
        unordered_set<int> machine_indices;
        for (const auto &entry : entries_it->second) machine_indices.insert(entry.first);

        for (int mi : machine_indices) {
            const auto &ms = sim_machines[mi];
            int min_g = ms.requiredGpuCount(job);
            int max_g = min(ms.spec.gpu_count, ms.getRemainingGPU());
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
    return (0.32 - 0.06 * vram_w) * slack + vram_cost_w * vram_waste +
           0.24 * reservation + 0.08 * ((double)job.duration / 5000.0) + scarcity;
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

    // 负载均衡奖励：避免让某台机器过载
    double load = 1.0 - gpu_ratio;
    double load_score = 0.0;
    if (load > 0.85) load_score -= 0.3 * (load - 0.85) / 0.15;
    else if (load < 0.3) load_score += 0.15 * (0.3 - load) / 0.3;

    if (profile.avg_duration > 0.0 && job.duration > profile.avg_duration * 1.4 && load > 0.55) {
        load_score -= 0.14 * (load - 0.55) *
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

    // 全局负载均衡评分 - 使用缓存避免重复计算
    double global_balance_bonus = 0.0;
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
        if (profile.long_job_ratio > 0.35 && profile.avg_duration > 0.0 &&
            job.duration > profile.avg_duration * 1.2) {
            double long_w = min(1.5, job.duration / profile.avg_duration) - 1.0;
            global_balance_bonus += 0.14 * long_w * max(0.0, cached_avg_gpu_load - current_load);
        }
        if (profile.long_job_ratio > 0.45 && current_load < 0.08) {
            global_balance_bonus += 0.10 * min(1.2, job.duration / max(1.0, profile.avg_duration));
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
    if (profile.cpu_demand_ratio > 0.55) {
        cw = 0.42;
        gw = 0.38;
        mw = 0.20;
    }
    if (profile.mem_demand_ratio > 0.55) {
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
