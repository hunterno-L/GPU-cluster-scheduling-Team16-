#include "scheduler.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <iostream>

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
}

vector<ScheduleRecord> GreedyScheduler::schedule() {
    if (jobs.empty()) return {};

    // 根据问题规模调整策略数量（兼顾质量与 60s 时限）
    int num_strategies = (jobs.size() <= 20) ? 4 : (jobs.size() <= 100) ? 6
                         : (jobs.size() <= 500 ? 4 : 2);
    Solution initial = generateMultiStrategySolution(num_strategies);
    Solution best = initial;

    // 已有全量合法解时跳过退火；仅对 61~100 规模或非法/漏任务做轻量退火
    bool need_refine = !isScheduleValid(initial) ||
                       initial.records.size() < jobs.size() ||
                       (jobs.size() > 60 && jobs.size() <= 100);
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

GreedyScheduler::Solution GreedyScheduler::generateMultiStrategySolution(int num_strategies) {
    Solution best_sol;

    for (int s = 0; s < num_strategies; ++s) {
        Solution sol = generateGreedySolutionWithStrategy(s);
        computeMetrics(sol);
        if (isBetterSolution(sol, best_sol)) {
            best_sol = sol;
        }
    }

    return best_sol;
}

GreedyScheduler::Solution GreedyScheduler::generateGreedySolutionWithStrategy(int strategy_seed) {
    Solution sol;
    if (jobs.empty()) return sol;

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;

    priority_queue<Job, vector<Job>, UrgencyComparator> pending_jobs;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;
    vector<MachineState> sim_machines = machines;

    int sort_strategy = strategy_seed % 4;

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
        int max_evaluate = static_cast<int>(pending_jobs.size());
        if (jobs.size() > 500) {
            max_evaluate = min(max_evaluate, 20);
        } else if (jobs.size() > 150) {
            max_evaluate = min(max_evaluate, 50);
        }

        if (jobs.size() > 500) {
            int pulled = 0;
            while (!pending_jobs.empty() && pulled < max_evaluate) {
                pending_list.push_back(pending_jobs.top());
                pending_jobs.pop();
                ++pulled;
            }
        } else if (jobs.size() <= 60) {
            while (!pending_jobs.empty()) {
                pending_list.push_back(pending_jobs.top());
                pending_jobs.pop();
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

        double best_score = -1e18;
        Job best_job = pending_list[0];
        int best_machine = -1, best_gpu = 0;

        for (const auto &job : pending_list) {
            auto entries_it = feasible_machines.find(job.job_id);
            if (entries_it == feasible_machines.end()) continue;
            if (entries_it->second.empty()) continue;  // 跳过没有可行机器的job

            for (const auto &entry : entries_it->second) {
                if (!sim_machines[entry.first].canStart(job, entry.second)) continue;

                double score = placementScore(job, entry.first, entry.second, sim_machines, current_time);

                if (sort_strategy > 0) {
                    double noise = (rand() % 1000) / 1000.0 * 0.1 * sort_strategy;
                    score += noise;
                }

                if (score > best_score ||
                    (score == best_score && job.weight > best_job.weight)) {
                    best_score = score;
                    best_job = job;
                    best_machine = entry.first;
                    best_gpu = entry.second;
                }
            }
        }

        bool scheduled_one = (best_machine >= 0);
        if (scheduled_one) {
            auto result = sim_machines[best_machine].startJob(best_job, current_time, best_gpu);
            records[best_job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                        result.second.job_id, result.second});
        }

        // 只有有可行机器的job才能重新入队等待
        for (const auto &job : pending_list) {
            if (scheduled_one && job.job_id == best_job.job_id) continue;
            auto it = feasible_machines.find(job.job_id);
            if (it != feasible_machines.end() && !it->second.empty()) {
                pending_jobs.push(job);
            }
            // 空feasible_machines的job直接跳过，永不可调度
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

    // 显存浪费积分（轻量近似官方 VRAM 空闲指标，避免退火中 O(n^2) 开销）
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

    // Destroy-Repair邻域：破坏并重建部分解
    if (jobs.size() <= 120) {
        for (int dc : {3, 5, 7}) {
            Solution n = neighborhoodDestroyRepair(sol, dc);
            if (isBetterSolution(n, best_neighbor)) best_neighbor = n;
        }
    }

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
    verifyAndFix(n);
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
    verifyAndFix(n);
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
        n.records[idx].gpu_used = it->second[choice].second;  // 使用新机器的GPU数量
    }
    verifyAndFix(n);
    computeMetrics(n);
    return n;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodDestroyRepair(const Solution &sol, int destroy_count) {
    Solution n = sol;

    vector<int> to_remove;
    for (int i = 0; i < destroy_count && i < (int)sol.records.size(); ++i) {
        to_remove.push_back(rand() % sol.records.size());
    }

    vector<ScheduleRecord> remaining;
    for (size_t i = 0; i < sol.records.size(); ++i) {
        bool removed = false;
        for (int idx : to_remove)
            if ((int)i == idx) { removed = true; break; }
        if (!removed) remaining.push_back(sol.records[i]);
    }

    for (int idx : to_remove) {
        int job_id = sol.records[idx].job_id;
        auto it = feasible_machines.find(job_id);
        if (it != feasible_machines.end() && !it->second.empty()) {
            int choice = rand() % it->second.size();
            ScheduleRecord rec = sol.records[idx];
            rec.server_id = machines[it->second[choice].first].spec.server_id;
            remaining.push_back(rec);
        }
    }

    n.records = remaining;
    verifyAndFix(n);
    computeMetrics(n);
    return n;
}

void GreedyScheduler::verifyAndFix(Solution &sol) {
    unordered_map<int, const Job*> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    long long current_time = jobs.front().release_time;
    vector<MachineState> sim = machines;

    struct Event {
        long long time;
        int job_id;
        int server_id;
        int gpu_used;
        bool is_start;
    };
    vector<Event> events;

    for (const auto &rec : sol.records) {
        events.push_back({rec.start_time, rec.job_id, rec.server_id, rec.gpu_used, true});
        events.push_back({rec.finish_time, rec.job_id, rec.server_id, rec.gpu_used, false});
    }

    sort(events.begin(), events.end(), [](const Event &a, const Event &b) {
        if (a.time != b.time) return a.time < b.time;
        // 同一时刻先处理结束事件，避免可立即复用的资源被误判为不可用
        return !a.is_start && b.is_start;
    });

    unordered_map<int, ScheduleRecord> new_records;

    for (const auto &e : events) {
        if (e.is_start) {
            auto job_it = job_map.find(e.job_id);
            if (job_it == job_map.end()) continue;
            auto mach_it = machine_index_by_id.find(e.server_id);
            if (mach_it == machine_index_by_id.end()) continue;

            if (!sim[mach_it->second].canStart(*job_it->second, e.gpu_used)) continue;

            auto result = sim[mach_it->second].startJob(*job_it->second, e.time, e.gpu_used);
            new_records[e.job_id] = result.first;
        } else {
            auto mach_it = machine_index_by_id.find(e.server_id);
            if (mach_it == machine_index_by_id.end()) continue;

            RunningJob rj{e.job_id, e.server_id, 0, e.gpu_used, 0, 0};
            for (auto &rec : new_records) {
                if (rec.second.job_id == e.job_id) {
                    rj.finish_time = rec.second.finish_time;
                    rj.cpu_used = jobs[rec.first - 1].cpu_cores;
                    rj.memory_used = jobs[rec.first - 1].memory;
                    break;
                }
            }
            sim[mach_it->second].releaseJob(rj);
        }
    }

    sol.records.clear();
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = new_records.find(job_id);
        if (it != new_records.end()) sol.records.push_back(it->second);
    }
}

void GreedyScheduler::updateGlobalLoadCache(long long current_time,
                                            const vector<MachineState> &sim_machines) const {
    if (cached_global_load_time == current_time) return;
    cached_global_load_time = current_time;

    double total_gpu = 0, total_used = 0;
    cached_machine_loads.resize(sim_machines.size());
    for (size_t i = 0; i < sim_machines.size(); ++i) {
        total_gpu += sim_machines[i].spec.gpu_count;
        total_used += sim_machines[i].spec.gpu_count - sim_machines[i].getRemainingGPU();
    }
    cached_avg_gpu_load = (total_gpu > 0) ? total_used / total_gpu : 0;
}

double GreedyScheduler::placementScore(const Job &job, int machine_index, int gpu_used,
                                       const vector<MachineState> &sim_machines,
                                       long long current_time) const {
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
    if (load > 0.85) load_score -= 0.3 * (load - 0.85) / 0.15;  // 过载惩罚加重
    else if (load < 0.3) load_score += 0.15 * (0.3 - load) / 0.3;

    double scarcity = 0.0;
    if (spec.gpu_memory >= 80) {
        // 稀缺高显存服务器：大任务奖励，小任务惩罚（保护 80GB 资源）
        if (job.gpu_memory >= memory_per_gpu) scarcity += 0.12;
        else scarcity -= 0.25;
    } else if (spec.gpu_memory <= 40 && job.gpu_memory <= memory_per_gpu) {
        scarcity += 0.08;  // 小任务优先用小显存服务器
    }

    // 显存浪费惩罚：分配显存越紧越好（对应官方 VRAM 空闲指标）
    double vram_waste = (double)(total_available - total_job_vram) / max(1, total_available);
    vram_score += 0.2 * (1.0 - vram_waste);

    // 紧迫性：高权重任务等待越久，放置优先级越高（对齐加权等待指标）
    double urgency = jobUrgency(job, current_time);
    double wait_penalty = (double)job.weight * max(0LL, current_time - job.release_time) / 1000.0;
    double urgency_bonus = urgency * 0.06 * gpu_ratio + wait_penalty * 0.02;

    // 全局负载均衡评分 - 使用缓存避免重复计算
    double global_balance_bonus = 0.0;
    updateGlobalLoadCache(current_time, sim_machines);
    if (machine_index < (int)cached_machine_loads.size()) {
        double current_load = (spec.gpu_count > 0) ? (spec.gpu_count - rem_gpu) / (double)spec.gpu_count : 0;
        if (current_load < cached_avg_gpu_load) {
            global_balance_bonus = 0.1 * (cached_avg_gpu_load - current_load);
        }
    }

    return pow(gpu_ratio * cpu_ratio * mem_ratio, 1.0/3.0) + vram_score + frag_score + load_score + scarcity + urgency_bonus + global_balance_bonus;
}

double GreedyScheduler::jobUrgency(const Job &job, long long current_time) const {
    double wspt = (double)job.weight / job.duration;
    long long waiting = current_time - job.release_time;
    double wait_factor = 1.0 + log1p((double)waiting) * 0.1;
    double resource_demand = (double)job.min_gpu / 8.0 + (double)job.memory / 512.0;
    return wspt * wait_factor * (1.0 + resource_demand * 0.2);
}

void GreedyScheduler::buildFeasibleMachines() {
    for (const auto &job : jobs) {
        vector<pair<int, int>> entries;
        for (int i = 0; i < (int)machines.size(); ++i) {
            int gpu_used = machines[i].requiredGpuCount(job);
            if (machines[i].canEverRun(job, gpu_used)) {
                entries.push_back({i, gpu_used});
            }
        }
        // 如果没有任何可行的服务器配置，跳过这个job（数据集中可能存在无法满足的极端需求）
        if (entries.empty()) {
            feasible_machines[job.job_id] = {};  // 标记为空，后续会跳过
            continue;
        }
        feasible_machines[job.job_id] = entries;
    }
}
