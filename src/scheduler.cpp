#include "scheduler.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <cstdlib>
#include <ctime>

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

    Solution initial = generateMultiStrategySolution(5);

    if (jobs.size() <= 50 && jobs.size() > 1) {
        Solution optimized = adaptiveSimulatedAnnealing(initial);
        return optimized.records;
    }

    return initial.records;
}

GreedyScheduler::Solution GreedyScheduler::generateMultiStrategySolution(int num_strategies) {
    Solution best_sol;

    for (int s = 0; s < num_strategies; ++s) {
        Solution sol = generateGreedySolutionWithStrategy(s);
        computeMetrics(sol);
        if (best_sol.records.empty() || sol.score < best_sol.score) {
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
        int count = 0;
        while (!pending_jobs.empty() && count < 5) {
            pending_list.push_back(pending_jobs.top());
            pending_jobs.pop();
            ++count;
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

            for (const auto &entry : entries_it->second) {
                if (!sim_machines[entry.first].canStart(job, entry.second)) continue;

                double score = placementScore(job, entry.first, entry.second, current_time);

                if (sort_strategy > 0) {
                    double noise = (rand() % 1000) / 1000.0 * 0.1 * sort_strategy;
                    score += noise;
                }

                if (score > best_score) {
                    best_score = score;
                    best_job = job;
                    best_machine = entry.first;
                    best_gpu = entry.second;
                }
            }
        }

        if (best_machine >= 0) {
            auto result = sim_machines[best_machine].startJob(best_job, current_time, best_gpu);
            records[best_job.job_id] = result.first;
            running_heap.push(FinishEvent{result.second.finish_time, result.second.server_id,
                                        result.second.job_id, result.second});
        }

        for (const auto &job : pending_list)
            if (job.job_id != best_job.job_id) pending_jobs.push(job);

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

    sol.records.reserve(records.size());
    for (int job_id = 1; job_id <= (int)jobs.size(); ++job_id) {
        auto it = records.find(job_id);
        if (it != records.end()) sol.records.push_back(it->second);
    }

    return sol;
}

void GreedyScheduler::computeMetrics(Solution &sol) const {
    if (sol.records.empty()) {
        sol.score = sol.weighted_waiting = sol.makespan = sol.gpu_utilization = 0;
        return;
    }

    unordered_map<int, const Job*> job_map;
    for (const auto &job : jobs) job_map[job.job_id] = &job;

    long long total_weighted_wait = 0;
    long long makespan = 0;
    long long total_gpu_used = 0;

    for (const auto &rec : sol.records) {
        auto it = job_map.find(rec.job_id);
        if (it == job_map.end()) continue;
        const Job *job = it->second;

        long long waiting = rec.start_time - job->release_time;
        total_weighted_wait += waiting * job->weight;
        makespan = max(makespan, rec.finish_time);
        total_gpu_used += (long long)rec.gpu_used * job->duration;
    }

    long long total_available = 0;
    for (const auto &srv : servers) {
        total_available += (long long)srv.gpu_count * (makespan - jobs.front().release_time);
    }

    sol.weighted_waiting = (double)total_weighted_wait;
    sol.makespan = (double)makespan;
    sol.gpu_utilization = (total_available > 0) ? (double)total_gpu_used / total_available : 0;

    double ww_norm = sol.weighted_waiting / 1000000.0;
    double mk_norm = sol.makespan / 100000.0;
    double gpu_norm = 1.0 - sol.gpu_utilization;

    sol.score = ww_norm * 1.0 + mk_norm * 0.5 + gpu_norm * 0.3;
}

GreedyScheduler::Solution GreedyScheduler::adaptiveSimulatedAnnealing(const Solution &initial) {
    Solution current = initial;
    Solution best = initial;

    double temperature = 1000.0;
    double initial_temp = temperature;
    double cooling_rate = (jobs.size() <= 20) ? 0.997 : 0.995;

    int accept_count = 0, reject_count = 0;
    int no_improve_count = 0;
    int max_no_improve = 50 + jobs.size() * 2;
    int stagnation_threshold = 20;
    int reheats = 0;
    const int max_reheats = 2;
    int max_iterations = 2000 + jobs.size() * 100;

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
            if (neighbor.score < best.score) {
                best = neighbor;
                no_improve_count = 0;
            }
        } else {
            reject_count++;
        }

        int total_moves = accept_count + reject_count;
        if (total_moves > 0) {
            double acceptance_rate = (double)accept_count / total_moves;
            if (acceptance_rate < 0.4 - 0.1) {
                temperature *= cooling_rate;
            } else if (acceptance_rate > 0.4 + 0.1) {
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

    for (size_t i = 0; i < min((size_t)6, sol.records.size()); ++i) {
        for (size_t j = i + 1; j < min((size_t)12, sol.records.size()); ++j) {
            Solution n = neighborhoodSwap(sol, i, j);
            if (n.score < best_neighbor.score) best_neighbor = n;
        }
    }

    for (size_t i = 0; i < min((size_t)10, sol.records.size()); ++i) {
        int job_id = sol.records[i].job_id;
        auto it = feasible_machines.find(job_id);
        if (it == feasible_machines.end()) continue;

        for (const auto &e : it->second) {
            int new_srv = machines[e.first].spec.server_id;
            if (new_srv != sol.records[i].server_id) {
                Solution n = neighborhoodMove(sol, i, new_srv);
                if (n.score < best_neighbor.score) best_neighbor = n;
            }
        }
    }

    for (size_t i = 0; i < min((size_t)6, sol.records.size()); ++i) {
        Solution n = neighborhoodReassign(sol, i);
        if (n.score < best_neighbor.score) best_neighbor = n;
    }

    for (int dc : {3, 5, 7}) {
        Solution n = neighborhoodDestroyRepair(sol, dc);
        if (n.score < best_neighbor.score) best_neighbor = n;
    }

    return best_neighbor;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodSwap(const Solution &sol, int idx1, int idx2) {
    Solution n = sol;
    if (idx1 >= (int)sol.records.size() || idx2 >= (int)sol.records.size()) return n;

    swap(n.records[idx1].server_id, n.records[idx2].server_id);
    verifyAndFix(n);
    computeMetrics(n);
    return n;
}

GreedyScheduler::Solution GreedyScheduler::neighborhoodMove(const Solution &sol, int idx, int new_server) {
    Solution n = sol;
    if (idx >= (int)sol.records.size()) return n;
    n.records[idx].server_id = new_server;
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
        return a.is_start && !b.is_start;
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

double GreedyScheduler::placementScore(const Job &job, int machine_index, int gpu_used, long long current_time) const {
    const auto &spec = machines[machine_index].spec;
    int rem_gpu = machines[machine_index].getRemainingGPU();
    int rem_cpu = machines[machine_index].getRemainingCPU();
    int rem_mem = machines[machine_index].getRemainingMemory();

    double gpu_ratio = (double)rem_gpu / spec.gpu_count;
    double cpu_ratio = (double)rem_cpu / spec.cpu_cores;
    double mem_ratio = (double)rem_mem / spec.memory;

    double vram_score = 0.0;
    int vram_per_gpu = spec.gpu_memory / gpu_used;
    if (job.gpu_memory <= vram_per_gpu)
        vram_score = 0.3 * (1.0 - (double)job.gpu_memory / vram_per_gpu);
    else
        vram_score = 0.2 * (1.0 - 1.0 / gpu_used);

    double frag_score = 0.0;
    int post_gpu = rem_gpu - gpu_used;
    int post_cpu = rem_cpu - job.cpu_cores;
    int post_mem = rem_mem - job.memory;

    if (post_gpu > 0) {
        if (post_gpu >= job.min_gpu) frag_score += 0.15 * (double)post_gpu / spec.gpu_count;
        else if (post_gpu >= 1) frag_score -= 0.2;
    }
    if (post_cpu > 0) {
        double r = (double)post_cpu / spec.cpu_cores;
        if (r >= 0.25) frag_score += 0.1;
        else if (r < 0.1) frag_score -= 0.1;
    }
    if (post_mem > 0) {
        double r = (double)post_mem / spec.memory;
        if (r >= 0.2) frag_score += 0.1;
        else if (r < 0.05) frag_score -= 0.1;
    }

    double load = 1.0 - gpu_ratio;
    double load_score = 0.0;
    if (load > 0.8) load_score -= 0.2 * (load - 0.8) / 0.2;
    else if (load < 0.3) load_score += 0.1 * (0.3 - load) / 0.3;

    double scarcity = (spec.gpu_memory <= 40) ? 0.15 : 0.0;
    double urgency = jobUrgency(job, current_time);
    double urgency_bonus = (urgency > 1.0) ? urgency * 0.05 * gpu_ratio : 0.0;

    return pow(gpu_ratio * cpu_ratio * mem_ratio, 1.0/3.0) + vram_score + frag_score + load_score + scarcity + urgency_bonus;
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
            if (machines[i].canEverRun(job, gpu_used))
                entries.push_back({i, gpu_used});
        }
        if (entries.empty())
            throw runtime_error("Job cannot run on any server.");
        feasible_machines[job.job_id] = entries;
    }
}
