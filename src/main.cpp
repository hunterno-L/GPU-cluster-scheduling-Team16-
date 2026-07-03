// lab4.0: Multi-strategy meta-scheduler
// Generates multiple candidate schedules per instance with different parameter combos,
// then selects the best using internal min-max normalization (course formula logic).
// Weights: 1.25 * norm(E_wait) + 1.0 * norm(E_memory) + 1.0 * norm(E_finish)

#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

#include "output.h"
#include "parser.h"
#include "scheduler.h"

using namespace std;

// ── Strategy parameter sets ──
// Design goal: diverse enough to cover different trade-off regimes
static const SchedulerParams STRATEGIES[] = {
    // 1. Best single-param from lab3.0 tuning
    {0.4, 1.0, 0.0, 1.00, 1.50},  // AG0.4+CP1.5 (lab3.0 best)
    // 2-4. Aging variants
    {0.0, 1.0, 0.0, 1.00, 2.00},  // no aging, fusion default
    {0.2, 1.0, 0.0, 1.00, 2.00},  // mild aging
    {0.3, 1.0, 0.0, 1.00, 2.00},  // moderate aging
    // 5-7. Cost premium variants (mem-trade-off aggressiveness)
    {0.4, 1.0, 0.0, 1.00, 1.10},  // strict cost premium
    {0.4, 1.0, 0.0, 1.00, 2.00},  // medium cost premium
    {0.4, 1.0, 0.0, 1.00, 3.00},  // loose cost premium
    // 8-9. MEM ratio variants
    {0.4, 1.0, 0.0, 0.70, 1.50},  // strict memory trade
    {0.4, 1.0, 0.0, 1.50, 1.50},  // loose memory trade
    // 10. Baseline WSPT (no aging, no mem-trade-off)
    {0.0, 1.0, 0.0, 0.00, 2.00},  // pure WSPT + waste cost only
    // 11-12. Duration exponent (short job preference)
    {0.0, 1.1, 0.0, 1.00, 2.00},
    {0.3, 1.1, 0.0, 1.00, 1.50},
};

static const int NUM_STRATEGIES = sizeof(STRATEGIES) / sizeof(STRATEGIES[0]);

// ── Metric computation ──
struct Metric {
    double e_wait = 0;
    double e_memory = 0;
    double e_finish = 0;
};

static bool validateSchedule(const vector<ServerSpec>& servers, const vector<Job>& jobs,
                              const vector<ScheduleRecord>& records, Metric& out) {
    int N = (int)jobs.size();
    if ((int)records.size() != N) return false;

    unordered_map<int,int> jobIdx;
    for (int i = 0; i < N; ++i) jobIdx[jobs[i].job_id] = i;

    // Timeline check
    vector<tuple<long long, int, int, int, int>> events;
    for (const auto& rec : records) {
        auto it = jobIdx.find(rec.job_id);
        if (it == jobIdx.end()) return false;
        const auto& j = jobs[it->second];
        if (rec.start_time < j.release_time) return false;
        if (rec.finish_time != rec.start_time + j.duration) return false;
        if (rec.gpu_used < j.min_gpu) return false;
        if (rec.gpu_used > servers[rec.server_id - 1].gpu_count) return false;
        if (j.gpu_memory > rec.gpu_used * servers[rec.server_id - 1].gpu_memory) return false;
        if (j.cpu_cores > servers[rec.server_id - 1].cpu_cores) return false;
        if (j.memory > servers[rec.server_id - 1].memory) return false;
        events.emplace_back(rec.start_time, 0, rec.server_id - 1, rec.gpu_used, it->second);
        events.emplace_back(rec.finish_time, 1, rec.server_id - 1, rec.gpu_used, it->second);
    }
    sort(events.begin(), events.end());

    // Resource concurrency check
    vector<int> gpu_use(servers.size(), 0), cpu_use(servers.size(), 0), mem_use(servers.size(), 0);
    long long prev_t = -1;
    for (size_t ei = 0; ei < events.size(); ) {
        long long t = get<0>(events[ei]);
        if (prev_t >= 0) {
            for (size_t si = 0; si < servers.size(); ++si) {
                if (gpu_use[si] > servers[si].gpu_count) return false;
                if (cpu_use[si] > servers[si].cpu_cores) return false;
                if (mem_use[si] > servers[si].memory) return false;
            }
        }
        prev_t = t;
        while (ei < events.size() && get<0>(events[ei]) == t) {
            int typ = get<1>(events[ei]), sidx = get<2>(events[ei]), g = get<3>(events[ei]), ji = get<4>(events[ei]);
            const auto& j = jobs[ji];
            if (typ == 0) {
                gpu_use[sidx] += g;
                cpu_use[sidx] += j.cpu_cores;
                mem_use[sidx] += j.memory;
            } else {
                gpu_use[sidx] -= g;
                cpu_use[sidx] -= j.cpu_cores;
                mem_use[sidx] -= j.memory;
            }
            ++ei;
        }
    }

    // Compute metrics
    long long H = 0;
    double e_wait = 0;
    for (const auto& rec : records) {
        auto it = jobIdx.find(rec.job_id);
        const auto& j = jobs[it->second];
        e_wait += (double)j.weight * (rec.start_time - j.release_time);
        H = max(H, rec.finish_time);
    }

    long long total_dur = 0;
    for (const auto& j : jobs) total_dur += j.duration;
    double e_memory = 0;
    for (const auto& rec : records) {
        auto it = jobIdx.find(rec.job_id);
        const auto& j = jobs[it->second];
        double p_i = (double)j.duration / max(1LL, total_dur);
        e_memory += p_i * (rec.gpu_used * servers[rec.server_id - 1].gpu_memory - j.gpu_memory);
    }

    out.e_wait = e_wait;
    out.e_memory = e_memory;
    out.e_finish = (double)H;
    return true;
}

// ── Main ──
int main(int argc, char* argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    vector<ServerSpec> servers;
    vector<Job> jobs;

    if (argc >= 2) {
        ifstream fin(argv[1]);
        if (!fin) { cerr << "Cannot open: " << argv[1] << endl; return 1; }
        auto [s, j] = readInstance(fin);
        servers = s; jobs = j;
    } else {
        auto [s, j] = readInstance(cin);
        servers = s; jobs = j;
    }
    if (jobs.empty()) return 0;

    // Phase 1: generate candidate schedules
    struct Candidate {
        vector<ScheduleRecord> records;
        Metric m;
        bool valid = false;
    };
    vector<Candidate> candidates(NUM_STRATEGIES);

    for (int si = 0; si < NUM_STRATEGIES; ++si) {
        SchedulerEngine engine(servers, jobs, STRATEGIES[si], nullptr, "best-fit", true);
        auto rec_map = engine.schedule();

        vector<ScheduleRecord> ordered;
        ordered.reserve(rec_map.size());
        for (const auto& kv : rec_map) ordered.push_back(kv.second);

        Metric met;
        if (validateSchedule(servers, jobs, ordered, met)) {
            candidates[si].records = move(ordered);
            candidates[si].m = met;
            candidates[si].valid = true;
        }
    }

    // Phase 2: internal min-max normalization and scoring
    const double W_WAIT = 1.25, W_MEM = 1.0, W_FIN = 1.0;

    double min_wait = 1e30, max_wait = -1e30;
    double min_mem = 1e30, max_mem = -1e30;
    double min_fin = 1e30, max_fin = -1e30;

    for (const auto& cand : candidates) {
        if (!cand.valid) continue;
        min_wait = min(min_wait, cand.m.e_wait);
        max_wait = max(max_wait, cand.m.e_wait);
        min_mem = min(min_mem, cand.m.e_memory);
        max_mem = max(max_mem, cand.m.e_memory);
        min_fin = min(min_fin, cand.m.e_finish);
        max_fin = max(max_fin, cand.m.e_finish);
    }

    int best_idx = -1;
    double best_score = 1e30;
    for (int i = 0; i < NUM_STRATEGIES; ++i) {
        if (!candidates[i].valid) continue;

        double nw = (max_wait > min_wait + 1e-12) ? (candidates[i].m.e_wait - min_wait) / (max_wait - min_wait) : 0;
        double nm = (max_mem > min_mem + 1e-12) ? (candidates[i].m.e_memory - min_mem) / (max_mem - min_mem) : 0;
        double nf = (max_fin > min_fin + 1e-12) ? (candidates[i].m.e_finish - min_fin) / (max_fin - min_fin) : 0;

        double score = W_WAIT * nw + W_MEM * nm + W_FIN * nf;
        if (score < best_score - 1e-12) {
            best_score = score;
            best_idx = i;
        }
    }

    if (best_idx < 0) {
        for (int i = 0; i < NUM_STRATEGIES; ++i) {
            if (candidates[i].valid) { best_idx = i; break; }
        }
    }

    if (best_idx >= 0) {
        writeScheduleRecords(cout, candidates[best_idx].records);
    }

    return 0;
}
