#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "output.h"
#include "parser.h"
#include "scheduler.h"

using namespace std;

struct ScheduleMetrics {
    long long weighted_wait = 0;
    double weighted_memory_idle = 0.0;
    long long makespan = 0;
};

ScheduleMetrics evaluateSchedule(
    const vector<ServerSpec> &servers,
    const vector<Job> &jobs,
    const vector<ScheduleRecord> &records
) {
    unordered_map<int, const Job *> job_by_id;
    for (const Job &job : jobs) {
        job_by_id[job.job_id] = &job;
    }

    unordered_map<int, const ServerSpec *> server_by_id;
    for (const ServerSpec &server : servers) {
        server_by_id[server.server_id] = &server;
    }

    long long weighted_wait = 0;
    long long memory_idle = 0;
    long long makespan = 0;

    for (const ScheduleRecord &record : records) {
        const Job &job = *job_by_id.at(record.job_id);
        const ServerSpec &server = *server_by_id.at(record.server_id);
        weighted_wait += static_cast<long long>(job.weight) * (record.start_time - job.release_time);
        memory_idle += static_cast<long long>(record.gpu_used) * server.gpu_memory - job.gpu_memory;
        makespan = max(makespan, record.finish_time);
    }

    // Per amended formula: E_memory = Σ (1/N) * (u_i * VG_{s_i} - v_i)
    const double weighted_memory_idle =
        static_cast<double>(memory_idle) / static_cast<double>(jobs.size());
    return ScheduleMetrics{weighted_wait, weighted_memory_idle, makespan};
}

double relativeComponent(double value, double best_value) {
    if (best_value > 0.0) {
        return value / best_value;
    }
    return value == 0.0 ? 1.0 : 1.0 + value;
}

double scheduleScore(const ScheduleMetrics &metrics, const ScheduleMetrics &best) {
    return relativeComponent(static_cast<double>(metrics.weighted_wait), static_cast<double>(best.weighted_wait)) +
           relativeComponent(metrics.weighted_memory_idle, best.weighted_memory_idle) +
           relativeComponent(static_cast<double>(metrics.makespan), static_cast<double>(best.makespan));
}

bool shouldUseOptimized(const ScheduleMetrics &baseline, const ScheduleMetrics &optimized) {
    ScheduleMetrics best{
        min(baseline.weighted_wait, optimized.weighted_wait),
        min(baseline.weighted_memory_idle, optimized.weighted_memory_idle),
        min(baseline.makespan, optimized.makespan),
    };

    const double baseline_score = scheduleScore(baseline, best);
    const double optimized_score = scheduleScore(optimized, best);
    constexpr double epsilon = 1e-12;
    if (optimized_score + epsilon < baseline_score) {
        return true;
    }
    if (baseline_score + epsilon < optimized_score) {
        return false;
    }
    if (optimized.weighted_wait != baseline.weighted_wait) {
        return optimized.weighted_wait < baseline.weighted_wait;
    }
    if (optimized.makespan != baseline.makespan) {
        return optimized.makespan < baseline.makespan;
    }
    return optimized.weighted_memory_idle < baseline.weighted_memory_idle;
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    pair<vector<ServerSpec>, vector<Job>> result = readInstance(cin);
    vector<ServerSpec> servers = result.first;
    vector<Job> jobs = result.second;
    if (jobs.empty()) {
        return 0;
    }

    GreedyScheduler baseline_scheduler(servers, jobs, SchedulingStrategy::Baseline);
    vector<ScheduleRecord> baseline_records = baseline_scheduler.schedule();
    ScheduleMetrics baseline_metrics = evaluateSchedule(servers, jobs, baseline_records);

    GreedyScheduler optimized_scheduler(servers, jobs, SchedulingStrategy::Optimized);
    vector<ScheduleRecord> optimized_records = optimized_scheduler.schedule();
    ScheduleMetrics optimized_metrics = evaluateSchedule(servers, jobs, optimized_records);

    const vector<ScheduleRecord> &records =
        shouldUseOptimized(baseline_metrics, optimized_metrics) ? optimized_records : baseline_records;
    writeScheduleRecords(cout, records);

    return 0;
}
