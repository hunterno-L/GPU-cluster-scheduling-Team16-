#include "scheduler.h"

#include <algorithm>
#include <stdexcept>

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

bool ReadyJobCompare::operator()(const ReadyJob &left, const ReadyJob &right) const {
    if (left.priority != right.priority) return left.priority < right.priority;
    if (left.feasible_machine_count != right.feasible_machine_count) {
        return left.feasible_machine_count > right.feasible_machine_count;
    }
    if (left.duration != right.duration) return left.duration < right.duration;
    return left.job_id > right.job_id;
}

GreedyScheduler::GreedyScheduler(vector<ServerSpec> input_servers, vector<Job> input_jobs)
    : servers(move(input_servers)), jobs(move(input_jobs)) {
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
    if (jobs.empty()) {
        return {};
    }

    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    priority_queue<ReadyJob, vector<ReadyJob>, ReadyJobCompare> pending_jobs;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;

    while (static_cast<int>(records.size()) < static_cast<int>(jobs.size())) {
        releaseFinishedJobs(current_time, running_heap);

        while (next_job_index < static_cast<int>(jobs.size()) &&
               jobs[next_job_index].release_time <= current_time) {
            pending_jobs.push(makeReadyJob(next_job_index));
            ++next_job_index;
        }

        tryStartPendingJobs(pending_jobs, current_time, records, running_heap);

        if (static_cast<int>(records.size()) == static_cast<int>(jobs.size())) {
            break;
        }

        current_time = nextEventTime(current_time, next_job_index, running_heap);
    }

    vector<ScheduleRecord> ordered_records;
    ordered_records.reserve(records.size());
    for (int job_id = 1; job_id <= static_cast<int>(jobs.size()); ++job_id) {
        ordered_records.push_back(records.at(job_id));
    }
    return ordered_records;
}

void GreedyScheduler::buildFeasibleMachines() {
    machine_flexibility.assign(machines.size(), 0);

    for (const auto &job : jobs) {
        vector<pair<int, int>> entries;

        for (int index = 0; index < static_cast<int>(machines.size()); ++index) {
            int gpu_used = machines[index].requiredGpuCount(job);
            if (machines[index].canEverRun(job, gpu_used)) {
                entries.push_back({index, gpu_used});
            }
        }

        if (entries.empty()) {
            throw runtime_error("A job cannot run on any server.");
        }

        feasible_machines[job.job_id] = entries;
        for (const auto &entry : entries) {
            ++machine_flexibility[entry.first];
        }
    }
}

void GreedyScheduler::releaseFinishedJobs(
    long long current_time,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
        FinishEvent event = running_heap.top();
        running_heap.pop();

        int machine_index = machine_index_by_id.at(event.server_id);
        machines[machine_index].releaseJob(event.running_job);
    }
}

void GreedyScheduler::tryStartPendingJobs(
    priority_queue<ReadyJob, vector<ReadyJob>, ReadyJobCompare> &pending_jobs,
    long long current_time,
    unordered_map<int, ScheduleRecord> &records,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    const int attempt_limit = dispatchAttemptLimit(static_cast<int>(pending_jobs.size()));
    vector<ReadyJob> deferred;
    deferred.reserve(min(attempt_limit, static_cast<int>(pending_jobs.size())));

    int attempts = 0;
    while (!pending_jobs.empty() && attempts < attempt_limit) {
        ReadyJob ready_job = pending_jobs.top();
        pending_jobs.pop();
        ++attempts;

        const Job &job = jobs[ready_job.job_index];
        auto started = tryStartOneJob(job, current_time);
        if (!started.has_value) {
            deferred.push_back(ready_job);
            continue;
        }

        records[job.job_id] = started.record;
        running_heap.push(
            FinishEvent{
                started.running_job.finish_time,
                started.running_job.server_id,
                started.running_job.job_id,
                started.running_job,
            }
        );
    }

    for (const ReadyJob &ready_job : deferred) {
        pending_jobs.push(ready_job);
    }
}

GreedyScheduler::StartResult GreedyScheduler::tryStartOneJob(const Job &job, long long current_time) {
    const vector<pair<int, int>> &entries = feasible_machines.at(job.job_id);
    int best_machine_index = -1;
    int best_gpu_used = 0;
    double best_cost = 0.0;

    for (const auto &entry : entries) {
        const int machine_index = entry.first;
        const int gpu_used = entry.second;
        const MachineState &machine = machines[machine_index];
        if (!machine.canStart(job, gpu_used)) {
            continue;
        }

        const double flexibility = static_cast<double>(machine_flexibility[machine_index]) / jobs.size();
        const double gpu_memory_waste = static_cast<double>(gpu_used * machine.spec.gpu_memory - job.gpu_memory) /
                                        (gpu_used * machine.spec.gpu_memory);
        const double cost = 2.0 * flexibility + 0.60 * machine.placementSlack(job, gpu_used) +
                            0.25 * gpu_memory_waste;

        if (best_machine_index == -1 || cost < best_cost ||
            (cost == best_cost && machine.spec.server_id < machines[best_machine_index].spec.server_id)) {
            best_machine_index = machine_index;
            best_gpu_used = gpu_used;
            best_cost = cost;
        }
    }

    if (best_machine_index != -1) {
        pair<ScheduleRecord, RunningJob> result =
            machines[best_machine_index].startJob(job, current_time, best_gpu_used);
        return StartResult{true, result.first, result.second};
    }

    return StartResult{};
}

ReadyJob GreedyScheduler::makeReadyJob(int job_index) const {
    const Job &job = jobs[job_index];
    const int feasible_machine_count = static_cast<int>(feasible_machines.at(job.job_id).size());
    const double priority = 1000000.0 * job.weight / job.duration;
    return ReadyJob{job_index, priority, feasible_machine_count, job.duration, job.job_id};
}

int GreedyScheduler::dispatchAttemptLimit(int ready_job_count) const {
    if (ready_job_count == 0) {
        return 0;
    }

    const int capacity_based_limit = max(256, static_cast<int>(machines.size()) * 8);
    return min(ready_job_count, min(2048, capacity_based_limit));
}

long long GreedyScheduler::nextEventTime(
    long long current_time,
    int next_job_index,
    const priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) const {
    vector<long long> candidates;

    if (next_job_index < static_cast<int>(jobs.size())) {
        candidates.push_back(jobs[next_job_index].release_time);
    }
    if (!running_heap.empty()) {
        candidates.push_back(running_heap.top().finish_time);
    }

    long long next_time = -1;
    for (long long candidate : candidates) {
        if (candidate <= current_time) {
            continue;
        }
        if (next_time == -1 || candidate < next_time) {
            next_time = candidate;
        }
    }

    if (next_time == -1) {
        throw runtime_error("No future event exists.");
    }

    return next_time;
}
