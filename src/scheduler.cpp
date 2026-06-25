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

GreedyScheduler::GreedyScheduler(
    vector<ServerSpec> input_servers,
    vector<Job> input_jobs,
    SchedulingStrategy input_strategy
)
    : servers(move(input_servers)), jobs(move(input_jobs)), strategy(input_strategy) {
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
    if (strategy == SchedulingStrategy::Baseline) {
        tryStartPendingJobsBaseline(pending_jobs, current_time, records, running_heap);
        return;
    }
    tryStartPendingJobsOptimized(pending_jobs, current_time, records, running_heap);
}

void GreedyScheduler::tryStartPendingJobsBaseline(
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
        StartResult started = tryStartOneJob(job, current_time);
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

void GreedyScheduler::tryStartPendingJobsOptimized(
    priority_queue<ReadyJob, vector<ReadyJob>, ReadyJobCompare> &pending_jobs,
    long long current_time,
    unordered_map<int, ScheduleRecord> &records,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    vector<ReadyJob> ready_jobs;
    ready_jobs.reserve(pending_jobs.size());
    while (!pending_jobs.empty()) {
        ready_jobs.push_back(pending_jobs.top());
        pending_jobs.pop();
    }

    vector<double> reservation_scores = buildReservationScores(ready_jobs);

    auto commit = [&](const DispatchChoice &choice) {
        const ReadyJob ready_job = ready_jobs[choice.ready_position];
        const Job &job = jobs[ready_job.job_index];
        const StartResult started = startOnMachine(job, current_time, choice.placement);
        if (!started.has_value) {
            throw logic_error("Selected dispatch choice could not be started.");
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

        const double contribution = reservationContribution(ready_job);
        for (const auto &entry : feasible_machines.at(job.job_id)) {
            reservation_scores[entry.first] = max(0.0, reservation_scores[entry.first] - contribution);
        }
        ready_jobs.erase(ready_jobs.begin() + static_cast<ptrdiff_t>(choice.ready_position));
    };

    vector<size_t> candidate_positions = buildCandidatePositions(ready_jobs, current_time);
    const size_t dispatch_limit = static_cast<size_t>(dispatchAttemptLimit(static_cast<int>(ready_jobs.size())));
    size_t dispatch_attempts = 0;

    while (!candidate_positions.empty() && dispatch_attempts < dispatch_limit) {
        const DispatchChoice choice =
            chooseDispatch(ready_jobs, candidate_positions, reservation_scores, current_time);
        ++dispatch_attempts;
        if (!choice.has_value) {
            break;
        }

        const size_t selected_position = choice.ready_position;
        commit(choice);

        candidate_positions.erase(
            remove(candidate_positions.begin(), candidate_positions.end(), selected_position),
            candidate_positions.end()
        );
        for (size_t &position : candidate_positions) {
            if (position > selected_position) {
                --position;
            }
        }
    }

    size_t fallback_position = 0;
    while (fallback_position < ready_jobs.size() && dispatch_attempts < dispatch_limit) {
        const ReadyJob &ready_job = ready_jobs[fallback_position];
        const PlacementChoice placement = choosePlacement(jobs[ready_job.job_index], reservation_scores);
        ++dispatch_attempts;
        if (!placement.has_value) {
            ++fallback_position;
            continue;
        }

        commit(DispatchChoice{true, fallback_position, placement, 0.0});
    }

    for (const ReadyJob &ready_job : ready_jobs) {
        pending_jobs.push(ready_job);
    }
}

vector<double> GreedyScheduler::buildReservationScores(const vector<ReadyJob> &ready_jobs) const {
    vector<double> scores(machines.size(), 0.0);
    for (const ReadyJob &ready_job : ready_jobs) {
        const double contribution = reservationContribution(ready_job);
        for (const auto &entry : feasible_machines.at(ready_job.job_id)) {
            scores[entry.first] += contribution;
        }
    }
    return scores;
}

vector<size_t> GreedyScheduler::buildCandidatePositions(
    const vector<ReadyJob> &ready_jobs,
    long long current_time
) const {
    constexpr size_t candidates_per_view = 8;
    struct RankedPosition {
        double score;
        size_t position;
    };

    vector<RankedPosition> top_priority;
    vector<RankedPosition> top_age;
    vector<RankedPosition> top_scarcity;
    vector<RankedPosition> top_heavy_work;
    top_priority.reserve(candidates_per_view + 1);
    top_age.reserve(candidates_per_view + 1);
    top_scarcity.reserve(candidates_per_view + 1);
    top_heavy_work.reserve(candidates_per_view + 1);

    auto better = [&](const RankedPosition &left, const RankedPosition &right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        return ready_jobs[left.position].job_id < ready_jobs[right.position].job_id;
    };

    auto add_ranked = [&](vector<RankedPosition> &top, RankedPosition candidate) {
        top.push_back(candidate);
        for (size_t index = top.size() - 1; index > 0 && better(top[index], top[index - 1]); --index) {
            swap(top[index], top[index - 1]);
        }
        if (top.size() > candidates_per_view) {
            top.pop_back();
        }
    };

    for (size_t position = 0; position < ready_jobs.size(); ++position) {
        const ReadyJob &ready_job = ready_jobs[position];
        add_ranked(top_priority, RankedPosition{ready_job.priority, position});
        add_ranked(
            top_age,
            RankedPosition{static_cast<double>(current_time - ready_job.release_time), position}
        );
        add_ranked(top_scarcity, RankedPosition{1.0 / ready_job.feasible_machine_count, position});
        add_ranked(top_heavy_work, RankedPosition{static_cast<double>(ready_job.heavy_work), position});
    }

    vector<size_t> selected;
    vector<bool> selected_mask(ready_jobs.size(), false);

    auto select_top = [&](const vector<RankedPosition> &top) {
        for (const RankedPosition &ranked : top) {
            const size_t position = ranked.position;
            if (!selected_mask[position]) {
                selected_mask[position] = true;
                selected.push_back(position);
            }
        }
    };

    select_top(top_priority);
    select_top(top_age);
    select_top(top_scarcity);
    select_top(top_heavy_work);

    return selected;
}

GreedyScheduler::PlacementChoice GreedyScheduler::choosePlacement(
    const Job &job,
    const vector<double> &reservation_scores
) const {
    const vector<pair<int, int>> &entries = feasible_machines.at(job.job_id);
    const double priority = static_cast<double>(job.weight) / job.duration;
    const double own_contribution =
        (1.0 + priority) / (static_cast<double>(entries.size()) * entries.size());

    vector<bool> candidate_machine(machines.size(), false);
    for (const auto &entry : entries) {
        candidate_machine[entry.first] = true;
    }

    double max_reservation = 0.0;
    for (size_t index = 0; index < reservation_scores.size(); ++index) {
        const double adjusted = max(
            0.0,
            reservation_scores[index] - (candidate_machine[index] ? own_contribution : 0.0)
        );
        max_reservation = max(max_reservation, adjusted);
    }

    PlacementChoice best;
    for (const auto &entry : entries) {
        const int machine_index = entry.first;
        const int gpu_used = entry.second;
        const MachineState &machine = machines[machine_index];
        if (!machine.canStart(job, gpu_used)) {
            continue;
        }

        const double adjusted_reservation = max(0.0, reservation_scores[machine_index] - own_contribution);
        const double reservation = max_reservation > 0.0 ? adjusted_reservation / max_reservation : 0.0;
        const double compactness = machine.placementSlack(job, gpu_used) / 3.0;
        const double imbalance = machine.postPlacementImbalance(job, gpu_used);
        const double gpu_pressure = static_cast<double>(gpu_used) / machine.remainingGpu();
        const double cost = 0.35 * compactness + 0.25 * imbalance +
                            0.30 * reservation + 0.10 * gpu_pressure;

        if (!best.has_value || cost < best.cost ||
            (cost == best.cost && machine.spec.server_id < machines[best.machine_index].spec.server_id)) {
            best = PlacementChoice{true, machine_index, gpu_used, cost};
        }
    }
    return best;
}

GreedyScheduler::DispatchChoice GreedyScheduler::chooseDispatch(
    const vector<ReadyJob> &ready_jobs,
    const vector<size_t> &positions,
    const vector<double> &reservation_scores,
    long long current_time
) const {
    if (positions.empty()) {
        return {};
    }

    double max_priority = 0.0;
    double max_age = 0.0;
    double max_scarcity = 0.0;
    double max_heavy_work = 0.0;
    for (size_t position : positions) {
        const ReadyJob &ready_job = ready_jobs[position];
        max_priority = max(max_priority, ready_job.priority);
        max_age = max(max_age, static_cast<double>(current_time - ready_job.release_time));
        max_scarcity = max(max_scarcity, 1.0 / ready_job.feasible_machine_count);
        max_heavy_work = max(max_heavy_work, static_cast<double>(ready_job.heavy_work));
    }

    DispatchChoice best;
    for (size_t position : positions) {
        const ReadyJob &ready_job = ready_jobs[position];
        const Job &job = jobs[ready_job.job_index];
        const PlacementChoice placement = choosePlacement(job, reservation_scores);
        if (!placement.has_value) {
            continue;
        }

        const double wspt = ready_job.priority / max(max_priority, 1.0);
        const double age = static_cast<double>(current_time - ready_job.release_time) / max(max_age, 1.0);
        const double scarcity = (1.0 / ready_job.feasible_machine_count) / max(max_scarcity, 1.0);
        const double heavy_work = static_cast<double>(ready_job.heavy_work) / max(max_heavy_work, 1.0);
        const double utility = 0.55 * wspt + 0.20 * age + 0.15 * scarcity + 0.10 * heavy_work;

        if (!best.has_value || utility > best.utility ||
            (utility == best.utility && ready_job.job_id < ready_jobs[best.ready_position].job_id) ||
            (utility == best.utility && ready_job.job_id == ready_jobs[best.ready_position].job_id &&
             machines[placement.machine_index].spec.server_id < machines[best.placement.machine_index].spec.server_id)) {
            best = DispatchChoice{true, position, placement, utility};
        }
    }
    return best;
}

GreedyScheduler::StartResult GreedyScheduler::startOnMachine(
    const Job &job,
    long long current_time,
    const PlacementChoice &placement
) {
    if (!placement.has_value || !machines[placement.machine_index].canStart(job, placement.gpu_used)) {
        return {};
    }

    pair<ScheduleRecord, RunningJob> result =
        machines[placement.machine_index].startJob(job, current_time, placement.gpu_used);
    return StartResult{true, result.first, result.second};
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
        const double gpu_memory_waste =
            static_cast<double>(gpu_used * machine.spec.gpu_memory - job.gpu_memory) /
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

    if (best_machine_index == -1) {
        return {};
    }

    pair<ScheduleRecord, RunningJob> result =
        machines[best_machine_index].startJob(job, current_time, best_gpu_used);
    return StartResult{true, result.first, result.second};
}

double GreedyScheduler::reservationContribution(const ReadyJob &ready_job) const {
    const double feasible_count = ready_job.feasible_machine_count;
    return (1.0 + ready_job.priority / 1000000.0) / (feasible_count * feasible_count);
}

ReadyJob GreedyScheduler::makeReadyJob(int job_index) const {
    const Job &job = jobs[job_index];
    const int feasible_machine_count = static_cast<int>(feasible_machines.at(job.job_id).size());
    const double priority = 1000000.0 * job.weight / job.duration;
    const long long heavy_work = static_cast<long long>(job.duration) * job.min_gpu;
    return ReadyJob{
        job_index,
        priority,
        feasible_machine_count,
        job.duration,
        job.job_id,
        job.release_time,
        heavy_work,
    };
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
