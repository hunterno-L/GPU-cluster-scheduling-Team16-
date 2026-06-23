#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H

#include <queue>
#include <unordered_map>
#include <vector>

#include "machine_state.h"
#include "models.h"

struct FinishEvent {
    long long finish_time;
    int server_id;
    int job_id;
    RunningJob running_job;

    bool operator>(const FinishEvent &other) const;
};

struct ReadyJob {
    int job_index;
    double priority;
    int feasible_machine_count;
    int duration;
    int job_id;
};

struct ReadyJobCompare {
    bool operator()(const ReadyJob &left, const ReadyJob &right) const;
};

class GreedyScheduler {
public:
    GreedyScheduler(std::vector<ServerSpec> input_servers, std::vector<Job> input_jobs);

    std::vector<ScheduleRecord> schedule();

private:
    struct StartResult {
        bool has_value = false;
        ScheduleRecord record{};
        RunningJob running_job{};
    };

    void buildFeasibleMachines();
    void releaseFinishedJobs(
        long long current_time,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    void tryStartPendingJobs(
        std::priority_queue<ReadyJob, std::vector<ReadyJob>, ReadyJobCompare> &pending_jobs,
        long long current_time,
        std::unordered_map<int, ScheduleRecord> &records,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    StartResult tryStartOneJob(const Job &job, long long current_time);
    ReadyJob makeReadyJob(int job_index) const;
    int dispatchAttemptLimit(int ready_job_count) const;
    long long nextEventTime(
        long long current_time,
        int next_job_index,
        const std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    ) const;

    std::vector<ServerSpec> servers;
    std::vector<Job> jobs;
    std::vector<MachineState> machines;
    std::unordered_map<int, int> machine_index_by_id;
    std::unordered_map<int, std::vector<std::pair<int, int>>> feasible_machines;
    std::vector<int> machine_flexibility;
};

#endif
