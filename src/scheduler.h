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
        std::queue<Job> &pending_jobs,
        long long current_time,
        std::unordered_map<int, ScheduleRecord> &records,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    StartResult tryStartOneJob(const Job &job, long long current_time);
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
};

#endif

