#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H

#include <cstddef>
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
    long long release_time;
    long long heavy_work;
};

struct ReadyJobCompare {
    bool operator()(const ReadyJob &left, const ReadyJob &right) const;
};

enum class SchedulingStrategy {
    Baseline,
    Optimized,
};

class GreedyScheduler {
public:
    GreedyScheduler(
        std::vector<ServerSpec> input_servers,
        std::vector<Job> input_jobs,
        SchedulingStrategy input_strategy = SchedulingStrategy::Optimized
    );

    std::vector<ScheduleRecord> schedule();

private:
    struct StartResult {
        bool has_value = false;
        ScheduleRecord record{};
        RunningJob running_job{};
    };

    struct PlacementChoice {
        bool has_value = false;
        int machine_index = -1;
        int gpu_used = 0;
        double cost = 0.0;
    };

    struct DispatchChoice {
        bool has_value = false;
        std::size_t ready_position = 0;
        PlacementChoice placement{};
        double utility = 0.0;
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
    void tryStartPendingJobsBaseline(
        std::priority_queue<ReadyJob, std::vector<ReadyJob>, ReadyJobCompare> &pending_jobs,
        long long current_time,
        std::unordered_map<int, ScheduleRecord> &records,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    void tryStartPendingJobsOptimized(
        std::priority_queue<ReadyJob, std::vector<ReadyJob>, ReadyJobCompare> &pending_jobs,
        long long current_time,
        std::unordered_map<int, ScheduleRecord> &records,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    std::vector<double> buildReservationScores(const std::vector<ReadyJob> &ready_jobs) const;
    std::vector<std::size_t> buildCandidatePositions(
        const std::vector<ReadyJob> &ready_jobs,
        long long current_time
    ) const;
    PlacementChoice choosePlacement(const Job &job, const std::vector<double> &reservation_scores) const;
    DispatchChoice chooseDispatch(
        const std::vector<ReadyJob> &ready_jobs,
        const std::vector<std::size_t> &positions,
        const std::vector<double> &reservation_scores,
        long long current_time
    ) const;
    StartResult startOnMachine(
        const Job &job,
        long long current_time,
        const PlacementChoice &placement
    );
    StartResult tryStartOneJob(const Job &job, long long current_time);
    double reservationContribution(const ReadyJob &ready_job) const;
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
    SchedulingStrategy strategy;
};

#endif
