#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H

#include <queue>
#include <unordered_map>
#include <vector>
#include <cstdlib>
#include <ctime>

#include "machine_state.h"
#include "models.h"

struct FinishEvent {
    long long finish_time;
    int server_id;
    int job_id;
    RunningJob running_job;
    bool operator>(const FinishEvent &other) const;
};

struct UrgencyComparator {
    bool operator()(const Job &a, const Job &b) const {
        double wspt_a = static_cast<double>(a.weight) / a.duration;
        double wspt_b = static_cast<double>(b.weight) / b.duration;
        if (wspt_a != wspt_b) return wspt_a < wspt_b;
        return a.job_id > b.job_id;
    }
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

    struct Solution {
        std::vector<ScheduleRecord> records;
        double score;
        double weighted_waiting;
        double makespan;
        double gpu_utilization;
        Solution() : score(0.0), weighted_waiting(0.0), makespan(0.0), gpu_utilization(0.0) {}
    };

    void buildFeasibleMachines();
    Solution generateMultiStrategySolution(int num_strategies = 5);
    Solution generateGreedySolutionWithStrategy(int strategy_seed);
    void computeMetrics(Solution &sol) const;
    Solution adaptiveSimulatedAnnealing(const Solution &initial_solution);
    Solution neighborhoodSwap(const Solution &sol, int idx1, int idx2);
    Solution neighborhoodMove(const Solution &sol, int idx, int new_server);
    Solution neighborhoodReassign(const Solution &sol, int idx);
    Solution neighborhoodDestroyRepair(const Solution &sol, int destroy_count);
    Solution getBestNeighbor(const Solution &sol);
    void verifyAndFix(Solution &sol);
    double placementScore(const Job &job, int machine_index, int gpu_used, long long current_time) const;
    double jobUrgency(const Job &job, long long current_time) const;

    std::vector<ServerSpec> servers;
    std::vector<Job> jobs;
    std::vector<MachineState> machines;
    std::unordered_map<int, int> machine_index_by_id;
    std::unordered_map<int, std::vector<std::pair<int, int>>> feasible_machines;
};

#endif
