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
    GreedyScheduler(std::vector<ServerSpec> input_servers, std::vector<Job> input_jobs,
                    bool input_backfill = false);
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
        double vram_idle;
        double gpu_utilization;
        Solution() : score(0.0), weighted_waiting(0.0), makespan(0.0), vram_idle(0.0), gpu_utilization(0.0) {}
    };

    struct PlacementPick {
        int machine_index = -1;
        int gpu_used = 0;
    };

    struct InstanceProfile {
        double vram_pressure = 0.0;
        double scarce_80_ratio = 0.0;
        double burst_t0_ratio = 0.0;
        double avg_min_gpu = 0.0;
        double hetero_ratio = 1.0;
        double avg_feasible = 0.0;
        double gpu_demand_ratio = 0.0;
        double cpu_demand_ratio = 0.0;
        double mem_demand_ratio = 0.0;
        double avg_duration = 0.0;
        double long_job_ratio = 0.0;
    };

    void computeInstanceProfile();
    int jobFeasibleCount(int job_id) const;
    bool isNarrowCluster() const;
    double instanceDifficulty() const;
    int adaptiveStrategyCount() const;
    int adaptivePendingCap(int queue_size) const;
    int resolvePlacementMode(int strategy_seed) const;
    bool shouldUseRuntimePareto() const;
    bool shouldLightRefine() const;
    bool shouldFastRefine() const;
    bool shouldLongJobRefine() const;

    PlacementPick chooseBestPlacement(const Job &job,
                                    const std::vector<MachineState> &sim_machines,
                                    long long current_time,
                                    const std::vector<double> &reservation_scores,
                                    int placement_mode,
                                    int sort_strategy) const;

    void buildFeasibleMachines();
    std::vector<std::pair<int, int>> paretoPlacementOptions(const Job &job) const;
    Solution generateMultiStrategySolution(int num_strategies = 5, Solution *runner_up = nullptr,
                                           Solution *third_place = nullptr);
    Solution generateGreedySolutionWithStrategy(int strategy_seed);
    Solution generateQueueCandidate(double duration_exp, double age_w, double flex_w,
                                    double slack_w, double waste_w, double mem_trade_ratio,
                                    double cost_premium_ratio, bool use_backfill) const;
    Solution refinePlacement(const Solution &initial_solution, int max_passes_override = -1);
    Solution fastRefinePlacement(const Solution &initial_solution);
    Solution placementALNS(const Solution &initial_solution);
    void computeMetrics(Solution &sol) const;
    void computeOfficialMetrics(Solution &sol) const;
    bool isBetterOfficialSolution(const Solution &candidate, const Solution &current) const;
    std::vector<double> buildReservationScores(const std::vector<Job> &pending_jobs) const;
    double reservationContribution(const Job &job) const;
    Solution adaptiveSimulatedAnnealing(const Solution &initial_solution);
    Solution neighborhoodSwap(const Solution &sol, int idx1, int idx2);
    Solution neighborhoodMove(const Solution &sol, int idx, int new_server);
    Solution neighborhoodReassign(const Solution &sol, int idx);
    Solution getBestNeighbor(const Solution &sol);
    void replaySchedule(Solution &sol);
    bool isBetterSolution(const Solution &candidate, const Solution &current) const;
    bool isScheduleValid(const Solution &sol) const;
    double placementScore(const Job &job, int machine_index, int gpu_used,
                          const std::vector<MachineState> &sim_machines,
                          long long current_time,
                          const std::vector<double> &reservation_scores) const;
    double placementCost(const Job &job, int machine_index, int gpu_used,
                         const std::vector<MachineState> &sim_machines,
                         long long current_time,
                         const std::vector<double> &reservation_scores) const;

    std::vector<ServerSpec> servers;
    std::vector<Job> jobs;
    std::unordered_map<int, const Job *> job_by_id;
    std::vector<MachineState> machines;
    std::unordered_map<int, int> machine_index_by_id;
    std::unordered_map<int, std::vector<std::pair<int, int>>> feasible_machines;
    InstanceProfile profile;
    bool backfill_enabled;

    // Cached global load balance data
    mutable long long cached_global_load_time = -1;
    mutable double cached_avg_gpu_load = 0.0;
    mutable std::vector<double> cached_machine_loads;
    void updateGlobalLoadCache(long long current_time,
                               const std::vector<MachineState> &sim_machines) const;
};

#endif
