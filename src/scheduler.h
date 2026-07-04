#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H

#include <queue>
#include <unordered_map>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <chrono>

#include "machine_state.h"
#include "models.h"
#include <unordered_set>

struct FinishEvent {
    long long finish_time;
    int server_id;
    int job_id;
    RunningJob running_job;
    bool operator>(const FinishEvent &other) const;
};

// 成员 A：默认 WSPT 比较器（replay 等确定性路径使用）
struct UrgencyComparator {
    bool operator()(const Job &a, const Job &b) const {
        double wspt_a = static_cast<double>(a.weight) / a.duration;
        double wspt_b = static_cast<double>(b.weight) / b.duration;
        if (wspt_a != wspt_b) return wspt_a < wspt_b;
        return a.job_id > b.job_id;
    }
};

class GreedyScheduler;

// 成员 A：多策略 pending 队列比较器（支持等待时间加权）
struct PendingJobComparator {
    const GreedyScheduler *sched = nullptr;
    int order_variant = 0;
    long long *current_time_ptr = nullptr;

    bool operator()(const Job &a, const Job &b) const;
};

class GreedyScheduler {
public:
    GreedyScheduler(std::vector<ServerSpec> input_servers, std::vector<Job> input_jobs);
    std::vector<ScheduleRecord> schedule();

    // 成员 A：任务排序（供 PendingJobComparator 调用）
    double jobOrderPriority(const Job &job, int order_variant, long long current_time) const;
    bool jobLessUrgent(const Job &a, const Job &b, int order_variant, long long current_time) const;
    bool jobMoreUrgentFirst(const Job &a, const Job &b, int order_variant, long long current_time) const;

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
        long long time_horizon = 1;
        double release_spread_ratio = 0.0;
    };

    void computeInstanceProfile();
    int jobFeasibleCount(int job_id) const;
    bool isNarrowCluster() const;
    bool isSingleServer() const;
    // 规模分级（非 case 编号）：大/超大实例降低多策略与精修预算
    bool isLargeInstance() const;
    bool isMegascaleInstance() const;
    double instanceDifficulty() const;
    bool shouldDrainFullPending() const;

    // 成员 A
    int resolveOrderVariant(int strategy_seed) const;
    bool placementTieBreakPrefer(const Job &a, const Job &b) const;
    Solution polishOrderVariants(const Solution &best, int placement_seed_hint);
    bool shouldOrderPolish() const;

    // 成员 B
    int adaptiveStrategyCount() const;
    int adaptivePendingCap(int queue_size) const;
    int resolvePlacementMode(int strategy_seed) const;
    bool shouldUseRuntimePareto() const;
    bool shouldLightRefine() const;
    bool shouldFastRefine() const;
    bool shouldLongJobRefine() const;
    bool shouldSingleServerRefine() const;

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
    Solution generateGreedySolutionWithStrategy(int strategy_seed, int order_variant_override = -1);
    Solution generateQueueCandidate(double duration_exp, double age_w, double flex_w,
                                    double slack_w, double waste_w,
                                    double mem_trade_ratio, double cost_premium_ratio,
                                    bool use_backfill) const;
    Solution pickCompositeBalancedBest(std::vector<Solution> candidates) const;
    void addQueueStrategyCandidates(Solution &best_sol, Solution &second_sol,
                                    Solution *third_sol);
    Solution generateDedicatedSingleServerSolution(int strategy_seed);
    Solution generateSingleServerShelfSolution(int strategy_seed);
    Solution refinePlacement(const Solution &initial_solution, int max_passes_override = -1);
    Solution fastRefinePlacement(const Solution &initial_solution);
    Solution lightLongJobAssignmentRefine(const Solution &initial_solution);
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
    void replayScheduleWithOrder(Solution &sol, int order_variant);
    void replayScheduleAdaptive(Solution &sol);
    void replayScheduleForRefine(Solution &sol);
    void replayForRefine(Solution &sol);
    void fillReplayOrderVariants(std::vector<int> &out) const;
    bool shouldDualReplayInRefine() const;
    bool shouldAssignmentReplayPolish() const;
    Solution polishAssignmentReplay(const Solution &best);
    Solution polishAssignmentReplayLight(const Solution &best);
    bool shouldUseAssignmentReplayLight() const;
    bool shouldUseLongJobDedicatedPath() const;
    bool shouldUseAssignmentFirstMainPath() const;
    bool shouldUseSingleServerDedicatedPath() const;
    bool shouldUseMemBoundDedicatedPath() const;
    bool shouldUseMemBoundMainPath() const;
    bool shouldUseMegascaleDedicatedPath() const;
    Solution generateLongJobDedicatedSolution();
    Solution generateSingleServerDedicatedSolution();
    Solution generateMemBoundDedicatedSolution();
    Solution generateMegascaleDedicatedSolution();
    Solution generateMegascaleWaveDispatchScheduler(int policy_seed = 0);
    Solution generateMegascaleUnifiedSolution();
    void polishMegascaleLight(Solution &best);
    Solution generateCriticalRatioSolution(int strategy_seed);
    Solution generatePlacementFirstSolution(int strategy_seed);
    Solution generateListSchedulingSolution(int strategy_seed);
    Solution generateListSchedulingSolutionFast(int strategy_seed);
    Solution generateMegascaleCriticalRatioList(int strategy_seed);
    Solution generateMegascaleBalancedAssignment(int strategy_seed, int max_replays = 1);
    Solution generateMegascaleWaitOptAssignment(int strategy_seed, int max_replays = 1);
    Solution generateMegascaleAdaptiveWaitAssignment(int strategy_seed, int max_replays = 1);
    Solution generateMegascaleProfileAwareAssignment(int strategy_seed, int max_replays = 1);
    Solution generateMegascaleCoreAssignment(int strategy_seed, int max_replays = 1);
    Solution generateMegascaleBurstWaveAssignment(int strategy_seed, int max_replays = 1);
    Solution mergeMegascaleDualAssignment(const Solution &balanced_src,
                                          const Solution &adaptive_src);
    Solution generateMegascaleOnlineWaitList(int strategy_seed);
    Solution runMegascaleWaitHybridPipeline(int strategy_seed);
    PlacementPick chooseWaitOptPlacement(const Job &job,
                                          const std::vector<MachineState> &sim_machines,
                                          long long current_time,
                                          const std::vector<double> &reservation_scores) const;
    Solution polishListSchedulingPipeline(const Solution &list_source, int max_replays = 1);
    void polishLargeInstanceReplay(Solution &best);
    void polishMegascaleInstanceReplay(Solution &best);
    Solution lightweightLNS(const Solution &initial, int max_iters = 60);
    Solution rebuildWithFlexibleJobs(const Solution &base,
                                     const std::unordered_set<int> &flexible_jobs) const;
    Solution jointLNS(const Solution &initial, int max_iters = 40);
    Solution megascaleJointLNS(const Solution &initial, int max_iters = 5);
    Solution megascaleLightAssignmentRefine(const Solution &initial, int max_tries = 8);
    Solution refineMegascaleWaitContributors(const Solution &initial, int max_tries = 12);
    Solution refineMegascaleSimPlacementSearch(const Solution &initial, int max_tries = 6);
    Solution refineMegascalePairSwapSearch(const Solution &initial, int max_pair_tries = 8);
    Solution refineMegascaleReplayBeamSearch(const Solution &initial, int budget_ms = 2000);
    Solution generateMegascaleWaveAssignReplay(int policy_seed = 0);
    Solution generateMegascaleIntegratedTimeline(int policy_seed = 0);
    Solution refineMegascaleTopJobSubproblem(const Solution &initial, int budget_ms = 2500);
    Solution megascaleAnytimeImprove(Solution best,
                                     std::chrono::steady_clock::time_point deadline);
    bool megascaleAnytimeExpired(std::chrono::steady_clock::time_point deadline) const;
    bool megascaleTryBatchWaitPatch(Solution &best, int replay_mode,
                                    const std::vector<size_t> &order,
                                    size_t &cursor, int max_patches);
    double criticalRatioPriority(const Job &job,
                                 const std::vector<MachineState> &sim_machines,
                                 long long current_time) const;
    Solution pickBetterSolution(const Solution &a, const Solution &b) const;
    Solution generateStaticAssignmentSolution(int strategy_seed, int max_replays = -1);
    Solution runAssignmentFirstPipeline(const Solution &assignment_source, int max_replays = -1);
    void replayMegascaleAssignmentList(Solution &sol, int sort_mode);
    Solution runMegascaleAssignmentFirstPipeline(const Solution &assignment_source,
                                                 int max_replays = -1);
    long long minEarliestStartForJob(const Job &job,
                                     const std::vector<MachineState> &sim_machines,
                                     long long current_time) const;
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
    double estStartPlacementAdjust(const Job &job, int machine_index, int gpu_used,
                                   const std::vector<MachineState> &sim_machines,
                                   long long current_time, bool as_cost) const;
    std::vector<long long> buildReleaseWaveQuantiles(int buckets = 4) const;
    int releaseWaveBucket(long long release_time,
                          const std::vector<long long> &quantiles) const;
    bool jobCanUsePlacement(int job_id, int server_id, int gpu_used) const;

    std::vector<ServerSpec> servers;
    std::vector<Job> jobs;
    std::unordered_map<int, const Job *> job_by_id;
    std::vector<MachineState> machines;
    std::unordered_map<int, int> machine_index_by_id;
    std::unordered_map<int, std::vector<std::pair<int, int>>> feasible_machines;
    InstanceProfile profile;

    mutable long long cached_global_load_time = -1;
    mutable double cached_avg_gpu_load = 0.0;
    mutable std::vector<double> cached_machine_loads;
    mutable int generation_placement_override_ = -1;
    mutable bool generation_single_tight_gpu_ = false;
    mutable int generation_single_shelf_variant_ = -1;
    mutable bool generation_critical_ratio_ = false;
    mutable bool generation_placement_first_ = false;
    void updateGlobalLoadCache(long long current_time,
                               const std::vector<MachineState> &sim_machines) const;
};

inline bool PendingJobComparator::operator()(const Job &a, const Job &b) const {
    if (!sched || !current_time_ptr) return UrgencyComparator()(a, b);
    return sched->jobLessUrgent(a, b, order_variant, *current_time_ptr);
}

#endif
