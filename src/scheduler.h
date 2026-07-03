#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include "machine_state.h"
#include "models.h"

struct FinishEvent {
    long long finish_time; int server_id; int job_id; RunningJob running_job;
    bool operator>(const FinishEvent &other) const;
};
struct ReadyJob {
    int job_index; double priority; int feasible_count; int duration; int job_id;
    long long release_time;    // original arrival time (for computing waiting_time)
    long long waiting_time;    // accumulated wait: current_time - release_time (updated on re-push)
};

// Runtime tuning parameters (replaces compile-time #defines for key knobs)
struct SchedulerParams {
    double age_w = 0.4;                // wait-time aging weight (0=off)
    double duration_exp = 1.0;         // exponent: weight / duration^exp (1.0=WSPT)
    double high_priority_boost = 0.0;  // extra boost for top-weight jobs
    double mem_trade_off_ratio = 1.00; // switch if waste < best_waste * ratio (0=disable)
    double cost_premium_ratio = 1.50;  // allow alt only if cost <= best_cost * ratio
};

struct ReadyJobCompare {
    const SchedulerParams* params;  // non-null pointer to runtime params
    explicit ReadyJobCompare(const SchedulerParams* p) : params(p) {}
    bool operator()(const ReadyJob &le, const ReadyJob &ri) const;
};

// Compile-time constants (still fixed per experiment)
#ifndef FLEX_W
#define FLEX_W 2.0
#endif
#ifndef SLACK_W
#define SLACK_W 0.00
#endif
#ifndef WASTE_W
#define WASTE_W 0.25
#endif
#ifndef BACKFILL_ITER
#define BACKFILL_ITER 0
#endif

class SchedulerEngine {
public:
    SchedulerEngine(const std::vector<ServerSpec>&, const std::vector<Job>&,
        const SchedulerParams& params,
        bool(*cmp)(const Job&,const Job&)=nullptr, const std::string& ="best-fit", bool bf=false);
    std::unordered_map<int,ScheduleRecord> schedule();
    static int totalWeight(const std::unordered_map<int,ScheduleRecord>&, const std::vector<Job>&);
    const SchedulerParams params;
private:
    using FH = std::priority_queue<FinishEvent,std::vector<FinishEvent>,std::greater<FinishEvent>>;
    using RH = std::priority_queue<ReadyJob,std::vector<ReadyJob>,ReadyJobCompare>;
    struct SR { bool ok=false; ScheduleRecord rec{}; RunningJob rj{}; };

    void buildFeasible();
    inline double cost(int mi, const Job &j, int g) const;
    SR tryOne(const Job&,long long);
    void rel(long long,FH&); int dlimit(int)const;
    std::vector<MachineState> ms; std::unordered_map<int,int> mi;
    std::vector<Job> jobs; std::unordered_map<int,std::vector<std::pair<int,int>>> fe;
    std::vector<int> mf;
    std::vector<double> mfi;
    double flex_inv;
    bool bf;
};

SchedulerEngine makeOptimized(const std::vector<ServerSpec>&, const std::vector<Job>&, const SchedulerParams& = SchedulerParams());
#endif
