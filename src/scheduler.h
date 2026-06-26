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
};
struct ReadyJobCompare {
    bool operator()(const ReadyJob &le, const ReadyJob &ri) const;
};

// Tuning knobs: modifiable by auto-scan script
#ifndef FLEX_W
#define FLEX_W 2.0
#endif
#ifndef SLACK_W
#define SLACK_W 0.40
#endif
#ifndef WASTE_W
#define WASTE_W 0.25    // 0.0=off; 0.25 matches scheduler-optimization1.0
#endif
#ifndef BACKFILL_ITER
#define BACKFILL_ITER 0  // 1=iterative, 0=single-pass
#endif

class SchedulerEngine {
public:
    SchedulerEngine(const std::vector<ServerSpec>&, const std::vector<Job>&,
        bool(*cmp)(const Job&,const Job&)=nullptr, const std::string& ="best-fit", bool bf=false);
    std::unordered_map<int,ScheduleRecord> schedule();
    static int totalWeight(const std::unordered_map<int,ScheduleRecord>&, const std::vector<Job>&);
private:
    using FH = std::priority_queue<FinishEvent,std::vector<FinishEvent>,std::greater<FinishEvent>>;
    using RH = std::priority_queue<ReadyJob,std::vector<ReadyJob>,ReadyJobCompare>;
    struct SR { bool ok=false; ScheduleRecord rec{}; RunningJob rj{}; };
    void buildFeasible();
    // Inlined cost computation (no vtable dispatch)
    inline double cost(int mi, const Job &j, int g) const;
    SR tryOne(const Job&,long long);
    void rel(long long,FH&); int dlimit(int)const; long long nt(long long,int,const FH&)const;
    std::vector<MachineState> ms; std::unordered_map<int,int> mi;
    std::vector<Job> jobs; std::unordered_map<int,std::vector<std::pair<int,int>>> fe;
    std::vector<int> mf;        // machine_flex (static count of feasible jobs per machine)
    std::vector<double> mfi;    // precomputed: mf[i] * flex_inv (cached flexibility ratio)
    double flex_inv;            // 1.0 / max(jobs.size(),1)
    bool bf;
};
SchedulerEngine makeOptimized(const std::vector<ServerSpec>&, const std::vector<Job>&);
#endif
