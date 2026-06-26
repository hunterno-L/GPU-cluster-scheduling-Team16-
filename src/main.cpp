#include <fstream>
#include <iostream>

#include "output.h"
#include "parser.h"
#include "scheduler.h"

using namespace std;

int main(int argc, char* argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    vector<ServerSpec> servers;
    vector<Job> jobs;

    if (argc >= 2) {
        ifstream fin(argv[1]);
        if (!fin) { cerr << "Cannot open: " << argv[1] << endl; return 1; }
        auto [s, j] = readInstance(fin);
        servers = s; jobs = j;
    } else {
        auto [s, j] = readInstance(cin);
        servers = s; jobs = j;
    }

    if (jobs.empty()) return 0;

    SchedulerEngine engine = makeOptimized(servers, jobs);
    auto records = engine.schedule();

    vector<ScheduleRecord> ordered;
    ordered.reserve(records.size());
    for (const auto& kv : records) ordered.push_back(kv.second);
    writeScheduleRecords(cout, ordered);

    return 0;
}
