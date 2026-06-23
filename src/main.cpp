#include <fstream>
#include <iostream>
#include "output.h"
#include "parser.h"
#include "scheduler.h"
using namespace std;
int main(int c,char**v){ ios::sync_with_stdio(false);cin.tie(nullptr);
    vector<ServerSpec> sv;vector<Job> jb;
    if(c>=2){ ifstream f(v[1]); if(!f){cerr<<"open fail\n";return 1;} auto[s,j]=readInstance(f);sv=s;jb=j; }
    else{ auto[s,j]=readInstance(cin);sv=s;jb=j; }
    if(jb.empty())return 0;
    auto e=makeOpt(sv,jb); auto rc=e.schedule();
    vector<ScheduleRecord> o;o.reserve(rc.size()); for(auto&kv:rc)o.push_back(kv.second);
    writeScheduleRecords(cout,o); return 0;
}
