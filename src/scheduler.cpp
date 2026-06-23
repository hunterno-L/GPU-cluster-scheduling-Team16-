#include "scheduler.h"
#include <algorithm>
#include <stdexcept>
#include <unordered_set>
using namespace std;

static bool cR(const Job &a, const Job &b) { if(a.release_time!=b.release_time)return a.release_time<b.release_time; return a.job_id<b.job_id; }
bool FinishEvent::operator>(const FinishEvent &o) const {
    if(finish_time!=o.finish_time)return finish_time>o.finish_time;
    if(server_id!=o.server_id)return server_id>o.server_id; return job_id>o.job_id;
}
bool ReadyJobCompare::operator()(const ReadyJob &l, const ReadyJob &r) const {
    if(l.priority!=r.priority)return l.priority<r.priority;
    if(l.feasible_count!=r.feasible_count)return l.feasible_count>r.feasible_count;
    if(l.duration!=r.duration)return l.duration<r.duration; return l.job_id>r.job_id;
}

SchedulerEngine::SchedulerEngine(const vector<ServerSpec> &sv, const vector<Job> &jb, bool(*c)(const Job&,const Job&), const string&, bool _bf):bf(_bf){
    vector<ServerSpec> s=sv; sort(s.begin(),s.end(),[](auto&a,auto&b){return a.server_id<b.server_id;});
    for(auto&x:s)ms.emplace_back(x); for(int i=0;i<(int)ms.size();++i)mi[ms[i].spec.server_id]=i;
    if(c){jobs=jb;sort(jobs.begin(),jobs.end(),c);}else{jobs=jb;sort(jobs.begin(),jobs.end(),cR);}
    flex_inv=1.0/max((int)jobs.size(),1);
    buildFeasible();
}
void SchedulerEngine::buildFeasible(){ mf.assign(ms.size(),0);
    for(auto&j:jobs){ vector<pair<int,int>>e;
        for(int i=0;i<(int)ms.size();++i){ int g=ms[i].requiredGpuCount(j);
            if(ms[i].canEverRun(j,g)){e.push_back({i,g});++mf[i];} }
        if(!e.empty())fe[j.job_id]=e; }
    // Precompute flexibility ratios
    mfi.resize(ms.size()); for(int i=0;i<(int)ms.size();++i)mfi[i]=(double)mf[i]*flex_inv;
}

// Fully inlined cost (hot path, ~80% of runtime)
inline double SchedulerEngine::cost(int mi, const Job &j, int g) const {
    const auto &m=ms[mi];
    double gs=((double)m.remainingGpu()-g)*m.inv_gpu_count;
    double cs=((double)m.remainingCpu()-j.cpu_cores)*m.inv_cpu_cores;
    double xs=((double)m.remainingMemory()-j.memory)*m.inv_memory;
    double sl=(gs+cs+xs)/3.0; // placementSlack inlined
    double w=0.0; int c=max(g,1),tm=c*m.spec.gpu_memory;
    if(tm>0)w=(double)(tm-j.gpu_memory)/tm;
    return FLEX_W*mfi[mi] + SLACK_W*sl + WASTE_W*w;
}

SchedulerEngine::SR SchedulerEngine::tryOne(const Job &job, long long t){
    auto it=fe.find(job.job_id); if(it==fe.end())return SR{};
    auto&es=it->second; int bi=-1,bg=-1; double bc=1e18;
    for(auto&e:es){ int i=e.first,g=e.second; if(!ms[i].canStart(job,g))continue;
        double c=cost(i,job,g); if(c<bc){bc=c;bi=i;bg=g;} }
    if(bi<0)return SR{}; auto[r,rj]=ms[bi].startJob(job,t,bg);
    return SR{true,r,rj};
}
int SchedulerEngine::dlimit(int rc)const{if(rc==0)return 0; return min(rc,min(2048,max(256,(int)ms.size()*8)));}

unordered_map<int,ScheduleRecord> SchedulerEngine::schedule(){
    if(jobs.empty())return{};
    unordered_map<int,ScheduleRecord> rec; unordered_set<int> sk;
    FH run; RH pq;
    vector<Job> av=jobs; sort(av.begin(),av.end(),cR);
    int ai=0; long long t=jobs[0].release_time;
    const int JN=(int)jobs.size(), MN=(int)ms.size();

    while((int)(rec.size()+sk.size())<JN){
        rel(t,run);
        while(ai<(int)av.size()&&av[ai].release_time<=t){
            int ji=ai; auto&j=av[ai++]; if(rec.count(j.job_id)||sk.count(j.job_id))continue;
            auto ft=fe.find(j.job_id); int fc=(ft!=fe.end())?(int)ft->second.size():0;
            pq.push(ReadyJob{ji,1000000.0*j.weight/max(j.duration,1),fc,j.duration,j.job_id});
        }
        int lim=dlimit((int)pq.size()); vector<ReadyJob> df;df.reserve(lim);
        for(int i=0;i<lim&&!pq.empty();++i){
            auto rj=pq.top();pq.pop(); auto&j=jobs[rj.job_index];
            if(rec.count(j.job_id)||sk.count(j.job_id))continue;
            auto r=tryOne(j,t); if(r.ok){
                rec[j.job_id]=r.rec; run.push(FinishEvent{r.rj.finish_time,r.rj.server_id,r.rj.job_id,r.rj});
            }else df.push_back(rj);
        }
        if(bf&&!df.empty()){
            bool changed=true; int guard=0; // safety cap on iterations
            while(changed&&guard++<100){
                changed=false; vector<ReadyJob> nd; nd.reserve(df.size());
                for(auto&rj:df){ auto&j=jobs[rj.job_index];
                    if(rec.count(j.job_id)||sk.count(j.job_id))continue;
                    auto r=tryOne(j,t); if(r.ok){
                        rec[j.job_id]=r.rec; run.push(FinishEvent{r.rj.finish_time,r.rj.server_id,r.rj.job_id,r.rj});
                        changed=true;
                    }else nd.push_back(rj);
                } df=move(nd);
            }
            for(auto&rj:df){ auto&j=jobs[rj.job_index];
                if(fe.find(j.job_id)==fe.end())sk.insert(j.job_id); else pq.push(rj); }
        }else{ for(auto&rj:df){
            auto&j=jobs[rj.job_index]; if(fe.find(j.job_id)==fe.end())sk.insert(j.job_id); else pq.push(rj); }}
        if((int)(rec.size()+sk.size())>=JN)break;
        { // nextTime inlined (avoids vector alloc each call)
            long long nt_val=-1; long long rn=ai<JN?jobs[ai].release_time:-1;
            if(rn>t&&(nt_val==-1||rn<nt_val))nt_val=rn;
            if(!run.empty()){ long long ft=run.top().finish_time; if(ft>t&&(nt_val==-1||ft<nt_val))nt_val=ft; }
            if(nt_val==-1){if(!run.empty())nt_val=run.top().finish_time; else throw runtime_error("deadlock");}
            t=nt_val;
        }
    } return rec;
}
void SchedulerEngine::rel(long long tt,FH&h){ while(!h.empty()&&h.top().finish_time<=tt){
    auto ev=h.top();h.pop(); auto it=mi.find(ev.server_id); if(it!=mi.end())ms[it->second].releaseJob(ev.running_job); }}
int SchedulerEngine::totalWeight(const unordered_map<int,ScheduleRecord>&rc,const vector<Job>&jb){
    unordered_map<int,int>w;for(auto&j:jb)w[j.job_id]=j.weight; int T=0;for(auto&kv:rc){auto it=w.find(kv.first);if(it!=w.end())T+=it->second;}return T;
}
SchedulerEngine makeOpt(const vector<ServerSpec>&sv,const vector<Job>&jb){ return SchedulerEngine(sv,jb,nullptr,"best-fit",true); }
