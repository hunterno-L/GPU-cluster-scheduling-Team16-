#include "parser.h"

using namespace std;

pair<vector<ServerSpec>, vector<Job>> readInstance(istream &input) {
    int server_count;
    int job_count;
    if (!(input >> server_count >> job_count)) {
        return {{}, {}};
    }

    vector<ServerSpec> servers;
    servers.reserve(server_count);
    for (int server_id = 1; server_id <= server_count; ++server_id) {
        int gpu_count;
        int gpu_memory;
        int cpu_cores;
        int memory;
        input >> gpu_count >> gpu_memory >> cpu_cores >> memory;
        servers.push_back(ServerSpec{server_id, gpu_count, gpu_memory, cpu_cores, memory});
    }

    vector<Job> jobs;
    jobs.reserve(job_count);
    for (int job_id = 1; job_id <= job_count; ++job_id) {
        int release_time;
        int duration;
        int min_gpu;
        int gpu_memory;
        int cpu_cores;
        int memory;
        int weight;
        input >> release_time >> duration >> min_gpu >> gpu_memory >> cpu_cores >> memory >> weight;
        jobs.push_back(Job{
            job_id,
            release_time,
            duration,
            min_gpu,
            gpu_memory,
            cpu_cores,
            memory,
            weight,
        });
    }

    return {servers, jobs};
}

