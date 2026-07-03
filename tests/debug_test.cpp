#include <iostream>
#include <fstream>
#include <vector>
#include "parser.cpp"
#include "scheduler.cpp"
#include "machine_state.cpp"
#include "output.cpp"

int main() {
    std::ifstream input("../../02-数据集/case001.in");
    auto result = readInstance(input);
    std::vector<ServerSpec> servers = result.first;
    std::vector<Job> jobs = result.second;
    
    std::cout << "Total jobs: " << jobs.size() << std::endl;
    for (const auto& job : jobs) {
        std::cout << "Job " << job.job_id << ": release=" << job.release_time 
                  << ", dur=" << job.duration << ", min_gpu=" << job.min_gpu
                  << ", gpu_mem=" << job.gpu_memory << std::endl;
    }
    return 0;
}
