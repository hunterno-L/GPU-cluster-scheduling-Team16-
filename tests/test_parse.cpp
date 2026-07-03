#include <iostream>
#include <sstream>
#include <vector>
#include "parser.h"
#include "models.h"

int main() {
    std::istringstream input("2 20\n6 80 92 416\n7 64 80 400\n788 266 1 60 8 88 12\n266 129 1 22 24 166 9\n1094 147 1 35 16 136 3\n1211 208 1 38 26 134 9\n1530 176 1 21 27 70 11\n1092 226 1 44 30 159 11\n2174 36 1 32 8 101 2\n1299 254 1 40 28 74 5\n2013 71 1 40 28 105 3\n2084 250 1 49 36 31 6\n1761 288 2 42 6 72 11\n886 102 1 19 21 84 6\n1733 223 2 85 20 158 1\n1269 26 1 39 23 57 8\n1097 76 1 51 27 28 10\n2493 63 1 23 37 129 1\n1113 59 2 70 12 100 1\n778 36 2 67 22 81 3\n1500 124 2 80 34 47 8\n320 110 1 11 10 86 3");
    auto result = readInstance(input);
    std::cout << "Servers: " << result.first.size() << ", Jobs: " << result.second.size() << std::endl;
    for (const auto& j : result.second) {
        std::cout << "Job " << j.job_id << ": release=" << j.release_time << ", dur=" << j.duration 
                  << ", min_gpu=" << j.min_gpu << ", gpu_mem=" << j.gpu_memory << std::endl;
    }
    return 0;
}
