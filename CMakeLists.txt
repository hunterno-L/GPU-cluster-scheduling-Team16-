cmake_minimum_required(VERSION 3.22)
project(gpu_scheduler)

set(CMAKE_CXX_STANDARD 17)

add_executable(execname
        src/main.cpp
        src/parser.cpp
        src/machine_state.cpp
        src/scheduler.cpp
        src/output.cpp)

include(CTest)

if(BUILD_TESTING)
    add_executable(scheduler_tests
            tests/scheduler_tests.cpp
            src/machine_state.cpp
            src/scheduler.cpp)
    target_include_directories(scheduler_tests PRIVATE src)
    add_test(NAME scheduler_tests COMMAND scheduler_tests)
endif()
