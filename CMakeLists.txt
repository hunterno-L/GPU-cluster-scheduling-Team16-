cmake_minimum_required(VERSION 3.22)
project(gpu_scheduler)

set(CMAKE_CXX_STANDARD 17)

# Dispatch strategy: D0 | D1 | D2 | D3 (default: empty = current)
if(NOT DEFINED DISPATCH)
    set(DISPATCH "")
endif()
# Placement strategy: P0 | P1 | P2 | P3 | P4 (default: empty = current)
if(NOT DEFINED PLACEMENT)
    set(PLACEMENT "")
endif()
# Backfill: 0=single-pass (default), 1=iterative
if(NOT DEFINED BACKFILL_ITER)
    set(BACKFILL_ITER 0)
endif()

add_executable(execname
        src/main.cpp
        src/parser.cpp
        src/machine_state.cpp
        src/scheduler.cpp
        src/output.cpp)

if(DISPATCH)
    target_compile_definitions(execname PRIVATE DISPATCH_${DISPATCH})
endif()
if(PLACEMENT)
    target_compile_definitions(execname PRIVATE PLACEMENT_${PLACEMENT})
endif()
target_compile_definitions(execname PRIVATE BACKFILL_ITER=${BACKFILL_ITER})
