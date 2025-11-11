cmake_minimum_required(VERSION 3.14)
project(strategy_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(run_strategy
    src/main_strategy.cpp
    src/strategy_engine.cpp
    src/pnl_logger.cpp
)
target_include_directories(run_strategy PRIVATE include)

find_library(SQLITE3_LIB sqlite3)
if(NOT SQLITE3_LIB)
  message(FATAL_ERROR "sqlite3 not found. Install libsqlite3-dev")
endif()
target_link_libraries(run_strategy PRIVATE ${SQLITE3_LIB} pthread)
