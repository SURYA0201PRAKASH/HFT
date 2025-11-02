cmake_minimum_required(VERSION 3.15)
project(hft_prototype)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- Toggle ZMQ integration ----
option(ENABLE_ZMQ "Build with ZeroMQ PUB/SUB integration" ON)

# Manual download of nlohmann json (header-only)
if(NOT EXISTS "${CMAKE_SOURCE_DIR}/include/nlohmann/json.hpp")
    message(STATUS "Downloading nlohmann/json.hpp...")
    file(DOWNLOAD 
        "https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp"
        "${CMAKE_SOURCE_DIR}/include/nlohmann/json.hpp"
        TLS_VERIFY ON
    )
endif()

# Core deps
find_package(Boost 1.70 REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)

# ---- Optional ZeroMQ discovery ----
if(ENABLE_ZMQ)
  include(CheckIncludeFileCXX)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(ZMQ QUIET libzmq)
  endif()

  # Fallback if pkg-config not present or didn’t find it
  if(NOT ZMQ_FOUND)
    find_library(ZMQ_LIBRARY NAMES zmq libzmq)
    if(ZMQ_LIBRARY)
      set(ZMQ_LIBRARIES ${ZMQ_LIBRARY})
      set(ZMQ_FOUND TRUE)
      message(STATUS "Found libzmq via find_library: ${ZMQ_LIBRARY}")
    endif()
  else()
    message(STATUS "Found libzmq via pkg-config: ${ZMQ_LIBRARIES}")
  endif()

  # Try to find cppzmq (header-only) for <zmq.hpp> convenience; optional
  find_path(CPPZMQ_INCLUDE_DIR NAMES zmq.hpp PATH_SUFFIXES cppzmq)
  if(CPPZMQ_INCLUDE_DIR)
    message(STATUS "Found cppzmq headers at: ${CPPZMQ_INCLUDE_DIR}")
  endif()

  if(ZMQ_FOUND)
    add_compile_definitions(ZMQ_BUILD)  # enables your #ifdef ZMQ_BUILD blocks
  else()
    message(WARNING "ENABLE_ZMQ=ON but libzmq not found. Build will continue WITHOUT ZeroMQ.")
  endif()
endif()

# ---- Target ----
add_executable(hft_prototype
    # src/main.cpp
	src/test_bybit.cpp
    src/bybit_ws_client.cpp
    src/deribit_client.cpp
    src/enhanced_deribit_client.cpp
    src/enhanced_logger.cpp
    src/enhanced_order_book.cpp
    src/market_data_recorder.cpp
    src/order_book.cpp
    src/order_book_manager.cpp
    src/sequence_manager.cpp
    src/tick_analytics.cpp
    src/tick_data.cpp
    src/tick_processor.cpp
    src/trading_strategy.cpp
    src/ws_client_day3.cpp
    src/config_loader.cpp
)

# Includes
target_include_directories(hft_prototype PRIVATE
    include
    ${Boost_INCLUDE_DIRS}
)
if(ENABLE_ZMQ AND CPPZMQ_INCLUDE_DIR)
  target_include_directories(hft_prototype PRIVATE ${CPPZMQ_INCLUDE_DIR})
endif()
if(SQLite3_INCLUDE_DIRS)
  include_directories(${SQLite3_INCLUDE_DIRS})
endif()

# Links
target_link_libraries(hft_prototype
    PRIVATE Boost::boost
    PRIVATE Boost::system
    PRIVATE OpenSSL::SSL
    PRIVATE OpenSSL::Crypto
    PRIVATE Threads::Threads
    PRIVATE SQLite::SQLite3
    PRIVATE yaml-cpp
)
if(ENABLE_ZMQ AND ZMQ_FOUND)
  target_link_libraries(hft_prototype PRIVATE ${ZMQ_LIBRARIES})
endif()
