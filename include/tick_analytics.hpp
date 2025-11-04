#pragma once
#include "tick_data.hpp"
#include <vector>
#include <deque>
#include <thread>
#include <atomic>
#include <iostream>
#include <nlohmann/json.hpp>
#include "market_data_recorder.hpp"

#ifdef ZMQ_BUILD
#include <zmq.hpp>
#endif

class TickAnalytics {
private:
    // === Statistical data ===
    std::deque<double> price_changes_;
    std::deque<double> volumes_;
    size_t window_size_{1000};

    // === Async worker state ===
    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> pull_thread_;
	std::unique_ptr<MarketDataRecorder> recorder_; // 23rd Oct 2025
	
#ifdef ZMQ_BUILD
    zmq::context_t zmq_ctx_{1};
    zmq::socket_t zmq_pull_{zmq_ctx_, zmq::socket_type::pull};
#endif

public:
    TickAnalytics() = default;
    ~TickAnalytics();
    // === Statistical functions ===
    double calculate_volatility(size_t lookback_ticks = 100) const;
    double calculate_vwap(const std::vector<Tick>& ticks) const;
    bool is_large_trade(const Tick& tick, double threshold_multiplier = 5.0) const;
    void add_tick(const Tick& tick);

#ifdef ZMQ_BUILD
    // === Asynchronous ZeroMQ Worker (Module C) ===
    void start_pull_server(const std::string& address, const Config& cfg);
    void stop();
#endif
};
