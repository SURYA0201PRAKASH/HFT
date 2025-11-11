// market_data_recorder.hpp
#pragma once
#include <string>
#include <fstream>
#include <atomic>
#include <mutex>
#include <chrono>
#include <memory>
#include "tick_data.hpp"
#include <sqlite3.h>
#include "config_loader.hpp"
#include "indicators_state.hpp"
#include <unordered_map>

class MarketDataRecorder {
public:
    MarketDataRecorder(const Config& cfg);
    ~MarketDataRecorder();
    
    void record_signal(const std::string& instrument, const std::string& signal, 
                      double imbalance, double bid, double ask);
    void start_recording();
    void stop_recording();
	void record(const Tick& tick, double vol, bool large);
    void record_with_indicators(const Tick& t, double vol, bool large, const Indicators& I);
private:
    std::ofstream signal_stream_;
    std::atomic<bool> recording_{false};
    std::mutex file_mutex_;
	sqlite3* db_ = nullptr;
};