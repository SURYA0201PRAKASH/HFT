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

class MarketDataRecorder {
public:
    MarketDataRecorder(const std::string& filename_prefix);
    ~MarketDataRecorder();
    
    void record_signal(const std::string& instrument, const std::string& signal, 
                      double imbalance, double bid, double ask);
    void start_recording();
    void stop_recording();
	void record(const Tick& tick, double vol, bool large);
    
private:
    std::ofstream signal_stream_;
    std::atomic<bool> recording_{false};
    std::mutex file_mutex_;
	sqlite3* db_ = nullptr;
};