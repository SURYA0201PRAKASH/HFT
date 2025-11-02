#pragma once
#include <fstream>
#include <string>
#include <chrono>
#include <mutex>
#include "nlohmann/json.hpp"

class EnhancedLogger {
public:
    EnhancedLogger(const std::string& filename);
    ~EnhancedLogger();
    
    void log_raw_message(const std::string& message);
    void log_parsed_book(const nlohmann::json& book_data, const std::string& instrument);
    void log_trade(const nlohmann::json& trade_data);
    void log_snapshot(const std::string& instrument, const nlohmann::json& snapshot);
    
private:
    std::ofstream raw_log_;
    std::ofstream book_log_;
    std::ofstream trade_log_;
    std::ofstream snapshot_log_;
    std::mutex log_mutex_;
};