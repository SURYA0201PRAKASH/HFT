#include "enhanced_logger.hpp"
#include <iostream>
#include <iomanip>

EnhancedLogger::EnhancedLogger(const std::string& filename) {
    raw_log_.open(filename + "_raw.jsonl", std::ios::app);
    book_log_.open(filename + "_book.jsonl", std::ios::app);
    trade_log_.open(filename + "_trade.jsonl", std::ios::app);
    snapshot_log_.open(filename + "_snapshot.jsonl", std::ios::app);
    
    if (!raw_log_.is_open() || !book_log_.is_open() || 
        !trade_log_.is_open() || !snapshot_log_.is_open()) {
        throw std::runtime_error("Failed to open log files");
    }
}

EnhancedLogger::~EnhancedLogger() {
    raw_log_.close();
    book_log_.close();
    trade_log_.close();
    snapshot_log_.close();
}

void EnhancedLogger::log_raw_message(const std::string& message) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    raw_log_ << ms << " " << message << "\n";
    raw_log_.flush();
}

void EnhancedLogger::log_parsed_book(const nlohmann::json& book_data, const std::string& instrument) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    nlohmann::json log_entry = {
        {"timestamp", ms},
        {"instrument", instrument},
        {"data", book_data}
    };
    
    book_log_ << log_entry.dump() << "\n";
    book_log_.flush();
}

void EnhancedLogger::log_trade(const nlohmann::json& trade_data) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    nlohmann::json log_entry = {
        {"timestamp", ms},
        {"data", trade_data}
    };
    
    trade_log_ << log_entry.dump() << "\n";
    trade_log_.flush();
}

void EnhancedLogger::log_snapshot(const std::string& instrument, const nlohmann::json& snapshot) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    nlohmann::json log_entry = {
        {"timestamp", ms},
        {"instrument", instrument},
        {"snapshot", snapshot}
    };
    
    snapshot_log_ << log_entry.dump() << "\n";
    snapshot_log_.flush();
}