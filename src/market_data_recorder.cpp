// market_data_recorder.cpp - USE THIS EXACT CODE:
#include "market_data_recorder.hpp"
#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include <cmath>  // for std::isnan, std::isinf

static double safe_value(double v) {
    return (std::isnan(v) || std::isinf(v)) ? 0.0 : v;
}

MarketDataRecorder::MarketDataRecorder(const std::string& filename_prefix) {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    std::string signal_filename = filename_prefix + "_signals_" + std::to_string(timestamp) + ".csv";
    signal_stream_.open(signal_filename);
    signal_stream_ << "timestamp,instrument,signal_type,imbalance,bid_price,ask_price\n";

    // ✅ Add this block below
    if (sqlite3_open("../data/stream_features.db", &db_) != SQLITE_OK) {
    std::cerr << "❌ Cannot open SQLite database\n";
    db_ = nullptr;
} else {
    // ✅ Enable WAL mode so readers and writers can coexist
    char* errMsg = nullptr;
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA temp_store=MEMORY;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA locking_mode=NORMAL;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA busy_timeout=5000;", nullptr, nullptr, &errMsg);

    const char* create_sql =
        "CREATE TABLE IF NOT EXISTS ticks_live ("
        "timestamp_ms INTEGER,"
        "instrument TEXT,"
        "trade_price REAL,"
        "trade_qty REAL,"
        "rolling_vol REAL,"
        "is_large INTEGER);";
    sqlite3_exec(db_, create_sql, nullptr, nullptr, nullptr);
    std::cout << "📀 SQLite database ready (WAL mode enabled)\n";
}

}


MarketDataRecorder::~MarketDataRecorder() {
	if (db_) {
    sqlite3_close(db_);
    db_ = nullptr;
}
    stop_recording();
}

void MarketDataRecorder::record_signal(const std::string& instrument, const std::string& signal, 
                                      double imbalance, double bid, double ask) {
    if (!recording_) return;
    
    std::lock_guard<std::mutex> lock(file_mutex_);
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    signal_stream_ << timestamp << "," << instrument << "," << signal << ","
                  << imbalance << "," << bid << "," << ask << "\n";
    signal_stream_.flush();
}

void MarketDataRecorder::start_recording() {
    recording_ = true;
//     // std::cout << "📊 Started market data recording\n";
}

void MarketDataRecorder::stop_recording() {
    recording_ = false;
    if (signal_stream_.is_open()) {
        signal_stream_.close();
    }
}

void MarketDataRecorder::record(const Tick& tick, double vol, bool large) {
    if (!recording_) return;

    std::lock_guard<std::mutex> lock(file_mutex_);
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (!signal_stream_.is_open()) return;

    // ✅ sanitize before use
    double safe_vol   = safe_value(vol);
    double safe_price = safe_value(tick.price);
    double safe_qty   = safe_value(tick.quantity);

    signal_stream_ << timestamp << ","
                   << tick.instrument << ","
                   << "TICK" << ","      // placeholder for signal_type
                   << safe_vol << ","
                   << safe_price << ","
                   << (large ? 1 : 0)
                   << "\n";
    signal_stream_.flush();

    // ✅ SQLite insert
    if (db_) {
        std::string sql = "INSERT INTO ticks_live (timestamp_ms, instrument, trade_price, trade_qty, rolling_vol, is_large) VALUES (" +
                          std::to_string(timestamp) + ", '" + tick.instrument + "', " +
                          std::to_string(safe_price) + ", " +
                          std::to_string(safe_qty) + ", " +
                          std::to_string(safe_vol) + ", " +
                          std::to_string(large ? 1 : 0) + ");";

        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) {
            std::cerr << "❌ SQLite insert error: " << sqlite3_errmsg(db_) << std::endl;
        }
    }
}

