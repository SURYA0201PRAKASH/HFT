// market_data_recorder.cpp - USE THIS EXACT CODE:
#include "market_data_recorder.hpp"
#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include <cmath>  // for std::isnan, std::isinf
#include <filesystem>
#include "indicators_state.hpp"

static double safe_value(double v) {
    return (std::isnan(v) || std::isinf(v)) ? 0.0 : v;
}

// ✅ Updated constructor to take Config instead of string
MarketDataRecorder::MarketDataRecorder(const Config& cfg) {
    // Get filename prefix (optional)
    std::string filename_prefix = "../data/" + cfg.exchange;
    exchange_ = cfg.exchange;
    sqlite_path_ = cfg.recording.sqlite_path;

    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::string signal_filename = filename_prefix + "_signals_" + std::to_string(timestamp) + ".csv";
    signal_stream_.open(signal_filename);
    signal_stream_ << "timestamp,instrument,signal_type,imbalance,bid_price,ask_price\n";

    // ✅ Open SQLite database from config
    const std::string& db_path = cfg.recording.sqlite_path;
    std::cout << "================ DEBUG: MarketDataRecorder =================" << std::endl;
    std::cout << "Requested SQLite path: " << cfg.recording.sqlite_path << std::endl;

    try {
        std::cout << "Absolute path: "
                  << std::filesystem::absolute(cfg.recording.sqlite_path)
                  << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    }

    // Verify directory existence
    auto parent_dir = std::filesystem::path(cfg.recording.sqlite_path).parent_path();
    std::cout << "Parent directory: " << parent_dir
              << " | Exists? " << std::filesystem::exists(parent_dir) << std::endl;

    // Print current working directory
    std::cout << "Current Working Directory: "
              << std::filesystem::current_path() << std::endl;
    std::cout << "============================================================" << std::endl;

    // 🧩 DEBUGGING SQLITE OPEN
    int rc = sqlite3_open(db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
        std::cerr << "❌ Cannot open SQLite database: " << db_path << "\n";
        std::cerr << "SQLite error code: " << rc
                  << " | msg: " << sqlite3_errmsg(db_) << std::endl;

        if (!std::filesystem::exists(db_path)) {
            std::cerr << "⚠️ DB file does not exist yet. Trying to create manually...\n";
            std::ofstream(db_path).close();
            std::cerr << "✅ File created? " << std::filesystem::exists(db_path) << std::endl;
        }

        std::error_code ec;
        auto perms = std::filesystem::status(db_path, ec).permissions();
        std::cerr << "📂 File permissions: " << ((int)perms)
                  << " | error? " << ec.message() << std::endl;

        auto parent = std::filesystem::path(db_path).parent_path();
        std::cerr << "📁 Parent dir perms: "
                  << ((int)std::filesystem::status(parent, ec).permissions())
                  << " | error? " << ec.message() << std::endl;

        std::cerr << "👤 Running as user: ";
        system("whoami");

        db_ = nullptr;
    } else {
        std::cout << "✅ SQLite open success for " << db_path << std::endl;

        char* errMsg = nullptr;
        sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errMsg);
        sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, &errMsg);
        sqlite3_exec(db_, "PRAGMA temp_store=MEMORY;", nullptr, nullptr, &errMsg);
        sqlite3_exec(db_, "PRAGMA locking_mode=NORMAL;", nullptr, nullptr, &errMsg);
        sqlite3_exec(db_, "PRAGMA busy_timeout=5000;", nullptr, nullptr, &errMsg);

        // ✅ Ensure table exists
        const char* create_sql = R"SQL(
        CREATE TABLE IF NOT EXISTS ticks_live (
            timestamp_ms INTEGER,
            instrument TEXT,
            trade_price REAL,
            trade_qty REAL,
            best_bid REAL,
            best_ask REAL,
            mid_price REAL,
            spread REAL,
            ema12 REAL,
            ema26 REAL,
            macd REAL,
            rsi14 REAL,
            bb_mid REAL,
            bb_up REAL,
            bb_low REAL,
            vwap REAL,
            rolling_vol REAL,
            bid_vol_1 REAL,
            ask_vol_1 REAL,
            bid_vol_5 REAL,
            ask_vol_5 REAL,
            order_imbalance REAL,
            ofi REAL,
            return_1s REAL,
            return_5s REAL,
            realized_vol_1s REAL,
            feed_latency_ms REAL,
            queue_latency_us REAL,
            tick_gap_ms REAL,
            tick_rate REAL,
            vol_zscore REAL,
            hour_of_day INTEGER,
            minute_of_day INTEGER,
            is_large INTEGER,
            trend_label INTEGER,
            exchange TEXT,
            PRIMARY KEY(timestamp_ms, instrument)
        );
        )SQL";

        sqlite3_exec(db_, create_sql, nullptr, nullptr, nullptr);
        std::cout << "📀 SQLite database ready (WAL mode enabled) → "
                  << db_path << "\n";
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
	std::cout << "[REC] record() | inst=" << tick.instrument
          << " | px=" << tick.price
          << " | qty=" << tick.quantity
          << " | type=" << (int)tick.type << std::endl;
	if (!recording_) {
    std::cerr << "⚠️ recording_ is false — skipping DB insert\n";
	}
	if (!signal_stream_.is_open()) {
    std::cerr << "⚠️ signal_stream_ closed — skipping DB insert\n";
	}
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
void MarketDataRecorder::record_with_indicators(
    const Tick& t, double vol, bool large, const Indicators& I)
{
	std::cout << "\n🧩 [record_with_indicators] this=" << this
              << " | recording_=" << recording_
              << " | db_=" << db_
              << " | path=" << sqlite_path_ << std::endl;
    if (!recording_ || !db_) return;
    std::lock_guard<std::mutex> lock(file_mutex_);

    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();

    auto safe = [](double v){ return (std::isnan(v)||std::isinf(v))?0.0:v; };

    std::ostringstream sql;
    sql << "INSERT OR REPLACE INTO ticks_live VALUES("
        << ts << ", '" << t.instrument << "', "
        << safe(t.price) << ", " << safe(t.quantity) << ", "
        << safe(I.best_bid) << ", " << safe(I.best_ask) << ", "
        << safe(I.mid) << ", " << safe(I.spread) << ", "
        << safe(I.ema12) << ", " << safe(I.ema26) << ", "
        << safe(I.macd) << ", " << safe(I.rsi14) << ", "
        << safe(I.bb_mid) << ", " << safe(I.bb_up) << ", " << safe(I.bb_low) << ", "
        << safe(I.vwap) << ", " << safe(vol) << ", "
        << safe(I.bid_vol_1) << ", " << safe(I.ask_vol_1) << ", "
        << safe(I.bid_vol_5) << ", " << safe(I.ask_vol_5) << ", "
        << safe(I.order_imbalance) << ", " << safe(I.ofi) << ", "
        << safe(I.return_1s) << ", " << safe(I.return_5s) << ", "
        << safe(I.realized_vol_1s) << ", "
        << safe(I.feed_latency_ms) << ", " << safe(I.queue_latency_us) << ", "
        << safe(I.tick_gap_ms) << ", " << safe(I.tick_rate) << ", "
        << safe(I.vol_zscore) << ", "
        << static_cast<int>(I.hour_of_day) << ", "
        << static_cast<int>(I.minute_of_day) << ", "
        << (large ? 1 : 0) << ", "
        << I.trend_label << ", "
        << "'" << exchange_ << "');";

    // --- measure only the exec call ---
    auto t0 = std::chrono::steady_clock::now();
    int rc = sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, nullptr);
    auto t1 = std::chrono::steady_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    if (rc != SQLITE_OK)
        std::cerr << "SQLite error: " << sqlite3_errmsg(db_) << std::endl;

    std::cout << "[DB INSERT LATENCY] "
              << micros << " µs"
              << " | Instrument: " << t.instrument
              << " | Price: " << t.price
              << " | Exchange: " << exchange_
              << std::endl;
}



