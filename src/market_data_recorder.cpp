// market_data_recorder.cpp
#include "market_data_recorder.hpp"
#include "tick_data.hpp"              // for Tick & TickType
#include "indicators_state.hpp"

#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include <cmath>                      // std::isnan, std::isinf
#include <filesystem>

static double safe_value(double v) {
    return (std::isnan(v) || std::isinf(v)) ? 0.0 : v;
}

// ===========================
// CONSTRUCTOR
// ===========================
MarketDataRecorder::MarketDataRecorder(const Config& cfg)
{
    std::string filename_prefix = "../data/" + cfg.exchange;
    exchange_   = cfg.exchange;
    sqlite_path_ = cfg.recording.sqlite_path;

    // ---- Signal CSV for quick debug / signals ----
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::string signal_filename =
        filename_prefix + "_signals_" + std::to_string(timestamp) + ".csv";

    signal_stream_.open(signal_filename);
    signal_stream_
        << "timestamp,instrument,signal_type,imbalance,bid_price,ask_price\n";

    const std::string& db_path = cfg.recording.sqlite_path;

    std::cout << "================ DEBUG: MarketDataRecorder =================\n";
    std::cout << "Requested SQLite path: " << db_path << "\n";

    try {
        std::cout << "Absolute path: "
                  << std::filesystem::absolute(db_path) << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Filesystem error: " << e.what() << "\n";
    }

    auto parent_dir = std::filesystem::path(db_path).parent_path();
    std::cout << "Parent directory: " << parent_dir
              << " | Exists? " << std::filesystem::exists(parent_dir) << "\n";

    std::cout << "Current Working Directory: "
              << std::filesystem::current_path() << "\n";
    std::cout << "============================================================\n";

    int rc = sqlite3_open(db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
        std::cerr << "❌ Cannot open SQLite database: " << db_path << "\n";
        std::cerr << "SQLite error code: " << rc
                  << " | msg: " << sqlite3_errmsg(db_) << "\n";

        if (!std::filesystem::exists(db_path)) {
            std::cerr << "⚠️ DB file does not exist yet. Trying to create manually...\n";
            std::ofstream(db_path).close();
            std::cerr << "✅ File created? "
                      << std::filesystem::exists(db_path) << "\n";
        }

        std::error_code ec;
        auto perms = std::filesystem::status(db_path, ec).permissions();
        std::cerr << "📂 File permissions: " << ((int)perms)
                  << " | error? " << ec.message() << "\n";

        auto parent = std::filesystem::path(db_path).parent_path();
        std::cerr << "📁 Parent dir perms: "
                  << ((int)std::filesystem::status(parent, ec).permissions())
                  << " | error? " << ec.message() << "\n";

        std::cerr << "👤 Running as user: ";
        system("whoami");

        db_ = nullptr;
        return;
    }

    std::cout << "✅ SQLite open success for " << db_path << "\n";

    char* errMsg = nullptr;
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;",           nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;",         nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA temp_store=MEMORY;",          nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA locking_mode=NORMAL;",        nullptr, nullptr, &errMsg);
    sqlite3_exec(db_, "PRAGMA busy_timeout=5000;",          nullptr, nullptr, &errMsg);

    // ===========================
    // 1) ENRICHED TICKS TABLE
    // ===========================
    const char* create_ticks_live_sql = R"SQL(
        CREATE TABLE IF NOT EXISTS ticks_live (
            timestamp_ms       INTEGER,
            instrument         TEXT,
            trade_price        REAL,
            trade_qty          REAL,
            best_bid           REAL,
            best_ask           REAL,
            mid_price          REAL,
            spread             REAL,
            ema12              REAL,
            ema26              REAL,
            macd               REAL,
            rsi14              REAL,
            bb_mid             REAL,
            bb_up              REAL,
            bb_low             REAL,
            vwap               REAL,
            rolling_vol        REAL,
            bid_vol_1          REAL,
            ask_vol_1          REAL,
            bid_vol_5          REAL,
            ask_vol_5          REAL,
            order_imbalance    REAL,
            ofi                REAL,
            return_1s          REAL,
            return_5s          REAL,
            realized_vol_1s    REAL,
            feed_latency_ms    REAL,
            queue_latency_us   REAL,
            tick_gap_ms        REAL,
            tick_rate          REAL,
            vol_zscore         REAL,
            hour_of_day        INTEGER,
            minute_of_day      INTEGER,
            is_large           INTEGER,
            trend_label        INTEGER,
            exchange           TEXT,
            PRIMARY KEY(timestamp_ms, instrument)
        );
    )SQL";

    // ===========================
    // 2) ORDERBOOK L1 TABLE
    // ===========================
    const char* create_ob_sql = R"SQL(
        CREATE TABLE IF NOT EXISTS orderbook_l1 (
            timestamp_ms     INTEGER,
            instrument       TEXT,
            best_bid         REAL,
            best_ask         REAL,
            mid_price        REAL,
            bid_vol_1        REAL,
            ask_vol_1        REAL,
            bid_vol_5        REAL,
            ask_vol_5        REAL,
            order_imbalance  REAL,
            ofi              REAL,
            tick_gap_ms      REAL,
            tick_rate        REAL,
            exchange         TEXT,
            PRIMARY KEY(timestamp_ms, instrument)
        );
    )SQL";

    // ===========================
    // 3) RAW TRADES TABLE
    // ===========================
    const char* create_trades_sql = R"SQL(
        CREATE TABLE IF NOT EXISTS trades_raw (
            timestamp_ms      INTEGER,
            instrument        TEXT,
            trade_price       REAL,
            trade_qty         REAL,
            side              TEXT,
            best_bid          REAL,
            best_ask          REAL,
            mid_price         REAL,
            exchange_ts_ns    INTEGER,
            ingest_ts_ns      INTEGER,
            exchange          TEXT,
            PRIMARY KEY(timestamp_ms, instrument, exchange_ts_ns)
        );
    )SQL";

    sqlite3_exec(db_, create_ticks_live_sql, nullptr, nullptr, nullptr);
    sqlite3_exec(db_, create_ob_sql,        nullptr, nullptr, nullptr);
    sqlite3_exec(db_, create_trades_sql,    nullptr, nullptr, nullptr);

    std::cout << "📀 SQLite database ready (WAL mode enabled) → "
              << db_path << "\n";
}

// ===========================
// DESTRUCTOR
// ===========================
MarketDataRecorder::~MarketDataRecorder() {
    stop_recording();
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

// ===========================
// SIGNAL CSV (unchanged)
// ===========================
void MarketDataRecorder::record_signal(const std::string& instrument,
                                       const std::string& signal,
                                       double imbalance,
                                       double bid,
                                       double ask)
{
    if (!recording_) return;

    std::lock_guard<std::mutex> lock(file_mutex_);
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    signal_stream_ << timestamp << ","
                   << instrument << ","
                   << signal << ","
                   << imbalance << ","
                   << bid << ","
                   << ask << "\n";
    signal_stream_.flush();
}

// ===========================
// START / STOP
// ===========================
void MarketDataRecorder::start_recording() {
    recording_ = true;
}

void MarketDataRecorder::stop_recording() {
    recording_ = false;
    if (signal_stream_.is_open()) {
        signal_stream_.close();
    }
}

// ===========================
// BASIC record()  → trades_raw
// (used only for simple logging)
// ===========================
void MarketDataRecorder::record(const Tick& tick, double vol, bool large) {
    std::cout << "[REC] record() | inst=" << tick.instrument
              << " | px=" << tick.price
              << " | qty=" << tick.quantity
              << " | type=" << (int)tick.type << std::endl;

    if (!recording_ || !db_) return;

    std::lock_guard<std::mutex> lock(file_mutex_);
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    double px   = safe_value(tick.price);
    double qty  = safe_value(tick.quantity);

    // Only store trades here; skip orderbook ticks
    if (tick.type != TickType::TRADE || px <= 0.0 || qty <= 0.0)
        return;

    std::ostringstream sql;
    sql << "INSERT OR IGNORE INTO trades_raw ("
        << "timestamp_ms, instrument, trade_price, trade_qty, side, "
        << "best_bid, best_ask, mid_price, exchange_ts_ns, ingest_ts_ns, exchange"
        << ") VALUES ("
        << ts << ", '" << tick.instrument << "', "
        << px << ", " << qty << ", "
        << "'" << tick.side << "', "
        << safe_value(tick.best_bid) << ", "
        << safe_value(tick.best_ask) << ", "
        << safe_value(tick.mid_price) << ", "
        << static_cast<long long>(tick.exchange_timestamp) << ", "
        << static_cast<long long>(tick.timestamp_ns) << ", "
        << "'" << exchange_ << "');";

    if (sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::cerr << "❌ SQLite insert error (record): "
                  << sqlite3_errmsg(db_) << std::endl;
    }
}

// ===========================
// record_with_indicators
// → splits into 3 tables
// ===========================
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

    auto sv = [](double v){ return (std::isnan(v) || std::isinf(v)) ? 0.0 : v; };

    double px      = sv(t.price);
    double qty     = sv(t.quantity);
    double bestBid = sv(I.best_bid);
    double bestAsk = sv(I.best_ask);
    double mid     = sv(I.mid);

    // ===========================
    // A) ORDERBOOK L1 TICKS
    // ===========================
    if (t.type == TickType::ORDERBOOK_SNAPSHOT ||
        t.type == TickType::ORDERBOOK_DELTA)
    {
        std::ostringstream sql_ob;
        sql_ob << "INSERT OR REPLACE INTO orderbook_l1 ("
               << "timestamp_ms, instrument, best_bid, best_ask, mid_price, "
               << "bid_vol_1, ask_vol_1, bid_vol_5, ask_vol_5, "
               << "order_imbalance, ofi, tick_gap_ms, tick_rate, exchange"
               << ") VALUES ("
               << ts << ", '" << t.instrument << "', "
               << bestBid << ", " << bestAsk << ", " << mid << ", "
               << sv(I.bid_vol_1) << ", " << sv(I.ask_vol_1) << ", "
               << sv(I.bid_vol_5) << ", " << sv(I.ask_vol_5) << ", "
               << sv(I.order_imbalance) << ", " << sv(I.ofi) << ", "
               << sv(I.tick_gap_ms) << ", " << sv(I.tick_rate) << ", "
               << "'" << exchange_ << "');";

        int rc_ob = sqlite3_exec(db_, sql_ob.str().c_str(), nullptr, nullptr, nullptr);
        if (rc_ob != SQLITE_OK) {
            std::cerr << "SQLite error (orderbook_l1): "
                      << sqlite3_errmsg(db_) << std::endl;
        }

        // IMPORTANT: do NOT write this into ticks_live.
        return;
    }

    // ===========================
    // B) TRADES → trades_raw
    // ===========================
    if (t.type == TickType::TRADE && px > 0.0 && qty > 0.0) {
        std::ostringstream sql_tr;
        sql_tr << "INSERT OR IGNORE INTO trades_raw ("
               << "timestamp_ms, instrument, trade_price, trade_qty, side, "
               << "best_bid, best_ask, mid_price, "
               << "exchange_ts_ns, ingest_ts_ns, exchange"
               << ") VALUES ("
               << ts << ", '" << t.instrument << "', "
               << px << ", " << qty << ", "
               << "'" << t.side << "', "
               << bestBid << ", " << bestAsk << ", " << mid << ", "
               << static_cast<long long>(t.exchange_timestamp) << ", "
               << static_cast<long long>(t.timestamp_ns) << ", "
               << "'" << exchange_ << "');";

        int rc_tr = sqlite3_exec(db_, sql_tr.str().c_str(), nullptr, nullptr, nullptr);
        if (rc_tr != SQLITE_OK) {
            std::cerr << "SQLite error (trades_raw): "
                      << sqlite3_errmsg(db_) << std::endl;
        }
    } else {
        // not a valid trade → don't write to trades_raw or ticks_live
        return;
    }

    // ===========================
    // C) ENRICHED TICKS → ticks_live
    //     (only valid trades reach here)
    // ===========================
    std::ostringstream sql;
    sql << "INSERT OR REPLACE INTO ticks_live VALUES("
        << ts << ", '" << t.instrument << "', "
        << px << ", " << qty << ", "
        << bestBid << ", " << bestAsk << ", "
        << mid << ", " << sv(I.spread) << ", "
        << sv(I.ema12) << ", " << sv(I.ema26) << ", "
        << sv(I.macd) << ", " << sv(I.rsi14) << ", "
        << sv(I.bb_mid) << ", " << sv(I.bb_up) << ", " << sv(I.bb_low) << ", "
        << sv(I.vwap) << ", " << sv(vol) << ", "
        << sv(I.bid_vol_1) << ", " << sv(I.ask_vol_1) << ", "
        << sv(I.bid_vol_5) << ", " << sv(I.ask_vol_5) << ", "
        << sv(I.order_imbalance) << ", " << sv(I.ofi) << ", "
        << sv(I.return_1s) << ", " << sv(I.return_5s) << ", "
        << sv(I.realized_vol_1s) << ", "
        << sv(I.feed_latency_ms) << ", " << sv(I.queue_latency_us) << ", "
        << sv(I.tick_gap_ms) << ", " << sv(I.tick_rate) << ", "
        << sv(I.vol_zscore) << ", "
        << static_cast<int>(I.hour_of_day) << ", "
        << static_cast<int>(I.minute_of_day) << ", "
        << (large ? 1 : 0) << ", "
        << I.trend_label << ", "
        << "'" << exchange_ << "');";

    auto t0 = std::chrono::steady_clock::now();
    int rc = sqlite3_exec(db_, sql.str().c_str(), nullptr, nullptr, nullptr);
    auto t1 = std::chrono::steady_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    if (rc != SQLITE_OK) {
        std::cerr << "SQLite error (ticks_live): "
                  << sqlite3_errmsg(db_) << std::endl;
    }

    std::cout << "[DB INSERT LATENCY] "
              << micros << " µs"
              << " | Instrument: " << t.instrument
              << " | Price: " << px
              << " | Exchange: " << exchange_
              << std::endl;
}
