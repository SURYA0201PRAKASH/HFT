#include "strategy_engine.hpp"
#include <iostream>
#include <numeric>
#include <cmath>
#include <thread>
#include <chrono>
#include <sqlite3.h>

StrategyEngine::StrategyEngine(std::string symbol,
                               double spread_bps,
                               std::size_t window,
                               double z_threshold,
                               double trade_qty)
    : symbol_(std::move(symbol)),
      spread_bps_(spread_bps),
      window_size_(window),
      z_threshold_(z_threshold),
      trade_qty_(trade_qty) {}

double StrategyEngine::average(const std::deque<double>& v) {
    return std::accumulate(v.begin(), v.end(), 0.0) / (double)v.size();
}

void StrategyEngine::generate_quotes(double mid, double z) {
    double spread = mid * (spread_bps_ / 10000.0);
    double bid = mid - spread * 0.5;
    double ask = mid + spread * 0.5;
    std::cout << "Mid=" << mid << "  z=" << z
              << "  → Bid=" << bid << "  Ask=" << ask << '\n';
}

void StrategyEngine::simulate_fill(double mid) {
    if (window_.empty()) return;
    double mean = average(window_);

    // toy fill model:
    // if below mean by threshold → buy; if above by threshold → sell (and only sell if long)
    if (mid < mean * (1.0 - z_threshold_) ) {
        position_ += trade_qty_;
        pnl_      -= trade_qty_ * mid;
        std::cout << "🟢 BUY @" << mid << "  Pos=" << position_ << "  PnL=" << pnl_ << '\n';
    } else if (mid > mean * (1.0 + z_threshold_) && position_ > 0.0) {
        position_ -= trade_qty_;
        pnl_      += trade_qty_ * mid;
        std::cout << "🔴 SELL@" << mid << "  Pos=" << position_ << "  PnL=" << pnl_ << '\n';
    }
}

void StrategyEngine::on_tick(const Tick& t) {
    if (t.price <= 0.0 || std::isnan(t.price)) return;

    window_.push_back(t.price);
    if (window_.size() > window_size_) window_.pop_front();

    if (window_.size() == window_size_) {
        double mean = average(window_);
        double z = (t.price - mean) / mean; // normalized deviation
        generate_quotes(t.price, z);
        simulate_fill(t.price);
    }
}

void StrategyEngine::run_backtest(const std::string& db_path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        std::cerr << "❌ cannot open db: " << db_path << '\n';
        return;
    }
    const char* sql =
        "SELECT instrument, trade_price, trade_qty, timestamp_ms "
        "FROM ticks_live "
        "ORDER BY timestamp_ms ASC;";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "❌ bad SQL\n";
        sqlite3_close(db);
        return;
    }

    Tick tick;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        tick.instrument   = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        tick.price        = sqlite3_column_double(stmt, 1);
        tick.quantity     = sqlite3_column_double(stmt, 2);
        long long ts_ms   = sqlite3_column_int64(stmt, 3);
        tick.timestamp_ns = (uint64_t)ts_ms * 1000000ULL;
        on_tick(tick);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    std::cout << "✅ Backtest complete | Final PnL=" << pnl_
              << "  Position=" << position_ << "\n";
}

void StrategyEngine::run_live_poll(const std::string& db_path, int interval_ms) {
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
        std::cerr << "❌ cannot open db: " << db_path << '\n';
        return;
    }

    const char* sql =
        "SELECT instrument, trade_price, trade_qty, timestamp_ms "
        "FROM ticks_live "
        "ORDER BY timestamp_ms DESC LIMIT 1;";

    while (true) {
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "❌ bad SQL\n";
            break;
        }

        if (sqlite3_step(stmt) == SQLITE_ROW) {
            Tick tick;
            tick.instrument   = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            tick.price        = sqlite3_column_double(stmt, 1);
            tick.quantity     = sqlite3_column_double(stmt, 2);
            long long ts_ms   = sqlite3_column_int64(stmt, 3);
            tick.timestamp_ns = (uint64_t)ts_ms * 1000000ULL;
            on_tick(tick);
        }
        sqlite3_finalize(stmt);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    sqlite3_close(db);
}
