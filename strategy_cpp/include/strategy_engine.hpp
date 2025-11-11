#pragma once
#include <deque>
#include <string>
#include <cstdint>

// Minimal tick model (uses mid price stored in trade_price)
struct Tick {
    std::string instrument;
    double      price       = 0.0;     // mid price
    double      quantity    = 0.0;
    uint64_t    timestamp_ns= 0;       // nanoseconds
};

class StrategyEngine {
public:
    StrategyEngine(std::string symbol,
                   double spread_bps   = 2.0,
                   std::size_t window  = 20,
                   double z_threshold  = 0.001,
                   double trade_qty    = 0.1);

    // Modes
    void run_backtest(const std::string& sqlite_db_path);          // replays all rows
    void run_live_poll(const std::string& sqlite_db_path,
                       int interval_ms = 1000);                    // polls newest row

private:
    // core
    void on_tick(const Tick& t);
    void generate_quotes(double mid, double z);
    void simulate_fill(double mid);   // simple fill model, updates PnL/position

    // utils
    static double average(const std::deque<double>& v);

    // config
    std::string symbol_;
    double      spread_bps_;
    std::size_t window_size_;
    double      z_threshold_;
    double      trade_qty_;

    // state
    std::deque<double> window_;
    double position_ = 0.0;   // units
    double pnl_      = 0.0;   // cash PnL only (unrealized ignored for demo)
};
