#pragma once
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>

// Represents one unified snapshot of the market state
struct MarketState {
    // --- Identification ---
    std::string instrument;                     // e.g., "BTC-PERPETUAL"
    std::chrono::system_clock::time_point ts;   // event timestamp

    // --- Raw microstructure features ---
    double mid_price   = 0.0;   // (best bid + best ask)/2
    double spread      = 0.0;   // (ask - bid)
    double depth_imb   = 0.0;   // order book imbalance
    double ofi         = 0.0;   // order flow imbalance
    double volatility  = 0.0;   // rolling volatility estimate
    double return_1s   = 0.0;   // short-term return (%)

    // --- Normalized (z-score) features ---
    double z_spread    = 0.0;
    double z_depth_imb = 0.0;
    double z_ofi       = 0.0;
    double z_vol       = 0.0;
    double z_ret       = 0.0;

    // --- Optional derived metrics for AI/agents ---
    double reward      = 0.0;   // placeholder for RL reward
    int position       = 0;     // current position (long/short/flat)

    // Utility: formatted timestamp string
    std::string time_string() const {
        auto t = std::chrono::system_clock::to_time_t(ts);
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&t), "%F %T");
        return oss.str();
    }
};
