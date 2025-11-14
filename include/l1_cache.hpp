#pragma once
#include <string>
#include <unordered_map>

struct L1Cache {
    double best_bid = 0.0;
    double best_ask = 0.0;
    double bid_vol_1 = 0.0;
    double ask_vol_1 = 0.0;
};

// Global L1 cache used across modules
extern std::unordered_map<std::string, L1Cache> g_l1;
