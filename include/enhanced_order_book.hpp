#pragma once
#include "order_book.hpp"
#include "tick_data.hpp"
#include <vector>
#include <deque>

class EnhancedOrderBook : public OrderBook {
private:
    std::deque<Tick> recent_ticks_;
    size_t max_ticks_{10000};
    
    uint64_t last_trade_timestamp_{0};
    double last_trade_price_{0.0};
    double volume_24h_{0.0};
    mutable std::mutex mtx_;
public:
    EnhancedOrderBook(const std::string& instrument);
    
    void add_tick(const Tick& tick);
    double calculate_vwap(size_t lookback_ticks = 100) const;
    double calculate_momentum(int lookback_ticks = 10) const;
    std::vector<Tick> get_recent_ticks(size_t count) const;
    
    uint64_t get_last_trade_time() const { return last_trade_timestamp_; }
    double get_last_trade_price() const { return last_trade_price_; }
    double get_24h_volume() const { return volume_24h_; }
};