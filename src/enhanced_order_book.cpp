#include "enhanced_order_book.hpp"
#include <algorithm>
#include <numeric>
#include <iostream>
#include <stdexcept>

// enhanced_order_book.cpp - constructor
EnhancedOrderBook::EnhancedOrderBook(const std::string& instrument) 
    : OrderBook(instrument)  // Properly initialize base class
{
//     std::cout << "🆕 EnhancedOrderBook created for " << instrument 
//              << " (inherits from OrderBook)" << std::endl;
}


void EnhancedOrderBook::add_tick(const Tick& tick) {
	std::lock_guard<std::mutex> lk(mtx_);
    // Light debug (optional)
    /*std::cout << "🧩 [EOB::add_tick] inst=" << tick.instrument
              << " | type=" << static_cast<int>(tick.type)
              << " | px=" << tick.price
              << " | qty=" << tick.quantity
              << " | seq=" << tick.sequence
              << " | this=" << static_cast<const void*>(this)
              << std::endl;*/

    // 1) Buffer the tick for analytics
    recent_ticks_.push_back(tick);
    if (recent_ticks_.size() > max_ticks_) {
        recent_ticks_.pop_front();
    }

    // 2) Update simple trade stats only (don’t touch book here)
    if (tick.type == TickType::TRADE && tick.price > 0 && tick.quantity > 0) {
        last_trade_price_ = tick.price;
        last_trade_timestamp_ = tick.timestamp_ns;
        volume_24h_ += tick.quantity;
    }

    // Note: OrderBook bids/asks are already updated upstream in OrderBookManager.
    // EnhancedOrderBook should read that state (get_bid/get_ask/get_mid_price)
    // for analytics, but must not call update_from_snapshot/apply_delta_update here.
}




double EnhancedOrderBook::calculate_vwap(size_t lookback_ticks) const {
	std::lock_guard<std::mutex> lk(mtx_);
//     std::cout << "\n=== VWAP DEBUG START ===" << std::endl;
//     std::cout << "Instrument: " << get_instrument() << std::endl;
//     std::cout << "Lookback ticks: " << lookback_ticks << std::endl;
    
    if (recent_ticks_.empty()) {
//         std::cout << "❌ No ticks available for VWAP" << std::endl;
//         std::cout << "=== VWAP DEBUG END ===\n" << std::endl;
        return get_mid_price();
    }
    
    double total_value = 0.0;
    double total_volume = 0.0;
    size_t trade_count = 0;
    
//     std::cout << "Processing trades for VWAP:" << std::endl;
    for (auto it = recent_ticks_.rbegin(); 
         it != recent_ticks_.rend() && trade_count < lookback_ticks; 
         ++it) {
        
        if (it->type == TickType::TRADE && it->price > 0 && it->quantity > 0) {
            double trade_value = it->price * it->quantity;
            total_value += trade_value;
            total_volume += it->quantity;
            trade_count++;
            
//             std::cout << "  Trade " << trade_count << ": " << it->price 
//                      << " x " << it->quantity << " = " << trade_value 
//                      << " (cumulative: " << total_value << " / " << total_volume << ")" << std::endl;
        }
    }
    
    if (total_volume > 0) {
        double vwap = total_value / total_volume;
//         std::cout << "💰 VWAP result: " << total_value << " / " << total_volume 
//                  << " = " << vwap << " (from " << trade_count << " trades)" << std::endl;
//         std::cout << "=== VWAP DEBUG END ===\n" << std::endl;
        return vwap;
    }
    
//     std::cout << "❌ No valid trades for VWAP calculation" << std::endl;
//     std::cout << "  Trades processed: " << trade_count << std::endl;
//     std::cout << "  Total volume: " << total_volume << std::endl;
//     std::cout << "=== VWAP DEBUG END ===\n" << std::endl;
    
    return get_mid_price();
}

double EnhancedOrderBook::calculate_momentum(int lookback_ticks) const {
	std::lock_guard<std::mutex> lk(mtx_);
//     std::cout << "\n=== MOMENTUM DEBUG START ===" << std::endl;
//     std::cout << "Instrument: " << get_instrument() << std::endl;
    
    // DEBUG: Check if we now have real order book data
    double current_bid = OrderBook::get_bid();
    double current_ask = OrderBook::get_ask(); 
    double current_mid = OrderBook::get_mid_price();
    
    // === ADD THIS CRITICAL DEBUG CODE ===
//     std::cout << "🔍 TICKTYPE CONSTANTS DEBUG:" << std::endl;
//     std::cout << "TickType::TRADE = " << static_cast<int>(TickType::TRADE) << std::endl;
//     std::cout << "TickType::ORDERBOOK_SNAPSHOT = " << static_cast<int>(TickType::ORDERBOOK_SNAPSHOT) << std::endl;
//     std::cout << "TickType::ORDERBOOK_DELTA = " << static_cast<int>(TickType::ORDERBOOK_DELTA) << std::endl;
    
    // Check actual tick values in buffer
//     std::cout << "🔍 ACTUAL TICK VALUES IN BUFFER (first 10):" << std::endl;
    int tick_num = 0;
    for (const auto& tick : recent_ticks_) {
        if (tick_num >= 10) break;
        
//         std::cout << "  Tick " << tick_num << ": type=" << static_cast<int>(tick.type) 
//                  << " | Match TRADE(" << static_cast<int>(TickType::TRADE) << "): " 
//                  << (tick.type == TickType::TRADE ? "YES" : "NO")
//                  << " | Match SNAPSHOT(" << static_cast<int>(TickType::ORDERBOOK_SNAPSHOT) << "): " 
//                  << (tick.type == TickType::ORDERBOOK_SNAPSHOT ? "YES" : "NO")
//                  << " | Match DELTA(" << static_cast<int>(TickType::ORDERBOOK_DELTA) << "): " 
//                  << (tick.type == TickType::ORDERBOOK_DELTA ? "YES" : "NO")
//                  << " | Price: " << tick.price << std::endl;
        tick_num++;
    }
    // === END CRITICAL DEBUG CODE ===
    
//     std::cout << "🔧 SYNC CHECK - Bid: " << current_bid 
//              << ", Ask: " << current_ask 
//              << ", Mid: " << current_mid << std::endl;
    
    // Check if parent OrderBook has data
    auto top_bids = OrderBook::get_top_bids(1);
    auto top_asks = OrderBook::get_top_asks(1);
    
//     std::cout << "Parent OrderBook state - Bids: " << top_bids.size() 
//              << ", Asks: " << top_asks.size() << std::endl;
    
    if (!top_bids.empty()) {
//         std::cout << "Best bid from parent: " << top_bids[0].price << " x " << top_bids[0].amount << std::endl;
    }
    if (!top_asks.empty()) {
//         std::cout << "Best ask from parent: " << top_asks[0].price << " x " << top_asks[0].amount << std::endl;
    }
    
    if (current_mid <= 0) {
//         std::cout << "❌ SYNC FAILED: EnhancedOrderBook has no order book data!" << std::endl;
//         std::cout << "=== MOMENTUM DEBUG END ===\n" << std::endl;
        return 0.0;
    }
    
//     std::cout << "✅ SYNC SUCCESS: EnhancedOrderBook has real market data" << std::endl;
    
    // === REMOVE THE DUPLICATE CODE BELOW - IT'S ALREADY ABOVE ===
//     std::cout << "Lookback ticks: " << lookback_ticks << std::endl;
//     std::cout << "Total ticks in buffer: " << recent_ticks_.size() << std::endl;
    
    // Count different tick types (REMOVE THE DUPLICATE DECLARATION)
    int trade_ticks = 0, snapshot_ticks = 0, delta_ticks = 0;
    for (const auto& tick : recent_ticks_) {
        if (tick.type == TickType::TRADE) trade_ticks++;
        else if (tick.type == TickType::ORDERBOOK_SNAPSHOT) snapshot_ticks++;
        else if (tick.type == TickType::ORDERBOOK_DELTA) delta_ticks++;
    }
//     std::cout << "Tick types - Trades: " << trade_ticks 
//              << ", Snapshots: " << snapshot_ticks 
//              << ", Deltas: " << delta_ticks << std::endl;
    
    // Check current price
    double current_price = get_mid_price();
//     std::cout << "Current mid price: " << current_price << std::endl;
//     std::cout << "Best bid: " << get_bid() << ", Best ask: " << get_ask() << std::endl;
    
    if (recent_ticks_.empty()) {
//         std::cout << "❌ No ticks available for momentum calculation" << std::endl;
//         std::cout << "=== MOMENTUM DEBUG END ===\n" << std::endl;
        return 0.0;
    }
    
    if (current_price <= 0) {
//         std::cout << "❌ Invalid current price: " << current_price << std::endl;
//         std::cout << "=== MOMENTUM DEBUG END ===\n" << std::endl;
        return 0.0;
    }
    
    // Show recent trades
//     std::cout << "Recent trades (last 10):" << std::endl;
    int trade_count = 0;
    for (auto it = recent_ticks_.rbegin(); it != recent_ticks_.rend() && trade_count < 10; ++it) {
        if (it->type == TickType::TRADE && it->price > 0) {
//             std::cout << "  Trade " << ++trade_count << ": " << it->price 
//                      << " (age: " << std::distance(recent_ticks_.rbegin(), it) << " ticks ago)" << std::endl;
        }
    }
    
    if (trade_count == 0) {
//         std::cout << "  No trade ticks found in buffer!" << std::endl;
    }
    
    // Try to find past price for momentum calculation
    double past_price = 0.0;
    int ticks_back = 0;
    
//     std::cout << "Searching for past price (lookback=" << lookback_ticks << "):" << std::endl;
    for (auto it = recent_ticks_.rbegin(); it != recent_ticks_.rend(); ++it) {
        if (it->type == TickType::TRADE && it->price > 0) {
//             std::cout << "  Found trade at position " << ticks_back << ": " << it->price;
            
            if (ticks_back >= lookback_ticks) {
                past_price = it->price;
//                 std::cout << " ✅ SELECTED (meets lookback requirement)" << std::endl;
                break;
            } else {
//                 std::cout << " ❌ Too recent (need " << (lookback_ticks - ticks_back) << " more ticks)" << std::endl;
            }
            ticks_back++;
        }
    }
    
    if (past_price <= 0) {
//         std::cout << "❌ No suitable past price found within lookback period" << std::endl;
//         std::cout << "  Total trade ticks checked: " << ticks_back << std::endl;
        
        // Try using the oldest available trade
        for (const auto& tick : recent_ticks_) {
            if (tick.type == TickType::TRADE && tick.price > 0) {
                past_price = tick.price;
//                 std::cout << "⚠️ Using oldest available trade: " << past_price << std::endl;
                break;
            }
        }
    }
    
    // Calculate momentum
    if (past_price > 0 && current_price > 0) {
        double momentum = (current_price - past_price) / past_price;
        double momentum_bps = momentum * 10000;
        
//         std::cout << "💰 Momentum calculation:" << std::endl;
//         std::cout << "  Past price: " << past_price << std::endl;
//         std::cout << "  Current price: " << current_price << std::endl;
//         std::cout << "  Difference: " << (current_price - past_price) << std::endl;
//         std::cout << "  Momentum: " << momentum << " (" << momentum_bps << " bps)" << std::endl;
//         std::cout << "=== MOMENTUM DEBUG END ===\n" << std::endl;
        std::cout << "📈 " << get_instrument() 
              << " Momentum: " << (momentum * 10000) << " bps" 
              << " | Trades in buffer: " << std::count_if(recent_ticks_.begin(), recent_ticks_.end(),
                    [](const Tick& t) { return t.type == TickType::TRADE; })
              << std::endl;
        return momentum;
    }
    
//     std::cout << "❌ Final: Cannot calculate momentum" << std::endl;
//     std::cout << "  Past price: " << past_price << std::endl;
//     std::cout << "  Current price: " << current_price << std::endl;
//     std::cout << "=== MOMENTUM DEBUG END ===\n" << std::endl;
    
    return 0.0;
}

std::vector<Tick> EnhancedOrderBook::get_recent_ticks(size_t count) const {
	std::lock_guard<std::mutex> lk(mtx_);
    std::vector<Tick> result;
    if (recent_ticks_.empty()) return result;
    
    size_t num_ticks = std::min(count, recent_ticks_.size());
    auto start_it = recent_ticks_.end() - num_ticks;
    
    for (auto it = start_it; it != recent_ticks_.end(); ++it) {
        result.push_back(*it);
    }
    
    return result;
}