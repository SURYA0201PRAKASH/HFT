#include "order_book.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <cmath>
#include <string>
#include "sequence_manager.hpp"

OrderBook::OrderBook(const std::string& instrument) 
    : instrument_(instrument) {
    clear();
}
OrderBook::~OrderBook() {
    delete sequence_manager_; // Clean up pointer
}
void OrderBook::clear() {
    bids_.fill(OrderBookLevel());
    asks_.fill(OrderBookLevel());
}

void OrderBook::update_from_snapshot(const nlohmann::json& data) {
    bool expected = false;
    while (!update_in_progress_.compare_exchange_weak(expected, true, 
                                                     std::memory_order_acq_rel)) {
        expected = false;
    }
    
    try {
        clear();
        if (data.contains("change_id")) {
            uint64_t snapshot_seq = safe_get_uint64(data["change_id"]);
            expected_sequence_.store(snapshot_seq, std::memory_order_release);
            sequence_.store(snapshot_seq, std::memory_order_release);
            //std::cout << "📋 SNAPSHOT LOADED for " << instrument_ 
            //          << " - Sequence: " << snapshot_seq << std::endl;
        }
        if (data.contains("prev_change_id")) {
            sequence_.store(safe_get_uint64(data["prev_change_id"]), std::memory_order_release);
        }
        if (data.contains("timestamp")) {
            timestamp_.store(safe_get_uint64(data["timestamp"]), std::memory_order_release);
        }
        
        uint64_t current_ts = timestamp_.load(std::memory_order_acquire);
        
        if (data.contains("bids") && data["bids"].is_array()) {
            size_t i = 0;
            for (const auto& bid : data["bids"]) {
                if (i >= DEPTH) break;
                if (bid.is_array() && bid.size() >= 3) {
                    // Handle Deribit format: ["new", price, amount]
                    std::string action = bid[0].get<std::string>();
                    double price = safe_get_double(bid[1]);
                    double amount = safe_get_double(bid[2]);
                    
                    if (action == "new" || action == "change") {
                        bids_[i] = OrderBookLevel(price, amount, current_ts);
                        i++;
                    }
                    // Ignore "delete" actions in snapshots
                }
            }
        }
        
        if (data.contains("asks") && data["asks"].is_array()) {
            size_t i = 0;
            for (const auto& ask : data["asks"]) {
                if (i >= DEPTH) break;
                if (ask.is_array() && ask.size() >= 3) {
                    // Handle Deribit format: ["new", price, amount]
                    std::string action = ask[0].get<std::string>();
                    double price = safe_get_double(ask[1]);
                    double amount = safe_get_double(ask[2]);
                    
                    if (action == "new" || action == "change") {
                        asks_[i] = OrderBookLevel(price, amount, current_ts);
                        i++;
                    }
                    // Ignore "delete" actions in snapshots
                }
            }
        }
        
        sort_and_truncate();
        change_number_.fetch_add(1, std::memory_order_release);
        
    } catch (const std::exception& e) {
        std::cerr << "Error parsing order book snapshot: " << e.what() << std::endl;
    }
    
    update_in_progress_.store(false, std::memory_order_release);
}

void OrderBook::apply_delta_update(const nlohmann::json& data) {
	
    bool expected = false;
    while (!update_in_progress_.compare_exchange_weak(expected, true, 
                                                     std::memory_order_acq_rel)) {
        expected = false;
    }
    
    try {
		uint64_t incoming_prev = 0;
        uint64_t incoming_current = 0;
        
        if (data.contains("prev_change_id")) {
            incoming_prev = safe_get_uint64(data["prev_change_id"]);
        }
        if (data.contains("change_id")) {
            incoming_current = safe_get_uint64(data["change_id"]);
        }
        
        // VALIDATE SEQUENCE BEFORE PROCESSING
        if (!validate_sequence(incoming_prev, incoming_current)) {
            std::cout << "❌ REJECTING UPDATE due to sequence issue" << std::endl;
            update_in_progress_.store(false, std::memory_order_release);
            return;
        }
        if (data.contains("prev_change_id")) {
            sequence_.store(safe_get_uint64(data["prev_change_id"]), std::memory_order_release);
        }
        if (data.contains("timestamp")) {
            timestamp_.store(safe_get_uint64(data["timestamp"]), std::memory_order_release);
        }
        
        uint64_t current_ts = timestamp_.load(std::memory_order_acquire);
        
        if (data.contains("bids") && data["bids"].is_array()) {
            for (const auto& bid : data["bids"]) {
                if (bid.is_array() && bid.size() >= 3) {
                    std::string action = bid[0].get<std::string>();
                    double price = safe_get_double(bid[1]);
                    double amount = safe_get_double(bid[2]);
                    
                    if (action == "delete" || amount == 0.0) {
                        int index = find_price_level(price, true);
                        if (index != -1) {
                            bids_[index] = OrderBookLevel();
                        }
                    } else if (action == "new" || action == "change") {
                        int index = find_price_level(price, true);
                        if (index != -1) {
                            bids_[index] = OrderBookLevel(price, amount, current_ts);
                        } else {
                            for (size_t i = 0; i < DEPTH; i++) {
                                if (bids_[i].price == 0.0 || bids_[i].price < price) {
                                    bids_[i] = OrderBookLevel(price, amount, current_ts);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Similar fix for asks...
        if (data.contains("asks") && data["asks"].is_array()) {
            for (const auto& ask : data["asks"]) {
                if (ask.is_array() && ask.size() >= 3) {
                    std::string action = ask[0].get<std::string>();
                    double price = safe_get_double(ask[1]);
                    double amount = safe_get_double(ask[2]);
                    
                    if (action == "delete" || amount == 0.0) {
                        int index = find_price_level(price, false);
                        if (index != -1) {
                            asks_[index] = OrderBookLevel();
                        }
                    } else if (action == "new" || action == "change") {
                        int index = find_price_level(price, false);
                        if (index != -1) {
                            asks_[index] = OrderBookLevel(price, amount, current_ts);
                        } else {
                            for (size_t i = 0; i < DEPTH; i++) {
                                if (asks_[i].price == 0.0 || asks_[i].price > price) {
                                    asks_[i] = OrderBookLevel(price, amount, current_ts);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        sort_and_truncate();
        change_number_.fetch_add(1, std::memory_order_release);
        
    } catch (const std::exception& e) {
        std::cerr << "Error applying delta update: " << e.what() << std::endl;
    }
    
    update_in_progress_.store(false, std::memory_order_release);
}

void OrderBook::sort_and_truncate() {
    std::sort(bids_.begin(), bids_.end(), [](const OrderBookLevel& a, const OrderBookLevel& b) {
        return a.price > b.price && a.price != 0.0;
    });
    
    std::sort(asks_.begin(), asks_.end(), [](const OrderBookLevel& a, const OrderBookLevel& b) {
        return a.price < b.price && a.price != 0.0;
    });
}

int OrderBook::find_price_level(double price, bool is_bid) const {
    const auto& levels = is_bid ? bids_ : asks_;
    for (size_t i = 0; i < DEPTH; i++) {
        if (std::abs(levels[i].price - price) < 1e-10) {
            return i;
        }
    }
    return -1;
}

double OrderBook::get_bid() const {
    return bids_[0].price > 0.0 ? bids_[0].price : 0.0;
}

double OrderBook::get_ask() const {
    return asks_[0].price > 0.0 ? asks_[0].price : 0.0;
}

double OrderBook::get_mid_price() const {
    double bid = get_bid();
    double ask = get_ask();
    return (bid > 0.0 && ask > 0.0) ? (bid + ask) / 2.0 : 0.0;
}

double OrderBook::get_spread() const {
    double bid = get_bid();
    double ask = get_ask();
    return (bid > 0.0 && ask > 0.0) ? ask - bid : 0.0;
}

std::vector<OrderBookLevel> OrderBook::get_top_bids(size_t count) const {
    std::vector<OrderBookLevel> result;
    for (size_t i = 0; i < std::min(count, DEPTH); i++) {
        if (bids_[i].price > 0.0) {
            result.push_back(bids_[i]);
        }
    }
    return result;
}

std::vector<OrderBookLevel> OrderBook::get_top_asks(size_t count) const {
    std::vector<OrderBookLevel> result;
    for (size_t i = 0; i < std::min(count, DEPTH); i++) {
        if (asks_[i].price > 0.0) {
            result.push_back(asks_[i]);
        }
    }
    return result;
}

std::string OrderBook::to_string() const {
    std::stringstream ss;
    ss << std::fixed << std::setprecision(2);
    ss << "OrderBook " << instrument_ << " (seq: " << sequence_.load() << ")\n";
    
    ss << "ASKS:\n";
    auto top_asks = get_top_asks(5);
    for (auto it = top_asks.rbegin(); it != top_asks.rend(); ++it) {
        ss << "  " << it->price << " x " << it->amount << "\n";
    }
    
    ss << "--- " << get_mid_price() << " (spread: " << get_spread() << ") ---\n";
    
    ss << "BIDS:\n";
    for (const auto& bid : get_top_bids(5)) {
        ss << "  " << bid.price << " x " << bid.amount << "\n";
    }
    
    return ss.str();
}

nlohmann::json OrderBook::to_json() const {
    nlohmann::json result;
    result["instrument"] = instrument_;
    result["sequence"] = sequence_.load();
    result["timestamp"] = timestamp_.load();
    result["change_number"] = change_number_.load();
    
    nlohmann::json bids_json = nlohmann::json::array();
    for (const auto& bid : get_top_bids(DEPTH)) {
        if (bid.price > 0.0) {
            bids_json.push_back({bid.price, bid.amount, bid.timestamp});
        }
    }
    result["bids"] = bids_json;
    
    nlohmann::json asks_json = nlohmann::json::array();
    for (const auto& ask : get_top_asks(DEPTH)) {
        if (ask.price > 0.0) {
            asks_json.push_back({ask.price, ask.amount, ask.timestamp});
        }
    }
    result["asks"] = asks_json;
    
    return result;
}




uint64_t safe_get_uint64(const nlohmann::json& j) {
    
    if (j.is_number()) {
        if (j.is_number_unsigned()) {
            uint64_t result = j.get<uint64_t>();
            return result;
        } else {
            int64_t signed_val = j.get<int64_t>();
            uint64_t result = (signed_val < 0) ? 0 : static_cast<uint64_t>(signed_val);
            return result;
        }
    } else if (j.is_string()) {
        try {
            std::string str_val = j.get<std::string>();
            uint64_t result = std::stoull(str_val);
            return result;
        } catch (const std::exception& e) {
            return 0;
        }
    }
    
    return 0;
}

double OrderBook::get_best_bid_price() const {
    return (bids_[0].price > 0.0) ? bids_[0].price : 0.0;
}

double OrderBook::get_best_ask_price() const {
    return (asks_[0].price > 0.0) ? asks_[0].price : 0.0;
}

double OrderBook::get_best_bid_size() const {
    return (bids_[0].price > 0.0) ? bids_[0].amount : 0.0;
}

double OrderBook::get_best_ask_size() const {
    return (asks_[0].price > 0.0) ? asks_[0].amount : 0.0;
}

double OrderBook::compute_imbalance() const {
    double bidSize = get_best_bid_size();
    double askSize = get_best_ask_size();
    if (bidSize + askSize == 0.0) return 0.0;
    return (bidSize - askSize) / (bidSize + askSize);
}

// Helper function for safe JSON parsing
double safe_get_double(const nlohmann::json& j) {
    if (j.is_number()) {
        return j.get<double>();
    } else if (j.is_string()) {
        try {
            return std::stod(j.get<std::string>());
        } catch (const std::exception& e) {
            return 0.0;
        }
    }
    return 0.0;
}
bool OrderBook::validate_sequence(uint64_t incoming_prev, uint64_t incoming_current,
                                 std::function<void(uint64_t, uint64_t)> request_callback) {
    uint64_t expected = expected_sequence_.load(std::memory_order_acquire);
    static std::atomic<int> validation_count1{0};
    int count = validation_count1.fetch_add(1);
    
    if (request_callback && sequence_manager_) {
        sequence_manager_->set_last_valid_sequence(expected);
        sequence_manager_->on_sequence_received(incoming_current, request_callback);
    }
    static int msg_count = 0;
    /*if (++msg_count % 1000 == 0) {
        incoming_prev += 5; // Simulate 5 missing messages
    }*/
    // Case 1: Perfect sequence match
    if (incoming_prev == expected) {
		//std::cout << "NO SEQUENCE GAP DETECTED for " << instrument_ << std::endl;
        //std::cout << "   Expected: " << expected << std::endl;
        //std::cout << "   Received: " << incoming_prev << std::endl;
        //std::cout << "   Current time: " << std::chrono::duration_cast<std::chrono::milliseconds>(
        //    std::chrono::system_clock::now().time_since_epoch()).count() << std::endl;
        expected_sequence_.store(incoming_current, std::memory_order_release);
        return true;
    }
    // Case 2: Sequence gap (we missed some messages)
    else if (incoming_prev > expected) {
        uint64_t gap_size = incoming_prev - expected;
        std::cout << "🚨 SEQUENCE GAP DETECTED for " << instrument_ << std::endl;
        std::cout << "   Expected: " << expected << std::endl;
        std::cout << "   Received: " << incoming_prev << std::endl;
        std::cout << "   Gap size: " << gap_size << " messages" << std::endl;
        std::cout << "   Current time: " << std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() << std::endl;
        
        needs_snapshot_.store(true, std::memory_order_release);
        return false;
    }
    // Case 3: Out-of-order or duplicate message
    else {
        std::cout << "⚠️ OUT-OF-ORDER MESSAGE for " << instrument_ << std::endl;
        return false;
    }
}

void OrderBook::check_and_request_missing_sequences(std::function<void(uint64_t, uint64_t)> request_callback) {
    if (request_callback && sequence_manager_) {
        sequence_manager_->check_and_request_missing(request_callback);
    }
}