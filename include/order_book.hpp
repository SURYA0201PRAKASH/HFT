#pragma once
#include <vector>
#include <array>
#include <atomic>
#include <string>
#include <cstdint>
#include "nlohmann/json.hpp"
#include <mutex>
double safe_get_double(const nlohmann::json& j);
uint64_t safe_get_uint64(const nlohmann::json& j);
class SequenceManager;
struct OrderBookLevel {
    double price;
    double amount;
    uint64_t timestamp;
    
    OrderBookLevel() : price(0.0), amount(0.0), timestamp(0) {}
    OrderBookLevel(double p, double a, uint64_t ts) : price(p), amount(a), timestamp(ts) {}
    
    bool operator<(const OrderBookLevel& other) const { return price < other.price; }
    bool operator>(const OrderBookLevel& other) const { return price > other.price; }
};

class OrderBook {
public:
    OrderBook(const std::string& instrument);
    ~OrderBook();
    void update_from_snapshot(const nlohmann::json& data);
    void apply_delta_update(const nlohmann::json& data);
    void clear();
    
    double get_bid() const;
    double get_ask() const;
    double get_mid_price() const;
    double get_spread() const;
    
    std::vector<OrderBookLevel> get_top_bids(size_t count = 5) const;
    std::vector<OrderBookLevel> get_top_asks(size_t count = 5) const;
    
    std::string to_string() const;
    nlohmann::json to_json() const;
    
    std::string get_instrument() const { return instrument_; }
    uint64_t get_sequence() const { return sequence_.load(std::memory_order_acquire); }
    uint64_t get_change_number() const { return change_number_.load(std::memory_order_acquire); }
    double get_best_bid_price() const;
    double get_best_ask_price() const;
    double get_best_bid_size() const;
    double get_best_ask_size() const;
    double compute_imbalance() const;
	bool validate_sequence(uint64_t incoming_prev, uint64_t incoming_current,
                          std::function<void(uint64_t, uint64_t)> request_callback = nullptr);
    void request_snapshot();
	void check_and_request_missing_sequences(std::function<void(uint64_t, uint64_t)> request_callback);
    
    SequenceManager* get_sequence_manager() { return sequence_manager_; }
private:
    std::string instrument_;
    std::atomic<uint64_t> sequence_{0};
    std::atomic<uint64_t> change_number_{0};
    std::atomic<uint64_t> timestamp_{0};
    std::atomic<uint64_t> expected_sequence_{0};
    std::atomic<uint64_t> gap_detection_time_{0};
    std::atomic<bool> needs_snapshot_{false};
    mutable std::mutex sequence_mutex_;
    static constexpr size_t DEPTH = 25;
    std::array<OrderBookLevel, DEPTH> bids_;
    std::array<OrderBookLevel, DEPTH> asks_;
    SequenceManager* sequence_manager_;
    mutable std::atomic<bool> update_in_progress_{false};
    
    void sort_and_truncate();
    int find_price_level(double price, bool is_bid) const;
};