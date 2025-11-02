#pragma once
#include <cstdint>
#include <string>
#include <chrono>
#include <vector>
#include <atomic>
#include <nlohmann/json.hpp>

// In tick_data.hpp - REPLACE the namespace with enum class
enum class TickType : int{
    TRADE = 1,
    BID = 2, 
    ASK = 3,
    ORDERBOOK_SNAPSHOT = 4,
    ORDERBOOK_DELTA = 5
};

struct Tick {
    // These fields stay the same
    uint64_t timestamp_ns;
    uint64_t exchange_timestamp;
    TickType type;  // Now using enum class
    
    std::string instrument;
    double price;
    double quantity;
    std::string side;
    std::string trade_id;
    uint64_t sequence;
    std::string channel;
    uint64_t enqueue_time_ns;
    // FIXED constructor - use default TRADE type
    Tick() : timestamp_ns(0), exchange_timestamp(0), 
             type(TickType::TRADE),  // Use enum value
             price(0.0), quantity(0.0), sequence(0) {}
    // === JSON Serialization for ZeroMQ transfer ===
    nlohmann::json to_json() const {
        return {
            {"timestamp_ns", timestamp_ns},
            {"exchange_timestamp", exchange_timestamp},
            {"type", static_cast<int>(type)},
            {"instrument", instrument},
            {"price", price},
            {"quantity", quantity},
            {"side", side},
            {"trade_id", trade_id},
            {"sequence", sequence},
            {"channel", channel},
            {"enqueue_time_ns", enqueue_time_ns}
        };
    }

    static Tick from_json(const nlohmann::json& j) {
        Tick t;
        t.timestamp_ns = j.value("timestamp_ns", 0ull);
        t.exchange_timestamp = j.value("exchange_timestamp", 0ull);
        t.type = static_cast<TickType>(j.value("type", 1));
        t.instrument = j.value("instrument", "");
        t.price = j.value("price", 0.0);
        t.quantity = j.value("quantity", 0.0);
        t.side = j.value("side", "");
        t.trade_id = j.value("trade_id", "");
        t.sequence = j.value("sequence", 0ull);
        t.channel = j.value("channel", "");
        t.enqueue_time_ns = j.value("enqueue_time_ns", 0ull);
        return t;
    }
};
class TickBuffer {
private:
    std::vector<Tick> buffer_;
    size_t capacity_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
    std::atomic<bool> full_{false};
    
public:
    TickBuffer(size_t size = 1000000);
    
    bool push(const Tick& tick);
    bool pop(Tick& tick);
    bool empty() const;
    size_t size() const;
    size_t capacity() const { return capacity_ - 1; }
};