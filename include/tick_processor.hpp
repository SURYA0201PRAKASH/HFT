#pragma once
#include "tick_data.hpp"
#include "order_book_manager.hpp"
#include <functional>
#include <atomic>
#include <thread>
#include <vector>
#include "config_loader.hpp"
#include "online_z.hpp"

#ifdef ZMQ_BUILD
#include <zmq.hpp>   // ✅ Required for ZeroMQ
#endif

class TickProcessor {
private:
    OrderBookManager& book_manager_;
    TickBuffer tick_buffer_;

    std::atomic<bool> running_{false};
    std::thread processor_thread_;

    std::vector<std::function<void(const Tick&)>> tick_callbacks_;

    std::atomic<uint64_t> ticks_processed_{0};
    std::atomic<uint64_t> ticks_dropped_{0};
    std::atomic<uint64_t> last_sequence_{0};

    std::atomic<uint64_t> min_latency_ns_{UINT64_MAX};
    std::atomic<uint64_t> max_latency_ns_{0};
    std::atomic<uint64_t> total_latency_ns_{0};
    std::atomic<uint64_t> latency_count_{0};

    void processing_loop();
    void process_single_tick(const Tick& tick);
    void update_latency_stats(uint64_t receive_time, uint64_t exchange_time);

public:
    TickProcessor(OrderBookManager& book_manager, size_t buffer_size = 1000000);
    ~TickProcessor();

    OrderBookManager& get_book_manager() { return book_manager_; }

    void start();
    void stop();
    void push_tick(const Tick& tick);

    void register_tick_callback(std::function<void(const Tick&)> callback);

    void print_stats() const;
    uint64_t get_ticks_processed() const { return ticks_processed_; }
    uint64_t get_ticks_dropped() const { return ticks_dropped_; }
    double get_avg_latency_ms() const;

    size_t get_buffer_size() const { return tick_buffer_.size(); }
    size_t get_buffer_capacity() const { return tick_buffer_.capacity(); }

    std::atomic<uint64_t> total_queue_latency_ns_{0};
    std::atomic<uint64_t> queue_latency_count_{0};
	// Average time (in microseconds) between tick enqueue → processing
	double get_avg_queue_latency_us() const {
    uint64_t count = queue_latency_count_.load(std::memory_order_acquire);
    if (count == 0) return 0.0;
    return (total_queue_latency_ns_.load(std::memory_order_acquire) / count) / 1000.0; // ns → µs
	}
// === Configuration & Normalization ===
private:
    Config cfg_;             // stores loaded YAML settings
    OnlineZ z_spread_{0.05};
    OnlineZ z_imb_{0.05};
    OnlineZ z_ofi_{0.05};
    OnlineZ z_vol_{0.05};
    OnlineZ z_ret_{0.05};

public:
    void set_config(const Config& cfg);

#ifdef ZMQ_BUILD
    // ✅ ZeroMQ Section
private:
    std::unique_ptr<std::thread> zmq_subscriber_thread_;
    std::atomic<bool> zmq_running_{false};
    zmq::context_t zmq_ctx_{1};
    zmq::socket_t zmq_push_{zmq_ctx_, zmq::socket_type::push};
    std::atomic<bool> zmq_push_active_{false};
    void zmq_subscriber_loop(const std::string& address);

public:
    // --- Existing Channels ---
    void start_subscriber(const std::string& address = "tcp://127.0.0.1:5555");
    void stop_subscriber();
    void publish_tick_to_orderbook(const Tick& tick);
    void start_publisher(const std::string& address = "tcp://127.0.0.1:6000");
    void stop_publisher();

    // --- ✅ NEW Analytics Channel (Module C) ---
private:
    zmq::socket_t zmq_push_analytics_{zmq_ctx_, zmq::socket_type::push};
    std::atomic<bool> zmq_push_analytics_active_{false};

public:
    void start_analytics_publisher(const std::string& address = "tcp://127.0.0.1:7000");
    void push_to_analytics(const Tick& tick);
    void stop_analytics_publisher();
#endif
};
