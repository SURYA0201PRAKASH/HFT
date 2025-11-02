#include "tick_processor.hpp"
#include <iostream>
#include <chrono>

#ifdef ZMQ_BUILD
#include <zmq.hpp>
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#endif


TickProcessor::TickProcessor(OrderBookManager& book_manager, size_t buffer_size)
    : book_manager_(book_manager), tick_buffer_(buffer_size) {}

TickProcessor::~TickProcessor() {
    stop();
}
#ifdef ZMQ_BUILD
// ========================
// PUSH socket publisher (TickProcessor → OrderBookManager)
// ========================
zmq::context_t push_ctx_(1);
std::unique_ptr<zmq::socket_t> push_socket_ = nullptr;

void TickProcessor::start_publisher(const std::string& address)
{
    push_socket_ = std::make_unique<zmq::socket_t>(push_ctx_, zmq::socket_type::push);
    push_socket_->set(zmq::sockopt::sndhwm, 100000);
    push_socket_->set(zmq::sockopt::linger, 0);

    try {
        push_socket_->connect(address);
        std::cout << "✅ [TickProcessor] ZMQ PUSH connected to " << address << std::endl;
    } catch (const zmq::error_t& e) {
        std::cerr << "❌ [TickProcessor] Failed to bind PUSH socket: " << e.what() << std::endl;
    }
}

void TickProcessor::stop_publisher()
{
    if (push_socket_) {
        push_socket_->close();
        push_socket_.reset();
        std::cout << "🧹 [TickProcessor] PUSH publisher closed" << std::endl;
    }
}
#endif

void TickProcessor::start() {
    running_.store(true, std::memory_order_release);
    processor_thread_ = std::thread(&TickProcessor::processing_loop, this);
#ifdef ZMQ_BUILD
    start_publisher("tcp://127.0.0.1:6000");
#endif
}

void TickProcessor::stop() {
    running_.store(false, std::memory_order_release);
    if (processor_thread_.joinable()) {
        processor_thread_.join();
    }
#ifdef ZMQ_BUILD
    stop_publisher();
#endif
}

void TickProcessor::push_tick(const Tick& tick) {
    Tick t = tick;

    // ✅ Mark enqueue time (when tick is added to buffer)
    t.enqueue_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (!tick_buffer_.push(t)) {
        ticks_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}


void TickProcessor::processing_loop() {
    Tick tick;
    auto last_stats_time = std::chrono::steady_clock::now();
    size_t total_cycles = 0;
    size_t empty_cycles = 0;
    
    while (running_.load(std::memory_order_acquire)) {
        total_cycles++;
        size_t processed_this_cycle = 0;
        
        while (tick_buffer_.pop(tick)) {
            process_single_tick(tick);
            ticks_processed_.fetch_add(1, std::memory_order_relaxed);
            processed_this_cycle++;
        }
        
        if (processed_this_cycle == 0) {
            empty_cycles++;
        }
        
        // Helpful debug instead of alarming starvation messages
        /*if (total_cycles % 1000 == 0) { // Every 1000 cycles (~100ms * 1000 = 100 seconds)
            std::cout << "📊 PROCESSOR STATS: " 
                      << "Cycles: " << total_cycles 
                      << " | Empty: " << empty_cycles 
                      << " (" << (empty_cycles * 100 / total_cycles) << "%)"
                      << " | Total processed: " << ticks_processed_.load() 
                      << " | Current buffer: " << tick_buffer_.size() 
                      << std::endl;
        }*/
        
        // Stats printing (every 5 seconds)
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(
            now - last_stats_time).count() >= 5) {
            
            print_stats();
            last_stats_time = now;
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    // Process remaining ticks before shutdown
    while (tick_buffer_.pop(tick)) {
        process_single_tick(tick);
    }
}

void TickProcessor::process_single_tick(const Tick& tick) {
    // ✅ 1. Measure queue latency (Ingest → Processing)
    uint64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (tick.enqueue_time_ns > 0 && now_ns > tick.enqueue_time_ns) {
        uint64_t queue_latency_ns = now_ns - tick.enqueue_time_ns;

        // Accumulate totals atomically
        total_queue_latency_ns_.fetch_add(queue_latency_ns, std::memory_order_relaxed);
        queue_latency_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // ✅ 2. Measure feed latency (Exchange → Ingest)
    update_latency_stats(tick.timestamp_ns, tick.exchange_timestamp);

    static int tick_count = 0;

    // Optional debug (every 100 ticks)
    /*
    if (tick_count++ % 100 == 0) {
        std::cout << "🔄 TickProcessor #" << tick_count 
                  << " | " << tick.instrument 
                  << " | Type: " << static_cast<int>(tick.type)
                  << std::endl;
    }
    */

    // ✅ 3. Forward tick to OrderBookManager directly (in-process path)
#if 0
    book_manager_.process_tick(tick);
#endif

#ifdef ZMQ_BUILD
    if (push_socket_) {
        nlohmann::json j = {
            {"instrument", tick.instrument},
            {"type", static_cast<int>(tick.type)},
            {"price", tick.price},
            {"quantity", tick.quantity},
            {"sequence", tick.sequence},
            {"exchange_ts_ns", tick.exchange_timestamp},
            {"timestamp_ns", tick.timestamp_ns}
        };

        std::string payload = j.dump();
        zmq::message_t msg(payload.begin(), payload.end());
        // non-blocking send; safe if receiver is slower (HWM will protect you)
        auto res = push_socket_->send(msg, zmq::send_flags::dontwait);

        static int dbg = 0;
        if ((dbg++ % 100) == 0) {
            std::cout << "📤 [TickProcessor→OrderBookManager] PUSH sent | "
                      << tick.instrument << " | Price=" << tick.price
                      << " | Qty=" << tick.quantity
                      << " | Seq=" << tick.sequence << std::endl;
        }
    }
#endif

    // ✅ 5. Notify registered callbacks (for analytics or strategy modules)
    for (const auto& callback : tick_callbacks_) {
        callback(tick);
    }

    // ✅ 6. Sequence tracking
    if (tick.sequence > last_sequence_.load(std::memory_order_acquire)) {
        last_sequence_.store(tick.sequence, std::memory_order_release);
    }

    // ✅ 7. Update stats
    ticks_processed_.fetch_add(1, std::memory_order_relaxed);
}



void TickProcessor::update_latency_stats(uint64_t receive_time_ns, uint64_t exchange_time_ns) {
    // Only calculate latency if we have valid timestamps
    if (exchange_time_ns == 0 || receive_time_ns == 0) return;
    
    // Ensure timestamps are reasonable (not from year 1970 or future)
    auto current_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    // Check if timestamps are within reasonable range (last 24 hours)
    bool receive_valid = (receive_time_ns > current_time - 24LL * 60 * 60 * 1000000000) && 
                        (receive_time_ns <= current_time);
    bool exchange_valid = (exchange_time_ns > current_time - 24LL * 60 * 60 * 1000000000) && 
                         (exchange_time_ns <= current_time);
    
    if (!receive_valid || !exchange_valid) {
        // Use simple offset if timestamps are invalid
        exchange_time_ns = receive_time_ns - 10000000; // 10ms default latency
    }
    
    // Calculate latency (ensure positive and reasonable)
    int64_t latency = static_cast<int64_t>(receive_time_ns) - static_cast<int64_t>(exchange_time_ns);
    
    // Only use reasonable latency values (0ms to 5 seconds)
    if (latency >= 0 && latency <= 5000000000LL) { // 0 to 5 seconds
        latency_count_.fetch_add(1, std::memory_order_relaxed);
        total_latency_ns_.fetch_add(latency, std::memory_order_relaxed);
        
        uint64_t current_min = min_latency_ns_.load(std::memory_order_relaxed);
        while (static_cast<uint64_t>(latency) < current_min && 
               !min_latency_ns_.compare_exchange_weak(current_min, latency, 
                                                     std::memory_order_relaxed)) {}
        
        uint64_t current_max = max_latency_ns_.load(std::memory_order_relaxed);
        while (static_cast<uint64_t>(latency) > current_max && 
               !max_latency_ns_.compare_exchange_weak(current_max, latency, 
                                                     std::memory_order_relaxed)) {}
    }
}

double TickProcessor::get_avg_latency_ms() const {
    uint64_t count = latency_count_.load(std::memory_order_acquire);
    if (count == 0) return 0.0;
    
    uint64_t total_ns = total_latency_ns_.load(std::memory_order_acquire);
    return (total_ns / count) / 1000000.0; // Convert ns to ms
}

void TickProcessor::print_stats() const {
    double avg_feed_latency_ms = get_avg_latency_ms();

    uint64_t q_count = queue_latency_count_.load(std::memory_order_acquire);
    double avg_queue_latency_us = (q_count == 0) ? 0.0 :
        (total_queue_latency_ns_.load(std::memory_order_acquire) / q_count) / 1000.0; // ns → µs

    std::cout << "📊 Tick Processor Stats - "
              << "Processed: " << ticks_processed_.load()
              << ", Dropped: " << ticks_dropped_.load()
              << ", Buffer: " << tick_buffer_.size() << "/" << tick_buffer_.capacity()
              << ", Avg Feed Latency: " << avg_feed_latency_ms << " ms"
              << ", Avg Queue Latency: " << avg_queue_latency_us << " µs"
              << std::endl;
}


void TickProcessor::register_tick_callback(std::function<void(const Tick&)> callback) {
    tick_callbacks_.push_back(callback);
}


#ifdef ZMQ_BUILD
#include <nlohmann/json.hpp>
using json = nlohmann::json;

// ✅ Start the internal ZeroMQ subscriber
void TickProcessor::start_subscriber(const std::string& address) {
    if (zmq_running_) {
        std::cout << "⚠️ ZMQ subscriber already running." << std::endl;
        return;
    }

    zmq_running_ = true;
    zmq_subscriber_thread_ = std::make_unique<std::thread>(
        [this, address]() { zmq_subscriber_loop(address); });

    std::cout << "✅ TickProcessor subscriber started on " << address << std::endl;
}

// ✅ Stop the subscriber cleanly
void TickProcessor::stop_subscriber() {
    zmq_running_ = false;

    if (zmq_subscriber_thread_ && zmq_subscriber_thread_->joinable())
        zmq_subscriber_thread_->join();

    std::cout << "🧹 ZMQ subscriber stopped cleanly." << std::endl;
}

// ✅ The background loop that listens for incoming messages
void TickProcessor::zmq_subscriber_loop(const std::string& address) {
    try {
        zmq::socket_t subscriber(zmq_ctx_, zmq::socket_type::sub);
        subscriber.set(zmq::sockopt::subscribe, "");
        subscriber.connect(address);

        std::cout << "🔗 Connected to ZMQ publisher at " << address << std::endl;

        while (zmq_running_) {
            zmq::message_t topic_msg;
            zmq::message_t payload_msg;

            // ✅ Use recv_result_t instead of bool
            auto topic_res = subscriber.recv(topic_msg, zmq::recv_flags::none);
            if (!topic_res.has_value() || !zmq_running_) continue;

            auto payload_res = subscriber.recv(payload_msg, zmq::recv_flags::none);
            if (!payload_res.has_value()) continue;

            std::string topic(static_cast<char*>(topic_msg.data()), topic_msg.size());
            std::string payload(static_cast<char*>(payload_msg.data()), payload_msg.size());

            try {
                json j = json::parse(payload);
                
                Tick tick;
                tick.instrument = j.value("instrument", topic);
                tick.price = j.value("price", 0.0);
                tick.quantity = j.value("quantity", 0.0);
                tick.sequence = j.value("sequence", 0);
                tick.exchange_timestamp = j.value("exchange_ts_ns", 0ULL);
                tick.timestamp_ns = j.value("ingress_ts_ns", 0ULL);
                tick.type = static_cast<TickType>(j.value("type", 0));

                push_tick(tick);

                static uint64_t tick_count = 0;
                if (++tick_count % 500 == 0) {
                    std::cout << "✅ TickProcessor received ZMQ tick | " << tick.instrument
                              << " | Price: " << tick.price
                              << " | Qty: " << tick.quantity
                              << " | Seq: " << tick.sequence << std::endl;
                }

            } catch (const std::exception& e) {
                std::cerr << "⚠️ ZMQ tick parse error: " << e.what()
                          << " | Topic: " << topic
                          << " | Payload (first 40 chars): " << payload.substr(0, 40)
                          << std::endl;
            }
        }

        subscriber.close();
    } catch (const std::exception& e) {
        std::cerr << "❌ ZMQ subscriber loop exception: " << e.what() << std::endl;
    }
}

#endif

void TickProcessor::start_analytics_publisher(const std::string& endpoint) {
#ifdef ZMQ_BUILD
try {
    if (!zmq_push_analytics_active_) {
        zmq_push_analytics_.set(zmq::sockopt::sndhwm, 100000);
        zmq_push_analytics_.set(zmq::sockopt::linger, 0);
        zmq_push_analytics_.connect(endpoint);
        zmq_push_analytics_active_.store(true, std::memory_order_release);
        std::cout << "✅ [TickProcessor] ZMQ PUSH connected to " << endpoint << " (analytics)" << std::endl;
    }
} catch (const zmq::error_t& e) {
    std::cerr << "❌ [TickProcessor] Analytics PUSH connect failed: " << e.what() << std::endl;
}
#endif
}

void TickProcessor::push_to_analytics(const Tick& tick) {
#ifdef ZMQ_BUILD
    if (!zmq_push_analytics_) return;
    nlohmann::json j = tick.to_json(); // Assuming you already have this
    std::string msg = j.dump();
    zmq_send(zmq_push_analytics_, msg.data(), msg.size(), 0);
#endif
}

void TickProcessor::stop_analytics_publisher() {
#ifdef ZMQ_BUILD
    if (zmq_push_analytics_active_) {
        zmq_push_analytics_.close();
        zmq_push_analytics_active_.store(false, std::memory_order_release);
        std::cout << "🧹 [TickProcessor] Analytics PUSH socket closed." << std::endl;
    }
#endif
}

void TickProcessor::set_config(const Config& cfg) { //23rd Oct 2025
    cfg_ = cfg;
    z_spread_ = OnlineZ(cfg_.zscore_alpha);
    z_imb_    = OnlineZ(cfg_.zscore_alpha);
    z_ofi_    = OnlineZ(cfg_.zscore_alpha);
    z_vol_    = OnlineZ(cfg_.zscore_alpha);
    z_ret_    = OnlineZ(cfg_.zscore_alpha);
}




