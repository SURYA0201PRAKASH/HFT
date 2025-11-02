#pragma once
#include "order_book.hpp"
#include "enhanced_order_book.hpp"  // Add this
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <string>
#include <thread>
#ifdef ZMQ_BUILD
#include <zmq.hpp>   // ✅ Required for ZMQ context_t and socket_t
#endif


class OrderBookManager {
public:
    OrderBookManager();
    
    void update_from_snapshot(const std::string& instrument, const nlohmann::json& data);
    void apply_delta_update(const std::string& instrument, const nlohmann::json& data);
    std::shared_ptr<OrderBook> get_order_book(const std::string& instrument);
    std::shared_ptr<EnhancedOrderBook> get_enhanced_order_book(const std::string& instrument);  // Add this
    
    void print_top_of_book(const std::string& instrument);
    nlohmann::json get_book_json(const std::string& instrument);
    
    template<typename Func>
    void for_each_order_book(Func func) {
        std::shared_lock lock(books_mutex_);
        for (const auto& [symbol, book] : order_books_) {
            func(*book);
        }
    }
    
    // Add tick processing capability
    void process_tick(const Tick& tick);  // Add this
#ifdef ZMQ_BUILD
    void start_pull_server(const std::string& address = "tcp://127.0.0.1:6000");
	void stop_pull_server();
	void start_publisher(const std::string& address = "tcp://127.0.0.1:6500");
    void stop_publisher();
    void publish_book_update(const std::string& instrument, double bid, double ask);
#endif
private:
    std::unordered_map<std::string, std::shared_ptr<OrderBook>> order_books_;
    std::unordered_map<std::string, std::shared_ptr<EnhancedOrderBook>> enhanced_books_;  // Add this
    mutable std::shared_mutex books_mutex_;
    
    std::shared_ptr<OrderBook> get_or_create_book(const std::string& instrument);
    std::shared_ptr<EnhancedOrderBook> get_or_create_enhanced_book(const std::string& instrument);  // Add this
	mutable std::mutex books_mtx_;
#ifdef ZMQ_BUILD
    std::atomic<bool> pull_running_{false};                // ✅ new
    std::thread pull_thread_;                              // ✅ new
	zmq::context_t zmq_ctx_pub_{1};
    zmq::socket_t zmq_pub_{zmq_ctx_pub_, zmq::socket_type::pub};
    std::atomic<bool> zmq_pub_active_{false};
#endif
};