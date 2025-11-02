#pragma once
#include <string>
#include "order_book.hpp"
#include <thread>     // ✅ For std::thread
#ifdef ZMQ_BUILD
#include <zmq.hpp>    // ✅ For ZeroMQ PUB/SUB sockets
#endif
// TradingStrategy: Book imbalance and other signals
class TradingStrategy {
public:
    // Check imbalance and return a signal string
    static std::string check_imbalance(const std::string& instrument, const OrderBook& book);

    // Get imbalance value (normalized between -1 and 1)
    static double compute_imbalance(const OrderBook& book);
#ifdef ZMQ_BUILD
    void start_subscriber(const std::string& address = "tcp://127.0.0.1:6500", const std::string& topic = "");
    void stop_subscriber();
#endif

#ifdef ZMQ_BUILD
private:
    zmq::context_t zmq_ctx_sub_{1};
    zmq::socket_t zmq_sub_{zmq_ctx_sub_, zmq::socket_type::sub};
    std::unique_ptr<std::thread> zmq_sub_thread_;
    std::atomic<bool> zmq_sub_running_{false};
    void zmq_subscriber_loop(const std::string& address);
#endif

};
