#include "trading_strategy.hpp"
#include <iostream>

double TradingStrategy::compute_imbalance(const OrderBook& book) {
    double bidSize = book.get_best_bid_size();
    double askSize = book.get_best_ask_size();

    if (bidSize + askSize == 0.0) return 0.0;
    return (bidSize - askSize) / (bidSize + askSize);
}

std::string TradingStrategy::check_imbalance(const std::string& instrument, const OrderBook& book) {
    double imbalance = compute_imbalance(book);
    double bid = book.get_best_bid_price();
    double ask = book.get_best_ask_price();

    // Add counter to reduce output frequency
    static std::unordered_map<std::string, int> signal_counters;
    int& count = signal_counters[instrument];
    
    if (++count % 50 == 0) { // Print only every 50 signals per instrument
        if (imbalance > 0.5) {
//             std::cout << "📈 BUY signal for " << instrument
//                      << " (imbalance=" << imbalance << ")"
//                      << " BestBid=" << bid << " BestAsk=" << ask << "\n";
            return "BUY";
        } else if (imbalance < -0.5) {
//             std::cout << "📉 SELL signal for " << instrument
//                      << " (imbalance=" << imbalance << ")"
//                      << " BestBid=" << bid << " BestAsk=" << ask << "\n";
            return "SELL";
        }
    } else {
        // Still return the signal, just don't print every time
        if (imbalance > 0.5) return "BUY";
        else if (imbalance < -0.5) return "SELL";
    }
    
    return "HOLD";
}
#ifdef ZMQ_BUILD
void TradingStrategy::start_subscriber(const std::string& address, const std::string& topic) {
    zmq_sub_.connect(address);
    zmq_sub_.set(zmq::sockopt::subscribe, topic);
    zmq_sub_running_.store(true, std::memory_order_release);
    zmq_sub_thread_ = std::make_unique<std::thread>(
        &TradingStrategy::zmq_subscriber_loop, this, address
    );
    std::cout << "✅ [TradingStrategy] SUB connected to " << address << " | Topic='" << topic << "'" << std::endl;
}

void TradingStrategy::stop_subscriber() {
    zmq_sub_running_.store(false, std::memory_order_release);
    if (zmq_sub_thread_ && zmq_sub_thread_->joinable()) zmq_sub_thread_->join();
    zmq_sub_.close();
    std::cout << "🛑 [TradingStrategy] SUB stopped." << std::endl;
}

void TradingStrategy::zmq_subscriber_loop(const std::string& address) {
    while (zmq_sub_running_.load(std::memory_order_acquire)) {
        zmq::message_t topic_msg, data_msg;

        if (!zmq_sub_.recv(topic_msg, zmq::recv_flags::none)) continue;
        if (!zmq_sub_.recv(data_msg, zmq::recv_flags::none)) continue;

        std::string topic(static_cast<char*>(topic_msg.data()), topic_msg.size());
        std::string payload(static_cast<char*>(data_msg.data()), data_msg.size());
        nlohmann::json j = nlohmann::json::parse(payload);

        std::string instrument = j.value("instrument", "");
        double bid = j.value("bid", 0.0);
        double ask = j.value("ask", 0.0);
        uint64_t ts = j.value("timestamp_ns", 0ull);

        std::cout << "📨 [Strategy] Received Book Update | " << instrument 
                  << " | Bid=" << bid << " | Ask=" << ask << std::endl;

        // TODO: Use this data in decision logic later
    }
}
#endif
