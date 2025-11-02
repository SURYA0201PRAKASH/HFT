#pragma once
#include "deribit_client.hpp"
#include "order_book_manager.hpp"
#include "enhanced_logger.hpp"
#include "tick_data.hpp"
#include "tick_processor.hpp"
#include <memory>

#ifdef ZMQ_BUILD
#include <zmq.h>
#endif

class EnhancedDeribitClient : public DeribitClient {
public:
    EnhancedDeribitClient(boost::asio::io_context& ioc,
                         ssl::context& ctx,
                         const std::string& host,
                         const std::string& port,
                         const std::string& target,
                         const std::string& client_id,
                         const std::string& client_secret);
    
    OrderBookManager& get_order_book_manager() { return book_manager_; }
    TickProcessor& get_tick_processor() { return *tick_processor_; }
    
    void start_tick_processing() {
        if (tick_processor_) tick_processor_->start();
    }
    
    void stop_tick_processing() {
        if (tick_processor_) tick_processor_->stop();
#ifdef ZMQ_BUILD
        teardown_zmq_publisher();  // stop publisher cleanly
#endif
    }

    void verify_initialization();
    void debug_tick_processor_status();
    void check_sequence_recovery();
    void test_sequence_recovery();

protected:
    void on_read(const beast::error_code& ec, std::size_t bytes_transferred) override;
    
private:
    void handle_order_book_update(const nlohmann::json& data);
    void handle_trade(const nlohmann::json& data);
    void request_order_book_snapshot(const std::string& instrument);
    void process_tick_data(const nlohmann::json& j);
    void request_missing_sequences(const std::string& instrument, uint64_t from_seq, uint64_t to_seq);

    Tick create_trade_tick(const nlohmann::json& trade_data, const std::string& channel);
    Tick create_orderbook_tick(const nlohmann::json& book_data, const std::string& channel);
    
    OrderBookManager book_manager_;
    std::unique_ptr<EnhancedLogger> logger_;
    std::unique_ptr<TickProcessor> tick_processor_;
    std::unordered_map<std::string, uint64_t> last_sequence_numbers_;

#ifdef ZMQ_BUILD
private:
    void* zmq_ctx_ = nullptr;
    void* zmq_pub_ = nullptr;
    std::string zmq_endpoint_ = "tcp://127.0.0.1:5555";
    void publish_tick_zmq(const Tick& tick);
    void setup_zmq_publisher();
    void teardown_zmq_publisher();
#endif
};
