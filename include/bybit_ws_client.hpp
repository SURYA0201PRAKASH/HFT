#pragma once
#include "ws_client_day3.hpp"
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <nlohmann/json.hpp>
#include <memory>

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;

class MarketDataRecorder;   // ← forward declaration

class BybitWsClient : public WSClient {
public:
    explicit BybitWsClient(net::io_context& ioc, ssl::context& ctx);
    ~BybitWsClient();

    void subscribe();
    void on_handshake(const beast::error_code& ec) override;
    void on_read(const beast::error_code& ec, std::size_t bytes_transferred) override;

    void start_heartbeat();
    void reconnect();

    // 💾 Recorder attachment
    void set_recorder(std::shared_ptr<MarketDataRecorder> r) { recorder_ = std::move(r); }

private:
    // Pass timestamp to propagate exchange event time
    void parse_orderbook_object(const nlohmann::json& rec, long long ts);

    std::shared_ptr<MarketDataRecorder> recorder_;  // ← NEW
};
