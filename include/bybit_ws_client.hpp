#pragma once
#include "ws_client_day3.hpp"     // or whatever base WSClient header you use
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <nlohmann/json.hpp>

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;

class BybitWsClient : public WSClient {
public:
    explicit BybitWsClient(net::io_context& ioc, ssl::context& ctx);
    ~BybitWsClient();

    // Core handlers
    void subscribe();
    void on_handshake(const beast::error_code& ec) override;
    void on_read(const beast::error_code& ec, std::size_t bytes_transferred) override;

    // Extra debug & lifecycle helpers (added for clarity)
    void start_heartbeat();   // 🔹 new
    void reconnect();         // 🔹 new
	void parse_orderbook_object(const nlohmann::json& rec);
};
