#pragma once
#include "ws_client_day3.hpp"
#include <nlohmann/json.hpp>

class BybitWsClient : public WSClient {
public:
    BybitWsClient(net::io_context& ioc, ssl::context& ctx);
    void subscribe();

protected:
    void on_handshake(const beast::error_code& ec) override;
    void on_read(const beast::error_code& ec, std::size_t bytes_transferred) override;
};
