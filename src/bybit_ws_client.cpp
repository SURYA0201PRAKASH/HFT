#include "bybit_ws_client.hpp"
#include <iostream>

BybitWsClient::BybitWsClient(net::io_context& ioc, ssl::context& ctx)
    : WSClient(ioc, ctx, "stream.bybit.com", "443", "/v5/public/linear") {}

void BybitWsClient::subscribe() {
    std::string sub = R"({"op":"subscribe","args":["orderbook.1.BTCUSDT"]})";
    send_text(sub);
}

void BybitWsClient::on_handshake(const beast::error_code& ec) {
    if (ec) {
        std::cerr << "❌ Bybit handshake failed: " << ec.message() << "\n";
        reconnect();
        return;
    }
    std::cout << "✅ Bybit handshake success\n";
    connected_ = true;
    subscribe();
    do_read();
    start_heartbeat();
}

void BybitWsClient::on_read(const beast::error_code& ec, std::size_t bytes_transferred) {
    if (ec) {
        WSClient::on_read(ec, bytes_transferred);
        return;
    }

    std::string msg = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());

    try {
        auto j = nlohmann::json::parse(msg, nullptr, false);
        if (j.is_discarded() || !j.contains("data")) return;

        const auto& data = j["data"];
        if (data.contains("a")) {
            double px = std::stod(data["a"][0][0].get<std::string>());
            double qty = std::stod(data["a"][0][1].get<std::string>());
            std::cout << "💹 [BYBIT] Ask: " << px << " Qty: " << qty << "\n";
        }
    } catch (...) {}

    do_read();
}
