#include "bybit_client.hpp"
#include <iostream>

BybitClient::BybitClient(boost::asio::io_context& ioc, ssl::context& ctx)
    : ws_client_(std::make_shared<BybitWsClient>(ioc, ctx))
{
    std::cout << "🟢 [BybitClient] ctor this=" << this << "\n";
}

BybitClient::~BybitClient() {
    std::cout << "🔴 [BybitClient] dtor this=" << this << "\n";
}

void BybitClient::connect() {
    std::cout << "🔗 [Bybit] Connecting to WebSocket..." << std::endl;
    ws_client_->run();  // ✅ FIX: use '->'
}

void BybitClient::subscribe(const std::string& symbol) {
    std::cout << "📡 [Bybit] Subscribing to " << symbol << std::endl;

    // ✅ FIX: correct method name and arrow operator
    std::string msg = R"({"op":"subscribe","args":["orderbook.1.)" + symbol + R"("]})";
    ws_client_->send_text(msg);  
}
