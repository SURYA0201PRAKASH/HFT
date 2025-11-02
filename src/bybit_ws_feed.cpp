#include "bybit_ws_feed.hpp"
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <iostream>
#include <thread>
#include <nlohmann/json.hpp>

using boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;
using json = nlohmann::json;

BybitWsFeed::BybitWsFeed(std::string symbol)
    : symbol_(std::move(symbol))
{}

void BybitWsFeed::start() {
    if (running_) return;
    running_ = true;

    // run in a detached thread so start() is non-blocking
    std::thread([this]() {
        try {
            boost::asio::io_context ioc;
            ssl::context ctx(ssl::context::sslv23);
            ctx.set_default_verify_paths();

            tcp::resolver resolver(ioc);
            auto const results = resolver.resolve("stream.bybit.com", "443");

            ssl::stream<tcp::socket> ws(ioc, ctx);
            boost::asio::connect(ws.lowest_layer(), results);
            ws.handshake(ssl::stream_base::client);

            // WebSocket handshake (raw)
            // Bybit public WS v5: wss://stream.bybit.com/v5/public/linear
            // We'll send HTTP Upgrade manually (simple way)

            std::string req =
                "GET /v5/public/linear HTTP/1.1\r\n"
                "Host: stream.bybit.com\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                "\r\n";

            boost::asio::write(ws, boost::asio::buffer(req));

            // NOTE: we are doing a very light WS client here just for testing
            // if you want a full WS client, we can switch to beast like Deribit code

            std::cout << "BybitWsFeed: connected (raw test)\n";

            // send subscribe (text frame style is skipped here, we just show transport is OK)
            // in real version we will switch to beast::websocket

        } catch (const std::exception& e) {
            std::cerr << "BybitWsFeed error: " << e.what() << "\n";
        }
    }).detach();
}

void BybitWsFeed::stop() {
    running_ = false;
}
