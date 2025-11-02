#include "deribit_client.hpp"
#include <iostream>
#include <iomanip>
#include <chrono>

// -------------------- Constructor --------------------
DeribitClient::DeribitClient(boost::asio::io_context& ioc,
                             ssl::context& ctx,
                             const std::string& host,
                             const std::string& port,
                             const std::string& target,
                             const std::string& client_id,
                             const std::string& client_secret)
    : WSClient(ioc, ctx, host, port, target),
      client_id_(client_id),
      client_secret_(client_secret),
      keepalive_timer_(ioc)
{
// 	std::cout << "🔍 DeribitClient constructor - Member timer address: " << &keepalive_timer_ << "\n";
}

// -------------------- WebSocket Handshake --------------------
void DeribitClient::on_handshake(const beast::error_code& ec) {
    if (ec) {
        std::cerr << "❌ WebSocket handshake error: " << ec.message() << "\n";
        reconnect();
        return;
    }

    std::cout << "✅ Connected, sending auth request...\n";
    connected_ = true;
    
    // Use member timer instead of local timer to avoid cancellation
    keepalive_timer_.expires_after(std::chrono::milliseconds(100));
    
    keepalive_timer_.async_wait([self = std::static_pointer_cast<DeribitClient>(shared_from_this())](boost::system::error_code ec) {
        if (!ec) {
            self->send_auth();
        } else {
            std::cerr << "❌ Auth timer error: " << ec.message() << "\n";
        }
    });
    
    start_heartbeat();
    do_read();
}

// -------------------- Auth --------------------
void DeribitClient::send_auth() {
//     std::cout << "🎯🎯🎯 send_auth() FINALLY CALLED! 🎯🎯🎯\n";
    
    json msg = {
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "public/auth"},
        {"params", {
            {"grant_type", "client_credentials"},
            {"client_id", client_id_},
            {"client_secret", client_secret_}
        }}
    };

    std::string msg_str = msg.dump();
//     std::cout << "🔍 Sending auth message (first 50 chars): " << msg_str.substr(0, 50) << "...\n";
    
    send_text(msg_str);
//     std::cout << "🔍 Auth message sent to send_text()\n";
}

// -------------------- Read --------------------
void DeribitClient::on_read(const beast::error_code& ec, std::size_t bytes_transferred) {
	if (ec) {
        std::cout << "❌ [DeribitClient::on_read] Error detected, passing to base class" << std::endl;
    }
    static auto last_read_time = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();
    auto time_since_last_read = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_read_time);
    last_read_time = now;
    
    if (time_since_last_read > std::chrono::milliseconds(100)) {
        std::cout << "⚠️ READ DELAY: " << time_since_last_read.count() << "ms since last read\n";
    }
    
    if (ec) {
        std::cerr << "❌ Read error: " << ec.message() << "\n";
        std::cerr << "❌ Error category: " << ec.category().name() << "\n";
        std::cerr << "❌ Error value: " << ec.value() << "\n";
        
        // Detailed error analysis
        if (ec == beast::websocket::error::closed) {
            std::cerr << "❌ WebSocket was closed by peer\n";
        } else if (ec == boost::asio::error::eof) {
            std::cerr << "❌ Connection closed unexpectedly (EOF)\n";
        } else if (ec == boost::asio::error::connection_reset) {
            std::cerr << "❌ Connection reset by peer\n";
        } else if (ec == boost::asio::error::operation_aborted) {
            std::cerr << "❌ Operation aborted - timer or async operation canceled\n";
        } else if (ec == boost::asio::error::timed_out) {
            std::cerr << "❌ Operation timed out\n";
        }
        
        connected_ = false;
        
        // Clean up before reconnecting
        beast::error_code close_ec;
//         std::cout << "🔍 Closing WebSocket connection...\n";
        
        // Try to close WebSocket gracefully first
        ws_.close(websocket::close_code::normal, close_ec);
        if (close_ec) {
            std::cerr << "❌ WebSocket close error: " << close_ec.message() << "\n";
        }
        
        // Then shutdown SSL
        ws_.next_layer().shutdown(close_ec);
        if (close_ec) {
            std::cerr << "❌ SSL shutdown error: " << close_ec.message() << "\n";
        }
        
        // Finally close TCP socket
        ws_.next_layer().next_layer().close(close_ec);
        if (close_ec) {
            std::cerr << "❌ Socket close error: " << close_ec.message() << "\n";
        }
        
//         std::cout << "🔍 Initiating reconnect...\n";
        reconnect();
        return;
    }

    std::string data = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());
    
//     std::cout << "🔍 Received data (" << data.length() << " bytes): ";
    if (data.length() > 200) {
//         std::cout << data.substr(0, 200) << "...\n";
    } else {
//         std::cout << data << "\n";
    }

    log_message(data);

    try {
        auto j = json::parse(data);
//         std::cout << "🔍 JSON parsed successfully, has keys: ";
        for (auto it = j.begin(); it != j.end(); ++it) {
//             std::cout << it.key() << " ";
        }
//         std::cout << "\n";

        // Handle authentication response
        if (j.contains("id") && j["id"] == 1) {
//             std::cout << "🔍 Processing auth response (ID=1)\n";
            if (j.contains("result")) {
//                 std::cout << "✅ Auth successful, access token: ";
                if (j["result"].contains("access_token")) {
//                     std::cout << j["result"]["access_token"].get<std::string>().substr(0, 20) << "...\n";
                } else {
//                     std::cout << "present but no access_token field\n";
                }
//                 std::cout << "✅ Auth successful, subscribing...\n";
                send_subscribe();
            } else if (j.contains("error")) {
                std::cerr << "❌ Auth failed: " << j["error"]["message"] << "\n";
                if (j["error"].contains("code")) {
                    std::cerr << "❌ Error code: " << j["error"]["code"] << "\n";
                }
                // Don't return here - we still want to continue reading
            }
        }
        // Handle subscription response
        else if (j.contains("id") && j["id"] == 2) {
//             std::cout << "🔍 Processing subscription response (ID=2)\n";
            if (j.contains("result")) {
//                 std::cout << "✅ Subscription successful\n";
                if (j["result"].is_array()) {
//                     std::cout << "✅ Subscribed to " << j["result"].size() << " channels:\n";
                    for (const auto& channel : j["result"]) {
//                         std::cout << "   - " << channel.get<std::string>() << "\n";
                    }
                }
                // Start a timer to send periodic keep-alive messages
                start_keepalive();
            } else if (j.contains("error")) {
                std::cerr << "❌ Subscription failed: " << j["error"]["message"] << "\n";
                if (j["error"].contains("code")) {
                    std::cerr << "❌ Error code: " << j["error"]["code"] << "\n";
                }
            }
        }
        // Handle incoming trade data
        else if (j.contains("method") && j["method"] == "subscription") {
//             std::cout << "🔍 Received subscription data\n";
            if (j.contains("params") && j["params"].contains("channel")) {
//                 std::cout << "📡 Channel: " << j["params"]["channel"] << "\n";
            }
            handle_trade_data(j);
        }
        // Handle heartbeat responses
        else if (j.contains("id") && j["id"] == 999) {
//             std::cout << "🔍 Received heartbeat response (ID=999)\n";
            // This is a response to our keep-alive message
//             std::cout << "💓 Keep-alive response received\n";
            if (j.contains("result")) {
//                 std::cout << "💓 Server time: " << j["result"] << "\n";
            }
        }
        // Handle other known message types
        else if (j.contains("method")) {
//             std::cout << "🔍 Received method call: " << j["method"] << "\n";
            if (j.contains("params")) {
//                 std::cout << "🔍 Params: " << j["params"].dump() << "\n";
            }
        }
        else if (j.contains("result") && !j.contains("id")) {
//             std::cout << "🔍 Received result without ID: " << j["result"].dump().substr(0, 100) << "\n";
        }
        else {
//             std::cout << "🔍 Received unknown message type: " << j.dump().substr(0, 200) << "\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "⚠️ JSON parse error: " << e.what() << "\n";
        std::cerr << "⚠️ Raw data that failed parsing: " << data.substr(0, 200) << "\n";
    }

    // Check if we're still connected before setting up next read
    if (connected_) {
        std::cout << "🔍 Setting up next async read...\n";
        do_read();
    } else {
        std::cerr << "❌ Not setting up next read - connection lost\n";
    }
    
    std::cout << "🔍 on_read() completed\n";
//     std::cout << "----------------------------------------\n";
}

// -------------------- Subscribe --------------------
void DeribitClient::send_subscribe() {
//     std::cout << "🔍 send_subscribe() called\n";
//     std::cout << "🔍 Connection state: " << (connected_ ? "CONNECTED" : "DISCONNECTED") << "\n";
//     std::cout << "🔍 Using keepalive timer at address: " << &keepalive_timer_ << "\n";
    
    // Check if we're still connected before setting up the timer
    if (!connected_) {
        std::cerr << "❌ Cannot subscribe - not connected!\n";
        return;
    }
    
    // Use the existing member timer - DON'T create a local timer!
    keepalive_timer_.expires_after(std::chrono::milliseconds(100));
    
    keepalive_timer_.async_wait([self = std::static_pointer_cast<DeribitClient>(shared_from_this())]
                                (boost::system::error_code ec) {
//         std::cout << "🔍 Subscribe timer callback triggered\n";
//         std::cout << "🔍 Timer error code: " << ec.message() << "\n";
//         std::cout << "🔍 Connection state in callback: " << (self->connected_ ? "CONNECTED" : "DISCONNECTED") << "\n";
        
        if (ec) {
            std::cerr << "❌ Subscribe timer error: " << ec.message() << "\n";
            if (ec == boost::asio::error::operation_aborted) {
                std::cerr << "❌ Timer was canceled - connection might be closing\n";
            }
            return;
        }
        
        if (!self->connected_) {
            std::cerr << "❌ Cannot subscribe - connection lost during timer wait\n";
            return;
        }
        
        try {
            json msg = {
			{"jsonrpc", "2.0"},
			{"id", 2},
			{"method", "public/subscribe"},
			{"params", {
			{"channels", json::array({
            "book.BTC-PERPETUAL.raw",
            "book.ETH-PERPETUAL.raw",
            "trades.BTC-PERPETUAL.raw",
            "trades.ETH-PERPETUAL.raw"
			})}
			}}
			};            
            std::string msg_str = msg.dump();
            std::cout << "📡 Subscribing to channels: " << msg["params"]["channels"].dump() << std::endl;

            for (const auto& channel : msg["params"]["channels"]) {
//                 std::cout << channel.get<std::string>() << " ";
            }
//             std::cout << "\n";
            
            self->send_text(msg_str);
//             std::cout << "🔍 Subscribe message sent to send_text()\n";
            
        } catch (const std::exception& e) {
            std::cerr << "❌ Exception in subscribe timer: " << e.what() << "\n";
        }
    });
    
//     std::cout << "🔍 Timer async_wait setup complete\n";
}

// -------------------- Handle Trade Data --------------------
void DeribitClient::handle_trade_data(const json& j) {
    try {
        const auto& params = j["params"];
        const std::string& channel = params["channel"];
        const auto& data = params["data"];
        
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
        
//         std::cout << "[" << timestamp << "] ";
        
        if (channel.find("trades") != std::string::npos) {
            // Handle trade data
            for (const auto& trade : data) {
                std::string instrument = trade["instrument_name"];
                double price = trade["price"];
                double amount = trade["amount"];
                std::string direction = trade["direction"];
                
//                 std::cout << "TRADE " << instrument << " " 
//                          << direction << " " << std::fixed << std::setprecision(2)
//                          << amount << " @ " << price << "\n";
            }
        } else if (channel.find("book") != std::string::npos) {
            // Handle order book data
//             std::cout << "ORDERBOOK " << channel << " update received\n";
        }
        
    } catch (const std::exception& e) {
        std::cerr << "⚠️ Trade data error: " << e.what() << "\n";
    }
}

// -------------------- Keep Alive --------------------
void DeribitClient::start_keepalive() {
    keepalive_timer_.expires_after(std::chrono::seconds(30));
    keepalive_timer_.async_wait(
        [self = std::static_pointer_cast<DeribitClient>(shared_from_this())]
        (const boost::system::error_code& ec) {
            if (!ec && self->connected_) {
                self->send_keepalive();
                self->start_keepalive();
            }
        }
    );
}

void DeribitClient::send_keepalive() {
    json msg = {
        {"jsonrpc", "2.0"},
        {"id", 999},
        {"method", "public/test"},
        {"params", {}}
    };
    
    send_text(msg.dump());
//     std::cout << "💓 Sent keep-alive message\n";
}

void DeribitClient::on_connected() {
    // Add delay before sending auth
    net::steady_timer timer(ws_.get_executor());
    timer.expires_after(std::chrono::milliseconds(100));
    timer.async_wait([self = shared_from_this()](boost::system::error_code ec) {
        if (!ec) {
            // Cast to DeribitClient and call send_auth
            std::static_pointer_cast<DeribitClient>(self)->send_auth();
        }
    });
}