#include "bybit_ws_client.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>

BybitWsClient::BybitWsClient(net::io_context& ioc, ssl::context& ctx)
    : WSClient(ioc, ctx, "stream.bybit.com", "443", "/v5/public/linear")
{
    std::cout << "🟢 [BybitWsClient::ctor] Created WS client @ " << this << std::endl;
}

// =============================
// Subscribe (initial + reconnect)
// =============================
void BybitWsClient::subscribe() {
    std::string sub = R"({"op":"subscribe","args":["orderbook.1.BTCUSDT"]})";
    std::cout << "📡 [BybitWsClient::subscribe] Sending subscription: " << sub << std::endl;
    send_text(sub);
}

// =============================
// Handshake handler
// =============================
void BybitWsClient::on_handshake(const beast::error_code& ec) {
    std::cout << "🔍 [BybitWsClient::on_handshake] called with ec=" << ec.message() << std::endl;

    if (ec) {
        std::cerr << "❌ [BybitWsClient] Handshake failed: " << ec.message() << std::endl;
        reconnect();
        return;
    }

    std::cout << "✅ [BybitWsClient] Handshake success" << std::endl;
    connected_ = true;

    subscribe();
    do_read();
    start_heartbeat();
}

// =============================
// Message reader
// =============================
void BybitWsClient::on_read(const beast::error_code& ec, std::size_t bytes_transferred) {
    if (ec) {
        std::cerr << "❌ [BybitWsClient::on_read] Error: " << ec.message()
                  << " (category: " << ec.category().name()
                  << ", value: " << ec.value() << ")" << std::endl;
        WSClient::on_read(ec, bytes_transferred);
        return;
    }

    std::string msg = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());

    // Add timestamp
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "\n🕒 [BybitWsClient] Message received at: " << std::put_time(std::localtime(&now), "%F %T") << std::endl;
    std::cout << "📦 Raw: " << msg.substr(0, 200) << (msg.size() > 200 ? "..." : "") << std::endl;

    try {
        auto j = nlohmann::json::parse(msg, nullptr, false);
        if (j.is_discarded()) {
            std::cerr << "⚠️ [BybitWsClient] JSON parse error, skipping message.\n";
            do_read();
            return;
        }

        // Acknowledge control / subscription messages
        if (j.contains("op")) {
            std::cout << "ℹ️ [BYBIT] Control: " << j.dump() << std::endl;
        }

        // Orderbook update
        if (j.contains("topic") && j.contains("data")) {
		std::string topic = j.value("topic", "");
		std::string typ   = j.value("type", "");
		long long ts      = j.value("ts", 0);

		std::cout << "📊 [BYBIT][" << topic << "] type=" << typ << " ts=" << ts << std::endl;

		// Handle both array and object
		if (j["data"].is_array() && !j["data"].empty()) {
			for (const auto& rec : j["data"]) {
				parse_orderbook_object(rec);
			}
		} else if (j["data"].is_object()) {
			parse_orderbook_object(j["data"]);
		}
	}
    } catch (const std::exception& e) {
        std::cerr << "❌ [BybitWsClient] Exception while parsing: " << e.what() << std::endl;
    }

    do_read();
}

// =============================
// Destructor
// =============================
BybitWsClient::~BybitWsClient() {
    std::cout << "🧹 [BybitWsClient::dtor] Destroying WS client @ " << this << std::endl;
}

// =============================
// Additional Debug Utilities
// =============================
void BybitWsClient::start_heartbeat() {
    std::cout << "💓 [BybitWsClient::start_heartbeat] Heartbeat started." << std::endl;
    WSClient::start_heartbeat();
}

void BybitWsClient::reconnect() {
    std::cout << "🔄 [BybitWsClient::reconnect] Attempting reconnect..." << std::endl;
    WSClient::reconnect();
}

void BybitWsClient::parse_orderbook_object(const nlohmann::json& rec) {
    try {
        if (rec.contains("a") && rec["a"].is_array() && !rec["a"].empty()) {
            const auto& a0 = rec["a"][0];
            if (a0.size() >= 2) {
                double ask_px  = std::stod(a0[0].get<std::string>());
                double ask_qty = std::stod(a0[1].get<std::string>());
                std::cout << "   🔴 Ask: " << ask_px << " | Qty: " << ask_qty << std::endl;
            }
        }
        if (rec.contains("b") && rec["b"].is_array() && !rec["b"].empty()) {
            const auto& b0 = rec["b"][0];
            if (b0.size() >= 2) {
                double bid_px  = std::stod(b0[0].get<std::string>());
                double bid_qty = std::stod(b0[1].get<std::string>());
                std::cout << "   🟢 Bid: " << bid_px << " | Qty: " << bid_qty << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ [Bybit parse_orderbook_object] " << e.what() << std::endl;
    }
}
