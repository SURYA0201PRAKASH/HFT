#pragma once
#include "ws_client_day3.hpp"
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <unordered_map>

namespace net   = boost::asio;
namespace ssl   = boost::asio::ssl;
namespace beast = boost::beast;

class MarketDataRecorder;   // forward declaration

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

    // Optional public helpers (not required externally, but kept for compatibility)
    void parse_trade_object(const nlohmann::json& trades, long long ts);

private:
    struct L1Snapshot {
        double best_bid  = 0.0;
        double best_ask  = 0.0;
        double bid_vol_1 = 0.0;
        double ask_vol_1 = 0.0;
        long long ts_ms  = 0;   // exchange event time in ms
        bool   valid     = false;
    };

    // Pass timestamp to propagate exchange event time
    void parse_orderbook_object(const nlohmann::json& rec, long long ts);
    void parse_orderbook_object(const nlohmann::json& rec, long long ts, const std::string& symbol);

    void parse_trade_object(const nlohmann::json& trades, long long ts, const std::string& symbol);

    std::shared_ptr<MarketDataRecorder> recorder_;  
    std::unordered_map<std::string, L1Snapshot> last_l1_;   // per-symbol L1 cache

    static constexpr long long L1_STALE_THRESHOLD_MS = 2000;  // 2s hybrid policy
};
