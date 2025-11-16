#include "bybit_ws_client.hpp"
#include "market_data_recorder.hpp"
#include "tick_analytics.hpp"
#include "indicators_state.hpp"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <unordered_map>

using nlohmann::json;

// =============================
// ctor / dtor
// =============================
BybitWsClient::BybitWsClient(net::io_context& ioc, ssl::context& ctx)
    : WSClient(ioc, ctx, "stream.bybit.com", "443", "/v5/public/linear")
{
    std::cout << "🟢 [BybitWsClient::ctor] Created WS client @ " << this << std::endl;
    std::cout << "📊 [BybitWsClient] Initial recorder ptr=" << recorder_.get() << std::endl;
}

BybitWsClient::~BybitWsClient() {
    std::cout << "🧹 [BybitWsClient::dtor] Destroying WS client @ " << this << std::endl;
}

// =============================
// Subscribe (initial + reconnect)
// =============================
void BybitWsClient::subscribe() {
    std::string sub = R"({
        "op":"subscribe",
        "args":["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"]
    })";

    std::cout << "📡 [BybitWsClient::subscribe] " << sub << std::endl;
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
// Main message handler
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

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "\n🕒 [BybitWsClient] Message received at: "
              << std::put_time(std::localtime(&now), "%F %T") << std::endl;
    std::cout << "📦 Raw: " << msg.substr(0, 200)
              << (msg.size() > 200 ? "..." : "") << std::endl;

    try {
        auto j = json::parse(msg, nullptr, false);
        if (j.is_discarded()) {
            std::cerr << "⚠️ [BybitWsClient] JSON parse error, skipping message.\n";
            do_read();
            return;
        }

        // Control / subscription acks
        if (j.contains("op")) {
            std::cout << "ℹ️ [BYBIT] Control: " << j.dump() << std::endl;
        }

        // Data messages: orderbook / trades
        if (j.contains("topic") && j.contains("data")) {
            std::string topic = j.value("topic", "");
            std::string typ   = j.value("type", "");
            long long ts      = j.value("ts", 0LL);  // ms

            // symbol from topic, e.g. "orderbook.50.BTCUSDT"
            std::string sym = "UNKNOWN";
            if (!topic.empty()) {
                auto pos = topic.find_last_of('.');
                if (pos != std::string::npos && pos + 1 < topic.size()) {
                    sym = topic.substr(pos + 1);
                }
            }

            std::cout << "📊 [BYBIT][" << topic << "] type=" << typ
                      << " ts=" << ts << " sym=" << sym << std::endl;

            // ---- ORDERBOOK ----
            if (topic.find("orderbook") != std::string::npos) {
                if (j["data"].is_array()) {
                    for (const auto& rec : j["data"]) {
                        parse_orderbook_object(rec, ts, sym);
                    }
                } else if (j["data"].is_object()) {
                    parse_orderbook_object(j["data"], ts, sym);
                }
            }
            // ---- TRADES ----
            else if (topic.find("publicTrade") != std::string::npos) {
                if (j["data"].is_array()) {
                    parse_trade_object(j["data"], ts, sym);
                } else if (j["data"].is_object()) {
                    parse_trade_object(json::array({ j["data"] }), ts, sym);
                }
            }
            else {
                std::cout << "ℹ️ [BYBIT] Unhandled topic: " << topic << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ [BybitWsClient] Exception while parsing: "
                  << e.what() << std::endl;
    }

    do_read();
}

// =============================
// Heartbeat / reconnect wrappers
// =============================
void BybitWsClient::start_heartbeat() {
    std::cout << "💓 [BybitWsClient::start_heartbeat] Heartbeat started." << std::endl;
    WSClient::start_heartbeat();
}

void BybitWsClient::reconnect() {
    std::cout << "🔄 [BybitWsClient::reconnect] Attempting reconnect..." << std::endl;
    WSClient::reconnect();
}

// =============================
// ORDERBOOK PARSERS
// =============================

// compatibility overload – symbol taken from JSON "s" if possible
void BybitWsClient::parse_orderbook_object(const json& rec, long long ts) {
    std::string sym = rec.value("s", "UNKNOWN");
    parse_orderbook_object(rec, ts, sym);
}

void BybitWsClient::parse_orderbook_object(const json& rec,
                                           long long ts,
                                           const std::string& symbol)
{
    try {
        std::string sym = rec.value("s", symbol);

        double ask_px = 0.0, ask_qty = 0.0;
        double bid_px = 0.0, bid_qty = 0.0;

        if (rec.contains("a") && rec["a"].is_array() && !rec["a"].empty()) {
            const auto& a0 = rec["a"][0];
            if (a0.size() >= 2) {
                ask_px  = std::stod(a0[0].get<std::string>());
                ask_qty = std::stod(a0[1].get<std::string>());
            }
        }

        if (rec.contains("b") && rec["b"].is_array() && !rec["b"].empty()) {
            const auto& b0 = rec["b"][0];
            if (b0.size() >= 2) {
                bid_px  = std::stod(b0[0].get<std::string>());
                bid_qty = std::stod(b0[1].get<std::string>());
            }
        }

        double mid_price =
            (ask_px > 0.0 && bid_px > 0.0) ? 0.5 * (ask_px + bid_px) : 0.0;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "   🟢 Bid: " << bid_px << " | Qty: " << bid_qty
                  << "   🔴 Ask: " << ask_px << " | Qty: " << ask_qty << std::endl;

        std::cout << "💡 [Bybit OB] sym=" << sym
                  << " bid=" << bid_px << " ask=" << ask_px
                  << " qtys=(" << bid_qty << "," << ask_qty
                  << ") ts=" << ts << std::endl;

        // ---- update per-symbol L1 cache (HYBRID L1) ----
        auto& L = last_l1_[sym];
        L.best_bid  = bid_px;
        L.best_ask  = ask_px;
        L.bid_vol_1 = bid_qty;
        L.ask_vol_1 = ask_qty;
        L.ts_ms     = ts;
        L.valid     = (bid_px > 0.0 && ask_px > 0.0);

        if (!recorder_ || mid_price <= 0.0) {
            return; // we still cache L1 even if no recorder
        }

        Tick tick;
        tick.instrument         = sym;
        tick.price              = mid_price;
        tick.quantity           = 0.5 * (bid_qty + ask_qty);
        tick.type               = TickType::ORDERBOOK_SNAPSHOT;
        tick.timestamp_ns       = static_cast<uint64_t>(ts) * 1'000'000ULL;
        tick.exchange_timestamp = tick.timestamp_ns;
        tick.sequence           = rec.value("u", 0ull); // Bybit sequence
        tick.enqueue_time_ns    = 0;

        static TickAnalytics analytics;
        static std::unordered_map<std::string, IndicatorState> states;

        analytics.add_tick(tick);
        double rolling_vol = analytics.calculate_volatility(100);
        bool   is_large    = analytics.is_large_trade(tick);

        double mid = mid_price > 0.0 ? mid_price : tick.price;

        Indicators ind = update_indicators(
            states[tick.instrument],
            mid,
            tick.price,
            tick.quantity,
            bid_px,
            ask_px,
            bid_qty,
            ask_qty
        );

        // L1 fields are already set in ind by update_indicators
        recorder_->start_recording();
        recorder_->record_with_indicators(tick, rolling_vol, is_large, ind);

        std::cout << "✅ [Bybit Recorder] OB snapshot recorded for "
                  << tick.instrument << " | mid=" << mid
                  << " | MACD=" << ind.macd
                  << " | RSI="  << ind.rsi14
                  << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "❌ [Bybit parse_orderbook_object] " << e.what() << std::endl;
    }
}

// =============================
// TRADE PARSERS
// =============================

// compatibility overload – no explicit symbol
void BybitWsClient::parse_trade_object(const json& trades, long long ts) {
    parse_trade_object(trades, ts, std::string{});
}

void BybitWsClient::parse_trade_object(const json& trades,
                                       long long ts,
                                       const std::string& symbol)
{
    try {
        static TickAnalytics analytics;
        static std::unordered_map<std::string, IndicatorState> states;
        static std::unordered_map<std::string, long long> last_ts_ms;

        for (const auto& t : trades) {
            std::string sym   = !symbol.empty()
                                ? symbol
                                : t.value("i", "UNKNOWN");           // Bybit instrument
            double      price = std::stod(t.value("p", "0"));
            double      qty   = std::stod(t.value("v", "0"));
            std::string side  = t.value("S", "");                     // "Buy" / "Sell"
            long long   trade_ts = t.value("T", ts);                  // ms

            std::cout << "💥 [BybitTrade] "
                      << sym << " " << side
                      << " px=" << price
                      << " qty=" << qty << std::endl;

            if (!recorder_) {
                continue;
            }

            // ---- HYBRID L1: per-symbol snapshot with staleness ----
            auto itL = last_l1_.find(sym);
            if (itL == last_l1_.end() || !itL->second.valid) {
                std::cout << "⚠️ [BybitTrade] Skipping " << sym
                          << " trade due to missing L1 snapshot" << std::endl;
                continue;
            }

            const L1Snapshot& L = itL->second;

            if (trade_ts > 0 && L.ts_ms > 0 &&
                (trade_ts - L.ts_ms) > L1_STALE_THRESHOLD_MS)
            {
                std::cout << "⚠️ [BybitTrade] Skipping " << sym
                          << " trade due to STALE L1 (Δ="
                          << (trade_ts - L.ts_ms) << " ms)" << std::endl;
                continue;
            }

            // -------- Build Tick --------
            Tick tick;
            tick.instrument         = sym;
            tick.price              = price;
            tick.quantity           = qty;
            tick.type               = TickType::TRADE;
            tick.timestamp_ns       = static_cast<uint64_t>(trade_ts) * 1'000'000ULL;
            tick.exchange_timestamp = tick.timestamp_ns;
            tick.sequence           = 0;
            tick.enqueue_time_ns    = 0;

            // -------- Basic analytics --------
            analytics.add_tick(tick);
            double rolling_vol = analytics.calculate_volatility(100);
            bool   is_large    = analytics.is_large_trade(tick);

            // -------- Indicators using L1 snapshot --------
            Indicators ind = update_indicators(
                states[tick.instrument],
                tick.price,           // mid ~ trade price here
                tick.price,
                tick.quantity,
                L.best_bid,
                L.best_ask,
                L.bid_vol_1,
                L.ask_vol_1
            );

            // -------- Latency metrics --------
            auto now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count()
            );

            if (tick.exchange_timestamp > 0 && now_ns > tick.exchange_timestamp) {
                ind.feed_latency_ms =
                    static_cast<double>(now_ns - tick.exchange_timestamp) / 1'000'000.0;
            } else {
                ind.feed_latency_ms = 0.0;
            }

            ind.queue_latency_us = 0.0;  // no queue buffer here

            // -------- Tick gap & tick rate (per instrument) --------
            long long ts_ms = trade_ts;
            double gap_ms   = 0.0;

            auto itGap = last_ts_ms.find(sym);
            if (itGap != last_ts_ms.end() && ts_ms > itGap->second) {
                gap_ms = static_cast<double>(ts_ms - itGap->second);
            }
            last_ts_ms[sym] = ts_ms;

            ind.tick_gap_ms = gap_ms;
            ind.tick_rate   = (gap_ms > 0.0) ? (1000.0 / gap_ms) : 0.0;

            // -------- Persist to SQLite --------
            recorder_->start_recording();
            recorder_->record_with_indicators(tick, rolling_vol, is_large, ind);

            std::cout << "✅ [Bybit Recorder] TRADE saved "
                      << sym << " | px=" << price
                      << " | qty=" << qty
                      << " | MACD=" << ind.macd
                      << " | RSI="  << ind.rsi14
                      << " | feed_lat(ms)=" << ind.feed_latency_ms
                      << " | tick_rate="    << ind.tick_rate
                      << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ [Bybit parse_trade_object] " << e.what() << std::endl;
    }
}
