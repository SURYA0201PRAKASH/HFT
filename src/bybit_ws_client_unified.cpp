#pragma once

#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/connect.hpp>

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>

#include "feature_engine.hpp"
#include <nlohmann/json.hpp>
#include "orderbook_common.hpp"

namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = net::ssl;

using tcp   = net::ip::tcp;
using json  = nlohmann::json;


// ========================= BybitWsClient (V5 public) =========================

class BybitWsClient {
public:
    BybitWsClient(net::io_context& ioc,
                  ssl::context& ctx,
                  const std::string& category = "linear",
                  bool testnet = false)
        : resolver_(ioc)
        , ws_(ioc, ctx)
    {
        // Host / path
        host_ = testnet ? "stream-testnet.bybit.com"
                        : "stream.bybit.com";

        if (category == "linear")
            path_ = "/v5/public/linear";
        else if (category == "inverse")
            path_ = "/v5/public/inverse";
        else if (category == "spot")
            path_ = "/v5/public/spot";
        else
            path_ = "/v5/public/linear";

        // Init instrument states (BTC & ETH USDT perps)
        instruments_.emplace("BTCUSDT", InstrumentState("BTCUSDT"));
        instruments_.emplace("ETHUSDT", InstrumentState("ETHUSDT"));

        // Basic metadata (you can refine tick_size etc later)
        instrument_meta_["BTCUSDT"] = InstrumentMeta{
            "BTCUSDT", "BTC", "USDT", 0.5, 0.0, 0.0
        };
        instrument_meta_["ETHUSDT"] = InstrumentMeta{
            "ETHUSDT", "ETH", "USDT", 0.05, 0.0, 0.0
        };
    }

    void connect() {
        resolver_.async_resolve(
            host_, "443",
            beast::bind_front_handler(
                &BybitWsClient::on_resolve, this));
    }

private:
    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    beast::flat_buffer buffer_;

    std::string host_;
    std::string path_;

    std::unordered_map<std::string, InstrumentState> instruments_;
    std::unordered_map<std::string, InstrumentMeta>  instrument_meta_;

    // ======================= ASIO / WS pipeline =======================

    void on_resolve(beast::error_code ec,
                    tcp::resolver::results_type results)
    {
        if (ec) return fail(ec, "resolve");

        beast::get_lowest_layer(ws_).async_connect(
            results,
            beast::bind_front_handler(
                &BybitWsClient::on_connect, this));
    }

    void on_connect(beast::error_code ec,
                    tcp::resolver::results_type::endpoint_type)
    {
        if (ec) return fail(ec, "connect");

        ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            beast::bind_front_handler(
                &BybitWsClient::on_ssl_handshake, this));
    }

    void on_ssl_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ssl_handshake");

        ws_.set_option(
            websocket::stream_base::timeout::suggested(
                beast::role_type::client));

        ws_.async_handshake(
            host_, path_,
            beast::bind_front_handler(
                &BybitWsClient::on_ws_handshake, this));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        send_subscribe();
    }

    // ======================= SUBSCRIBE =======================

    void send_subscribe() {
        // Depth 50 so we can build L5 book easily
        json sub = {
            {"op","subscribe"},
            {"args", json::array({
                "orderbook.50.BTCUSDT",
                "publicTrade.BTCUSDT",
                "orderbook.50.ETHUSDT",
                "publicTrade.ETHUSDT"
            })}
        };

        ws_.async_write(
            net::buffer(sub.dump()),
            beast::bind_front_handler(
                &BybitWsClient::on_sub_sent, this));
    }

    void on_sub_sent(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "sub_send");
        std::cout << "📡 Bybit subscribed\n";
        do_read();
    }

    // ======================= READ LOOP =======================

    void do_read() {
        ws_.async_read(
            buffer_,
            beast::bind_front_handler(
                &BybitWsClient::on_read, this));
    }

    void on_read(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "read");

        std::string data = beast::buffers_to_string(buffer_.data());
        buffer_.consume(buffer_.size());

        json msg;
        try {
            msg = json::parse(data);
        } catch (...) {
            std::cerr << "⚠ [Bybit] Non-JSON message: " << data << "\n";
            return do_read();
        }

        // Subscription ack or other control messages
        if (msg.contains("op")) {
            std::string op = msg["op"].get<std::string>();
            if (op == "subscribe") {
                std::cout << "✅ Bybit subscribe ack: " << data << "\n";
            }
            return do_read();
        }

        if (msg.contains("topic")) {
            const std::string topic = msg["topic"].get<std::string>();

            if (topic.rfind("orderbook.", 0) == 0) {
                handle_orderbook(msg);
                return do_read();
            }
            if (topic.rfind("publicTrade.", 0) == 0) {
                handle_trades(msg);
                return do_read();
            }
        }

        // Default
        do_read();
    }

    // ======================= ORDERBOOK HANDLER =======================

    void handle_orderbook(const json& msg) {
        const std::string type = msg.value("type", std::string("snapshot"));
        const auto& data       = msg.at("data");

        const std::string instr = data.at("s").get<std::string>();

        // Ensure instrument state
        auto it = instruments_.find(instr);
        if (it == instruments_.end()) {
            it = instruments_.emplace(instr, InstrumentState(instr)).first;
        }
        auto& state = it->second;

        long long exch_ts_ms = msg.value("cts",
                                 msg.value("ts", 0LL));
        long long local_ns   = now_ns();
        long long local_ms   = now_ms();
        long long feed_latency_ns = (exch_ts_ms > 0)
            ? (local_ms - exch_ts_ms) * 1'000'000LL
            : 0LL;

        long long dt_last_book_ms = (state.last_book_ts_ms == 0)
            ? 0
            : (exch_ts_ms - state.last_book_ts_ms);
        state.last_book_ts_ms = exch_ts_ms;

        // Build Deribit-style book JSON to feed L5OrderBook
        json book_update;
        book_update["change_id"]      = data.value("u", 0LL);
        book_update["prev_change_id"] = state.book.change_id;
        book_update["timestamp"]      = exch_ts_ms;
        book_update["bids"]           = json::array();
        book_update["asks"]           = json::array();

        // Bids (data.b: [ [price, size], ... ] )
        if (data.contains("b") && data["b"].is_array()) {
            for (const auto& lvl : data["b"]) {
                if (!lvl.is_array() || lvl.size() < 2) continue;
                double px  = std::stod(lvl[0].get<std::string>());
                double qty = std::stod(lvl[1].get<std::string>());
                std::string ev = (qty == 0.0) ? "delete" : "change";
                book_update["bids"].push_back(
                    json::array({ev, px, qty})
                );
            }
        }

        // Asks (data.a: [ [price, size], ... ] )
        if (data.contains("a") && data["a"].is_array()) {
            for (const auto& lvl : data["a"]) {
                if (!lvl.is_array() || lvl.size() < 2) continue;
                double px  = std::stod(lvl[0].get<std::string>());
                double qty = std::stod(lvl[1].get<std::string>());
                std::string ev = (qty == 0.0) ? "delete" : "change";
                book_update["asks"].push_back(
                    json::array({ev, px, qty})
                );
            }
        }

        // Apply to L5 book
        if (type == "snapshot") {
            state.book.apply_snapshot(book_update);
        } else { // "delta"
            state.book.apply_delta(book_update);
        }

        state.mid_price = state.book.mid();

        // ---------- Build feature JSON (same shape as Deribit) ----------
        json out;
        out["ev"]           = "BOOK";
        out["message_type"] = (type == "snapshot" ? "SNAPSHOT" : "DELTA");
        out["instrument"]   = instr;

        out["change_id"]        = state.book.change_id;
        out["prev_change_id"]   = state.book.prev_change_id;
        out["exchange_ts_ms"]   = state.book.exchange_ts_ms;
        out["ingest_ts_ns"]     = local_ns;
        out["feed_latency_ns"]  = feed_latency_ns;
        out["dt_last_book_ms"]  = dt_last_book_ms;

        // L5 arrays
        out["bid_px"]  = json::array();
        out["bid_qty"] = json::array();
        out["ask_px"]  = json::array();
        out["ask_qty"] = json::array();

        for (int i=0;i<5;++i) {
            out["bid_px"].push_back(state.book.bid_px[i]);
            out["bid_qty"].push_back(state.book.bid_qty[i]);
            out["ask_px"].push_back(state.book.ask_px[i]);
            out["ask_qty"].push_back(state.book.ask_qty[i]);
        }

        out["best_bid"] = state.book.best_bid();
        out["best_ask"] = state.book.best_ask();
        out["mid"]      = state.book.mid();

        // Instrument metadata
        if (instrument_meta_.count(instr)) {
            const auto& meta = instrument_meta_.at(instr);
            out["instrument_meta"] = {
                {"instrument_name", meta.instrument_name},
                {"base_asset",      meta.base_asset},
                {"quote_asset",     meta.quote_asset},
                {"tick_size",       meta.tick_size},
                {"lot_size",        meta.lot_size},
                {"contract_value",  meta.contract_value}
            };
        }

        // Cross-asset: mids for all instruments
        json mids = json::object();
        for (const auto& kv : instruments_) {
            mids[kv.first] = kv.second.mid_price;
        }
        out["all_mid_price"] = mids;

        std::cout << out.dump() << "\n";

        // ===== Call FeatureEngine =====
        std::string inst = out["instrument"].get<std::string>();
        auto &eng = g_feature_engine[inst];
        eng.update_from_book(out);
    }

    // ======================= TRADES HANDLER =======================

    void handle_trades(const json& msg) {
        const auto& trades = msg.at("data");
        long long msg_ts   = msg.value("ts", 0LL);

        for (const auto& t : trades) {
            const std::string instr = t.at("s").get<std::string>();

            auto it = instruments_.find(instr);
            if (it == instruments_.end()) {
                it = instruments_.emplace(instr, InstrumentState(instr)).first;
            }
            auto& state = it->second;

            long long exch_ts_ms = t.value("T", msg_ts);
            long long local_ns   = now_ns();
            long long local_ms   = now_ms();
            long long feed_latency_ns = (exch_ts_ms > 0)
                ? (local_ms - exch_ts_ms) * 1'000'000LL
                : 0LL;

            long long dt_last_trade_ms = (state.last_trade_ts_ms == 0)
                ? 0
                : (exch_ts_ms - state.last_trade_ts_ms);
            state.last_trade_ts_ms = exch_ts_ms;

            // price & size are strings
            double trade_price = std::stod(t.at("p").get<std::string>());
            double trade_qty   = std::stod(t.at("v").get<std::string>());
            std::string dir    = t.at("S").get<std::string>(); // "Buy"/"Sell"
            std::string side   = (dir == "Buy" ? "BUY" : "SELL");

            // Optional mark/index (mostly for options, but keep for symmetry)
            double mark_price  = 0.0;
            if (t.contains("mP") && !t["mP"].is_null()) {
                mark_price = std::stod(t["mP"].get<std::string>());
            }
            double index_price = 0.0;
            if (t.contains("iP") && !t["iP"].is_null()) {
                index_price = std::stod(t["iP"].get<std::string>());
            }
            std::string trade_id = t.value("i", std::string(""));

            // Build feature JSON (same form as Deribit)
            json out;
            out["ev"]             = "TRADE";
            out["message_type"]   = "TRADE";
            out["instrument"]     = instr;

            out["trade_price"]    = trade_price;
            out["trade_qty"]      = trade_qty;
            out["trade_side"]     = side;
            out["exchange_ts_ms"] = exch_ts_ms;
            out["ingest_ts_ns"]   = local_ns;
            out["feed_latency_ns"]   = feed_latency_ns;
            out["dt_last_trade_ms"]  = dt_last_trade_ms;

            out["mark_price"]     = mark_price;
            out["index_price"]    = index_price;
            out["trade_id"]       = trade_id;

            // instrument metadata
            if (instrument_meta_.count(instr)) {
                const auto& meta = instrument_meta_.at(instr);
                out["instrument_meta"] = {
                    {"instrument_name", meta.instrument_name},
                    {"base_asset",      meta.base_asset},
                    {"quote_asset",     meta.quote_asset},
                    {"tick_size",       meta.tick_size},
                    {"lot_size",        meta.lot_size},
                    {"contract_value",  meta.contract_value}
                };
            }

            // current mid + all mids
            out["mid"] = state.mid_price;
            json mids = json::object();
            for (const auto& kv : instruments_) {
                mids[kv.first] = kv.second.mid_price;
            }
            out["all_mid_price"] = mids;

            std::cout << out.dump() << "\n";

            // ===== Call FeatureEngine =====
            std::string inst = out["instrument"].get<std::string>();
            auto &eng = g_feature_engine[inst];
            eng.update_from_trade(out);
        }
    }

    // ======================= Time helpers =======================

    static long long now_ns() {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(
            system_clock::now().time_since_epoch()).count();
    }

    static long long now_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()).count();
    }

    // ======================= Error =======================

    void fail(beast::error_code ec, char const* what) {
        std::cerr << "❌ [Bybit] " << what << ": " << ec.message() << "\n";
    }

public:
    // For setting SNI from main():
    auto& get_ssl_stream() {
        return ws_.next_layer();
    }
};
