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



// ========================= SimpleDeribitClient =========================

class SimpleDeribitClient {
public:
    SimpleDeribitClient(net::io_context& ioc, ssl::context& ctx)
        : resolver_(ioc)
        , ws_(ioc, ctx)
    {
        // Init instrument states (BTC & ETH)
        instruments_.emplace("BTC-PERPETUAL", InstrumentState("BTC-PERPETUAL"));
        instruments_.emplace("ETH-PERPETUAL", InstrumentState("ETH-PERPETUAL"));

        // Init metadata (fill more accurately later if needed)
        instrument_meta_["BTC-PERPETUAL"] = InstrumentMeta{
            "BTC-PERPETUAL", "BTC", "USD", 0.5, 0.0, 0.0
        };
        instrument_meta_["ETH-PERPETUAL"] = InstrumentMeta{
            "ETH-PERPETUAL", "ETH", "USD", 0.05, 0.0, 0.0
        };
    }

    void connect() {
        resolver_.async_resolve(
            "test.deribit.com", "443",
            beast::bind_front_handler(
                &SimpleDeribitClient::on_resolve, this));
    }

private:
    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    beast::flat_buffer buffer_;

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
                &SimpleDeribitClient::on_connect, this));
    }

    void on_connect(beast::error_code ec,
                    tcp::resolver::results_type::endpoint_type)
    {
        if (ec) return fail(ec, "connect");

        ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            beast::bind_front_handler(
                &SimpleDeribitClient::on_ssl_handshake, this));
    }

    void on_ssl_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ssl_handshake");

        ws_.set_option(
            websocket::stream_base::timeout::suggested(
                beast::role_type::client));

        ws_.async_handshake(
            "test.deribit.com", "/ws/api/v2",
            beast::bind_front_handler(
                &SimpleDeribitClient::on_ws_handshake, this));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        send_auth();
    }

    // ======================= AUTH / SUBSCRIBE =======================

    void send_auth() {
        json auth = {
            {"jsonrpc","2.0"},
            {"id",1},
            {"method","public/auth"},
            {"params",{
                {"grant_type","client_credentials"},
                {"client_id","ykCxoRwu"},
                {"client_secret","25wBQ-OaL-_DKbf1YLSsvzHPiLNLUx_nuKF2QYHrlgo"}
            }}
        };

        ws_.async_write(
            net::buffer(auth.dump()),
            beast::bind_front_handler(
                &SimpleDeribitClient::on_auth_sent, this));
    }

    void on_auth_sent(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "auth_send");
        do_read();
    }

    void send_subscribe() {
        json sub = {
            {"jsonrpc","2.0"},
            {"id",2},
            {"method","public/subscribe"},
            {"params",{
                {"channels",{
                    "book.BTC-PERPETUAL.raw",
                    "trades.BTC-PERPETUAL.raw",
                    "book.ETH-PERPETUAL.raw",
                    "trades.ETH-PERPETUAL.raw"
                }}
            }}
        };

        ws_.async_write(
            net::buffer(sub.dump()),
            beast::bind_front_handler(
                &SimpleDeribitClient::on_sub_sent, this));
    }

    void on_sub_sent(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "sub_send");
        std::cout << "📡 Subscribed\n";
        do_read();
    }

    // ======================= READ LOOP =======================

    void do_read() {
        ws_.async_read(
            buffer_,
            beast::bind_front_handler(
                &SimpleDeribitClient::on_read, this));
    }

    void on_read(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "read");

        std::string data = beast::buffers_to_string(buffer_.data());
        buffer_.consume(buffer_.size());

        // Parse JSON
        json msg;
        try {
            msg = json::parse(data);
        } catch (...) {
            // Raw dump if not JSON
            std::cerr << "⚠ Non-JSON message: " << data << "\n";
            return do_read();
        }

        // AUTH response → trigger subscribe
        if (msg.contains("id") && msg["id"] == 1 && msg.contains("result")) {
            std::cout << "🔐 Auth OK → subscribing…\n";
            send_subscribe();
            return;
        }

        // HEARTBEAT
        if (msg.contains("method") && msg["method"] == "heartbeat") {
            handle_heartbeat(msg);
            return do_read();
        }

        // Subscription messages (orderbook & trades)
        if (msg.contains("method") && msg["method"] == "subscription") {
            handle_subscription(msg);
            return do_read();
        }

        // Default: ignore
        do_read();
    }

    // ======================= HANDLERS =======================

    void handle_heartbeat(const json& msg) {
        // You can enrich this if you want
        json out;
        out["ev"]            = "HEARTBEAT";
        out["message_type"]  = "HEARTBEAT";
        out["ingest_ts_ns"]  = now_ns();

        std::cout << out.dump() << "\n";
    }

    void handle_subscription(const json& msg) {
        const auto& params  = msg.at("params");
        const std::string channel = params.at("channel").get<std::string>();

        // MESSAGE CLASSIFICATION
        if (channel.rfind("book.", 0) == 0) {
            handle_book(params);
        } else if (channel.rfind("trades.", 0) == 0) {
            handle_trades(params);
        }
    }

    void handle_book(const json& params) {
        const auto& data = params.at("data");
        const std::string instr = data.at("instrument_name").get<std::string>();
        const std::string type  = data.at("type").get<std::string>(); // snapshot / change

        // Ensure instrument state exists
        auto it = instruments_.find(instr);
        if (it == instruments_.end()) {
            it = instruments_.emplace(instr, InstrumentState(instr)).first;
        }
        auto& state = it->second;

        long long exch_ts_ms = data.value("timestamp", 0LL);
        long long local_ns   = now_ns();
        long long local_ms   = now_ms();
        long long feed_latency_ns = (exch_ts_ms > 0)
            ? (local_ms - exch_ts_ms) * 1'000'000LL
            : 0LL;

        long long dt_last_book_ms = (state.last_book_ts_ms == 0)
            ? 0
            : (exch_ts_ms - state.last_book_ts_ms);
        state.last_book_ts_ms = exch_ts_ms;

        // Update L5 book
        if (type == "snapshot") {
            state.book.apply_snapshot(data);
        } else { // "change"
            state.book.apply_delta(data);
        }

        state.mid_price = state.book.mid();

        // ---------- Build feature JSON ----------
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

    void handle_trades(const json& params) {
        const auto& trades = params.at("data");

        for (const auto& t : trades) {
            const std::string instr = t.at("instrument_name").get<std::string>();

            auto it = instruments_.find(instr);
            if (it == instruments_.end()) {
                it = instruments_.emplace(instr, InstrumentState(instr)).first;
            }
            auto& state = it->second;

            long long exch_ts_ms = t.value("timestamp", 0LL);
            long long local_ns   = now_ns();
            long long local_ms   = now_ms();
            long long feed_latency_ns = (exch_ts_ms > 0)
                ? (local_ms - exch_ts_ms) * 1'000'000LL
                : 0LL;

            long long dt_last_trade_ms = (state.last_trade_ts_ms == 0)
                ? 0
                : (exch_ts_ms - state.last_trade_ts_ms);
            state.last_trade_ts_ms = exch_ts_ms;

            double trade_price = t.at("price").get<double>();
            double trade_qty   = t.at("amount").get<double>();
            std::string dir    = t.at("direction").get<std::string>();
            std::string side   = (dir == "buy" ? "BUY" : "SELL");

            // optional fields
            double mark_price  = t.value("mark_price",  0.0);
            double index_price = t.value("index_price", 0.0);
            std::string trade_id = t.value("trade_id", std::string(""));

            // Build feature JSON
            json out;
            out["ev"]            = "TRADE";
            out["message_type"]  = "TRADE";
            out["instrument"]    = instr;

            out["trade_price"]   = trade_price;
            out["trade_qty"]     = trade_qty;
            out["trade_side"]    = side;
            out["exchange_ts_ms"]= exch_ts_ms;
            out["ingest_ts_ns"]  = local_ns;
            out["feed_latency_ns"] = feed_latency_ns;
            out["dt_last_trade_ms"] = dt_last_trade_ms;

            out["mark_price"]    = mark_price;
            out["index_price"]   = index_price;
            out["trade_id"]      = trade_id;

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
        std::cerr << "❌ " << what << ": " << ec.message() << "\n";
    }
	public:
    auto& get_ssl_stream() {
        return ws_.next_layer();
    }

};
