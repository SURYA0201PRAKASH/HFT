#include "enhanced_deribit_client.hpp"

#include <iostream>
#include <chrono>
#include <atomic>
#include <mutex>
#include <thread>

#include "sequence_manager.hpp"
#include "order_book.hpp"
#include "l1_cache.hpp"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#ifdef ZMQ_BUILD
#include <zmq.h>
#endif

// Global L1 cache definition
std::unordered_map<std::string, L1Cache> g_l1;

EnhancedDeribitClient::EnhancedDeribitClient(boost::asio::io_context& ioc,
                                             ssl::context& ctx,
                                             const std::string& host,
                                             const std::string& port,
                                             const std::string& target,
                                             const std::string& client_id,
                                             const std::string& client_secret)
    : DeribitClient(ioc, ctx, host, port, target, client_id, client_secret)
    , book_manager_()
    , logger_(std::make_unique<EnhancedLogger>("deribit_market_data"))
    , tick_processor_(std::make_unique<TickProcessor>(book_manager_))
{
    std::cout << "🔧 EnhancedDeribitClient constructor" << std::endl;
    std::cout << "🔧 Book manager address: " << &book_manager_ << std::endl;
    std::cout << "🔧 Tick processor address: " << tick_processor_.get() << std::endl;

    if (tick_processor_) {
        std::cout << "🔧 Tick processor->book_manager address: "
                  << &(tick_processor_->get_book_manager()) << std::endl;
    }

    verify_initialization();

#ifdef ZMQ_BUILD
    // === Initialize ZeroMQ Publisher ===
    zmq_ctx_ = zmq_ctx_new();
    zmq_pub_ = zmq_socket(zmq_ctx_, ZMQ_PUB);

    int hwm = 100000;
    zmq_setsockopt(zmq_pub_, ZMQ_SNDHWM, &hwm, sizeof(hwm));

    int linger = 0;
    zmq_setsockopt(zmq_pub_, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_bind(zmq_pub_, zmq_endpoint_.c_str()) != 0) {
        std::cerr << "❌ ZMQ bind failed: " << zmq_strerror(zmq_errno()) << std::endl;
    } else {
        std::cout << "✅ ZMQ publisher bound to " << zmq_endpoint_ << std::endl;
    }

    // Start subscriber inside TickProcessor
    tick_processor_->start_subscriber("tcp://127.0.0.1:5555");

    // Start TickAnalytics pull server via OrderBookManager
    book_manager_.start_pull_server("tcp://127.0.0.1:6000");
#endif
}

void EnhancedDeribitClient::verify_initialization() {
    std::cout << "✅ VERIFYING INITIALIZATION:" << std::endl;
    std::cout << "   Book manager valid: " << (&book_manager_ != nullptr) << std::endl;
    std::cout << "   Tick processor valid: " << (tick_processor_ != nullptr) << std::endl;

    if (tick_processor_) {
        std::cout << "   Tick processor active: " << tick_processor_.get() << std::endl;
        std::cout << "   Buffer capacity: " << tick_processor_->get_buffer_capacity() << std::endl;
        std::cout << "   Buffer size: " << tick_processor_->get_buffer_size() << std::endl;

        OrderBookManager& proc_book_mgr = tick_processor_->get_book_manager();
        std::cout << "   Address match: " << (&book_manager_ == &proc_book_mgr) << std::endl;
    }
}

void EnhancedDeribitClient::debug_tick_processor_status() {
    if (tick_processor_) {
        std::cout << "🔍 TICK PROCESSOR STATUS: "
                  << " | Buffer: " << tick_processor_->get_buffer_size()
                  << "/" << tick_processor_->get_buffer_capacity()
                  << " | Processed: " << tick_processor_->get_ticks_processed()
                  << " | Dropped: " << tick_processor_->get_ticks_dropped()
                  << std::endl;
    } else {
        std::cout << "❌ TICK PROCESSOR IS NULL!" << std::endl;
    }
}

void EnhancedDeribitClient::on_read(const beast::error_code& ec,
                                    std::size_t bytes_transferred)
{
    if (ec) {
        std::cout << "❌ [EnhancedDeribitClient::on_read] Error detected" << std::endl;
        std::cout << "🔄 [EnhancedDeribitClient] Stopping tick processing due to disconnect"
                  << std::endl;
        stop_tick_processing();
    }

    static std::atomic<int> message_count{0};
    static std::mutex message_log_mutex;

    {
        std::lock_guard lock(message_log_mutex);
        (void) ++message_count;
        // You can re-enable first-N logs if needed
    }

    static auto last_recovery_check = std::chrono::steady_clock::now();
    auto now = std::chrono::steady_clock::now();

    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_recovery_check).count() >= 30) {
        check_sequence_recovery();
        last_recovery_check = now;
    }

    if (ec) {
        std::cerr << "Read error: " << ec.message() << "\n";
        connected_ = false;

        beast::error_code close_ec;
        ws_.next_layer().shutdown(close_ec);
        ws_.next_layer().next_layer().close(close_ec);

        reconnect();
        return;
    }

    std::string data = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());

    logger_->log_raw_message(data);

    try {
        auto j = nlohmann::json::parse(data);

        // First: push ticks into TickProcessor (trade + L1-backed book ticks)
        process_tick_data(j);

        // Then: existing protocol logic
        if (j.contains("id") && j["id"] == 1) {
            if (j.contains("result")) {
                // Auth OK → subscribe
                send_subscribe();
            } else if (j.contains("error")) {
                std::cerr << "❌ Auth failed: " << j["error"]["message"] << "\n";
                return;
            }
        }
        else if (j.contains("id") && j["id"] == 2) {
            if (j.contains("result")) {
                // Subscription success
                start_keepalive();

                request_order_book_snapshot("BTC-PERPETUAL");
                request_order_book_snapshot("ETH-PERPETUAL");

                start_tick_processing();
            } else if (j.contains("error")) {
                std::cerr << "❌ Subscription failed: " << j["error"]["message"] << "\n";
            }
        }
        else if (j.contains("method") && j["method"] == "subscription") {
            const auto& params  = j["params"];
            const std::string& channel = params["channel"];

            if (channel.find("book.") != std::string::npos) {
                handle_order_book_update(params);
            } else if (channel.find("trades.") != std::string::npos) {
                handle_trade(params); // now just a stub message
            }
        }
        else if (j.contains("id") && j["id"] >= 1000 && j["id"] < 2000) {
            // Snapshot responses
            if (j.contains("result")) {
                const auto& result = j["result"];
                if (result.contains("instrument_name")) {
                    std::string instrument = result["instrument_name"];
                    book_manager_.update_from_snapshot(instrument, result);
                    logger_->log_snapshot(instrument, result);
                }
            }
        }

        static auto last_message_time = now;
        auto time_diff = std::chrono::duration_cast<std::chrono::microseconds>(
            now - last_message_time);
        last_message_time = now;

        (void) time_diff; // re-enable if you want timing logs

    } catch (const std::exception& e) {
        std::cerr << "⚠️ JSON parse error: " << e.what() << "\n";
    }

    do_read();
}

// ================== TICK PIPELINE ==================

void EnhancedDeribitClient::process_tick_data(const nlohmann::json& j) {
    static int call_count = 0;

    if (call_count++ % 100 == 0) {
        debug_tick_processor_status();
    }

    if (!j.contains("method") || j["method"] != "subscription")
        return;

    const auto& params  = j["params"];
    const std::string& channel = params["channel"];

    auto receive_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    if (channel.find("trades.") != std::string::npos) {
        const auto& trades_data = params["data"];

        if (trades_data.is_array()) {
            int trades_processed = 0;
            size_t buffer_before = tick_processor_
                ? tick_processor_->get_buffer_size()
                : 0;

            for (const auto& trade_data : trades_data) {
                Tick tick = create_trade_tick(trade_data, channel);
                tick.timestamp_ns = receive_time;
                tick.enqueue_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count();

                if (trade_data.contains("timestamp")) {
                    uint64_t timestamp_ms = trade_data["timestamp"].get<uint64_t>();
                    tick.exchange_timestamp = timestamp_ms * 1'000'000ULL;
                }

                if (tick_processor_) {
                    tick_processor_->push_tick(tick);
#ifdef ZMQ_BUILD
                    publish_tick_zmq(tick);
#endif
                    if (trades_processed < 5) {
                        size_t buffer_after = tick_processor_->get_buffer_size();
                        (void) buffer_before;
                        buffer_before = buffer_after;
                    }
                    trades_processed++;
                }

                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    }
    else if (channel.find("book.") != std::string::npos) {
        Tick tick = create_orderbook_tick(params["data"], channel);
        tick.timestamp_ns = receive_time;

        if (params["data"].contains("timestamp")) {
            uint64_t timestamp_ms = params["data"]["timestamp"].get<uint64_t>();
            tick.exchange_timestamp = timestamp_ms * 1'000'000ULL;
        }

        if (tick_processor_) {
            size_t buffer_before = tick_processor_->get_buffer_size();
            (void) buffer_before;

            tick_processor_->push_tick(tick);
#ifdef ZMQ_BUILD
            publish_tick_zmq(tick);
#endif
        }
    }
}

// TRADE TICK (injects L1 from global cache)
Tick EnhancedDeribitClient::create_trade_tick(const nlohmann::json& trade_data,
                                              const std::string& channel)
{
    Tick tick;
    tick.channel = channel;
    tick.type    = TickType::TRADE;

    // instrument from "trades.BTC-PERPETUAL.raw"
    size_t dot1 = channel.find('.');
    size_t dot2 = channel.find('.', dot1 + 1);
    tick.instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);

    // core fields
    tick.price    = safe_get_double(trade_data["price"]);
    tick.quantity = safe_get_double(trade_data["amount"]);
    tick.side     = trade_data.value("direction", "");
    tick.trade_id = trade_data.value("trade_id", "");
    tick.sequence = trade_data.value("trade_seq", 0);

    if (trade_data.contains("timestamp")) {
        uint64_t ts_ms = trade_data["timestamp"].get<uint64_t>();
        tick.exchange_timestamp = ts_ms * 1'000'000ULL;
    }

    // Inject L1
    auto it = g_l1.find(tick.instrument);
    if (it != g_l1.end()) {
        const L1Cache& l1 = it->second;
        tick.best_bid  = l1.best_bid;
        tick.best_ask  = l1.best_ask;
        tick.bid_vol_1 = l1.bid_vol_1;
        tick.ask_vol_1 = l1.ask_vol_1;

        if (l1.best_bid > 0 && l1.best_ask > 0) {
            tick.mid_price = (l1.best_bid + l1.best_ask) / 2.0;
        } else {
            tick.mid_price = tick.price;
        }
    } else {
        tick.best_bid  = 0.0;
        tick.best_ask  = 0.0;
        tick.bid_vol_1 = 0.0;
        tick.ask_vol_1 = 0.0;
        tick.mid_price = tick.price;
    }

    static int trade_debug_count = 0;
    if (trade_debug_count++ < 20) {
        std::cout << "🟦 TRADE TICK → "
                  << tick.instrument
                  << " | px=" << tick.price
                  << " | qty=" << tick.quantity
                  << " | bid=" << tick.best_bid
                  << " | ask=" << tick.best_ask
                  << " | mid=" << tick.mid_price
                  << " | seq=" << tick.sequence
                  << std::endl;
    }

    return tick;
}

// ORDERBOOK TICK (updates L1 + returns best bid/ask + mid)
Tick EnhancedDeribitClient::create_orderbook_tick(const nlohmann::json& book_data,
                                                  const std::string& channel)
{
    Tick tick;
    tick.channel = channel;

    // instrument from "book.BTC-PERPETUAL.raw"
    size_t dot1 = channel.find('.');
    size_t dot2 = channel.find('.', dot1 + 1);
    tick.instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);

    tick.type = (book_data.contains("type") && book_data["type"] == "snapshot")
                ? TickType::ORDERBOOK_SNAPSHOT
                : TickType::ORDERBOOK_DELTA;

    tick.sequence = book_data.value("change_id", 0);

    auto extract_px_qty = [](const nlohmann::json& levels,
                             double& out_px,
                             double& out_qty) -> bool
    {
        if (!levels.is_array()) return false;

        for (const auto& lvl : levels) {
            if (!lvl.is_array() || lvl.size() < 3) continue;

            std::string op = lvl[0].is_string() ? lvl[0].get<std::string>() : "";
            if (op == "new" || op == "change") {
                if (lvl[1].is_number() && lvl[2].is_number()) {
                    out_px  = lvl[1].get<double>();
                    out_qty = lvl[2].get<double>();
                    return true;
                }
            }
        }
        return false;
    };

    // ==== UPDATE GLOBAL L1 CACHE ====
    auto& l1 = g_l1[tick.instrument];

    // bids → highest bid
    if (book_data.contains("bids") && book_data["bids"].is_array()) {
        double best_px  = l1.best_bid;
        double best_qty = l1.bid_vol_1;
        double px = 0.0, qty = 0.0;

        for (const auto& lvl : book_data["bids"]) {
            if (!lvl.is_array() || lvl.size() < 3) continue;

            std::string op = lvl[0].is_string() ? lvl[0].get<std::string>() : "";
            if (op == "new" || op == "change") {
                if (lvl[1].is_number() && lvl[2].is_number()) {
                    px  = lvl[1].get<double>();
                    qty = lvl[2].get<double>();
                    if (px > 0 && (px > best_px)) {
                        best_px  = px;
                        best_qty = qty;
                    }
                }
            }
        }

        if (best_px > 0.0) {
            l1.best_bid  = best_px;
            l1.bid_vol_1 = best_qty;
        }
    }

    // asks → lowest ask
    if (book_data.contains("asks") && book_data["asks"].is_array()) {
        double best_px  = (l1.best_ask > 0.0) ? l1.best_ask : 0.0;
        double best_qty = (l1.best_ask > 0.0) ? l1.ask_vol_1 : 0.0;
        double px = 0.0, qty = 0.0;

        for (const auto& lvl : book_data["asks"]) {
            if (!lvl.is_array() || lvl.size() < 3) continue;

            std::string op = lvl[0].is_string() ? lvl[0].get<std::string>() : "";
            if (op == "new" || op == "change") {
                if (lvl[1].is_number() && lvl[2].is_number()) {
                    px  = lvl[1].get<double>();
                    qty = lvl[2].get<double>();
                    if (px > 0 && (best_px == 0.0 || px < best_px)) {
                        best_px  = px;
                        best_qty = qty;
                    }
                }
            }
        }

        if (best_px > 0.0) {
            l1.best_ask  = best_px;
            l1.ask_vol_1 = best_qty;
        }
    }

    if (l1.best_bid > 0.0 && l1.best_ask > 0.0) {
        tick.mid_price = (l1.best_bid + l1.best_ask) / 2.0;
    } else {
        // fallback to some price from the message
        double px_dummy = 0.0, qty_dummy = 0.0;
        if (book_data.contains("bids") && extract_px_qty(book_data["bids"], px_dummy, qty_dummy)) {
            tick.mid_price = px_dummy;
        } else if (book_data.contains("asks") && extract_px_qty(book_data["asks"], px_dummy, qty_dummy)) {
            tick.mid_price = px_dummy;
        } else {
            tick.mid_price = 0.0;
        }
    }

    tick.best_bid  = l1.best_bid;
    tick.best_ask  = l1.best_ask;
    tick.bid_vol_1 = l1.bid_vol_1;
    tick.ask_vol_1 = l1.ask_vol_1;

    static int dbg = 0;
    if (dbg++ < 20) {
        std::cout << "📘 OB TICK → " << tick.instrument
                  << " | bid=" << tick.best_bid
                  << " | ask=" << tick.best_ask
                  << " | mid=" << tick.mid_price
                  << " | seq=" << tick.sequence
                  << std::endl;
    }

    return tick;
}

// ================== ORDER BOOK / TRADES (legacy hooks) ==================

void EnhancedDeribitClient::handle_order_book_update(const nlohmann::json& data) {
    try {
        const std::string& channel = data["channel"];
        const auto& book_data = data["data"];

        size_t dot1 = channel.find('.');
        size_t dot2 = channel.find('.', dot1 + 1);
        std::string instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);

        // Only update OrderBookManager + logs
        if (book_data.contains("type") && book_data["type"] == "snapshot") {
            book_manager_.update_from_snapshot(instrument, book_data);
            logger_->log_snapshot(instrument, book_data);
        } else {
            book_manager_.apply_delta_update(instrument, book_data);
        }

        logger_->log_parsed_book(book_data, instrument);

        static int count = 0;
        if (++count % 100 == 0) {
            book_manager_.print_top_of_book(instrument);
        }

    } catch (const std::exception& e) {
        std::cerr << "Error handling order book update: " << e.what() << std::endl;
    }
}

void EnhancedDeribitClient::handle_trade(const nlohmann::json& /*data*/) {
    static int trade_log_count = 0;
    if (trade_log_count++ < 5) {
        // std::cout << "🔧 Trades are now processed through tick system" << std::endl;
    }
}

// ================== SNAPSHOT / GAP RECOVERY ==================

void EnhancedDeribitClient::request_order_book_snapshot(const std::string& instrument) {
    static int request_id = 1000;
    int current_request_id = request_id++;

    net::steady_timer timer(ws_.get_executor());
    timer.expires_after(std::chrono::milliseconds(200));

    timer.async_wait(
        [self = shared_from_this(), instrument, current_request_id]
        (boost::system::error_code ec) mutable
        {
            if (!ec) {
                nlohmann::json request = {
                    {"jsonrpc", "2.0"},
                    {"id", current_request_id},
                    {"method", "public/get_order_book"},
                    {"params", {
                        {"instrument_name", instrument},
                        {"depth", 25}
                    }}
                };
                self->send_text(request.dump());
            }
        }
    );
}

void EnhancedDeribitClient::request_missing_sequences(const std::string& instrument,
                                                      uint64_t from_seq,
                                                      uint64_t to_seq)
{
    static int request_id = 5000;
    int current_id = request_id++;

    std::cout << "🎯 REQUESTING MISSING DATA for " << instrument
              << ": sequences " << from_seq << " to " << to_seq << std::endl;

    nlohmann::json request = {
        {"jsonrpc", "2.0"},
        {"id", current_id},
        {"method", "public/get_trades_by_sequence"},
        {"params", {
            {"instrument_name", instrument},
            {"start_sequence", from_seq},
            {"end_sequence", to_seq},
            {"count", 1000}
        }}
    };

    send_text(request.dump());
}

void EnhancedDeribitClient::check_sequence_recovery() {
    auto& manager = get_order_book_manager();
    std::vector<std::string> instruments = {"BTC-PERPETUAL", "ETH-PERPETUAL"};

    for (const auto& instrument : instruments) {
        if (auto book = manager.get_order_book(instrument)) {
            book->check_and_request_missing_sequences(
                [this, instrument](uint64_t from, uint64_t to) {
                    request_missing_sequences(instrument, from, to);
                }
            );
        }
    }
}

void EnhancedDeribitClient::test_sequence_recovery() {
    std::cout << "\n🧪 TESTING SEQUENCE RECOVERY SYSTEM" << std::endl;

    auto& manager = get_order_book_manager();
    if (auto eth_book = manager.get_order_book("ETH-PERPETUAL")) {
        auto* seq_mgr = eth_book->get_sequence_manager();
        if (seq_mgr) {
            std::cout << "TEST 1: Perfect sequence 1, 2, 3" << std::endl;
            seq_mgr->set_last_valid_sequence(0);
            seq_mgr->on_sequence_received(1, [](uint64_t, uint64_t){});
            seq_mgr->on_sequence_received(2, [](uint64_t, uint64_t){});
            seq_mgr->on_sequence_received(3, [](uint64_t, uint64_t){});

            std::cout << "TEST 2: Gap detection 3 → 7" << std::endl;
            seq_mgr->on_sequence_received(7, [](uint64_t from, uint64_t to) {
                std::cout << "   🔄 Gap callback: Requesting " << from
                          << " to " << to << std::endl;
            });

            seq_mgr->print_stats();
        } else {
            std::cout << "❌ Sequence manager not initialized" << std::endl;
        }
    }
}

// ================== ZMQ PUBLISHER ==================
#ifdef ZMQ_BUILD
void EnhancedDeribitClient::teardown_zmq_publisher() {
    tick_processor_->stop_subscriber();

    if (zmq_pub_) {
        zmq_close(zmq_pub_);
        zmq_pub_ = nullptr;
    }
    if (zmq_ctx_) {
        zmq_ctx_term(zmq_ctx_);
        zmq_ctx_ = nullptr;
    }
    std::cout << "🧹 ZMQ publisher closed cleanly." << std::endl;
}

void EnhancedDeribitClient::publish_tick_zmq(const Tick& tick) {
    if (!zmq_pub_) return;

    std::string topic = tick.instrument;

    json j = {
        {"instrument", tick.instrument},
        {"type", (int)tick.type},
        {"price", tick.price},
        {"quantity", tick.quantity},
        {"sequence", tick.sequence},
        {"exchange_ts_ns", tick.exchange_timestamp},
        {"ingress_ts_ns", tick.timestamp_ns}
    };

    std::string payload = j.dump();

    int rc1 = zmq_send(zmq_pub_, topic.data(), topic.size(), ZMQ_SNDMORE);
    int rc2 = zmq_send(zmq_pub_, payload.data(), payload.size(), 0);

    if (rc1 < 0 || rc2 < 0) {
        std::cerr << "⚠️ ZMQ send failed for " << topic << ": "
                  << zmq_strerror(zmq_errno()) << std::endl;
    }
}
#endif

// ================== IExchangeClient interface ==================

void EnhancedDeribitClient::connect() {
    std::cout << "🔗 [Deribit] Connecting..." << std::endl;
    run();
}

void EnhancedDeribitClient::subscribe(const std::string& symbol) {
    std::cout << "📡 [Deribit] Subscribing to " << symbol << std::endl;
    send_subscribe();
}
