#include "enhanced_deribit_client.hpp"
#include <iostream>
#include <chrono>
#include "sequence_manager.hpp"
#include "order_book.hpp"
#ifdef ZMQ_BUILD
#include <zmq.h>
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#endif
#include "l1_cache.hpp"

// Define global L1 cache here
std::unordered_map<std::string, L1Cache> g_l1;

EnhancedDeribitClient::EnhancedDeribitClient(boost::asio::io_context& ioc,
                                           ssl::context& ctx,
                                           const std::string& host,
                                           const std::string& port,
                                           const std::string& target,
                                           const std::string& client_id,
                                           const std::string& client_secret)
    : DeribitClient(ioc, ctx, host, port, target, client_id, client_secret)
    , book_manager_()  // Initialize first
    , logger_(std::make_unique<EnhancedLogger>("deribit_market_data"))
    , tick_processor_(std::make_unique<TickProcessor>(book_manager_))
{
    std::cout << "🔧 EnhancedDeribitClient constructor" << std::endl;
    std::cout << "🔧 Book manager address: " << &book_manager_ << std::endl;
    std::cout << "🔧 Tick processor address: " << tick_processor_.get() << std::endl;
    
    // FIXED: Use the new public method
    if (tick_processor_) {
        std::cout << "🔧 Tick processor->book_manager address: " 
                  << &(tick_processor_->get_book_manager()) << std::endl;
    }
    
    verify_initialization();  // Call verification method
	#ifdef ZMQ_BUILD
    // === Initialize ZeroMQ Publisher ===
    zmq_ctx_ = zmq_ctx_new();
    zmq_pub_ = zmq_socket(zmq_ctx_, ZMQ_PUB);

    int hwm = 100000;   // High-water mark to prevent overflow
    zmq_setsockopt(zmq_pub_, ZMQ_SNDHWM, &hwm, sizeof(hwm));

    int linger = 0;     // Don’t block on close
    zmq_setsockopt(zmq_pub_, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_bind(zmq_pub_, zmq_endpoint_.c_str()) != 0)
        std::cerr << "❌ ZMQ bind failed: " << zmq_strerror(zmq_errno()) << std::endl;
    else
        std::cout << "✅ ZMQ publisher bound to " << zmq_endpoint_ << std::endl;
	tick_processor_->start_subscriber("tcp://127.0.0.1:5555");
	#endif
	#ifdef ZMQ_BUILD
    book_manager_.start_pull_server("tcp://127.0.0.1:6000");
    #endif
}

// FIX THE VERIFICATION METHOD TOO:
void EnhancedDeribitClient::verify_initialization() {
    std::cout << "✅ VERIFYING INITIALIZATION:" << std::endl;
    std::cout << "   Book manager valid: " << (&book_manager_ != nullptr) << std::endl;
    std::cout << "   Tick processor valid: " << (tick_processor_ != nullptr) << std::endl;
    
    if (tick_processor_) {
        std::cout << "   Tick processor active: " << tick_processor_.get() << std::endl;
        std::cout << "   Buffer capacity: " << tick_processor_->get_buffer_capacity() << std::endl;
        std::cout << "   Buffer size: " << tick_processor_->get_buffer_size() << std::endl;
        
        // Check if book manager references match
        OrderBookManager& proc_book_mgr = tick_processor_->get_book_manager();
        std::cout << "   Address match: " << (&book_manager_ == &proc_book_mgr) << std::endl;
    }
}

// FIX THE DEBUG METHOD - IT SHOULD BE A MEMBER FUNCTION:
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

void EnhancedDeribitClient::on_read(const beast::error_code& ec, std::size_t bytes_transferred) {
	if (ec) {
        std::cout << "❌ [EnhancedDeribitClient::on_read] Error detected" << std::endl;
        std::cout << "🔄 [EnhancedDeribitClient] Stopping tick processing due to disconnect" << std::endl;
        stop_tick_processing();
    }
	//SuPr1 std::cout << "🔍 on_read callback, ec: " << ec.message() 
    //          << ", bytes: " << bytes_transferred 
    //          << ", connected: " << connected_ 
    //          << ", buffer size: " << buffer_.size() << "\n";
	static std::atomic<int> message_count{0};
    static std::mutex message_log_mutex;
    
    {
        std::lock_guard lock(message_log_mutex);
        int count = ++message_count;
        /*if (count <= 50) { // Log first 50 messages
            std::cout << "📨 MESSAGE #" << count << " received, size: " << bytes_transferred << " bytes\n";
        }*/
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
	// std::cout << "\n📩 RAW MESSAGE (" << bytes_transferred << " bytes): " 
    // 23rd Oct 2025      << data.substr(0, 400) << std::endl;

    logger_->log_raw_message(data);
    //std::cout << "Raw book data: " << data << std::endl;
	
    try {
        auto j = nlohmann::json::parse(data);

        // Process tick data first
        process_tick_data(j);

        // Then handle existing logic
        if (j.contains("id") && j["id"] == 1) {
            if (j.contains("result")) {
//                 std::cout << "✅ Auth successful, subscribing...\n";
                send_subscribe();
            } else if (j.contains("error")) {
                std::cerr << "❌ Auth failed: " << j["error"]["message"] << "\n";
                return;
            }
        }
		else if (j.contains("method") && j["method"] == "subscription") {
            const auto& params = j["params"];
            const std::string& channel = params["channel"];
            
            if (channel.find("book.") != std::string::npos) {
				//std::cout << "📘 BOOK UPDATE for " << channel << std::endl;
                handle_order_book_update(params);
            } else if (channel.find("trades.") != std::string::npos) {
                handle_trade(params);  // This will now just log, not process
				//std::cout << "📨 TRADE MESSAGE RECEIVED for " << channel << std::endl;
            }
        }
        else if (j.contains("id") && j["id"] == 2) {
            if (j.contains("result")) {
//                 std::cout << "✅ Subscription successful\n";
                start_keepalive();
                
                request_order_book_snapshot("BTC-PERPETUAL");
                request_order_book_snapshot("ETH-PERPETUAL");
                
                // Start tick processing after successful subscription
                start_tick_processing();
            } else if (j.contains("error")) {
                std::cerr << "❌ Subscription failed: " << j["error"]["message"] << "\n";
            }
        }
        else if (j.contains("method") && j["method"] == "subscription") {
            const auto& params = j["params"];
            const std::string& channel = params["channel"];
            
            if (channel.find("book.") != std::string::npos) {
				std::cout<<"aaaaaaaaaaa"<<std::endl;
                handle_order_book_update(params);
            } else if (channel.find("trades.") != std::string::npos) {
				std::cout<<"ccccccccccc"<<std::endl;
                handle_trade(params);
				std::cout << "📨 TRADE MESSAGE RECEIVED for " << channel << std::endl;
            }
        }
        else if (j.contains("id") && j["id"] >= 1000 && j["id"] < 2000) {
            if (j.contains("result")) {
                const auto& result = j["result"];
                if (result.contains("instrument_name")) {
                    std::string instrument = result["instrument_name"];
                    book_manager_.update_from_snapshot(instrument, result);
                    logger_->log_snapshot(instrument, result);
//                     std::cout << "✅ Snapshot received for " << instrument << "\n";
                }
            }
        }
		auto now = std::chrono::steady_clock::now();
        static auto last_message_time = now;
        auto time_diff = std::chrono::duration_cast<std::chrono::microseconds>(now - last_message_time);
        last_message_time = now;
        
        /*if (j.contains("method") && j["method"] == "subscription") {
            const auto& params = j["params"];
            const std::string& channel = params["channel"];
            
            std::cout << "⏱️ CHANNEL: " << channel 
                      << " | TIME SINCE LAST: " << time_diff.count() << "μs"
                      << " | DATA SIZE: " << (params.contains("data") ? params["data"].size() : 0)
                      << std::endl;
        }*/

    } catch (const std::exception& e) {
        std::cerr << "⚠️ JSON parse error: " << e.what() << "\n";
    }

    do_read();
}

// NEW: Process incoming messages as ticks
// FIXED: Proper JSON value extraction before multiplication

void EnhancedDeribitClient::process_tick_data(const nlohmann::json& j) {
    static int call_count = 0;
    
    // Debug every 100 calls to avoid spam
    if (call_count++ % 100 == 0) {
        debug_tick_processor_status();
    }
    
    if (j.contains("method") && j["method"] == "subscription") {
        const auto& params = j["params"];
        const std::string& channel = params["channel"];
        
        auto receive_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        
        if (channel.find("trades.") != std::string::npos) {
            // TRADE CHANNEL: Process each trade in the array as a separate tick
            const auto& trades_data = params["data"];
            
            if (trades_data.is_array()) {
                int trades_processed = 0;
                size_t buffer_before = tick_processor_ ? tick_processor_->get_buffer_size() : 0;
                
                for (const auto& trade_data : trades_data) {
                    Tick tick = create_trade_tick(trade_data, channel);
                    tick.timestamp_ns = receive_time;
                    tick.enqueue_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                    if (trade_data.contains("timestamp")) {
                        uint64_t timestamp_ms = trade_data["timestamp"].get<uint64_t>();
                        tick.exchange_timestamp = timestamp_ms * 1000000;
                    }
                    
                    if (tick_processor_) {
                        // push_tick returns void, so we can't get a bool result
                        tick_processor_->push_tick(tick);
                        #ifdef ZMQ_BUILD
						publish_tick_zmq(tick);
						#endif
                        if (trades_processed < 5) { // Log first 5 trades
                            size_t buffer_after = tick_processor_->get_buffer_size();
                            //std::cout << "🚀 PUSHED TRADE TICK: " << tick.instrument 
                            //         << " | Buffer: " << buffer_before << " -> " << buffer_after
                            //         << "/" << tick_processor_->get_buffer_capacity() << std::endl;
                            buffer_before = buffer_after;
                        }
                        trades_processed++;
                    }
                    
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }
        } 
        else if (channel.find("book.") != std::string::npos) {
            // ORDER BOOK CHANNEL: Process as single tick
            Tick tick = create_orderbook_tick(params["data"], channel);
            tick.timestamp_ns = receive_time;
            
            if (params["data"].contains("timestamp")) {
                uint64_t timestamp_ms = params["data"]["timestamp"].get<uint64_t>();
                tick.exchange_timestamp = timestamp_ms * 1000000;
            }
            
            if (tick_processor_) {
                size_t buffer_before = tick_processor_->get_buffer_size();
                
                // push_tick returns void, not bool
                tick_processor_->push_tick(tick);
                #ifdef ZMQ_BUILD
				publish_tick_zmq(tick);
				#endif
                if (call_count < 10) { // Log first 10 book updates
                    size_t buffer_after = tick_processor_->get_buffer_size();
                    //std::cout << "🚀 PUSHED BOOK TICK: " << tick.instrument 
                    //         << " | Buffer: " << buffer_before << " -> " << buffer_after
                    //         << "/" << tick_processor_->get_buffer_capacity() << std::endl;
                }
            }
        }
    }
}

// NEW: Separate method for creating trade ticks
Tick EnhancedDeribitClient::create_trade_tick(const nlohmann::json& trade_data, const std::string& channel) {
    Tick tick;
    tick.channel = channel;
    tick.type = TickType::TRADE;
    static int trade_creation_count = 0;
    if (trade_creation_count++ < 50) {
//         std::cout << "🔄 CREATING TRADE TICK #" << trade_creation_count 
//                  << " - " << tick.instrument 
//                  << " | Type value: " << static_cast<int>(tick.type) 
//                  << " | Price: " << tick.price << std::endl;
    }
    // Extract instrument from channel name
    size_t dot1 = channel.find('.');
    size_t dot2 = channel.find('.', dot1 + 1);
    tick.instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);
    
    // Parse trade data
    tick.price = safe_get_double(trade_data["price"]);
    tick.quantity = safe_get_double(trade_data["amount"]);
    tick.side = trade_data.value("direction", "");
    tick.trade_id = trade_data.value("trade_id", "");
    tick.sequence = trade_data.value("trade_seq", 0);
    std::cout << "🧾 TRADE RAW JSON: " << trade_data.dump() << std::endl;

    return tick;
}

Tick EnhancedDeribitClient::create_orderbook_tick(const nlohmann::json& book_data, const std::string& channel) {
    Tick tick;
    tick.channel = channel;
    
    // Extract instrument from channel name
    size_t dot1 = channel.find('.');
    size_t dot2 = channel.find('.', dot1 + 1);
    tick.instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);
    
    if (book_data.contains("type") && book_data["type"] == "snapshot") {
        tick.type = TickType::ORDERBOOK_SNAPSHOT;
    } else {
        tick.type = TickType::ORDERBOOK_DELTA;
    }
    tick.sequence = book_data.value("change_id", 0);
    // ==== NEW: extract a usable price/qty from book delta/snapshot ====
auto extract_px_qty = [](const nlohmann::json& levels,
                         double& out_px, double& out_qty,
                         std::string& out_side) -> bool
{
    // Deribit raw book entries look like: ["new" | "change" | "delete", price, amount]
    // We ignore "delete" because it has qty 0.0 and would keep tick.quantity at 0.
    if (!levels.is_array() || levels.empty()) return false;

    for (const auto& lvl : levels) {
        if (!lvl.is_array() || lvl.size() < 3) continue;

        const std::string op = lvl[0].is_string() ? lvl[0].get<std::string>() : "";
        if (op == "new" || op == "change") {
            // price and amount can be numbers
            if (lvl[1].is_number() && lvl[2].is_number()) {
                out_px  = lvl[1].get<double>();
                out_qty = lvl[2].get<double>();
                // out_side already set by caller to "bid"/"ask"
                (void)op; // silence unused if you want
                return true;
            }
        }
    }
    return false;
};

double px = 0.0, qty = 0.0;
std::string side_found;

// Prefer the side that actually changed in this message.
// If bids array has usable entries, mark as bid; else try asks.
bool got = false;
if (book_data.contains("bids")) {
    side_found = "bid";
    got = extract_px_qty(book_data["bids"], px, qty, side_found);
}
if (!got && book_data.contains("asks")) {
    side_found = "ask";
    got = extract_px_qty(book_data["asks"], px, qty, side_found);
}

if (got) {
    tick.price    = px;
    tick.quantity = qty;
    tick.side     = side_found; // optional but handy for downstream
} else {
    // Keep debug visible so we can see when nothing usable is present
    //std::cout << "⚠️ [DEBUG] No actionable levels in message for "
    //          << tick.instrument << " (all deletes?) | Seq=" << tick.sequence << std::endl;
}

// (optional) quick sanity print
//std::cout << "   🧾 Tick after extract → "
//          << "Instrument=" << tick.instrument
//          << " | Side=" << tick.side
//          << " | Price=" << tick.price
//          << " | Qty=" << tick.quantity
//          << " | Seq=" << tick.sequence
//          << std::endl;

    return tick;
}


// Rest of your existing methods remain the same...
void EnhancedDeribitClient::handle_order_book_update(const nlohmann::json& data) {
    try {
        const std::string& channel = data["channel"];
        const auto& book_data = data["data"];
        
        size_t dot1 = channel.find('.');
        size_t dot2 = channel.find('.', dot1 + 1);
        std::string instrument = channel.substr(dot1 + 1, dot2 - dot1 - 1);
        
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


void EnhancedDeribitClient::handle_trade(const nlohmann::json& data) {
    // COMMENT THIS OUT or modify to avoid duplicate processing
    /*
    try {
        const auto& trade_data = data["data"];
        logger_->log_trade(trade_data);
        
        // Add counter to reduce trade output
        static int trade_count = 0;
        if (++trade_count % 20 == 0) {
            for (const auto& trade : trade_data) {
                std::string instrument = trade["instrument_name"];
                double price = trade["price"];
                double amount = trade["amount"];
                std::string direction = trade["direction"];
                
//                 std::cout << "TRADE " << instrument << " " << direction 
                          << " " << amount << " @ " << price << "\n";
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error handling trade: " << e.what() << std::endl;
    }
    */
    
    // Instead, just log that we're processing trades through the new system
    static int trade_log_count = 0;
    if (trade_log_count++ < 5) {
//         std::cout << "🔧 Trades are now processed through tick system" << std::endl;
    }
}
void EnhancedDeribitClient::request_order_book_snapshot(const std::string& instrument) {
    static int request_id = 1000;
    int current_request_id = request_id++;  // Create local copy
    
    // Add delay before sending snapshot request
    net::steady_timer timer(ws_.get_executor());
    timer.expires_after(std::chrono::milliseconds(200));
    
    // FIX: Use local copy instead of static variable directly
    timer.async_wait([self = shared_from_this(), instrument, current_request_id](boost::system::error_code ec) mutable {
        if (!ec) {
            nlohmann::json request = {
                {"jsonrpc", "2.0"},
                {"id", current_request_id},  // Use local copy
                {"method", "public/get_order_book"},
                {"params", {
                    {"instrument_name", instrument},
                    {"depth", 25}
                }}
            };
            self->send_text(request.dump());
//             std::cout << "📋 Requesting snapshot for " << instrument << "\n";
        }
    });
}

void EnhancedDeribitClient::request_missing_sequences(const std::string& instrument, 
                                                     uint64_t from_seq, uint64_t to_seq) {
    static int request_id = 5000;
    int current_id = request_id++;
    
    std::cout << "🎯 REQUESTING MISSING DATA for " << instrument 
              << ": sequences " << from_seq << " to " << to_seq << std::endl;
    
    // Deribit-specific gap request (example - adjust based on actual API)
    nlohmann::json request = {
        {"jsonrpc", "2.0"},
        {"id", current_id},
        {"method", "public/get_trades_by_sequence"},
        {"params", {
            {"instrument_name", instrument},
            {"start_sequence", from_seq},
            {"end_sequence", to_seq},
            {"count", 1000} // Limit response size
        }}
    };
    
    send_text(request.dump());
}

void EnhancedDeribitClient::check_sequence_recovery() {
    auto& manager = get_order_book_manager();
    
    // Check both instruments
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
        auto* seq_mgr = eth_book->get_sequence_manager(); // Use pointer
        if (seq_mgr) {
            // Simulate perfect sequence
            std::cout << "TEST 1: Perfect sequence 1, 2, 3" << std::endl;
            seq_mgr->set_last_valid_sequence(0);
            seq_mgr->on_sequence_received(1, [](uint64_t, uint64_t){});
            seq_mgr->on_sequence_received(2, [](uint64_t, uint64_t){});
            seq_mgr->on_sequence_received(3, [](uint64_t, uint64_t){});
            
            // Simulate gap
            std::cout << "TEST 2: Gap detection 3 → 7" << std::endl;
            seq_mgr->on_sequence_received(7, [](uint64_t from, uint64_t to) {
                std::cout << "   🔄 Gap callback: Requesting " << from << " to " << to << std::endl;
            });
            
            seq_mgr->print_stats();
        } else {
            std::cout << "❌ Sequence manager not initialized" << std::endl;
        }
    }
}

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

    if (rc1 < 0 || rc2 < 0)
        std::cerr << "⚠️ ZMQ send failed for " << topic << ": "
                  << zmq_strerror(zmq_errno()) << std::endl;
	else{}
        //std::cout << "✅ [PUB] ZMQ Sent → Topic: " << topic
        //          << " | Payload Size: " << payload.size() << " bytes" << std::endl;
}
#endif
// ✅ Implement IExchangeClient interface methods
void EnhancedDeribitClient::connect() {
    std::cout << "🔗 [Deribit] Connecting..." << std::endl;
    this->run();   // triggers async connect chain from DeribitClient (DNS -> TLS -> WS)
}

void EnhancedDeribitClient::subscribe(const std::string& symbol) {
    std::cout << "📡 [Deribit] Subscribing to " << symbol << std::endl;
    this->send_subscribe();  // DeribitClient already defines this
}
