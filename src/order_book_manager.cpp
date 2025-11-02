#include "order_book_manager.hpp"
#include <iostream>
#include <mutex>
#include "trading_strategy.hpp"
#include <fstream>
#include <sqlite3.h>
#include <chrono>

OrderBookManager::OrderBookManager() {}

std::shared_ptr<OrderBook> OrderBookManager::get_or_create_book(const std::string& instrument) {
    std::unique_lock lock(books_mutex_);
    auto it = order_books_.find(instrument);
    if (it == order_books_.end()) {
        order_books_[instrument] = std::make_shared<OrderBook>(instrument);
        // Also create enhanced book
        enhanced_books_[instrument] = std::make_shared<EnhancedOrderBook>(instrument);
    }
    return order_books_[instrument];
}

std::shared_ptr<EnhancedOrderBook> OrderBookManager::get_or_create_enhanced_book(const std::string& instrument) {
    std::unique_lock lock(books_mutex_);
    auto it = enhanced_books_.find(instrument);
    if (it == enhanced_books_.end()) {
        enhanced_books_[instrument] = std::make_shared<EnhancedOrderBook>(instrument);
        // Also ensure regular book exists
        order_books_[instrument] = std::make_shared<OrderBook>(instrument);
    }
    return enhanced_books_[instrument];
}

// NEW: Process incoming ticks
void OrderBookManager::process_tick(const Tick& tick) {
    static uint64_t processed_count = 0;

    // 🔎 ENTER: basic tick info
    //std::cout << "🧠 [DEBUG][OBM::process_tick] ENTER  "
    //          << "inst=" << tick.instrument
    //          << " | type=" << static_cast<int>(tick.type)
    //          << " | px=" << tick.price
    //          << " | qty=" << tick.quantity
    //          << " | seq=" << tick.sequence
    //          << " | exch_ns=" << tick.exchange_timestamp
    //          << " | ingress_ns=" << tick.timestamp_ns
    //          << std::endl;

    // 🧪 Sanity checks (don’t return early—just warn so we see the state)
    if (tick.instrument.empty()) {
        //std::cout << "⚠️ [DEBUG][OBM] Empty instrument in tick" << std::endl;
    }
    if (tick.exchange_timestamp == 0 || tick.timestamp_ns == 0) {
        //std::cout << "⚠️ [DEBUG][OBM] Missing timestamps exch/ingress" << std::endl;
    }

    // (Optional) If you have a mutex for the books map, consider scoping it here:
    // std::lock_guard<std::mutex> lk(books_mutex_);

    // 🔎 Snapshot container state before
    size_t map_size_before = order_books_.size();
    //std::cout << "📚 [DEBUG][OBM] books.size(before)=" << map_size_before << std::endl;

    try {
        // 🔧 Obtain (or create) the enhanced book
        auto enhanced_book = get_or_create_enhanced_book(tick.instrument);

        // Trace pointer and existence
        //std::cout << "📌 [DEBUG][OBM] book ptr=" << static_cast<const void*>(enhanced_book.get())
        //          << " | inst=" << tick.instrument
        //          << std::endl;

        if (!enhanced_book) {
            //std::cout << "❌ [DEBUG][OBM] get_or_create_enhanced_book returned null for "
            //          << tick.instrument << std::endl;
        } else {
            // 🔧 Add tick
            enhanced_book->add_tick(tick);
            ++processed_count;

            if (processed_count <= 10 || processed_count % 1000 == 0) {
                //std::cout << "✅ [DEBUG][OBM] add_tick OK | count=" << processed_count
                //          << " | inst=" << tick.instrument
                //          << " | px=" << tick.price
                //          << " | qty=" << tick.quantity
                //          << " | seq=" << tick.sequence
                //          << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ [DEBUG][OBM::process_tick] Exception: " << e.what()
                  << " | inst=" << tick.instrument
                  << " | px=" << tick.price
                  << " | qty=" << tick.quantity
                  << " | seq=" << tick.sequence
                  << std::endl;
    } catch (...) {
        std::cerr << "❌ [DEBUG][OBM::process_tick] Unknown exception"
                  << " | inst=" << tick.instrument
                  << " | px=" << tick.price
                  << " | qty=" << tick.quantity
                  << " | seq=" << tick.sequence
                  << std::endl;
    }

    // 🔎 Snapshot container state after
    size_t map_size_after = order_books_.size();
    if (map_size_after != map_size_before) {
        //std::cout << "🧾 [DEBUG][OBM] books.size(after)=" << map_size_after
        //          << " (was " << map_size_before << ")" << std::endl;
    }

    // 🔚 EXIT
    //std::cout << "🏁 [DEBUG][OBM::process_tick] EXIT   "
    //          << "inst=" << tick.instrument
    //          << " | seq=" << tick.sequence
    //          << std::endl;
}


// order_book_manager.cpp - MODIFY THESE METHODS:

void OrderBookManager::update_from_snapshot(const std::string& instrument, const nlohmann::json& data) {
    // Update regular book (existing code)
    auto book = get_or_create_book(instrument);
    book->update_from_snapshot(data);
    
    // NEW: Also update the enhanced book with the same snapshot data
    auto enhanced_book = get_or_create_enhanced_book(instrument);
    enhanced_book->update_from_snapshot(data);  // ← CRITICAL: Sync the levels!
    
    std::ofstream out("/mnt/c/Group_project/hft-prototype/orderbooks/orderbook_" + instrument + ".json");
    out << book->to_json().dump(2);
    out.close();
}

void OrderBookManager::apply_delta_update(const std::string& instrument, const nlohmann::json& data) {
    auto book = get_or_create_book(instrument);
    book->apply_delta_update(data);

    auto enhanced_book = get_or_create_enhanced_book(instrument);
    enhanced_book->apply_delta_update(data);

    TradingStrategy strategy;
    strategy.check_imbalance(instrument, *book);

    // ✅ INSERT SQLite logging here
    sqlite3* db;
    if (sqlite3_open("../build/orderbook_data.db", &db) == SQLITE_OK){
		sqlite3_busy_timeout(db, 3000);  // wait up to 3 seconds if DB is locked
        const char* create_sql =
            "CREATE TABLE IF NOT EXISTS orderbook ("
            "timestamp INTEGER, "
            "instrument TEXT, "
            "side TEXT, "
            "price REAL, "
            "amount REAL);";
        sqlite3_exec(db, create_sql, nullptr, nullptr, nullptr);

        uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        auto bids = book->get_top_bids(10);
        auto asks = book->get_top_asks(10);

        sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

        for (const auto& b : bids) {
            std::string sql = "INSERT INTO orderbook VALUES (" + std::to_string(timestamp) +
                              ", '" + instrument + "', 'BID', " + std::to_string(b.price) +
                              ", " + std::to_string(b.amount) + ");";
            sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);
        }

        for (const auto& a : asks) {
            std::string sql = "INSERT INTO orderbook VALUES (" + std::to_string(timestamp) +
                              ", '" + instrument + "', 'ASK', " + std::to_string(a.price) +
                              ", " + std::to_string(a.amount) + ");";
            sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);
        }

        sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
        sqlite3_close(db);
    }

    // ✅ Existing JSON dump (keep this too)
    std::ofstream out("/mnt/c/Group_project/hft-prototype/orderbooks/orderbook_" + instrument + ".json");
    out << book->to_json().dump(2);
    out.close();
}


std::shared_ptr<OrderBook> OrderBookManager::get_order_book(const std::string& instrument) {
    std::shared_lock lock(books_mutex_);
    auto it = order_books_.find(instrument);
    return it != order_books_.end() ? it->second : nullptr;
}

std::shared_ptr<EnhancedOrderBook> OrderBookManager::get_enhanced_order_book(const std::string& instrument) {
    std::shared_lock lock(books_mutex_);
    auto it = enhanced_books_.find(instrument);
    return it != enhanced_books_.end() ? it->second : nullptr;
}

// Rest of your existing methods...

void OrderBookManager::print_top_of_book(const std::string& instrument) {
    if (auto book = get_order_book(instrument)) {
//         std::cout << book->to_string() << std::endl;
    }
}

nlohmann::json OrderBookManager::get_book_json(const std::string& instrument) {
    if (auto book = get_order_book(instrument)) {
        return book->to_json();
    }
    return nlohmann::json();
}

#ifdef ZMQ_BUILD
#include <zmq.hpp>
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#endif

#ifdef ZMQ_BUILD
void OrderBookManager::start_pull_server(const std::string& address) {
    if (pull_running_.exchange(true)) {
        //std::cout << "⚠️ [OrderBookManager] PULL server already running\n";
        return;
    }
    pull_thread_ = std::thread([this, address]() {
        try {
            zmq::context_t ctx(1);
            zmq::socket_t pull_socket(ctx, zmq::socket_type::pull);
            pull_socket.set(zmq::sockopt::rcvhwm, 100000);
            pull_socket.set(zmq::sockopt::linger, 0);
            pull_socket.bind(address);
            //std::cout << "✅ [OrderBookManager] Listening for ticks on " << address << std::endl;

            while (pull_running_.load(std::memory_order_acquire)) {
                zmq::message_t msg;
                auto res = pull_socket.recv(msg, zmq::recv_flags::dontwait);
                if (!res) {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                    continue;
                }

                // Parse & process
                std::string payload(static_cast<char*>(msg.data()), msg.size());
                auto tick_json = json::parse(payload);

                Tick tick;
                tick.instrument         = tick_json.value("instrument", "");
                tick.price              = tick_json.value("price", 0.0);
                tick.quantity           = tick_json.value("quantity", 0.0);
                tick.sequence           = tick_json.value("sequence", 0ull);
                tick.timestamp_ns       = tick_json.value("timestamp_ns", 0ull);
                tick.exchange_timestamp = tick_json.value("exchange_ts_ns", 0ull);
                tick.type = static_cast<TickType>(tick_json.value("type", 0));

                this->process_tick(tick);

                // (Debug prints kept, but consider gating them)
                /*std::cout << "📥 [OrderBookManager] Received tick from ZMQ | "
                          << tick.instrument << " | Price=" << tick.price
                          << " | Qty=" << tick.quantity << std::endl;
                std::cout << "🧩 Tick integrity check: "
                          << "Instrument=" << tick.instrument
                          << " | Price=" << tick.price
                          << " | Qty=" << tick.quantity
                          << " | Seq=" << tick.sequence
                          << " | ExchangeTS=" << tick.exchange_timestamp
                          << " | TimestampNS=" << tick.timestamp_ns
                          << std::endl;*/

                if (tick.price == 0 || tick.quantity == 0) {
                    std::cout << "⚠️ WARNING: Incomplete tick received (Price=0 or Qty=0)\n";
                }
            }

            pull_socket.close();
        } catch (const zmq::error_t& e) {
            std::cerr << "❌ [OrderBookManager] ZMQ Error: " << e.what() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "❌ [OrderBookManager] Exception: " << e.what() << std::endl;
        }
    });
}
#endif

#ifdef ZMQ_BUILD
void OrderBookManager::stop_pull_server() {
    if (!pull_running_.load(std::memory_order_acquire)) {
        //std::cout << "ℹ️ [OrderBookManager] Pull server is not running." << std::endl;
        return;
    }

    //std::cout << "🛑 [OrderBookManager] Stopping pull server..." << std::endl;
    pull_running_.store(false, std::memory_order_release);

    if (pull_thread_.joinable()) {
        pull_thread_.join();
    }

    //std::cout << "✅ [OrderBookManager] Pull server stopped cleanly." << std::endl;
}
#endif

#ifdef ZMQ_BUILD
void OrderBookManager::start_publisher(const std::string& address) {
    try {
        zmq_pub_.bind(address);
        zmq_pub_active_.store(true, std::memory_order_release);
        //std::cout << "✅ [OrderBookManager] ZMQ PUB bound to " << address << std::endl;
    } catch (const zmq::error_t& e) {
        std::cerr << "❌ [OrderBookManager] PUB bind failed: " << e.what() << std::endl;
    }
}

void OrderBookManager::stop_publisher() {
    if (zmq_pub_active_) {
        zmq_pub_.close();
        zmq_pub_active_.store(false, std::memory_order_release);
        //std::cout << "🛑 [OrderBookManager] ZMQ PUB stopped." << std::endl;
    }
}

void OrderBookManager::publish_book_update(const std::string& instrument, double bid, double ask) {
    if (!zmq_pub_active_) return;

    nlohmann::json msg = {
        {"instrument", instrument},
        {"bid", bid},
        {"ask", ask},
        {"timestamp_ns", static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()
        )}
    };

    std::string topic = instrument;
    std::string payload = msg.dump();

    zmq::message_t topic_msg(topic.begin(), topic.end());
    zmq::message_t data_msg(payload.begin(), payload.end());

    zmq_pub_.send(topic_msg, zmq::send_flags::sndmore);
    zmq_pub_.send(data_msg, zmq::send_flags::none);

    std::cout << "📢 [OrderBookManager→Strategy] PUB sent | " << instrument 
              << " | Bid=" << bid << " | Ask=" << ask << std::endl;
}
#endif



