#include "enhanced_deribit_client.hpp"
#include "tick_analytics.hpp"
#include <boost/asio/ssl.hpp>
#include <iostream>
#include <cstdlib>
#include <csignal>
#include <thread>
#include <chrono>
#include <iomanip>
#include "trading_strategy.hpp"   // ✅ Required for TradingStrategy class
#include "config_loader.hpp"
#include "exchange_interface.hpp"
#include "bybit_client.hpp"

std::shared_ptr<EnhancedDeribitClient> global_client = nullptr;
std::atomic<bool> running{true};  // Add this global running flag

// NEW: Tick-based strategy
class TickBasedStrategy {
private:
    TickAnalytics analytics_;
    
public:
    void on_tick(const Tick& tick) {
        analytics_.add_tick(tick);
        
        if (tick.type == TickType::TRADE) {
            double volatility = analytics_.calculate_volatility(50);
            bool large_trade = analytics_.is_large_trade(tick);
            
            if (large_trade) {
//                 std::cout << "🚨 LARGE TRADE: " << tick.instrument 
//                          << " " << tick.quantity << " @ " << tick.price 
//                          << " (Volatility: " << volatility * 10000 << " bps)" << std::endl;
            }
            
            if (volatility > 0.001) {
//                 std::cout << "📈 High volatility detected: " 
//                          << tick.instrument << " - " << volatility * 10000 << " bps" << std::endl;
            }
        }
    }
};

void signal_handler(int signal) {
    std::cout << "\n🛑 Received signal " << signal << ", shutting down...\n";
    running.store(false, std::memory_order_release);

    if (global_client) {
        auto& manager = global_client->get_order_book_manager();
        manager.stop_pull_server();               // ✅ Stop ZMQ pull thread
        global_client->stop_tick_processing();    // ✅ Stop tick processor
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    exit(signal);
}

// Enhanced status display with tick statistics
void print_status_and_books() {
    static int empty_count = 0;

    // Hold last valid pointers for safe fallback
    static std::shared_ptr<OrderBookManager> last_manager = nullptr;
    static std::shared_ptr<TickProcessor> last_processor = nullptr;

    while (running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::seconds(2));

        if (!global_client || !running.load(std::memory_order_acquire))
            continue;

        auto& manager = global_client->get_order_book_manager();
        auto& tick_processor = global_client->get_tick_processor();

        // Non-owning pointers for display
        last_manager = std::shared_ptr<OrderBookManager>(&manager, [](auto*) {});
        last_processor = std::shared_ptr<TickProcessor>(&tick_processor, [](auto*) {});

        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::string time_str = std::ctime(&now_time);
        time_str.pop_back(); // remove newline

        std::cout << "\n==================== 🧠 SYSTEM STATUS ====================\n";
        std::cout << "🕒 Time: " << time_str << std::endl;

        // --- TICK PROCESSOR METRICS ---
        std::cout << "📊 Tick Processor Stats\n"
                  << "   ├─ Processed: " << tick_processor.get_ticks_processed()
                  << " | Dropped: " << tick_processor.get_ticks_dropped()
                  << "\n   ├─ Feed Latency: "
                  << std::fixed << std::setprecision(3)
                  << tick_processor.get_avg_latency_ms() << " ms"
                  << " | Queue Latency: "
                  << tick_processor.get_avg_queue_latency_us() << " µs"
                  << "\n   └─ Buffer: "
                  << tick_processor.get_buffer_size() << " / "
                  << tick_processor.get_buffer_capacity()
                  << std::endl;

        // --- ORDER BOOK SNAPSHOT ---
        if (auto btc_book = manager.get_order_book("BTC-PERPETUAL")) {
            std::cout << "🔷 BTC-PERPETUAL"
                      << " | Bid: " << std::fixed << std::setprecision(2) << btc_book->get_bid()
                      << " | Ask: " << btc_book->get_ask()
                      << " | Mid: " << btc_book->get_mid_price()
                      << " | Spread: " << btc_book->get_spread()
                      << std::endl;
        }

        if (auto eth_book = manager.get_order_book("ETH-PERPETUAL")) {
            std::cout << "🔶 ETH-PERPETUAL"
                      << " | Bid: " << std::fixed << std::setprecision(2) << eth_book->get_bid()
                      << " | Ask: " << eth_book->get_ask()
                      << " | Mid: " << eth_book->get_mid_price()
                      << " | Spread: " << eth_book->get_spread()
                      << std::endl;
        }

        std::cout << "-----------------------------------------------------------\n";
    }

    std::cout << "📊 Status thread stopped gracefully.\n";
}

void launch_deribit(const Config& cfg) {
    std::string client_id     = "ykCxoRwu";
    std::string client_secret = "25wBQ-OaL-_DKbf1YLSsvzHPiLNLUx_nuKF2QYHrlgo";

    boost::asio::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);

    std::string host = "test.deribit.com";
    std::string port = "443";
    std::string target = "/ws/api/v2";

#ifdef ZMQ_BUILD
    TickAnalytics analytics;
    analytics.start_pull_server("tcp://127.0.0.1:6000",cfg);
#endif

    // --- Create Deribit client ---
    global_client = std::make_shared<EnhancedDeribitClient>(
        ioc, ctx, host, port, target, client_id, client_secret
    );

    global_client->get_tick_processor().set_config(cfg);

#ifdef ZMQ_BUILD
    // ZMQ setup
    auto& manager = global_client->get_order_book_manager();
    manager.start_publisher("tcp://127.0.0.1:6500");

    TradingStrategy trade_strategy;
    trade_strategy.start_subscriber("tcp://127.0.0.1:6500", "BTC-PERPETUAL");

    global_client->get_tick_processor().start_subscriber("tcp://127.0.0.1:5555");
    std::cout << "📡 ZMQ subscriber connected to tcp://127.0.0.1:5555\n";
#endif

    // Tick-based analytics strategy
    TickBasedStrategy strategy;
    global_client->get_tick_processor().register_tick_callback(
        [&strategy](const Tick& tick) { strategy.on_tick(tick); }
    );

    // --- Start status monitor thread ---
    std::thread status_thread(print_status_and_books);
    status_thread.detach();

    // --- Run Deribit client ---
    global_client->run();
    ioc.run();  // blocking; clean shutdown via signal_handler

    // --- Graceful shutdown ---
    std::cout << "\n🛑 Disconnecting Deribit...\n";

    if (global_client) {
        try {
            global_client->stop_tick_processing();

#ifdef ZMQ_BUILD
            auto& manager = global_client->get_order_book_manager();
            manager.stop_pull_server();
#endif
        } catch (const std::exception& e) {
            std::cerr << "⚠️ Deribit stop error: " << e.what() << "\n";
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    global_client.reset();

    std::cout << "✅ Deribit client stopped cleanly.\n";
}

void launch_bybit(const Config& cfg) {
    boost::asio::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);

    // --- Create recorder using full Config (not 2 strings) ---
    auto recorder = std::make_shared<MarketDataRecorder>(cfg);
    std::cout << "🧩 Recorder created, writing to " 
              << cfg.recording.sqlite_path << std::endl;

    // --- Create Bybit WS client ---
    auto bybit_client = std::make_shared<BybitWsClient>(ioc, ctx);
    bybit_client->set_recorder(recorder);  // attach the recorder
    std::cout << "🧠 Recorder attached to Bybit client @ " << bybit_client.get()
              << " (recorder=" << recorder.get() << ")" << std::endl;

    // --- Subscribe all configured symbols ---
    for (const auto& sym : cfg.symbols) {
        (void)sym;  // currently unused, topic is hardcoded inside subscribe()
        bybit_client->subscribe();
    }

    // --- Start connection & event loop ---
    bybit_client->run();   // ✅ instead of connect()
    ioc.run();
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Modes:
    //   ./hft_prototype deribit  → only Deribit
    //   ./hft_prototype bybit    → only Bybit
    //   ./hft_prototype both     → Deribit + Bybit in parallel
    std::string mode = "deribit";
    if (argc > 1) {
        mode = argv[1];
    }

    try {
        if (mode == "deribit") {
            Config deribit_cfg = load_config("configs/base_deribit.yaml");
            std::cout << "🧭 Using configuration: configs/base_deribit.yaml"
                      << " | Exchange: " << deribit_cfg.exchange << std::endl;
            launch_deribit(deribit_cfg);
        }
        else if (mode == "bybit") {
            Config bybit_cfg = load_config("configs/base_bybit.yaml");
            std::cout << "🧭 Using configuration: configs/base_bybit.yaml"
                      << " | Exchange: " << bybit_cfg.exchange << std::endl;
            launch_bybit(bybit_cfg);
        }
        else if (mode == "both") {
            Config deribit_cfg = load_config("configs/base_deribit.yaml");
            Config bybit_cfg   = load_config("configs/base_bybit.yaml");

            std::cout << "🧭 Starting BOTH exchanges in parallel:\n"
                      << "   - Deribit config: configs/base_deribit.yaml ("
                      << deribit_cfg.exchange << ")\n"
                      << "   - Bybit   config: configs/base_bybit.yaml ("
                      << bybit_cfg.exchange << ")\n";

            // Run Deribit + Bybit in two threads
            std::thread deribit_thread([&](){
                launch_deribit(deribit_cfg);
            });

            std::thread bybit_thread([&](){
                launch_bybit(bybit_cfg);
            });

            deribit_thread.join();
            bybit_thread.join();
        }
        else {
            std::cerr << "❌ Unknown mode: " << mode
                      << " (use 'deribit', 'bybit', or 'both')" << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
