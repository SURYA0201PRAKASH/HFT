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
// Enhanced status display with tick statistics
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


int main() {
	Config cfg = load_config("configs/base.yaml"); // 23rd Oct 2025
    try {
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        std::string client_id     = "ykCxoRwu";
        std::string client_secret = "25wBQ-OaL-_DKbf1YLSsvzHPiLNLUx_nuKF2QYHrlgo";
        
        if (client_id.empty() || client_secret.empty()) {
            std::cerr << "Error: Deribit credentials are empty\n";
            return 1;
        }

        boost::asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);

        std::string host = "test.deribit.com";
        std::string port = "443";
        std::string target = "/ws/api/v2";

//         std::cout << "🚀 Starting Tick-by-Tick HFT System...\n";
//         std::cout << "📊 Will display real-time tick analytics every 2 seconds\n";
//         std::cout << "Press Ctrl+C to stop...\n";
        #ifdef ZMQ_BUILD
        TickAnalytics analytics;
        analytics.start_pull_server("tcp://127.0.0.1:6000");
        #endif
        // Create enhanced client
        global_client = std::make_shared<EnhancedDeribitClient>(
            ioc, ctx, host, port, target, client_id, client_secret
        );
		global_client->get_tick_processor().set_config(cfg); // 23rd Oct 2025
		#ifdef ZMQ_BUILD
		// 🔹 Start PUB/SUB bridge between OrderBookManager → TradingStrategy
		auto& manager = global_client->get_order_book_manager();
        manager.start_publisher("tcp://127.0.0.1:6500");

        TradingStrategy trade_strategy;
        trade_strategy.start_subscriber("tcp://127.0.0.1:6500", "BTC-PERPETUAL");
        #endif
		#ifdef ZMQ_BUILD
		// ✅ Start ZeroMQ subscriber inside TickProcessor
		global_client->get_tick_processor().start_subscriber("tcp://127.0.0.1:5555");
		std::cout << "📡 ZMQ subscriber connected to tcp://127.0.0.1:5555" << std::endl;
		#endif
        // Setup tick-based strategy
        TickBasedStrategy strategy;
        
        // Register tick callback for real-time processing
        global_client->get_tick_processor().register_tick_callback(
            [&strategy](const Tick& tick) {
                strategy.on_tick(tick);
            }
        );

        // Start the enhanced status display thread
        std::thread status_thread(print_status_and_books);
        status_thread.detach();
        
        // Start the main client (tick processing starts automatically after auth)
        global_client->run();
        ioc.run();
        
//         std::cout << "👋 Application shutdown complete." << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}