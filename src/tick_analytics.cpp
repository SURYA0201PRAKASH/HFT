#include "tick_analytics.hpp"
#include <numeric>
#include <cmath>
#include <algorithm>
#include "market_data_recorder.hpp"

double TickAnalytics::calculate_volatility(size_t lookback_ticks) const {
    if (price_changes_.size() < lookback_ticks) return 0.0;
    
    std::vector<double> returns(
        price_changes_.end() - lookback_ticks, 
        price_changes_.end()
    );
    
    double mean = std::accumulate(returns.begin(), returns.end(), 0.0) / returns.size();
    double variance = 0.0;
    
    for (double ret : returns) {
        variance += (ret - mean) * (ret - mean);
    }
    variance /= returns.size();
    
    return std::sqrt(variance);
}

double TickAnalytics::calculate_vwap(const std::vector<Tick>& ticks) const {
    double total_value = 0.0;
    double total_volume = 0.0;
    
    for (const auto& tick : ticks) {
        if (tick.type == TickType::TRADE) {
            total_value += tick.price * tick.quantity;
            total_volume += tick.quantity;
        }
    }
    
    return total_volume > 0 ? total_value / total_volume : 0.0;
}

bool TickAnalytics::is_large_trade(const Tick& tick, double threshold_multiplier) const {
    if (tick.type != TickType::TRADE) return false;
    if (volumes_.size() < 10) return false;
    
    double avg_volume = std::accumulate(
        volumes_.end() - 10, volumes_.end(), 0.0) / 10.0;
    
    return tick.quantity > avg_volume * threshold_multiplier;
}

void TickAnalytics::add_tick(const Tick& tick) {
    double last_price = price_changes_.empty() ? tick.price : price_changes_.back();
    double change = (tick.price - last_price) / last_price;
    price_changes_.push_back(change);
    volumes_.push_back(tick.quantity);

    if (price_changes_.size() > window_size_) {
        price_changes_.pop_front();
        volumes_.pop_front();
    }
}


#ifdef ZMQ_BUILD
#include <nlohmann/json.hpp>
#include <iostream>

TickAnalytics::~TickAnalytics() {
    stop();
}

void TickAnalytics::start_pull_server(const std::string& address, const Config& cfg) {
    std::filesystem::create_directories("../data");
    recorder_ = std::make_unique<MarketDataRecorder>(cfg);
    std::cout << "🧩 Recorder created, writing to "
              << cfg.recording.sqlite_path << std::endl;
    recorder_->start_recording();

    try {
        // ✅ Bind a ZeroMQ PULL socket to receive ticks from TickProcessor
        zmq_pull_.bind(address);
        std::cout << "📊 [TickAnalytics] Listening for analytics ticks on " << address << std::endl;
    } catch (const zmq::error_t& e) {
        std::cerr << "❌ [TickAnalytics] Failed to bind: " << e.what() << std::endl;
        return;
    }

    running_ = true;

    // 🧵 Launch background thread to process incoming tick messages
    pull_thread_ = std::make_unique<std::thread>([this]() {
        zmq::message_t msg;
        while (running_) {
            try {
                if (!zmq_pull_.recv(msg, zmq::recv_flags::none))
                    continue;

                std::string payload(static_cast<char*>(msg.data()), msg.size());
                auto j = nlohmann::json::parse(payload);

                // ✅ Convert JSON to Tick
                Tick tick = Tick::from_json(j);
                add_tick(tick);

                // --- Compute derived analytics ---
                double mid = tick.price;                // If you don't have bid/ask, mid = price
                double rolling_vol = calculate_volatility(100);
                bool is_large = is_large_trade(tick);

                // --- Compute indicator set ---
                Indicators ind = update_indicators(states_[tick.instrument],
                                                   mid, tick.price, tick.quantity);

                // --- Record everything ---
                if (recorder_) {
                    recorder_->record_with_indicators(tick, rolling_vol, is_large, ind);
                }

                /* Optional debug output
                std::cout << "📈 [TickAnalytics] "
                          << tick.instrument
                          << " price=" << tick.price
                          << " vol=" << rolling_vol
                          << (is_large ? " ⚠️ LARGE TRADE" : "")
                          << std::endl;
                */

            } catch (const std::exception& e) {
                std::cerr << "❌ [TickAnalytics] Error: " << e.what() << std::endl;
            }
        }

        // 🧹 Clean shutdown
        zmq_pull_.close();
        zmq_ctx_.close();
    });  // ✅ closes lambda properly
}


void TickAnalytics::stop() {
    running_ = false;
    if (pull_thread_ && pull_thread_->joinable())
        pull_thread_->join();
}
#endif
