#include "strategy_engine.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    // args: [mode] [db_path] [symbol]
    std::string mode    = (argc > 1) ? argv[1] : "backtest";
    std::string db_path = (argc > 2) ? argv[2] : "../data/bybit_stream_features.db";
    std::string symbol  = (argc > 3) ? argv[3] : "BTCUSDT";

    StrategyEngine eng(symbol, /*spread_bps*/ 2.0, /*window*/ 20,
                       /*z_threshold*/ 0.001, /*qty*/ 0.1);

    if (mode == "live") {
        std::cout << "🚀 LIVE (polling DB): " << db_path << " | " << symbol << "\n";
        eng.run_live_poll(db_path, 1000);
    } else {
        std::cout << "📊 BACKTEST from: " << db_path << " | " << symbol << "\n";
        eng.run_backtest(db_path);
    }
    return 0;
}
