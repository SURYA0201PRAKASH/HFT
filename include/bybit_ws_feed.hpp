#pragma once
#include "exchange_feed.hpp"
#include <string>

class BybitWsFeed : public IMarketDataFeed {
public:
    // e.g. "BTCUSDT", "ETHUSDT"
    explicit BybitWsFeed(std::string symbol);

    void start() override;
    void stop() override;

private:
    std::string symbol_;
    bool running_ = false;
};
