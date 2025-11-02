#pragma once
#include <functional>
#include <string>
#include <cstdint>

struct Tick {
    std::string instrument;
    double      price  = 0.0;
    double      qty    = 0.0;
    std::int64_t exch_ts = 0;   // exchange timestamp ms
};

class IMarketDataFeed {
public:
    using TickHandler = std::function<void(const Tick&)>;

    virtual ~IMarketDataFeed() = default;
    virtual void start() = 0;
    virtual void stop()  = 0;

    void set_handler(TickHandler h) { handler_ = std::move(h); }

protected:
    TickHandler handler_;
};
