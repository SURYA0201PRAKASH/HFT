#pragma once
#include <deque>
#include <string>
#include <unordered_map>
#include <cmath>

struct Indicators {
    // Existing
    double ema12 = 0, ema26 = 0, macd = 0;
    double rsi14 = 0;
    double bb_mid = 0, bb_up = 0, bb_low = 0;
    double vwap = 0;
    double mid = 0, spread = 0;

    // New
    double best_bid = 0, best_ask = 0;
    double bid_vol_1 = 0, ask_vol_1 = 0;
    double bid_vol_5 = 0, ask_vol_5 = 0;
    double order_imbalance = 0, ofi = 0;
    double return_1s = 0, return_5s = 0;
    double realized_vol_1s = 0;
    double feed_latency_ms = 0, queue_latency_us = 0;
    double tick_gap_ms = 0, tick_rate = 0;
    double vol_zscore = 0;
    int hour_of_day = 0, minute_of_day = 0;
    int trend_label = 0;
    std::string exchange = "deribit";
};


struct IndicatorState {
    bool initialized=false;
    double ema12=0, ema26=0;
    double prev_price=0;
    double avg_gain=0, avg_loss=0;
    int rsi_count=0;

    std::deque<double> bb_win;
    double sum=0, sumsq=0;
    long double cum_pv=0, cum_vol=0;

    static constexpr double k12=2.0/(12.0+1.0);
    static constexpr double k26=2.0/(26.0+1.0);
    static constexpr int bb_period=20;
    static constexpr double bb_k=2.0;
};

inline Indicators update_indicators(IndicatorState& s,
                                    double mid,
                                    double trade_price,
                                    double trade_qty,
                                    double best_bid,
                                    double best_ask,
                                    double bid_vol_1,
                                    double ask_vol_1)
{
    Indicators o{};

    // ===== Initialization =====
    if (!s.initialized) {
        s.ema12 = s.ema26 = mid;
        s.prev_price = mid;
        s.initialized = true;
    }

    // ===== EMA / MACD =====
    s.ema12 += IndicatorState::k12 * (mid - s.ema12);
    s.ema26 += IndicatorState::k26 * (mid - s.ema26);
    o.ema12 = s.ema12;
    o.ema26 = s.ema26;
    o.macd  = s.ema12 - s.ema26;

    // ===== RSI(14) =====
    double chg = mid - s.prev_price;
    double gain = (chg > 0) ? chg : 0;
    double loss = (chg < 0) ? -chg : 0;

    if (s.rsi_count < 14) {
        s.avg_gain += gain;
        s.avg_loss += loss;
        s.rsi_count++;

        if (s.rsi_count == 14) {
            s.avg_gain /= 14.0;
            s.avg_loss /= 14.0;
        }

        o.rsi14 = 50.0;       // initialization phase
    } 
    else {
        s.avg_gain = (s.avg_gain * 13.0 + gain) / 14.0;
        s.avg_loss = (s.avg_loss * 13.0 + loss) / 14.0;

        double rs = (s.avg_loss <= 1e-12) ? 1e6 : (s.avg_gain / s.avg_loss);
        o.rsi14 = 100.0 - (100.0 / (1.0 + rs));
    }

    s.prev_price = mid;

    // ===== Bollinger Bands =====
    s.bb_win.push_back(mid);
    s.sum += mid;
    s.sumsq += mid * mid;

    if ((int)s.bb_win.size() > IndicatorState::bb_period) {
        double old = s.bb_win.front();
        s.bb_win.pop_front();
        s.sum -= old;
        s.sumsq -= old * old;
    }

    int n = s.bb_win.size();
    double mean = s.sum / n;
    double var = std::max(0.0, (s.sumsq / n) - mean * mean);
    double stddev = std::sqrt(var);

    o.bb_mid = mean;
    o.bb_up  = mean + IndicatorState::bb_k * stddev;
    o.bb_low = mean - IndicatorState::bb_k * stddev;

    // ===== VWAP =====
    if (trade_qty > 0) {
        s.cum_pv  += (long double)trade_price * trade_qty;
        s.cum_vol += trade_qty;
    }
    o.vwap = (s.cum_vol > 0) ? (double)(s.cum_pv / s.cum_vol) : trade_price;

    // ===== ORDERBOOK FIELDS (THIS IS THE FIX YOU ASKED FOR) =====
    o.best_bid  = best_bid;
    o.best_ask  = best_ask;
    o.bid_vol_1 = bid_vol_1;
    o.ask_vol_1 = ask_vol_1;

    // Correct spread calculation
    o.spread = (best_ask > 0 && best_bid > 0)
               ? (best_ask - best_bid)
               : 0.0;

    // OFI (Order Flow Imbalance)
    o.ofi = bid_vol_1 - ask_vol_1;

    // Normalized imbalance ratio
    o.order_imbalance = (bid_vol_1 - ask_vol_1) /
                        (bid_vol_1 + ask_vol_1 + 1e-9);

    // ===== Mid =====
    o.mid = mid;

    // ===== Trend Label =====
    o.trend_label = (o.ema12 > o.ema26) ? 1 : -1;

    // ===== Tick-to-tick return placeholder =====
    static double last_mid = mid;
    double ret = std::log(mid / (last_mid + 1e-9));

    o.return_1s = ret;
    o.realized_vol_1s = std::fabs(ret);

    last_mid = mid;

    // ===== Time features =====
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm = *std::localtime(&t);
    o.hour_of_day = local_tm.tm_hour;
    o.minute_of_day = local_tm.tm_min;

    return o;
}


