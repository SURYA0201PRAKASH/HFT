#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <deque>
#include <cstdint>
#include <limits>
#include <nlohmann/json.hpp>
#include <unordered_map>

using json = nlohmann::json;

/**
 * Normalized microstructure tick produced by deribit_ws_client.cpp
 * This mirrors the JSON you already print.
 */
struct NormalizedTick {
    // Core identifiers
    std::string instrument;
    std::string message_type; // "BOOK" or "TRADE"
    std::string ev;           // "BOOK" or "TRADE" (you already set this)

    // Timestamps
    std::int64_t exchange_ts_ms   = 0;
    std::int64_t ingest_ts_ns     = 0;
    std::int64_t feed_latency_ns  = 0;
    std::int64_t dt_last_book_ms  = 0;
    std::int64_t dt_last_trade_ms = 0;

    // Book snapshot (L5)
    double best_bid = std::numeric_limits<double>::quiet_NaN();
    double best_ask = std::numeric_limits<double>::quiet_NaN();
    double mid      = std::numeric_limits<double>::quiet_NaN();

    std::vector<double> bid_px;  // size <= 5
    std::vector<double> bid_qty; // size <= 5
    std::vector<double> ask_px;  // size <= 5
    std::vector<double> ask_qty; // size <= 5

    // Sequence IDs
    std::int64_t change_id      = 0;
    std::int64_t prev_change_id = 0;

    // Trade fields (only valid if ev == "TRADE")
    bool   has_trade      = false;
    double trade_price    = std::numeric_limits<double>::quiet_NaN();
    double trade_qty      = std::numeric_limits<double>::quiet_NaN();
    std::string trade_side; // "BUY" / "SELL"
    double index_price    = std::numeric_limits<double>::quiet_NaN();
    double mark_price     = std::numeric_limits<double>::quiet_NaN();
    std::string trade_id;

    // Cross-asset mids
    std::unordered_map<std::string,double> all_mid_price;

    NormalizedTick() = default;
    explicit NormalizedTick(const json& j);
};

/**
 * FeatureEngine:
 *  - consumes NormalizedTick
 *  - maintains rolling state per instrument
 *  - outputs a feature vector as JSON
 */
class FeatureEngine {
public:
    struct FeatureVector {
        std::string instrument;
        std::int64_t exchange_ts_ms = 0;
        std::int64_t ingest_ts_ns   = 0;

        // QUOTE FEATURES
        double best_bid = std::numeric_limits<double>::quiet_NaN();
        double best_ask = std::numeric_limits<double>::quiet_NaN();
        double mid      = std::numeric_limits<double>::quiet_NaN();
        double log_mid  = std::numeric_limits<double>::quiet_NaN();
        double spread_abs = std::numeric_limits<double>::quiet_NaN();
        double spread_bps = std::numeric_limits<double>::quiet_NaN();
        double d_best_bid = std::numeric_limits<double>::quiet_NaN();
        double d_best_ask = std::numeric_limits<double>::quiet_NaN();
        double mid_return_1tick = std::numeric_limits<double>::quiet_NaN();
        double mid_return_1s    = std::numeric_limits<double>::quiet_NaN();
        double mid_return_5s    = std::numeric_limits<double>::quiet_NaN();
        double mid_return_10s   = std::numeric_limits<double>::quiet_NaN();

        // DEPTH & IMBALANCE
        double bid_px_1 = std::numeric_limits<double>::quiet_NaN();
        double bid_px_2 = std::numeric_limits<double>::quiet_NaN();
        double bid_px_3 = std::numeric_limits<double>::quiet_NaN();
        double bid_px_4 = std::numeric_limits<double>::quiet_NaN();
        double bid_px_5 = std::numeric_limits<double>::quiet_NaN();

        double ask_px_1 = std::numeric_limits<double>::quiet_NaN();
        double ask_px_2 = std::numeric_limits<double>::quiet_NaN();
        double ask_px_3 = std::numeric_limits<double>::quiet_NaN();
        double ask_px_4 = std::numeric_limits<double>::quiet_NaN();
        double ask_px_5 = std::numeric_limits<double>::quiet_NaN();

        double bid_qty_1 = std::numeric_limits<double>::quiet_NaN();
        double bid_qty_2 = std::numeric_limits<double>::quiet_NaN();
        double bid_qty_3 = std::numeric_limits<double>::quiet_NaN();
        double bid_qty_4 = std::numeric_limits<double>::quiet_NaN();
        double bid_qty_5 = std::numeric_limits<double>::quiet_NaN();

        double ask_qty_1 = std::numeric_limits<double>::quiet_NaN();
        double ask_qty_2 = std::numeric_limits<double>::quiet_NaN();
        double ask_qty_3 = std::numeric_limits<double>::quiet_NaN();
        double ask_qty_4 = std::numeric_limits<double>::quiet_NaN();
        double ask_qty_5 = std::numeric_limits<double>::quiet_NaN();

        double bid_vol_L1 = std::numeric_limits<double>::quiet_NaN();
        double ask_vol_L1 = std::numeric_limits<double>::quiet_NaN();
        double bid_vol_L5 = std::numeric_limits<double>::quiet_NaN();
        double ask_vol_L5 = std::numeric_limits<double>::quiet_NaN();
        double imbalance_L1 = std::numeric_limits<double>::quiet_NaN();
        double imbalance_L5 = std::numeric_limits<double>::quiet_NaN();
        double vwap_bid_L5  = std::numeric_limits<double>::quiet_NaN();
        double vwap_ask_L5  = std::numeric_limits<double>::quiet_NaN();
        double microprice   = std::numeric_limits<double>::quiet_NaN();

        // ORDER FLOW IMBALANCE
        double ofi      = std::numeric_limits<double>::quiet_NaN();
        double ofi_1s   = std::numeric_limits<double>::quiet_NaN();
        double ofi_5s   = std::numeric_limits<double>::quiet_NaN();

        // TRADE FLOW
        double n_trades       = 0.0;
        double n_buy_trades   = 0.0;
        double n_sell_trades  = 0.0;
        double sum_qty        = 0.0;
        double buy_vol        = 0.0;
        double sell_vol       = 0.0;
        double signed_vol     = 0.0;
        double trade_imbalance = std::numeric_limits<double>::quiet_NaN();
        double avg_trade_size  = std::numeric_limits<double>::quiet_NaN();
        double max_trade_size  = std::numeric_limits<double>::quiet_NaN();

        // VOLATILITY & RETURNS
        double ret_1s       = std::numeric_limits<double>::quiet_NaN();
        double ret_5s       = std::numeric_limits<double>::quiet_NaN();
        double ret_10s      = std::numeric_limits<double>::quiet_NaN();
        double rv_1s        = std::numeric_limits<double>::quiet_NaN();
        double rv_5s        = std::numeric_limits<double>::quiet_NaN();
        double rv_10s       = std::numeric_limits<double>::quiet_NaN();
        double hl_range_1s  = std::numeric_limits<double>::quiet_NaN();
        double hl_range_5s  = std::numeric_limits<double>::quiet_NaN();

        // TIME & INTENSITY
        double dt_last_trade_ms = std::numeric_limits<double>::quiet_NaN();
        double dt_last_book_ms  = std::numeric_limits<double>::quiet_NaN();
        double trade_intensity_1s = std::numeric_limits<double>::quiet_NaN();
        double trade_intensity_5s = std::numeric_limits<double>::quiet_NaN();
        double book_intensity_1s  = std::numeric_limits<double>::quiet_NaN();
        double book_intensity_5s  = std::numeric_limits<double>::quiet_NaN();

        // TECHNICAL INDICATORS (price-based on mid)
        double ema_12 = std::numeric_limits<double>::quiet_NaN();
        double ema_26 = std::numeric_limits<double>::quiet_NaN();
        double sma_20 = std::numeric_limits<double>::quiet_NaN();
        double sma_50 = std::numeric_limits<double>::quiet_NaN();
        double macd      = std::numeric_limits<double>::quiet_NaN();
        double macd_signal = std::numeric_limits<double>::quiet_NaN();
        double macd_hist   = std::numeric_limits<double>::quiet_NaN();
        double rsi_14   = std::numeric_limits<double>::quiet_NaN();
        double bb_mid   = std::numeric_limits<double>::quiet_NaN();
        double bb_up    = std::numeric_limits<double>::quiet_NaN();
        double bb_dn    = std::numeric_limits<double>::quiet_NaN();
        double bb_pos   = std::numeric_limits<double>::quiet_NaN();
        double vwap     = std::numeric_limits<double>::quiet_NaN();

        // CROSS-ASSET
        double ethbtc_mid_spread        = std::numeric_limits<double>::quiet_NaN();
        double eth_minus_btc_return_1s  = std::numeric_limits<double>::quiet_NaN();

        // META FLAGS (simple versions)
        double is_spread_wide = 0.0;
        double is_depth_thin  = 0.0;

        json to_json() const;
    };

    FeatureEngine() = default;

    /// Main entry: consume one NormalizedTick and produce features.
    FeatureVector process_tick(const NormalizedTick& tick);

private:
    struct InstrumentState {
        // last values
        double last_mid      = std::numeric_limits<double>::quiet_NaN();
        double last_best_bid = std::numeric_limits<double>::quiet_NaN();
        double last_best_ask = std::numeric_limits<double>::quiet_NaN();
        double last_best_bid_qty = 0.0;
        double last_best_ask_qty = 0.0;

        std::int64_t last_book_ts_ms  = 0;
        std::int64_t last_trade_ts_ms = 0;

        // histories for time-window features
        std::deque<std::pair<std::int64_t,double>> mid_history;    // (ts, mid)
        std::deque<std::pair<std::int64_t,double>> mid_high_low;   // (ts, mid) reused for HL ranges
        std::deque<std::pair<std::int64_t,double>> trade_sizes;    // (ts, size)
        std::deque<std::int64_t> trade_ts; // for intensity
        std::deque<std::int64_t> book_ts;  // for intensity
        std::deque<std::pair<std::int64_t,double>> ofi_history;    // (ts, ofi_increment)

        // rolling OFI
        double current_ofi = 0.0;

        // EMA/RSI buffers
        double ema_12 = std::numeric_limits<double>::quiet_NaN();
        double ema_26 = std::numeric_limits<double>::quiet_NaN();
        double ema_9  = std::numeric_limits<double>::quiet_NaN(); // for MACD signal
        std::deque<double> rsi_gains;
        std::deque<double> rsi_losses;
        double last_rsi_price = std::numeric_limits<double>::quiet_NaN();

        std::deque<double> sma20_buf;
        std::deque<double> sma50_buf;

        // VWAP (very simple rolling 60s VWAP)
        std::deque<std::tuple<std::int64_t,double,double>> vwap_trades; // ts, price, qty
    };

    std::unordered_map<std::string, InstrumentState> per_instr_;

    // helpers
    static void trim_history(std::deque<std::pair<std::int64_t,double>>& dq,
                             std::int64_t now_ms, std::int64_t window_ms);
    static void trim_history_ts(std::deque<std::int64_t>& dq,
                                std::int64_t now_ms, std::int64_t window_ms);
    static void trim_vwap(std::deque<std::tuple<std::int64_t,double,double>>& dq,
                          std::int64_t now_ms, std::int64_t window_ms);

    static double get_return_window(const std::deque<std::pair<std::int64_t,double>>& hist,
                                    std::int64_t now_ms, double now_mid,
                                    std::int64_t window_ms);

    static double realized_variance(const std::deque<std::pair<std::int64_t,double>>& hist,
                                    std::int64_t window_ms, std::int64_t now_ms);

    static double hl_range(const std::deque<std::pair<std::int64_t,double>>& hist,
                           std::int64_t window_ms, std::int64_t now_ms);

    static double compute_rsi(std::deque<double>& gains,
                              std::deque<double>& losses,
                              double& last_price,
                              double new_price,
                              std::size_t period);

    static double safe_log(double x);
public:
	void update_from_book(const json& j);
	void update_from_trade(const json& j);

};
extern std::unordered_map<std::string, FeatureEngine> g_feature_engine;
