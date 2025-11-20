#include "feature_engine.hpp"
#include <algorithm>
#include <numeric>
#include <cmath>
#include <iostream>

using std::int64_t;
std::unordered_map<std::string, FeatureEngine> g_feature_engine;
// ---------- NormalizedTick implementation ----------

NormalizedTick::NormalizedTick(const json& j) {
    // -------- Instrument Normalization (CRITICAL) --------
    std::string instr;

    if (j.contains("instrument"))
        instr = j["instrument"].get<std::string>();

    // Prefix exchange so BTC-PERPETUAL and BTCUSDT don't collide
    if (!instr.empty()) {
        if (instr.find("PERPETUAL") != std::string::npos) {
            instr = "DERIBIT_" + instr;
        } else {
            instr = "BYBIT_" + instr;
        }
    }

    // *** CRITICAL FIX ***
    // Store normalized instrument into the class member
    this->instrument = instr;
    // ------------------------------------

    message_type      = j.value("message_type", "");
    ev                = j.value("ev", message_type);
    exchange_ts_ms    = j.value("exchange_ts_ms", int64_t{0});
    ingest_ts_ns      = j.value("ingest_ts_ns", int64_t{0});
    feed_latency_ns   = j.value("feed_latency_ns", int64_t{0});
    dt_last_book_ms   = j.value("dt_last_book_ms", int64_t{0});
    dt_last_trade_ms  = j.value("dt_last_trade_ms", int64_t{0});
    change_id         = j.value("change_id", int64_t{0});
    prev_change_id    = j.value("prev_change_id", int64_t{0});

    if (j.contains("best_bid")) best_bid = j["best_bid"].get<double>();
    if (j.contains("best_ask")) best_ask = j["best_ask"].get<double>();
    if (j.contains("mid"))      mid      = j["mid"].get<double>();

    if (j.contains("bid_px") && j["bid_px"].is_array()) {
        for (auto& v : j["bid_px"]) {
            if (bid_px.size() >= 5) break;
            bid_px.push_back(v.get<double>());
        }
    }
    if (j.contains("bid_qty") && j["bid_qty"].is_array()) {
        for (auto& v : j["bid_qty"]) {
            if (bid_qty.size() >= 5) break;
            bid_qty.push_back(v.get<double>());
        }
    }
    if (j.contains("ask_px") && j["ask_px"].is_array()) {
        for (auto& v : j["ask_px"]) {
            if (ask_px.size() >= 5) break;
            ask_px.push_back(v.get<double>());
        }
    }
    if (j.contains("ask_qty") && j["ask_qty"].is_array()) {
        for (auto& v : j["ask_qty"]) {
            if (ask_qty.size() >= 5) break;
            ask_qty.push_back(v.get<double>());
        }
    }

    // Trade block
    if (ev == "TRADE" || message_type == "TRADE") {
        has_trade = true;
        if (j.contains("trade_price")) trade_price = j["trade_price"].get<double>();
        if (j.contains("trade_qty"))   trade_qty   = j["trade_qty"].get<double>();
        trade_side = j.value("trade_side", "");
        if (j.contains("index_price")) index_price = j["index_price"].get<double>();
        if (j.contains("mark_price"))  mark_price  = j["mark_price"].get<double>();
        trade_id = j.value("trade_id", "");
    }

    // Cross-asset mids
    if (j.contains("all_mid_price") && j["all_mid_price"].is_object()) {
        for (auto it = j["all_mid_price"].begin(); it != j["all_mid_price"].end(); ++it) {
            all_mid_price[it.key()] = it.value().get<double>();
        }
    }
}


// ---------- FeatureVector::to_json ----------

json FeatureEngine::FeatureVector::to_json() const {
    json j;
    j["instrument"]      = instrument;
    j["exchange_ts_ms"]  = exchange_ts_ms;
    j["ingest_ts_ns"]    = ingest_ts_ns;

    // QUOTE
    j["best_bid"]  = best_bid;
    j["best_ask"]  = best_ask;
    j["mid"]       = mid;
    j["log_mid"]   = log_mid;
    j["spread_abs"] = spread_abs;
    j["spread_bps"] = spread_bps;
    j["d_best_bid"] = d_best_bid;
    j["d_best_ask"] = d_best_ask;
    j["mid_return_1tick"] = mid_return_1tick;
    j["mid_return_1s"]    = mid_return_1s;
    j["mid_return_5s"]    = mid_return_5s;
    j["mid_return_10s"]   = mid_return_10s;

    // Depth
    j["bid_px_1"] = bid_px_1; j["bid_px_2"] = bid_px_2; j["bid_px_3"] = bid_px_3;
    j["bid_px_4"] = bid_px_4; j["bid_px_5"] = bid_px_5;
    j["ask_px_1"] = ask_px_1; j["ask_px_2"] = ask_px_2; j["ask_px_3"] = ask_px_3;
    j["ask_px_4"] = ask_px_4; j["ask_px_5"] = ask_px_5;

    j["bid_qty_1"] = bid_qty_1; j["bid_qty_2"] = bid_qty_2; j["bid_qty_3"] = bid_qty_3;
    j["bid_qty_4"] = bid_qty_4; j["bid_qty_5"] = bid_qty_5;
    j["ask_qty_1"] = ask_qty_1; j["ask_qty_2"] = ask_qty_2; j["ask_qty_3"] = ask_qty_3;
    j["ask_qty_4"] = ask_qty_4; j["ask_qty_5"] = ask_qty_5;

    j["bid_vol_L1"] = bid_vol_L1;
    j["ask_vol_L1"] = ask_vol_L1;
    j["bid_vol_L5"] = bid_vol_L5;
    j["ask_vol_L5"] = ask_vol_L5;
    j["imbalance_L1"] = imbalance_L1;
    j["imbalance_L5"] = imbalance_L5;
    j["vwap_bid_L5"]  = vwap_bid_L5;
    j["vwap_ask_L5"]  = vwap_ask_L5;
    j["microprice"]   = microprice;

    // OFI
    j["ofi"]    = ofi;
    j["ofi_1s"] = ofi_1s;
    j["ofi_5s"] = ofi_5s;

    // Trade flow
    j["n_trades"]      = n_trades;
    j["n_buy_trades"]  = n_buy_trades;
    j["n_sell_trades"] = n_sell_trades;
    j["sum_qty"]       = sum_qty;
    j["buy_vol"]       = buy_vol;
    j["sell_vol"]      = sell_vol;
    j["signed_vol"]    = signed_vol;
    j["trade_imbalance"] = trade_imbalance;
    j["avg_trade_size"]  = avg_trade_size;
    j["max_trade_size"]  = max_trade_size;

    // Volatility
    j["ret_1s"]      = ret_1s;
    j["ret_5s"]      = ret_5s;
    j["ret_10s"]     = ret_10s;
    j["rv_1s"]       = rv_1s;
    j["rv_5s"]       = rv_5s;
    j["rv_10s"]      = rv_10s;
    j["hl_range_1s"] = hl_range_1s;
    j["hl_range_5s"] = hl_range_5s;

    // Time / intensity
    j["dt_last_trade_ms"]   = dt_last_trade_ms;
    j["dt_last_book_ms"]    = dt_last_book_ms;
    j["trade_intensity_1s"] = trade_intensity_1s;
    j["trade_intensity_5s"] = trade_intensity_5s;
    j["book_intensity_1s"]  = book_intensity_1s;
    j["book_intensity_5s"]  = book_intensity_5s;

    // Technical indicators
    j["ema_12"] = ema_12;
    j["ema_26"] = ema_26;
    j["sma_20"] = sma_20;
    j["sma_50"] = sma_50;
    j["macd"]        = macd;
    j["macd_signal"] = macd_signal;
    j["macd_hist"]   = macd_hist;
    j["rsi_14"]      = rsi_14;
    j["bb_mid"]      = bb_mid;
    j["bb_up"]       = bb_up;
    j["bb_dn"]       = bb_dn;
    j["bb_pos"]      = bb_pos;
    j["vwap"]        = vwap;

    // Cross-asset
    j["ethbtc_mid_spread"]       = ethbtc_mid_spread;
    j["eth_minus_btc_return_1s"] = eth_minus_btc_return_1s;

    // Meta flags
    j["is_spread_wide"] = is_spread_wide;
    j["is_depth_thin"]  = is_depth_thin;

    return j;
}

// ---------- Helper functions ----------

void FeatureEngine::trim_history(
    std::deque<std::pair<int64_t,double>>& dq,
    int64_t now_ms, int64_t window_ms)
{
    while (!dq.empty() && dq.front().first < now_ms - window_ms) {
        dq.pop_front();
    }
}

void FeatureEngine::trim_history_ts(
    std::deque<int64_t>& dq,
    int64_t now_ms, int64_t window_ms)
{
    while (!dq.empty() && dq.front() < now_ms - window_ms) {
        dq.pop_front();
    }
}

void FeatureEngine::trim_vwap(
    std::deque<std::tuple<int64_t,double,double>>& dq,
    int64_t now_ms, int64_t window_ms)
{
    while (!dq.empty() && std::get<0>(dq.front()) < now_ms - window_ms) {
        dq.pop_front();
    }
}

double FeatureEngine::get_return_window(
    const std::deque<std::pair<int64_t,double>>& hist,
    int64_t now_ms, double now_mid, int64_t window_ms)
{
    if (std::isnan(now_mid) || hist.empty())
        return std::numeric_limits<double>::quiet_NaN();

    double ref_mid = std::numeric_limits<double>::quiet_NaN();
    for (auto it = hist.rbegin(); it != hist.rend(); ++it) {
        if (it->first <= now_ms - window_ms) {
            ref_mid = it->second;
            break;
        }
    }
    if (std::isnan(ref_mid))
        ref_mid = hist.front().second;

    if (ref_mid <= 0.0 || now_mid <= 0.0)
        return std::numeric_limits<double>::quiet_NaN();

    return std::log(now_mid / ref_mid);
}

double FeatureEngine::realized_variance(
    const std::deque<std::pair<int64_t,double>>& hist,
    int64_t window_ms, int64_t now_ms)
{
    if (hist.size() < 2) return std::numeric_limits<double>::quiet_NaN();

    double rv = 0.0;
    double prev_log = std::numeric_limits<double>::quiet_NaN();
    int64_t prev_ts = 0;

    for (auto const& [ts, mid] : hist) {
        if (ts < now_ms - window_ms) continue;
        if (mid <= 0.0) continue;
        double current_log = std::log(mid);
        if (!std::isnan(prev_log) && ts > prev_ts) {
            double dr = current_log - prev_log;
            rv += dr * dr;
        }
        prev_log = current_log;
        prev_ts  = ts;
    }
    return rv;
}

double FeatureEngine::hl_range(
    const std::deque<std::pair<int64_t,double>>& hist,
    int64_t window_ms, int64_t now_ms)
{
    double lo = std::numeric_limits<double>::infinity();
    double hi = -std::numeric_limits<double>::infinity();
    bool any = false;

    for (auto const& [ts, mid] : hist) {
        if (ts < now_ms - window_ms) continue;
        if (std::isnan(mid)) continue;
        any = true;
        lo = std::min(lo, mid);
        hi = std::max(hi, mid);
    }
    if (!any) return std::numeric_limits<double>::quiet_NaN();
    return hi - lo;
}

double FeatureEngine::safe_log(double x) {
    if (x <= 0.0) return std::numeric_limits<double>::quiet_NaN();
    return std::log(x);
}

double FeatureEngine::compute_rsi(
    std::deque<double>& gains,
    std::deque<double>& losses,
    double& last_price,
    double new_price,
    std::size_t period)
{
    if (std::isnan(new_price))
        return std::numeric_limits<double>::quiet_NaN();

    if (std::isnan(last_price)) {
        last_price = new_price;
        return std::numeric_limits<double>::quiet_NaN();
    }

    double diff = new_price - last_price;
    last_price = new_price;

    double gain = diff > 0 ? diff : 0.0;
    double loss = diff < 0 ? -diff : 0.0;

    gains.push_back(gain);
    losses.push_back(loss);
    if (gains.size() > period) gains.pop_front();
    if (losses.size() > period) losses.pop_front();

    if (gains.size() < period || losses.size() < period)
        return std::numeric_limits<double>::quiet_NaN();

    double avg_gain = std::accumulate(gains.begin(), gains.end(), 0.0) / period;
    double avg_loss = std::accumulate(losses.begin(), losses.end(), 0.0) / period;

    if (avg_loss == 0.0)
        return 100.0;

    double rs  = avg_gain / avg_loss;
    double rsi = 100.0 - (100.0 / (1.0 + rs));
    return rsi;
}

// ---------- main processing ----------

FeatureEngine::FeatureVector FeatureEngine::process_tick(const NormalizedTick& t) {
    FeatureVector f;
    f.instrument      = t.instrument;
    f.exchange_ts_ms  = t.exchange_ts_ms;
    f.ingest_ts_ns    = t.ingest_ts_ns;

    auto& st = per_instr_[t.instrument];
    const int64_t now_ms = t.exchange_ts_ms;

    // ----- update basic book state if BOOK event -----
    if (t.ev == "BOOK") {
        st.book_ts.push_back(now_ms);
        st.mid_history.emplace_back(now_ms, t.mid);
        st.mid_high_low.emplace_back(now_ms, t.mid);
        trim_history(st.mid_history, now_ms, 10'000);
        trim_history(st.mid_high_low, now_ms, 10'000);
        trim_history_ts(st.book_ts, now_ms, 5'000);

        // OFI increment using best level change
        if (!std::isnan(st.last_best_bid) && !std::isnan(st.last_best_ask)) {
            double ofi_inc = 0.0;
            double bb  = t.best_bid;
            double bbq = (!t.bid_qty.empty() ? t.bid_qty[0] : 0.0);
            double ba  = t.best_ask;
            double baq = (!t.ask_qty.empty() ? t.ask_qty[0] : 0.0);

            // Bid side
            if (bb > st.last_best_bid)
                ofi_inc += bbq;
            else if (bb < st.last_best_bid)
                ofi_inc -= st.last_best_bid_qty;
            else
                ofi_inc += (bbq - st.last_best_bid_qty);

            // Ask side
            if (ba < st.last_best_ask)
                ofi_inc -= baq;
            else if (ba > st.last_best_ask)
                ofi_inc += st.last_best_ask_qty;
            else
                ofi_inc -= (baq - st.last_best_ask_qty);

            st.current_ofi += ofi_inc;
            st.ofi_history.emplace_back(now_ms, ofi_inc);
            trim_history(st.ofi_history, now_ms, 5'000);
        }

        st.last_best_bid      = t.best_bid;
        st.last_best_ask      = t.best_ask;
        st.last_best_bid_qty  = (!t.bid_qty.empty() ? t.bid_qty[0] : 0.0);
        st.last_best_ask_qty  = (!t.ask_qty.empty() ? t.ask_qty[0] : 0.0);
        st.last_book_ts_ms    = now_ms;
    }

    // ----- update trade state if TRADE event -----
    if (t.has_trade) {
        st.trade_ts.push_back(now_ms);
        trim_history_ts(st.trade_ts, now_ms, 5'000);

        st.trade_sizes.emplace_back(now_ms, t.trade_qty);
        trim_history(st.trade_sizes, now_ms, 5'000);

        st.vwap_trades.emplace_back(now_ms, t.trade_price, t.trade_qty);
        trim_vwap(st.vwap_trades, now_ms, 60'000);
        st.last_trade_ts_ms = now_ms;
    }

    // ----- QUOTE FEATURES -----
    f.best_bid = t.best_bid;
    f.best_ask = t.best_ask;
    f.mid      = t.mid;
    f.log_mid  = safe_log(t.mid);

    if (!std::isnan(t.best_bid) && !std::isnan(t.best_ask)) {
        f.spread_abs = t.best_ask - t.best_bid;
        if (t.mid > 0.0)
            f.spread_bps = (f.spread_abs / t.mid) * 10'000.0;
    }

    f.d_best_bid = (std::isnan(st.last_mid) ? std::numeric_limits<double>::quiet_NaN()
                                            : t.best_bid - st.last_best_bid);
    f.d_best_ask = (std::isnan(st.last_mid) ? std::numeric_limits<double>::quiet_NaN()
                                            : t.best_ask - st.last_best_ask);

    if (!std::isnan(st.last_mid) && !std::isnan(t.mid) && st.last_mid > 0.0)
        f.mid_return_1tick = std::log(t.mid / st.last_mid);

    // time-window returns using mid_history (10s buffer kept)
    f.mid_return_1s  = get_return_window(st.mid_history, now_ms, t.mid, 1'000);
    f.mid_return_5s  = get_return_window(st.mid_history, now_ms, t.mid, 5'000);
    f.mid_return_10s = get_return_window(st.mid_history, now_ms, t.mid, 10'000);

    f.ret_1s  = f.mid_return_1s;
    f.ret_5s  = f.mid_return_5s;
    f.ret_10s = f.mid_return_10s;

    // store last_mid at end
    if (!std::isnan(t.mid))
        st.last_mid = t.mid;

    // ----- DEPTH & IMBALANCE -----
    auto get_or_nan = [](const std::vector<double>& v, std::size_t i) -> double {
        return i < v.size() ? v[i] : std::numeric_limits<double>::quiet_NaN();
    };

    f.bid_px_1 = get_or_nan(t.bid_px, 0);
    f.bid_px_2 = get_or_nan(t.bid_px, 1);
    f.bid_px_3 = get_or_nan(t.bid_px, 2);
    f.bid_px_4 = get_or_nan(t.bid_px, 3);
    f.bid_px_5 = get_or_nan(t.bid_px, 4);

    f.ask_px_1 = get_or_nan(t.ask_px, 0);
    f.ask_px_2 = get_or_nan(t.ask_px, 1);
    f.ask_px_3 = get_or_nan(t.ask_px, 2);
    f.ask_px_4 = get_or_nan(t.ask_px, 3);
    f.ask_px_5 = get_or_nan(t.ask_px, 4);

    f.bid_qty_1 = get_or_nan(t.bid_qty, 0);
    f.bid_qty_2 = get_or_nan(t.bid_qty, 1);
    f.bid_qty_3 = get_or_nan(t.bid_qty, 2);
    f.bid_qty_4 = get_or_nan(t.bid_qty, 3);
    f.bid_qty_5 = get_or_nan(t.bid_qty, 4);

    f.ask_qty_1 = get_or_nan(t.ask_qty, 0);
    f.ask_qty_2 = get_or_nan(t.ask_qty, 1);
    f.ask_qty_3 = get_or_nan(t.ask_qty, 2);
    f.ask_qty_4 = get_or_nan(t.ask_qty, 3);
    f.ask_qty_5 = get_or_nan(t.ask_qty, 4);

    f.bid_vol_L1 = f.bid_qty_1;
    f.ask_vol_L1 = f.ask_qty_1;

    auto sum_vec = [](const std::vector<double>& v)->double {
        return std::accumulate(v.begin(), v.end(), 0.0);
    };
    f.bid_vol_L5 = sum_vec(t.bid_qty);
    f.ask_vol_L5 = sum_vec(t.ask_qty);

    if (std::isfinite(f.bid_vol_L1 + f.ask_vol_L1) && (f.bid_vol_L1 + f.ask_vol_L1) > 0.0)
        f.imbalance_L1 = (f.bid_vol_L1 - f.ask_vol_L1) / (f.bid_vol_L1 + f.ask_vol_L1);

    if (std::isfinite(f.bid_vol_L5 + f.ask_vol_L5) && (f.bid_vol_L5 + f.ask_vol_L5) > 0.0)
        f.imbalance_L5 = (f.bid_vol_L5 - f.ask_vol_L5) / (f.bid_vol_L5 + f.ask_vol_L5);

    // VWAP L5 (book-side)
    auto vwap_side = [](const std::vector<double>& px, const std::vector<double>& qty)->double {
        if (px.empty() || qty.empty()) return std::numeric_limits<double>::quiet_NaN();
        double num = 0.0, den = 0.0;
        std::size_t n = std::min(px.size(), qty.size());
        for (std::size_t i=0;i<n;++i) {
            num += px[i] * qty[i];
            den += qty[i];
        }
        if (den <= 0.0) return std::numeric_limits<double>::quiet_NaN();
        return num / den;
    };

    f.vwap_bid_L5 = vwap_side(t.bid_px, t.bid_qty);
    f.vwap_ask_L5 = vwap_side(t.ask_px, t.ask_qty);

    if (!std::isnan(t.best_bid) && !std::isnan(t.best_ask)) {
        double denom = t.best_bid * f.bid_vol_L1 + t.best_ask * f.ask_vol_L1;
        if (denom > 0.0)
            f.microprice = (t.best_ask * f.bid_vol_L1 + t.best_bid * f.ask_vol_L1) / denom;
    }

    // ----- OFI & its windows -----
    f.ofi = st.current_ofi;

    // windowed OFI
    double ofi_1s_val = 0.0;
    double ofi_5s_val = 0.0;
    for (auto const& [ts, inc] : st.ofi_history) {
        if (ts >= now_ms - 1'000) ofi_1s_val += inc;
        if (ts >= now_ms - 5'000) ofi_5s_val += inc;
    }
    f.ofi_1s = ofi_1s_val;
    f.ofi_5s = ofi_5s_val;

    // ----- TRADE FLOW -----
    if (t.has_trade) {
        f.n_trades = 0.0;
        f.n_buy_trades = 0.0;
        f.n_sell_trades = 0.0;
        f.sum_qty = 0.0;
        f.buy_vol = 0.0;
        f.sell_vol = 0.0;
        f.max_trade_size = 0.0;

        for (auto const& [ts, sz] : st.trade_sizes) {
            if (ts < now_ms - 5'000) continue;
            f.n_trades += 1.0;
            f.sum_qty  += sz;
            f.max_trade_size = std::max(f.max_trade_size, sz);
        }

        // Simple sign using current trade side
        if (t.trade_side == "BUY") {
            f.n_buy_trades = 1.0;
            f.buy_vol      = t.trade_qty;
            f.signed_vol   = t.trade_qty;
        } else if (t.trade_side == "SELL") {
            f.n_sell_trades = 1.0;
            f.sell_vol      = t.trade_qty;
            f.signed_vol    = -t.trade_qty;
        }

        if (f.n_trades > 0.0)
            f.avg_trade_size = f.sum_qty / f.n_trades;

        double total_vol = f.buy_vol + f.sell_vol;
        if (total_vol > 0.0)
            f.trade_imbalance = (f.buy_vol - f.sell_vol) / total_vol;
    }

    // ----- VOLATILITY & HL RANGES -----
    f.rv_1s  = realized_variance(st.mid_history, 1'000, now_ms);
    f.rv_5s  = realized_variance(st.mid_history, 5'000, now_ms);
    f.rv_10s = realized_variance(st.mid_history,10'000, now_ms);

    f.hl_range_1s = hl_range(st.mid_high_low, 1'000, now_ms);
    f.hl_range_5s = hl_range(st.mid_high_low, 5'000, now_ms);

    // ----- TIME & INTENSITY -----
    if (st.last_trade_ts_ms > 0)
        f.dt_last_trade_ms = static_cast<double>(now_ms - st.last_trade_ts_ms);
    if (st.last_book_ts_ms > 0)
        f.dt_last_book_ms = static_cast<double>(now_ms - st.last_book_ts_ms);

    trim_history_ts(st.trade_ts, now_ms, 5'000);
    trim_history_ts(st.book_ts , now_ms, 5'000);

    double trades_1s = 0.0, trades_5s = 0.0;
    for (auto ts : st.trade_ts) {
        if (ts >= now_ms - 1'000) trades_1s += 1.0;
        if (ts >= now_ms - 5'000) trades_5s += 1.0;
    }
    double books_1s = 0.0, books_5s = 0.0;
    for (auto ts : st.book_ts) {
        if (ts >= now_ms - 1'000) books_1s += 1.0;
        if (ts >= now_ms - 5'000) books_5s += 1.0;
    }

    f.trade_intensity_1s = trades_1s;
    f.trade_intensity_5s = trades_5s / 5.0;
    f.book_intensity_1s  = books_1s;
    f.book_intensity_5s  = books_5s / 5.0;

    // ----- TECHNICAL INDICATORS (EMA, MACD, RSI, BB, SMA, VWAP) -----
    // EMA (12, 26, 9) on mid
    if (!std::isnan(t.mid)) {
        double alpha12 = 2.0 / (12.0 + 1.0);
        double alpha26 = 2.0 / (26.0 + 1.0);
        double alpha9  = 2.0 / (9.0  + 1.0);

        if (std::isnan(st.ema_12)) st.ema_12 = t.mid;
        else st.ema_12 = alpha12 * t.mid + (1.0 - alpha12) * st.ema_12;

        if (std::isnan(st.ema_26)) st.ema_26 = t.mid;
        else st.ema_26 = alpha26 * t.mid + (1.0 - alpha26) * st.ema_26;

        double macd_val = st.ema_12 - st.ema_26;
        if (std::isnan(st.ema_9)) st.ema_9 = macd_val;
        else st.ema_9 = alpha9 * macd_val + (1.0 - alpha9) * st.ema_9;

        f.ema_12 = st.ema_12;
        f.ema_26 = st.ema_26;
        f.macd   = macd_val;
        f.macd_signal = st.ema_9;
        f.macd_hist   = macd_val - st.ema_9;
    }

    // SMA20 / SMA50 and Bollinger Bands on mid
    if (!std::isnan(t.mid)) {
        st.sma20_buf.push_back(t.mid);
        if (st.sma20_buf.size() > 20) st.sma20_buf.pop_front();

        st.sma50_buf.push_back(t.mid);
        if (st.sma50_buf.size() > 50) st.sma50_buf.pop_front();

        if (!st.sma20_buf.empty()) {
            double sum = std::accumulate(st.sma20_buf.begin(), st.sma20_buf.end(), 0.0);
            f.sma_20 = sum / static_cast<double>(st.sma20_buf.size());

            // Bollinger with +/-2 std on sma20 buffer
            double mean = f.sma_20;
            double s2 = 0.0;
            for (double x : st.sma20_buf) {
                double d = x - mean;
                s2 += d * d;
            }
            if (st.sma20_buf.size() > 1) {
                double var = s2 / static_cast<double>(st.sma20_buf.size() - 1);
                double sd = std::sqrt(var);
                f.bb_mid = mean;
                f.bb_up  = mean + 2.0 * sd;
                f.bb_dn  = mean - 2.0 * sd;
                if (!std::isnan(t.mid) && sd > 0.0)
                    f.bb_pos = (t.mid - mean) / (2.0 * sd);
            }
        }

        if (!st.sma50_buf.empty()) {
            double sum = std::accumulate(st.sma50_buf.begin(), st.sma50_buf.end(), 0.0);
            f.sma_50 = sum / static_cast<double>(st.sma50_buf.size());
        }
    }

    // RSI(14) on mid
    f.rsi_14 = compute_rsi(st.rsi_gains, st.rsi_losses, st.last_rsi_price, t.mid, 14);

    // VWAP (60s rolling)
    {
        double num = 0.0, den = 0.0;
        for (auto const& [ts, price, qty] : st.vwap_trades) {
            (void)ts;
            num += price * qty;
            den += qty;
        }
        if (den > 0.0)
            f.vwap = num / den;
    }

    // ----- CROSS-ASSET (ETH/BTC) -----
    // Try Deribit first
	auto it_btc = t.all_mid_price.find("DERIBIT_BTC-PERPETUAL");
	auto it_eth = t.all_mid_price.find("DERIBIT_ETH-PERPETUAL");

	// If not found, try Bybit
	if (it_btc == t.all_mid_price.end())
		it_btc = t.all_mid_price.find("BYBIT_BTCUSDT");

	if (it_eth == t.all_mid_price.end())
		it_eth = t.all_mid_price.find("BYBIT_ETHUSDT");
    if (it_btc != t.all_mid_price.end() && it_eth != t.all_mid_price.end()) {
        double mid_btc = it_btc->second;
        double mid_eth = it_eth->second;
        if (mid_btc > 0.0) {
            f.ethbtc_mid_spread = mid_eth / mid_btc; // ETH/BTC ratio
        }

        // simple 1s return difference using current tick mids
        // (you can refine later using per_instr_ states)
        if (!std::isnan(f.ret_1s)) {
            // if this tick is ETH: diff = ret_ETH - ret_BTC (approx using log(mid_eth/mid_btc) change)
            f.eth_minus_btc_return_1s = f.ret_1s; // placeholder; refine if you want true cross returns
        }
    }

    // ----- META FLAGS -----
    // very simple thresholds; tune later
    if (!std::isnan(f.spread_bps) && f.spread_bps > 5.0)
        f.is_spread_wide = 1.0;
    if (!std::isnan(f.bid_vol_L5 + f.ask_vol_L5) && (f.bid_vol_L5 + f.ask_vol_L5) < 10'000.0)
        f.is_depth_thin = 1.0;

    return f;
}


void FeatureEngine::update_from_book(const json& j) {
    try {
        // 1. Normalize incoming JSON
        NormalizedTick tick(j);

        // force event type to BOOK (sometimes j["ev"] may already be BOOK)
        tick.ev = "BOOK";
        tick.message_type = "BOOK";

        // 2. Process tick → full feature extraction
        auto fvec = process_tick(tick);

        // 3. Convert to JSON
        json out = fvec.to_json();

        // 4. Output (later we store to SQLite)
        std::cout << out.dump() << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[FeatureEngine][BOOK] ERROR: " << e.what() << std::endl;
    }
}

void FeatureEngine::update_from_trade(const json& j) {
    try {
        // 1. Normalize incoming JSON
        NormalizedTick tick(j);

        tick.ev = "TRADE";
        tick.message_type = "TRADE";
        tick.has_trade = true;

        // 2. Process
        auto fvec = process_tick(tick);

        // 3. Output
        json out = fvec.to_json();
        std::cout << out.dump() << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[FeatureEngine][TRADE] ERROR: " << e.what() << std::endl;
    }
}
