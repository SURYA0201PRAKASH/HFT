#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <deque>

// ========================= Instrument Metadata =========================

struct InstrumentMeta {
    std::string instrument_name;
    std::string base_asset;
    std::string quote_asset;
    double tick_size      = 0.0;
    double lot_size       = 0.0;
    double contract_value = 0.0;
};

// ========================= L5 Orderbook (same as Deribit) =========================


struct L5OrderBook {
    std::string instrument;

    // Top-5 levels
    double bid_px[5]  = {0};
    double bid_qty[5] = {0};
    double ask_px[5]  = {0};
    double ask_qty[5] = {0};

    long long change_id       = 0;
    long long prev_change_id  = 0;
    long long exchange_ts_ms  = 0;

    L5OrderBook() = default;
    explicit L5OrderBook(const std::string& instr) : instrument(instr) {}

    // ---------- SNAPSHOT ----------
    void apply_snapshot(const json& data) {
        change_id      = data.value("change_id", 0LL);
        prev_change_id = data.value("prev_change_id", 0LL);
        exchange_ts_ms = data.value("timestamp", 0LL);

        std::vector<std::pair<double,double>> bids;
        std::vector<std::pair<double,double>> asks;

        if (data.contains("bids")) {
            for (const auto& b : data["bids"]) {
                // [ "new"/"change", price, qty ]
                double px  = b[1].get<double>();
                double qty = b[2].get<double>();
                bids.emplace_back(px, qty);
            }
        }

        if (data.contains("asks")) {
            for (const auto& a : data["asks"]) {
                double px  = a[1].get<double>();
                double qty = a[2].get<double>();
                asks.emplace_back(px, qty);
            }
        }

        // sort & take top-5
        std::sort(bids.begin(), bids.end(),
                  [](auto& x, auto& y){ return x.first > y.first; });
        std::sort(asks.begin(), asks.end(),
                  [](auto& x, auto& y){ return x.first < y.first; });

        for (int i = 0; i < 5; ++i) {
            if (i < (int)bids.size()) {
                bid_px[i]  = bids[i].first;
                bid_qty[i] = bids[i].second;
            } else {
                bid_px[i] = bid_qty[i] = 0.0;
            }

            if (i < (int)asks.size()) {
                ask_px[i]  = asks[i].first;
                ask_qty[i] = asks[i].second;
            } else {
                ask_px[i] = ask_qty[i] = 0.0;
            }
        }
    }

    // ---------- DELTA ----------
    void apply_delta(const json& data) {
        change_id      = data.value("change_id", 0LL);
        prev_change_id = data.value("prev_change_id", 0LL);
        exchange_ts_ms = data.value("timestamp", 0LL);

        if (data.contains("bids")) {
            for (const auto& b : data["bids"]) {
                std::string ev = b[0].get<std::string>(); // "new"/"change"/"delete"
                double px      = b[1].get<double>();
                double qty     = b[2].get<double>();
                apply_one_update(true, ev, px, qty);
            }
        }

        if (data.contains("asks")) {
            for (const auto& a : data["asks"]) {
                std::string ev = a[0].get<std::string>();
                double px      = a[1].get<double>();
                double qty     = a[2].get<double>();
                apply_one_update(false, ev, px, qty);
            }
        }

        sort_top5();
    }

    double best_bid() const { return bid_px[0]; }
    double best_ask() const { return ask_px[0]; }
    double mid() const {
        if (best_bid() <= 0.0 || best_ask() <= 0.0) return 0.0;
        return 0.5 * (best_bid() + best_ask());
    }

private:
    void apply_one_update(bool is_bid,
                          const std::string& ev,
                          double price,
                          double qty)
    {
        double* px   = is_bid ? bid_px  : ask_px;
        double* qtys = is_bid ? bid_qty : ask_qty;

        // DELETE
        if (ev == "delete") {
            for (int i=0; i<5; ++i) {
                if (px[i] == price) {
                    px[i]   = 0.0;
                    qtys[i] = 0.0;
                    return;
                }
            }
            return;
        }

        // NEW / CHANGE: first look for existing price
        for (int i=0; i<5; ++i) {
            if (px[i] == price) {
                qtys[i] = qty;
                return;
            }
        }

        // If it's a NEW price level better than the worst one, insert at position 4
        int worst_idx = 4;

        bool better =
            (is_bid && price > px[worst_idx]) ||
            (!is_bid && (px[worst_idx] == 0.0 || price < px[worst_idx]));

        if (better) {
            px[worst_idx]   = price;
            qtys[worst_idx] = qty;
        }
    }

    void sort_top5() {
        std::vector<std::pair<double,double>> bids;
        std::vector<std::pair<double,double>> asks;

        for (int i=0; i<5; ++i) {
            if (bid_px[i] > 0.0)
                bids.emplace_back(bid_px[i], bid_qty[i]);
            if (ask_px[i] > 0.0)
                asks.emplace_back(ask_px[i], ask_qty[i]);
        }

        std::sort(bids.begin(), bids.end(),
                  [](auto& x, auto& y){ return x.first > y.first; });
        std::sort(asks.begin(), asks.end(),
                  [](auto& x, auto& y){ return x.first < y.first; });

        for (int i=0; i<5; ++i) {
            if (i < (int)bids.size()) {
                bid_px[i]  = bids[i].first;
                bid_qty[i] = bids[i].second;
            } else {
                bid_px[i] = bid_qty[i] = 0.0;
            }

            if (i < (int)asks.size()) {
                ask_px[i]  = asks[i].first;
                ask_qty[i] = asks[i].second;
            } else {
                ask_px[i] = ask_qty[i] = 0.0;
            }
        }
    }
};

// ========================= Per-Instrument State =========================

struct InstrumentState {
    L5OrderBook book;
    long long   last_book_ts_ms  = 0;
    long long   last_trade_ts_ms = 0;
    double      mid_price        = 0.0;

    InstrumentState() = default;
    explicit InstrumentState(const std::string& instr)
        : book(instr) {}
};