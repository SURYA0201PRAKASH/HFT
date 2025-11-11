#include "pnl_logger.hpp"
#include <iostream>

PnLLogger::PnLLogger(const std::string& path) : out_(path, std::ios::app) {
    if (out_.tellp() == 0) out_ << "timestamp_ms,pnl,position\n";
}

void PnLLogger::log(long long ts_ms, double pnl, double pos) {
    if (!out_) {
        std::cerr << "⚠️ cannot write PnL log\n";
        return;
    }
    out_ << ts_ms << ',' << pnl << ',' << pos << '\n';
    out_.flush();
}
