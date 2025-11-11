#pragma once
#include <fstream>
#include <string>

class PnLLogger {
public:
    explicit PnLLogger(const std::string& path);
    void log(long long ts_ms, double pnl, double pos);
private:
    std::ofstream out_;
};
