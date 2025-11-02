#pragma once
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

struct Config {
    std::vector<std::string> symbols;
    int depth_levels;
    std::vector<int> windows_ms;
    double zscore_alpha;
    std::string csv_path;
    std::string sqlite_path;
};

Config load_config(const std::string& path);
