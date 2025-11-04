#pragma once
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

struct ConfigRecording {
    std::string csv_path;
    std::string sqlite_path;
};

struct Config {
    std::string exchange = "deribit";
    std::vector<std::string> symbols;
    int depth_levels = 0;
    std::vector<int> windows_ms;
    double zscore_alpha = 0.0;
    ConfigRecording recording;   // ✅ Nested struct for YAML
};

// Declaration
Config load_config(const std::string& path);
