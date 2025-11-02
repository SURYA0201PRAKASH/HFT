#include "config_loader.hpp"
#include <iostream>
#include <filesystem>
#include <stdexcept>

// Load YAML configuration with safe path resolution
Config load_config(const std::string& path) {
    Config cfg;

    namespace fs = std::filesystem;
    fs::path p(path);

    // --- try local (./configs/base.yaml) and parent (../configs/base.yaml)
    if (!fs::exists(p)) {
        fs::path alt = fs::path("../") / path;
        if (fs::exists(alt)) {
            p = alt;
        } else {
            std::cerr << "\033[1;31m[ERROR]\033[0m Config file not found at: "
                      << path << " or " << alt << std::endl;
            throw std::runtime_error("bad file: " + path);
        }
    }

    // --- attempt YAML load
    YAML::Node node;
    try {
        node = YAML::LoadFile(p.string());
    } catch (const YAML::Exception& e) {
        std::cerr << "\033[1;31m[YAML ERROR]\033[0m " << e.what() << std::endl;
        throw std::runtime_error("Failed to parse YAML: " + std::string(e.what()));
    }

    // --- extract values
    if (!node["symbols"]) throw std::runtime_error("Missing 'symbols' in config");
    for (auto s : node["symbols"]) cfg.symbols.push_back(s.as<std::string>());

    cfg.depth_levels = node["book"]["depth_levels"].as<int>();
    for (auto w : node["features"]["windows_ms"])
        cfg.windows_ms.push_back(w.as<int>());

    cfg.zscore_alpha = node["features"]["zscore_alpha"].as<double>();
    cfg.csv_path = node["recording"]["csv_path"].as<std::string>();
    cfg.sqlite_path = node["recording"]["sqlite_path"].as<std::string>();

    // --- pretty summary
    std::cout << "\033[1;32m[CONFIG LOADED]\033[0m " << p << std::endl;
    std::cout << "  Symbols: ";
    for (auto& s : cfg.symbols) std::cout << s << " ";
    std::cout << "\n  Depth levels: " << cfg.depth_levels
              << "\n  Windows: ";
    for (auto w : cfg.windows_ms) std::cout << w << " ";
    std::cout << "\n  Z-score α: " << cfg.zscore_alpha
              << "\n  CSV Path: " << cfg.csv_path
              << "\n  SQLite Path: " << cfg.sqlite_path << std::endl;

    return cfg;
}
