#include "config_loader.hpp"
#include <iostream>

int main() {
    Config cfg = load_config("configs/base.yaml");
    std::cout << "Exchange: " << cfg.exchange << std::endl;
    for (auto& s : cfg.symbols)
        std::cout << "Symbol: " << s << std::endl;
}
