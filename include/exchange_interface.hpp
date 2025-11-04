#pragma once
#include <string>

class IExchangeClient {
public:
    virtual ~IExchangeClient() = default;
    virtual void connect() = 0;
    virtual void subscribe(const std::string& symbol) = 0;
};
