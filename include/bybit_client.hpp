#pragma once
#include "exchange_interface.hpp"
#include "bybit_ws_client.hpp"
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <memory>
#include <iostream>

class BybitClient : public IExchangeClient {
private:
    std::shared_ptr<BybitWsClient> ws_client_;

public:
    BybitClient(boost::asio::io_context& ioc, ssl::context& ctx);
    ~BybitClient() override;

    void connect() override;
    void subscribe(const std::string& symbol) override;
};
