#include "ws_client_day3.hpp"
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <iostream>
#include "bybit_ws_client.hpp"

int main() {
    try {
        net::io_context ioc;
        ssl::context ctx(ssl::context::tls_client);

        auto client = std::make_shared<BybitWsClient>(ioc, ctx);
        client->run();

        ioc.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
    }
    return 0;
}
