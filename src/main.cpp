#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "deribit_ws_client.cpp"
#include "bybit_ws_client_unified.cpp"   // ← you will provide this next

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;

int main(int argc, char* argv[]) {
    try {
        if (argc < 2) {
            std::cout << "Usage:\n"
                      << "  ./hft_prototype deribit\n"
                      << "  ./hft_prototype bybit\n"
                      << "  ./hft_prototype both\n";
            return 0;
        }

        std::string mode = argv[1];
        std::cout << "🟦 Selected mode: " << mode << "\n";

        net::io_context ioc;

        // TLS context
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();
        ctx.set_verify_mode(ssl::verify_peer);

        std::unique_ptr<SimpleDeribitClient> deribit;
        std::unique_ptr<BybitWsClient>  bybit;

        // ============================
        // DERIBIT ENABLED?
        // ============================
        if (mode == "deribit" || mode == "both") {
            deribit = std::make_unique<SimpleDeribitClient>(ioc, ctx);

            if(!SSL_set_tlsext_host_name(
                    deribit->get_ssl_stream().native_handle(),
                    "test.deribit.com"))
            {
                std::cerr << "Failed to set SNI hostname for Deribit\n";
                return 1;
            }

            std::cout << "🔌 Connecting to Deribit…\n";
            deribit->connect();
        }

        // ============================
        // BYBIT ENABLED?
        // ============================
        if (mode == "bybit" || mode == "both") {
            bybit = std::make_unique<BybitWsClient>(ioc, ctx);

            if(!SSL_set_tlsext_host_name(
                    bybit->get_ssl_stream().native_handle(),
                    "stream.bybit.com"))
            {
                std::cerr << "Failed to set SNI hostname for Bybit\n";
                return 1;
            }

            std::cout << "🔌 Connecting to Bybit…\n";
            bybit->connect();
        }

        // ============================
        // RUN BOTH IN PARALLEL
        // ============================
        ioc.run();
    }
    catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}
