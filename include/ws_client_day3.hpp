#pragma once

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <set>
#include <thread>      // Add this
#include <chrono>      // Add this
#include "nlohmann/json.hpp"
#include <mutex>
#include <atomic>  // if not already present

namespace beast      = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
using tcp           = net::ip::tcp;
using json          = nlohmann::json;

class WSClient : public std::enable_shared_from_this<WSClient> {
public:
    WSClient(net::io_context& ioc,
             ssl::context& ctx,
             const std::string& host,
             const std::string& port,
             const std::string& target);

    virtual ~WSClient();

    void run();
    void send_text(const std::string& msg);
    bool connected_ = false;
protected:
    // Overridable hooks
    virtual void on_handshake(const beast::error_code& ec);
    virtual void on_read(const beast::error_code& ec, std::size_t bytes_transferred);

    // Async chain
    void on_resolve(const beast::error_code& ec, tcp::resolver::results_type results);
    void on_connect(const beast::error_code& ec, tcp::resolver::results_type::iterator it);
    void on_ssl_handshake(const beast::error_code& ec);
    void do_read();
    virtual void on_connected() {}
    virtual void on_authenticated() {}
    // Logging
    void log_message(const std::string& raw);

    // Heartbeat & reconnect
    void start_heartbeat();
    void on_heartbeat(const boost::system::error_code& ec);
    void reconnect();

    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_;
    beast::flat_buffer buffer_;
    std::string host_, port_, target_;
    std::ofstream log_file_;
    net::steady_timer heartbeat_timer_;
    net::steady_timer reconnect_timer_;
    int last_seq_number_ = 0;
    std::set<std::string> subscribed_channels_;
	std::mutex write_mutex_;
	std::atomic<bool> shutting_down_{false};
};