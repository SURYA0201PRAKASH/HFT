#include "ws_client_day3.hpp"
#include <iostream>
#include <chrono>
#include <thread>
// -------------------- Constructor --------------------
WSClient::WSClient(net::io_context& ioc,
                   ssl::context& ctx,
                   const std::string& host,
                   const std::string& port,
                   const std::string& target)
    : resolver_(ioc),
      ws_(ioc, ctx),
      host_(host),
      port_(port),
      target_(target),
      heartbeat_timer_(ioc),
      reconnect_timer_(ioc)
{
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    ws_.next_layer().set_verify_callback(
        ssl::rfc2818_verification(host_)
    );

    log_file_.open("ws_log.txt", std::ios::app);
}

// -------------------- Virtual Defaults --------------------
void WSClient::on_handshake(const beast::error_code& ec) {
    (void)ec;
}

void WSClient::on_read(const beast::error_code& ec, std::size_t bytes_transferred) {
	if (ec) {
        // ADD DETAILED LOGGING HERE:
        std::cout << "❌ [WSClient::on_read] ERROR: " << ec.message() 
                  << " (category: " << ec.category().name() 
                  << ", value: " << ec.value() << ")" << std::endl;
        
        if (ec == beast::websocket::error::closed) {
            std::cout << "🔌 [WSClient] WebSocket closed by peer" << std::endl;
        } else if (ec == boost::asio::error::eof) {
            std::cout << "🔌 [WSClient] TCP EOF received" << std::endl;
        } else if (ec == boost::asio::error::connection_reset) {
            std::cout << "🔌 [WSClient] Connection reset by peer" << std::endl;
        } else if (ec == boost::asio::error::operation_aborted) {
            std::cout << "⏹️ [WSClient] Operation aborted" << std::endl;
        } else if (ec == boost::asio::error::timed_out) {
            std::cout << "⏰ [WSClient] Operation timed out" << std::endl;
        }
        
        connected_ = false;
        reconnect();  // This calls WSClient::reconnect()
        return;
    }
    (void)ec;
    (void)bytes_transferred;
}

// -------------------- Async Connect Chain --------------------
void WSClient::run() {
    if (connected_) {
        std::cout << "⚠️ [WSClient] run() called but connection already open, skipping.\n";
        return;
    }
    if (shutting_down_.load()) {
        std::cout << "⚠️ [WSClient] run() called during shutdown, skipping.\n";
        return;
    }

    resolver_.async_resolve(host_, port_,
        beast::bind_front_handler(&WSClient::on_resolve, shared_from_this()));
}


void WSClient::on_resolve(const beast::error_code& ec, tcp::resolver::results_type results) {
 	std::cout << "🔍 DNS resolution callback, ec: " << ec.message() << "\n";
    if (ec) {
        reconnect();
        return;
    }

    net::async_connect(
        ws_.next_layer().next_layer(),
        results.begin(),
        results.end(),
        beast::bind_front_handler(&WSClient::on_connect, shared_from_this())
    );
}

void WSClient::on_connect(const beast::error_code& ec, tcp::resolver::results_type::iterator it) {
// 	std::cout << "🔍 TCP connect callback, ec: " << ec.message() << "\n";
    if (ec) {
        std::cerr << "❌ Connect error: " << ec.message() << "\n";
        reconnect();
        return;
    }

    // ✅ Set SNI hostname (required by Deribit and most TLS servers)
    if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_.c_str())) {
        beast::error_code ec2{ static_cast<int>(::ERR_get_error()), net::error::get_ssl_category() };
        std::cerr << "❌ SNI set failed: " << ec2.message() << "\n";
        return;
    }

    // Now perform TLS handshake
    ws_.next_layer().async_handshake(
        ssl::stream_base::client,
        beast::bind_front_handler(&WSClient::on_ssl_handshake, shared_from_this())
    );
}

void WSClient::on_ssl_handshake(const beast::error_code& ec) {
//     std::cout << "🔍 SSL handshake callback called" << std::endl;
    
    if (ec) {
        std::cerr << "❌ SSL handshake failed: " << ec.message() << "\n";
        if (ec.category() == net::error::get_ssl_category()) {
            std::cerr << "SSL error code: " << ERR_get_error() << "\n";
        }
        reconnect();
        return;
    }

//     std::cout << "✅ SSL handshake ok, starting websocket handshake...\n";
    
    // Add debug for SNI
    if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_.c_str())) {
        beast::error_code ec2{ static_cast<int>(::ERR_get_error()), net::error::get_ssl_category() };
        std::cerr << "❌ SNI set failed: " << ec2.message() << "\n";
        reconnect();
        return;
    }
//     std::cout << "✅ SNI hostname set: " << host_ << "\n";

    ws_.async_handshake(
        host_,
        target_,
        beast::bind_front_handler(&WSClient::on_handshake, shared_from_this())
    );
//     std::cout << "🔍 WebSocket handshake async operation started\n";
}

// -------------------- Read --------------------
void WSClient::do_read() {
    if (shutting_down_.load()) {
        std::cout << "⏹ [WSClient] Skipping do_read() during shutdown\n";
        return;
    }

    //SuPr1 std::cout << "🔍 do_read() called - setting up async read\n";
    ws_.async_read(buffer_,
        [self = shared_from_this()](beast::error_code ec, std::size_t bytes_transferred) {
            //std::cout << "🔍 async_read callback setup\n";
            self->on_read(ec, bytes_transferred);
        }
    );
}


// -------------------- Logging --------------------
void WSClient::log_message(const std::string& raw) {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms  = duration_cast<milliseconds>(now.time_since_epoch()).count();
    log_file_ << ms << " " << raw << "\n";
    log_file_.flush();
}

// -------------------- Heartbeat & Reconnect --------------------
void WSClient::start_heartbeat() {
    heartbeat_timer_.expires_after(std::chrono::seconds(15));
    heartbeat_timer_.async_wait(
        [self = shared_from_this()](const boost::system::error_code& ec) {
            self->on_heartbeat(ec);
        }
    );
}

void WSClient::on_heartbeat(const boost::system::error_code& ec) {
    if (ec) {
        std::cout << "❌ [WSClient] Heartbeat timer error: " << ec.message() << "\n";
        return;
    }
    if (!connected_) {
        std::cout << "⚠️ [WSClient] Not connected, skipping ping\n";
        return;
    }

    std::cout << "💓 Sending WebSocket ping...\n";

    ws_.async_ping({},
        [self = shared_from_this()](beast::error_code ec) {
            if (ec) {
                std::cout << "❌ [WSClient] Ping failed: " << ec.message() << "\n";
                self->connected_ = false;
                self->reconnect();
            } else {
                std::cout << "💓 Pong received OK\n";
            }
        }
    );

    start_heartbeat(); // schedule next
}


void WSClient::reconnect() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    std::cout << "🔄 [RECONNECT] Initiated at: " << ms << std::endl;
    std::cout << "🔄 [RECONNECT] Current connected state: " << connected_ << std::endl;

    // Forcefully mark disconnected (but only if truly not open)
    if (connected_) {
        std::cout << "⚠️ [RECONNECT] Connection still active — skipping reconnect.\n";
        return;
    }

    // Arm the timer
    reconnect_timer_.expires_after(std::chrono::seconds(3));

    reconnect_timer_.async_wait(
        [self = shared_from_this()](boost::system::error_code ec) {
            std::cout << "🔄 [RECONNECT] Timer callback - EC: " << ec.message() << std::endl;
            if (ec) {
                std::cout << "❌ [RECONNECT] Timer error: " << ec.message() << std::endl;
                return;
            }

            if (self->connected_) {
                std::cout << "⚠️ [RECONNECT] Skipped — connection still open.\n";
                return;
            }

            std::cout << "🔄 [RECONNECT] Restarting connection...\n";
            self->run();
        }
    );
}


void WSClient::send_text(const std::string& msg) {
//     //std::cout << "🔍 send_text() called, message length: " << msg.length() << "\n";
    
    // Add delay to prevent concurrent writes
    //std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    ws_.async_write(
        net::buffer(msg),
        [self = shared_from_this()](beast::error_code ec, std::size_t bytes_transferred) {
//             //std::cout << "🔍 async_write callback, ec: " << ec.message() 
            //          << ", bytes: " << bytes_transferred << "\n";
            if (ec) {
                std::cerr << "❌ Send error: " << ec.message() << "\n";
                self->reconnect();
            } else {
//             //    std::cout << "✅ Message sent successfully\n";
            }
        }
    );
//     std::cout << "🔍 async_write operation started\n";
}
// ================= Destructor Debug =================
WSClient::~WSClient() {
    shutting_down_.store(true);
    std::cout << "🧹 [WSClient] Destructor called\n";

    if (ws_.is_open()) {
        boost::beast::error_code ec;
        ws_.close(boost::beast::websocket::close_code::normal, ec);
        if (ec)
            std::cerr << "⚠️ [WSClient] Close error: " << ec.message() << "\n";
    }
	if (connected_) {
        beast::error_code ec;
        ws_.close(boost::beast::websocket::close_code::normal, ec);
        if (ec)
            std::cerr << "⚠️ [WSClient] Close error: " << ec.message() << "\n";
        else
            std::cout << "🧹 [WSClient] Closed websocket cleanly\n";
        connected_ = false;
    }
    boost::system::error_code ec;
    reconnect_timer_.cancel(ec);
    if (ec)
        std::cerr << "⚠️ [WSClient] Timer cancel error: " << ec.message() << "\n";
    else
        std::cout << "⏹ [WSClient] Reconnect timer cancelled\n";
}


