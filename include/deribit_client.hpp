#pragma once

#include "ws_client_day3.hpp"
#include "nlohmann/json.hpp"
#include <iostream>
#include <atomic>
#include <string>

using json = nlohmann::json;

class DeribitClient : public WSClient {
public:
    // Connection state tracking - move this before the method declarations
    enum class ConnectionState {
        DISCONNECTED,
        RESOLVING,
        CONNECTING_TCP,
        CONNECTING_SSL,
        CONNECTING_WS,
        CONNECTED,
        AUTHENTICATING,
        AUTHENTICATED,
        SUBSCRIBING,
        SUBSCRIBED,
        READY,
        ERROR_STATE
    };

    DeribitClient(boost::asio::io_context& ioc,
                  ssl::context& ctx,
                  const std::string& host,
                  const std::string& port,
                  const std::string& target,
                  const std::string& client_id,
                  const std::string& client_secret);

    // Public methods for connection state querying
    std::string get_connection_state_str() const;
    bool is_fully_connected() const;
    void print_connection_status() const;

protected:
    // Overrides from WSClient
    void on_handshake(const beast::error_code& ec) override;
    void on_read(const beast::error_code& ec, std::size_t bytes_transferred) override;
    void on_connected() override;
    
    // Connection management
    void send_auth();
    void send_subscribe();
    void handle_trade_data(const json& j);
    void start_keepalive();
    void send_keepalive();
    void update_connection_state(ConnectionState new_state);  // Now ConnectionState is known
    
    // Timer management
    net::steady_timer& get_keepalive_timer() { return keepalive_timer_; }
    void cancel_all_timers();

private:
    std::string client_id_;
    std::string client_secret_;
    net::steady_timer keepalive_timer_;
    
    // Connection state tracking - declaration moved to public section
    std::atomic<ConnectionState> connection_state_{ConnectionState::DISCONNECTED};
    std::chrono::steady_clock::time_point last_state_change_;
    
    // Statistics
    std::atomic<uint64_t> messages_received_{0};
    std::atomic<uint64_t> messages_sent_{0};
    std::atomic<uint64_t> auth_attempts_{0};
    std::atomic<uint64_t> subscribe_attempts_{0};
    
    // Debug flags
    bool enable_verbose_logging_{true};
    bool enable_json_debug_{true};
};

// Inline implementation for frequently used methods
inline std::string DeribitClient::get_connection_state_str() const {
    switch (connection_state_.load()) {
        case ConnectionState::DISCONNECTED: return "DISCONNECTED";
        case ConnectionState::RESOLVING: return "RESOLVING";
        case ConnectionState::CONNECTING_TCP: return "CONNECTING_TCP";
        case ConnectionState::CONNECTING_SSL: return "CONNECTING_SSL";
        case ConnectionState::CONNECTING_WS: return "CONNECTING_WS";
        case ConnectionState::CONNECTED: return "CONNECTED";
        case ConnectionState::AUTHENTICATING: return "AUTHENTICATING";
        case ConnectionState::AUTHENTICATED: return "AUTHENTICATED";
        case ConnectionState::SUBSCRIBING: return "SUBSCRIBING";
        case ConnectionState::SUBSCRIBED: return "SUBSCRIBED";
        case ConnectionState::READY: return "READY";
        case ConnectionState::ERROR_STATE: return "ERROR_STATE";
        default: return "UNKNOWN";
    }
}

inline bool DeribitClient::is_fully_connected() const {
    auto state = connection_state_.load();
    return state == ConnectionState::READY || state == ConnectionState::SUBSCRIBED;
}

inline void DeribitClient::print_connection_status() const {
    auto now = std::chrono::steady_clock::now();
    auto state_duration = std::chrono::duration_cast<std::chrono::seconds>(
        now - last_state_change_).count();
    
    std::cout << "📊 CONNECTION STATUS: " << get_connection_state_str()
              << " (for " << state_duration << "s)"
              << ", Msgs RX: " << messages_received_.load()
              << ", Msgs TX: " << messages_sent_.load()
              << ", Auth attempts: " << auth_attempts_.load()
              << ", Subscribe attempts: " << subscribe_attempts_.load()
              << "\n";
}