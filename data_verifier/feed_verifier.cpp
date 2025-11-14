#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <iomanip>
#include <optional>
#include <sqlite3.h>
#include <cmath>

using tcp = boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
using json = nlohmann::json;

// ------------------- DB Snapshot Struct -------------------
struct DbSnapshot {
    double price = 0.0;
    long long ts_ms = 0;
};

// ------------------- SQLite Tail Class -------------------
class SQLiteTail {
public:
    explicit SQLiteTail(const std::string& db_path) {
        if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
            std::cerr << "❌ Cannot open SQLite DB: " << sqlite3_errmsg(db_) << "\n";
            db_ = nullptr;
            return;
        }
        sqlite3_busy_timeout(db_, 2000);
        const char* sql =
            "SELECT trade_price, timestamp_ms FROM ticks_live "
            "WHERE instrument = ? ORDER BY timestamp_ms DESC LIMIT 1;";
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            std::cerr << "❌ sqlite3_prepare_v2 failed: " << sqlite3_errmsg(db_) << "\n";
            stmt_ = nullptr;
        }
    }

    ~SQLiteTail() {
        if (stmt_) sqlite3_finalize(stmt_);
        if (db_) sqlite3_close(db_);
    }

    std::optional<DbSnapshot> get_last(const std::string& inst) {
        if (!db_ || !stmt_) return std::nullopt;
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        sqlite3_bind_text(stmt_, 1, inst.c_str(), -1, SQLITE_TRANSIENT);

        if (sqlite3_step(stmt_) == SQLITE_ROW) {
            DbSnapshot s;
            s.price = sqlite3_column_double(stmt_, 0);
            s.ts_ms = sqlite3_column_int64(stmt_, 1);
            return s;
        }
        return std::nullopt;
    }

private:
    sqlite3* db_ = nullptr;
    sqlite3_stmt* stmt_ = nullptr;
};

// ------------------- Main Program -------------------
int main(int argc, char** argv) {
    try {
        std::string db_path = "../data/bybit_stream_features.db";
        if (argc > 1) db_path = argv[1];

        SQLiteTail db(db_path);
        const long long MAX_DT_MS = 250;   // max allowed delay (ms)
        const double MAX_DP_PCT = 0.05;    // max allowed % diff

        // ✅ Bybit public WebSocket (linear futures)
        std::string host = "stream.bybit.com";
        std::string port = "443";
        std::string target = "/v5/public/linear";

        boost::asio::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);

        tcp::resolver resolver(ioc);
        auto const results = resolver.resolve(host, port);
        beast::websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);

        auto& lowest = beast::get_lowest_layer(ws);
        boost::asio::connect(lowest, results);

        if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str())) {
            throw beast::system_error(
                beast::error_code(static_cast<int>(::ERR_get_error()),
                boost::asio::error::get_ssl_category()));
        }

        ws.next_layer().handshake(ssl::stream_base::client);
        ws.handshake(host, target);

        std::cout << "✅ Connected to Bybit public WS\n";

        // --- Subscribe to trades for BTCUSDT & ETHUSDT ---
        json sub = {
            {"op", "subscribe"},
            {"args", {"publicTrade.BTCUSDT", "publicTrade.ETHUSDT"}}
        };

        ws.write(boost::asio::buffer(sub.dump()));
        std::cout << "📡 Subscribed to publicTrade.BTCUSDT / ETHUSDT\n";

        // --- Read Loop ---
        beast::flat_buffer buffer;
        while (true) {
            ws.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());

            json j = json::parse(msg, nullptr, false);
            if (j.is_discarded() || !j.contains("topic") || !j.contains("data"))
                continue;

            std::string topic = j["topic"].get<std::string>();
            if (topic.find("publicTrade.") == std::string::npos) continue;

            for (auto& tr : j["data"]) {
                std::string sym = tr["s"].get<std::string>();
                double px = std::stod(tr["p"].get<std::string>());
                long long ts = tr["T"].get<long long>();

                auto dbopt = db.get_last(sym);
                if (!dbopt) {
                    std::cout << "[SYNC] " << sym << " | waiting for DB rows...\n";
                    continue;
                }

                auto dbs = *dbopt;
                long long dt = std::llabs(ts - dbs.ts_ms);
                double dp_pct = std::abs(px - dbs.price) / px * 100.0;

                std::cout << std::fixed << std::setprecision(5)
                          << "[SYNC] " << sym
                          << " Δt=" << dt << "ms(" << (dt <= MAX_DT_MS ? "OK" : "WARN") << ")"
                          << " Δp=" << dp_pct << "%(" << (dp_pct <= MAX_DP_PCT ? "OK" : "WARN") << ")"
                          << " exch=" << px
                          << " db=" << dbs.price << "\n";
            }
        }

    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << "\n";
    }
}
