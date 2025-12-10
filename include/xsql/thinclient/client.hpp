#pragma once

/**
 * @file client.hpp
 * @brief HTTP client wrapper for *sql tools
 *
 * Connects to a running *sql server and executes queries.
 * Uses cpp-httplib.
 * Enable with XSQL_WITH_THINCLIENT CMake option.
 */

#ifdef XSQL_HAS_THINCLIENT

#include <httplib.h>
#include <string>
#include <stdexcept>

namespace xsql::thinclient {

// ============================================================================
// Client Configuration
// ============================================================================

struct client_config {
    std::string host = "127.0.0.1";
    int port = 5555;
    int timeout_sec = 30;
};

// ============================================================================
// HTTP Client
// ============================================================================

class client {
public:
    explicit client(const client_config& config = {})
        : config_(config)
        , cli_(config.host, config.port)
    {
        cli_.set_connection_timeout(config.timeout_sec);
        cli_.set_read_timeout(config.timeout_sec);
        cli_.set_write_timeout(config.timeout_sec);
    }

    /**
     * Execute SQL query on server.
     * @param sql SQL query string
     * @return Result string (CSV by default)
     * @throws std::runtime_error on connection or query error
     */
    std::string query(const std::string& sql) {
        auto res = cli_.Post("/query", sql, "text/plain");
        check_response(res, "query");

        if (res->status != 200) {
            throw std::runtime_error("Query error: " + res->body);
        }

        return res->body;
    }

    /**
     * Get server status.
     * @return JSON status string
     */
    std::string status() {
        auto res = cli_.Get("/status");
        check_response(res, "status");
        return res->body;
    }

    /**
     * Request server shutdown.
     */
    void shutdown() {
        auto res = cli_.Post("/shutdown", "", "text/plain");
        // Don't check response - server may close connection before responding
    }

    /**
     * Check if server is reachable.
     */
    bool ping() {
        auto res = cli_.Get("/status");
        return res && res->status == 200;
    }

private:
    client_config config_;
    httplib::Client cli_;

    void check_response(const httplib::Result& res, const char* operation) {
        if (!res) {
            std::string msg = "Connection failed (" + std::string(operation) + "): ";
            msg += "Could not connect to " + config_.host + ":" + std::to_string(config_.port);
            throw std::runtime_error(msg);
        }
    }
};

}  // namespace xsql::thinclient

#else  // !XSQL_HAS_THINCLIENT

// Stub when thinclient not enabled
namespace xsql::thinclient {

struct client_config {
    std::string host = "127.0.0.1";
    int port = 5555;
    int timeout_sec = 30;
};

class client {
public:
    explicit client(const client_config& = {}) {
        throw std::runtime_error("Thin client not enabled. Build with XSQL_WITH_THINCLIENT=ON");
    }
    std::string query(const std::string&) { return {}; }
    std::string status() { return {}; }
    void shutdown() {}
    bool ping() { return false; }
};

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
