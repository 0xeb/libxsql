// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <stdexcept>
#include <string>

/**
 * @file client.hpp
 * @brief HTTP client wrapper for *sql tools
 *
 * Connects to a running *sql server and executes queries.
 * Uses cpp-httplib.
 * Enable with XSQL_WITH_THINCLIENT CMake option.
 */

#ifdef XSQL_HAS_THINCLIENT

// Guard against macro-defined strtoull (e.g. `#define strtoull _strtoui64`)
// which breaks httplib's use of std::strtoull
#ifdef strtoull
#pragma push_macro("strtoull")
#undef strtoull
#define _XSQL_RESTORE_STRTOULL
#endif

#include <httplib.h>

#ifdef _XSQL_RESTORE_STRTOULL
#pragma pop_macro("strtoull")
#undef _XSQL_RESTORE_STRTOULL
#endif

namespace xsql::thinclient {

// ============================================================================
// Client Configuration
// ============================================================================

struct client_config {
    std::string host = "127.0.0.1";
    int port = 5555;
    int timeout_sec = 30;
    std::string auth_token;
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
        if (!config.auth_token.empty()) {
            cli_.set_default_headers({{"X-XSQL-Token", config.auth_token}});
        }
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
     * @throws std::runtime_error on connection error or a non-200 response —
     * a 401 Unauthorized body must never be handed back as if it were a
     * status document.
     */
    std::string status() {
        auto res = cli_.Get("/status");
        check_response(res, "status");
        if (res->status != 200) {
            throw std::runtime_error("Status error: " + res->body);
        }
        return res->body;
    }

    /**
     * Request cooperative cancellation of queries already in flight.
     * @throws std::runtime_error on connection error or when the configured
     * server executor does not support cancellation.
     */
    void cancel() {
        auto res = cli_.Post("/cancel", "", "text/plain");
        check_response(res, "cancel");
        if (res->status != 200) {
            throw std::runtime_error("Cancel error: " + res->body);
        }
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
    std::string auth_token;
};

class client {
public:
    explicit client(const client_config& = {}) {
        throw std::runtime_error("Thin client not enabled. Build with XSQL_WITH_THINCLIENT=ON");
    }
    std::string query(const std::string&) { return {}; }
    std::string status() { return {}; }
    void cancel() {}
    void shutdown() {}
    bool ping() { return false; }
};

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
