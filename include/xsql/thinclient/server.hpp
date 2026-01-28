#pragma once

/**
 * @file server.hpp
 * @brief HTTP server wrapper for *sql tools
 *
 * Provides HTTP endpoints for SQL queries. Uses cpp-httplib.
 * Enable with XSQL_WITH_THINCLIENT CMake option.
 */

#ifdef XSQL_HAS_THINCLIENT

// Windows SDK compatibility for cpp-httplib
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#include <httplib.h>
#include <string>
#include <functional>
#include <atomic>
#include <thread>
#include <mutex>
#include <iostream>

namespace xsql::thinclient {

// ============================================================================
// Server Configuration
// ============================================================================

/**
 * Route setup callback.
 * Called with the httplib::Server& so you can register your own routes.
 * This gives full control over endpoints to the application.
 */
using route_setup_t = std::function<void(httplib::Server& svr)>;

struct server_config {
    int port = 8081;
    std::string bind_address = "127.0.0.1";
    std::string auth_token;
    bool allow_insecure_no_auth = false;

    // Required: callback to set up your routes on the httplib::Server
    route_setup_t setup_routes;
};

// ============================================================================
// HTTP Server
// ============================================================================

class server {
public:
    explicit server(const server_config& config)
        : config_(config), running_(false) {}

    ~server() {
        stop();
    }

    /**
     * Start server (blocking).
     * Returns when server is stopped.
     */
    void run() {
        // Let the application set up its routes
        if (config_.setup_routes) {
            config_.setup_routes(svr_);
        }

        auto is_loopback_bind_address = [](const std::string& addr) -> bool {
            return addr == "localhost" || addr == "127.0.0.1" || addr == "::1" || addr.rfind("127.", 0) == 0;
        };

        if (!config_.allow_insecure_no_auth && config_.auth_token.empty() && !is_loopback_bind_address(config_.bind_address)) {
            std::cerr << "Refusing to bind to " << config_.bind_address
                      << " without auth token. Set server_config::auth_token or allow_insecure_no_auth=true.\n";
            return;
        }

        running_ = true;
        svr_.listen(config_.bind_address.c_str(), config_.port);
        running_ = false;
    }

    /**
     * Start server in background thread.
     */
    void run_async() {
        server_thread_ = std::thread([this] { run(); });
        // Wait for server to start
        while (!running_ && !svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    /**
     * Stop server gracefully.
     */
    void stop() {
        if (svr_.is_running()) {
            svr_.stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        running_ = false;
    }

    bool is_running() const { return running_; }
    int port() const { return config_.port; }

    /**
     * Get a reference to the underlying httplib::Server.
     * Useful for advanced configuration.
     */
    httplib::Server& http_server() { return svr_; }

    /**
     * Check authorization from request.
     * Returns true if authorized, false if not (and sets 401 response).
     * Applications can use this helper in their route handlers.
     */
    bool authorize(const httplib::Request& req, httplib::Response& res) const {
        if (config_.auth_token.empty()) return true;

        std::string token;
        if (req.has_header("X-XSQL-Token")) {
            token = req.get_header_value("X-XSQL-Token");
        } else if (req.has_header("Authorization")) {
            const std::string auth = req.get_header_value("Authorization");
            const std::string prefix = "Bearer ";
            if (auth.rfind(prefix, 0) == 0) {
                token = auth.substr(prefix.size());
            }
        }

        if (token == config_.auth_token) return true;

        res.status = 401;
        res.set_content(R"({"success":false,"error":"Unauthorized"})", "application/json");
        return false;
    }

    /**
     * Schedule a graceful shutdown after the current response.
     * Applications can call this from their shutdown endpoint.
     */
    void schedule_shutdown() {
        std::thread([this] {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            svr_.stop();
        }).detach();
    }

private:
    server_config config_;
    httplib::Server svr_;
    std::thread server_thread_;
    std::atomic<bool> running_;
};

}  // namespace xsql::thinclient

#else  // !XSQL_HAS_THINCLIENT

#include <stdexcept>

// Stub when thinclient not enabled
namespace xsql::thinclient {

struct server_config {};

class server {
public:
    explicit server(const server_config&) {
        throw std::runtime_error("Thin client not enabled. Build with XSQL_WITH_THINCLIENT=ON");
    }
    void run() {}
    void run_async() {}
    void stop() {}
    bool is_running() const { return false; }
    int port() const { return 0; }
};

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
