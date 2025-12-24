#pragma once

/**
 * @file server.hpp
 * @brief HTTP server wrapper for *sql tools
 *
 * Provides HTTP endpoints for SQL queries. Uses cpp-httplib.
 * Enable with XSQL_WITH_THINCLIENT CMake option.
 */

#ifdef XSQL_HAS_THINCLIENT

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
 * Query handler callback.
 * Takes SQL string, returns result string (CSV/JSON).
 * Throw exception on error.
 */
using query_handler_t = std::function<std::string(const std::string& sql)>;

/**
 * Status handler callback (optional).
 * Returns JSON status string.
 */
using status_handler_t = std::function<std::string()>;

struct server_config {
    int port = 5555;
    std::string bind_address = "127.0.0.1";
    std::string auth_token;

    // Required: handles SQL queries
    query_handler_t on_query;

    // Optional: returns status JSON
    status_handler_t on_status;

    // Optional: called on shutdown request
    std::function<void()> on_shutdown;
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
        setup_routes();
        running_ = true;
        std::cout << "Listening on " << config_.bind_address << ":" << config_.port << "\n";
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

private:
    server_config config_;
    httplib::Server svr_;
    std::thread server_thread_;
    std::atomic<bool> running_;
    std::mutex query_mutex_;  // Serialize queries (IDA is single-threaded)

    void setup_routes() {
        // POST /query - Execute SQL
        svr_.Post("/query", [this](const httplib::Request& req, httplib::Response& res) {
            handle_query(req, res);
        });

        // GET /status - Health check
        svr_.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
            handle_status(req, res);
        });

        // POST /shutdown - Graceful shutdown
        svr_.Post("/shutdown", [this](const httplib::Request& req, httplib::Response& res) {
            handle_shutdown(req, res);
        });

        // GET / - Simple welcome
        svr_.Get("/", [this](const httplib::Request&, httplib::Response& res) {
            res.set_content("xsql server running. POST /query with SQL.\n", "text/plain");
        });
    }

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
        res.set_content("Unauthorized", "text/plain");
        return false;
    }

    void handle_query(const httplib::Request& req, httplib::Response& res) {    
        if (!authorize(req, res)) return;
        if (!config_.on_query) {
            res.status = 500;
            res.set_content("No query handler configured", "text/plain");       
            return;
        }

        std::string sql = req.body;
        if (sql.empty()) {
            res.status = 400;
            res.set_content("Empty query", "text/plain");
            return;
        }

        try {
            // Serialize queries
            std::lock_guard<std::mutex> lock(query_mutex_);
            std::string result = config_.on_query(sql);
            res.set_content(result, "text/csv");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(std::string("Error: ") + e.what(), "text/plain");
        }
    }

    void handle_status(const httplib::Request& req, httplib::Response& res) {
        if (!authorize(req, res)) return;
        if (config_.on_status) {
            try {
                std::string status = config_.on_status();
                res.set_content(status, "application/json");
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(std::string("{\"error\": \"") + e.what() + "\"}", "application/json");
            }
        } else {
            res.set_content("{\"status\": \"ok\"}", "application/json");
        }
    }

    void handle_shutdown(const httplib::Request& req, httplib::Response& res) {
        if (!authorize(req, res)) return;
        res.set_content("Shutting down\n", "text/plain");
        if (config_.on_shutdown) {
            config_.on_shutdown();
        }
        // Schedule stop after response is sent
        std::thread([this] {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            svr_.stop();
        }).detach();
    }
};

}  // namespace xsql::thinclient

#else  // !XSQL_HAS_THINCLIENT

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
