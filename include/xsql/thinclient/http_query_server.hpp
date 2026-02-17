#pragma once

/**
 * @file http_query_server.hpp
 * @brief Consolidated HTTP query server for all *sql tools
 *
 * Provides the standard 5 endpoints (/, /help, /query, /status, /shutdown)
 * with two execution modes:
 *   - Direct: query callback runs on httplib worker thread (bnsql-style)
 *   - Command-queue: queries are queued for main-thread execution (idasql-style)
 *
 * Replaces the per-tool *HTTPServer classes with a single reusable implementation.
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
#include <xsql/json.hpp>

#include <atomic>
#include <cctype>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>

namespace xsql::thinclient {

// ============================================================================
// Configuration
// ============================================================================

struct http_query_server_config {
    /// Tool name shown in responses (e.g., "idasql", "bnsql")
    std::string tool_name;

    /// Help text returned by GET /help
    std::string help_text;

    /// Port to listen on (0 = random port in 8100-8199)
    int port = 0;

    /// Bind address (default: localhost only)
    std::string bind_address = "127.0.0.1";

    /// Auth token (empty = no auth required).
    /// When set, all endpoints except GET / and GET /help require
    /// Authorization: Bearer <token> or X-XSQL-Token: <token>.
    std::string auth_token;

    /// Query callback: SQL string in, JSON string out.
    /// Thread safety is the caller's responsibility when use_queue=false.
    using query_fn_t = std::function<std::string(const std::string& sql)>;
    query_fn_t query_fn;

    /// Optional: extra fields merged into GET /status response.
    /// Return a JSON object; its fields are merged with the base response.
    using status_fn_t = std::function<xsql::json()>;
    status_fn_t status_fn;

    /// Optional: register additional endpoints beyond the standard 5.
    /// Called after standard routes are set up.
    using extra_routes_fn_t = std::function<void(httplib::Server& svr)>;
    extra_routes_fn_t extra_routes;

    /// If true, queries are queued for main-thread execution via
    /// run_until_stopped() / process_one_command(). Required for tools
    /// with thread-affinity constraints (e.g., idasql/Hex-Rays).
    bool use_queue = false;
};

// ============================================================================
// http_query_server
// ============================================================================

class http_query_server {
public:
    explicit http_query_server(const http_query_server_config& config)
        : config_(config) {}

    ~http_query_server() { stop(); }

    // Non-copyable
    http_query_server(const http_query_server&) = delete;
    http_query_server& operator=(const http_query_server&) = delete;

    /**
     * Start HTTP server on configured port.
     * Runs in a background thread.
     * @return Actual port used, or -1 on failure.
     */
    int start() {
        if (running_.load()) return port_;

        int port = config_.port;
        if (port == 0) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(8100, 8199);
            port = dis(gen);
        }

        svr_ = std::make_unique<httplib::Server>();
        setup_routes(*svr_, port);

        if (config_.extra_routes) {
            config_.extra_routes(*svr_);
        }

        port_ = port;
        server_thread_ = std::thread([this, port]() {
            svr_->listen(config_.bind_address.c_str(), port);
        });

        // Wait for server to start
        int attempts = 0;
        while (!svr_->is_running() && attempts < 50) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            attempts++;
        }

        if (!svr_->is_running()) {
            server_thread_.detach();
            svr_.reset();
            return -1;
        }

        running_.store(true);
        return port_;
    }

    /**
     * Block until stopped, processing queued commands on the calling thread.
     * Only meaningful when config.use_queue = true.
     */
    void run_until_stopped() {
        while (running_.load()) {
            if (interrupt_check_ && interrupt_check_()) {
                stop();
                break;
            }
            process_one_command_internal(std::chrono::milliseconds(100));
        }
    }

    /**
     * Process one queued command if available.
     * @return true if a command was processed, false if queue was empty.
     */
    bool process_one_command() {
        return process_one_command_internal(std::chrono::milliseconds(0));
    }

    /** Stop the server gracefully. */
    void stop() {
        running_.store(false);
        queue_cv_.notify_all();
        drain_pending_commands(
            xsql::json{{"success", false}, {"error", "HTTP server stopped"}}.dump());

        if (svr_ && svr_->is_running()) {
            svr_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        svr_.reset();
    }

    bool is_running() const { return running_.load(); }
    int port() const { return port_; }

    std::string url() const {
        std::ostringstream ss;
        ss << "http://" << config_.bind_address << ":" << port_;
        return ss.str();
    }

    /** Set interrupt check function (called during run_until_stopped loop). */
    void set_interrupt_check(std::function<bool()> check) {
        interrupt_check_ = std::move(check);
    }

    /** Access the underlying httplib::Server (valid after start()). */
    httplib::Server* http_server() { return svr_.get(); }

private:
    // ========================================================================
    // Internal command queue (for use_queue mode)
    // ========================================================================

    struct pending_command {
        std::string sql;
        std::string result;
        bool completed = false;
        std::mutex done_mutex;
        std::condition_variable done_cv;
    };

    std::string queue_and_wait(const std::string& sql) {
        if (!running_.load()) {
            return xsql::json{{"success", false}, {"error", "Server not running"}}.dump();
        }

        auto cmd = std::make_shared<pending_command>();
        cmd->sql = sql;

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            pending_commands_.push(cmd);
        }
        queue_cv_.notify_one();

        // Wait for main thread to execute with 60s timeout
        {
            std::unique_lock<std::mutex> lock(cmd->done_mutex);
            int wait_count = 0;
            while (!cmd->completed && wait_count < 600) {
                cmd->done_cv.wait_for(lock, std::chrono::milliseconds(100));
                wait_count++;
            }
            if (!cmd->completed) {
                return xsql::json{{"success", false}, {"error", "Request timed out"}}.dump();
            }
        }

        return cmd->result;
    }

    bool process_one_command_internal(std::chrono::milliseconds timeout) {
        std::shared_ptr<pending_command> cmd;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (timeout.count() > 0) {
                queue_cv_.wait_for(lock, timeout,
                    [this]() { return !pending_commands_.empty() || !running_.load(); });
            }
            if (!pending_commands_.empty()) {
                cmd = pending_commands_.front();
                pending_commands_.pop();
            }
        }

        if (!cmd) return false;

        try {
            if (config_.query_fn) {
                cmd->result = config_.query_fn(cmd->sql);
            } else {
                cmd->result = xsql::json{{"success", false}, {"error", "No query handler"}}.dump();
            }
        } catch (const std::exception& e) {
            cmd->result = xsql::json{{"success", false}, {"error", e.what()}}.dump();
        }

        {
            std::lock_guard<std::mutex> lock(cmd->done_mutex);
            cmd->completed = true;
        }
        cmd->done_cv.notify_one();
        return true;
    }

    void drain_pending_commands(const std::string& result) {
        std::queue<std::shared_ptr<pending_command>> pending;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            std::swap(pending, pending_commands_);
        }

        while (!pending.empty()) {
            auto cmd = pending.front();
            pending.pop();
            if (!cmd) continue;

            {
                std::lock_guard<std::mutex> lock(cmd->done_mutex);
                if (!cmd->completed) {
                    cmd->result = result;
                    cmd->completed = true;
                }
            }
            cmd->done_cv.notify_one();
        }
    }

    // ========================================================================
    // Auth helper
    // ========================================================================

    bool check_auth(const httplib::Request& req, httplib::Response& res) const {
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

    // ========================================================================
    // Route setup
    // ========================================================================

    void setup_routes(httplib::Server& svr, int port) {
        const auto& tool = config_.tool_name;

        // GET / - Welcome message (public, no auth)
        svr.Get("/", [tool, port](const httplib::Request&, httplib::Response& res) {
            // Uppercase tool name for display (e.g. "pdbsql" -> "PDBSQL HTTP Server")
            std::string display = tool;
            for (auto& c : display) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            std::string welcome = display + " HTTP Server\n\nEndpoints:\n"
                "  GET  /help     - API documentation\n"
                "  POST /query    - Execute SQL query\n"
                "  GET  /status   - Health check\n"
                "  POST /shutdown - Stop server\n\n"
                "Example: curl -X POST http://localhost:" + std::to_string(port) +
                "/query -d \"SELECT name FROM funcs LIMIT 5\"\n";
            res.set_content(welcome, "text/plain");
        });

        // GET /help - API documentation (public, no auth)
        auto help_text = config_.help_text;
        svr.Get("/help", [help_text](const httplib::Request&, httplib::Response& res) {
            res.set_content(help_text, "text/plain");
        });

        // POST /query - Execute SQL
        svr.Post("/query", [this](const httplib::Request& req, httplib::Response& res) {
            if (!check_auth(req, res)) return;

            if (req.body.empty()) {
                res.status = 400;
                res.set_content(
                    xsql::json{{"success", false}, {"error", "Empty query"}}.dump(),
                    "application/json");
                return;
            }

            std::string result;
            if (config_.use_queue) {
                result = queue_and_wait(req.body);
            } else {
                if (!config_.query_fn) {
                    res.status = 500;
                    res.set_content(
                        xsql::json{{"success", false}, {"error", "Query callback not set"}}.dump(),
                        "application/json");
                    return;
                }
                result = config_.query_fn(req.body);
            }
            res.set_content(result, "application/json");
        });

        // GET /status - Server status
        svr.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
            if (!check_auth(req, res)) return;

            xsql::json status = {
                {"success", true},
                {"status", "ok"},
                {"tool", config_.tool_name}
            };
            if (config_.status_fn) {
                auto extra = config_.status_fn();
                status.merge_patch(extra);
            }
            res.set_content(status.dump(), "application/json");
        });

        // POST /shutdown - Graceful shutdown
        svr.Post("/shutdown", [this](const httplib::Request& req, httplib::Response& res) {
            if (!check_auth(req, res)) return;

            res.set_content(
                xsql::json{{"success", true}, {"message", "Shutting down"}}.dump(),
                "application/json");
            std::thread([this] {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                stop();
            }).detach();
        });
    }

    // ========================================================================
    // State
    // ========================================================================

    http_query_server_config config_;
    std::unique_ptr<httplib::Server> svr_;
    std::thread server_thread_;
    std::atomic<bool> running_{false};
    int port_{0};

    std::function<bool()> interrupt_check_;

    // Command queue (for use_queue mode)
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<std::shared_ptr<pending_command>> pending_commands_;
};

// ============================================================================
// Format helpers
// ============================================================================

/**
 * Format HTTP server info for display.
 */
inline std::string format_http_info(const std::string& tool, int port,
                                     const std::string& stop_hint = "Press Ctrl+C to stop and return to REPL.") {
    std::ostringstream ss;
    ss << "HTTP server started on port " << port << "\n";
    ss << "URL: http://127.0.0.1:" << port << "\n\n";
    ss << "Endpoints:\n";
    ss << "  GET  /help     - API documentation\n";
    ss << "  POST /query    - Execute SQL query\n";
    ss << "  GET  /status   - Health check\n";
    ss << "  POST /shutdown - Stop server\n\n";
    ss << "Example:\n";
    ss << "  curl -X POST http://127.0.0.1:" << port << "/query -d \"SELECT name FROM funcs LIMIT 5\"\n\n";
    ss << stop_hint << "\n";
    return ss.str();
}

/**
 * Format HTTP server status.
 */
inline std::string format_http_status(int port, bool running) {
    std::ostringstream ss;
    if (running) {
        ss << "HTTP server running on port " << port << "\n";
        ss << "URL: http://127.0.0.1:" << port << "\n";
    } else {
        ss << "HTTP server not running\n";
        ss << "Use '.http start' to start\n";
    }
    return ss.str();
}

}  // namespace xsql::thinclient

#else  // !XSQL_HAS_THINCLIENT

#include <stdexcept>
#include <string>
#include <functional>

namespace xsql::thinclient {

struct http_query_server_config {
    std::string tool_name;
    std::string help_text;
    int port = 0;
    std::string bind_address = "127.0.0.1";
    std::string auth_token;
    using query_fn_t = std::function<std::string(const std::string& sql)>;
    query_fn_t query_fn;
    using status_fn_t = std::function<void()>;
    status_fn_t status_fn;
    using extra_routes_fn_t = std::function<void(int)>;
    extra_routes_fn_t extra_routes;
    bool use_queue = false;
};

class http_query_server {
public:
    explicit http_query_server(const http_query_server_config&) {
        throw std::runtime_error("Thin client not enabled. Build with XSQL_WITH_THINCLIENT=ON");
    }
    int start() { return -1; }
    void run_until_stopped() {}
    bool process_one_command() { return false; }
    void stop() {}
    bool is_running() const { return false; }
    int port() const { return 0; }
    std::string url() const { return ""; }
    void set_interrupt_check(std::function<bool()>) {}
};

inline std::string format_http_info(const std::string&, int, const std::string& = "") { return ""; }
inline std::string format_http_status(int, bool) { return ""; }

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
