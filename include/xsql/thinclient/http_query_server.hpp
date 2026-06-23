// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

/**
 * @file http_query_server.hpp
 * @brief Consolidated HTTP query server for all *sql tools
 *
 * Provides the standard 5 endpoints (/, /help, /query, /status, /shutdown)
 * with two execution modes:
 *   - Direct: query callback runs on httplib worker thread
 *   - Command-queue: queries are queued for main-thread execution
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

#include <xsql/json.hpp>
#include <xsql/query_script.hpp>
#include <xsql/thinclient/clipboard.hpp>

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
#include <exception>

namespace xsql::thinclient {

// ============================================================================
// Configuration
// ============================================================================

struct http_query_server_config {
    /// Tool name shown in responses
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

    /// Preferred callback: a single-statement executor. When set, the server
    /// owns multi-statement orchestration (xsql::run_script), query-string
    /// option parsing (continue_on_error / include_sql), and output formatting
    /// (json/text/csv/tsv) directly from the ScriptResult — no JSON round-trip.
    /// Takes precedence over query_fn. The executor runs one statement and fills
    /// `out`; it is invoked on the worker thread (use_queue=false) or the main
    /// thread via the queue (use_queue=true), same threading contract as query_fn.
    using statement_executor_t =
        std::function<void(const std::string& sql, xsql::ScriptStatementResult& out)>;
    statement_executor_t statement_executor;

    /// Optional: extra fields merged into GET /status response.
    /// Return a JSON object; its fields are merged with the base response.
    using status_fn_t = std::function<xsql::json()>;
    status_fn_t status_fn;

    /// Optional: register additional endpoints beyond the standard 5.
    /// Called after standard routes are set up.
    using extra_routes_fn_t = std::function<void(httplib::Server& svr)>;
    extra_routes_fn_t extra_routes;

    /// Queue wait timeout in milliseconds for use_queue mode.
    /// 0 means wait indefinitely.
    int queue_admission_timeout_ms = 60000;

    /// Maximum queued requests in use_queue mode.
    /// 0 means unbounded.
    size_t max_queue = 0;

    /// Optional dynamic queue timeout callback (overrides queue_admission_timeout_ms).
    using queue_timeout_fn_t = std::function<int()>;
    queue_timeout_fn_t queue_admission_timeout_ms_fn;

    /// Optional dynamic max queue callback (overrides max_queue).
    using max_queue_fn_t = std::function<size_t()>;
    max_queue_fn_t max_queue_fn;

    /// If true, queries are queued for main-thread execution via
    /// run_until_stopped() / process_one_command(). Required for tools
    /// with thread-affinity constraints.
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
            // Bind failed (listen() already returned). Tear down cleanly: stop()
            // unblocks listen() if it is somehow still mid-flight, then JOIN the
            // thread (never detach -- a detached thread racing svr_.reset() is a
            // use-after-free) and clear the stale endpoint so status/port() do
            // not report a port we never bound.
            svr_->stop();
            if (server_thread_.joinable()) {
                server_thread_.join();
            }
            svr_.reset();
            port_ = 0;
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
        port_ = 0;  // no stale endpoint after stop; next start() assigns a fresh one
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
        std::string result;              // legacy query_fn path: JSON string
        bool use_executor = false;       // executor path: fill script_result
        xsql::ScriptOptions opts;
        xsql::ScriptResult script_result;
        bool completed = false;
        std::mutex done_mutex;
        std::condition_variable done_cv;
    };

    static xsql::ScriptResult make_error_script_result(const std::string& msg) {
        xsql::ScriptResult r;
        r.success = false;
        r.parse_error = msg;
        return r;
    }

    // Render a ScriptResult into the response per the `format` query param.
    static void set_formatted(httplib::Response& res, const xsql::ScriptResult& sr,
                              const std::string& format, bool include_sql) {
        if (format == "text") {
            res.set_content(xsql::script_result_to_text(sr), "text/plain");
        } else if (format == "csv") {
            res.set_content(xsql::script_result_to_csv(sr), "text/csv");
        } else if (format == "tsv") {
            res.set_content(xsql::script_result_to_tsv(sr), "text/tab-separated-values");
        } else {
            res.set_content(xsql::script_result_to_json(sr, include_sql), "application/json");
        }
    }

    enum class admit_result { ok, not_running, queue_full, timeout };

    // Admit a command to the queue and wait for the main thread to run it.
    // Shared by the legacy string path and the executor path.
    admit_result admit_and_wait(const std::shared_ptr<pending_command>& cmd) {
        if (!running_.load()) {
            return admit_result::not_running;
        }

        int queue_timeout_ms = config_.queue_admission_timeout_ms;
        if (config_.queue_admission_timeout_ms_fn) {
            queue_timeout_ms = config_.queue_admission_timeout_ms_fn();
        }
        if (queue_timeout_ms < 0) {
            queue_timeout_ms = 0;
        }

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            size_t max_queue = config_.max_queue;
            if (config_.max_queue_fn) {
                max_queue = config_.max_queue_fn();
            }
            if (max_queue > 0 && pending_commands_.size() >= max_queue) {
                return admit_result::queue_full;
            }
            pending_commands_.push(cmd);
        }
        queue_cv_.notify_one();

        // Wait for main thread to execute.
        {
            std::unique_lock<std::mutex> lock(cmd->done_mutex);
            if (queue_timeout_ms == 0) {
                while (!cmd->completed) {
                    cmd->done_cv.wait_for(lock, std::chrono::milliseconds(100));
                }
            } else if (!cmd->done_cv.wait_for(lock, std::chrono::milliseconds(queue_timeout_ms),
                                              [&]() { return cmd->completed; })) {
                return admit_result::timeout;
            }
        }
        return admit_result::ok;
    }

    std::string queue_and_wait(const std::string& sql) {
        auto cmd = std::make_shared<pending_command>();
        cmd->sql = sql;
        switch (admit_and_wait(cmd)) {
            case admit_result::not_running:
                return xsql::json{{"success", false}, {"error", "Server not running"}}.dump();
            case admit_result::queue_full:
                return xsql::json{
                    {"success", false}, {"error", "Queue full"},
                    {"hint", "Reduce concurrency or increase max_queue"}}.dump();
            case admit_result::timeout:
                return xsql::json{
                    {"success", false}, {"error", "Request timed out while waiting in queue"},
                    {"hint", "Reduce concurrency or increase queue_admission_timeout_ms"}}.dump();
            case admit_result::ok:
                break;
        }
        return cmd->result;
    }

    xsql::ScriptResult queue_and_wait_script(const std::string& sql,
                                             const xsql::ScriptOptions& opts) {
        auto cmd = std::make_shared<pending_command>();
        cmd->sql = sql;
        cmd->use_executor = true;
        cmd->opts = opts;
        switch (admit_and_wait(cmd)) {
            case admit_result::not_running:
                return make_error_script_result("Server not running");
            case admit_result::queue_full:
                return make_error_script_result("Queue full");
            case admit_result::timeout:
                return make_error_script_result("Request timed out while waiting in queue");
            case admit_result::ok:
                break;
        }
        return cmd->script_result;
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
            if (cmd->use_executor && config_.statement_executor) {
                cmd->script_result =
                    xsql::run_script(cmd->sql, cmd->opts, config_.statement_executor);
            } else if (config_.query_fn) {
                cmd->result = config_.query_fn(cmd->sql);
            } else {
                cmd->result = xsql::json{{"success", false}, {"error", "No query handler"}}.dump();
            }
        } catch (const std::exception& e) {
            if (cmd->use_executor) {
                cmd->script_result = make_error_script_result(e.what());
            } else {
                cmd->result = xsql::json{{"success", false}, {"error", e.what()}}.dump();
            }
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
                "/query -d \"SELECT name FROM sqlite_master WHERE type='table' LIMIT 10\"\n";
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

            try {
                // Output format (default json). text/csv/tsv are for direct
                // terminal/pipe use; json stays the canonical format.
                std::string format = "json";
                auto fmt_it = req.params.find("format");
                if (fmt_it != req.params.end() && !fmt_it->second.empty()) {
                    format = fmt_it->second;
                }

                if (config_.statement_executor) {
                    // Preferred path: the server owns option parsing + run_script
                    // + formatting, straight from the ScriptResult (no round-trip).
                    xsql::ScriptOptions opts;
                    auto cont_it = req.params.find("continue_on_error");
                    if (cont_it != req.params.end() && cont_it->second == "1") {
                        opts.continue_on_error = true;
                    }
                    auto incl_it = req.params.find("include_sql");
                    if (incl_it != req.params.end() && incl_it->second == "1") {
                        opts.include_sql = true;
                    }
                    xsql::ScriptResult script = config_.use_queue
                        ? queue_and_wait_script(req.body, opts)
                        : xsql::run_script(req.body, opts, config_.statement_executor);
                    set_formatted(res, script, format, opts.include_sql);
                } else if (config_.query_fn) {
                    // Legacy path: callback returns a JSON string. Re-parse only
                    // for non-json formats (continue_on_error/include_sql are not
                    // available here — the callback owns its own ScriptOptions).
                    std::string result = config_.use_queue
                        ? queue_and_wait(req.body)
                        : config_.query_fn(req.body);
                    if (format == "json") {
                        res.set_content(result, "application/json");
                    } else {
                        set_formatted(res, xsql::json_to_script_result(result), format, false);
                    }
                } else {
                    res.status = 500;
                    res.set_content(
                        xsql::json{{"success", false}, {"error", "Query callback not set"}}.dump(),
                        "application/json");
                    return;
                }
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(
                    xsql::json{{"success", false}, {"error", e.what()}}.dump(),
                    "application/json");
            } catch (...) {
                res.status = 500;
                res.set_content(
                    xsql::json{{"success", false}, {"error", "Unhandled query exception"}}.dump(),
                    "application/json");
            }
        });

        // GET /status - Server status
        svr.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
            if (!check_auth(req, res)) return;

            try {
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
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(
                    xsql::json{{"success", false}, {"error", e.what()}}.dump(),
                    "application/json");
            } catch (...) {
                res.status = 500;
                res.set_content(
                    xsql::json{{"success", false}, {"error", "Unhandled status exception"}}.dump(),
                    "application/json");
            }
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
inline std::string format_http_info(const std::string& tool,
                                    int port,
                                    const std::string& bind_addr,
                                    const std::string& stop_hint = "Press Ctrl+C to stop and return to REPL.") {
    std::ostringstream ss;
    const std::string rendered_host = format_url_host(bind_addr);
    ss << "HTTP server started on port " << port << "\n";
    ss << "URL: http://" << rendered_host << ":" << port << "\n\n";
    ss << "Endpoints:\n";
    ss << "  GET  /help     - API documentation\n";
    ss << "  POST /query    - Execute SQL query\n";
    ss << "  GET  /status   - Health check\n";
    ss << "  POST /shutdown - Stop server\n\n";
    ss << "Example:\n";
    ss << "  curl -X POST http://" << rendered_host << ":" << port
       << "/query -d \"SELECT name FROM sqlite_master WHERE type='table' LIMIT 10\"\n\n";
    ss << stop_hint << "\n";
    return ss.str();
}

inline std::string format_http_info(const std::string& tool,
                                    int port,
                                    const std::string& stop_hint = "Press Ctrl+C to stop and return to REPL.") {
    return format_http_info(tool, port, "127.0.0.1", stop_hint);
}

/**
 * Format HTTP server status.
 */
inline std::string format_http_status(int port, bool running, const std::string& bind_addr) {
    std::ostringstream ss;
    const std::string rendered_host = format_url_host(bind_addr);
    if (running) {
        ss << "HTTP server running on port " << port << "\n";
        ss << "URL: http://" << rendered_host << ":" << port << "\n";
    } else {
        ss << "HTTP server not running\n";
        ss << "Use '.http start' to start\n";
    }
    return ss.str();
}

inline std::string format_http_status(int port, bool running) {
    return format_http_status(port, running, "127.0.0.1");
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
    int queue_admission_timeout_ms = 60000;
    size_t max_queue = 0;
    using queue_timeout_fn_t = std::function<int()>;
    queue_timeout_fn_t queue_admission_timeout_ms_fn;
    using max_queue_fn_t = std::function<size_t()>;
    max_queue_fn_t max_queue_fn;
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

inline std::string format_http_info(const std::string&, int, const std::string&, const std::string& = "") { return ""; }
inline std::string format_http_info(const std::string&, int, const std::string& = "") { return ""; }
inline std::string format_http_status(int, bool, const std::string&) { return ""; }
inline std::string format_http_status(int, bool) { return ""; }

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
