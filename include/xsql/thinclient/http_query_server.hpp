// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

/**
 * @file http_query_server.hpp
 * @brief Consolidated HTTP query server for all *sql tools
 *
 * Provides the standard 6 endpoints (/, /help, /query, /status, /cancel, /shutdown)
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
#include <xsql/thinclient/auth_compare.hpp>
#include <xsql/thinclient/clipboard.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace xsql::thinclient {

// ============================================================================
// Configuration
// ============================================================================

struct http_query_server_config {
    /// Tool name shown in responses
    std::string tool_name;

    /// Help text returned by GET /help
    std::string help_text;

    /// Port to listen on (0 = random free port in 8100-8999, OS-assigned
    /// fallback)
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
    /// owns multi-statement orchestration (xsql::run_script), option parsing
    /// (continue_on_error / include_sql, from query string OR a JSON request
    /// body), and output formatting (json/text/csv/tsv) directly from the
    /// ScriptResult — no JSON round-trip.
    /// Takes precedence over query_fn. The executor runs one statement and fills
    /// `out`; it is invoked on the worker thread (use_queue=false) or the main
    /// thread via the queue (use_queue=true), same threading contract as
    /// query_fn.
    using statement_executor_t =
        std::function<void(const std::string& sql, xsql::ScriptStatementResult& out)>;
    statement_executor_t statement_executor;

    /// Optional: a whole-script executor. When set, the server parses the
    /// options (continue_on_error / include_sql, from query string OR a JSON
    /// request body) and delegates the ENTIRE script to this callback, which
    /// owns multi-statement orchestration
    /// itself. Use this when the engine must wrap the whole run (e.g. batch /
    /// cache-refresh semantics that guarantee a later statement never reads
    /// stale data after an earlier mutation) rather than executing statements
    /// independently. The returned ScriptResult is formatted by the server
    /// (json/text/csv/tsv). Takes precedence over statement_executor and
    /// query_fn. With use_queue=true the whole executor runs on the thread that
    /// calls process_one_command()/run_until_stopped(), preserving engine thread
    /// affinity. Otherwise thread safety is the caller's responsibility.
    using script_executor_t =
        std::function<xsql::ScriptResult(const std::string& script,
                                         const xsql::ScriptOptions& opts)>;
    script_executor_t script_executor;

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

    /// Optional dynamic queue timeout callback (overrides
    /// queue_admission_timeout_ms).
    using queue_timeout_fn_t = std::function<int()>;
    queue_timeout_fn_t queue_admission_timeout_ms_fn;

    /// Optional dynamic max queue callback (overrides max_queue).
    using max_queue_fn_t = std::function<size_t()>;
    max_queue_fn_t max_queue_fn;

    /// If true, queries are queued for main-thread execution via
    /// run_until_stopped() / process_one_command(). Required for tools
    /// with thread-affinity constraints.
    bool use_queue = false;

    /// If true (and use_queue is false), serialize /query execution under an
    /// internal mutex so concurrent requests run one-at-a-time. For executors
    /// that are not concurrency-safe (e.g. a per-engine DB handle) but run on
    /// the HTTP worker rather than a main-thread queue (REPL/background/plugin
    /// servers). Locks per request (around the whole script), preserving
    /// multi-statement atomicity. Ignored when use_queue is true.
    bool serialize_requests = false;

    /// If true, GET /status is bearer-guarded like /query (401 without a token
    /// when auth_token is set). Secure by default; set false only when /status
    /// is intentionally a public liveness probe that exposes no sensitive
    /// readiness data. Appended to preserve positional aggregate initializers.
    bool status_requires_auth = true;

    /// Maximum accepted request-body size in bytes (0 = unlimited). httplib
    /// fully buffers the body in memory before the handler runs, so an
    /// unbounded default would let one oversized POST exhaust the host
    /// process; 64 MiB comfortably covers real SQL scripts. Appended last to
    /// preserve positional aggregate initializers.
    std::size_t max_request_body_bytes = 64ull * 1024 * 1024;
};

// ============================================================================
// http_query_server
// ============================================================================

class http_query_server {
public:
    explicit http_query_server(const http_query_server_config& config)
        : config_(config) {}

    ~http_query_server() {
        stop();
        // Block until any detached POST /shutdown thread has finished touching
        // our members (it decrements the latch after its own stop() returns).
        //
        // Deliberate trade-off: this wait is intentionally UNBOUNDED (no timeout).
        // The latch only clears once the detached stop() returns, and stop() can
        // only return after the server thread — and thus any in-flight query —
        // has drained. If a query_fn/executor wedges (an engine deadlock, an
        // infinite loop in a user callback), the destructor hangs here forever.
        // That is preferred over a timed wait: a timeout would let the destructor
        // proceed and free members the detached thread is still reading, i.e. a
        // use-after-free. We choose a diagnosable hang over silent memory
        // corruption. If you hit this hang, the bug is a non-terminating
        // query_fn/executor, not this latch.
        std::unique_lock<std::mutex> lk(shutdown_latch_mu_);
        shutdown_latch_cv_.wait(lk, [this] { return shutdown_threads_inflight_ == 0; });
    }

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

        // A fixed (non-zero) port binds exactly that port with the fixed-port
        // socket profile: POSIX SO_REUSEADDR (fast restart out of our own
        // TIME_WAIT socket, but EADDRINUSE against a LIVE listener) and Windows
        // SO_EXCLUSIVEADDRUSE. Never httplib's default, which sets SO_REUSEPORT
        // on POSIX — that lets a second live process bind the same port and the
        // kernel load-balance connections between them, the exact cross-routing
        // failure the ephemeral search below exists to prevent.
        if (config_.port != 0) {
            const int bound = try_serve(config_.port, /*exclusive=*/false);
            return bound > 0 ? bound : -1;
        }

        // Ephemeral (port == 0): search the documented 8100-8999 range for a
        // port we can bind *exclusively*. Exclusive binding is what makes the
        // search correct under concurrency: if a port is already held by a live
        // server the bind fails and we move on, so two servers can never share a
        // port. A shared port cross-routes clients to the wrong server (the root
        // cause of flaky HTTP tests, where a client received another server's
        // response). The wide 900-slot band keeps a predictable high range clear of
        // the tools' fixed defaults (8080/8081/17198-17200) while making collisions
        // vanishingly rare. Shuffle so concurrent servers spread across the range;
        // if the whole range is busy, fall back to an OS-assigned free port.
        std::array<int, 900> range;
        for (int i = 0; i < 900; ++i) range[i] = 8100 + i;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(range.begin(), range.end(), gen);
        // Bound the search: a plain bind failure is cheap, but a port that
        // BINDS and then never reports a running accept loop costs ~500 ms of
        // polling plus a teardown each — that is an environment problem
        // (firewall sandboxing, exhausted handles) which 900 candidates cannot
        // fix, and unbounded it turns start() into a multi-minute hang. Give
        // up after a few such stalls or a total wall-clock budget.
        const auto search_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        int stalled_binds = 0;
        for (int candidate : range) {
            if (std::chrono::steady_clock::now() >= search_deadline) return -1;
            const int bound = try_serve(candidate, /*exclusive=*/true);
            if (bound > 0) return bound;
            if (bound == kBoundButNotServing && ++stalled_binds >= 3) return -1;
        }
        // Range exhausted (heavy concurrency): let the OS assign any free port.
        const int bound = try_serve(0, /*exclusive=*/true);
        return bound > 0 ? bound : -1;
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

    /** Stop the server gracefully. Safe to call concurrently / repeatedly:
     *  the POST /shutdown handler stops from a detached thread while a
     *  run_until_stopped() owner and the destructor may also call stop(). The
     *  teardown (svr_->stop / join / reset) is serialized so the worker thread
     *  is joined exactly once (a bare joinable()+join() race aborts with
     *  "thread::join failed"). */
    void stop() {
        running_.store(false);
        queue_cv_.notify_all();
        drain_pending_commands("HTTP server stopped");

        std::lock_guard<std::mutex> teardown_lock(stop_mutex_);
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
    // Server binding
    // ========================================================================

    // A bind that succeeded but whose accept loop never reported running —
    // distinct from a plain bind failure (-1) so start()'s ephemeral search can
    // stop burning ~500 ms per candidate on an environment that will never
    // serve. Both map to -1 at the public start() boundary.
    static constexpr int kBoundButNotServing = -2;

    // Bind + start serving on `port` (0 = OS-assigned). Returns the bound port,
    // -1 if the bind failed (port busy) so the caller can try another, or
    // kBoundButNotServing (see above). Binds synchronously (bind_to_port /
    // bind_to_any_port) before spawning the accept thread, so a busy port is
    // detected immediately instead of after a poll.
    //
    // Socket profiles — NEITHER uses httplib's default_socket_options, whose
    // POSIX SO_REUSEPORT would let a second live process bind the same port
    // with the kernel load-balancing connections between them (cross-routing):
    //   exclusive=true  (ephemeral probe): Windows SO_EXCLUSIVEADDRUSE; POSIX
    //     no reuse flags at all, so a live OR TIME_WAIT port fails EADDRINUSE
    //     and the search moves on.
    //   exclusive=false (fixed port): Windows SO_EXCLUSIVEADDRUSE; POSIX
    //     SO_REUSEADDR only — fast restart out of our own TIME_WAIT socket,
    //     while a bind against a live listener still fails.
    // A generic lambda deduces the socket handle type across httplib versions.
    int try_serve(int port, bool exclusive) {
        auto svr = std::make_unique<httplib::Server>();
        svr->set_socket_options([exclusive](auto sock) {
#ifdef _WIN32
            (void)exclusive;
            int yes = 1;
            setsockopt(sock, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
                       reinterpret_cast<const char*>(&yes), sizeof(yes));
#else
            if (!exclusive) {
                int yes = 1;
                setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
                           reinterpret_cast<const char*>(&yes), sizeof(yes));
            }
#endif
        });
        if (config_.max_request_body_bytes > 0) {
            svr->set_payload_max_length(config_.max_request_body_bytes);
        }

        int bound = port;
        if (port == 0) {
            bound = svr->bind_to_any_port(config_.bind_address.c_str());
            if (bound <= 0) return -1;
        } else if (!svr->bind_to_port(config_.bind_address.c_str(), port)) {
            return -1;  // port already taken -- caller tries the next one
        }

        setup_routes(*svr, bound);
        if (config_.extra_routes) config_.extra_routes(*svr);

        svr_ = std::move(svr);
        port_ = bound;
        server_thread_ = std::thread([this]() { svr_->listen_after_bind(); });
        return await_running();
    }

    // Wait for the accept loop to come up after a successful bind. The bind
    // already succeeded, so this normally returns immediately; the teardown path
    // guards the rare case where listen_after_bind never reports running (never
    // detach the worker -- a detached thread racing svr_.reset() is a
    // use-after-free -- and clear the endpoint so status/port() do not report a
    // port we never served).
    int await_running() {
        int attempts = 0;
        while (!svr_->is_running() && attempts < 50) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            attempts++;
        }
        if (!svr_->is_running()) {
            svr_->stop();
            if (server_thread_.joinable()) server_thread_.join();
            svr_.reset();
            port_ = 0;
            return kBoundButNotServing;
        }
        running_.store(true);
        return port_;
    }

    // ========================================================================
    // Internal command queue (for use_queue mode)
    // ========================================================================

    enum class command_state {
        queued,
        started,
        completed,
        cancelled,
    };

    struct pending_command {
        std::string sql;
        std::string result;              // legacy query_fn path: JSON string
        bool use_executor = false;       // executor path: fill script_result
        xsql::ScriptOptions opts;
        xsql::ScriptResult script_result;
        // Late-bound /cancel snapshot: opts.should_cancel compares the live
        // epoch against this cell. The worker re-stores the current epoch the
        // moment execution starts, so a /cancel that arrives while the command
        // is still QUEUED does not abort it — only actually-running queries
        // observe a cancel (matching the libxsql-py per-request model).
        std::shared_ptr<std::atomic<std::uint64_t>> cancel_epoch_cell;
        int http_status = 200;
        command_state state = command_state::queued;
        std::mutex done_mutex;
        std::condition_variable done_cv;
    };

    struct queued_string_result {
        int http_status = 200;
        std::string body;
    };

    struct queued_script_result {
        int http_status = 200;
        xsql::ScriptResult script;
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
        } else if (format == "jsonl") {
            res.set_content(xsql::script_result_to_jsonl(sr),
                            "application/x-ndjson");
        } else {
            res.set_content(xsql::script_result_to_json(sr, include_sql), "application/json");
        }
    }

    enum class admit_result { ok, not_running, queue_full, timeout };

    // Admit a command to the queue and wait for the main thread to run it.
    // Shared by the legacy string path and the executor path.
    admit_result admit_and_wait(const std::shared_ptr<pending_command>& cmd) {
        int queue_timeout_ms = config_.queue_admission_timeout_ms;
        if (config_.queue_admission_timeout_ms_fn) {
            queue_timeout_ms = config_.queue_admission_timeout_ms_fn();
        }
        if (queue_timeout_ms < 0) {
            queue_timeout_ms = 0;
        }

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            // Synchronize the running check with stop()'s queue drain. Without
            // this check under queue_mutex_, stop could drain the queue between
            // an earlier check and this push, leaving an infinite-timeout
            // waiter stranded forever.
            if (!running_.load()) {
                return admit_result::not_running;
            }
            size_t max_queue = config_.max_queue;
            if (config_.max_queue_fn) {
                max_queue = config_.max_queue_fn();
            }
            // max_queue bounds outstanding requests = those waiting in the queue
            // PLUS commands currently in-flight on processing threads.
            // The worker pops a command before running it, so an in-flight command
            // is no longer in pending_commands_; counting processing keeps this
            // path's effective ceiling identical to the serialize path (which
            // counts its lock holder via serialize_pending_). See POST /query.
            const size_t outstanding =
                pending_commands_.size() + processing_count_;
            if (max_queue > 0 && outstanding >= max_queue) {
                return admit_result::queue_full;
            }
            pending_commands_.push_back(cmd);
        }
        queue_cv_.notify_one();

        // Wait for main thread to execute.
        {
            std::unique_lock<std::mutex> lock(cmd->done_mutex);
            if (queue_timeout_ms == 0) {
                while (cmd->state != command_state::completed &&
                       cmd->state != command_state::cancelled) {
                    cmd->done_cv.wait_for(lock, std::chrono::milliseconds(100));
                }
            } else {
                const auto deadline =
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(queue_timeout_ms);
                if (!cmd->done_cv.wait_until(lock, deadline,
                                              [&]() {
                                                  return cmd->state != command_state::queued;
                                              })) {
                    // Admission timed out before execution began (the predicate
                    // held false under the lock, so the command is still
                    // queued). Mark it cancelled so a worker that races the
                    // erase below skips it, and REMOVE it from the queue so the
                    // corpse stops occupying a max_queue slot — the serialize
                    // path releases its slot immediately via PendingGuard, and
                    // this keeps the two ceilings genuinely equivalent.
                    cmd->state = command_state::cancelled;
                    lock.unlock();
                    {
                        std::lock_guard<std::mutex> qlock(queue_mutex_);
                        auto it = std::find(pending_commands_.begin(),
                                            pending_commands_.end(), cmd);
                        if (it != pending_commands_.end()) {
                            pending_commands_.erase(it);
                        }
                    }
                    return admit_result::timeout;
                }

                // queue_admission_timeout_ms bounds only time spent waiting for
                // the main thread. Once execution starts, returning a timeout
                // would be dishonest: the mutation could continue after the
                // client was told it failed. Wait for the real result instead.
                while (cmd->state == command_state::started) {
                    cmd->done_cv.wait(lock);
                }
            }
        }
        return admit_result::ok;
    }

    queued_string_result queue_and_wait(const std::string& sql) {
        auto cmd = std::make_shared<pending_command>();
        cmd->sql = sql;
        switch (admit_and_wait(cmd)) {
            case admit_result::not_running:
                return {
                    503,
                    xsql::json{
                        {"success", false}, {"error", "Server not running"}}.dump()};
            case admit_result::queue_full:
                return {
                    503,
                    xsql::json{
                        {"success", false}, {"error", "Queue full"},
                        {"hint", "Reduce concurrency or increase max_queue"}}.dump()};
            case admit_result::timeout:
                return {
                    408,
                    xsql::json{
                        {"success", false},
                        {"error", "Request timed out while waiting in queue"},
                        {"hint", "Reduce concurrency or increase "
                                 "queue_admission_timeout_ms"}}.dump()};
            case admit_result::ok:
                break;
        }
        return {cmd->http_status, std::move(cmd->result)};
    }

    queued_script_result queue_and_wait_script(
        const std::string& sql, const xsql::ScriptOptions& opts,
        std::shared_ptr<std::atomic<std::uint64_t>> cancel_epoch_cell) {
        auto cmd = std::make_shared<pending_command>();
        cmd->sql = sql;
        cmd->use_executor = true;
        cmd->opts = opts;
        cmd->cancel_epoch_cell = std::move(cancel_epoch_cell);
        switch (admit_and_wait(cmd)) {
            case admit_result::not_running:
                return {
                    503, make_error_script_result("Server not running")};
            case admit_result::queue_full:
                return {503, make_error_script_result("Queue full")};
            case admit_result::timeout:
                return {
                    408,
                    make_error_script_result(
                        "Request timed out while waiting in queue")};
            case admit_result::ok:
                break;
        }
        return {cmd->http_status, std::move(cmd->script_result)};
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
                pending_commands_.pop_front();
                // Count this command as in-flight so admit_and_wait's max_queue
                // ceiling stays exact across the pop→run→complete window.
                ++processing_count_;
            }
        }

        if (!cmd) return false;

        // Decrement the processing count on every exit path after a pop.
        struct ProcessingGuard {
            std::mutex& m;
            std::size_t& count;
            ~ProcessingGuard() {
                std::lock_guard<std::mutex> lock(m);
                if (count != 0) {
                    --count;
                }
            }
        } processing_guard{queue_mutex_, processing_count_};

        // Skip a command whose waiter already timed out / was drained: the client
        // is gone, so running it (especially an engine mutation) would be wrong.
        // Otherwise mark it started while holding the same mutex used by the
        // admission waiter, making the timeout-vs-start race deterministic.
        {
            std::lock_guard<std::mutex> lock(cmd->done_mutex);
            if (cmd->state == command_state::cancelled ||
                cmd->state == command_state::completed) {
                return true;  // popped and discarded; nothing to execute
            }
            // Arm the /cancel snapshot at the moment execution starts, then
            // flip the state under the same lock: only cancels issued AFTER
            // this point abort the command. A /cancel that raced in while the
            // command sat in the queue is deliberately not honored — it targets
            // the query that was in flight at the time, not this one.
            if (cmd->cancel_epoch_cell) {
                cmd->cancel_epoch_cell->store(cancel_epoch_.load());
            }
            cmd->state = command_state::started;
        }
        cmd->done_cv.notify_one();

        try {
            if (cmd->use_executor && config_.script_executor) {
                cmd->script_result = config_.script_executor(cmd->sql, cmd->opts);
            } else if (cmd->use_executor && config_.statement_executor) {
                cmd->script_result =
                    xsql::run_script(cmd->sql, cmd->opts, config_.statement_executor);
            } else if (cmd->use_executor) {
                cmd->script_result = make_error_script_result("No query handler");
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
        } catch (...) {
            if (cmd->use_executor) {
                cmd->script_result =
                    make_error_script_result("Unhandled query exception");
            } else {
                cmd->result = xsql::json{
                    {"success", false},
                    {"error", "Unhandled query exception"}}.dump();
            }
        }

        {
            std::lock_guard<std::mutex> lock(cmd->done_mutex);
            cmd->state = command_state::completed;
        }
        cmd->done_cv.notify_one();
        return true;
    }

    // Fail every queued command on shutdown. Each waiter reads back a different
    // field depending on its path: the legacy query_fn path returns cmd->result
    // (a JSON string), while the executor path returns cmd->script_result. Filling
    // only cmd->result left executor waiters with a default-constructed, message-
    // less ScriptResult (silent failure on stop). Branch on use_executor so both
    // paths surface the shutdown reason.
    void drain_pending_commands(const std::string& message) {
        std::deque<std::shared_ptr<pending_command>> pending;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            std::swap(pending, pending_commands_);
        }

        const std::string legacy_json =
            xsql::json{{"success", false}, {"error", message}}.dump();

        while (!pending.empty()) {
            auto cmd = pending.front();
            pending.pop_front();
            if (!cmd) continue;

            {
                std::lock_guard<std::mutex> lock(cmd->done_mutex);
                if (cmd->state != command_state::completed &&
                    cmd->state != command_state::cancelled) {
                    if (cmd->use_executor) {
                        cmd->script_result = make_error_script_result(message);
                    } else {
                        cmd->result = legacy_json;
                    }
                    cmd->http_status = 503;
                    cmd->state = command_state::completed;
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

        if (detail::timing_safe_equal(token, config_.auth_token)) return true;

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
        // /cancel exists as a route for every config but hard-409s unless the
        // whole-script executor is wired; only advertise what actually works.
        const bool cancel_supported = static_cast<bool>(config_.script_executor);
        svr.Get("/", [tool, port, cancel_supported](const httplib::Request&,
                                                    httplib::Response& res) {
            // Uppercase tool name for display (e.g. "pdbsql" -> "PDBSQL HTTP Server")
            std::string display = tool;
            for (auto& c : display) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            std::string welcome = display + " HTTP Server\n\nEndpoints:\n"
                "  GET  /help     - API documentation\n"
                "  POST /query    - Execute SQL query\n";
            if (cancel_supported) {
                welcome += "  POST /cancel   - Cancel the in-flight query\n";
            }
            welcome +=
                "  GET  /status   - Health check\n"
                "  POST /shutdown - Stop server\n\n"
                "Example: curl -X POST http://localhost:" + std::to_string(port) +
                                  "/query -d \"SELECT name FROM sqlite_master WHERE "
                                  "type='table' LIMIT 10\"\n";
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
                // Serialize concurrent requests for non-queue executors that
                // aren't concurrency-safe (REPL/background/plugin servers).
                // Honor the SAME effective admission ceiling as the queue path:
                // max_queue bounds outstanding requests = those waiting PLUS the
                // one in-flight. serialize_pending_ counts requests waiting for OR
                // holding the serialize lock (the holder is the in-flight one), so
                // `now_pending > max_q` rejects exactly when the queue path's
                // `pending + processing >= max_queue` does. Limits: max_queue ->
                // 503, queue_admission_timeout_ms -> 408.
                std::unique_lock<std::timed_mutex> serialize_lock(serialize_mutex_, std::defer_lock);
                struct PendingGuard {
                    std::atomic<size_t>* counter;
                    bool engaged = false;
                    ~PendingGuard() {
                        if (engaged) counter->fetch_sub(1, std::memory_order_acq_rel);
                    }
                } pending_guard{&serialize_pending_};
                if (config_.serialize_requests && !config_.use_queue) {
                    const size_t max_q = config_.max_queue_fn ? config_.max_queue_fn()
                                                              : config_.max_queue;
                    const size_t now_pending =
                        serialize_pending_.fetch_add(1, std::memory_order_acq_rel) + 1;
                    pending_guard.engaged = true;
                    if (max_q > 0 && now_pending > max_q) {
                        res.status = 503;
                        res.set_content(xsql::json{
                            {"success", false}, {"error", "Queue full"},
                            {"hint", "Reduce concurrency or increase max_queue"}}.dump(),
                            "application/json");
                        return;
                    }
                    int timeout_ms = config_.queue_admission_timeout_ms_fn
                        ? config_.queue_admission_timeout_ms_fn()
                        : config_.queue_admission_timeout_ms;
                    if (timeout_ms < 0) timeout_ms = 0;
                    if (timeout_ms == 0) {
                        serialize_lock.lock();
                    } else if (!serialize_lock.try_lock_for(
                                   std::chrono::milliseconds(timeout_ms))) {
                        res.status = 408;
                        res.set_content(xsql::json{
                            {"success", false},
                            {"error", "Request timed out while waiting for serialization"},
                            {"hint", "Reduce concurrency or increase "
                                         "queue_admission_timeout_ms"}}.dump(),
                            "application/json");
                        return;
                    }
                }

                // Output format (default json). text/csv/tsv are for direct
                // terminal/pipe use; json stays the canonical format. An
                // unrecognized value is a 400 (do not silently fall through to
                // json), matching the strict JSON-body handling below.
                std::string format = "json";
                auto fmt_it = req.params.find("format");
                if (fmt_it != req.params.end() && !fmt_it->second.empty()) {
                    format = fmt_it->second;
                    if (format != "json" && format != "jsonl" &&
                        format != "text" && format != "csv" &&
                        format != "tsv") {
                        res.status = 400;
                        res.set_content(
                            xsql::json{{"success", false},
                                       {"error", "unrecognized ?format '" + format +
                                            "' (expected json, jsonl, text, csv, or tsv)"}}.dump(),
                            "application/json");
                        return;
                    }
                }

                // Resolve the SQL text + script options. Two request shapes are
                // accepted:
                //   1. Raw-SQL body (default): the body IS the SQL;
                //      continue_on_error / include_sql come from query params.
                //   2. JSON body: {"sql": "...", "continue_on_error": bool,
                //      "include_sql": bool}.
                // When Content-Type is application/json the body MUST be a JSON
                // object carrying a string "sql" -- a malformed or sql-less body
                // is a 400, NOT silently run as raw SQL. Without that content
                // type, a leading '{' is only a lenient hint: parse it if it is
                // valid JSON-with-sql, otherwise treat the body as raw SQL. Query
                // params still apply and either source enabling a flag wins.
                std::string sql_text = req.body;
                bool json_continue_on_error = false;
                bool json_include_sql = false;
                {
                    const std::string ctype = req.get_header_value("Content-Type");
                    const bool json_declared =
                        ctype.find("application/json") != std::string::npos;
                    const bool looks_json =
                        json_declared || (!req.body.empty() && req.body.front() == '{');
                    if (looks_json) {
                        xsql::json body =
                            xsql::json::parse(req.body, nullptr, /*allow_exceptions=*/false);
                        const bool has_sql = body.is_object() && body.contains("sql") &&
                                             body["sql"].is_string();
                        if (has_sql) {
                            sql_text = body["sql"].get<std::string>();
                            auto read_flag = [&body](const char* key) -> bool {
                                if (!body.contains(key)) return false;
                                const auto& v = body[key];
                                if (v.is_boolean()) return v.get<bool>();
                                if (v.is_number()) return v.get<double>() != 0;
                                if (v.is_string()) {
                                    const auto s = v.get<std::string>();
                                    return s == "1" || s == "true";
                                }
                                return false;
                            };
                            json_continue_on_error = read_flag("continue_on_error");
                            json_include_sql = read_flag("include_sql");
                        } else if (json_declared) {
                            // Declared application/json but not a usable
                            // {"sql": "..."} object -> reject rather than guess.
                            res.status = 400;
                            res.set_content(
                                xsql::json{{"success", false},
                                           {"error", body.is_discarded()
                                                ? "malformed JSON request body"
                                                : "JSON request body must be an "
                                                  "object with a string \"sql\""}}.dump(),
                                "application/json");
                            return;
                        }
                        // else: undeclared leading-'{' that isn't valid
                        // JSON-with-sql -> fall through and treat body as raw SQL.
                    }
                }
                if (sql_text.empty()) {
                    res.status = 400;
                    res.set_content(
                        xsql::json{{"success", false}, {"error", "Empty query"}}.dump(),
                        "application/json");
                    return;
                }

                // Merge options from query params and the JSON body; either source
                // setting a flag true enables it. Returns the /cancel snapshot
                // cell so the caller can re-arm it when execution actually starts.
                auto parse_opts = [&](xsql::ScriptOptions& opts) {
                    auto cont_it = req.params.find("continue_on_error");
                    if (cont_it != req.params.end() && cont_it->second == "1") {
                        opts.continue_on_error = true;
                    }
                    auto incl_it = req.params.find("include_sql");
                    if (incl_it != req.params.end() && incl_it->second == "1") {
                        opts.include_sql = true;
                    }
                    if (json_continue_on_error) opts.continue_on_error = true;
                    if (json_include_sql) opts.include_sql = true;
                    // Cooperative cancellation via POST /cancel. The predicate
                    // compares the live epoch against a per-request cell that is
                    // re-armed at EXECUTION start (worker pop / just before the
                    // direct call), so a /cancel only ever aborts queries that
                    // were actually running when it arrived — queued-but-unstarted
                    // work is not collateral. Unlike a shared bool that each new
                    // request resets, a newly arrived request also cannot
                    // un-cancel the query already in flight.
                    auto cell = std::make_shared<std::atomic<std::uint64_t>>(
                        cancel_epoch_.load());
                    opts.should_cancel = [this, cell]() {
                        return cancel_epoch_.load() != cell->load();
                    };
                    return cell;
                };

                if (config_.script_executor) {
                    // Engine-owned orchestration: the server parses options and
                    // formatting but hands the whole script to the executor so it
                    // can wrap the run (batch/refresh semantics). In use_queue mode
                    // the executor runs on the queue-processing thread.
                    xsql::ScriptOptions opts;
                    auto cancel_cell = parse_opts(opts);
                    xsql::ScriptResult script;
                    if (config_.use_queue) {
                        auto queued =
                            queue_and_wait_script(sql_text, opts, cancel_cell);
                        res.status = queued.http_status;
                        script = std::move(queued.script);
                    } else {
                        cancel_cell->store(cancel_epoch_.load());
                        script = config_.script_executor(sql_text, opts);
                    }
                    set_formatted(res, script, format, opts.include_sql);
                } else if (config_.statement_executor) {
                    // Preferred path: the server owns option parsing + run_script
                    // + formatting, straight from the ScriptResult (no round-trip).
                    xsql::ScriptOptions opts;
                    auto cancel_cell = parse_opts(opts);
                    xsql::ScriptResult script;
                    if (config_.use_queue) {
                        auto queued =
                            queue_and_wait_script(sql_text, opts, cancel_cell);
                        res.status = queued.http_status;
                        script = std::move(queued.script);
                    } else {
                        cancel_cell->store(cancel_epoch_.load());
                        script = xsql::run_script(
                            sql_text, opts, config_.statement_executor);
                    }
                    set_formatted(res, script, format, opts.include_sql);
                } else if (config_.query_fn) {
                    // Legacy path: callback returns a JSON string. Re-parse only
                    // for non-json formats (continue_on_error/include_sql are not
                    // available here — the callback owns its own ScriptOptions).
                    std::string result;
                    if (config_.use_queue) {
                        auto queued = queue_and_wait(sql_text);
                        res.status = queued.http_status;
                        result = std::move(queued.body);
                    } else {
                        result = config_.query_fn(sql_text);
                    }
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

        // GET /status - Server status. It shares the configured authentication
        // boundary by default. A tool may explicitly opt out when this route is
        // a deliberately public liveness probe that exposes no sensitive data.
        svr.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
            if (config_.status_requires_auth && !check_auth(req, res)) return;
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
            {
                std::lock_guard<std::mutex> lk(shutdown_latch_mu_);
                ++shutdown_threads_inflight_;
            }
            try {
                std::thread([this] {
                    // The decrement+notify must run on EVERY exit path of this
                    // detached body: an exception escaping a detached thread is
                    // std::terminate, and a skipped decrement leaves the
                    // destructor's deliberately-unbounded latch wait hanging
                    // forever. The guard releases only after stop() has fully
                    // returned (or thrown), so the destructor can never race
                    // ahead and free members this thread still touches.
                    struct LatchRelease {
                        http_query_server* self;
                        ~LatchRelease() {
                            std::lock_guard<std::mutex> lk(
                                self->shutdown_latch_mu_);
                            --self->shutdown_threads_inflight_;
                            self->shutdown_latch_cv_.notify_all();
                        }
                    } release{this};
                    try {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(100));
                        stop();
                    } catch (...) {
                    }
                }).detach();
            } catch (...) {
                // The std::thread constructor threw (e.g. resource exhaustion),
                // so the detached lambda that owes the decrement will never run.
                // Undo the increment ourselves and wake the destructor's
                // unbounded wait — otherwise it hangs forever on a thread that
                // was never started.
                std::lock_guard<std::mutex> lk(shutdown_latch_mu_);
                --shutdown_threads_inflight_;
                shutdown_latch_cv_.notify_all();
            }
        });

        // POST /cancel - Cooperatively cancel the in-flight query (guarded like
        // /query and /shutdown). Advances the server-wide epoch; every query
        // whose EXECUTION started before this request observes the change
        // through ScriptOptions::should_cancel (each request re-arms its epoch
        // snapshot at execution start, so queued-but-unstarted work is never
        // collateral) and keeps any partial rows already gathered.
        svr.Post("/cancel", [this](const httplib::Request& req, httplib::Response& res) {
            if (!check_auth(req, res)) return;
            // Only the whole-script executor receives ScriptOptions and can
            // propagate should_cancel into an in-flight statement. The legacy
            // query callback and statement executor receive SQL alone; they can
            // observe cancellation only between statements, so acknowledging
            // an in-flight cancellation request would be dishonest.
            if (!config_.script_executor) {
                res.status = 409;
                res.set_content(
                    xsql::json{
                        {"success", false},
                        {"error", "cancellation is not supported by "
                                                     "the configured query callback"}}
                        .dump(),
                    "application/json");
                return;
            }
            cancel_epoch_.fetch_add(1);
            res.set_content(
                xsql::json{{"success", true}, {"message", "cancel requested"}}.dump(),
                "application/json");
        });
    }

    // ========================================================================
    // State
    // ========================================================================

    http_query_server_config config_;
    std::unique_ptr<httplib::Server> svr_;
    std::thread server_thread_;
    std::mutex stop_mutex_;            // serializes stop() teardown across threads
    // Latch for detached POST /shutdown threads: the handler spawns a detached
    // thread that sleeps then calls stop(), touching our members. The destructor
    // waits on this latch so it never frees those members out from under a still-
    // running shutdown thread.
    std::mutex shutdown_latch_mu_;
    std::condition_variable shutdown_latch_cv_;
    std::size_t shutdown_threads_inflight_{0};
    std::timed_mutex serialize_mutex_; // serializes /query when serialize_requests
    std::atomic<size_t> serialize_pending_{0};  // in-flight+waiting serialize requests
    std::atomic<bool> running_{false};
    // Server-wide cooperative-cancel epoch. Each /query holds a per-request
    // snapshot cell that is re-armed when its execution starts; POST /cancel
    // increments the epoch, so exactly the queries executing at that moment
    // observe it — queued work starts clean, and later requests cannot clear
    // cancellation for the query already in flight. Whole-script executors that
    // run through run_database_script receive the predicate and can cancel
    // mid-statement.
    std::atomic<std::uint64_t> cancel_epoch_{0};
    int port_{0};

    std::function<bool()> interrupt_check_;

    // Command queue (for use_queue mode). A deque (not std::queue) so an
    // admission-timed-out command can be erased from the middle: its slot must
    // be released immediately, not held until a worker pops the corpse.
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<std::shared_ptr<pending_command>> pending_commands_;
    std::size_t processing_count_ = 0;
};

// ============================================================================
// Format helpers
// ============================================================================

/**
 * Format HTTP server info for display. Pass cancel_supported=false when the
 * server has no whole-script executor (POST /cancel would 409 there, so it
 * should not be advertised) — the default keeps existing callers' output.
 */
inline std::string format_http_info(const std::string& tool,
                                    int port,
                                    const std::string& bind_addr,
                                    const std::string& stop_hint = "Press Ctrl+C to stop and return to REPL.",
                                    bool cancel_supported = true) {
    std::ostringstream ss;
    const std::string rendered_host = format_url_host(bind_addr);
    ss << "HTTP server started on port " << port << "\n";
    ss << "URL: http://" << rendered_host << ":" << port << "\n\n";
    ss << "Endpoints:\n";
    ss << "  GET  /help     - API documentation\n";
    ss << "  POST /query    - Execute SQL query\n";
    if (cancel_supported) {
        ss << "  POST /cancel   - Cancel the in-flight query\n";
    }
    ss << "  GET  /status   - Health check\n";
    ss << "  POST /shutdown - Stop server\n\n";
    ss << "Example:\n";
    ss << "  curl -X POST http://" << rendered_host << ":" << port
       << "/query -d \"SELECT name FROM sqlite_master WHERE type='table' LIMIT "
          "10\"\n\n";
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

#include <xsql/json.hpp>
#include <xsql/query_script.hpp>

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>

// The stub must stay field-for-field and method-for-method in sync with the
// real class above so a consumer compiles identically in both builds; the
// httplib-typed members use this forward declaration (never dereferenced in a
// no-thinclient build).
namespace httplib {
class Server;
}  // namespace httplib

namespace xsql::thinclient {

struct http_query_server_config {
    std::string tool_name;
    std::string help_text;
    int port = 0;
    std::string bind_address = "127.0.0.1";
    std::string auth_token;
    using query_fn_t = std::function<std::string(const std::string& sql)>;
    query_fn_t query_fn;
    using statement_executor_t =
        std::function<void(const std::string& sql,
                           xsql::ScriptStatementResult& out)>;
    statement_executor_t statement_executor;
    using script_executor_t =
        std::function<xsql::ScriptResult(const std::string& script,
                                         const xsql::ScriptOptions& opts)>;
    script_executor_t script_executor;
    using status_fn_t = std::function<xsql::json()>;
    status_fn_t status_fn;
    using extra_routes_fn_t = std::function<void(httplib::Server& svr)>;
    extra_routes_fn_t extra_routes;
    int queue_admission_timeout_ms = 60000;
    size_t max_queue = 0;
    using queue_timeout_fn_t = std::function<int()>;
    queue_timeout_fn_t queue_admission_timeout_ms_fn;
    using max_queue_fn_t = std::function<size_t()>;
    max_queue_fn_t max_queue_fn;
    bool use_queue = false;
    bool serialize_requests = false;
    bool status_requires_auth = true;
    std::size_t max_request_body_bytes = 64ull * 1024 * 1024;
};

class http_query_server {
public:
    explicit http_query_server(const http_query_server_config&) {
        throw std::runtime_error("Thin client not enabled. Build with XSQL_WITH_THINCLIENT=ON");
    }
    http_query_server(const http_query_server&) = delete;
    http_query_server& operator=(const http_query_server&) = delete;
    int start() { return -1; }
    void run_until_stopped() {}
    bool process_one_command() { return false; }
    void stop() {}
    bool is_running() const { return false; }
    int port() const { return 0; }
    std::string url() const { return ""; }
    void set_interrupt_check(std::function<bool()>) {}
    httplib::Server* http_server() { return nullptr; }
};

inline std::string format_http_info(const std::string&, int, const std::string&, const std::string& = "", bool = true) { return ""; }
inline std::string format_http_info(const std::string&, int, const std::string& = "") { return ""; }
inline std::string format_http_status(int, bool, const std::string&) { return ""; }
inline std::string format_http_status(int, bool) { return ""; }

}  // namespace xsql::thinclient

#endif  // XSQL_HAS_THINCLIENT
