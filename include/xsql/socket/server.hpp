/**
 * @file server.hpp
 * @brief Generic TCP socket server for xsql tools
 *
 * Provides a simple single-threaded server that accepts JSON queries
 * and returns JSON responses. Used by pdbsql and idasql.
 *
 * Usage:
 *   xsql::socket::Server server;
 *   server.set_query_handler([&](const std::string& sql) {
 *       return execute_sql(sql);  // Returns QueryResult
 *   });
 *   server.run(13337);  // Blocking
 */

#pragma once

#include "protocol.hpp"

#include <functional>
#include <atomic>
#include <thread>
#include <string>
#include <cstdint>
#include <iostream>

// Platform-specific socket includes
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    typedef SOCKET socket_t;
    #define SOCKET_INVALID INVALID_SOCKET
    #define SOCKET_ERROR_CODE WSAGetLastError()
    #define CLOSE_SOCKET closesocket
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <unistd.h>
    #include <arpa/inet.h>
    #include <errno.h>
    typedef int socket_t;
    #define SOCKET_INVALID -1
    #define SOCKET_ERROR_CODE errno
    #define CLOSE_SOCKET close
#endif

namespace xsql::socket {

//=============================================================================
// Server Configuration
//=============================================================================

struct ServerConfig {
    int port = 13337;
    std::string bind_address = "127.0.0.1";
    bool verbose = true;
};

//=============================================================================
// Socket Server
//=============================================================================

class Server {
public:
    using query_handler_t = std::function<QueryResult(const std::string& sql)>;
    using log_func_t = std::function<void(const std::string& msg)>;

private:
    ServerConfig config_;
    query_handler_t query_handler_;
    log_func_t log_func_;
    std::atomic<bool> running_{false};
    socket_t listen_sock_ = SOCKET_INVALID;
    std::thread server_thread_;
    bool wsa_init_ = false;

public:
    Server() = default;

    explicit Server(const ServerConfig& config) : config_(config) {}

    ~Server() {
        stop();
    }

    // Non-copyable
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void set_config(const ServerConfig& config) { config_ = config; }
    void set_query_handler(query_handler_t handler) { query_handler_ = std::move(handler); }
    void set_log_func(log_func_t func) { log_func_ = std::move(func); }

    bool is_running() const { return running_; }
    int port() const { return config_.port; }

    /**
     * Run server (blocking).
     * Returns when server is stopped.
     */
    bool run(int port = 0) {
        if (port > 0) config_.port = port;
        return run_server();
    }

    /**
     * Run server in background thread.
     */
    bool run_async(int port = 0) {
        if (port > 0) config_.port = port;
        server_thread_ = std::thread([this] { run_server(); });

        // Wait for server to start (with timeout)
        for (int i = 0; i < 100 && !running_; i++) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return running_;
    }

    /**
     * Stop server gracefully.
     */
    void stop() {
        if (!running_) return;
        running_ = false;

        if (listen_sock_ != SOCKET_INVALID) {
            CLOSE_SOCKET(listen_sock_);
            listen_sock_ = SOCKET_INVALID;
        }

        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

private:
    void log(const std::string& msg) {
        if (log_func_) {
            log_func_(msg);
        } else if (config_.verbose) {
            std::cerr << "[xsql] " << msg << std::endl;
        }
    }

    bool init_winsock() {
#ifdef _WIN32
        if (!wsa_init_) {
            WSADATA wsa;
            if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
                log("WSAStartup failed");
                return false;
            }
            wsa_init_ = true;
        }
#endif
        return true;
    }

    void cleanup_winsock() {
#ifdef _WIN32
        if (wsa_init_) {
            WSACleanup();
            wsa_init_ = false;
        }
#endif
    }

    bool send_message(socket_t sock, const std::string& payload) {
        uint32_t len = static_cast<uint32_t>(payload.size());
        if (send(sock, reinterpret_cast<char*>(&len), 4, 0) != 4) return false;
        if (send(sock, payload.c_str(), static_cast<int>(len), 0) != static_cast<int>(len)) return false;
        return true;
    }

    bool recv_message(socket_t sock, std::string& payload) {
        uint32_t len = 0;
        int received = recv(sock, reinterpret_cast<char*>(&len), 4, 0);
        if (received != 4) return false;
        if (len > 10 * 1024 * 1024) return false;  // 10MB limit

        payload.resize(len);
        size_t total = 0;
        while (total < len) {
            int n = recv(sock, payload.data() + total, static_cast<int>(len - total), 0);
            if (n <= 0) return false;
            total += n;
        }
        return true;
    }

    void handle_client(socket_t client) {
        std::string request;
        while (running_ && recv_message(client, request)) {
            std::string sql = extract_sql_from_request(request);
            if (sql.empty()) {
                send_message(client, "{\"success\":false,\"error\":\"Invalid request: missing sql field\"}");
                continue;
            }

            QueryResult result;
            if (query_handler_) {
                try {
                    result = query_handler_(sql);
                } catch (const std::exception& e) {
                    result = QueryResult::fail(e.what());
                }
            } else {
                result = QueryResult::fail("No query handler configured");
            }

            if (!send_message(client, result_to_json(result))) break;
        }
        CLOSE_SOCKET(client);
    }

    bool run_server() {
        if (!init_winsock()) return false;

        listen_sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_sock_ == SOCKET_INVALID) {
            log("socket() failed");
            cleanup_winsock();
            return false;
        }

        // Allow address reuse
        int opt = 1;
        setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<char*>(&opt), sizeof(opt));

        // Bind
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(config_.bind_address.c_str());
        addr.sin_port = htons(static_cast<uint16_t>(config_.port));

        if (bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            log("bind() failed: " + std::to_string(SOCKET_ERROR_CODE));
            CLOSE_SOCKET(listen_sock_);
            listen_sock_ = SOCKET_INVALID;
            cleanup_winsock();
            return false;
        }

        if (listen(listen_sock_, 1) < 0) {
            log("listen() failed");
            CLOSE_SOCKET(listen_sock_);
            listen_sock_ = SOCKET_INVALID;
            cleanup_winsock();
            return false;
        }

        running_ = true;
        log("Server listening on " + config_.bind_address + ":" + std::to_string(config_.port));

        // Set socket timeout so accept() can check running_ flag
#ifdef _WIN32
        DWORD timeout = 500;
        setsockopt(listen_sock_, SOL_SOCKET, SO_RCVTIMEO,
                   reinterpret_cast<char*>(&timeout), sizeof(timeout));
#else
        struct timeval tv = {0, 500000};
        setsockopt(listen_sock_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif

        while (running_) {
            socket_t client = accept(listen_sock_, nullptr, nullptr);
            if (client == SOCKET_INVALID) {
                continue;  // Timeout or error, check running_ and retry
            }

            log("Client connected");
            handle_client(client);
            log("Client disconnected");
        }

        if (listen_sock_ != SOCKET_INVALID) {
            CLOSE_SOCKET(listen_sock_);
            listen_sock_ = SOCKET_INVALID;
        }

        cleanup_winsock();
        log("Server stopped");
        return true;
    }
};

}  // namespace xsql::socket
