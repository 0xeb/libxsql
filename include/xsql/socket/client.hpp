/**
 * @file client.hpp
 * @brief TCP socket client for xsql tools
 *
 * Connects to an xsql server and executes SQL queries.
 * Self-contained - no dependencies on database libraries.
 *
 * Usage:
 *   xsql::socket::Client client;
 *   if (client.connect("localhost", 13337)) {
 *       auto result = client.query("SELECT * FROM functions");
 *   }
 */

#pragma once

#include "protocol.hpp"

#include <string>
#include <cstdint>
#include <cstddef>
#include <limits>

// Platform-specific socket includes
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    typedef SOCKET socket_t;
    #define SOCKET_INVALID INVALID_SOCKET
    #define CLOSE_SOCKET closesocket
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    typedef int socket_t;
    #define SOCKET_INVALID -1
    #define CLOSE_SOCKET close
#endif

namespace xsql::socket {

//=============================================================================
// Socket Client
//=============================================================================

class Client {
    socket_t sock_ = SOCKET_INVALID;
    std::string error_;
    bool wsa_init_ = false;
    size_t max_message_bytes_ = 10 * 1024 * 1024;
    std::string auth_token_;

public:
    Client() {
#ifdef _WIN32
        WSADATA wsa;
        wsa_init_ = (WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
#endif
    }

    ~Client() {
        disconnect();
#ifdef _WIN32
        if (wsa_init_) WSACleanup();
#endif
    }

    // Non-copyable
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /**
     * Connect to server.
     * @param host Hostname or IP address
     * @param port Port number
     * @return true on success
     */
    bool connect(const std::string& host, int port) {
        // Use getaddrinfo for hostname resolution (supports both hostnames and IPs)
        struct addrinfo hints{}, *result = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        std::string port_str = std::to_string(port);
        int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
        if (ret != 0 || result == nullptr) {
            error_ = "failed to resolve host: " + host;
            return false;
        }

        sock_ = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        if (sock_ == SOCKET_INVALID) {
            freeaddrinfo(result);
            error_ = "socket() failed";
            return false;
        }

        if (::connect(sock_, result->ai_addr, static_cast<int>(result->ai_addrlen)) < 0) {
            CLOSE_SOCKET(sock_);
            sock_ = SOCKET_INVALID;
            freeaddrinfo(result);
            error_ = "connect() failed";
            return false;
        }

        freeaddrinfo(result);
        return true;
    }

    /**
     * Disconnect from server.
     */
    void disconnect() {
        if (sock_ != SOCKET_INVALID) {
            CLOSE_SOCKET(sock_);
            sock_ = SOCKET_INVALID;
        }
    }

    bool is_connected() const { return sock_ != SOCKET_INVALID; }
    const std::string& error() const { return error_; }

    void set_max_message_bytes(size_t bytes) { max_message_bytes_ = bytes; }
    size_t max_message_bytes() const { return max_message_bytes_; }

    void set_auth_token(std::string token) { auth_token_ = std::move(token); }
    const std::string& auth_token() const { return auth_token_; }

    /**
     * Execute SQL query.
     * @param sql SQL query string
     * @return Query result
     */
    RemoteResult query(const std::string& sql) {
        RemoteResult result;

        if (!is_connected()) {
            result.error = "not connected";
            return result;
        }

        // Build JSON request
        std::string request = "{\"sql\":\"";
        request += json_escape(sql);
        request += "\"";
        if (!auth_token_.empty()) {
            request += ",\"token\":\"";
            request += json_escape(auth_token_);
            request += "\"";
        }
        request += "}";

        if (!send_message(request)) {
            result.error = "send failed";
            return result;
        }

        std::string response;
        if (!recv_message(response)) {
            result.error = "recv failed";
            return result;
        }

        return parse_response(response);
    }

private:
    bool send_message(const std::string& payload) {
        if (payload.size() > max_message_bytes_) return false;
        if (payload.size() > static_cast<size_t>((std::numeric_limits<uint32_t>::max)())) return false;

        auto send_all = [&](const char* data, size_t len) -> bool {
            size_t total = 0;
            while (total < len) {
                int n = send(sock_, data + total, static_cast<int>(len - total), 0);
                if (n <= 0) return false;
                total += static_cast<size_t>(n);
            }
            return true;
        };

        uint32_t len = static_cast<uint32_t>(payload.size());
        uint32_t len_net = htonl(len);

        if (!send_all(reinterpret_cast<const char*>(&len_net), sizeof(len_net))) return false;
        return send_all(payload.data(), payload.size());
    }

    bool recv_message(std::string& payload) {
        auto recv_all = [&](char* data, size_t len) -> bool {
            size_t total = 0;
            while (total < len) {
                int n = recv(sock_, data + total, static_cast<int>(len - total), 0);
                if (n <= 0) return false;
                total += static_cast<size_t>(n);
            }
            return true;
        };

        uint32_t len_net = 0;
        if (!recv_all(reinterpret_cast<char*>(&len_net), sizeof(len_net))) return false;

        uint32_t len = ntohl(len_net);
        if (static_cast<size_t>(len) > max_message_bytes_) return false;

        payload.resize(len);
        return recv_all(payload.data(), payload.size());
    }
};

}  // namespace xsql::socket
