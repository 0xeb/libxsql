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
        request += "\"}";

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
        uint32_t len = static_cast<uint32_t>(payload.size());
        if (send(sock_, reinterpret_cast<char*>(&len), 4, 0) != 4) return false;
        if (send(sock_, payload.c_str(), static_cast<int>(len), 0) != static_cast<int>(len)) return false;
        return true;
    }

    bool recv_message(std::string& payload) {
        uint32_t len = 0;
        int received = recv(sock_, reinterpret_cast<char*>(&len), 4, 0);
        if (received != 4) return false;
        if (len > 100 * 1024 * 1024) return false;  // 100MB limit

        payload.resize(len);
        size_t total = 0;
        while (total < len) {
            int n = recv(sock_, payload.data() + total, static_cast<int>(len - total), 0);
            if (n <= 0) return false;
            total += n;
        }
        return true;
    }
};

}  // namespace xsql::socket
