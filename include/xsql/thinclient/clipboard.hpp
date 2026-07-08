// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

/**
 * @file clipboard.hpp
 * @brief Clipboard helpers for thinclient-style HTTP/MCP server UX.
 *
 * These helpers are header-only so all *sql tools can share
 * consistent clipboard payloads and command-output parsing.
 */

#include <cstdlib>
#include <cstring>
#include <functional>
#include <sstream>
#include <string>
#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace xsql::thinclient {

inline std::string normalize_clipboard_host(const std::string& host) {
    if (host.empty()) {
        return "127.0.0.1";
    }
    return host;
}

inline std::string format_url_host(const std::string& host) {
    std::string normalized = normalize_clipboard_host(host);
    if (!normalized.empty() &&
        normalized.front() != '[' &&
        normalized.find(':') != std::string::npos) {
        normalized = "[" + normalized + "]";
    }
    return normalized;
}

inline std::string build_http_clipboard_payload(
    const std::string& tool_name,
    const std::string& host,
    int port,
    const std::string& sample_sql = "SELECT 1",
    const std::string& connect_with_label = "",
    const std::string& server_label = "")
{
    std::ostringstream ss;
    const std::string normalized_host = format_url_host(host);
    const std::string connect_label = connect_with_label.empty() ? tool_name : connect_with_label;
    const std::string rendered_server_label = server_label.empty() ? tool_name : server_label;
    ss << "Use " << connect_label << " to connect to this " << rendered_server_label
       << " HTTP server.\n";
    ss << "curl -X POST http://" << normalized_host << ":" << port
       << "/query -d \"" << sample_sql << "\"";
    return ss.str();
}

inline std::string build_mcp_clipboard_payload(
    const std::string& server_name,
    const std::string& host,
    int port)
{
    std::ostringstream ss;
    const std::string normalized_host = format_url_host(host);
    ss << "{\n";
    ss << "  \"mcpServers\": {\n";
    ss << "    \"" << server_name << "\": {\n";
    ss << "      \"url\": \"http://" << normalized_host << ":" << port << "/sse\"\n";
    ss << "    }\n";
    ss << "  }\n";
    ss << "}\n";
    return ss.str();
}

inline bool parse_started_port(const std::string& output, const std::string& prefix, int& out_port) {
    if (output.rfind(prefix, 0) != 0) {
        return false;
    }
    const size_t start = prefix.size();
    size_t end = start;
    while (end < output.size() && output[end] >= '0' && output[end] <= '9') {
        ++end;
    }
    if (end == start) {
        return false;
    }
    const std::string port_text = output.substr(start, end - start);
    int port = std::atoi(port_text.c_str());
    if (port <= 0) {
        return false;
    }
    out_port = port;
    return true;
}

inline bool extract_url_host_from_output_line(const std::string& output,
                                              const std::string& prefix,
                                              std::string& out_host) {
    const size_t prefix_pos = output.find(prefix);
    if (prefix_pos == std::string::npos) {
        return false;
    }

    const size_t url_start = prefix_pos + prefix.size();
    const size_t url_end = output.find_first_of("\r\n", url_start);
    const std::string url = output.substr(url_start, url_end == std::string::npos
                                                       ? std::string::npos
                                                       : url_end - url_start);
    if (url.empty()) {
        return false;
    }

    size_t host_start = 0;
    const size_t scheme_pos = url.find("://");
    if (scheme_pos != std::string::npos) {
        host_start = scheme_pos + 3;
    }
    if (host_start >= url.size()) {
        return false;
    }

    if (url[host_start] == '[') {
        const size_t bracket_end = url.find(']', host_start + 1);
        if (bracket_end == std::string::npos) {
            return false;
        }
        out_host = url.substr(host_start, bracket_end - host_start + 1);
        return !out_host.empty();
    }

    const size_t port_sep = url.rfind(':');
    if (port_sep == std::string::npos || port_sep <= host_start) {
        return false;
    }

    out_host = url.substr(host_start, port_sep - host_start);
    return !out_host.empty();
}

inline bool extract_http_start_endpoint(const std::string& output, std::string& out_host, int& out_port) {
    if (!parse_started_port(output, "HTTP server started on port ", out_port)) {
        return false;
    }
    if (!extract_url_host_from_output_line(output, "URL: ", out_host)) {
        out_host = "127.0.0.1";
    }
    return true;
}

inline bool extract_mcp_start_endpoint(const std::string& output, std::string& out_host, int& out_port) {
    if (!parse_started_port(output, "MCP server started on port ", out_port)) {
        return false;
    }
    if (!extract_url_host_from_output_line(output, "SSE endpoint: ", out_host)) {
        out_host = "127.0.0.1";
    }
    return true;
}

using clipboard_copy_fn_t = std::function<bool(const std::string&)>;

inline clipboard_copy_fn_t& clipboard_copy_override() {
    static clipboard_copy_fn_t fn;
    return fn;
}

inline void set_clipboard_copy_override_for_tests(clipboard_copy_fn_t fn) {
    clipboard_copy_override() = std::move(fn);
}

inline bool try_copy_text_to_clipboard_windows_impl(const std::string& text) {
#ifdef _WIN32
    if (text.empty()) {
        return false;
    }

    int wide_len = MultiByteToWideChar(CP_UTF8, 0, text.c_str(), -1, nullptr, 0);
    if (wide_len <= 0) {
        return false;
    }

    std::wstring wide(static_cast<size_t>(wide_len), L'\0');
    if (MultiByteToWideChar(CP_UTF8, 0, text.c_str(), -1, wide.data(), wide_len) <= 0) {
        return false;
    }

    if (!OpenClipboard(nullptr)) {
        return false;
    }

    struct close_guard_t {
        ~close_guard_t() { CloseClipboard(); }
    } close_guard;

    if (!EmptyClipboard()) {
        return false;
    }

    const size_t bytes = static_cast<size_t>(wide_len) * sizeof(wchar_t);
    HGLOBAL handle = GlobalAlloc(GMEM_MOVEABLE, bytes);
    if (handle == nullptr) {
        return false;
    }

    void* mem = GlobalLock(handle);
    if (mem == nullptr) {
        GlobalFree(handle);
        return false;
    }

    memcpy(mem, wide.c_str(), bytes);
    GlobalUnlock(handle);

    if (SetClipboardData(CF_UNICODETEXT, handle) == nullptr) {
        GlobalFree(handle);
        return false;
    }

    return true;
#else
    (void)text;
    return false;
#endif
}

inline bool try_copy_text_to_clipboard_windows(const std::string& text) {
    auto& override_fn = clipboard_copy_override();
    if (override_fn) {
        return override_fn(text);
    }
    return try_copy_text_to_clipboard_windows_impl(text);
}

}  // namespace xsql::thinclient
