// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <cctype>
#include <cstddef>
#include <limits>
#include <mutex>
#include <string>
#include <vector>

namespace xsql::runtime {

struct RuntimeSettingsSnapshot {
    int query_timeout_ms = 60000;
    int queue_admission_timeout_ms = 120000;
    std::size_t max_queue = 64;
    bool hints_enabled = true;
    std::size_t timeout_stack_depth = 0;
};

struct RuntimeSettingsCoreOptions {
    // Maximum depth of the PRAGMA <prefix>.timeout_push stack. Defaults to a
    // bounded 64 so a client cannot grow the stack without limit (a
    // memory-growth DoS); set explicitly to 0 for unbounded. Products may still
    // override the cap without changing the common PRAGMA parser or the public
    // wrapper shape.
    std::size_t max_timeout_stack_depth = 64;
};

// One enumerated runtime setting, as surfaced by the read-only `runtime_settings`
// SQL table (a live discovery view over the PRAGMA <prefix>.* surface). Products
// append their own tool-specific entries after enumerate_common().
struct RuntimeSettingEntry {
    std::string key;
    std::string value;   // live value rendered as text (int -> decimal, bool -> "1"/"0")
    std::string type;    // "int" | "bool"
    std::string scope;   // "common" | "action" (a dispatch-only verb) | "<tool>"
};

class RuntimeSettingsCore {
public:
    explicit RuntimeSettingsCore(RuntimeSettingsCoreOptions options = {})
        : max_timeout_stack_depth_(options.max_timeout_stack_depth) {}

    RuntimeSettingsSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        RuntimeSettingsSnapshot snap;
        snap.query_timeout_ms = query_timeout_ms_;
        snap.queue_admission_timeout_ms = queue_admission_timeout_ms_;
        snap.max_queue = max_queue_;
        snap.hints_enabled = hints_enabled_;
        snap.timeout_stack_depth = timeout_stack_.size();
        return snap;
    }

    int query_timeout_ms() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return query_timeout_ms_;
    }

    int queue_admission_timeout_ms() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_admission_timeout_ms_;
    }

    std::size_t max_queue() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return max_queue_;
    }

    bool hints_enabled() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return hints_enabled_;
    }

    // The configured timeout_push stack cap (0 == unbounded). Exposed so the
    // common PRAGMA parser can report the actual cap instead of a hardcoded
    // constant when a product overrides RuntimeSettingsCoreOptions.
    std::size_t max_timeout_stack_depth() const {
        return max_timeout_stack_depth_;
    }

    // Enumerate the COMMON runtime keys with their LIVE values, for the read-only
    // `runtime_settings` SQL table. Products append their own extra entries (e.g.
    // idasql: enable_idapython, idapython_output_max) after these. The two
    // action verbs (timeout_push/timeout_pop) are dispatch-only PRAGMAs, listed so
    // an agent can discover them; their "value" is the current effective timeout.
    std::vector<RuntimeSettingEntry> enumerate_common() const {
        const RuntimeSettingsSnapshot snap = snapshot();
        std::vector<RuntimeSettingEntry> out;
        out.reserve(8);
        out.push_back({"query_timeout_ms",
                       std::to_string(snap.query_timeout_ms), "int", "common"});
        out.push_back({"queue_admission_timeout_ms",
                       std::to_string(snap.queue_admission_timeout_ms), "int", "common"});
        out.push_back({"max_queue",
                       std::to_string(snap.max_queue), "int", "common"});
        out.push_back({"hints_enabled",
                       snap.hints_enabled ? "1" : "0", "bool", "common"});
        out.push_back({"timeout_stack_depth",
                       std::to_string(snap.timeout_stack_depth), "int", "common"});
        out.push_back({"max_timeout_stack_depth",
                       std::to_string(max_timeout_stack_depth_), "int", "common"});
        out.push_back({"timeout_push",
                       std::to_string(snap.query_timeout_ms), "int", "action"});
        out.push_back({"timeout_pop",
                       std::to_string(snap.query_timeout_ms), "int", "action"});
        return out;
    }

    bool set_query_timeout_ms(int value) {
        if (!is_valid_timeout(value)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        query_timeout_ms_ = value;
        return true;
    }

    bool set_queue_admission_timeout_ms(int value) {
        if (!is_valid_timeout(value)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        queue_admission_timeout_ms_ = value;
        return true;
    }

    bool set_max_queue(std::size_t value) {
        if (value > kMaxQueueLimit) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        max_queue_ = value;
        return true;
    }

    void set_hints_enabled(bool enabled) {
        std::lock_guard<std::mutex> lock(mutex_);
        hints_enabled_ = enabled;
    }

    bool timeout_push(int timeout_ms, int* effective_timeout_ms = nullptr) {
        if (!is_valid_timeout(timeout_ms)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (max_timeout_stack_depth_ != 0 &&
            timeout_stack_.size() >= max_timeout_stack_depth_) {
            return false;
        }
        timeout_stack_.push_back(query_timeout_ms_);
        query_timeout_ms_ = timeout_ms;
        if (effective_timeout_ms != nullptr) {
            *effective_timeout_ms = query_timeout_ms_;
        }
        return true;
    }

    bool timeout_pop(int* effective_timeout_ms = nullptr) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (timeout_stack_.empty()) {
            return false;
        }
        query_timeout_ms_ = timeout_stack_.back();
        timeout_stack_.pop_back();
        if (effective_timeout_ms != nullptr) {
            *effective_timeout_ms = query_timeout_ms_;
        }
        return true;
    }

    static bool is_valid_timeout(int value) {
        return value >= 0 && value <= kMaxTimeoutMs;
    }

private:
    static constexpr int kMaxTimeoutMs = 3600 * 1000;  // 1 hour
    static constexpr std::size_t kMaxQueueLimit = 10000;

    mutable std::mutex mutex_;
    int query_timeout_ms_ = 60000;
    int queue_admission_timeout_ms_ = 120000;
    std::size_t max_queue_ = 64;
    bool hints_enabled_ = true;
    std::vector<int> timeout_stack_;
    // Bounded by default (see RuntimeSettingsCoreOptions); the constructor
    // overrides this from options. 0 means explicitly unbounded.
    std::size_t max_timeout_stack_depth_ = 64;
};

struct RuntimePragmaRequest {
    bool matched = false;
    bool has_value = false;
    std::string key;
    std::string value;
};

struct RuntimePragmaReply {
    bool handled = false;
    bool success = false;
    std::string name;
    std::string value;
    std::string error;
};

inline std::string trim_copy(const std::string& text) {
    std::size_t begin = 0;
    std::size_t end = text.size();
    while (begin < end && std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
        --end;
    }
    return text.substr(begin, end - begin);
}

inline std::string to_lower_copy(std::string text) {
    for (char& c : text) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return text;
}

inline std::string strip_optional_quotes(const std::string& text) {
    if (text.size() >= 2) {
        const char first = text.front();
        const char last = text.back();
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return text.substr(1, text.size() - 2);
        }
    }
    return text;
}

inline bool parse_int_value(const std::string& text, int& value) {
    try {
        std::size_t consumed = 0;
        const long long parsed = std::stoll(trim_copy(text), &consumed, 10);
        if (consumed != trim_copy(text).size()) {
            return false;
        }
        if (parsed < (std::numeric_limits<int>::min)() ||
            parsed > (std::numeric_limits<int>::max)()) {
            return false;
        }
        value = static_cast<int>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

inline bool parse_bool_value(const std::string& text, bool& value) {
    const std::string lower = to_lower_copy(trim_copy(text));
    if (lower == "1" || lower == "on" || lower == "true" || lower == "yes") {
        value = true;
        return true;
    }
    if (lower == "0" || lower == "off" || lower == "false" || lower == "no") {
        value = false;
        return true;
    }
    return false;
}

inline RuntimePragmaRequest parse_runtime_pragma(const char* sql,
                                                 const std::string& product_prefix) {
    RuntimePragmaRequest request;
    if (sql == nullptr) {
        return request;
    }

    std::string text = trim_copy(sql);
    if (text.empty()) {
        return request;
    }
    if (text.back() == ';') {
        text.pop_back();
        text = trim_copy(text);
    }

    const std::string lower = to_lower_copy(text);
    const std::string pragma_prefix = "pragma";
    if (lower.rfind(pragma_prefix, 0) != 0) {
        return request;
    }

    const std::string body = trim_copy(text.substr(pragma_prefix.size()));
    const std::string body_lower = to_lower_copy(body);
    const std::string runtime_prefix = to_lower_copy(product_prefix) + ".";
    if (body_lower.rfind(runtime_prefix, 0) != 0) {
        return request;
    }

    std::string key_expr = trim_copy(body.substr(runtime_prefix.size()));
    std::string value_expr;
    const std::size_t eq_pos = key_expr.find('=');
    if (eq_pos != std::string::npos) {
        request.has_value = true;
        value_expr = trim_copy(key_expr.substr(eq_pos + 1));
        key_expr = trim_copy(key_expr.substr(0, eq_pos));
        value_expr = strip_optional_quotes(value_expr);
    }

    request.matched = true;
    request.key = to_lower_copy(key_expr);
    request.value = value_expr;
    return request;
}

inline RuntimePragmaReply pragma_result(const std::string& name,
                                        const std::string& value) {
    RuntimePragmaReply reply;
    reply.handled = true;
    reply.success = true;
    reply.name = name;
    reply.value = value;
    return reply;
}

inline RuntimePragmaReply pragma_error(const std::string& error) {
    RuntimePragmaReply reply;
    reply.handled = true;
    reply.success = false;
    reply.error = error;
    return reply;
}

inline std::string unknown_runtime_pragma_error(const std::string& product_prefix) {
    return "Unknown " + product_prefix + " pragma key";
}

inline RuntimePragmaReply handle_common_runtime_pragma(const RuntimePragmaRequest& request,
                                                       const std::string& product_prefix,
                                                       RuntimeSettingsCore& settings) {
    if (!request.matched) {
        return {};
    }

    const std::string& key = request.key;
    const std::string& value_expr = request.value;

    if (key == "query_timeout_ms") {
        if (value_expr.empty()) {
            return pragma_result("query_timeout_ms",
                                 std::to_string(settings.query_timeout_ms()));
        }
        int timeout_ms = 0;
        if (!parse_int_value(value_expr, timeout_ms) ||
            !settings.set_query_timeout_ms(timeout_ms)) {
            return pragma_error("Invalid " + product_prefix + ".query_timeout_ms value");
        }
        return pragma_result("query_timeout_ms",
                             std::to_string(settings.query_timeout_ms()));
    }

    if (key == "queue_admission_timeout_ms") {
        if (value_expr.empty()) {
            return pragma_result("queue_admission_timeout_ms",
                                 std::to_string(settings.queue_admission_timeout_ms()));
        }
        int timeout_ms = 0;
        if (!parse_int_value(value_expr, timeout_ms) ||
            !settings.set_queue_admission_timeout_ms(timeout_ms)) {
            return pragma_error("Invalid " + product_prefix +
                                ".queue_admission_timeout_ms value");
        }
        return pragma_result("queue_admission_timeout_ms",
                             std::to_string(settings.queue_admission_timeout_ms()));
    }

    if (key == "max_queue") {
        if (value_expr.empty()) {
            return pragma_result("max_queue", std::to_string(settings.max_queue()));
        }
        int queue_limit = 0;
        if (!parse_int_value(value_expr, queue_limit) || queue_limit < 0 ||
            !settings.set_max_queue(static_cast<std::size_t>(queue_limit))) {
            return pragma_error("Invalid " + product_prefix + ".max_queue value");
        }
        return pragma_result("max_queue", std::to_string(settings.max_queue()));
    }

    if (key == "hints_enabled") {
        if (value_expr.empty()) {
            return pragma_result("hints_enabled", settings.hints_enabled() ? "1" : "0");
        }
        bool enabled = false;
        if (!parse_bool_value(value_expr, enabled)) {
            return pragma_error("Invalid " + product_prefix + ".hints_enabled value");
        }
        settings.set_hints_enabled(enabled);
        return pragma_result("hints_enabled", settings.hints_enabled() ? "1" : "0");
    }

    if (key == "timeout_push") {
        if (value_expr.empty()) {
            return pragma_error(product_prefix + ".timeout_push requires a timeout value");
        }
        int timeout_ms = 0;
        if (!parse_int_value(value_expr, timeout_ms)) {
            return pragma_error("Invalid " + product_prefix + ".timeout_push value");
        }
        int effective_timeout = 0;
        if (!settings.timeout_push(timeout_ms, &effective_timeout)) {
            // timeout_push fails for two distinct reasons: the value is out of
            // range, or the value is valid but the bounded stack is already full.
            // Report each accurately rather than blaming the (valid) value.
            if (!RuntimeSettingsCore::is_valid_timeout(timeout_ms)) {
                return pragma_error("Invalid " + product_prefix + ".timeout_push value");
            }
            return pragma_error(product_prefix + ".timeout_push stack full (max " +
                                std::to_string(settings.max_timeout_stack_depth()) +
                                " entries)");
        }
        return pragma_result("query_timeout_ms", std::to_string(effective_timeout));
    }

    if (key == "timeout_pop") {
        int effective_timeout = 0;
        if (!settings.timeout_pop(&effective_timeout)) {
            return pragma_error(product_prefix + ".timeout_pop stack is empty");
        }
        return pragma_result("query_timeout_ms", std::to_string(effective_timeout));
    }

    return {};
}

}  // namespace xsql::runtime
