// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
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

enum class RuntimeSettingType {
    boolean,
    integer,
    string,
};

struct RuntimeSettingSpec {
    std::string key;
    RuntimeSettingType type = RuntimeSettingType::string;
    std::string scope;
    std::string default_value;
    bool writable = true;
    int64_t minimum = (std::numeric_limits<int64_t>::min)();
    int64_t maximum = (std::numeric_limits<int64_t>::max)();
};

// One row in the canonical SQL surface. The richer specification (including
// default and writable) remains available through RuntimeSettingsCore::specs();
// SQL intentionally exposes only this stable four-column shape.
struct RuntimeSettingEntry {
    std::string key;
    std::string value;
    std::string type;
    std::string scope;
    // Internal registration-local context used by the writable table adapter.
    // It is deliberately not exposed as a SQL column.
    std::shared_ptr<void> connection_state;
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

inline std::string trim_copy(const std::string& text);
inline bool parse_bool_value(const std::string& text, bool& value);

class RuntimeSettingsCore {
public:
    explicit RuntimeSettingsCore(RuntimeSettingsCoreOptions options = {})
        : max_timeout_stack_depth_(options.max_timeout_stack_depth) {
        // The built-in registrations are constants that cannot legitimately
        // fail; downstream code relies on that (unchecked .at() lookups by
        // these keys). Aggregate and assert so a silently-ignored failure can
        // never leave the registry half-built.
        bool ok = true;
        ok &= register_integer_setting(
            "query_timeout_ms", 60000, 0, kMaxTimeoutMs, "common");
        ok &= register_integer_setting(
            "queue_admission_timeout_ms", 120000, 0, kMaxTimeoutMs, "common");
        ok &= register_integer_setting("max_queue", 64, 0, kMaxQueueLimit,
                                       "common");
        ok &= register_bool_setting("hints_enabled", true, "common");
        ok &= register_integer_setting(
            "timeout_stack_depth", 0, 0,
            (std::numeric_limits<int64_t>::max)(), "common", false);
        // Clamp the size_t depth into int64_t range: a SIZE_MAX ("unbounded on
        // a 32-bit-int platform"-style) option must not wrap negative and make
        // this registration silently fail its own range check.
        ok &= register_integer_setting(
            "max_timeout_stack_depth",
            static_cast<int64_t>((std::min)(
                max_timeout_stack_depth_,
                static_cast<std::size_t>(
                    (std::numeric_limits<int64_t>::max)()))),
            0, (std::numeric_limits<int64_t>::max)(), "common", false);
        ok &= register_integer_setting(
            "timeout_push", 60000, 0, kMaxTimeoutMs, "action", false);
        ok &= register_integer_setting(
            "timeout_pop", 60000, 0, kMaxTimeoutMs, "action", false);
        assert(ok && "built-in runtime setting registration failed");
        (void)ok;
    }

    // Not designed for polymorphic use: extension happens by registering keys
    // into this same registry (register_*_setting), not by overriding — the
    // accessors are non-virtual and every consumer calls through a base
    // reference, so an override would be silently shadowed. In-tree ownership
    // is by value, static singleton, or shared_ptr (whose deleter is captured
    // at construction), so no virtual destructor is needed either.
    ~RuntimeSettingsCore() = default;

    bool register_bool_setting(const std::string& key, bool default_value,
                               const std::string& scope, bool writable = true) {
        RuntimeSettingSpec spec;
        spec.key = key;
        spec.type = RuntimeSettingType::boolean;
        spec.scope = scope;
        spec.default_value = default_value ? "1" : "0";
        spec.writable = writable;
        return register_setting(std::move(spec));
    }

    bool register_integer_setting(
        const std::string& key, int64_t default_value, int64_t minimum,
        int64_t maximum, const std::string& scope, bool writable = true) {
        if (minimum > maximum || default_value < minimum ||
            default_value > maximum) {
            return false;
        }
        RuntimeSettingSpec spec;
        spec.key = key;
        spec.type = RuntimeSettingType::integer;
        spec.scope = scope;
        spec.default_value = std::to_string(default_value);
        spec.writable = writable;
        spec.minimum = minimum;
        spec.maximum = maximum;
        return register_setting(std::move(spec));
    }

    bool register_string_setting(const std::string& key,
                                 std::string default_value,
                                 const std::string& scope,
                                 bool writable = true) {
        // The default must satisfy the same contract writes do (non-empty,
        // bounded) — a key whose default could never be re-assigned to its own
        // value would be incoherent. Mirrors register_integer_setting, which
        // range-checks its own default.
        if (default_value.empty() ||
            default_value.size() > kMaxStringSettingBytes) {
            return false;
        }
        RuntimeSettingSpec spec;
        spec.key = key;
        spec.type = RuntimeSettingType::string;
        spec.scope = scope;
        spec.default_value = std::move(default_value);
        spec.writable = writable;
        return register_setting(std::move(spec));
    }

    std::vector<RuntimeSettingSpec> specs() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RuntimeSettingSpec> result;
        result.reserve(order_.size());
        for (const auto& key : order_) {
            result.push_back(settings_.at(key).spec);
        }
        return result;
    }

    std::vector<RuntimeSettingEntry> enumerate() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RuntimeSettingEntry> rows;
        rows.reserve(order_.size());
        for (const auto& key : order_) {
            const auto& record = settings_.at(key);
            std::string value = record.value;
            if (key == "timeout_stack_depth") {
                value = std::to_string(timeout_stack_.size());
            } else if (key == "max_timeout_stack_depth") {
                value = std::to_string(max_timeout_stack_depth_);
            } else if (key == "timeout_push" || key == "timeout_pop") {
                value = settings_.at("query_timeout_ms").value;
            }
            rows.push_back(
                {record.spec.key, std::move(value),
                 type_name(record.spec.type), record.spec.scope});
        }
        return rows;
    }

    RuntimePragmaReply normalize(const std::string& key,
                                 const std::string& value,
                                 const std::string& prefix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return normalize_locked(key, value, prefix);
    }

    // Direct setters are committed writes. They intentionally do not participate
    // in a connection's SQL transaction; a later successful SQL xCommit may win
    // for the same key, while a SQL rollback can never replay over this value.
    RuntimePragmaReply apply(const std::string& key, const std::string& value,
                             const std::string& prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        RuntimePragmaReply reply = normalize_locked(key, value, prefix);
        if (reply.handled && reply.success) {
            settings_.at(key).value = reply.value;
        }
        return reply;
    }

    std::optional<std::string> value(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = settings_.find(key);
        if (it == settings_.end()) {
            return std::nullopt;
        }
        if (key == "timeout_stack_depth") {
            return std::to_string(timeout_stack_.size());
        }
        if (key == "max_timeout_stack_depth") {
            return std::to_string(max_timeout_stack_depth_);
        }
        if (key == "timeout_push" || key == "timeout_pop") {
            return settings_.at("query_timeout_ms").value;
        }
        return it->second.value;
    }

    bool bool_value(const std::string& key, bool fallback = false) const {
        const auto setting = value(key);
        if (!setting) {
            return fallback;
        }
        return *setting == "1";
    }

    int64_t integer_value(const std::string& key, int64_t fallback = 0) const {
        const auto setting = value(key);
        if (!setting) {
            return fallback;
        }
        try {
            return std::stoll(*setting);
        } catch (...) {
            return fallback;
        }
    }

    // Called only by the runtime_settings table after every value has already
    // been normalized. std::string::swap is noexcept, so all keys become visible
    // atomically under one lock without invoking product callbacks in xCommit.
    // (The try/catch covers only mutex::lock()'s theoretical std::system_error —
    // same rationale as discard_staged.)
    void commit_staged(
        std::unordered_map<std::string, std::string>& staged,
        bool owns_staged_query_timeout) noexcept {
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [key, value] : staged) {
                const auto it = settings_.find(key);
                if (it != settings_.end() && it->second.spec.writable) {
                    it->second.value.swap(value);
                }
            }
            if (owns_staged_query_timeout &&
                staged_query_timeout_transactions_ != 0) {
                --staged_query_timeout_transactions_;
            }
        } catch (...) {
        }
    }

    void discard_staged(bool owns_staged_query_timeout) noexcept {
        if (!owns_staged_query_timeout) {
            return;
        }
        // noexcept is load-bearing (called from noexcept vtable rollback
        // hooks), but mutex::lock() can throw std::system_error — swallow it
        // rather than terminate; the counter then stays conservative.
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            if (staged_query_timeout_transactions_ != 0) {
                --staged_query_timeout_transactions_;
            }
        } catch (...) {
        }
    }

    bool begin_staged_query_timeout() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!timeout_stack_.empty()) {
            return false;
        }
        ++staged_query_timeout_transactions_;
        return true;
    }

    bool timeout_stack_active() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return !timeout_stack_.empty();
    }

    bool staged_query_timeout_active() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return staged_query_timeout_transactions_ != 0;
    }

    RuntimeSettingsSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        RuntimeSettingsSnapshot snap;
        snap.query_timeout_ms =
            static_cast<int>(integer_value_locked("query_timeout_ms"));
        snap.queue_admission_timeout_ms =
            static_cast<int>(integer_value_locked("queue_admission_timeout_ms"));
        snap.max_queue =
            static_cast<std::size_t>(integer_value_locked("max_queue"));
        snap.hints_enabled = settings_.at("hints_enabled").value == "1";
        snap.timeout_stack_depth = timeout_stack_.size();
        return snap;
    }

    int query_timeout_ms() const {
        return static_cast<int>(integer_value("query_timeout_ms", 60000));
    }

    int queue_admission_timeout_ms() const {
        return static_cast<int>(
            integer_value("queue_admission_timeout_ms", 120000));
    }

    std::size_t max_queue() const {
        return static_cast<std::size_t>(integer_value("max_queue", 64));
    }

    bool hints_enabled() const {
        return bool_value("hints_enabled", true);
    }

    // The configured timeout_push stack cap (0 == unbounded). Exposed so the
    // common PRAGMA parser can report the actual cap instead of a hardcoded
    // constant when a product overrides RuntimeSettingsCoreOptions.
    std::size_t max_timeout_stack_depth() const {
        return max_timeout_stack_depth_;
    }

    // Compatibility spelling retained for consumers that only want common rows.
    // Product keys now live in this same registry rather than subclass overrides.
    std::vector<RuntimeSettingEntry> enumerate_common() const {
        auto rows = enumerate();
        rows.erase(
            std::remove_if(
                rows.begin(), rows.end(),
                [](const RuntimeSettingEntry& row) {
                    return row.scope != "common" && row.scope != "action";
                }),
            rows.end());
        return rows;
    }

    bool set_query_timeout_ms(int value) {
        return apply("query_timeout_ms", std::to_string(value), "runtime").success;
    }

    bool set_queue_admission_timeout_ms(int value) {
        return apply(
            "queue_admission_timeout_ms", std::to_string(value), "runtime").success;
    }

    bool set_max_queue(std::size_t value) {
        if (value > static_cast<std::size_t>((std::numeric_limits<int64_t>::max)())) {
            return false;
        }
        return apply("max_queue", std::to_string(value), "runtime").success;
    }

    void set_hints_enabled(bool enabled) {
        (void)apply("hints_enabled", enabled ? "1" : "0", "runtime");
    }

    bool timeout_push(int timeout_ms, int* effective_timeout_ms = nullptr) {
        if (!is_valid_timeout(timeout_ms)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (staged_query_timeout_transactions_ != 0) {
            return false;
        }
        if (max_timeout_stack_depth_ != 0 &&
            timeout_stack_.size() >= max_timeout_stack_depth_) {
            return false;
        }
        try {
            timeout_stack_.push_back(
                static_cast<int>(integer_value_locked("query_timeout_ms")));
        } catch (...) {
            return false;
        }
        settings_.at("query_timeout_ms").value = std::to_string(timeout_ms);
        if (effective_timeout_ms != nullptr) {
            *effective_timeout_ms = timeout_ms;
        }
        return true;
    }

    bool timeout_pop(int* effective_timeout_ms = nullptr) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (staged_query_timeout_transactions_ != 0) {
            return false;
        }
        if (timeout_stack_.empty()) {
            return false;
        }
        const int restored = timeout_stack_.back();
        timeout_stack_.pop_back();
        settings_.at("query_timeout_ms").value = std::to_string(restored);
        if (effective_timeout_ms != nullptr) {
            *effective_timeout_ms = restored;
        }
        return true;
    }

    static bool is_valid_timeout(int value) {
        return value >= 0 && value <= kMaxTimeoutMs;
    }

private:
    struct Record {
        RuntimeSettingSpec spec;
        std::string value;
    };

    static constexpr int64_t kMaxTimeoutMs = 3600 * 1000;
    static constexpr int64_t kMaxQueueLimit = 10000;
    // Byte cap for string-typed setting values (and defaults). Settings are
    // small named knobs; anything larger belongs in a real storage surface.
    static constexpr std::size_t kMaxStringSettingBytes = 4096;

    static const char* type_name(RuntimeSettingType type) noexcept {
        switch (type) {
        case RuntimeSettingType::boolean:
            return "bool";
        case RuntimeSettingType::integer:
            return "int";
        case RuntimeSettingType::string:
            return "string";
        }
        return "string";
    }

    bool register_setting(RuntimeSettingSpec spec) {
        if (spec.key.empty() || spec.scope.empty()) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (settings_.find(spec.key) != settings_.end()) {
            return false;
        }
        const std::string key = spec.key;
        const std::string value = spec.default_value;
        settings_.emplace(key, Record{std::move(spec), value});
        order_.push_back(key);
        return true;
    }

    RuntimePragmaReply normalize_locked(const std::string& key,
                                        const std::string& input,
                                        const std::string& prefix) const {
        const auto it = settings_.find(key);
        if (it == settings_.end() || it->second.spec.scope == "action") {
            return {};
        }
        RuntimePragmaReply reply;
        reply.handled = true;
        reply.name = key;
        if (!it->second.spec.writable) {
            reply.error = "runtime_settings: '" + key + "' is read-only";
            return reply;
        }

        const RuntimeSettingSpec& spec = it->second.spec;
        if (spec.type == RuntimeSettingType::boolean) {
            bool parsed = false;
            if (!parse_bool_value(input, parsed)) {
                reply.error = "Invalid " + prefix + "." + key + " value";
                return reply;
            }
            reply.value = parsed ? "1" : "0";
        } else if (spec.type == RuntimeSettingType::integer) {
            const std::string trimmed = trim_copy(input);
            try {
                std::size_t consumed = 0;
                const long long parsed = std::stoll(trimmed, &consumed, 10);
                if (consumed != trimmed.size() || parsed < spec.minimum ||
                    parsed > spec.maximum) {
                    reply.error = "Invalid " + prefix + "." + key + " value";
                    return reply;
                }
                reply.value = std::to_string(parsed);
            } catch (...) {
                reply.error = "Invalid " + prefix + "." + key + " value";
                return reply;
            }
        } else {
            // RuntimeSettingType::string — reject empty (the writable table
            // already rejects NULL, and an empty string is the same "unset"
            // shape) and bound the size: a staged value lives in the
            // per-connection overlay until COMMIT, so an unbounded blob is a
            // per-connection memory hole.
            if (input.empty()) {
                reply.error =
                    "Invalid " + prefix + "." + key + " value (may not be empty)";
                return reply;
            }
            if (input.size() > kMaxStringSettingBytes) {
                reply.error = "Invalid " + prefix + "." + key +
                              " value (exceeds " +
                              std::to_string(kMaxStringSettingBytes) +
                              " bytes)";
                return reply;
            }
            reply.value = input;
        }
        reply.success = true;
        return reply;
    }

    int64_t integer_value_locked(const std::string& key) const {
        const Record& record = settings_.at(key);
        try {
            return std::stoll(record.value);
        } catch (...) {
            // Registry invariant: integer values always parse (normalize_locked
            // canonicalizes every write). Fall back to the spec default rather
            // than throwing out of a caller that holds mutex_.
            try {
                return std::stoll(record.spec.default_value);
            } catch (...) {
                return 0;
            }
        }
    }

    mutable std::mutex mutex_;
    std::vector<std::string> order_;
    std::unordered_map<std::string, Record> settings_;
    std::vector<int> timeout_stack_;
    std::size_t staged_query_timeout_transactions_ = 0;
    std::size_t max_timeout_stack_depth_ = 64;
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

// Strip SQL comments — line `-- ...` and block `/* ... */` — anywhere in the
// text (leading, interior, trailing), outside string literals, so the PRAGMA
// classifier sees only real tokens. One quote-aware pass replaces the old
// leading/trailing pair, whose split responsibilities left gaps: a trailing
// block comment survived into the value, a `--` INSIDE a block comment
// truncated the statement, an interior comment broke prefix matching, and a
// lone-CR line ending swallowed everything after it.
//
// Rules: comment markers inside '...' or "..." are data (doubled quotes are the
// SQL escape); a line comment ends at \n OR \r; an unterminated block comment
// discards the remainder (SQLite treats it as whitespace to end-of-input).
// Each comment is replaced by a single space so token boundaries survive.
inline std::string strip_sql_comments(const std::string& text) {
    std::string out;
    out.reserve(text.size());
    std::size_t i = 0;
    while (i < text.size()) {
        const char c = text[i];
        if (c == '\'' || c == '"') {
            const char quote = c;
            out.push_back(c);
            ++i;
            while (i < text.size()) {
                out.push_back(text[i]);
                if (text[i] == quote) {
                    if (i + 1 < text.size() && text[i + 1] == quote) {
                        out.push_back(text[i + 1]);  // escaped quote: stay inside
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                ++i;
            }
            continue;
        }
        if (c == '-' && i + 1 < text.size() && text[i + 1] == '-') {
            i += 2;
            while (i < text.size() && text[i] != '\n' && text[i] != '\r') ++i;
            out.push_back(' ');
            continue;
        }
        if (c == '/' && i + 1 < text.size() && text[i + 1] == '*') {
            const std::size_t end = text.find("*/", i + 2);
            i = (end == std::string::npos) ? text.size() : end + 2;
            out.push_back(' ');
            continue;
        }
        out.push_back(c);
        ++i;
    }
    return out;
}

inline RuntimePragmaRequest parse_runtime_pragma(const char* sql,
                                                 const std::string& product_prefix) {
    RuntimePragmaRequest request;
    if (sql == nullptr) {
        return request;
    }

    std::string text = trim_copy(strip_sql_comments(sql));
    if (text.empty()) {
        return request;
    }
    // Statement terminators (also `;;` — comment stripping can expose more
    // than one).
    while (!text.empty() && text.back() == ';') {
        text.pop_back();
        text = trim_copy(text);
    }
    if (text.empty()) {
        return request;
    }

    const std::string lower = to_lower_copy(text);
    const std::string pragma_prefix = "pragma";
    if (lower.rfind(pragma_prefix, 0) != 0) {
        return request;
    }
    // Word boundary: "pragmatic_helper(...)" starts with "pragma" but is not
    // the PRAGMA keyword.
    if (text.size() <= pragma_prefix.size() ||
        std::isspace(static_cast<unsigned char>(text[pragma_prefix.size()])) ==
            0) {
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

    // Only imperative actions remain PRAGMAs. Typed value settings are changed
    // exclusively through UPDATE runtime_settings so they retain SQL transaction
    // semantics instead of bypassing the connection's staged overlay.
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
            if (settings.staged_query_timeout_active()) {
                return pragma_error(product_prefix +
                                    ".timeout_push is unavailable while runtime_settings has "
                                    "a staged query_timeout_ms write (COMMIT or ROLLBACK the "
                                    "transaction holding it first; the interlock is "
                                    "process-wide, so it may belong to another connection)");
            }
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
            if (settings.staged_query_timeout_active()) {
                return pragma_error(product_prefix +
                                    ".timeout_pop is unavailable while runtime_settings has "
                                    "a staged query_timeout_ms write (COMMIT or ROLLBACK the "
                                    "transaction holding it first; the interlock is "
                                    "process-wide, so it may belong to another connection)");
            }
            return pragma_error(product_prefix + ".timeout_pop stack is empty");
        }
        return pragma_result("query_timeout_ms", std::to_string(effective_timeout));
    }

    return {};
}

}  // namespace xsql::runtime
