/**
 * xsql/functions.hpp - SQL function registration helpers
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Provides utilities for registering custom SQL functions.
 */

#pragma once

#include <sqlite3.h>
#include <string>
#include <functional>
#include <cstdint>

namespace xsql {

// ============================================================================
// SQL Function Types
// ============================================================================

using SqlScalarFn = std::function<void(sqlite3_context*, int argc, sqlite3_value**)>;

// ============================================================================
// Result Helpers
// ============================================================================

inline void result_int64(sqlite3_context* ctx, int64_t value) {
    sqlite3_result_int64(ctx, value);
}

inline void result_int(sqlite3_context* ctx, int value) {
    sqlite3_result_int(ctx, value);
}

inline void result_double(sqlite3_context* ctx, double value) {
    sqlite3_result_double(ctx, value);
}

inline void result_text(sqlite3_context* ctx, const std::string& value) {
    sqlite3_result_text(ctx, value.c_str(), -1, SQLITE_TRANSIENT);
}

inline void result_text(sqlite3_context* ctx, const char* value) {
    sqlite3_result_text(ctx, value, -1, SQLITE_TRANSIENT);
}

inline void result_blob(sqlite3_context* ctx, const void* data, size_t size) {
    sqlite3_result_blob(ctx, data, static_cast<int>(size), SQLITE_TRANSIENT);
}

inline void result_null(sqlite3_context* ctx) {
    sqlite3_result_null(ctx);
}

inline void result_error(sqlite3_context* ctx, const std::string& msg) {
    sqlite3_result_error(ctx, msg.c_str(), -1);
}

// ============================================================================
// Argument Helpers
// ============================================================================

inline int64_t arg_int64(sqlite3_value* val) {
    return sqlite3_value_int64(val);
}

inline int arg_int(sqlite3_value* val) {
    return sqlite3_value_int(val);
}

inline double arg_double(sqlite3_value* val) {
    return sqlite3_value_double(val);
}

inline std::string arg_text(sqlite3_value* val) {
    const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
    return text ? text : "";
}

inline const void* arg_blob(sqlite3_value* val) {
    return sqlite3_value_blob(val);
}

inline int arg_bytes(sqlite3_value* val) {
    return sqlite3_value_bytes(val);
}

inline int arg_type(sqlite3_value* val) {
    return sqlite3_value_type(val);
}

inline bool arg_is_null(sqlite3_value* val) {
    return sqlite3_value_type(val) == SQLITE_NULL;
}

// ============================================================================
// Function Registration
// ============================================================================

namespace detail {

// Wrapper to store std::function and call it from C callback
struct FunctionWrapper {
    SqlScalarFn fn;
};

inline void scalar_callback(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* wrapper = static_cast<FunctionWrapper*>(sqlite3_user_data(ctx));
    if (wrapper && wrapper->fn) {
        wrapper->fn(ctx, argc, argv);
    }
}

inline void destroy_wrapper(void* ptr) {
    delete static_cast<FunctionWrapper*>(ptr);
}

} // namespace detail

inline int register_scalar_function(
    sqlite3* db,
    const char* name,
    int argc,
    SqlScalarFn fn,
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC
) {
    auto* wrapper = new detail::FunctionWrapper{std::move(fn)};
    return sqlite3_create_function_v2(
        db,
        name,
        argc,
        flags,
        wrapper,
        detail::scalar_callback,
        nullptr,
        nullptr,
        detail::destroy_wrapper
    );
}

} // namespace xsql
