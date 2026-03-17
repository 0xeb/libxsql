/**
 * xsql/functions.hpp - SQL function registration helpers
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Public tool code should use only xsql wrapper types here.
 */

#pragma once

#include "status.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>

namespace xsql {

class Statement;

class FunctionArg {
public:
    explicit FunctionArg(const void* value = nullptr) : value_(value) {}

    int64_t as_int64() const;
    int as_int() const;
    double as_double() const;
    std::string as_text() const;
    const char* as_c_str() const;
    const void* as_blob() const;
    int bytes() const;
    int type() const;
    bool is_null() const;
    bool is_nochange() const;

private:
    const void* value_ = nullptr;
};

class QueryRow {
public:
    explicit QueryRow(const Statement* statement = nullptr) : statement_(statement) {}

    std::string text(int col) const;
    int int_value(int col) const;
    int64_t int64_value(int col) const;
    double double_value(int col) const;
    bool is_null(int col) const;

private:
    const Statement* statement_ = nullptr;
};

class FunctionContext {
public:
    explicit FunctionContext(void* ctx = nullptr) : ctx_(ctx) {}

    void result_int(int val);
    void result_int64(int64_t val);
    void result_text(const std::string& val);
    void result_text(const char* val);
    void result_text_static(const char* val);
    void result_double(double val);
    void result_blob(const void* data, size_t len);
    void result_null();
    void result_error(const std::string& msg);
    void result_error(const char* msg);

    template<typename Fn>
    bool query_each(const std::string& sql, Fn&& fn, std::string* error = nullptr);

    std::string db_error() const;

private:
    void* ctx_ = nullptr;
};

using ScalarFn = std::function<void(FunctionContext& ctx, int argc, FunctionArg* argv)>;

namespace detail {

template<typename ArgPtr, typename Fn>
inline void with_args(int argc, ArgPtr argv, Fn&& fn) {
    auto* typed_args = reinterpret_cast<FunctionArg*>(
        const_cast<void**>(reinterpret_cast<void* const*>(argv)));
    if constexpr (std::is_invocable_v<Fn, int, FunctionArg*>) {
        fn(argc, typed_args);
    } else {
        fn(typed_args);
    }
}

bool function_query_each(void* context,
                         const std::string& sql,
                         const std::function<void(const QueryRow&)>& fn,
                         std::string* error);

} // namespace detail

template<typename Fn>
inline bool FunctionContext::query_each(const std::string& sql, Fn&& fn, std::string* error) {
    return detail::function_query_each(
        ctx_,
        sql,
        std::function<void(const QueryRow&)>(std::forward<Fn>(fn)),
        error);
}

} // namespace xsql
