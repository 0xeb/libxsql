// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

// Per-aggregation context for custom aggregate functions. Each query that
// invokes the aggregate gets its own state pointer (via state_ptr()), zero
// initialized by SQLite on first access and stable across step() calls
// for the same aggregation. Allocate the actual state on the heap on first
// access; release it in the final() callback.
class AggregateContext {
public:
    explicit AggregateContext(void* ctx = nullptr) : ctx_(ctx) {}

    // Returns a pointer to one pointer-sized slot owned by SQLite. The slot is
    // zero-initialized on first access and stable for the duration of the
    // aggregation. Typical usage: store a heap pointer to your state object
    // here, allocate on first step, free in final.
    void** state_ptr();

    void result_blob(const void* data, size_t len);
    void result_null();
    void result_error(const std::string& msg);
    void result_error(const char* msg);

private:
    void* ctx_ = nullptr;
};

using AggregateStepFn = std::function<void(AggregateContext& ctx, int argc, FunctionArg* argv)>;
using AggregateFinalFn = std::function<void(AggregateContext& ctx)>;

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
