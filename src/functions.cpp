// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <xsql/functions.hpp>

#include <xsql/statement.hpp>

#include <sqlite3.h>

namespace xsql {

int64_t FunctionArg::as_int64() const {
    return sqlite3_value_int64(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

int FunctionArg::as_int() const {
    return sqlite3_value_int(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

double FunctionArg::as_double() const {
    return sqlite3_value_double(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

std::string FunctionArg::as_text() const {
    const auto* text = sqlite3_value_text(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
    return text ? reinterpret_cast<const char*>(text) : "";
}

const char* FunctionArg::as_c_str() const {
    return reinterpret_cast<const char*>(
        sqlite3_value_text(static_cast<sqlite3_value*>(const_cast<void*>(value_))));
}

const void* FunctionArg::as_blob() const {
    return sqlite3_value_blob(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

int FunctionArg::bytes() const {
    return sqlite3_value_bytes(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

int FunctionArg::type() const {
    return sqlite3_value_type(static_cast<sqlite3_value*>(const_cast<void*>(value_)));
}

bool FunctionArg::is_null() const {
    return type() == SQLITE_NULL;
}

bool FunctionArg::is_nochange() const {
    return sqlite3_value_nochange(static_cast<sqlite3_value*>(const_cast<void*>(value_))) != 0;
}

std::string QueryRow::text(int col) const {
    return statement_ ? statement_->text(col) : std::string();
}

int QueryRow::int_value(int col) const {
    return statement_ ? statement_->int_value(col) : 0;
}

int64_t QueryRow::int64_value(int col) const {
    return statement_ ? statement_->int64_value(col) : 0;
}

double QueryRow::double_value(int col) const {
    return statement_ ? statement_->double_value(col) : 0.0;
}

bool QueryRow::is_null(int col) const {
    return !statement_ || statement_->column_is_null(col);
}

void FunctionContext::result_int(int val) {
    sqlite3_result_int(static_cast<sqlite3_context*>(ctx_), val);
}

void FunctionContext::result_int64(int64_t val) {
    sqlite3_result_int64(static_cast<sqlite3_context*>(ctx_), val);
}

void FunctionContext::result_text(const std::string& val) {
    sqlite3_result_text(static_cast<sqlite3_context*>(ctx_), val.c_str(), -1, SQLITE_TRANSIENT);
}

void FunctionContext::result_text(const char* val) {
    sqlite3_result_text(static_cast<sqlite3_context*>(ctx_), val, -1, SQLITE_TRANSIENT);
}

void FunctionContext::result_text_static(const char* val) {
    sqlite3_result_text(static_cast<sqlite3_context*>(ctx_), val, -1, SQLITE_STATIC);
}

void FunctionContext::result_double(double val) {
    sqlite3_result_double(static_cast<sqlite3_context*>(ctx_), val);
}

void FunctionContext::result_blob(const void* data, size_t len) {
    sqlite3_result_blob(static_cast<sqlite3_context*>(ctx_), data, static_cast<int>(len), SQLITE_TRANSIENT);
}

void FunctionContext::result_null() {
    sqlite3_result_null(static_cast<sqlite3_context*>(ctx_));
}

void FunctionContext::result_error(const std::string& msg) {
    sqlite3_result_error(static_cast<sqlite3_context*>(ctx_), msg.c_str(), -1);
}

void FunctionContext::result_error(const char* msg) {
    sqlite3_result_error(static_cast<sqlite3_context*>(ctx_), msg, -1);
}

std::string FunctionContext::db_error() const {
    auto* db = sqlite3_context_db_handle(static_cast<sqlite3_context*>(ctx_));
    return db ? std::string(sqlite3_errmsg(db)) : std::string("No database handle");
}

namespace detail {

bool function_query_each(void* context,
                         const std::string& sql,
                         const std::function<void(const QueryRow&)>& fn,
                         std::string* error) {
    auto* db = sqlite3_context_db_handle(static_cast<sqlite3_context*>(context));
    auto stmt = prepare_statement(db, sql.c_str(), error);
    if (!stmt.valid()) {
        return false;
    }

    while (true) {
        StepResult step = stmt.step();
        if (step == StepResult::row) {
            fn(QueryRow(&stmt));
            continue;
        }
        if (step == StepResult::done) {
            if (error) {
                error->clear();
            }
            return true;
        }
        if (error && error->empty()) {
            *error = stmt.error();
        }
        return false;
    }
}

} // namespace detail

} // namespace xsql
