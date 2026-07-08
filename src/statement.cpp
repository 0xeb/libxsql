// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#include <xsql/statement.hpp>

#include <sqlite3.h>

#include <utility>

namespace xsql {

struct Statement::Impl {
    sqlite3* db = nullptr;
    sqlite3_stmt* stmt = nullptr;
    std::string error;
};

namespace {

const std::string& empty_error() {
    static const std::string kEmpty;
    return kEmpty;
}

} // namespace

Statement::Statement() = default;

Statement::~Statement() {
    if (impl_ && impl_->stmt) {
        sqlite3_finalize(impl_->stmt);
        impl_->stmt = nullptr;
    }
}

Statement::Statement(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Statement::Statement(Statement&& other) noexcept = default;
Statement& Statement::operator=(Statement&& other) noexcept = default;

bool Statement::valid() const {
    return impl_ && impl_->stmt != nullptr;
}

bool Statement::is_readonly() const {
    return valid() && sqlite3_stmt_readonly(impl_->stmt) != 0;
}

const std::string& Statement::error() const {
    return impl_ ? impl_->error : empty_error();
}

Status Statement::reset() {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_reset(impl_->stmt);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::clear_bindings() {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_clear_bindings(impl_->stmt);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_null(int index) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_null(impl_->stmt, index);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_int(int index, int value) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_int(impl_->stmt, index, value);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_int64(int index, int64_t value) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_int64(impl_->stmt, index, value);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_double(int index, double value) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_double(impl_->stmt, index, value);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_text(int index, const std::string& value) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_text(impl_->stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

Status Statement::bind_blob(int index, const void* data, size_t size) {
    if (!valid()) {
        return Status::misuse;
    }
    int rc = sqlite3_bind_blob(impl_->stmt, index, data, static_cast<int>(size), SQLITE_TRANSIENT);
    impl_->error = is_ok(rc) ? std::string() : std::string(sqlite3_errmsg(impl_->db));
    return to_status(rc);
}

StepResult Statement::step() {
    if (!valid()) {
        return StepResult::error;
    }

    int rc = sqlite3_step(impl_->stmt);
    if (is_row(rc)) {
        impl_->error.clear();
        return StepResult::row;
    }
    if (is_done(rc)) {
        impl_->error.clear();
        return StepResult::done;
    }
    if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
        impl_->error = sqlite3_errmsg(impl_->db);
        return StepResult::busy;
    }

    impl_->error = sqlite3_errmsg(impl_->db);
    return StepResult::error;
}

int Statement::column_count() const {
    return valid() ? sqlite3_column_count(impl_->stmt) : 0;
}

std::string Statement::column_name(int col) const {
    if (!valid()) {
        return {};
    }
    const char* name = sqlite3_column_name(impl_->stmt, col);
    return name ? name : "";
}

bool Statement::column_is_null(int col) const {
    return !valid() || sqlite3_column_type(impl_->stmt, col) == SQLITE_NULL;
}

int Statement::column_type(int col) const {
    return valid() ? sqlite3_column_type(impl_->stmt, col) : SQLITE_NULL;
}

int Statement::int_value(int col) const {
    return valid() ? sqlite3_column_int(impl_->stmt, col) : 0;
}

int64_t Statement::int64_value(int col) const {
    return valid() ? sqlite3_column_int64(impl_->stmt, col) : 0;
}

double Statement::double_value(int col) const {
    return valid() ? sqlite3_column_double(impl_->stmt, col) : 0.0;
}

std::string Statement::text(int col) const {
    if (!valid()) {
        return {};
    }
    const auto* txt = sqlite3_column_text(impl_->stmt, col);
    if (!txt) {
        return {};
    }
    const int size = sqlite3_column_bytes(impl_->stmt, col);
    return std::string(reinterpret_cast<const char*>(txt), static_cast<size_t>(size));
}

std::vector<uint8_t> Statement::blob(int col) const {
    if (!valid()) {
        return {};
    }
    const auto* data = static_cast<const uint8_t*>(sqlite3_column_blob(impl_->stmt, col));
    const int size = sqlite3_column_bytes(impl_->stmt, col);
    if (!data || size <= 0) {
        return {};
    }
    return std::vector<uint8_t>(data, data + size);
}

int Statement::bytes(int col) const {
    return valid() ? sqlite3_column_bytes(impl_->stmt, col) : 0;
}

namespace detail {

Statement prepare_statement(void* db_handle, const char* sql, std::string* error) {
    auto* db = reinterpret_cast<sqlite3*>(db_handle);
    if (!db) {
        if (error) {
            *error = "Database not open";
        }
        return Statement();
    }

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (!is_ok(rc)) {
        if (error) {
            *error = sqlite3_errmsg(db);
        }
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return Statement();
    }

    auto impl = std::make_unique<Statement::Impl>();
    impl->db = db;
    impl->stmt = stmt;
    if (error) {
        error->clear();
    }
    return Statement(std::move(impl));
}

} // namespace detail

} // namespace xsql
