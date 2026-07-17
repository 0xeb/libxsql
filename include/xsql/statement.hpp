// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include "status.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace xsql {

class Statement;

namespace detail {
Statement prepare_statement(void* db_handle, const char* sql, std::string* error);

// Prepare-time authorizer denial message (thread-local). A sqlite3 authorizer
// that denies an unsupported write stashes an actionable, capability-scoped
// message here; prepare_statement substitutes it for SQLite's generic
// "not authorized" text on an SQLITE_AUTH failure. See Database's write-surface
// authorizer (unsupported-write-surface handling).
void set_authorizer_denial(std::string message);
void clear_authorizer_denial();
const std::string& authorizer_denial_message();
}

enum class StepResult {
    row,
    done,
    busy,
    error
};

class Statement {
public:
    Statement();
    ~Statement();

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    Statement(Statement&& other) noexcept;
    Statement& operator=(Statement&& other) noexcept;

    bool valid() const;
    explicit operator bool() const { return valid(); }

    bool is_readonly() const;
    const std::string& error() const;

    Status reset();
    Status clear_bindings();

    Status bind_null(int index);
    Status bind_int(int index, int value);
    Status bind_int64(int index, int64_t value);
    Status bind_double(int index, double value);
    Status bind_text(int index, const std::string& value);
    Status bind_blob(int index, const void* data, size_t size);

    StepResult step();

    int column_count() const;
    std::string column_name(int col) const;
    bool column_is_null(int col) const;
    int column_type(int col) const;
    int int_value(int col) const;
    int64_t int64_value(int col) const;
    double double_value(int col) const;
    std::string text(int col) const;
    std::vector<uint8_t> blob(int col) const;
    int bytes(int col) const;

private:
    struct Impl;
    explicit Statement(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;

    friend class Database;
    friend class QueryRow;
    friend Statement detail::prepare_statement(void* db_handle, const char* sql, std::string* error);
};

} // namespace xsql
