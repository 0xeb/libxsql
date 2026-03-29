// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>

#include <sqlite3.h>

#include <chrono>

namespace xsql {

struct Database::Impl {
    sqlite3* db = nullptr;
    std::string last_error;
};

namespace {

const std::string& empty_error() {
    static const std::string kEmpty;
    return kEmpty;
}

struct ScalarFnWrapper {
    ScalarFn fn;
};

void scalar_fn_callback(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* wrapper = static_cast<ScalarFnWrapper*>(sqlite3_user_data(ctx));
    if (!wrapper || !wrapper->fn) {
        return;
    }

    FunctionContext fctx(ctx);
    detail::with_args(argc, reinterpret_cast<void* const*>(argv),
        [&](int count, FunctionArg* args) {
            wrapper->fn(fctx, count, args);
        });
}

void destroy_scalar_fn_wrapper(void* ptr) {
    delete static_cast<ScalarFnWrapper*>(ptr);
}

} // namespace

Database::Database()
    : impl_(std::make_unique<Impl>()) {
    open(":memory:");
}

Database::Database(const char* path)
    : impl_(std::make_unique<Impl>()) {
    open(path);
}

Database::~Database() {
    close();
}

Database::Database(Database&& other) noexcept = default;
Database& Database::operator=(Database&& other) noexcept = default;

bool Database::open(const char* path) {
    close();
    int rc = sqlite3_open(path, &impl_->db);
    if (!is_ok(rc)) {
        impl_->last_error = impl_->db ? sqlite3_errmsg(impl_->db) : "Failed to allocate database";
        if (impl_->db) {
            sqlite3_close(impl_->db);
            impl_->db = nullptr;
        }
        return false;
    }
    impl_->last_error.clear();
    return true;
}

void Database::close() {
    if (impl_ && impl_->db) {
        sqlite3_close(impl_->db);
        impl_->db = nullptr;
    }
}

bool Database::is_open() const {
    return impl_ && impl_->db != nullptr;
}

bool Database::register_table(const VTableDef& def) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return false;
    }
    return xsql::register_vtable(*this, def.name.c_str(), &def);
}

bool Database::register_table(const char* module_name, const VTableDef* def) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return false;
    }
    return xsql::register_vtable(*this, module_name, def);
}

bool Database::create_table(const char* table_name, const char* module_name) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return false;
    }
    return xsql::create_vtable(*this, table_name, module_name);
}

bool Database::register_and_create_table(const VTableDef& def) {
    return register_table(def) && create_table(def.name.c_str(), def.name.c_str());
}

bool Database::register_and_create_table(const VTableDef& def, const char* table_name) {
    return register_table(def) && create_table(table_name, def.name.c_str());
}

Status Database::register_function(const char* name, int argc, ScalarFn fn) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return Status::error;
    }

    auto* wrapper = new ScalarFnWrapper{std::move(fn)};
    int rc = sqlite3_create_function_v2(
        impl_->db,
        name,
        argc,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        wrapper,
        scalar_fn_callback,
        nullptr,
        nullptr,
        destroy_scalar_fn_wrapper);
    if (!is_ok(rc)) {
        delete wrapper;
        impl_->last_error = sqlite3_errmsg(impl_->db);
    } else {
        impl_->last_error.clear();
    }
    return to_status(rc);
}

Statement Database::prepare_statement(const char* sql) {
    std::string error;
    auto stmt = detail::prepare_statement(native_handle_unsafe(), sql, &error);
    impl_->last_error = error;
    return stmt;
}

Statement Database::prepare_statement(const std::string& sql) {
    return prepare_statement(sql.c_str());
}

bool Database::is_readonly_statement(const char* sql) {
    auto stmt = prepare_statement(sql);
    return stmt.valid() && stmt.is_readonly();
}

bool Database::is_readonly_statement(const std::string& sql) {
    return is_readonly_statement(sql.c_str());
}

Result Database::query(const char* sql) {
    return query(sql, QueryOptions{});
}

Result Database::query(const char* sql, const QueryOptions& options) {
    Result result;
    if (!is_open()) {
        result.error = "Database not open";
        return result;
    }

    auto stmt = prepare_statement(sql);
    if (!stmt.valid()) {
        result.error = stmt.error().empty() ? impl_->last_error : stmt.error();
        return result;
    }

    struct TimeoutState {
        std::chrono::steady_clock::time_point started_at{};
        int timeout_ms = 0;
        bool timed_out = false;
    };

    struct ProgressHandler {
        static int callback(void* user_data) {
            auto* state = static_cast<TimeoutState*>(user_data);
            if (!state || state->timeout_ms <= 0) {
                return 0;
            }
            const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - state->started_at).count();
            if (elapsed_ms >= state->timeout_ms) {
                state->timed_out = true;
                return 1;
            }
            return 0;
        }
    };

    TimeoutState timeout_state;
    const bool timeout_enabled = options.timeout_ms > 0;
    if (timeout_enabled) {
        timeout_state.started_at = std::chrono::steady_clock::now();
        timeout_state.timeout_ms = options.timeout_ms;
        const int progress_steps = options.progress_steps > 0 ? options.progress_steps : 1000;
        sqlite3_progress_handler(impl_->db, progress_steps, &ProgressHandler::callback, &timeout_state);
    }

    const auto query_started_at = std::chrono::steady_clock::now();
    const int col_count = stmt.column_count();
    result.columns.reserve(static_cast<size_t>(col_count));
    for (int i = 0; i < col_count; ++i) {
        result.columns.push_back(stmt.column_name(i));
    }

    while (true) {
        StepResult step = stmt.step();
        if (step == StepResult::row) {
            Row row;
            row.values.reserve(static_cast<size_t>(col_count));
            for (int i = 0; i < col_count; ++i) {
                row.values.push_back(stmt.column_is_null(i) ? "" : stmt.text(i));
            }
            result.rows.push_back(std::move(row));
            continue;
        }

        result.elapsed_ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - query_started_at).count());

        if (timeout_enabled) {
            sqlite3_progress_handler(impl_->db, 0, nullptr, nullptr);
        }

        const bool timed_out = timeout_enabled && timeout_state.timed_out;
        if (timed_out) {
            result.timed_out = true;
            if (col_count > 0) {
                result.partial = true;
                result.warnings.push_back("query timed out; returning partial rows");
            } else {
                result.error = "Query timed out";
            }
            break;
        }

        if (step == StepResult::done) {
            break;
        }

        result.error = stmt.error().empty() ? sqlite3_errmsg(impl_->db) : stmt.error();
        break;
    }

    return result;
}

Result Database::query(const std::string& sql) {
    return query(sql.c_str());
}

Result Database::query(const std::string& sql, const QueryOptions& options) {
    return query(sql.c_str(), options);
}

std::string Database::scalar(const char* sql) {
    auto result = query(sql);
    if (result.ok() && !result.empty()) {
        return result[0][0];
    }
    return "";
}

std::string Database::scalar(const std::string& sql) {
    return scalar(sql.c_str());
}

Status Database::exec(const char* sql) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return Status::error;
    }

    char* err = nullptr;
    int rc = sqlite3_exec(impl_->db, sql, nullptr, nullptr, &err);
    if (err) {
        impl_->last_error = err;
        sqlite3_free(err);
    } else if (!is_ok(rc)) {
        impl_->last_error = sqlite3_errmsg(impl_->db);
    } else {
        impl_->last_error.clear();
    }
    return to_status(rc);
}

Status Database::exec(const std::string& sql) {
    return exec(sql.c_str());
}

int Database::exec(const char* sql, int (*callback)(void*, int, char**, char**), void* data) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return to_sqlite_status(Status::error);
    }

    char* err = nullptr;
    int rc = sqlite3_exec(impl_->db, sql, callback, data, &err);
    if (err) {
        impl_->last_error = err;
        sqlite3_free(err);
    } else if (!is_ok(rc)) {
        impl_->last_error = sqlite3_errmsg(impl_->db);
    } else {
        impl_->last_error.clear();
    }
    return rc;
}

bool Database::execute_script(const std::string& script,
                              std::vector<StatementResult>& results,
                              std::string& error) {
    bool ok = xsql::execute_script(*this, script, results, error);
    impl_->last_error = ok ? std::string() : error;
    return ok;
}

bool Database::export_tables(const std::vector<std::string>& tables,
                             const std::string& output_path,
                             std::string& error) {
    bool ok = xsql::export_tables(*this, tables, output_path, error);
    impl_->last_error = ok ? std::string() : error;
    return ok;
}

const std::string& Database::last_error() const {
    return impl_ ? impl_->last_error : empty_error();
}

int64_t Database::last_insert_rowid() const {
    return is_open() ? sqlite3_last_insert_rowid(impl_->db) : 0;
}

int Database::changes() const {
    return is_open() ? sqlite3_changes(impl_->db) : 0;
}

void* Database::native_handle_unsafe() const {
    return impl_ ? impl_->db : nullptr;
}

} // namespace xsql
