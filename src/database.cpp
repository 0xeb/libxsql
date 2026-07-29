// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#include <xsql/aggregates.hpp>
#include <xsql/database.hpp>
#include <xsql/vtable.hpp>

#include <sqlite3.h>

#include <chrono>
#include <cctype>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace xsql {

// Write capabilities of a registered table module, consulted by the prepare-time
// write-surface authorizer (unsupported-write-surface handling).
struct WriteCaps {
    bool insertable = false;
    bool deletable = false;
    std::unordered_set<std::string> writable_columns;
};

struct WriteSurfaceRegistry {
    std::mutex mutex;
    std::unordered_map<std::string, WriteCaps> module_caps;
    std::unordered_map<std::string, WriteCaps> table_caps;
    std::string preparing_drop;
};

struct Database::Impl {
    sqlite3* db = nullptr;
    std::string last_error;
    WriteSurfaceRegistry write_surfaces;
};

namespace {

const std::string& empty_error() {
    static const std::string kEmpty;
    return kEmpty;
}

std::string write_surface_key(const char* db_name, const char* table_name) {
    std::string key = db_name && *db_name ? db_name : "main";
    for (char& ch : key) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    key.push_back('\x1f');
    if (table_name) {
        for (const char* p = table_name; *p; ++p) {
            key.push_back(static_cast<char>(
                std::tolower(static_cast<unsigned char>(*p))));
        }
    }
    return key;
}

std::string normalized_identifier(const char* value) {
    std::string out;
    if (!value) return out;
    for (const char* p = value; *p; ++p) {
        out.push_back(static_cast<char>(
            std::tolower(static_cast<unsigned char>(*p))));
    }
    return out;
}

// Intentionally leaked, function-local statics: an engine-lifetime Database
// (a process-global singleton in several consumer tools) can be destroyed
// during static teardown, after ordinary namespace-scope statics in this TU
// would already be gone — destruction-order UB. Magic statics make first use
// thread-safe; leaking makes last use always safe.
std::mutex& registry_index_mutex() {
    static auto* m = new std::mutex();
    return *m;
}

std::unordered_map<sqlite3*, WriteSurfaceRegistry*>& registry_by_connection() {
    static auto* map = new std::unordered_map<sqlite3*, WriteSurfaceRegistry*>();
    return *map;
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

struct AggregateFnWrapper {
    AggregateStepFn step;
    AggregateFinalFn final;
};

void aggregate_step_callback(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* wrapper = static_cast<AggregateFnWrapper*>(sqlite3_user_data(ctx));
    if (!wrapper || !wrapper->step) {
        return;
    }

    AggregateContext actx(ctx);
    detail::with_args(argc, reinterpret_cast<void* const*>(argv),
        [&](int count, FunctionArg* args) {
            wrapper->step(actx, count, args);
        });
}

void aggregate_final_callback(sqlite3_context* ctx) {
    auto* wrapper = static_cast<AggregateFnWrapper*>(sqlite3_user_data(ctx));
    if (!wrapper || !wrapper->final) {
        return;
    }

    AggregateContext actx(ctx);
    wrapper->final(actx);
}

void destroy_aggregate_fn_wrapper(void* ptr) {
    delete static_cast<AggregateFnWrapper*>(ptr);
}

// Prepare-time write-surface authorizer (unsupported-write-surface handling). Fires once per
// DML action at prepare time, INDEPENDENT of how many rows match — so a 0-row
// DELETE/UPDATE on an unwritable surface is denied consistently instead of
// silently "succeeding" (xUpdate is never called when nothing matches). The
// INSERT/DELETE and table-level UPDATE messages match detail::unsupported_* so
// the matched-row path and this 0-row path report the same actionable,
// capability-scoped error; the column-scoped UPDATE denial additionally names
// the offending column (which only prepare time knows).
int write_surface_authorizer(void* pArg, int action, const char* a1,
                             const char* a2, const char* db_name,
                             const char* /*trigger*/) {
    auto* surfaces = static_cast<WriteSurfaceRegistry*>(pArg);
    if (!surfaces || !a1) return SQLITE_OK;

    std::lock_guard<std::mutex> lock(surfaces->mutex);
    const std::string target = write_surface_key(db_name, a1);
    if (action == SQLITE_DROP_VTABLE || action == SQLITE_DROP_TABLE) {
        surfaces->preparing_drop =
            surfaces->table_caps.count(target) != 0 ? target : std::string{};
        return SQLITE_OK;
    }
    // SQLite authorizes an internal DELETE from the virtual table while
    // compiling DROP TABLE. Permit that one companion action without retiring
    // the capability; xDestroy performs retirement only when DROP executes.
    if (action == SQLITE_DELETE && surfaces->preparing_drop == target) {
        surfaces->preparing_drop.clear();
        return SQLITE_OK;
    }
    surfaces->preparing_drop.clear();

    if (action != SQLITE_INSERT && action != SQLITE_DELETE && action != SQLITE_UPDATE) {
        return SQLITE_OK;
    }
    auto it = surfaces->table_caps.find(target);
    if (it == surfaces->table_caps.end()) return SQLITE_OK;   // not one of our vtables
    const WriteCaps& caps = it->second;
    const std::string table(a1);
    std::string phrase;
    const char* leaf = nullptr;
    switch (action) {
        case SQLITE_INSERT:
            if (caps.insertable) return SQLITE_OK;
            phrase = "INSERT INTO " + table + " is not supported";
            leaf = "insert";
            break;
        case SQLITE_DELETE:
            if (caps.deletable) return SQLITE_OK;
            phrase = "DELETE FROM " + table + " is not supported";
            leaf = "delete";
            break;
        case SQLITE_UPDATE:
            if (a2 && caps.writable_columns.count(normalized_identifier(a2)) != 0) {
                return SQLITE_OK;
            }
            // A table with NO writable columns at all (e.g. a generator without
            // row_lookup) is a table-level capability gap: "column X is
            // read-only" would misattribute it to a per-column flag when no
            // UPDATE of any column can work.
            phrase = (a2 && !caps.writable_columns.empty())
                ? "column \"" + std::string(a2) + "\" is read-only"
                : "UPDATE " + table + " is not supported";
            leaf = (a2 && !caps.writable_columns.empty()) ? a2 : "*";
            break;
        default:
            return SQLITE_OK;
    }
    detail::set_authorizer_denial(
        phrase + " (capability mutation." + table + "." + leaf + " is unavailable)");
    return SQLITE_DENY;
}

} // namespace

namespace detail {

void write_surface_connected(sqlite3* db, const char* module_name,
                             const char* schema_name, const char* table_name) {
    if (!db || !module_name || !table_name || !*table_name) return;
    std::lock_guard<std::mutex> index_lock(registry_index_mutex());
    auto registry_it = registry_by_connection().find(db);
    if (registry_it == registry_by_connection().end()) return;
    auto* registry = registry_it->second;
    std::lock_guard<std::mutex> registry_lock(registry->mutex);
    auto caps_it =
        registry->module_caps.find(normalized_identifier(module_name));
    if (caps_it == registry->module_caps.end()) return;
    registry->table_caps[write_surface_key(schema_name, table_name)] =
        caps_it->second;
}

void write_surface_destroyed(sqlite3* db, const char* schema_name,
                             const char* table_name) {
    if (!db || !table_name || !*table_name) return;
    std::lock_guard<std::mutex> index_lock(registry_index_mutex());
    auto registry_it = registry_by_connection().find(db);
    if (registry_it == registry_by_connection().end()) return;
    auto* registry = registry_it->second;
    std::lock_guard<std::mutex> registry_lock(registry->mutex);
    registry->table_caps.erase(write_surface_key(schema_name, table_name));
}

} // namespace detail

void** AggregateContext::state_ptr() {
    if (!ctx_) return nullptr;
    return static_cast<void**>(sqlite3_aggregate_context(
        static_cast<sqlite3_context*>(ctx_), sizeof(void*)));
}

void AggregateContext::result_blob(const void* data, size_t len) {
    if (!ctx_) return;
    sqlite3_result_blob(static_cast<sqlite3_context*>(ctx_), data,
                        static_cast<int>(len), SQLITE_TRANSIENT);
}

void AggregateContext::result_null() {
    if (!ctx_) return;
    sqlite3_result_null(static_cast<sqlite3_context*>(ctx_));
}

void AggregateContext::result_error(const std::string& msg) {
    if (!ctx_) return;
    sqlite3_result_error(static_cast<sqlite3_context*>(ctx_), msg.c_str(),
                         static_cast<int>(msg.size()));
}

void AggregateContext::result_error(const char* msg) {
    if (!ctx_) return;
    sqlite3_result_error(static_cast<sqlite3_context*>(ctx_), msg ? msg : "", -1);
}

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

// Moving only transfers the Impl pointer. The Impl itself stays put on the heap,
// so the authorizer's pArg and the write-surface registry entry (both of which
// hold &impl_->write_surfaces) remain valid without re-registration.
Database::Database(Database&& other) noexcept = default;

// Move-assignment must close the connection it is about to drop. A defaulted
// operator would just delete the old Impl, leaking its sqlite3 handle and
// stranding that handle's entry in the process-wide write-surface registry --
// which is keyed by sqlite3* and only ever erased by close().
Database& Database::operator=(Database&& other) noexcept {
    if (this != &other) {
        close();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

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
    {
        std::lock_guard<std::mutex> lock(impl_->write_surfaces.mutex);
        impl_->write_surfaces.module_caps.clear();
        impl_->write_surfaces.table_caps.clear();
        impl_->write_surfaces.preparing_drop.clear();
    }
    {
        std::lock_guard<std::mutex> lock(registry_index_mutex());
        registry_by_connection()[impl_->db] = &impl_->write_surfaces;
    }
    // Install the write-surface authorizer. It reads the registry live at
    // prepare time (populated as tables are registered/created below), so a write
    // to an unsupported surface is denied at prepare — including the 0-row case.
    sqlite3_set_authorizer(impl_->db, write_surface_authorizer,
                           &impl_->write_surfaces);
    register_builtin_aggregates(*this);
    return true;
}

void Database::close() {
    if (impl_ && impl_->db) {
        {
            std::lock_guard<std::mutex> lock(registry_index_mutex());
            registry_by_connection().erase(impl_->db);
        }
        // close_v2, not close: with an unfinalized statement or unfinished
        // backup, sqlite3_close returns SQLITE_BUSY and leaves the connection
        // open — nulling the handle then leaked it permanently, with the live
        // authorizer still pointing at an Impl about to be freed (a
        // use-after-free on the next prepare). close_v2 marks the connection
        // zombie and finishes the close when the last statement is finalized.
        sqlite3_close_v2(impl_->db);
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
    if (!xsql::create_vtable(*this, table_name, module_name)) {
        return false;
    }
    return true;
}

void Database::record_write_surface(const char* module_name, bool insertable,
                                    bool deletable,
                                    std::vector<std::string> writable_columns) {
    if (!impl_ || !module_name) return;
    WriteCaps caps;
    caps.insertable = insertable;
    caps.deletable = deletable;
    for (const auto& column : writable_columns) {
        caps.writable_columns.insert(normalized_identifier(column.c_str()));
    }
    std::lock_guard<std::mutex> lock(impl_->write_surfaces.mutex);
    impl_->write_surfaces.module_caps[normalized_identifier(module_name)] =
        std::move(caps);
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
        // Do NOT delete `wrapper` here: sqlite3_create_function_v2 invokes the
        // xDestroy callback (destroy_scalar_fn_wrapper) even when registration
        // fails, which already deletes the wrapper. A manual delete would be a
        // double-free. (See SQLite docs: the destructor "is also invoked if the
        // call to sqlite3_create_function_v2() fails".)
        impl_->last_error = sqlite3_errmsg(impl_->db);
    } else {
        impl_->last_error.clear();
    }
    return to_status(rc);
}

Status Database::register_aggregate(const char* name, int argc,
                                    AggregateStepFn step, AggregateFinalFn final) {
    if (!is_open()) {
        impl_->last_error = "Database not open";
        return Status::error;
    }

    auto* wrapper = new AggregateFnWrapper{std::move(step), std::move(final)};
    int rc = sqlite3_create_function_v2(
        impl_->db,
        name,
        argc,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        wrapper,
        nullptr,
        aggregate_step_callback,
        aggregate_final_callback,
        destroy_aggregate_fn_wrapper);
    if (!is_ok(rc)) {
        // Do NOT delete `wrapper` here: sqlite3_create_function_v2 invokes the
        // xDestroy callback (destroy_aggregate_fn_wrapper) even when registration
        // fails, which already deletes the wrapper. A manual delete would be a
        // double-free. (See SQLite docs: the destructor "is also invoked if the
        // call to sqlite3_create_function_v2() fails".)
        impl_->last_error = sqlite3_errmsg(impl_->db);
    } else {
        impl_->last_error.clear();
    }
    return to_status(rc);
}

Statement Database::prepare_statement(const char* sql) {
    // A DROP prepare abandoned before its companion internal DELETE arrives
    // leaves the one-slot authorizer latch armed, and the next unrelated
    // DELETE on the same table would be waved through without a capability
    // check. Every new prepare starts with a clean latch.
    if (impl_) {
        std::lock_guard<std::mutex> lock(impl_->write_surfaces.mutex);
        impl_->write_surfaces.preparing_drop.clear();
    }
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
    const auto query_started_at = std::chrono::steady_clock::now();

    struct TimeoutState {
        std::chrono::steady_clock::time_point started_at{};
        int timeout_ms = 0;
        bool timed_out = false;
        const std::function<bool()>* cancel = nullptr;  // optional cooperative-cancel predicate
        bool cancelled = false;

        std::string cancel_error;

        bool poll_cancel() noexcept {
            if (!cancel_error.empty() || cancelled) {
                return true;
            }
            if (!cancel || !*cancel) {
                return false;
            }
            try {
                cancelled = (*cancel)();
            } catch (const std::exception& e) {
                cancel_error = std::string("cancellation predicate threw: ") + e.what();
                return true;
            } catch (...) {
                cancel_error = "cancellation predicate threw a non-standard exception";
                return true;
            }
            return cancelled;
        }

        bool poll_interrupt() noexcept {
            if (poll_cancel()) {
                return true;
            }
            if (timeout_ms <= 0) {
                return false;
            }
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - started_at)
                    .count();
            if (elapsed_ms >= timeout_ms) {
                timed_out = true;
                return true;
            }
            return false;
        }
    };

    struct ProgressHandler {
        static int callback(void* user_data) {
            auto* state = static_cast<TimeoutState*>(user_data);
            if (!state) {
                return 0;
            }
            return state->poll_interrupt() ? 1 : 0;
        }
    };

    TimeoutState timeout_state;
    timeout_state.started_at = query_started_at;
    timeout_state.timeout_ms = options.timeout_ms;
    timeout_state.cancel = options.should_cancel ? &options.should_cancel : nullptr;
    const bool timeout_enabled = options.timeout_ms > 0;
    const bool cancel_enabled = static_cast<bool>(options.should_cancel);
    // The progress-handler + interrupt-checker machinery is needed whenever EITHER a
    // deadline OR a cancel predicate is in play (cancel must work under timeout_ms==0).
    const bool guard_enabled = timeout_enabled || cancel_enabled;

    // Arm before prepare so xBestIndex and other prepare-time callbacks consume
    // the same per-statement deadline as sqlite3_step(). Both guards restore an
    // outer libxsql scope rather than clearing it.
    std::optional<ScopedProgressHandler> progress_guard;
    std::optional<ScopedInterruptChecker> checker_guard;
    if (guard_enabled) {
        const int progress_steps = options.progress_steps > 0 ? options.progress_steps : 1000;
        progress_guard.emplace(
            impl_->db, progress_steps, &ProgressHandler::callback,
            &timeout_state);

        // Interrupt checker for long C++ virtual-table loops: fire on the deadline
        // (when one is set) OR when the cancel predicate says so.
        checker_guard.emplace([&timeout_state]() {
            return timeout_state.poll_interrupt();
        });
    }

    auto stmt = prepare_statement(sql);
    if (!stmt.valid()) {
        result.elapsed_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - query_started_at)
                .count());
        if (!timeout_state.cancel_error.empty()) {
            result.error = timeout_state.cancel_error;
        } else if (timeout_state.cancelled) {
            result.error = "Query cancelled";
        } else if (timeout_enabled && timeout_state.timed_out) {
            result.timed_out = true;
            result.error = "Query timed out";
        } else {
            result.error =
                stmt.error().empty() ? impl_->last_error : stmt.error();
        }
        return result;
    }

    const int col_count = stmt.column_count();
    const bool partial_capable = col_count > 0 && stmt.is_readonly();
    result.columns.reserve(static_cast<size_t>(col_count));
    for (int i = 0; i < col_count; ++i) {
        result.columns.push_back(stmt.column_name(i));
    }

    auto classify_interrupt = [&]() {
        if (!timeout_state.cancel_error.empty()) {
            result.rows.clear();
            result.partial = false;
            result.timed_out = false;
            result.warnings.clear();
            result.error = timeout_state.cancel_error;
            return true;
        }
        // With NO rows gathered there is no prefix to keep: an empty "partial"
        // result would read as a valid truncation of the real one, so a zero-row
        // cancel/timeout is an error (same rule as the step-failure branch below).
        if (timeout_state.cancelled) {
            if (partial_capable && !result.rows.empty()) {
                result.partial = true;
                result.warnings.push_back(
                    "query cancelled; returning partial rows");
            } else {
                result.rows.clear();
                result.error = "Query cancelled";
            }
            return true;
        }
        if (timeout_enabled && timeout_state.timed_out) {
            result.timed_out = true;
            if (partial_capable && !result.rows.empty()) {
                result.partial = true;
                result.warnings.push_back(
                    "query timed out; returning partial rows");
            } else {
                result.rows.clear();
                result.error = "Query timed out";
            }
            return true;
        }
        return false;
    };

    const bool mutating = !stmt.is_readonly();
    bool interrupt_sent = false;
    while (true) {
        // Poll before the first step (and every later step). An already-cancelled
        // short mutation must never execute and commit before SQLite's progress
        // callback gets a chance to run.
        if (guard_enabled && timeout_state.poll_interrupt()) {
            // A mutation whose provisional RETURNING rows are already in flight
            // must NOT be abandoned here: finalizing a non-errored statement
            // COMPLETES it, and SQLite commits the DML. Ask SQLite to abort
            // instead — that path rolls the statement back — and keep stepping
            // until the abort surfaces as a step error.
            if (mutating && !result.rows.empty()) {
                if (!interrupt_sent) {
                    sqlite3_interrupt(impl_->db);
                    interrupt_sent = true;
                }
            } else {
                result.elapsed_ms = static_cast<int>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - query_started_at)
                        .count());
                classify_interrupt();
                break;
            }
        }

        StepResult step = stmt.step();
        if (step == StepResult::row) {
            Row row;
            row.values.reserve(static_cast<size_t>(col_count));
            row.nulls.reserve(static_cast<size_t>(col_count));
            for (int i = 0; i < col_count; ++i) {
                const bool is_null = stmt.column_is_null(i);
                // Carry the real text even for NULL (empty here) but flag it, so a
                // genuine "" / "NULL" text value stays distinct from SQL NULL.
                row.values.push_back(is_null ? "" : stmt.text(i));
                row.nulls.push_back(is_null ? 1 : 0);
            }
            result.rows.push_back(std::move(row));
            continue;
        }

        result.elapsed_ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - query_started_at).count());

        // SQLITE_DONE for a mutation means it completed and COMMITTED — a
        // cancel/timeout latched during its final step must not relabel the
        // honest result as a failure. A read-only statement can reach DONE with
        // latched state when a cooperative vtable truncated its scan (e.g. a
        // cache_builder bailing on vtab_interrupted), so reads still classify:
        // rows gathered are a partial answer, none is an error.
        if (step == StepResult::done && mutating) {
            break;
        }

        if (classify_interrupt()) break;

        if (step == StepResult::done) {
            break;
        }

        result.error = stmt.error().empty() ? sqlite3_errmsg(impl_->db) : stmt.error();
        // Rows already delivered by a read-only statement stay valid, so a
        // mid-scan failure is a partial answer. With NO rows gathered there is
        // no prefix to keep: reporting partial would tell the caller an empty
        // result set is a valid truncation of the real one. Mirrors the
        // `rows > 0` gate in stream_database_script_json/_ndjson.
        if (partial_capable && !result.rows.empty()) {
            result.partial = true;
            result.warnings.push_back(
                "query failed; returning partial rows");
        } else if (!partial_capable) {
            result.rows.clear();
        }
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

    // sqlite3_exec runs its own prepare internally, so clear the denial slot
    // first and, on an authorizer denial, surface the capability-scoped message
    // rather than SQLite's fixed "not authorized" (unsupported-write-surface handling).
    // Same clean-slate rule as prepare_statement for the DROP companion latch.
    {
        std::lock_guard<std::mutex> lock(impl_->write_surfaces.mutex);
        impl_->write_surfaces.preparing_drop.clear();
    }
    detail::clear_authorizer_denial();
    char* err = nullptr;
    int rc = sqlite3_exec(impl_->db, sql, nullptr, nullptr, &err);
    if (rc == SQLITE_AUTH && !detail::authorizer_denial_message().empty()) {
        impl_->last_error = detail::authorizer_denial_message();
        if (err) sqlite3_free(err);
    } else if (err) {
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

    detail::clear_authorizer_denial();
    char* err = nullptr;
    int rc = sqlite3_exec(impl_->db, sql, callback, data, &err);
    if (rc == SQLITE_AUTH && !detail::authorizer_denial_message().empty()) {
        impl_->last_error = detail::authorizer_denial_message();
        if (err) sqlite3_free(err);
    } else if (err) {
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

sqlite3* Database::sqlite_handle() const {
    return impl_ ? impl_->db : nullptr;
}

} // namespace xsql
