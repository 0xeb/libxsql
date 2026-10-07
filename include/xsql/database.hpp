// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

/**
 * xsql/database.hpp - RAII SQLite database wrapper with query helpers
 */

#pragma once

#include "functions.hpp"
#include "script.hpp"
#include "statement.hpp"
#include "status.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

struct sqlite3;

namespace xsql {

struct VTableDef;
template<typename RowData>
struct CachedTableDef;
template<typename RowData>
struct GeneratorTableDef;

class Database;
struct ScriptOptions;

namespace detail {

// Concrete table associations follow successful SQLite module lifecycle
// callbacks. Keeping the implementation in the compiled layer avoids exposing
// the authorizer registry in this public header.
void write_surface_connected(sqlite3* db, const char* module_name,
                             const char* schema_name, const char* table_name);
void write_surface_destroyed(sqlite3* db, const char* schema_name,
                             const char* table_name);

} // namespace detail

inline void stream_database_script_json(
    Database& db,
    const std::string& script,
    const ScriptOptions& options,
    const std::function<bool(const char*, std::size_t)>& sink);
inline void stream_database_script_ndjson(
    Database& db,
    const std::string& script,
    const ScriptOptions& options,
    const std::function<bool(const char*, std::size_t)>& sink);

bool register_vtable(Database& db, const char* module_name, const VTableDef* def);
bool create_vtable(Database& db, const char* table_name, const char* module_name);

template<typename RowData>
bool register_cached_vtable(Database& db,
                            const char* module_name,
                            const CachedTableDef<RowData>* def);

template<typename RowData>
bool register_generator_vtable(Database& db,
                               const char* module_name,
                               const GeneratorTableDef<RowData>* def);

struct Row {
    std::vector<std::string> values;
    // Per-cell SQL-NULL flags, parallel to `values` (nonzero => the cell was SQL
    // NULL). May be left empty by legacy producers; consumers must treat an empty
    // `nulls` vector as "nullness unknown" and fall back to their prior behavior.
    // This lets a genuine text value (even the literal string "NULL" or "") be
    // distinguished from a real SQL NULL on every output path.
    std::vector<char> nulls;
    // Per-cell SQLite storage class, parallel to `values`: SQLITE_INTEGER (1),
    // SQLITE_FLOAT (2), SQLITE_TEXT (3), SQLITE_BLOB (4) or SQLITE_NULL (5),
    // read before the cell is converted to text. Database::query fills it; other
    // producers may leave it empty, and type() then answers 0 ("unknown"). Lets an
    // embedder hand an integer back as an integer, not as its decimal text.
    std::vector<char> types;

    const std::string& operator[](size_t i) const { return values[i]; }
    std::string& operator[](size_t i) { return values[i]; }
    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }
    bool is_null(size_t i) const { return i < nulls.size() && nulls[i] != 0; }
    int type(size_t i) const { return i < types.size() ? types[i] : 0; }
};

// Flags for Database::register_function.
enum class FunctionFlags : unsigned {
    none = 0,
    // The function always returns the same result for the same arguments and has
    // no side effects, so SQLite may evaluate a constant-argument call once and
    // reuse the value (and allow the function in index expressions).
    deterministic = 1u << 0,
};

constexpr FunctionFlags operator|(FunctionFlags a, FunctionFlags b) {
    return static_cast<FunctionFlags>(static_cast<unsigned>(a) | static_cast<unsigned>(b));
}

constexpr bool has_flag(FunctionFlags flags, FunctionFlags flag) {
    return (static_cast<unsigned>(flags) & static_cast<unsigned>(flag)) != 0;
}

// One bound statement parameter for Database::query(sql, params, options). SQL
// names its placeholders `?`, `?NNN`, `:name`, `@name` or `$name`; params bind by
// position from index 1. Text binds by length, so an embedded NUL is kept.
using QueryParam = std::variant<std::nullptr_t, int64_t, double, std::string,
                                std::vector<uint8_t>>;

struct QueryOptions {
    int timeout_ms = 0;
    int progress_steps = 1000;
    // Optional cooperative cancellation predicate (see ScriptOptions::should_cancel).
    // When set and it returns true, the query stops ASAP. A read-only,
    // result-bearing statement keeps rows already gathered (partial=true + a
    // "query cancelled" warning); with zero rows gathered it is an error instead
    // (an empty "partial" would read as a valid truncation). A mutation, including
    // DML with RETURNING, is aborted through sqlite3_interrupt so SQLite rolls the
    // statement back, and reports an error; if it reaches completion before the
    // abort lands it has committed, and the honest committed result is returned.
    // Works even when timeout_ms==0.
    std::function<bool()> should_cancel;
};

struct Result {
    std::vector<std::string> columns;
    std::vector<Row> rows;
    std::string error;
    std::vector<std::string> warnings;
    bool timed_out = false;
    // Set only when `rows` is a valid non-empty prefix of the full result set
    // (cancel / timeout / mid-scan failure on a read-only statement with at least
    // one row gathered). Never set on an empty result: partial && rows.empty()
    // does not occur, so an empty successful result is always complete.
    bool partial = false;
    int elapsed_ms = 0;

    bool ok() const { return error.empty(); }
    size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }

    const Row& operator[](size_t i) const { return rows[i]; }

    auto begin() { return rows.begin(); }
    auto end() { return rows.end(); }
    auto begin() const { return rows.begin(); }
    auto end() const { return rows.end(); }
};

class Database {
public:
    Database();
    explicit Database(const char* path);
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    Database(Database&& other) noexcept;
    Database& operator=(Database&& other) noexcept;

    bool open(const char* path = ":memory:");
    void close();
    bool is_open() const;

    bool register_table(const VTableDef& def);
    bool register_table(const char* module_name, const VTableDef* def);
    bool create_table(const char* table_name, const char* module_name);
    bool register_and_create_table(const VTableDef& def);
    bool register_and_create_table(const VTableDef& def, const char* table_name);

    template<typename... Defs>
    bool register_and_create_tables(Defs&... defs) {
        return (register_and_create_table(defs) && ...);
    }

    template<typename RowData>
    bool register_cached_table(const CachedTableDef<RowData>& def) {
        return xsql::register_cached_vtable(*this, def.name.c_str(), &def);
    }

    template<typename RowData>
    bool register_cached_table(const char* module_name, const CachedTableDef<RowData>* def) {
        return xsql::register_cached_vtable(*this, module_name, def);
    }

    template<typename RowData>
    bool register_and_create_cached_table(const CachedTableDef<RowData>& def) {
        return register_cached_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    template<typename RowData>
    bool register_and_create_cached_table(const CachedTableDef<RowData>& def, const char* table_name) {
        return register_cached_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    template<typename RowData>
    bool register_generator_table(const GeneratorTableDef<RowData>& def) {
        return xsql::register_generator_vtable(*this, def.name.c_str(), &def);
    }

    template<typename RowData>
    bool register_generator_table(const char* module_name, const GeneratorTableDef<RowData>* def) {
        return xsql::register_generator_vtable(*this, module_name, def);
    }

    template<typename RowData>
    bool register_and_create_generator_table(const GeneratorTableDef<RowData>& def) {
        return register_generator_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    template<typename RowData>
    bool register_and_create_generator_table(const GeneratorTableDef<RowData>& def, const char* table_name) {
        return register_generator_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    // Register a scalar SQL function. By default it is NOT deterministic, which is
    // SQLite's own default: every call runs. That is required for a function with
    // side effects or whose result follows state that can change between calls
    // (writes, undo/redo, script execution, clocks, random values), because SQLite
    // may evaluate a deterministic constant-argument call once and reuse the value.
    // A pure function opts in with FunctionFlags::deterministic.
    Status register_function(const char* name, int argc, ScalarFn fn,
                             FunctionFlags flags = FunctionFlags::none);

    // Invalidate every cached table registered on this connection, as a write
    // through each would (see xsql/cache_registry.hpp). For a caller that
    // mutates the engine outside SQL; SQL functions use
    // FunctionContext::invalidate_cached_tables().
    void invalidate_cached_tables();

    // Register a custom aggregate function. step() is invoked once per input
    // row; final() is invoked once per aggregation to produce the result.
    // For per-aggregation state, use AggregateContext::state_ptr().
    Status register_aggregate(const char* name, int argc,
                              AggregateStepFn step, AggregateFinalFn final);

    Statement prepare_statement(const char* sql);
    Statement prepare_statement(const std::string& sql);
    bool is_readonly_statement(const char* sql);
    bool is_readonly_statement(const std::string& sql);

    Result query(const char* sql);
    Result query(const char* sql, const QueryOptions& options);
    Result query(const std::string& sql);
    Result query(const std::string& sql, const QueryOptions& options);
    // As query(sql, options), with `params` bound to the statement's placeholders
    // first: same timeout, cancellation and partial-result rules. The number of
    // params must equal the statement's parameter count, else the result carries
    // an error and the statement does not run.
    Result query(const char* sql, const std::vector<QueryParam>& params,
                 const QueryOptions& options);
    Result query(const std::string& sql, const std::vector<QueryParam>& params,
                 const QueryOptions& options);

    std::string scalar(const char* sql);
    std::string scalar(const std::string& sql);

    Status exec(const char* sql);
    Status exec(const std::string& sql);
    int exec(const char* sql, int (*callback)(void*, int, char**, char**), void* data);

    bool execute_script(const std::string& script,
                        std::vector<StatementResult>& results,
                        std::string& error);

    bool export_tables(const std::vector<std::string>& tables,
                       const std::string& output_path,
                       std::string& error);

    const std::string& last_error() const;
    int64_t last_insert_rowid() const;
    int changes() const;
    // Rows changed by INSERT/UPDATE/DELETE on this connection since it opened
    // (sqlite3_total_changes64), virtual-table writes included.
    int64_t total_changes() const;
    // Row writes accepted by this connection's virtual tables since it opened:
    // every INSERT, UPDATE or DELETE a table's write callback carried out, on
    // tables that count as writes (see the builders' counts_as_write). Temp and
    // ordinary SQLite tables are not included. A later ROLLBACK does not lower
    // it: a host write is not undone by SQLite. Compare two readings to ask "did
    // a host-backed table change since X?".
    uint64_t vtab_write_count() const;

private:
    Result query_impl(const char* sql, const std::vector<QueryParam>* params,
                      const QueryOptions& options);
    struct Impl;
    std::unique_ptr<Impl> impl_;

    void* native_handle_unsafe() const;
    sqlite3* sqlite_handle() const;

    // Record a module's write capabilities. Its xCreate/xConnect callbacks map
    // concrete schema/table names, including raw SQL and reopened databases.
    void record_write_surface(const char* module_name, bool insertable,
                              bool deletable,
                              std::vector<std::string> writable_columns);

    template<typename RowData>
    friend bool register_cached_vtable(Database& db,
                                       const char* module_name,
                                       const CachedTableDef<RowData>* def);

    template<typename RowData>
    friend bool register_generator_vtable(Database& db,
                                          const char* module_name,
                                          const GeneratorTableDef<RowData>* def);

    friend bool register_vtable(Database& db,
                                const char* module_name,
                                const VTableDef* def);
    friend bool create_vtable(Database& db,
                              const char* table_name,
                              const char* module_name);
    friend void stream_database_script_json(
        Database& db,
        const std::string& script,
        const ScriptOptions& options,
        const std::function<bool(const char*, std::size_t)>& sink);
    friend void stream_database_script_ndjson(
        Database& db,
        const std::string& script,
        const ScriptOptions& options,
        const std::function<bool(const char*, std::size_t)>& sink);
    // Reads the SQLite result code to tell a table that requires constraints
    // from a genuine failure.
    friend bool export_tables(Database& db,
                              const std::vector<std::string>& requested_tables,
                              const std::string& output_path,
                              std::string& error);
};

} // namespace xsql
