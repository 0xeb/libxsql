// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/**
 * xsql/vtable.hpp - SQLite Virtual Table framework
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Features:
 *   - Declarative column definitions using lambdas
 *   - Live data access (fresh on every query)
 *   - Optional UPDATE/DELETE support via column setters
 *   - before_modify hook for undo/transaction integration
 *   - Fluent builder API
 *   - Constraint pushdown via filter_eq() for O(1) lookups
 *
 * Example (read-only table):
 *
 *   auto def = xsql::table("numbers")
 *       .count([&]() { return data.size(); })
 *       .column_int64("value", [&](size_t i) { return data[i]; })
 *       .build();
 *
 * Example (writable table):
 *
 *   auto def = xsql::table("items")
 *       .count([&]() { return items.size(); })
 *       .on_modify([](const std::string& op) { log(op); })
 *       .column_text_rw("name", getter, setter)
 *       .deletable(delete_fn)
 *       .build();
 *
 * Example (with constraint pushdown):
 *
 *   auto def = xsql::table("xrefs")
 *       .count([&]() { return count_all_xrefs(); })
 *       .column_int64("to_ea", [&](size_t i) { return cache[i].to; })
 *       .filter_eq("to_ea", [](int64_t target) {
 *           return std::make_unique<XrefsToIterator>(target);
 *       }, 10.0)  // estimated cost
 *       .build();
 */

#pragma once

#include "database.hpp"
#include <sqlite3.h>
#include <string>
#include <vector>
#include <functional>
#include <sstream>
#include <cstring>
#include <cctype>
#include <memory>
#include <new>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

namespace xsql {

namespace detail {
template <typename T>
inline T* clone_def(const T* src) {
    if (!src) return nullptr;
    return new (std::nothrow) T(*src);
}

template <typename T>
inline void destroy_def(void* p) {
    delete static_cast<T*>(p);
}

inline sqlite3* sqlite_db(void* db_handle) {
    return reinterpret_cast<sqlite3*>(db_handle);
}

inline bool register_vtable_sqlite(sqlite3* db, const char* module_name, const VTableDef* def);
inline bool create_vtable_sqlite(sqlite3* db, const char* table_name, const char* module_name);

template<typename RowData>
inline bool register_cached_vtable_sqlite(sqlite3* db,
                                          const char* module_name,
                                          const CachedTableDef<RowData>* def);

template<typename RowData>
inline bool register_generator_vtable_sqlite(sqlite3* db,
                                             const char* module_name,
                                             const GeneratorTableDef<RowData>* def);

inline bool register_vtable_opaque(void* db_handle, const char* module_name, const VTableDef* def) {
    return register_vtable_sqlite(sqlite_db(db_handle), module_name, def);
}

inline bool create_vtable_opaque(void* db_handle, const char* table_name, const char* module_name) {
    return create_vtable_sqlite(sqlite_db(db_handle), table_name, module_name);
}

template<typename RowData>
inline bool register_cached_vtable_opaque(void* db_handle,
                                          const char* module_name,
                                          const CachedTableDef<RowData>* def) {
    return register_cached_vtable_sqlite(sqlite_db(db_handle), module_name, def);
}

template<typename RowData>
inline bool register_generator_vtable_opaque(void* db_handle,
                                             const char* module_name,
                                             const GeneratorTableDef<RowData>* def) {
    return register_generator_vtable_sqlite(sqlite_db(db_handle), module_name, def);
}
} // namespace detail

inline thread_local std::string g_vtab_error_message;

inline void clear_vtab_error() {
    g_vtab_error_message.clear();
}

inline void set_vtab_error(std::string message) {
    g_vtab_error_message = std::move(message);
}

inline const std::string& get_vtab_error() {
    return g_vtab_error_message;
}

// ============================================================================ 
// Column Types
// ============================================================================ 

enum class ColumnType {
    Integer,
    Text,
    Real,
    Blob
};

inline const char* column_type_sql(ColumnType t) {
    switch (t) {
        case ColumnType::Integer: return "INTEGER";
        case ColumnType::Text:    return "TEXT";
        case ColumnType::Real:    return "REAL";
        case ColumnType::Blob:    return "BLOB";
    }
    return "TEXT";
}

// ============================================================================
// Column Definition
// ============================================================================

struct ColumnDef {
    std::string name;
    ColumnType type;
    bool writable;

    // Getter: Fetch value at row index
    std::function<void(FunctionContext&, size_t)> get;

    // Setter: Update value at row index (optional, for UPDATE support)
    std::function<bool(size_t, FunctionArg)> set;

    ColumnDef(const char* n, ColumnType t, bool w,
              std::function<void(FunctionContext&, size_t)> getter,
              std::function<bool(size_t, FunctionArg)> setter = nullptr)
        : name(n), type(t), writable(w), get(std::move(getter)), set(std::move(setter)) {}
};

// ============================================================================
// Row Iterator (for constraint pushdown)
// ============================================================================

/**
 * Abstract iterator for filtered table access.
 *
 * Implement this interface to provide optimized iteration for specific
 * constraint patterns (e.g., WHERE to_ea = X uses first_to/next_to API).
 */
struct RowIterator {
    virtual ~RowIterator() = default;

    // Advance to next row. Returns true if there is a row, false if exhausted.
    // Must be called before accessing the first row.
    virtual bool next() = 0;

    // True if iterator is exhausted (no current row)
    virtual bool eof() const = 0;

    // Get column value into FunctionContext
    virtual void column(FunctionContext& ctx, int col) = 0;

    // Get current row's rowid
    virtual int64_t rowid() const = 0;
};

// ============================================================================
// Filter Definition (for constraint pushdown)
// ============================================================================

// Filter ID 0 reserved for "no filter" (full scan)
constexpr int FILTER_NONE = 0;

// Index IDs start at INDEX_BASE (indexes are auto-generated filters)
constexpr int INDEX_BASE = 1000;

/**
 * Defines a filter for a specific column constraint.
 *
 * When SQLite queries with WHERE column = value, xBestIndex checks if
 * we have a filter for that column. If so, xFilter creates the specialized
 * iterator instead of doing a full scan.
 */
struct FilterDef {
    int column_index;           // Which column this filter applies to
    int filter_id;              // Unique ID (passed in idxNum)
    double estimated_cost;      // Cost estimate for query planner
    double estimated_rows;      // Estimated row count

    // Factory: create iterator for the given constraint value
    std::function<std::unique_ptr<RowIterator>(FunctionArg)> create;

    FilterDef(int col, int id, double cost, double rows,
              std::function<std::unique_ptr<RowIterator>(FunctionArg)> factory)
        : column_index(col), filter_id(id), estimated_cost(cost),
          estimated_rows(rows), create(std::move(factory)) {}
};

// ============================================================================
// Virtual Table Definition
// ============================================================================

struct VTableDef {
    std::string name;

    // Row count (called fresh each time for live data)
    std::function<size_t()> row_count;

    // Estimated row count for query planning (should be cheap, optional).
    // If not set, a conservative default is used (planning avoids calling row_count()).
    std::function<size_t()> estimate_rows;

    // Columns
    std::vector<ColumnDef> columns;

    // Filters for constraint pushdown (optional)
    std::vector<FilterDef> filters;

    // DELETE handler: Delete row at index, returns success
    std::function<bool(size_t)> delete_row;
    bool supports_delete = false;

    // INSERT handler: Insert row with column values, returns success
    std::function<bool(int argc, FunctionArg* argv)> insert_row;
    bool supports_insert = false;

    // Hook called before any modification (INSERT/UPDATE/DELETE)
    std::function<void(const std::string&)> before_modify;

    std::string schema() const {
        std::ostringstream ss;
        ss << "CREATE TABLE " << name << "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "\"" << columns[i].name << "\" " << column_type_sql(columns[i].type);
        }
        ss << ")";
        return ss.str();
    }

    // Find column index by name, -1 if not found
    int find_column(const std::string& col_name) const {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == col_name) return static_cast<int>(i);
        }
        return -1;
    }

    // Find filter for given column, nullptr if none
    const FilterDef* find_filter(int col_index) const {
        for (const auto& f : filters) {
            if (f.column_index == col_index) return &f;
        }
        return nullptr;
    }
};

// ============================================================================
// SQLite Virtual Table Implementation
// ============================================================================

struct Vtab {
    sqlite3_vtab base;
    const VTableDef* def;
};

struct Cursor {
    sqlite3_vtab_cursor base;
    const VTableDef* def;

    // Index-based iteration (legacy, when no filter)
    size_t idx = 0;
    size_t total = 0;

    // Iterator-based iteration (when filter applied)
    std::unique_ptr<RowIterator> iter;
    bool using_iterator = false;
    bool iterator_eof = false;
};

// xConnect/xCreate
inline int vtab_connect(sqlite3* db, void* pAux, int, const char* const*,
                        sqlite3_vtab** ppVtab, char**) {
    const VTableDef* def = static_cast<const VTableDef*>(pAux);

    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;

    auto* vtab = new Vtab();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return to_sqlite_status(Status::ok);
}

// xDisconnect/xDestroy
inline int vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<Vtab*>(pVtab);
    return to_sqlite_status(Status::ok);
}

// xOpen
inline int vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    auto* cursor = new Cursor();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->def = vtab->def;
    cursor->idx = 0;
    cursor->total = 0;
    cursor->iter = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    *ppCursor = &cursor->base;
    return to_sqlite_status(Status::ok);
}

// xClose
inline int vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<Cursor*>(pCursor);
    return to_sqlite_status(Status::ok);
}

// xNext
inline int vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    if (cursor->using_iterator && cursor->iter) {
        if (!cursor->iter->next()) {
            cursor->iterator_eof = true;
        }
    } else {
        cursor->idx++;
    }
    return to_sqlite_status(Status::ok);
}

// xEof
inline int vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    if (cursor->using_iterator) {
        if (!cursor->iter || cursor->iterator_eof) return 1;
        return cursor->iter->eof() ? 1 : 0;
    }
    return cursor->idx >= cursor->total ? 1 : 0;
}

// xColumn - fetches live data each time
inline int vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);

    // During UPDATE, SQLite may ask for unchanged column values. Returning
    // without a value marks the column as SQLITE_NOCHANGE in xUpdate.
    if (sqlite3_vtab_nochange(ctx)) {
        return to_sqlite_status(Status::ok);
    }

    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return to_sqlite_status(Status::ok);
    }
    FunctionContext fctx(ctx);
    if (cursor->using_iterator && cursor->iter) {
        if (cursor->iterator_eof) {
            sqlite3_result_null(ctx);
            return to_sqlite_status(Status::ok);
        }
        cursor->iter->column(fctx, col);
    } else {
        cursor->def->columns[col].get(fctx, cursor->idx);
    }
    return to_sqlite_status(Status::ok);
}

// xRowid
inline int vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    if (cursor->using_iterator && cursor->iter) {
        if (cursor->iterator_eof) {
            *pRowid = 0;
            return to_sqlite_status(Status::ok);
        }
        *pRowid = cursor->iter->rowid();
    } else {
        *pRowid = static_cast<sqlite3_int64>(cursor->idx);
    }
    return to_sqlite_status(Status::ok);
}

// xFilter - get fresh count for iteration or create filtered iterator
inline int vtab_filter(sqlite3_vtab_cursor* pCursor, int idxNum, const char*,
                       int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);

    // Reset state
    cursor->iter = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    cursor->idx = 0;
    cursor->total = 0;

    // Check if a filter was selected by xBestIndex
    if (idxNum != FILTER_NONE && argc > 0) {
        // Find the filter with this ID
        for (const auto& filter : cursor->def->filters) {
            if (filter.filter_id == idxNum) {
                // Create the filtered iterator
                cursor->iter = filter.create(FunctionArg(argv[0]));
                cursor->using_iterator = true;
                cursor->iterator_eof = true;
                if (cursor->iter) {
                    cursor->iterator_eof = !cursor->iter->next();
                }
                return to_sqlite_status(Status::ok);
            }
        }
    }

    // No filter - full scan using index-based iteration
    cursor->total = cursor->def->row_count();
    return to_sqlite_status(Status::ok);
}

// xBestIndex - query planner hook for constraint pushdown
inline int vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    const VTableDef* def = vtab->def;

    // Look for constraints we can optimize FIRST (before calling row_count)
    // This avoids expensive cache rebuilds when a filter will be used
    const FilterDef* best_filter = nullptr;
    int best_constraint_idx = -1;

    for (int i = 0; i < pInfo->nConstraint; i++) {
        const auto& constraint = pInfo->aConstraint[i];

        // Only handle usable EQ constraints for now
        if (!constraint.usable) continue;
        if (constraint.op != SQLITE_INDEX_CONSTRAINT_EQ) continue;

        // Check if we have a filter for this column
        const FilterDef* filter = def->find_filter(constraint.iColumn);
        if (filter) {
            // Use the filter with lowest cost if multiple match
            if (!best_filter || filter->estimated_cost < best_filter->estimated_cost) {
                best_filter = filter;
                best_constraint_idx = i;
            }
        }
    }

    if (best_filter && best_constraint_idx >= 0) {
        // Tell SQLite we'll handle this constraint
        pInfo->aConstraintUsage[best_constraint_idx].argvIndex = 1;  // First arg
        pInfo->aConstraintUsage[best_constraint_idx].omit = 1;       // Don't recheck
        pInfo->idxNum = best_filter->filter_id;
        pInfo->estimatedCost = best_filter->estimated_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(best_filter->estimated_rows);
    } else {
        // No filter - full scan. Prefer cheap estimate_rows() for planning.
        // Avoid calling row_count() here since it may be expensive or have side effects.
        size_t full_count = 100000;
        if (def->estimate_rows) {
            full_count = def->estimate_rows();
        }
        pInfo->idxNum = FILTER_NONE;
        pInfo->estimatedCost = static_cast<double>(full_count);
        pInfo->estimatedRows = full_count;
    }

    return to_sqlite_status(Status::ok);
}

// xUpdate - handles INSERT, UPDATE, DELETE
inline int vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    const VTableDef* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row) {
            return to_sqlite_status(Status::read_only);
        }

        size_t rowid = static_cast<size_t>(sqlite3_value_int64(argv[0]));

        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        if (!def->delete_row(rowid)) {
            return to_sqlite_status(Status::error);
        }
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] != NULL: UPDATE
    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        size_t old_rowid = static_cast<size_t>(sqlite3_value_int64(argv[0]));

        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        for (size_t i = 2; i < static_cast<size_t>(argc) && (i - 2) < def->columns.size(); ++i) {
            if (sqlite3_value_nochange(argv[i])) continue;
            size_t col_idx = i - 2;
            const auto& col = def->columns[col_idx];
            if (col.writable && col.set) {
                clear_vtab_error();
                if (!col.set(old_rowid, FunctionArg(argv[i]))) {
                    const std::string& err = get_vtab_error();
                    if (!err.empty()) {
                        pVtab->zErrMsg = sqlite3_mprintf("%s", err.c_str());
                    }
                    clear_vtab_error();
                    return to_sqlite_status(Status::error);
                }
            }
        }
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] == NULL: INSERT
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        if (!def->supports_insert || !def->insert_row) {
            return to_sqlite_status(Status::read_only);
        }

        if (def->before_modify) {
            def->before_modify("INSERT INTO " + def->name);
        }

        // Pass column values starting at argv[2] (argv[0]=NULL, argv[1]=rowid)
        bool ok = false;
        detail::with_args(argc - 2, &argv[2], [&](FunctionArg* args) {
            ok = def->insert_row(argc - 2, args);
        });
        if (!ok) {
            return to_sqlite_status(Status::error);
        }
        return to_sqlite_status(Status::ok);
    }

    return to_sqlite_status(Status::read_only);
}

// Create module with xUpdate support
inline sqlite3_module create_module() {
    sqlite3_module mod = {};
    mod.iVersion = 1;
    mod.xCreate = vtab_connect;
    mod.xConnect = vtab_connect;
    mod.xBestIndex = vtab_best_index;
    mod.xDisconnect = vtab_disconnect;
    mod.xDestroy = vtab_disconnect;
    mod.xOpen = vtab_open;
    mod.xClose = vtab_close;
    mod.xFilter = vtab_filter;
    mod.xNext = vtab_next;
    mod.xEof = vtab_eof;
    mod.xColumn = vtab_column;
    mod.xRowid = vtab_rowid;
    mod.xUpdate = vtab_update;
    return mod;
}

inline sqlite3_module& get_module() {
    static sqlite3_module mod = create_module();
    return mod;
}

// ============================================================================
// Registration
// ============================================================================

namespace detail {

inline bool register_vtable_sqlite(sqlite3* db, const char* module_name, const VTableDef* def) {
    if (!db || !module_name || !def) return false;

    VTableDef* owned = detail::clone_def(def);
    if (!owned) return false;

    int rc = sqlite3_create_module_v2(db, module_name, &get_module(),
                                      owned, &detail::destroy_def<VTableDef>);
    if (!xsql::is_ok(rc)) {
        delete owned;
        return false;
    }
    return true;
}

/// Validate that a name contains only alphanumeric chars and underscores
inline bool is_valid_sql_identifier(const char* name) {
    if (!name || !*name) return false;
    for (const char* p = name; *p; ++p) {
        if (!std::isalnum(static_cast<unsigned char>(*p)) && *p != '_') return false;
    }
    return true;
}

inline bool create_vtable_sqlite(sqlite3* db, const char* table_name, const char* module_name) {
    // Validate identifiers to prevent SQL injection
    if (!is_valid_sql_identifier(table_name) || !is_valid_sql_identifier(module_name)) {
        return false;
    }
    std::string sql = "CREATE VIRTUAL TABLE " + std::string(table_name) +
                      " USING " + std::string(module_name) + ";";
    char* err = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (err) sqlite3_free(err);
    return xsql::is_ok(rc);
}

} // namespace detail

inline bool register_vtable(Database& db, const char* module_name, const VTableDef* def) {
    return detail::register_vtable_opaque(db.native_handle_unsafe(), module_name, def);
}

inline bool create_vtable(Database& db, const char* table_name, const char* module_name) {
    return detail::create_vtable_opaque(db.native_handle_unsafe(), table_name, module_name);
}

// ============================================================================
// Table Builder (Fluent API)
// ============================================================================

class VTableBuilder {
    VTableDef def_;
public:
    explicit VTableBuilder(const char* name) {
        def_.name = name;
        def_.supports_delete = false;
    }

    VTableBuilder& count(std::function<size_t()> fn) {
        def_.row_count = std::move(fn);
        return *this;
    }

    // Estimated row count for query planning (optional, should be cheap).
    VTableBuilder& estimate_rows(std::function<size_t()> fn) {
        def_.estimate_rows = std::move(fn);
        return *this;
    }

    // Hook called before any modification
    VTableBuilder& on_modify(std::function<void(const std::string&)> fn) {
        def_.before_modify = std::move(fn);
        return *this;
    }

    // Read-only integer column (int64)
    VTableBuilder& column_int64(const char* name, std::function<int64_t(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_int64(getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable integer column (int64)
    VTableBuilder& column_int64_rw(const char* name,
                                    std::function<int64_t(size_t)> getter,
                                    std::function<bool(size_t, int64_t)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_int64(getter(idx));
            },
            [setter = std::move(setter)](size_t idx, FunctionArg val) -> bool {
                return setter(idx, val.as_int64());
            });
        return *this;
    }

    // Read-only integer column (int)
    VTableBuilder& column_int(const char* name, std::function<int(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_int(getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable integer column (int)
    VTableBuilder& column_int_rw(const char* name,
                                  std::function<int(size_t)> getter,
                                  std::function<bool(size_t, int)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_int(getter(idx));
            },
            [setter = std::move(setter)](size_t idx, FunctionArg val) -> bool {
                return setter(idx, val.as_int());
            });
        return *this;
    }

    // Read-only text column
    VTableBuilder& column_text(const char* name, std::function<std::string(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_text(getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable text column
    VTableBuilder& column_text_rw(const char* name,
                                   std::function<std::string(size_t)> getter,
                                   std::function<bool(size_t, const char*)> setter) {
        def_.columns.emplace_back(name, ColumnType::Text, true,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_text(getter(idx));
            },
            [setter = std::move(setter)](size_t idx, FunctionArg val) -> bool {
                const char* text = val.as_c_str();
                return setter(idx, text ? text : "");
            });
        return *this;
    }

    // Read-only double column
    VTableBuilder& column_double(const char* name, std::function<double(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Real, false,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                ctx.result_double(getter(idx));
            },
            nullptr);
        return *this;
    }

    // Read-only blob column
    VTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Blob, false,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                auto val = getter(idx);
                ctx.result_blob(val.data(), val.size());
            },
            nullptr);
        return *this;
    }

    // Enable DELETE support
    VTableBuilder& deletable(std::function<bool(size_t)> delete_fn) {
        def_.supports_delete = true;
        def_.delete_row = std::move(delete_fn);
        return *this;
    }

    // Enable INSERT support with wrapped FunctionArg values.
    VTableBuilder& insertable(std::function<bool(int argc, FunctionArg* argv)> insert_fn) {
        def_.supports_insert = true;
        def_.insert_row = std::move(insert_fn);
        return *this;
    }

    // ========================================================================
    // Constraint Pushdown Filters
    // ========================================================================

    /**
     * Add an equality filter for int64 column.
     *
     * When SQLite queries with WHERE column = value, the filter's iterator
     * factory is called instead of doing a full table scan.
     *
     * @param column_name Column to filter on (must exist)
     * @param factory     Creates iterator for the given constraint value
     * @param cost        Estimated cost (lower = preferred by query planner)
     * @param est_rows    Estimated rows returned (default: 10)
     *
     * Example:
     *   .filter_eq("to_ea", [](int64_t target) {
     *       return std::make_unique<XrefsToIterator>(target);
     *   }, 10.0)
     */
    VTableBuilder& filter_eq(const char* column_name,
                              std::function<std::unique_ptr<RowIterator>(int64_t)> factory,
                              double cost = 10.0,
                              double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) {
            // Column not found - programming error, but don't crash
            return *this;
        }

        // Filter IDs start at 1 (0 = FILTER_NONE)
        int filter_id = static_cast<int>(def_.filters.size()) + 1;

        def_.filters.emplace_back(
            col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                return factory(val.as_int64());
            }
        );
        return *this;
    }

    /**
     * Add an equality filter for text column.
     */
    VTableBuilder& filter_eq_text(const char* column_name,
                                   std::function<std::unique_ptr<RowIterator>(const char*)> factory,
                                   double cost = 10.0,
                                   double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;

        int filter_id = static_cast<int>(def_.filters.size()) + 1;

        def_.filters.emplace_back(
            col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                const char* text = val.as_c_str();
                return factory(text ? text : "");
            }
        );
        return *this;
    }

    VTableDef build() { return std::move(def_); }
};

inline VTableBuilder table(const char* name) {
    return VTableBuilder(name);
}

// ============================================================================
// Convenience Macros
// ============================================================================

#define XSQL_COLUMN_INT64(name, getter) \
    .column_int64(#name, getter)

#define XSQL_COLUMN_INT(name, getter) \
    .column_int(#name, getter)

#define XSQL_COLUMN_TEXT(name, getter) \
    .column_text(#name, getter)

#define XSQL_COLUMN_DOUBLE(name, getter) \
    .column_double(#name, getter)

// ============================================================================
// Cached Table API (query-scoped cache, freed after query completes)
// ============================================================================
//
// Use cached_table<T>() for tables that need to enumerate data into a cache.
// The cache lives in the cursor and is automatically freed when the query ends.
//
// Example:
//   struct XrefInfo { ea_t from_ea; ea_t to_ea; };
//
//   auto def = xsql::cached_table<XrefInfo>("xrefs")
//       .estimate_rows([]() { return get_func_qty() * 10; })
//       .cache_builder([](std::vector<XrefInfo>& cache) {
//           // enumerate and populate cache
//       })
//       .column_int64("from_ea", [](const XrefInfo& r) { return r.from_ea; })
//       .filter_eq("to_ea", [](int64_t t) { return make_iterator(t); })
//       .build();

template<typename RowData>
struct CachedColumnDef {
    std::string name;
    ColumnType type;
    bool writable;
    std::function<void(FunctionContext&, const RowData&)> get;
    std::function<bool(RowData&, FunctionArg)> set;

    CachedColumnDef(const char* n, ColumnType t, bool w,
                    std::function<void(FunctionContext&, const RowData&)> getter,
                    std::function<bool(RowData&, FunctionArg)> setter = nullptr)
        : name(n), type(t), writable(w), get(std::move(getter)), set(std::move(setter)) {}
};

// Index definition for cached tables
struct CachedIndexDef {
    int column_index;                                        // Which column is indexed
    std::function<int64_t(const void*)> key_extractor;       // Extract key from row (type-erased)
};

// Shared cache with indexes - lazily built, shared across all cursors
template<typename RowData>
struct SharedCache {
    std::vector<RowData> data;
    // Map from column value -> list of row indices in data
    std::vector<std::unordered_map<int64_t, std::vector<size_t>>> indexes;
    bool built = false;
    mutable std::mutex mutex;
};

template<typename RowData>
struct CachedTableDef {
    std::string name;
    std::function<size_t()> estimate_rows_fn;
    std::function<void(std::vector<RowData>&)> cache_builder_fn;
    std::vector<CachedColumnDef<RowData>> columns;
    std::vector<FilterDef> filters;
    std::function<bool(RowData&)> delete_row;
    bool supports_delete = false;
    std::function<bool(int argc, FunctionArg* argv)> insert_row;
    bool supports_insert = false;
    std::function<void(const std::string&)> before_modify;
    std::function<void(const std::string&)> after_modify;

    // Populate a RowData from xUpdate argv values (argv[2..] = column values).
    // Used when the shared cache is not available (e.g., filter_eq path).
    // If not set, UPDATE only works when the shared cache contains the row.
    std::function<void(RowData&, int argc, FunctionArg* argv)> row_from_argv;

    // Optional row lookup by rowid for UPDATE/DELETE fallback.
    // Useful for filter iterators whose rowid is not a positional index.
    std::function<bool(RowData&, int64_t)> row_lookup;

    // Index definitions: column index -> key extractor
    std::vector<std::pair<int, std::function<int64_t(const RowData&)>>> index_defs;

    // Shared cache - lazily built on first query, shared across all cursors
    bool use_shared_cache = true;
    mutable std::shared_ptr<SharedCache<RowData>> shared_cache;

    std::string schema() const {
        std::ostringstream ss;
        ss << "CREATE TABLE " << name << "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "\"" << columns[i].name << "\" " << column_type_sql(columns[i].type);
        }
        ss << ")";
        return ss.str();
    }

    int find_column(const std::string& col_name) const {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == col_name) return static_cast<int>(i);
        }
        return -1;
    }

    const FilterDef* find_filter(int col_index) const {
        for (const auto& f : filters) {
            if (f.column_index == col_index) return &f;
        }
        return nullptr;
    }

    // Find index position for a column (-1 if not indexed)
    int find_index(int col_index) const {
        for (size_t i = 0; i < index_defs.size(); ++i) {
            if (index_defs[i].first == col_index) return static_cast<int>(i);
        }
        return -1;
    }

    // Ensure shared cache is built (thread-safe, lazy initialization)
    void ensure_cache_built() const {
        if (!use_shared_cache) return;
        if (!shared_cache) {
            shared_cache = std::make_shared<SharedCache<RowData>>();
        }
        std::lock_guard<std::mutex> lock(shared_cache->mutex);
        if (shared_cache->built) return;

        // Build the cache
        if (cache_builder_fn) {
            cache_builder_fn(shared_cache->data);
        }

        // Build indexes
        shared_cache->indexes.resize(index_defs.size());
        for (size_t idx = 0; idx < index_defs.size(); ++idx) {
            auto& index_map = shared_cache->indexes[idx];
            const auto& key_fn = index_defs[idx].second;
            for (size_t row = 0; row < shared_cache->data.size(); ++row) {
                int64_t key = key_fn(shared_cache->data[row]);
                index_map[key].push_back(row);
            }
        }

        shared_cache->built = true;
    }

    // Invalidate cache (call when underlying data changes)
    void invalidate_cache() const {
        if (shared_cache) {
            std::lock_guard<std::mutex> lock(shared_cache->mutex);
            shared_cache->data.clear();
            shared_cache->indexes.clear();
            shared_cache->built = false;
        }
    }
};

template<typename RowData>
struct CachedCursor {
    sqlite3_vtab_cursor base;
    const CachedTableDef<RowData>* def;
    std::vector<RowData> cache;           // Used only for non-shared fallback
    bool cache_built = false;
    size_t current_row = 0;
    std::unique_ptr<RowIterator> iterator;
    bool using_iterator = false;
    bool iterator_eof = false;

    // Index-based iteration
    bool using_index = false;
    const std::vector<size_t>* index_matches = nullptr;  // Points into shared_cache->indexes
    size_t index_pos = 0;
};

template<typename RowData>
struct CachedVtab {
    sqlite3_vtab base;
    const CachedTableDef<RowData>* def;
};

// SQLite callbacks for cached tables
template<typename RowData>
inline int cached_vtab_connect(sqlite3* db, void* pAux, int, const char* const*,
                               sqlite3_vtab** ppVtab, char**) {
    const auto* def = static_cast<const CachedTableDef<RowData>*>(pAux);
    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;
    auto* vtab = new CachedVtab<RowData>();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    auto* cursor = new CachedCursor<RowData>();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->def = vtab->def;
    cursor->cache_built = false;
    cursor->current_row = 0;
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    *ppCursor = &cursor->base;
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<CachedCursor<RowData>*>(pCursor);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);
    if (cursor->using_iterator && cursor->iterator) {
        if (!cursor->iterator->next()) {
            cursor->iterator_eof = true;
        }
    } else if (cursor->using_index) {
        cursor->index_pos++;
    } else {
        cursor->current_row++;
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);
    if (cursor->using_iterator) {
        if (!cursor->iterator || cursor->iterator_eof) return 1;
        return cursor->iterator->eof() ? 1 : 0;
    }
    if (cursor->using_index) {
        if (!cursor->index_matches) return 1;
        return cursor->index_pos >= cursor->index_matches->size() ? 1 : 0;
    }
    // Full scan using shared cache
    if (cursor->def->use_shared_cache && cursor->def->shared_cache && cursor->def->shared_cache->built) {
        return cursor->current_row >= cursor->def->shared_cache->data.size() ? 1 : 0;
    }
    return cursor->current_row >= cursor->cache.size() ? 1 : 0;
}

template<typename RowData>
inline int cached_vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);

    // During UPDATE, SQLite may ask for unchanged column values. Returning
    // without a value marks the column as SQLITE_NOCHANGE in xUpdate.
    // Only safe when the update handler can look up the original row by rowid
    // in the shared cache. Without shared cache, row_from_argv needs real
    // column values — returning NOCHANGE would cause it to read 0/NULL for
    // unchanged fields like ea or func_addr, breaking write operations.
    if (sqlite3_vtab_nochange(ctx) && cursor->def->use_shared_cache) {
        return to_sqlite_status(Status::ok);
    }

    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return to_sqlite_status(Status::ok);
    }
    FunctionContext fctx(ctx);
    if (cursor->using_iterator && cursor->iterator) {
        if (cursor->iterator_eof) {
            sqlite3_result_null(ctx);
            return to_sqlite_status(Status::ok);
        }
        cursor->iterator->column(fctx, col);
    } else if (cursor->using_index) {
        // Index-based access: get row from shared cache via index
        if (cursor->index_matches && cursor->index_pos < cursor->index_matches->size()) {
            size_t row_idx = (*cursor->index_matches)[cursor->index_pos];
            const auto& shared = cursor->def->shared_cache;
            if (shared && row_idx < shared->data.size()) {
                cursor->def->columns[col].get(fctx, shared->data[row_idx]);
            } else {
                sqlite3_result_null(ctx);
            }
        } else {
            sqlite3_result_null(ctx);
        }
    } else {
        // Full scan: use shared cache if available, else local cache
        const auto& shared = cursor->def->shared_cache;
        if (cursor->def->use_shared_cache && shared && shared->built && cursor->current_row < shared->data.size()) {
            cursor->def->columns[col].get(fctx, shared->data[cursor->current_row]);
        } else if (cursor->current_row < cursor->cache.size()) {
            cursor->def->columns[col].get(fctx, cursor->cache[cursor->current_row]);
        } else {
            sqlite3_result_null(ctx);
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);
    if (cursor->using_iterator && cursor->iterator) {
        if (cursor->iterator_eof) {
            *pRowid = 0;
            return to_sqlite_status(Status::ok);
        }
        *pRowid = cursor->iterator->rowid();
    } else {
        *pRowid = static_cast<sqlite3_int64>(cursor->current_row);
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_filter(sqlite3_vtab_cursor* pCursor, int idxNum, const char*,
                              int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);

    // Reset cursor state
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    cursor->using_index = false;
    cursor->index_matches = nullptr;
    cursor->index_pos = 0;
    cursor->cache.clear();
    cursor->cache_built = false;
    cursor->current_row = 0;

    if (idxNum != FILTER_NONE && argc > 0) {
        // Check for index-based lookup (idxNum >= INDEX_BASE)
        if (cursor->def->use_shared_cache && idxNum >= INDEX_BASE) {
            int index_pos = idxNum - INDEX_BASE;
            const auto& index_defs = cursor->def->index_defs;
            if (index_pos >= 0 && static_cast<size_t>(index_pos) < index_defs.size()) {
                // Ensure cache and indexes are built
                cursor->def->ensure_cache_built();
                const auto& shared = cursor->def->shared_cache;
                if (shared && shared->built && static_cast<size_t>(index_pos) < shared->indexes.size()) {
                    int64_t key = FunctionArg(argv[0]).as_int64();
                    auto it = shared->indexes[index_pos].find(key);
                    if (it != shared->indexes[index_pos].end()) {
                        cursor->using_index = true;
                        cursor->index_matches = &it->second;
                        cursor->index_pos = 0;
                    } else {
                        // No matches - return empty result
                        cursor->using_index = true;
                        cursor->index_matches = nullptr;
                    }
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Check for filter-based lookup
        for (const auto& filter : cursor->def->filters) {
            if (filter.filter_id == idxNum) {
                cursor->iterator = filter.create(FunctionArg(argv[0]));
                cursor->using_iterator = true;
                cursor->iterator_eof = true;
                if (cursor->iterator) {
                    cursor->iterator_eof = !cursor->iterator->next();
                }
                return to_sqlite_status(Status::ok);
            }
        }
    }

    // Full scan - use shared cache or query-local cache depending on table policy.
    if (cursor->def->use_shared_cache) {
        cursor->def->ensure_cache_built();
    } else if (cursor->def->cache_builder_fn) {
        cursor->cache.clear();
        cursor->def->cache_builder_fn(cursor->cache);
        cursor->cache_built = true;
    }
    cursor->using_iterator = false;
    cursor->using_index = false;
    cursor->current_row = 0;
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    const auto* def = vtab->def;

    // Track best option: filter, index, or full scan
    const FilterDef* best_filter = nullptr;
    int best_filter_constraint_idx = -1;
    int best_index_pos = -1;
    int best_index_constraint_idx = -1;
    double best_cost = 1e9;

    for (int i = 0; i < pInfo->nConstraint; i++) {
        const auto& constraint = pInfo->aConstraint[i];
        if (!constraint.usable) continue;
        if (constraint.op != SQLITE_INDEX_CONSTRAINT_EQ) continue;

        // Check for explicit filter
        const FilterDef* filter = def->find_filter(constraint.iColumn);
        if (filter && filter->estimated_cost < best_cost) {
            best_filter = filter;
            best_filter_constraint_idx = i;
            best_cost = filter->estimated_cost;
        }

        // Check for indexed column
        int idx_pos = def->use_shared_cache ? def->find_index(constraint.iColumn) : -1;
        if (idx_pos >= 0) {
            // Index lookups are very cheap (hash lookup)
            double index_cost = 1.0;
            if (index_cost < best_cost) {
                best_index_pos = idx_pos;
                best_index_constraint_idx = i;
                best_cost = index_cost;
                best_filter = nullptr;  // Index beats filter
            }
        }
    }

    // Prefer index over filter if both matched
    if (best_index_pos >= 0 && best_index_constraint_idx >= 0) {
        pInfo->aConstraintUsage[best_index_constraint_idx].argvIndex = 1;
        pInfo->aConstraintUsage[best_index_constraint_idx].omit = 1;
        pInfo->idxNum = INDEX_BASE + best_index_pos;
        pInfo->estimatedCost = 1.0;
        pInfo->estimatedRows = 5;  // Assume small result set
    } else if (best_filter && best_filter_constraint_idx >= 0) {
        pInfo->aConstraintUsage[best_filter_constraint_idx].argvIndex = 1;
        pInfo->aConstraintUsage[best_filter_constraint_idx].omit = 1;
        pInfo->idxNum = best_filter->filter_id;
        pInfo->estimatedCost = best_filter->estimated_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(best_filter->estimated_rows);
    } else {
        // No filter or index - full scan
        size_t estimated_rows = 1000;
        if (def->estimate_rows_fn) {
            estimated_rows = def->estimate_rows_fn();
        }
        pInfo->idxNum = FILTER_NONE;
        pInfo->estimatedCost = static_cast<double>(estimated_rows);
        pInfo->estimatedRows = estimated_rows;
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    const auto* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row) {
            return to_sqlite_status(Status::read_only);
        }

        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        const size_t rowid = raw_rowid >= 0 ? static_cast<size_t>(raw_rowid) : static_cast<size_t>(-1);

        const auto& shared = def->shared_cache;
        RowData temp_row{};
        RowData* row_ptr = nullptr;

        if (shared && shared->built && raw_rowid >= 0 && rowid < shared->data.size()) {
            row_ptr = &shared->data[rowid];
        } else if (def->row_lookup && def->row_lookup(temp_row, raw_rowid)) {
            row_ptr = &temp_row;
        } else if (!def->use_shared_cache && def->cache_builder_fn && raw_rowid >= 0) {
            std::vector<RowData> rows;
            def->cache_builder_fn(rows);
            if (rowid < rows.size()) {
                temp_row = std::move(rows[rowid]);
                row_ptr = &temp_row;
            }
        }

        if (!row_ptr) {
            return to_sqlite_status(Status::error);
        }

        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        if (!def->delete_row(*row_ptr)) {
            return to_sqlite_status(Status::error);
        }
        def->invalidate_cache();
        if (def->after_modify) def->after_modify("DELETE FROM " + def->name);
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] != NULL: UPDATE
    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        const size_t old_rowid = raw_rowid >= 0 ? static_cast<size_t>(raw_rowid) : static_cast<size_t>(-1);

        // Check if any column is writable
        bool has_writable = false;
        for (const auto& col : def->columns) {
            if (col.writable && col.set) { has_writable = true; break; }
        }
        if (!has_writable) return to_sqlite_status(Status::read_only);

        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        // Try shared cache first; if unavailable or rowid out of range,
        // construct a temporary row from column values (argv[2..])
        const auto& shared = def->shared_cache;
        RowData* row_ptr = nullptr;
        RowData temp_row{};

        if (shared && shared->built && raw_rowid >= 0 && old_rowid < shared->data.size()) {
            row_ptr = &shared->data[old_rowid];
        } else if (def->row_from_argv) {
            // Build temp row from argv column values.
            // Handles the filter_eq path where rows come from an iterator.
            detail::with_args(argc, argv, [&](FunctionArg* args) {
                def->row_from_argv(temp_row, argc, args);
            });
            row_ptr = &temp_row;
        } else if (def->row_lookup && def->row_lookup(temp_row, raw_rowid)) {
            row_ptr = &temp_row;
        } else if (!def->use_shared_cache && def->cache_builder_fn && raw_rowid >= 0) {
            std::vector<RowData> rows;
            def->cache_builder_fn(rows);
            if (old_rowid < rows.size()) {
                temp_row = std::move(rows[old_rowid]);
                row_ptr = &temp_row;
            }
        } else {
            return to_sqlite_status(Status::read_only);
        }

        for (size_t i = 2; i < static_cast<size_t>(argc) && (i - 2) < def->columns.size(); ++i) {
            if (sqlite3_value_nochange(argv[i])) continue;
            size_t col_idx = i - 2;
            const auto& col = def->columns[col_idx];
            if (col.writable && col.set) {
                clear_vtab_error();
                if (!col.set(*row_ptr, FunctionArg(argv[i]))) {
                    const std::string& err = get_vtab_error();
                    if (!err.empty()) {
                        pVtab->zErrMsg = sqlite3_mprintf("%s", err.c_str());
                    }
                    clear_vtab_error();
                    return to_sqlite_status(Status::error);
                }
            }
        }
        def->invalidate_cache();
        if (def->after_modify) def->after_modify("UPDATE " + def->name);
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] == NULL: INSERT
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        if (!def->supports_insert || !def->insert_row) {
            return to_sqlite_status(Status::read_only);
        }

        if (def->before_modify) {
            def->before_modify("INSERT INTO " + def->name);
        }

        bool ok = false;
        detail::with_args(argc - 2, &argv[2], [&](FunctionArg* args) {
            ok = def->insert_row(argc - 2, args);
        });
        if (!ok) {
            return to_sqlite_status(Status::error);
        }
        def->invalidate_cache();
        if (def->after_modify) def->after_modify("INSERT INTO " + def->name);
        return to_sqlite_status(Status::ok);
    }

    return to_sqlite_status(Status::read_only);
}

template<typename RowData>
inline sqlite3_module create_cached_module() {
    sqlite3_module mod = {};
    mod.iVersion = 1;
    mod.xCreate = cached_vtab_connect<RowData>;
    mod.xConnect = cached_vtab_connect<RowData>;
    mod.xBestIndex = cached_vtab_best_index<RowData>;
    mod.xDisconnect = cached_vtab_disconnect<RowData>;
    mod.xDestroy = cached_vtab_disconnect<RowData>;
    mod.xOpen = cached_vtab_open<RowData>;
    mod.xClose = cached_vtab_close<RowData>;
    mod.xFilter = cached_vtab_filter<RowData>;
    mod.xNext = cached_vtab_next<RowData>;
    mod.xEof = cached_vtab_eof<RowData>;
    mod.xColumn = cached_vtab_column<RowData>;
    mod.xRowid = cached_vtab_rowid<RowData>;
    mod.xUpdate = cached_vtab_update<RowData>;
    return mod;
}

template<typename RowData>
inline sqlite3_module& get_cached_module() {
    static sqlite3_module mod = create_cached_module<RowData>();
    return mod;
}

template<typename RowData>
inline bool detail::register_cached_vtable_sqlite(sqlite3* db,
                                                  const char* module_name,
                                                  const CachedTableDef<RowData>* def) {
    if (!db || !module_name || !def) return false;

    auto* owned = detail::clone_def(def);
    if (!owned) return false;

    int rc = sqlite3_create_module_v2(
        db,
        module_name,
        &get_cached_module<RowData>(),
        owned,
        &detail::destroy_def<CachedTableDef<RowData>>
    );

    if (!xsql::is_ok(rc)) {
        delete owned;
        return false;
    }
    return true;
}

template<typename RowData>
inline bool register_cached_vtable(Database& db,
                                   const char* module_name,
                                   const CachedTableDef<RowData>* def) {
    return detail::register_cached_vtable_opaque<RowData>(db.native_handle_unsafe(), module_name, def);
}

// Cached Table Builder
template<typename RowData>
class CachedTableBuilder {
    CachedTableDef<RowData> def_;
public:
    explicit CachedTableBuilder(const char* name) {
        def_.name = name;
        def_.supports_delete = false;
        def_.supports_insert = false;
    }

    CachedTableBuilder& estimate_rows(std::function<size_t()> fn) {
        def_.estimate_rows_fn = std::move(fn);
        return *this;
    }

    CachedTableBuilder& cache_builder(std::function<void(std::vector<RowData>&)> fn) {
        def_.cache_builder_fn = std::move(fn);
        return *this;
    }

    // Force query-lived cache (no shared persistent cache across queries).
    CachedTableBuilder& no_shared_cache() {
        def_.use_shared_cache = false;
        def_.shared_cache.reset();
        return *this;
    }

    CachedTableBuilder& on_modify(std::function<void(const std::string&)> fn) {
        def_.before_modify = std::move(fn);
        return *this;
    }

    CachedTableBuilder& after_modify(std::function<void(const std::string&)> fn) {
        def_.after_modify = std::move(fn);
        return *this;
    }

    CachedTableBuilder& column(const char* name,
                               ColumnType type,
                               std::function<void(FunctionContext&, const RowData&)> getter) {
        def_.columns.emplace_back(name, type, false, std::move(getter), nullptr);
        return *this;
    }

    CachedTableBuilder& column_int64(const char* name, std::function<int64_t(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int64(getter(row));
            }, nullptr);
        return *this;
    }

    CachedTableBuilder& column_int64_rw(const char* name,
                                         std::function<int64_t(const RowData&)> getter,
                                         std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int64(getter(row));
            },
            std::move(setter));
        return *this;
    }

    CachedTableBuilder& column_int64_rw(const char* name,
                                         std::function<int64_t(const RowData&)> getter,
                                         std::function<bool(RowData&, int64_t)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int64(getter(row));
            },
            [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
                return setter(row, val.as_int64());
            });
        return *this;
    }

    CachedTableBuilder& column_int(const char* name, std::function<int(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int(getter(row));
            }, nullptr);
        return *this;
    }

    CachedTableBuilder& column_int_rw(const char* name,
                                       std::function<int(const RowData&)> getter,
                                       std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int(getter(row));
            },
            std::move(setter));
        return *this;
    }

    CachedTableBuilder& column_int_rw(const char* name,
                                       std::function<int(const RowData&)> getter,
                                       std::function<bool(RowData&, int)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int(getter(row));
            },
            [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
                return setter(row, val.as_int());
            });
        return *this;
    }

    CachedTableBuilder& column_text(const char* name, std::function<std::string(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_text(getter(row));
            }, nullptr);
        return *this;
    }

    CachedTableBuilder& column_text_rw(const char* name,
                                        std::function<std::string(const RowData&)> getter,
                                        std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.emplace_back(name, ColumnType::Text, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_text(getter(row));
            },
            std::move(setter));
        return *this;
    }

    CachedTableBuilder& column_text_rw(const char* name,
                                        std::function<std::string(const RowData&)> getter,
                                        std::function<bool(RowData&, const char*)> setter) {
        def_.columns.emplace_back(name, ColumnType::Text, true,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_text(getter(row));
            },
            [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
                const char* text = val.as_c_str();
                return setter(row, text ? text : "");
            });
        return *this;
    }

    CachedTableBuilder& column_double(const char* name, std::function<double(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Real, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_double(getter(row));
            }, nullptr);
        return *this;
    }

    CachedTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Blob, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                auto val = getter(row);
                ctx.result_blob(val.data(), val.size());
            }, nullptr);
        return *this;
    }

    CachedTableBuilder& filter_eq(const char* column_name,
                                   std::function<std::unique_ptr<RowIterator>(int64_t)> factory,
                                   double cost = 10.0, double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        int filter_id = static_cast<int>(def_.filters.size()) + 1;
        def_.filters.emplace_back(col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                return factory(val.as_int64());
            });
        return *this;
    }

    CachedTableBuilder& filter_eq_text(const char* column_name,
                                        std::function<std::unique_ptr<RowIterator>(const char*)> factory,
                                        double cost = 10.0, double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        int filter_id = static_cast<int>(def_.filters.size()) + 1;
        def_.filters.emplace_back(col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                const char* text = val.as_c_str();
                return factory(text ? text : "");
            });
        return *this;
    }

    CachedTableBuilder& row_populator(std::function<void(RowData&, int argc, FunctionArg* argv)> fn) {
        def_.row_from_argv = std::move(fn);
        return *this;
    }

    CachedTableBuilder& row_lookup(std::function<bool(RowData&, int64_t)> fn) {
        def_.row_lookup = std::move(fn);
        return *this;
    }

    CachedTableBuilder& deletable(std::function<bool(RowData&)> delete_fn) {
        def_.supports_delete = true;
        def_.delete_row = std::move(delete_fn);
        return *this;
    }

    CachedTableBuilder& insertable(std::function<bool(int argc, FunctionArg* argv)> insert_fn) {
        def_.supports_insert = true;
        def_.insert_row = std::move(insert_fn);
        return *this;
    }

    /**
     * Add an index on an integer column for O(1) lookups.
     *
     * The index is built lazily when the table is first queried.
     * When SQLite uses WHERE column = value, the index provides
     * direct access to matching rows without scanning.
     *
     * @param column_name Name of the column to index (must be int64)
     * @param key_extractor Function to extract the key from a row
     * @return Reference to builder for chaining
     *
     * Example:
     *   .index_on("to_ea", [](const XrefInfo& r) { return r.to_ea; })
     */
    CachedTableBuilder& index_on(const char* column_name,
                                  std::function<int64_t(const RowData&)> key_extractor) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        def_.index_defs.emplace_back(col_idx, std::move(key_extractor));
        return *this;
    }

    CachedTableDef<RowData> build() {
        // Pre-create shared cache only when requested.
        if (def_.use_shared_cache) {
            def_.shared_cache = std::make_shared<SharedCache<RowData>>();
        } else {
            def_.shared_cache.reset();
        }
        return std::move(def_);
    }
};

template<typename RowData>
inline CachedTableBuilder<RowData> cached_table(const char* name) {
    return CachedTableBuilder<RowData>(name);
}

// ============================================================================
// Generator Table API (streaming, no full-cache materialization)
// ============================================================================
//
// Use generator_table<T>() for expensive sources where full scans must be lazy
// (e.g., LIMIT should stop work early).
//
// The generator is owned by the cursor and destroyed when the query ends.
// SQLite will call:
//   xFilter -> generator->next() once (position on first row)
//   xNext   -> generator->next() for subsequent rows
//
// Constraints can still be pushed down using filter_eq(), which uses RowIterator.

template<typename RowData>
struct Generator {
    virtual ~Generator() = default;

    // Advance to next row. Returns true if there is a row, false if exhausted.
    // Must be called before accessing the first row.
    virtual bool next() = 0;

    // Current row (valid only after next() returns true)
    virtual const RowData& current() const = 0;

    // Current rowid (valid only after next() returns true)
    virtual int64_t rowid() const = 0;
};

// Parametric filter: requires ALL specified columns to be EQ-constrained.
// Used for table-valued functions with hidden input parameters.
// Filter IDs start at PARAMETRIC_FILTER_BASE to avoid collision with single-column filters.
constexpr int PARAMETRIC_FILTER_BASE = 500;

template<typename RowData>
struct ParametricFilterDef {
    std::vector<int> column_indices;     // Columns that must ALL be EQ-constrained
    int filter_id;                       // Unique ID (PARAMETRIC_FILTER_BASE + N)
    double estimated_cost;
    double estimated_rows;
    // Factory receives constraint values in column_indices order
    std::function<std::unique_ptr<Generator<RowData>>(
        const std::vector<FunctionArg>&)> create;
};

template<typename RowData>
struct GeneratorTableDef {
    std::string name;
    std::function<size_t()> estimate_rows_fn;
    std::function<std::unique_ptr<Generator<RowData>>()> generator_factory_fn;
    std::vector<CachedColumnDef<RowData>> columns;
    std::vector<FilterDef> filters;
    std::vector<ParametricFilterDef<RowData>> parametric_filters;
    std::unordered_set<int> hidden_columns;  // Column indices marked HIDDEN

    std::string schema() const {
        std::ostringstream ss;
        ss << "CREATE TABLE " << name << "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "\"" << columns[i].name << "\" " << column_type_sql(columns[i].type);
            if (hidden_columns.count(static_cast<int>(i))) ss << " HIDDEN";
        }
        ss << ")";
        return ss.str();
    }

    int find_column(const std::string& col_name) const {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == col_name) return static_cast<int>(i);
        }
        return -1;
    }

    const FilterDef* find_filter(int col_index) const {
        for (const auto& f : filters) {
            if (f.column_index == col_index) return &f;
        }
        return nullptr;
    }
};

template<typename RowData>
struct GeneratorCursor {
    sqlite3_vtab_cursor base;
    const GeneratorTableDef<RowData>* def = nullptr;
    std::unique_ptr<Generator<RowData>> generator;
    bool generator_eof = false;
    std::unique_ptr<RowIterator> iterator;
    bool using_iterator = false;
    bool iterator_eof = false;
};

template<typename RowData>
struct GeneratorVtab {
    sqlite3_vtab base;
    const GeneratorTableDef<RowData>* def = nullptr;
};

// SQLite callbacks for generator tables
template<typename RowData>
inline int generator_vtab_connect(sqlite3* db, void* pAux, int, const char* const*,
                                  sqlite3_vtab** ppVtab, char**) {
    const auto* def = static_cast<const GeneratorTableDef<RowData>*>(pAux);
    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;
    auto* vtab = new GeneratorVtab<RowData>();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    auto* cursor = new GeneratorCursor<RowData>();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->def = vtab->def;
    cursor->generator = nullptr;
    cursor->generator_eof = false;
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    *ppCursor = &cursor->base;
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (cursor->using_iterator && cursor->iterator) {
        if (!cursor->iterator->next()) {
            cursor->iterator_eof = true;
        }
    } else {
        if (!cursor->generator || !cursor->generator->next()) {
            cursor->generator_eof = true;
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (cursor->using_iterator) {
        if (!cursor->iterator || cursor->iterator_eof) return 1;
        return cursor->iterator->eof() ? 1 : 0;
    }
    return (!cursor->generator || cursor->generator_eof) ? 1 : 0;
}

template<typename RowData>
inline int generator_vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return to_sqlite_status(Status::ok);
    }

    FunctionContext fctx(ctx);
    if (cursor->using_iterator && cursor->iterator) {
        if (cursor->iterator_eof) {
            sqlite3_result_null(ctx);
            return to_sqlite_status(Status::ok);
        }
        cursor->iterator->column(fctx, col);
        return to_sqlite_status(Status::ok);
    }

    if (!cursor->generator || cursor->generator_eof) {
        sqlite3_result_null(ctx);
        return to_sqlite_status(Status::ok);
    }

    cursor->def->columns[col].get(fctx, cursor->generator->current());
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (cursor->using_iterator && cursor->iterator) {
        if (cursor->iterator_eof) {
            *pRowid = 0;
            return to_sqlite_status(Status::ok);
        }
        *pRowid = cursor->iterator->rowid();
        return to_sqlite_status(Status::ok);
    }

    if (!cursor->generator || cursor->generator_eof) {
        *pRowid = 0;
        return to_sqlite_status(Status::ok);
    }

    *pRowid = static_cast<sqlite3_int64>(cursor->generator->rowid());
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_filter(sqlite3_vtab_cursor* pCursor, int idxNum, const char*,
                                 int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);

    cursor->generator = nullptr;
    cursor->generator_eof = false;
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;

    if (idxNum != FILTER_NONE && argc > 0) {
        // Check parametric filters first (multi-column, creates a Generator)
        if (idxNum >= PARAMETRIC_FILTER_BASE) {
            for (const auto& pf : cursor->def->parametric_filters) {
                if (pf.filter_id == idxNum) {
                    std::vector<FunctionArg> args;
                    args.reserve(argc);
                    for (int i = 0; i < argc; i++) {
                        args.emplace_back(argv[i]);
                    }
                    cursor->generator = pf.create(args);
                    cursor->using_iterator = false;
                    cursor->generator_eof = true;
                    if (cursor->generator) {
                        cursor->generator_eof = !cursor->generator->next();
                    }
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Single-column filters (creates a RowIterator)
        for (const auto& filter : cursor->def->filters) {
            if (filter.filter_id == idxNum) {
                cursor->iterator = filter.create(FunctionArg(argv[0]));
                cursor->using_iterator = true;
                cursor->iterator_eof = true;
                if (cursor->iterator) {
                    cursor->iterator_eof = !cursor->iterator->next();
                }
                return to_sqlite_status(Status::ok);
            }
        }
    }

    // Full scan - create generator and position to first row.
    cursor->using_iterator = false;
    cursor->generator_eof = true;
    if (cursor->def->generator_factory_fn) {
        cursor->generator = cursor->def->generator_factory_fn();
        if (cursor->generator) {
            cursor->generator_eof = !cursor->generator->next();
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    const auto* def = vtab->def;

    // First, check parametric filters (multi-column, e.g. hidden params for table-valued functions).
    // A parametric filter matches when ALL its required columns are EQ-constrained.
    for (const auto& pf : def->parametric_filters) {
        // Map each required column to its constraint index
        std::vector<int> matched_constraints(pf.column_indices.size(), -1);
        bool all_matched = true;

        for (size_t p = 0; p < pf.column_indices.size(); ++p) {
            int required_col = pf.column_indices[p];
            bool found = false;
            for (int i = 0; i < pInfo->nConstraint; i++) {
                const auto& c = pInfo->aConstraint[i];
                if (!c.usable) continue;
                if (c.op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
                if (c.iColumn == required_col) {
                    matched_constraints[p] = i;
                    found = true;
                    break;
                }
            }
            if (!found) { all_matched = false; break; }
        }

        if (all_matched) {
            // Assign sequential argvIndex values (1-based)
            for (size_t p = 0; p < matched_constraints.size(); ++p) {
                pInfo->aConstraintUsage[matched_constraints[p]].argvIndex = static_cast<int>(p + 1);
                pInfo->aConstraintUsage[matched_constraints[p]].omit = 1;
            }
            pInfo->idxNum = pf.filter_id;
            pInfo->estimatedCost = pf.estimated_cost;
            pInfo->estimatedRows = static_cast<sqlite3_int64>(pf.estimated_rows);
            return to_sqlite_status(Status::ok);
        }
    }

    // Next, check single-column filters.
    const FilterDef* best_filter = nullptr;
    int best_constraint_idx = -1;

    for (int i = 0; i < pInfo->nConstraint; i++) {
        const auto& constraint = pInfo->aConstraint[i];
        if (!constraint.usable) continue;
        if (constraint.op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
        const FilterDef* filter = def->find_filter(constraint.iColumn);
        if (filter) {
            if (!best_filter || filter->estimated_cost < best_filter->estimated_cost) {
                best_filter = filter;
                best_constraint_idx = i;
            }
        }
    }

    if (best_filter && best_constraint_idx >= 0) {
        pInfo->aConstraintUsage[best_constraint_idx].argvIndex = 1;
        pInfo->aConstraintUsage[best_constraint_idx].omit = 1;
        pInfo->idxNum = best_filter->filter_id;
        pInfo->estimatedCost = best_filter->estimated_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(best_filter->estimated_rows);
    } else {
        size_t estimated_rows = 1000;
        if (def->estimate_rows_fn) {
            estimated_rows = def->estimate_rows_fn();
        }
        pInfo->idxNum = FILTER_NONE;
        pInfo->estimatedCost = static_cast<double>(estimated_rows);
        pInfo->estimatedRows = estimated_rows;
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_update(sqlite3_vtab*, int, sqlite3_value**, sqlite3_int64*) {
    return to_sqlite_status(Status::read_only);
}

template<typename RowData>
inline sqlite3_module create_generator_module() {
    sqlite3_module mod = {};
    mod.iVersion = 1;
    mod.xCreate = generator_vtab_connect<RowData>;
    mod.xConnect = generator_vtab_connect<RowData>;
    mod.xBestIndex = generator_vtab_best_index<RowData>;
    mod.xDisconnect = generator_vtab_disconnect<RowData>;
    mod.xDestroy = generator_vtab_disconnect<RowData>;
    mod.xOpen = generator_vtab_open<RowData>;
    mod.xClose = generator_vtab_close<RowData>;
    mod.xFilter = generator_vtab_filter<RowData>;
    mod.xNext = generator_vtab_next<RowData>;
    mod.xEof = generator_vtab_eof<RowData>;
    mod.xColumn = generator_vtab_column<RowData>;
    mod.xRowid = generator_vtab_rowid<RowData>;
    mod.xUpdate = generator_vtab_update<RowData>;
    return mod;
}

template<typename RowData>
inline sqlite3_module& get_generator_module() {
    static sqlite3_module mod = create_generator_module<RowData>();
    return mod;
}

template<typename RowData>
inline bool detail::register_generator_vtable_sqlite(sqlite3* db,
                                                     const char* module_name,
                                                     const GeneratorTableDef<RowData>* def) {
    if (!db || !module_name || !def) return false;

    auto* owned = detail::clone_def(def);
    if (!owned) return false;

    int rc = sqlite3_create_module_v2(
        db,
        module_name,
        &get_generator_module<RowData>(),
        owned,
        &detail::destroy_def<GeneratorTableDef<RowData>>
    );

    if (!xsql::is_ok(rc)) {
        delete owned;
        return false;
    }
    return true;
}

template<typename RowData>
inline bool register_generator_vtable(Database& db,
                                      const char* module_name,
                                      const GeneratorTableDef<RowData>* def) {
    return detail::register_generator_vtable_opaque<RowData>(db.native_handle_unsafe(), module_name, def);
}

// Generator Table Builder
template<typename RowData>
class GeneratorTableBuilder {
    GeneratorTableDef<RowData> def_;
public:
    explicit GeneratorTableBuilder(const char* name) {
        def_.name = name;
    }

    GeneratorTableBuilder& estimate_rows(std::function<size_t()> fn) {
        def_.estimate_rows_fn = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& generator(std::function<std::unique_ptr<Generator<RowData>>()> fn) {
        def_.generator_factory_fn = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& column_int64(const char* name, std::function<int64_t(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int64(getter(row));
            }, nullptr);
        return *this;
    }

    GeneratorTableBuilder& column_int(const char* name, std::function<int(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_int(getter(row));
            }, nullptr);
        return *this;
    }

    GeneratorTableBuilder& column_text(const char* name, std::function<std::string(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_text(getter(row));
            }, nullptr);
        return *this;
    }

    GeneratorTableBuilder& column_double(const char* name, std::function<double(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Real, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                ctx.result_double(getter(row));
            }, nullptr);
        return *this;
    }

    GeneratorTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(const RowData&)> getter) {
        def_.columns.emplace_back(name, ColumnType::Blob, false,
            [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
                auto val = getter(row);
                ctx.result_blob(val.data(), val.size());
            }, nullptr);
        return *this;
    }

    GeneratorTableBuilder& filter_eq(const char* column_name,
                                     std::function<std::unique_ptr<RowIterator>(int64_t)> factory,
                                     double cost = 10.0, double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        int filter_id = static_cast<int>(def_.filters.size()) + 1;
        def_.filters.emplace_back(col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                return factory(val.as_int64());
            });
        return *this;
    }

    GeneratorTableBuilder& filter_eq_text(const char* column_name,
                                          std::function<std::unique_ptr<RowIterator>(const char*)> factory,
                                          double cost = 10.0, double est_rows = 10.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        int filter_id = static_cast<int>(def_.filters.size()) + 1;
        def_.filters.emplace_back(col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                const char* text = val.as_c_str();
                return factory(text ? text : "");
            });
        return *this;
    }

    // Hidden columns: input parameters not part of output, constrained via WHERE clause.
    // These appear as HIDDEN in the schema and are typically used with parametric_filter().
    GeneratorTableBuilder& hidden_column_int64(const char* name) {
        int idx = static_cast<int>(def_.columns.size());
        // Hidden columns use a no-op getter (value comes from WHERE, not from RowData)
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [](FunctionContext& ctx, const RowData&) { ctx.result_null(); }, nullptr);
        def_.hidden_columns.insert(idx);
        return *this;
    }

    GeneratorTableBuilder& hidden_column_text(const char* name) {
        int idx = static_cast<int>(def_.columns.size());
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [](FunctionContext& ctx, const RowData&) { ctx.result_null(); }, nullptr);
        def_.hidden_columns.insert(idx);
        return *this;
    }

    GeneratorTableBuilder& hidden_column_int(const char* name) {
        int idx = static_cast<int>(def_.columns.size());
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [](FunctionContext& ctx, const RowData&) { ctx.result_null(); }, nullptr);
        def_.hidden_columns.insert(idx);
        return *this;
    }

    /**
     * Parametric filter: requires ALL named columns to be EQ-constrained.
     * The factory receives constraint values in the order of param_columns.
     * Returns a Generator (not a RowIterator) since the result set is computed fresh.
     */
    GeneratorTableBuilder& parametric_filter(
            std::initializer_list<const char*> param_columns,
            std::function<std::unique_ptr<Generator<RowData>>(
                const std::vector<FunctionArg>&)> factory,
            double cost = 1.0, double est_rows = 100.0) {
        ParametricFilterDef<RowData> pf;
        for (const char* col_name : param_columns) {
            int idx = def_.find_column(col_name);
            if (idx < 0) return *this;  // Column not found
            pf.column_indices.push_back(idx);
        }
        pf.filter_id = PARAMETRIC_FILTER_BASE + static_cast<int>(def_.parametric_filters.size());
        pf.estimated_cost = cost;
        pf.estimated_rows = est_rows;
        pf.create = std::move(factory);
        def_.parametric_filters.push_back(std::move(pf));
        return *this;
    }

    GeneratorTableDef<RowData> build() { return std::move(def_); }
};

template<typename RowData>
inline GeneratorTableBuilder<RowData> generator_table(const char* name) {
    return GeneratorTableBuilder<RowData>(name);
}

} // namespace xsql
