// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

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
 */

#pragma once

#include "database.hpp"
#include "interruption.hpp"
#include <sqlite3.h>
#include <string>
#include <vector>
#include <functional>
#include <exception>
#include <sstream>
#include <cstring>
#include <cctype>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <algorithm>
#include <utility>

namespace xsql {

namespace detail {
template <typename T>
inline T* clone_def(const T* src) {
    if (!src) return nullptr;
    auto* clone = new (std::nothrow) T(*src);
    if (clone) {
        try {
            clone->transaction_hooks.initialize_state();
        } catch (...) {
            delete clone;
            return nullptr;
        }
    }
    return clone;
}

template <typename T>
inline void destroy_def(void* p) {
    delete static_cast<T*>(p);
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

// Assign pVtab->zErrMsg, freeing any message a previous assignment left there.
// SQLite frees zErrMsg only when it consumes it, so a second bare assignment
// before consumption leaks the first buffer — every zErrMsg write in this file
// funnels through here.
inline void set_vtab_errmsg(sqlite3_vtab* pVtab, const char* message) {
    if (!pVtab) return;
    sqlite3_free(pVtab->zErrMsg);
    pVtab->zErrMsg = sqlite3_mprintf("%s", message);
}

inline int return_vtab_error(sqlite3_vtab* pVtab) {
    const std::string& err = get_vtab_error();
    if (!err.empty() && pVtab) {
        set_vtab_errmsg(pVtab, err.c_str());
    }
    clear_vtab_error();
    return to_sqlite_status(Status::error);
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

// Internal filter for rowid equality constraints.
constexpr int ROWID_FILTER = -1;

// Internal scan mode for exact no-column full scans such as COUNT(*).
constexpr int COUNT_ONLY_SCAN = -2;

// Internal generator mode for required constraints that are missing.
constexpr int MISSING_REQUIRED_CONSTRAINT = -3;

// Index IDs start at INDEX_BASE (indexes are auto-generated filters). An
// equality index lookup uses idxNum = INDEX_BASE + index_pos.
constexpr int INDEX_BASE = 1000;

// Range (>=, >, <=, <, BETWEEN) pushdown on an indexed column. Encoded as
//   idxNum = RANGE_BASE + index_pos * RANGE_STRIDE + range_flags
// where range_flags is a 4-bit mask (RANGE_HAS_LOW/LOW_STRICT/HAS_HIGH/
// HIGH_STRICT). RANGE_BASE sits ABOVE the equality index space so the two never
// collide; index_pos * RANGE_STRIDE reserves 16 flag values per index. A range
// plan always sets idxNum >= RANGE_BASE > 0, so "VIRTUAL TABLE INDEX 0 = full
// scan" stays a valid discriminator and the EQ encoding (INDEX_BASE + pos) is
// unchanged. argv order for a range plan is [low?, high?] (low first when both
// bounds are present, matching how xBestIndex assigns argvIndex).
constexpr int RANGE_BASE = 2000;
constexpr int RANGE_STRIDE = 16;         // 4 flag bits per index slot
constexpr int RANGE_HAS_LOW = 0x1;       // a lower bound (>= or >) is present
constexpr int RANGE_LOW_STRICT = 0x2;    // lower bound is strict (>, not >=)
constexpr int RANGE_HAS_HIGH = 0x4;      // an upper bound (<= or <) is present
constexpr int RANGE_HIGH_STRICT = 0x8;   // upper bound is strict (<, not <=)
constexpr int RANGE_FLAG_MASK = RANGE_HAS_LOW | RANGE_LOW_STRICT |
                                RANGE_HAS_HIGH | RANGE_HIGH_STRICT;

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

    // Which SQLite constraint operator this filter matches. EQ filters are
    // applied with omit=1 (SQLite trusts the iterator). LIKE/GLOB filters are a
    // best-effort superset optimization applied with omit=0, so SQLite still
    // re-applies the real pattern test for correctness.
    int op = SQLITE_INDEX_CONSTRAINT_EQ;

    // Factory: create iterator for the given constraint value
    std::function<std::unique_ptr<RowIterator>(FunctionArg)> create;

    FilterDef(int col, int id, double cost, double rows,
              std::function<std::unique_ptr<RowIterator>(FunctionArg)> factory,
              int constraint_op = SQLITE_INDEX_CONSTRAINT_EQ)
        : column_index(col), filter_id(id), estimated_cost(cost),
          estimated_rows(rows), op(constraint_op), create(std::move(factory)) {}
};

namespace detail {
template<typename Columns, typename IsHiddenColumn>
inline std::string render_table_schema(const std::string& name,
                                       const Columns& columns,
                                       IsHiddenColumn is_hidden_column) {
    std::ostringstream ss;
    ss << "CREATE TABLE " << name << "(";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << "\"" << columns[i].name << "\" " << column_type_sql(columns[i].type);
        if (is_hidden_column(i)) ss << " HIDDEN";
    }
    ss << ")";
    return ss.str();
}

template<typename Columns>
inline std::string render_table_schema(const std::string& name, const Columns& columns) {
    return render_table_schema(name, columns, [](size_t) { return false; });
}

template<typename Columns>
inline int find_column_index(const Columns& columns, const std::string& col_name) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == col_name) return static_cast<int>(i);
    }
    return -1;
}

inline const FilterDef* find_filter_by_column(const std::vector<FilterDef>& filters,
                                              int col_index) {
    for (const auto& f : filters) {
        if (f.column_index == col_index) return &f;
    }
    return nullptr;
}

inline const FilterDef* find_filter_by_column_and_op(const std::vector<FilterDef>& filters,
                                                     int col_index,
                                                     int op) {
    for (const auto& f : filters) {
        if (f.column_index == col_index && f.op == op) return &f;
    }
    return nullptr;
}
} // namespace detail

// ============================================================================
// Virtual Table Definition
// ============================================================================

// One coherent transaction lifecycle for writable virtual tables.
//
// SQLite propagates xSync failures, but ignores xCommit/xRollback failures.
// Consequently prepare_commit/savepoint/release/rollback_to are fallible Status
// callbacks, while commit/rollback are deliberately infallible finish callbacks.
// Implementations of the latter must not throw; the C-boundary adapters still
// catch defensively so an accidental exception can never unwind through SQLite.
struct TransactionHooks {
    using State = std::shared_ptr<void>;

    // A definition may be registered on multiple SQLite connections. The
    // factory is run when the definition is cloned for one registration, so
    // staged backing state is connection-local even when the public definition
    // object is process-global and reused. A `state` assigned directly on the
    // definition (with no factory) is preserved by the clone and therefore
    // SHARED across registrations — set state_factory when each connection
    // needs its own instance.
    std::function<State()> state_factory;
    State state;
    std::function<Status(const State&)> prepare_commit;
    std::function<void(const State&)> commit;
    std::function<void(const State&)> rollback;
    std::function<Status(const State&, int)> savepoint;
    std::function<Status(const State&, int)> release;
    std::function<Status(const State&, int)> rollback_to;

    void initialize_state() {
        // Only a factory replaces the state: a caller-assigned `state` (already
        // copied from the source definition) must survive registration, not be
        // silently nulled — hooks dereference it inside SQLite callbacks.
        if (state_factory) {
            state = state_factory();
        }
    }
};

namespace detail {

struct TransactionState {
    struct Snapshot {
        bool touched = false;
        bool wrote = false;
        bool prepared = false;
    };

    // Set before invoking a mutation callback. Unlike `wrote`, this remains
    // true when a callback mutates external state and then reports failure, so
    // xRollback can still run the compensating hook.
    bool touched = false;
    bool wrote = false;
    bool prepared = false;
    std::unordered_map<int, Snapshot> savepoints;

    void reset() noexcept {
        touched = false;
        wrote = false;
        prepared = false;
        savepoints.clear();
    }
};

} // namespace detail

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

    TransactionHooks transaction_hooks;

    std::string schema() const {
        return detail::render_table_schema(name, columns);
    }

    // Find column index by name, -1 if not found
    int find_column(const std::string& col_name) const {
        return detail::find_column_index(columns, col_name);
    }

    // Find filter for given column, nullptr if none
    const FilterDef* find_filter(int col_index) const {
        return detail::find_filter_by_column(filters, col_index);
    }
};

// ============================================================================
// SQLite Virtual Table Implementation
// ============================================================================

struct Vtab {
    sqlite3_vtab base;
    const VTableDef* def;
    sqlite3* db = nullptr;
    std::string schema_name;
    std::string table_name;
    detail::TransactionState transaction;
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
inline int vtab_connect(sqlite3* db, void* pAux, int argc, const char* const* argv,
                        sqlite3_vtab** ppVtab, char**) {
    const VTableDef* def = static_cast<const VTableDef*>(pAux);

    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;

    auto* vtab = new Vtab();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    vtab->db = db;
    if (argc > 1 && argv && argv[1]) vtab->schema_name = argv[1];
    if (argc > 2 && argv && argv[2]) vtab->table_name = argv[2];
    *ppVtab = &vtab->base;
    detail::write_surface_connected(
        db, argc > 0 && argv ? argv[0] : nullptr,
        vtab->schema_name.c_str(), vtab->table_name.c_str());
    return to_sqlite_status(Status::ok);
}

// xDisconnect/xDestroy
inline int vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<Vtab*>(pVtab);
    return to_sqlite_status(Status::ok);
}

inline int vtab_destroy(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    detail::write_surface_destroyed(
        vtab->db, vtab->schema_name.c_str(), vtab->table_name.c_str());
    delete vtab;
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

namespace detail {

// Apply an UPDATE's column setters, shared by all three vtab modules. A write to a
// read-only column is rejected ("column X is read-only") rather than silently
// dropped -- SQLite marks unchanged columns NOCHANGE, so a non-NOCHANGE value on a
// non-writable column is a real write. The rejection is atomic (nothing applies if
// any target is read-only). `nochange_eligible` MUST be false on reconstruct-by-argv
// paths (update_from_column_values / no rowid resolution), where unchanged read-only
// identity columns carry their real value in argv and are skipped, not rejected.
// Setters apply in order and mutate live state that can't be cleanly rolled back, so
// a later failure names the columns already applied.
//
// col_writable(i): column i is writable (has a usable setter)
// col_name(i):     column i's name (const std::string&)
// apply_col(i):    invoke column i's setter with argv[i+2]; return its bool result
template <typename WritableFn, typename NameFn, typename ApplyFn>
inline Status apply_update_columns(sqlite3_vtab* pVtab, int argc,
                                   sqlite3_value** argv, size_t ncols,
                                   bool nochange_eligible, WritableFn&& col_writable,
                                   NameFn&& col_name, ApplyFn&& apply_col) {
    // A fully read-only table keeps its historical per-module behavior (the simple
    // VTableDef module no-ops such an UPDATE; the cached / row_lookup modules already
    // returned read_only before here), so gate the pre-pass on the table having at
    // least one writable column.
    bool has_writable = false;
    for (size_t c = 0; c < ncols; ++c) {
        if (col_writable(c)) { has_writable = true; break; }
    }

    // Reject read-only writes before mutating anything (atomic).
    if (nochange_eligible && has_writable) {
        for (int i = 2; i < argc && static_cast<size_t>(i - 2) < ncols; ++i) {
            if (sqlite3_value_nochange(argv[i])) continue;
            const size_t c = static_cast<size_t>(i - 2);
            if (!col_writable(c)) {
                set_vtab_errmsg(pVtab, ("column \"" + col_name(c) +
                                        "\" is read-only").c_str());
                return Status::error;
            }
        }
    }

    // Apply writable setters in order, tracking which applied for a partial-failure
    // message (the earlier ones can't be rolled back).
    std::string applied;
    for (int i = 2; i < argc && static_cast<size_t>(i - 2) < ncols; ++i) {
        if (sqlite3_value_nochange(argv[i])) continue;
        const size_t c = static_cast<size_t>(i - 2);
        if (!col_writable(c)) continue;  // read-only on a non-eligible path: skip
        clear_vtab_error();
        if (!apply_col(c)) {
            std::string err = get_vtab_error();
            if (err.empty())
                err = "UPDATE setter failed for column \"" + col_name(c) + "\"";
            if (!applied.empty())
                err += " (partial UPDATE: column(s) [" + applied +
                       "] were already applied and cannot be rolled back)";
            set_vtab_errmsg(pVtab, err.c_str());
            clear_vtab_error();
            return Status::error;
        }
        if (!applied.empty()) applied += ", ";
        applied += col_name(c);
    }
    return Status::ok;
}

// --- Capability-scoped errors for unsupported write surfaces -----------------
//
// A write to a surface/column that has no mutation support must NOT surface as
// SQLite's generic "attempt to write a readonly database" — that implies the
// whole database is read-only when in fact it is writable and only THIS surface
// is not. Compose an actionable message naming the surface and the missing
// capability (mutation.<table>.<leaf>, the convention every tool uses in its
// `capabilities` table), and return SQLITE_ERROR (not SQLITE_READONLY) with it.
// This is the matched-row half of the fix; the prepare-time authorizer produces
// the same message for the 0-row case (except that a column-scoped UPDATE
// denial names the offending column, which only prepare time knows).
inline int unsupported_write_error(sqlite3_vtab* pVtab, std::string phrase,
                                   const std::string& table, const char* leaf) {
    phrase += " (capability mutation." + table + "." + leaf + " is unavailable)";
    set_vtab_errmsg(pVtab, phrase.c_str());
    return to_sqlite_status(Status::error);
}
inline int unsupported_insert(sqlite3_vtab* pVtab, const std::string& table) {
    return unsupported_write_error(pVtab, "INSERT INTO " + table + " is not supported",
                                   table, "insert");
}
inline int unsupported_delete(sqlite3_vtab* pVtab, const std::string& table) {
    return unsupported_write_error(pVtab, "DELETE FROM " + table + " is not supported",
                                   table, "delete");
}
inline int unsupported_update(sqlite3_vtab* pVtab, const std::string& table) {
    return unsupported_write_error(pVtab, "UPDATE " + table + " is not supported",
                                   table, "*");
}

// Names of columns with usable setters. SQLite's prepare-time authorizer uses
// these to reject even a zero-row UPDATE of a read-only column.
template <typename Def>
inline std::vector<std::string> def_writable_columns(const Def* def) {
    std::vector<std::string> out;
    for (const auto& c : def->columns) {
        if (c.writable && static_cast<bool>(c.set)) out.push_back(c.name);
    }
    return out;
}

}  // namespace detail

// xUpdate - handles INSERT, UPDATE, DELETE
inline int vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    const VTableDef* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row) {
            return detail::unsupported_delete(pVtab, def->name);
        }

        // A negative rowid cannot map to a valid 0-based row index; reject it
        // before it wraps to a huge size_t handed to the host delete callback.
        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        if (raw_rowid < 0) {
            return to_sqlite_status(Status::error);
        }
        size_t rowid = static_cast<size_t>(raw_rowid);

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        if (!def->delete_row(rowid)) {
            return to_sqlite_status(Status::error);
        }
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] != NULL: UPDATE
    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        // A negative rowid cannot map to a valid 0-based row index; reject it
        // before it wraps to a huge size_t handed to the host column setters.
        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        if (raw_rowid < 0) {
            return to_sqlite_status(Status::error);
        }
        size_t old_rowid = static_cast<size_t>(raw_rowid);

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        // This module's xColumn always honors NOCHANGE, so a non-NOCHANGE value on a
        // read-only column is a genuine write. See detail::apply_update_columns.
        const Status st = detail::apply_update_columns(
            pVtab, argc, argv, def->columns.size(), /*nochange_eligible=*/true,
            [&](size_t c) { return def->columns[c].writable && static_cast<bool>(def->columns[c].set); },
            [&](size_t c) -> const std::string& { return def->columns[c].name; },
            [&](size_t c) { return def->columns[c].set(old_rowid, FunctionArg(argv[c + 2])); });
        if (st == Status::ok) vtab->transaction.wrote = true;
        return to_sqlite_status(st);
    }

    // argc > 1, argv[0] == NULL: INSERT
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        if (!def->supports_insert || !def->insert_row) {
            return detail::unsupported_insert(pVtab, def->name);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("INSERT INTO " + def->name);
        }

        // Pass column values starting at argv[2] (argv[0]=NULL, argv[1]=rowid)
        bool ok = false;
        clear_vtab_error();
        detail::with_args(argc - 2, &argv[2], [&](FunctionArg* args) {
            ok = def->insert_row(argc - 2, args);
        });
        if (!ok) {
            return return_vtab_error(pVtab);
        }
        clear_vtab_error();
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    return to_sqlite_status(Status::read_only);
}

// ============================================================================
// Transaction hook plumbing (shared by all three module flavors)
// ============================================================================
//
// SQLite only enrolls a vtab into the connection's transaction set (and thus
// only ever calls xSync/xCommit/xRollback on it) if the module provides a
// non-null xBegin that returns SQLITE_OK (sqlite3VtabBegin -> addToVTrans).
// So xBegin is MANDATORY as the enabler. The fallible prepare_commit hook fires
// in xSync because SQLite propagates xSync errors but discards xCommit's return.
//
// A read-only SELECT never enrolls the vtab, but a schema write (CREATE VIRTUAL
// TABLE) DOES enroll it and drives a full begin/commit cycle. A successful
// xUpdate sets TransactionState::wrote. Crucially, xSync does NOT clear it: if a
// later participant's xSync fails, SQLite calls xRollback on every participant
// and an already-prepared table must still discard its staged external state.
template <typename VtabT>
inline int vtab_xbegin(sqlite3_vtab* pVtab) {
    if (auto* vtab = reinterpret_cast<VtabT*>(pVtab)) {
        vtab->transaction.reset();
    }
    return to_sqlite_status(Status::ok);
}

inline int return_transaction_hook_status(sqlite3_vtab* pVtab, Status status,
                                          const char* hook_name) {
    if (status == Status::ok) {
        clear_vtab_error();
        return to_sqlite_status(Status::ok);
    }
    const std::string message = get_vtab_error().empty()
        ? std::string(hook_name) + " transaction hook failed"
        : get_vtab_error();
    set_vtab_errmsg(pVtab, message.c_str());
    clear_vtab_error();
    return to_sqlite_status(status);
}

template <typename VtabT>
inline int vtab_prepare_commit(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab || !vtab->transaction.wrote || vtab->transaction.prepared) {
        return to_sqlite_status(Status::ok);
    }
    if (vtab->def && vtab->def->transaction_hooks.prepare_commit) {
        clear_vtab_error();
        try {
            const Status status =
                vtab->def->transaction_hooks.prepare_commit(
                    vtab->def->transaction_hooks.state);
            if (status != Status::ok) {
                return return_transaction_hook_status(pVtab, status, "prepare_commit");
            }
        } catch (const std::exception& e) {
            set_vtab_error(e.what());
            return return_vtab_error(pVtab);
        } catch (...) {
            set_vtab_error("prepare_commit transaction hook threw a non-standard exception");
            return return_vtab_error(pVtab);
        }
        clear_vtab_error();
    }
    vtab->transaction.prepared = true;
    return to_sqlite_status(Status::ok);
}

template <typename VtabT>
inline int vtab_finish_commit(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab) {
        return to_sqlite_status(Status::ok);
    }
    if (vtab->transaction.wrote && vtab->def &&
        vtab->def->transaction_hooks.commit) {
        try {
            vtab->def->transaction_hooks.commit(
                vtab->def->transaction_hooks.state);
        } catch (...) {
            // No usable failure channel exists here. The public contract makes
            // commit infallible; contain accidental violations at the boundary.
        }
    }
    vtab->transaction.reset();
    clear_vtab_error();
    return to_sqlite_status(Status::ok);
}

template <typename VtabT>
inline int vtab_xrollback(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab) {
        return to_sqlite_status(Status::ok);
    }
    if (vtab->transaction.touched && vtab->def &&
        vtab->def->transaction_hooks.rollback) {
        try {
            vtab->def->transaction_hooks.rollback(
                vtab->def->transaction_hooks.state);
        } catch (...) {
            // No usable failure channel exists here. The public contract makes
            // rollback infallible; contain accidental violations at the boundary.
        }
    }
    vtab->transaction.reset();
    clear_vtab_error();
    return to_sqlite_status(Status::ok);
}

template <typename VtabT>
inline int vtab_run_savepoint_hook(
    sqlite3_vtab* pVtab, int savepoint,
    const std::function<Status(const TransactionHooks::State&, int)>
        TransactionHooks::*hook_member,
    const char* hook_name) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab || !vtab->def)
        return to_sqlite_status(Status::ok);
    const auto& hook = vtab->def->transaction_hooks.*hook_member;
    if (!hook)
        return to_sqlite_status(Status::ok);
    clear_vtab_error();
    try {
        const Status status =
            hook(vtab->def->transaction_hooks.state, savepoint);
        if (status != Status::ok) {
            return return_transaction_hook_status(pVtab, status, hook_name);
        }
    } catch (const std::exception& e) {
        set_vtab_error(e.what());
        return return_vtab_error(pVtab);
    } catch (...) {
        set_vtab_error(std::string(hook_name) + " hook threw a non-standard exception");
        return return_vtab_error(pVtab);
    }
    clear_vtab_error();
    return to_sqlite_status(Status::ok);
}

template <typename VtabT> inline int vtab_xsavepoint(sqlite3_vtab* pVtab, int savepoint) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab)
        return to_sqlite_status(Status::ok);
    vtab->transaction.savepoints[savepoint] = {
        vtab->transaction.touched, vtab->transaction.wrote,
        vtab->transaction.prepared};
    const int rc =
        vtab_run_savepoint_hook<VtabT>(
            pVtab, savepoint, &TransactionHooks::savepoint, "savepoint");
    if (rc != to_sqlite_status(Status::ok)) {
        vtab->transaction.savepoints.erase(savepoint);
    }
    return rc;
}

template <typename VtabT> inline int vtab_xrelease(sqlite3_vtab* pVtab, int savepoint) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab)
        return to_sqlite_status(Status::ok);
    const int rc =
        vtab_run_savepoint_hook<VtabT>(
            pVtab, savepoint, &TransactionHooks::release, "release");
    if (rc != to_sqlite_status(Status::ok))
        return rc;
    for (auto it = vtab->transaction.savepoints.begin();
         it != vtab->transaction.savepoints.end();) {
        if (it->first >= savepoint) {
            it = vtab->transaction.savepoints.erase(it);
        } else {
            ++it;
        }
    }
    return rc;
}

template <typename VtabT> inline int vtab_xrollback_to(sqlite3_vtab* pVtab, int savepoint) {
    auto* vtab = reinterpret_cast<VtabT*>(pVtab);
    if (!vtab)
        return to_sqlite_status(Status::ok);
    const int rc =
        vtab_run_savepoint_hook<VtabT>(
            pVtab, savepoint, &TransactionHooks::rollback_to, "rollback_to");
    if (rc != to_sqlite_status(Status::ok))
        return rc;
    const auto snapshot = vtab->transaction.savepoints.find(savepoint);
    if (snapshot != vtab->transaction.savepoints.end()) {
        vtab->transaction.touched = snapshot->second.touched;
        vtab->transaction.wrote = snapshot->second.wrote;
        vtab->transaction.prepared = snapshot->second.prepared;
    } else {
        // SQLite may enlist a virtual table only after a SAVEPOINT was opened.
        // In that case xSavepoint was never delivered to this module, and every
        // write it has seen is necessarily newer than the rollback target.
        vtab->transaction.touched = false;
        vtab->transaction.wrote = false;
        vtab->transaction.prepared = false;
    }
    for (auto it = vtab->transaction.savepoints.begin();
         it != vtab->transaction.savepoints.end();) {
        if (it->first > savepoint) {
            it = vtab->transaction.savepoints.erase(it);
        } else {
            ++it;
        }
    }
    return rc;
}

// Create module with xUpdate support
inline sqlite3_module create_module() {
    sqlite3_module mod = {};
    mod.iVersion = 3;
    mod.xCreate = vtab_connect;
    mod.xConnect = vtab_connect;
    mod.xBestIndex = vtab_best_index;
    mod.xDisconnect = vtab_disconnect;
    mod.xDestroy = vtab_destroy;
    mod.xOpen = vtab_open;
    mod.xClose = vtab_close;
    mod.xFilter = vtab_filter;
    mod.xNext = vtab_next;
    mod.xEof = vtab_eof;
    mod.xColumn = vtab_column;
    mod.xRowid = vtab_rowid;
    mod.xUpdate = vtab_update;
    // xBegin enrolls the vtab; xSync runs the fallible hook; xCommit clears state.
    mod.xBegin = vtab_xbegin<Vtab>;
    mod.xSync = vtab_prepare_commit<Vtab>;
    mod.xCommit = vtab_finish_commit<Vtab>;
    mod.xRollback = vtab_xrollback<Vtab>;
    mod.xSavepoint = vtab_xsavepoint<Vtab>;
    mod.xRelease = vtab_xrelease<Vtab>;
    mod.xRollbackTo = vtab_xrollback_to<Vtab>;
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
        // Do NOT delete `owned` here: sqlite3_create_module_v2 invokes the
        // xDestroy callback (destroy_def) even when registration fails, which
        // already deletes the clone. A manual delete would be a double-free.
        // (See SQLite docs: the destructor "is also invoked if the call ...
        // fails" — same rule as sqlite3_create_function_v2.)
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
    if (!detail::register_vtable_sqlite(db.sqlite_handle(), module_name, def)) return false;
    db.record_write_surface(module_name,
                            def->supports_insert && static_cast<bool>(def->insert_row),
                            def->supports_delete && static_cast<bool>(def->delete_row),
                            detail::def_writable_columns(def));
    return true;
}

inline bool create_vtable(Database& db, const char* table_name, const char* module_name) {
    return detail::create_vtable_sqlite(db.sqlite_handle(), table_name, module_name);
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

    // Exact row count of the table (required: rows are addressed by index).
    VTableBuilder& row_count(std::function<size_t()> fn) {
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

    VTableBuilder& transaction_hooks(TransactionHooks hooks) {
        def_.transaction_hooks = std::move(hooks);
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

    VTableBuilder& column_text_nullable_rw(
                                   const char* name,
                                   std::function<std::optional<std::string>(size_t)> getter,
                                   std::function<bool(size_t, FunctionArg)> setter) {
        def_.columns.emplace_back(name, ColumnType::Text, true,
            [getter = std::move(getter)](FunctionContext& ctx, size_t idx) {
                auto value = getter(idx);
                if (value.has_value()) {
                    ctx.result_text(*value);
                } else {
                    ctx.result_null();
                }
            },
            std::move(setter));
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

namespace detail {
template<typename RowData>
inline CachedColumnDef<RowData> make_row_column(
        const char* name,
        ColumnType type,
        bool writable,
        std::function<void(FunctionContext&, const RowData&)> getter,
        std::function<bool(RowData&, FunctionArg)> setter = nullptr) {
    return CachedColumnDef<RowData>(
        name, type, writable, std::move(getter), std::move(setter));
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_int64(
        std::function<int64_t(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        ctx.result_int64(getter(row));
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_int(
        std::function<int(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        ctx.result_int(getter(row));
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_text(
        std::function<std::string(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        ctx.result_text(getter(row));
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_nullable_text(
        std::function<std::optional<std::string>(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        auto value = getter(row);
        if (value.has_value()) {
            ctx.result_text(*value);
        } else {
            ctx.result_null();
        }
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_nullable_int(
        std::function<std::optional<int>(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        auto value = getter(row);
        if (value.has_value()) {
            ctx.result_int(*value);
        } else {
            ctx.result_null();
        }
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_double(
        std::function<double(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        ctx.result_double(getter(row));
    };
}

template<typename RowData>
inline std::function<void(FunctionContext&, const RowData&)> row_getter_blob(
        std::function<std::vector<uint8_t>(const RowData&)> getter) {
    return [getter = std::move(getter)](FunctionContext& ctx, const RowData& row) {
        auto val = getter(row);
        ctx.result_blob(val.data(), val.size());
    };
}

template<typename RowData>
inline std::function<bool(RowData&, FunctionArg)> row_setter_int64(
        std::function<bool(RowData&, int64_t)> setter) {
    return [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
        return setter(row, val.as_int64());
    };
}

template<typename RowData>
inline std::function<bool(RowData&, FunctionArg)> row_setter_int(
        std::function<bool(RowData&, int)> setter) {
    return [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
        return setter(row, val.as_int());
    };
}

template<typename RowData>
inline std::function<bool(RowData&, FunctionArg)> row_setter_text(
        std::function<bool(RowData&, const char*)> setter) {
    return [setter = std::move(setter)](RowData& row, FunctionArg val) -> bool {
        const char* text = val.as_c_str();
        return setter(row, text ? text : "");
    };
}
} // namespace detail

// Index definition for cached tables
struct CachedIndexDef {
    int column_index;                                        // Which column is indexed
    std::function<int64_t(const void*)> key_extractor;       // Extract key from row (type-erased)
};

// A per-index sorted view used ONLY for range (>=, >, <=, <, BETWEEN) pushdown.
// The hash `indexes` above stays the sole structure for the hot equality path;
// this parallel structure is built LAZILY on the first range query for an index
// and then cached alongside the hash index (invalidated on the same rebuilds).
// It is a sorted vector of (key, row-position) pairs so lower_bound/upper_bound
// can carve out the matching window; duplicate keys sort together, so a
// multi-match key is a contiguous run. `built` gates a one-time construction.
struct SortedIndex {
    std::vector<std::pair<int64_t, size_t>> entries;  // sorted by key ascending
    bool built = false;
};

// Shared cache with indexes - lazily built, shared across all cursors
template<typename RowData>
struct SharedCache {
    std::vector<RowData> data;
    // Map from column value -> list of row indices in data
    std::vector<std::unordered_map<int64_t, std::vector<size_t>>> indexes;
    // Parallel sorted views for range pushdown, one slot per hash index. Built on
    // demand (first range query) under `mutex`, cleared whenever `indexes` is.
    std::vector<SortedIndex> sorted_indexes;
    bool built = false;
    // Preserves row data while SQLite applies a scan-driven UPDATE/DELETE.
    bool mutation_snapshot = false;
    mutable std::mutex mutex;
};

template<typename RowData>
struct CachedTableDef {
    std::string name;
    std::function<size_t()> estimate_rows_fn;
    std::function<size_t()> row_count_fn;
    std::function<void(std::vector<RowData>&)> cache_builder_fn;
    std::function<void(
        const TransactionHooks::State&, std::vector<RowData>&)>
        stateful_cache_builder_fn;

    // Optional projection-aware variant for query-scoped (no_shared_cache)
    // tables. Receives SQLite's colUsed bitmask (bit i => column i is read by
    // the query; bit 63 => "column >= 63 or unknown, assume all"). The builder
    // may skip materializing expensive columns the query doesn't select (e.g.
    // a large text blob for a COUNT/aggregate). When set, it is used INSTEAD of
    // cache_builder_fn for the non-shared full-scan build. It is NOT used for
    // shared caches (a cross-query cache can't depend on one query's colUsed),
    // preserving correctness and the query-scoped-only cache model.
    std::function<void(std::vector<RowData>&, uint64_t)> projection_cache_builder_fn;
    std::vector<CachedColumnDef<RowData>> columns;
    std::vector<FilterDef> filters;
    std::function<bool(RowData&)> delete_row;
    bool supports_delete = false;
    std::function<bool(int argc, FunctionArg* argv)> insert_row;
    bool supports_insert = false;
    std::function<void(const std::string&)> before_modify;
    std::function<void(const std::string&)> after_modify;

    TransactionHooks transaction_hooks;

    // Populate a RowData from xUpdate argv values (argv[2..] = column values).
    // Used when the shared cache is not available (e.g., filter_eq path).
    // If not set, UPDATE only works when the shared cache contains the row.
    std::function<void(RowData&, int argc, FunctionArg* argv)> row_from_argv;

    // Optional row lookup by rowid for UPDATE/DELETE fallback.
    // Useful for filter iterators whose rowid is not a positional index.
    std::function<bool(RowData&, int64_t)> row_lookup;

    // Optional stable rowid for a row. When set, the full-scan and index cursors
    // report rowid_fn(row) instead of the cache position, so the rowid is
    // consistent with what filter iterators return (e.g. an ordinal/ea key). This
    // is required for tables whose filter iterators key by a stable id AND whose
    // UPDATE/DELETE reconstruct via row_lookup(that id): without it a full-scan
    // rowid (cache position) and an iterator rowid (the key) would disagree.
    std::function<int64_t(const RowData&)> rowid_fn;

    // Opt-IN: extend the query-scoped pre-mutation snapshot (see
    // query_scoped_uses_mutation_snapshot) to a table that resolves its rowid via
    // rowid_fn (and possibly row_lookup / a filter_eq iterator) rather than by
    // cache position. Set this for a no_shared_cache table whose rowid encodes a
    // SHIFTING identity -- e.g. types_members' packed (ordinal, member_index):
    // once an earlier row in a multi-row UPDATE/DELETE moves/erases a member, the
    // remaining scan rowids no longer map to their original members via LIVE
    // row_lookup. With this flag the framework snapshots the full pre-mutation row
    // set once per statement and resolves each rowid by matching rowid_fn over that
    // stable snapshot; the delete/update handler then re-resolves the live object
    // from the snapshot row's stable key (name/offset), so a member renamed within
    // the same statement fails honestly instead of hitting the wrong member.
    // Requires rowid_fn. No effect on shared-cache or read-only tables.
    bool snapshot_mutations = false;

    // Opt-OUT of rowid-based UPDATE reconstruction + the SQLITE_NOCHANGE
    // optimization. Set this when the table's rowid is NOT a reliable lookup key
    // for UPDATE -- i.e. row_lookup() cannot resolve every rowid the scans
    // produce to the exact row (full-scan position vs key mismatch, or a filter
    // iterator whose rowid is func-local while row_lookup() expects a global
    // index). For such tables the framework must reconstruct from the REAL
    // column values in argv (row_from_argv) and therefore must NOT report
    // unchanged columns as NOCHANGE (which would feed row_from_argv 0/NULL for
    // unchanged identity fields like ea or func_addr). Default OFF: the post-
    // 1ceb960 behavior (NOCHANGE on, row_lookup-first) used by tables whose
    // filter iterators round-trip through row_lookup() (e.g. types_members) and
    // by tables with many/paired writable columns that must skip unchanged ones.
    bool update_from_column_values = false;

    // Index definitions: column index -> key extractor
    std::vector<std::pair<int, std::function<int64_t(const RowData&)>>> index_defs;

    // Shared cache - lazily built on first query, shared across all cursors for the
    // engine's lifetime. This is an OPT-OUT model: the default is true, so a table
    // caches across queries unless it calls .no_shared_cache(). Correct only over
    // IMMUTABLE data (e.g. clangsql AST, dwarfsql DWARF) or static rows; a table over
    // MUTABLE engine state MUST opt out with .no_shared_cache() (query-scoped, rebuilt
    // per statement) or it will serve stale rows after out-of-band edits. (bnsql hit
    // exactly this and was reverted to query-scoped for every mutable table.)
    bool use_shared_cache = true;
    mutable std::shared_ptr<SharedCache<RowData>> shared_cache;

    std::string schema() const {
        return detail::render_table_schema(name, columns);
    }

    int find_column(const std::string& col_name) const {
        return detail::find_column_index(columns, col_name);
    }

    // Find a filter for (column, constraint-op). A column may carry more than one
    // filter (e.g. an EQ filter and a LIKE/prefix filter on the same column), so
    // the operator must be matched too.
    const FilterDef* find_filter(int col_index, int op = SQLITE_INDEX_CONSTRAINT_EQ) const {
        return detail::find_filter_by_column_and_op(filters, col_index, op);
    }

    // Find index position for a column (-1 if not indexed)
    int find_index(int col_index) const {
        for (size_t i = 0; i < index_defs.size(); ++i) {
            if (index_defs[i].first == col_index) return static_cast<int>(i);
        }
        return -1;
    }

    void build_rows(std::vector<RowData>& rows) const {
        if (stateful_cache_builder_fn) {
            stateful_cache_builder_fn(transaction_hooks.state, rows);
        } else if (cache_builder_fn) {
            cache_builder_fn(rows);
        }
    }

    bool has_cache_builder() const noexcept {
        return static_cast<bool>(stateful_cache_builder_fn) ||
               static_cast<bool>(cache_builder_fn);
    }

    // Ensure shared cache is built (thread-safe, lazy initialization)
    void ensure_cache_built() const {
        if (!use_shared_cache) return;
        if (!shared_cache) {
            shared_cache = std::make_shared<SharedCache<RowData>>();
        }
        std::lock_guard<std::mutex> lock(shared_cache->mutex);
        if (shared_cache->built) return;

        shared_cache->data.clear();
        shared_cache->indexes.clear();
        shared_cache->sorted_indexes.clear();
        shared_cache->mutation_snapshot = false;

        // Build the cache
        if (stateful_cache_builder_fn || cache_builder_fn) {
            build_rows(shared_cache->data);
            if (!get_vtab_error().empty()) {
                shared_cache->data.clear();
                shared_cache->indexes.clear();
                shared_cache->sorted_indexes.clear();
                shared_cache->built = false;
                shared_cache->mutation_snapshot = false;
                return;
            }
        }

        // Build the hash (equality) indexes eagerly -- the hot point-lookup path.
        // The parallel sorted (range) views are built lazily on the first range query
        // for each index, inline in xFilter's RANGE_BASE branch under this same cache
        // mutex; here we only size the slot vector so those slots exist.
        shared_cache->indexes.resize(index_defs.size());
        shared_cache->sorted_indexes.assign(index_defs.size(), SortedIndex{});
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
            shared_cache->sorted_indexes.clear();
            shared_cache->built = false;
            shared_cache->mutation_snapshot = false;
        }
    }
};

namespace detail {
template<typename RowData>
inline bool cached_table_has_scan_driven_mutation(const CachedTableDef<RowData>* def) {
    if (!def) return false;
    if (def->supports_delete) return true;
    for (const auto& col : def->columns) {
        if (col.writable && col.set) return true;
    }
    return false;
}

template<typename RowData>
inline bool cached_table_supports_count_only_scan(const CachedTableDef<RowData>* def) {
    return def && def->row_count_fn && !def->rowid_fn &&
           !cached_table_has_scan_driven_mutation(def);
}

template<typename RowData>
inline void cached_table_invalidate_after_mutation(const CachedTableDef<RowData>* def) {
    if (!def || !def->shared_cache) return;
    std::lock_guard<std::mutex> lock(def->shared_cache->mutex);
    def->shared_cache->indexes.clear();
    def->shared_cache->sorted_indexes.clear();
    def->shared_cache->built = false;
    def->shared_cache->mutation_snapshot = !def->shared_cache->data.empty();
}

// Query-scoped scan-driven mutation support. A no_shared_cache table has no
// stable row set across a multi-row DELETE/UPDATE: each xUpdate would otherwise
// rebuild from the ALREADY-mutated state, stranding the scan's positional rowids
// (rebuild-during-mutation is also what crashes engines like BN when a per-row
// delete redefines a type mid-rebuild). For POSITIONAL query-scoped tables (no
// row_lookup, no rowid_fn), snapshot the full pre-mutation row set ONCE into the
// otherwise-unused shared_cache slot; the reconstruction branch in
// cached_vtab_update reads positions from this stable snapshot -- mirroring
// SharedCache::mutation_snapshot for shared tables. It is invisible to the read
// path (xColumn/xRowid gate shared reads on use_shared_cache) and is dropped at
// the next scan (cached_vtab_filter) = next statement. No-op for shared tables,
// read-only tables, and tables that reconstruct via row_lookup/rowid_fn.
template<typename RowData>
inline bool query_scoped_uses_mutation_snapshot(const CachedTableDef<RowData>* def) {
    if (!def || def->use_shared_cache || !def->has_cache_builder() ||
        !cached_table_has_scan_driven_mutation(def)) {
        return false;
    }
    // Opt-in path: a table that resolves its rowid via rowid_fn matches the
    // snapshot by KEY (rowid_fn over the pre-mutation rows), not by position, so
    // it works even with a filter_eq iterator / row_lookup whose rowid is not a
    // cache position. This is the fix for shifting-identity rowids (e.g.
    // types_members' packed (ordinal, member_index)).
    if (def->snapshot_mutations && def->rowid_fn) {
        return true;
    }
    // Default positional path: full-scan rowid == cache position. A filter_eq
    // iterator or a rowid_fn would make that rowid non-positional, so a positional
    // snapshot would resolve the wrong row -- those tables keep the per-xUpdate
    // rebuild path unless they opt in above.
    return !def->row_lookup && !def->rowid_fn && def->filters.empty();
}

template<typename RowData>
inline bool ensure_query_scoped_mutation_snapshot(const CachedTableDef<RowData>* def) {
    if (!query_scoped_uses_mutation_snapshot(def)) return true;
    if (!def->shared_cache) {
        def->shared_cache = std::make_shared<SharedCache<RowData>>();
    }
    auto& snap = *def->shared_cache;
    std::lock_guard<std::mutex> lock(snap.mutex);
    if (!snap.mutation_snapshot) {
        snap.data.clear();
        clear_vtab_error();
        def->build_rows(snap.data);
        if (!get_vtab_error().empty()) {
            snap.data.clear();
            snap.indexes.clear();
            snap.mutation_snapshot = false;
            snap.built = false;
            return false;
        }
        snap.mutation_snapshot = true;
        snap.built = false;
    }
    return true;
}

template<typename RowData>
inline void clear_query_scoped_mutation_snapshot(const CachedTableDef<RowData>* def) {
    if (!def || def->use_shared_cache || !def->shared_cache) return;
    std::lock_guard<std::mutex> lock(def->shared_cache->mutex);
    if (def->shared_cache->mutation_snapshot) {
        def->shared_cache->mutation_snapshot = false;
        def->shared_cache->data.clear();
    }
}

// Decide whether a LIKE/GLOB constraint is worth routing to a prefix filter:
// true iff the pattern has a usable literal prefix (its first character is not a
// wildcard/escape). If the right-hand side is not available at plan time (e.g. a
// bound parameter), return true and let the iterator extract the prefix at filter
// time -- it is always a correct superset, so the worst case is a full walk.
inline bool like_constraint_has_usable_prefix(sqlite3_index_info* pInfo, int i) {
    sqlite3_value* rhs = nullptr;
    int rc = sqlite3_vtab_rhs_value(pInfo, i, &rhs);
    if (rc != SQLITE_OK || rhs == nullptr) {
        return true;  // unknown at plan time -> claim; iterator stays a superset
    }
    const unsigned char* text = sqlite3_value_text(rhs);
    if (!text) return false;
    const char c0 = static_cast<char>(text[0]);
    return c0 != '\0' && c0 != '%' && c0 != '_' && c0 != '\\';
}

// Build (once) the sorted (key,row) view for range pushdown from a data vector +
// its key extractor. Entries are sorted ascending by key; a stable sort keeps a
// deterministic per-key order (matching the data order for equal keys), so the
// window a range carves is reproducible. Caller holds the cache/cursor lock.
template<typename RowData>
inline void build_sorted_index(SortedIndex& sorted,
                               const std::vector<RowData>& data,
                               const std::function<int64_t(const RowData&)>& key_fn) {
    sorted.entries.clear();
    sorted.entries.reserve(data.size());
    for (size_t row = 0; row < data.size(); ++row) {
        sorted.entries.emplace_back(key_fn(data[row]), row);
    }
    std::stable_sort(sorted.entries.begin(), sorted.entries.end(),
                     [](const std::pair<int64_t, size_t>& a,
                        const std::pair<int64_t, size_t>& b) {
                         return a.first < b.first;
                     });
    sorted.built = true;
}

// Carve the [begin,end) window from a built sorted index honoring the range
// flags + bounds and append the matching row positions to `out`. `low`/`high`
// are only read when the corresponding RANGE_HAS_* flag is set.
inline void collect_range_matches(const SortedIndex& sorted, int range_flags,
                                  int64_t low, int64_t high,
                                  std::vector<size_t>& out) {
    const auto key_less = [](const std::pair<int64_t, size_t>& e, int64_t k) {
        return e.first < k;
    };
    const auto key_greater = [](int64_t k, const std::pair<int64_t, size_t>& e) {
        return k < e.first;
    };
    auto it_begin = sorted.entries.begin();
    auto it_end = sorted.entries.end();
    if (range_flags & RANGE_HAS_LOW) {
        // >= low : first key not < low.  > low : first key not <= low (i.e. > low).
        it_begin = std::lower_bound(sorted.entries.begin(), sorted.entries.end(),
                                    low, key_less);
        if (range_flags & RANGE_LOW_STRICT) {
            // Skip the run equal to `low` (strict >): advance to first key > low.
            it_begin = std::upper_bound(it_begin, sorted.entries.end(), low,
                                        key_greater);
        }
    }
    if (range_flags & RANGE_HAS_HIGH) {
        // <= high : one past the last key not > high (upper_bound of high).
        // < high  : first key not < high (lower_bound of high).
        it_end = (range_flags & RANGE_HIGH_STRICT)
                     ? std::lower_bound(it_begin, sorted.entries.end(), high, key_less)
                     : std::upper_bound(it_begin, sorted.entries.end(), high, key_greater);
    }
    for (auto it = it_begin; it != it_end; ++it) {
        out.push_back(it->second);
    }
}
} // namespace detail

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

    // Rowid equality lookup
    bool using_rowid_lookup = false;
    bool rowid_lookup_eof = false;
    RowData rowid_lookup_row{};
    int64_t rowid_lookup_id = 0;

    // Index-based iteration
    bool using_index = false;
    const std::vector<size_t>* index_matches = nullptr;  // Points into *_indexes below
    size_t index_pos = 0;

    // Range (>=, >, <=, <, BETWEEN) window: a cursor-owned list of row positions
    // carved from the sorted index. When a range plan runs, index_matches points
    // at this vector so xNext/xColumn/xRowid reuse the existing index machinery.
    std::vector<size_t> range_matches;
    // Row data the index positions point into: shared_cache->data (shared tables)
    // or index_cache (query-scoped tables). Set per xFilter.
    const std::vector<RowData>* index_data_source = nullptr;

    // Per-cursor index cache (query-scoped tables): built once on the first index
    // xFilter and REUSED across subsequent xFilter calls on this cursor (a JOIN's
    // inner-loop reuses one cursor), so a query-scoped table stays fast in a JOIN
    // without an engine-lifetime shared cache. Persists across xFilter (not reset
    // at the top); dropped at xClose with the cursor. Freshness is per statement:
    // a new statement opens a new cursor, rebuilding from the live source.
    std::vector<RowData> index_cache;
    std::vector<std::unordered_map<int64_t, std::vector<size_t>>> cursor_indexes;
    bool cursor_index_built = false;

    // Cursor-owned copy of a shared-cache EQ lookup's matched rows. The old code
    // stored pointers into the shared index map + data and read them across the
    // statement without the cache mutex, so a concurrent invalidate/rebuild freed
    // them (use-after-free). Copying under the lock keeps no shared pointer alive
    // past xFilter; EQ matched-sets are small, so the copy is cheap.
    std::vector<RowData> index_row_copy;
    // Parallel sorted views for range pushdown on a query-scoped table, one slot
    // per index, built lazily on the first range query against this cursor and
    // reused thereafter (like cursor_indexes). Dropped with the cursor.
    std::vector<SortedIndex> cursor_sorted_indexes;

    // Exact count-only/no-column full scan
    bool using_count_only = false;
    size_t count_only_total = 0;
};

template<typename RowData>
struct CachedVtab {
    sqlite3_vtab base;
    const CachedTableDef<RowData>* def;
    sqlite3* db = nullptr;
    std::string schema_name;
    std::string table_name;
    detail::TransactionState transaction;
    // NOTE: per-query projection state (colUsed) is NOT stored here. SQLite
    // reuses one vtab object across cursors, table aliases, and interleaved
    // prepared statements, so stashing colUsed on the vtab lets one scan clobber
    // another's projection. It is passed per plan via idxStr (xBestIndex ->
    // xFilter) instead.
};

// SQLite callbacks for cached tables
template<typename RowData>
inline int cached_vtab_connect(sqlite3* db, void* pAux, int argc,
                               const char* const* argv,
                               sqlite3_vtab** ppVtab, char**) {
    const auto* def = static_cast<const CachedTableDef<RowData>*>(pAux);
    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;
    auto* vtab = new CachedVtab<RowData>();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    vtab->db = db;
    if (argc > 1 && argv && argv[1]) vtab->schema_name = argv[1];
    if (argc > 2 && argv && argv[2]) vtab->table_name = argv[2];
    *ppVtab = &vtab->base;
    detail::write_surface_connected(
        db, argc > 0 && argv ? argv[0] : nullptr,
        vtab->schema_name.c_str(), vtab->table_name.c_str());
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_destroy(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<CachedVtab<RowData>*>(pVtab);
    detail::write_surface_destroyed(
        vtab->db, vtab->schema_name.c_str(), vtab->table_name.c_str());
    delete vtab;
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
    cursor->using_rowid_lookup = false;
    cursor->rowid_lookup_eof = false;
    cursor->rowid_lookup_id = 0;
    cursor->using_count_only = false;
    cursor->count_only_total = 0;
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
    } else if (cursor->using_rowid_lookup) {
        cursor->rowid_lookup_eof = true;
    } else if (cursor->using_index) {
        cursor->index_pos++;
    } else if (cursor->using_count_only) {
        cursor->current_row++;
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
    if (cursor->using_rowid_lookup) {
        return cursor->rowid_lookup_eof ? 1 : 0;
    }
    if (cursor->using_index) {
        if (!cursor->index_matches) return 1;
        return cursor->index_pos >= cursor->index_matches->size() ? 1 : 0;
    }
    if (cursor->using_count_only) {
        return cursor->current_row >= cursor->count_only_total ? 1 : 0;
    }
    // Full scan using shared cache
    if (cursor->def->use_shared_cache && cursor->def->shared_cache &&
        (cursor->def->shared_cache->built || cursor->def->shared_cache->mutation_snapshot)) {
        return cursor->current_row >= cursor->def->shared_cache->data.size() ? 1 : 0;
    }
    return cursor->current_row >= cursor->cache.size() ? 1 : 0;
}

template<typename RowData>
inline int cached_vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);

    // During UPDATE, SQLite may ask for unchanged column values. Returning
    // without a value marks the column as SQLITE_NOCHANGE in xUpdate.
    // Safe only when the update handler reconstructs the original row by rowid:
    //   - a POSITIONAL shared-cache index (use_shared_cache and no stable
    //     rowid_fn -- with rowid_fn the rowid is a key, not a position, so
    //     xUpdate refuses positional reconstruction), OR
    //   - a resolving row_lookup.
    // and never when the table opts out via update_from_column_values. Otherwise
    // xUpdate reconstructs from real argv values, so reporting NOCHANGE would
    // feed row_from_argv 0/NULL for unchanged identity fields (ea, func_addr).
    // This MUST match cached_vtab_update's reconstruct_by_rowid below.
    // A query-scoped table with a mutation snapshot also reconstructs by rowid
    // (positionally, from the stable snapshot), so it is NOCHANGE-eligible too --
    // essential for tables with multiple setters that map to the SAME underlying
    // value (e.g. comments' comment/rpt_comment, pseudocode's comment/
    // comment_placement): without NOCHANGE, the unchanged setter fires with its
    // stale argv value and clobbers the changed one.
    if (sqlite3_vtab_nochange(ctx)
        && !cursor->def->update_from_column_values
        && (((!cursor->def->rowid_fn) && cursor->def->use_shared_cache)
            || cursor->def->row_lookup
            || detail::query_scoped_uses_mutation_snapshot(cursor->def))) {
        return to_sqlite_status(Status::ok);
    }

    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return to_sqlite_status(Status::ok);
    }
    if (cursor->using_count_only) {
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
    } else if (cursor->using_rowid_lookup) {
        if (cursor->rowid_lookup_eof) {
            sqlite3_result_null(ctx);
            return to_sqlite_status(Status::ok);
        }
        cursor->def->columns[col].get(fctx, cursor->rowid_lookup_row);
    } else if (cursor->using_index) {
        // Index-based access: get row via the index's data source (shared cache
        // for shared tables, the per-cursor index_cache for query-scoped tables).
        if (cursor->index_matches && cursor->index_pos < cursor->index_matches->size()) {
            size_t row_idx = (*cursor->index_matches)[cursor->index_pos];
            const auto* src = cursor->index_data_source;
            if (src && row_idx < src->size()) {
                cursor->def->columns[col].get(fctx, (*src)[row_idx]);
            } else {
                sqlite3_result_null(ctx);
            }
        } else {
            sqlite3_result_null(ctx);
        }
    } else {
        // Full scan: use shared cache if available, else local cache
        const auto& shared = cursor->def->shared_cache;
        if (cursor->def->use_shared_cache && shared &&
            (shared->built || shared->mutation_snapshot) &&
            cursor->current_row < shared->data.size()) {
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
    const auto* def = cursor->def;
    if (cursor->using_iterator && cursor->iterator) {
        if (cursor->iterator_eof) {
            *pRowid = 0;
            return to_sqlite_status(Status::ok);
        }
        *pRowid = cursor->iterator->rowid();
    } else if (cursor->using_rowid_lookup) {
        *pRowid = cursor->rowid_lookup_eof ? 0 : cursor->rowid_lookup_id;
    } else if (cursor->using_index) {
        // Index path: rowid honors rowid_fn; otherwise the matched cache POSITION
        // (row_idx), NOT cursor->current_row (which the index path does not
        // advance). This must match the full-scan path so positional /
        // row_lookup()-based UPDATE/DELETE reconstruction resolves the same row --
        // a query-scoped table with index_on + row_lookup (e.g. idasql `names`)
        // deletes via this path once index_on is enabled for query-scoped tables.
        if (cursor->index_matches && cursor->index_pos < cursor->index_matches->size()) {
            size_t row_idx = (*cursor->index_matches)[cursor->index_pos];
            if (def->rowid_fn) {
                const auto* src = cursor->index_data_source;
                if (src && row_idx < src->size()) {
                    *pRowid = def->rowid_fn((*src)[row_idx]);
                    return to_sqlite_status(Status::ok);
                }
            }
            *pRowid = static_cast<sqlite3_int64>(row_idx);
        } else {
            *pRowid = static_cast<sqlite3_int64>(cursor->current_row);
        }
    } else if (cursor->using_count_only) {
        *pRowid = static_cast<sqlite3_int64>(cursor->current_row);
    } else if (def->rowid_fn) {
        // Full scan: report a stable rowid (e.g. ordinal/ea) instead of the cache
        // position, so it agrees with what filter iterators return and with
        // row_lookup()-based UPDATE/DELETE reconstruction.
        const RowData* r = nullptr;
        if (def->use_shared_cache && def->shared_cache &&
            cursor->current_row < def->shared_cache->data.size()) {
            r = &def->shared_cache->data[cursor->current_row];
        } else if (cursor->current_row < cursor->cache.size()) {
            r = &cursor->cache[cursor->current_row];
        }
        *pRowid = r ? def->rowid_fn(*r) : static_cast<sqlite3_int64>(cursor->current_row);
    } else {
        *pRowid = static_cast<sqlite3_int64>(cursor->current_row);
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int cached_vtab_filter(sqlite3_vtab_cursor* pCursor, int idxNum, const char* idxStr,
                              int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<CachedCursor<RowData>*>(pCursor);

    // Reset cursor state
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    cursor->using_rowid_lookup = false;
    cursor->rowid_lookup_eof = false;
    cursor->rowid_lookup_id = 0;
    cursor->using_index = false;
    cursor->index_matches = nullptr;
    cursor->index_pos = 0;
    cursor->range_matches.clear();
    cursor->using_count_only = false;
    cursor->count_only_total = 0;
    cursor->cache.clear();
    cursor->cache_built = false;
    cursor->current_row = 0;

    // A new scan begins a new statement: drop any query-scoped mutation snapshot
    // preserved from a prior scan-driven UPDATE/DELETE (see
    // ensure_query_scoped_mutation_snapshot). No-op for shared/read-only tables.
    detail::clear_query_scoped_mutation_snapshot(cursor->def);

    if (idxNum == COUNT_ONLY_SCAN && argc == 0 &&
        detail::cached_table_supports_count_only_scan(cursor->def)) {
        clear_vtab_error();
        cursor->using_count_only = true;
        cursor->count_only_total = cursor->def->row_count_fn();
        if (!get_vtab_error().empty()) {
            return return_vtab_error(pCursor->pVtab);
        }
        return to_sqlite_status(Status::ok);
    }

    if (idxNum != FILTER_NONE && argc > 0) {
        if (idxNum == ROWID_FILTER) {
            if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
                cursor->using_rowid_lookup = true;
                cursor->rowid_lookup_id = 0;
                cursor->rowid_lookup_eof = true;
                return to_sqlite_status(Status::ok);
            }

            const int64_t raw_rowid = FunctionArg(argv[0]).as_int64();
            cursor->using_rowid_lookup = true;
            cursor->rowid_lookup_id = raw_rowid;
            cursor->rowid_lookup_eof = true;

            if (raw_rowid >= 0) {
                clear_vtab_error();
                bool row_found = false;
                if (cursor->def->row_lookup) {
                    row_found = cursor->def->row_lookup(cursor->rowid_lookup_row, raw_rowid);
                    cursor->rowid_lookup_eof = !row_found;
                }

                if (!row_found) {
                    if (cursor->def->use_shared_cache) {
                        cursor->def->ensure_cache_built();
                        if (!get_vtab_error().empty()) {
                            return return_vtab_error(pCursor->pVtab);
                        }
                        const auto& shared = cursor->def->shared_cache;
                        const size_t rowid = static_cast<size_t>(raw_rowid);
                        if (shared && shared->built && rowid < shared->data.size()) {
                            cursor->rowid_lookup_row = shared->data[rowid];
                            cursor->rowid_lookup_eof = false;
                        }
                    } else if (!cursor->def->row_lookup &&
                               cursor->def->has_cache_builder()) {
                        std::vector<RowData> rows;
                        cursor->def->build_rows(rows);
                        if (!get_vtab_error().empty()) {
                            return return_vtab_error(pCursor->pVtab);
                        }
                        const size_t rowid = static_cast<size_t>(raw_rowid);
                        if (rowid < rows.size()) {
                            cursor->rowid_lookup_row = std::move(rows[rowid]);
                            cursor->rowid_lookup_eof = false;
                        }
                    }
                }
                if (!get_vtab_error().empty()) {
                    return return_vtab_error(pCursor->pVtab);
                }
            }
            return to_sqlite_status(Status::ok);
        }

        // Check for a range (>=, >, <=, <, BETWEEN) plan on an indexed column.
        // RANGE_BASE sits above INDEX_BASE, so this must be tested BEFORE the
        // equality-index branch below. The sorted view is built lazily here and
        // cached (shared cache for shared tables, per-cursor for query-scoped).
        if (idxNum >= RANGE_BASE) {
            const int encoded = idxNum - RANGE_BASE;
            const int index_pos = encoded / RANGE_STRIDE;
            const int range_flags = encoded & RANGE_FLAG_MASK;
            const auto& index_defs = cursor->def->index_defs;
            if (index_pos >= 0 && static_cast<size_t>(index_pos) < index_defs.size()) {
                // Decode bounds from argv in the order xBestIndex assigned argvIndex:
                // low first (if present), then high. Track the next slot to consume so
                // a high-only range still reads argv[0].
                int slot = 0;
                int64_t low = 0, high = 0;
                if (range_flags & RANGE_HAS_LOW) {
                    if (slot < argc) low = FunctionArg(argv[slot]).as_int64();
                    ++slot;
                }
                if (range_flags & RANGE_HAS_HIGH) {
                    if (slot < argc) high = FunctionArg(argv[slot]).as_int64();
                }
                clear_vtab_error();
                if (cursor->def->use_shared_cache) {
                    cursor->def->ensure_cache_built();
                    if (!get_vtab_error().empty()) {
                        return return_vtab_error(pCursor->pVtab);
                    }
                    const auto& shared = cursor->def->shared_cache;
                    if (shared) {
                        // The shared cache is engine-lifetime and shared across
                        // connections, so the lazy sorted-view build (writer) and the
                        // window collect (reader) MUST run under the cache mutex --
                        // otherwise two connections range-querying the same index race
                        // on the same std::vector. Take the lock BEFORE reading
                        // `built`/`sorted_indexes` so a concurrent invalidate (which
                        // clears them under the same mutex) cannot land between the
                        // check and the indexed access (TOCTOU -> OOB). The matched
                        // rows are then COPIED into cursor-owned storage under this
                        // lock (see below), so no pointer into shared->data survives
                        // past the lock.
                        std::lock_guard<std::mutex> lock(shared->mutex);
                        if (shared->built &&
                            static_cast<size_t>(index_pos) < shared->sorted_indexes.size()) {
                            auto& sorted = shared->sorted_indexes[index_pos];
                            if (!sorted.built) {
                                detail::build_sorted_index(sorted, shared->data,
                                                           index_defs[index_pos].second);
                            }
                            cursor->range_matches.clear();
                            detail::collect_range_matches(sorted, range_flags, low, high,
                                                          cursor->range_matches);
                            // Copy the matched rows into cursor-owned storage under the
                            // cache mutex, then remap range_matches to local indices, so
                            // index_data_source never points into shared->data past this
                            // lock. The old code set index_data_source = &shared->data and
                            // dereferenced it UNLOCKED during xColumn/xRowid iteration; a
                            // concurrent invalidate/rebuild clears+reallocates shared->data
                            // under this same mutex, so that read was a use-after-free /
                            // data race (mirrors the EQ branch's fix below).
                            // collect_range_matches only emits positions valid for the
                            // current shared->data (built under this lock).
                            cursor->index_row_copy.clear();
                            cursor->index_row_copy.reserve(cursor->range_matches.size());
                            for (size_t& slot : cursor->range_matches) {
                                cursor->index_row_copy.push_back(shared->data[slot]);
                                slot = cursor->index_row_copy.size() - 1;
                            }
                            cursor->using_index = true;
                            cursor->index_matches = &cursor->range_matches;
                            cursor->index_data_source = &cursor->index_row_copy;
                            cursor->index_pos = 0;
                            return to_sqlite_status(Status::ok);
                        }
                    }
                } else if (cursor->def->has_cache_builder()) {
                    // Query-scoped: build the per-cursor cache once (reused across
                    // this cursor's xFilter calls), then the sorted view on demand.
                    if (!cursor->cursor_index_built) {
                        cursor->index_cache.clear();
                        cursor->def->build_rows(cursor->index_cache);
                        if (!get_vtab_error().empty()) {
                            cursor->index_cache.clear();
                            return return_vtab_error(pCursor->pVtab);
                        }
                        cursor->cursor_indexes.assign(index_defs.size(), {});
                        for (size_t ix = 0; ix < index_defs.size(); ++ix) {
                            auto& index_map = cursor->cursor_indexes[ix];
                            const auto& key_fn = index_defs[ix].second;
                            for (size_t row = 0; row < cursor->index_cache.size(); ++row) {
                                index_map[key_fn(cursor->index_cache[row])].push_back(row);
                            }
                        }
                        cursor->cursor_index_built = true;
                    }
                    if (cursor->cursor_sorted_indexes.size() != index_defs.size()) {
                        cursor->cursor_sorted_indexes.assign(index_defs.size(), SortedIndex{});
                    }
                    auto& sorted = cursor->cursor_sorted_indexes[index_pos];
                    if (!sorted.built) {
                        detail::build_sorted_index(sorted, cursor->index_cache,
                                                   index_defs[index_pos].second);
                    }
                    cursor->range_matches.clear();
                    detail::collect_range_matches(sorted, range_flags, low, high,
                                                  cursor->range_matches);
                    cursor->using_index = true;
                    cursor->index_matches = &cursor->range_matches;
                    cursor->index_data_source = &cursor->index_cache;
                    cursor->index_pos = 0;
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Check for index-based lookup (idxNum >= INDEX_BASE)
        if (idxNum >= INDEX_BASE) {
            int index_pos = idxNum - INDEX_BASE;
            const auto& index_defs = cursor->def->index_defs;
            if (index_pos >= 0 && static_cast<size_t>(index_pos) < index_defs.size()) {
                clear_vtab_error();
                if (cursor->def->use_shared_cache) {
                    // Shared table: index lives in the engine-lifetime shared cache.
                    cursor->def->ensure_cache_built();
                    if (!get_vtab_error().empty()) {
                        return return_vtab_error(pCursor->pVtab);
                    }
                    const auto& shared = cursor->def->shared_cache;
                    if (shared) {
                        // Take the cache mutex and copy the matched rows into
                        // cursor-owned storage. The old code stored pointers into the
                        // shared index map + data and read them across the statement
                        // without this lock, so a concurrent invalidate/rebuild freed
                        // them (use-after-free). Re-check `built` under the lock.
                        std::lock_guard<std::mutex> lock(shared->mutex);
                        if (shared->built &&
                            static_cast<size_t>(index_pos) < shared->indexes.size()) {
                            int64_t key = FunctionArg(argv[0]).as_int64();
                            auto it = shared->indexes[index_pos].find(key);
                            cursor->index_row_copy.clear();
                            cursor->range_matches.clear();
                            if (it != shared->indexes[index_pos].end()) {
                                cursor->index_row_copy.reserve(it->second.size());
                                for (size_t pos : it->second) {
                                    if (pos < shared->data.size()) {
                                        cursor->range_matches.push_back(
                                            cursor->index_row_copy.size());
                                        cursor->index_row_copy.push_back(shared->data[pos]);
                                    }
                                }
                            }
                            cursor->using_index = true;
                            cursor->index_matches = &cursor->range_matches;
                            cursor->index_data_source = &cursor->index_row_copy;
                            cursor->index_pos = 0;
                            return to_sqlite_status(Status::ok);
                        }
                    }
                } else if (cursor->def->has_cache_builder()) {
                    // Query-scoped table: build the per-cursor cache + hash indexes
                    // ONCE, then reuse across this cursor's xFilter calls (a JOIN's
                    // inner loop reuses one cursor) so the JOIN stays fast without an
                    // engine-lifetime shared cache. Rebuilt per statement (new cursor).
                    if (!cursor->cursor_index_built) {
                        cursor->index_cache.clear();
                        cursor->def->build_rows(cursor->index_cache);
                        if (!get_vtab_error().empty()) {
                            cursor->index_cache.clear();
                            return return_vtab_error(pCursor->pVtab);
                        }
                        cursor->cursor_indexes.assign(index_defs.size(), {});
                        for (size_t ix = 0; ix < index_defs.size(); ++ix) {
                            auto& index_map = cursor->cursor_indexes[ix];
                            const auto& key_fn = index_defs[ix].second;
                            for (size_t row = 0; row < cursor->index_cache.size(); ++row) {
                                index_map[key_fn(cursor->index_cache[row])].push_back(row);
                            }
                        }
                        cursor->cursor_index_built = true;
                    }
                    int64_t key = FunctionArg(argv[0]).as_int64();
                    auto& index_map = cursor->cursor_indexes[index_pos];
                    auto it = index_map.find(key);
                    cursor->using_index = true;
                    cursor->index_matches = (it != index_map.end()) ? &it->second : nullptr;
                    cursor->index_data_source = &cursor->index_cache;
                    cursor->index_pos = 0;
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Check for filter-based lookup
        for (const auto& filter : cursor->def->filters) {
            if (filter.filter_id == idxNum) {
                clear_vtab_error();
                cursor->iterator = filter.create(FunctionArg(argv[0]));
                cursor->using_iterator = true;
                cursor->iterator_eof = true;
                if (cursor->iterator) {
                    cursor->iterator_eof = !cursor->iterator->next();
                }
                if (!get_vtab_error().empty()) {
                    return return_vtab_error(pCursor->pVtab);
                }
                return to_sqlite_status(Status::ok);
            }
        }
    }

    // Full scan - use shared cache or query-local cache depending on table policy.
    if (cursor->def->use_shared_cache) {
        clear_vtab_error();
        cursor->def->ensure_cache_built();
        if (!get_vtab_error().empty()) {
            return return_vtab_error(pCursor->pVtab);
        }
    } else if (cursor->def->projection_cache_builder_fn ||
               cursor->def->has_cache_builder()) {
        cursor->cache.clear();
        clear_vtab_error();
        if (cursor->def->projection_cache_builder_fn) {
            // colUsed arrives per-plan via idxStr from xBestIndex (not shared
            // vtab state). Absent/unparseable => assume all columns (~0, safe).
            uint64_t col_used = ~0ull;
            if (idxStr && *idxStr) {
                col_used = static_cast<uint64_t>(strtoull(idxStr, nullptr, 10));
            }
            cursor->def->projection_cache_builder_fn(cursor->cache, col_used);
        } else {
            cursor->def->build_rows(cursor->cache);
        }
        if (!get_vtab_error().empty()) {
            cursor->cache.clear();
            return return_vtab_error(pCursor->pVtab);
        }
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

    // Carry the columns this query reads (colUsed) to xFilter via idxStr so a
    // projection-aware builder can skip expensive unused columns. Passing it per
    // plan (not on the shared vtab) keeps self-joins / aliases / interleaved
    // statements from clobbering each other's projection. Only allocated for
    // projection-aware tables; idxStr stays null otherwise (no behavior change).
    if (def->projection_cache_builder_fn) {
        if (char* s = sqlite3_mprintf("%llu",
                static_cast<unsigned long long>(pInfo->colUsed))) {
            pInfo->idxStr = s;
            pInfo->needToFreeIdxStr = 1;
        }
    }

    // Track best option: filter, index, or full scan
    const FilterDef* best_filter = nullptr;
    int best_filter_constraint_idx = -1;
    int best_index_pos = -1;
    int best_index_constraint_idx = -1;
    int best_rowid_constraint_idx = -1;
    double best_cost = 1e9;

    // Range-pushdown candidate: the lower/upper bound constraints that target a
    // single indexed column. SQLite delivers `x BETWEEN a AND b` as a GE + an LE
    // constraint on the same column, so a lower and an upper bound on the SAME
    // indexed column are consumed together (two argv slots). A range plan is only
    // chosen when NO equality index/filter won (equality is strictly cheaper).
    int range_index_pos = -1;         // indexed column carrying the range
    int range_low_constraint_idx = -1;
    int range_high_constraint_idx = -1;
    bool range_low_strict = false;    // GT (>) vs GE (>=)
    bool range_high_strict = false;   // LT (<) vs LE (<=)

    auto estimate_full_scan_rows = [&]() -> size_t {
        size_t estimated_rows = 1000;
        if (def->estimate_rows_fn) {
            estimated_rows = def->estimate_rows_fn();
        }
        return estimated_rows;
    };

    // A range is only served off a sorted view, which needs the cache: shared
    // tables (engine-lifetime) or query-scoped tables with a cache_builder.
    const bool range_eligible =
        def->use_shared_cache || def->has_cache_builder();

    for (int i = 0; i < pInfo->nConstraint; i++) {
        const auto& constraint = pInfo->aConstraint[i];
        if (!constraint.usable) continue;

        const int op = constraint.op;
        const bool is_eq = (op == SQLITE_INDEX_CONSTRAINT_EQ);
        const bool is_like = (op == SQLITE_INDEX_CONSTRAINT_LIKE ||
                              op == SQLITE_INDEX_CONSTRAINT_GLOB);
        const bool is_low = (op == SQLITE_INDEX_CONSTRAINT_GE ||
                             op == SQLITE_INDEX_CONSTRAINT_GT);
        const bool is_high = (op == SQLITE_INDEX_CONSTRAINT_LE ||
                              op == SQLITE_INDEX_CONSTRAINT_LT);

        // Range (>=, >, <=, <) on an indexed column: record the tightest bound(s)
        // for a single indexed column. Only pursued if no equality lookup wins.
        if ((is_low || is_high) && range_eligible && constraint.iColumn >= 0) {
            const int idx_pos = def->find_index(constraint.iColumn);
            if (idx_pos >= 0 &&
                (range_index_pos < 0 || range_index_pos == idx_pos)) {
                range_index_pos = idx_pos;
                if (is_low && range_low_constraint_idx < 0) {
                    range_low_constraint_idx = i;
                    range_low_strict = (op == SQLITE_INDEX_CONSTRAINT_GT);
                } else if (is_high && range_high_constraint_idx < 0) {
                    range_high_constraint_idx = i;
                    range_high_strict = (op == SQLITE_INDEX_CONSTRAINT_LT);
                }
            }
            continue;
        }

        if (!is_eq && !is_like) continue;

        if (constraint.iColumn < 0) {
            if (is_eq) best_rowid_constraint_idx = i;  // rowid only via equality
            continue;
        }

        if (is_like) {
            // Prefix/LIKE pushdown: a best-effort superset filter. omit stays 0
            // (set below) so SQLite re-applies the real pattern test. Only claim
            // the constraint when the pattern has a usable literal prefix -- a
            // leading-wildcard pattern would force the iterator to walk/return
            // everything, so it is left to the full scan instead.
            const FilterDef* filter = def->find_filter(constraint.iColumn, op);
            if (filter && filter->estimated_cost < best_cost &&
                detail::like_constraint_has_usable_prefix(pInfo, i)) {
                best_filter = filter;
                best_filter_constraint_idx = i;
                best_cost = filter->estimated_cost;
            }
            continue;
        }

        // Equality: check for explicit filter
        const FilterDef* filter = def->find_filter(constraint.iColumn, SQLITE_INDEX_CONSTRAINT_EQ);
        if (filter && filter->estimated_cost < best_cost) {
            best_filter = filter;
            best_filter_constraint_idx = i;
            best_cost = filter->estimated_cost;
        }

        // Check for indexed column. Query-scoped tables also use it: xFilter builds
        // the hash index on a per-cursor cache (needs a cache_builder to (re)build).
        int idx_pos = (def->use_shared_cache || def->has_cache_builder())
                          ? def->find_index(constraint.iColumn) : -1;
        if (idx_pos >= 0) {
            if (def->use_shared_cache) {
                // Hash lookup on the already-built shared cache is near-free:
                // compete by cost and displace a costlier filter.
                double index_cost = 1.0;
                if (index_cost < best_cost) {
                    best_index_pos = idx_pos;
                    best_index_constraint_idx = i;
                    best_cost = index_cost;
                    best_filter = nullptr;  // Index beats costlier filter
                }
            } else if (best_index_pos < 0) {
                // Query-scoped: the index scan must first BUILD the per-cursor
                // cache (a full engine read), so it never outranks an explicit
                // filter (a targeted engine read). Record it as the fallback
                // that beats a full scan (built once, reused across a JOIN's
                // inner loop) — chosen below only when no filter matched.
                best_index_pos = idx_pos;
                best_index_constraint_idx = i;
            }
        }
    }

    if (pInfo->nConstraint == 0 && pInfo->colUsed == 0 &&
        detail::cached_table_supports_count_only_scan(def)) {
        const size_t estimated_rows = estimate_full_scan_rows();
        pInfo->idxNum = COUNT_ONLY_SCAN;
        pInfo->estimatedCost = static_cast<double>(estimated_rows);
        pInfo->estimatedRows = estimated_rows;
    } else if (best_rowid_constraint_idx >= 0) {
        pInfo->aConstraintUsage[best_rowid_constraint_idx].argvIndex = 1;
        pInfo->aConstraintUsage[best_rowid_constraint_idx].omit = 1;
        pInfo->idxNum = ROWID_FILTER;
        pInfo->estimatedCost = 1.0;
        pInfo->estimatedRows = 1;
    } else if (best_index_pos >= 0 && best_index_constraint_idx >= 0 &&
               !best_filter) {
        pInfo->aConstraintUsage[best_index_constraint_idx].argvIndex = 1;
        pInfo->aConstraintUsage[best_index_constraint_idx].omit = 1;
        pInfo->idxNum = INDEX_BASE + best_index_pos;
        pInfo->estimatedCost = 1.0;
        pInfo->estimatedRows = 5;  // Assume small result set
    } else if (best_filter && best_filter_constraint_idx >= 0) {
        pInfo->aConstraintUsage[best_filter_constraint_idx].argvIndex = 1;
        // EQ filters are trusted (omit). LIKE/GLOB prefix filters are a superset
        // optimization -- leave the constraint in place so SQLite re-checks the
        // exact pattern (correctness regardless of the iterator's prefix logic).
        pInfo->aConstraintUsage[best_filter_constraint_idx].omit =
            (best_filter->op == SQLITE_INDEX_CONSTRAINT_EQ) ? 1 : 0;
        pInfo->idxNum = best_filter->filter_id;
        pInfo->estimatedCost = best_filter->estimated_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(best_filter->estimated_rows);
    } else if (range_index_pos >= 0 &&
               (range_low_constraint_idx >= 0 || range_high_constraint_idx >= 0)) {
        // Range plan on an indexed column: cheaper than a full scan (a sorted-view
        // window), pricier than an EQ point lookup. A two-bound (BETWEEN) window
        // is tighter than a one-bound half-scan, so it costs less. The bounds are
        // handled EXACTLY by the sorted-view carve (strict/inclusive honored), so
        // the consumed constraints are omitted (SQLite need not re-check them).
        int range_flags = 0;
        int argv_slot = 1;
        if (range_low_constraint_idx >= 0) {
            range_flags |= RANGE_HAS_LOW;
            if (range_low_strict) range_flags |= RANGE_LOW_STRICT;
            pInfo->aConstraintUsage[range_low_constraint_idx].argvIndex = argv_slot++;
            pInfo->aConstraintUsage[range_low_constraint_idx].omit = 1;
        }
        if (range_high_constraint_idx >= 0) {
            range_flags |= RANGE_HAS_HIGH;
            if (range_high_strict) range_flags |= RANGE_HIGH_STRICT;
            pInfo->aConstraintUsage[range_high_constraint_idx].argvIndex = argv_slot++;
            pInfo->aConstraintUsage[range_high_constraint_idx].omit = 1;
        }
        const size_t estimated_rows = estimate_full_scan_rows();
        const bool two_bound =
            (range_low_constraint_idx >= 0 && range_high_constraint_idx >= 0);
        // Fraction-of-scan heuristic: a two-sided window is assumed to select ~1/4
        // of rows, a one-sided ~1/2. Always strictly below the full-scan cost so a
        // pushed range beats the scan, and above the EQ cost of 1.0.
        const double frac = two_bound ? 0.25 : 0.5;
        const double full_cost = static_cast<double>(estimated_rows);
        double range_cost = 2.0 + full_cost * frac;
        // Keep the range strictly cheaper than a full scan even on tiny tables so
        // the planner prefers the pushed window (and the plan reports a virtual
        // index, not INDEX 0). Never dip to/below the EQ point-lookup cost (1.0).
        // Parenthesized to defeat a function-like max() macro (Windows <windows.h>
        // defines one unless NOMINMAX is set; this header can be included after it).
        if (range_cost >= full_cost) range_cost = (std::max)(1.5, full_cost * 0.75);
        pInfo->idxNum = RANGE_BASE + range_index_pos * RANGE_STRIDE + range_flags;
        pInfo->estimatedCost = range_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(
            estimated_rows > 0 ? static_cast<size_t>(estimated_rows * frac) + 1 : 1);
    } else {
        // No filter or index - full scan
        size_t estimated_rows = estimate_full_scan_rows();
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
            return detail::unsupported_delete(pVtab, def->name);
        }

        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        const size_t rowid = raw_rowid >= 0 ? static_cast<size_t>(raw_rowid) : static_cast<size_t>(-1);

        // Query-scoped positional tables: snapshot the pre-mutation row set once
        // so a multi-row DELETE reconstructs from a STABLE set instead of a
        // rebuild on the already-mutated state (which strands rowids and can crash
        // engines that mutate mid-rebuild). No-op otherwise.
        if (!detail::ensure_query_scoped_mutation_snapshot(def)) {
            return return_vtab_error(pVtab);
        }
        const auto& shared = def->shared_cache;
        RowData temp_row{};
        RowData* row_ptr = nullptr;

        // Positional cache lookups (shared->data[rowid] / cache_builder[rowid])
        // are valid ONLY when the rowid is a cache position: not opted out
        // (reconstruct_by_rowid) and no stable rowid_fn. An opt-out table's
        // filter-iterator rowid may overlap a cache index, so it must resolve via
        // row_lookup (mirrors the xUpdate reconstruction; see CachedTableDef).
        const bool reconstruct_by_rowid =
            !def->update_from_column_values &&
            (((!def->rowid_fn) && def->use_shared_cache) || static_cast<bool>(def->row_lookup));

        if (def->snapshot_mutations && def->rowid_fn && shared && shared->mutation_snapshot) {
            // Opt-in snapshot (shifting-identity rowid): resolve by matching
            // rowid_fn over the STABLE pre-mutation snapshot, so a multi-row DELETE
            // targets the row identified at scan time even after earlier rows
            // shifted live positions. Terminal: if no snapshot row matches (the
            // identity changed within this statement) fail honestly rather than
            // resolving live (which would hit the wrong row).
            for (auto& snap_row : shared->data) {
                if (def->rowid_fn(snap_row) == raw_rowid) { row_ptr = &snap_row; break; }
            }
        } else if (reconstruct_by_rowid && !def->rowid_fn && shared && shared->built &&
            raw_rowid >= 0 && rowid < shared->data.size()) {
            row_ptr = &shared->data[rowid];
        } else if (reconstruct_by_rowid && !def->rowid_fn && shared && shared->mutation_snapshot &&
                   !def->row_lookup && raw_rowid >= 0 && rowid < shared->data.size()) {
            row_ptr = &shared->data[rowid];
        } else if (!def->use_shared_cache && shared && shared->mutation_snapshot &&
                   raw_rowid >= 0 && rowid < shared->data.size()) {
            // Query-scoped positional snapshot: stable across the multi-row
            // statement, so this rowid still maps to the pre-mutation row (the
            // per-xUpdate rebuild below would read the already-mutated data).
            row_ptr = &shared->data[rowid];
        } else if (def->row_lookup && def->row_lookup(temp_row, raw_rowid)) {
            row_ptr = &temp_row;
        } else if (!def->rowid_fn && !def->use_shared_cache &&
                   def->has_cache_builder() && raw_rowid >= 0) {
            // Non-shared positional rebuild: a full-scan rowid IS the row's index
            // in the freshly rebuilt cache (exact, no overlap risk), so it is
            // correct even for opt-out tables -- e.g. names (no_shared_cache,
            // opt-out, row_lookup keyed by ea) whose DELETE rowid is a cache
            // position that row_lookup cannot resolve. Only the *shared* positional
            // branches above are gated on reconstruct_by_rowid.
            std::vector<RowData> rows;
            def->build_rows(rows);
            if (rowid < rows.size()) {
                temp_row = std::move(rows[rowid]);
                row_ptr = &temp_row;
            }
        }

        if (!row_ptr) {
            return to_sqlite_status(Status::error);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        if (!def->delete_row(*row_ptr)) {
            return to_sqlite_status(Status::error);
        }
        detail::cached_table_invalidate_after_mutation(def);
        if (def->after_modify) def->after_modify("DELETE FROM " + def->name);
        vtab->transaction.wrote = true;
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
        if (!has_writable) return detail::unsupported_update(pVtab, def->name);

        // Snapshot before the before_modify hook (same order as DELETE): a
        // failed snapshot aborts the statement before any observable side
        // effect, so the hook never fires for a mutation that cannot happen.
        if (!detail::ensure_query_scoped_mutation_snapshot(def)) {
            return return_vtab_error(pVtab);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        // Try shared cache first; if unavailable or rowid out of range,
        // construct a temporary row from column values (argv[2..])
        const auto& shared = def->shared_cache;
        RowData* row_ptr = nullptr;
        RowData temp_row{};

        // Reconstruct-by-rowid (and thus NOCHANGE-eligible) exactly when the row
        // can be resolved from the rowid alone: a POSITIONAL shared-cache index
        // (no stable rowid_fn), a resolving row_lookup, or a query-scoped mutation
        // snapshot (positional, stable for the statement) -- and never when opted
        // out. MUST match the cached_vtab_column() NOCHANGE gate. When true,
        // unchanged identity columns arrive as NOCHANGE/0/NULL, so row_from_argv
        // must NOT be used even if rowid resolution later fails (it would write
        // the wrong row); fall through to read_only instead.
        const bool reconstruct_by_rowid =
            !def->update_from_column_values &&
            (((!def->rowid_fn) && def->use_shared_cache) || static_cast<bool>(def->row_lookup)
             || detail::query_scoped_uses_mutation_snapshot(def));

        if (def->snapshot_mutations && def->rowid_fn && shared && shared->mutation_snapshot) {
            // Opt-in snapshot (shifting-identity rowid, see DELETE): resolve by
            // matching rowid_fn over the STABLE pre-mutation snapshot so a multi-row
            // UPDATE targets the scan-time row even after earlier rows shifted live
            // indices. NOCHANGE stays enabled (reconstruct_by_rowid is true), so the
            // snapshot row supplies unchanged columns. Terminal: no match => the
            // identity changed within this statement; fail honestly.
            for (auto& snap_row : shared->data) {
                if (def->rowid_fn(snap_row) == raw_rowid) { row_ptr = &snap_row; break; }
            }
        } else if (reconstruct_by_rowid && !def->rowid_fn && shared && shared->built &&
            raw_rowid >= 0 && old_rowid < shared->data.size()) {
            // Positional shared-cache reconstruction -- valid only when the rowid
            // is a cache position: not opted out (reconstruct_by_rowid) and no
            // stable rowid_fn. An opt-out table's filter-iterator rowid may
            // overlap a cache index, so it must reconstruct from argv instead.
            row_ptr = &shared->data[old_rowid];
        } else if (reconstruct_by_rowid && !def->rowid_fn && shared && shared->mutation_snapshot &&
                   !def->row_lookup && raw_rowid >= 0 && old_rowid < shared->data.size()) {
            row_ptr = &shared->data[old_rowid];
        } else if (reconstruct_by_rowid && def->row_lookup && def->row_lookup(temp_row, raw_rowid)) {
            // Trusted rowid: resolves to the exact row (e.g. a filter iterator
            // rowid that round-trips through row_lookup). NOCHANGE is enabled.
            row_ptr = &temp_row;
        } else if (!reconstruct_by_rowid && def->row_from_argv) {
            // NOCHANGE is disabled for this table, so unchanged identity columns
            // (ea, func_addr, ...) carry their REAL values in argv. Reconstruct
            // from them -- the path for opt-out tables and for tables whose rowid
            // cannot be resolved to a row (no positional cache, no row_lookup,
            // e.g. shared-cache + rowid_fn without row_lookup).
            detail::with_args(argc, argv, [&](FunctionArg* args) {
                def->row_from_argv(temp_row, argc, args);
            });
            row_ptr = &temp_row;
        } else if (!def->use_shared_cache && shared && shared->mutation_snapshot &&
                   raw_rowid >= 0 && old_rowid < shared->data.size()) {
            // Query-scoped positional snapshot (see DELETE): a stable pre-mutation
            // row for a multi-row UPDATE, instead of a rebuild on mutated data.
            row_ptr = &shared->data[old_rowid];
        } else if (!def->update_from_column_values && !def->rowid_fn &&
                   !def->use_shared_cache && def->has_cache_builder() &&
                   raw_rowid >= 0) {
            // Position-based rebuild: a full-scan rowid is the row's index in the
            // cache_builder output (only valid when the rowid is positional, i.e.
            // no rowid_fn, and not opted out). NOCHANGE is off here, so safe.
            std::vector<RowData> rows;
            def->build_rows(rows);
            if (old_rowid < rows.size()) {
                temp_row = std::move(rows[old_rowid]);
                row_ptr = &temp_row;
            }
        } else if (def->row_from_argv) {
            // Last-resort reconstruction via row_populator. CONTRACT: when this
            // path can run under NOCHANGE (reconstruct_by_rowid tables whose
            // positional/row_lookup resolution did not fire, e.g. a not-yet-built
            // shared cache reached via filter_eq), the populator MUST resolve the
            // row from the rowid in argv[0] (real even under NOCHANGE) and apply
            // only non-NOCHANGE column values. Populators that read identity from
            // argv COLUMNS must keep NOCHANGE disabled -- by opting out, or by
            // exposing a stable rowid_fn without a row_lookup -- so those columns
            // carry real values (see the cached_vtab_column() NOCHANGE gate).
            detail::with_args(argc, argv, [&](FunctionArg* args) {
                def->row_from_argv(temp_row, argc, args);
            });
            row_ptr = &temp_row;
        } else if (def->row_lookup && def->row_lookup(temp_row, raw_rowid)) {
            // Safe rowid resolution (row_lookup returns a real row, never argv).
            row_ptr = &temp_row;
        } else {
            return to_sqlite_status(Status::read_only);
        }

        // reconstruct_by_rowid is exactly the xColumn NOCHANGE gate: on reconstruct-
        // by-argv paths read-only identity columns carry real values, so they are
        // skipped (historical behavior), not rejected as read-only writes.
        const Status st = detail::apply_update_columns(
            pVtab, argc, argv, def->columns.size(), reconstruct_by_rowid,
            [&](size_t c) { return def->columns[c].writable && static_cast<bool>(def->columns[c].set); },
            [&](size_t c) -> const std::string& { return def->columns[c].name; },
            [&](size_t c) { return def->columns[c].set(*row_ptr, FunctionArg(argv[c + 2])); });
        if (st != Status::ok) return to_sqlite_status(st);
        detail::cached_table_invalidate_after_mutation(def);
        if (def->after_modify) def->after_modify("UPDATE " + def->name);
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] == NULL: INSERT
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        if (!def->supports_insert || !def->insert_row) {
            return detail::unsupported_insert(pVtab, def->name);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("INSERT INTO " + def->name);
        }

        bool ok = false;
        clear_vtab_error();
        detail::with_args(argc - 2, &argv[2], [&](FunctionArg* args) {
            ok = def->insert_row(argc - 2, args);
        });
        if (!ok) {
            return return_vtab_error(pVtab);
        }
        clear_vtab_error();
        def->invalidate_cache();
        if (def->after_modify) def->after_modify("INSERT INTO " + def->name);
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    return to_sqlite_status(Status::read_only);
}

template<typename RowData>
inline sqlite3_module create_cached_module() {
    sqlite3_module mod = {};
    mod.iVersion = 3;
    mod.xCreate = cached_vtab_connect<RowData>;
    mod.xConnect = cached_vtab_connect<RowData>;
    mod.xBestIndex = cached_vtab_best_index<RowData>;
    mod.xDisconnect = cached_vtab_disconnect<RowData>;
    mod.xDestroy = cached_vtab_destroy<RowData>;
    mod.xOpen = cached_vtab_open<RowData>;
    mod.xClose = cached_vtab_close<RowData>;
    mod.xFilter = cached_vtab_filter<RowData>;
    mod.xNext = cached_vtab_next<RowData>;
    mod.xEof = cached_vtab_eof<RowData>;
    mod.xColumn = cached_vtab_column<RowData>;
    mod.xRowid = cached_vtab_rowid<RowData>;
    mod.xUpdate = cached_vtab_update<RowData>;
    // xBegin enrolls the vtab; xSync runs the fallible hook; xCommit clears state.
    mod.xBegin = vtab_xbegin<CachedVtab<RowData>>;
    mod.xSync = vtab_prepare_commit<CachedVtab<RowData>>;
    mod.xCommit = vtab_finish_commit<CachedVtab<RowData>>;
    mod.xRollback = vtab_xrollback<CachedVtab<RowData>>;
    mod.xSavepoint = vtab_xsavepoint<CachedVtab<RowData>>;
    mod.xRelease = vtab_xrelease<CachedVtab<RowData>>;
    mod.xRollbackTo = vtab_xrollback_to<CachedVtab<RowData>>;
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
        // Do NOT delete `owned` here: sqlite3_create_module_v2 invokes the
        // xDestroy callback (destroy_def) even when registration fails, which
        // already deletes the clone. A manual delete would be a double-free.
        // (See SQLite docs: the destructor "is also invoked if the call ...
        // fails" — same rule as sqlite3_create_function_v2.)
        return false;
    }
    return true;
}

template<typename RowData>
inline bool register_cached_vtable(Database& db,
                                   const char* module_name,
                                   const CachedTableDef<RowData>* def) {
    if (!detail::register_cached_vtable_sqlite<RowData>(db.sqlite_handle(), module_name, def))
        return false;
    db.record_write_surface(module_name,
                            def->supports_insert && static_cast<bool>(def->insert_row),
                            def->supports_delete && static_cast<bool>(def->delete_row),
                            detail::def_writable_columns(def));
    return true;
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

    // Exact row count for the COUNT_ONLY_SCAN fast path (optional; cheap
    // count without materializing the cache). Same name and semantics as
    // GeneratorTableBuilder::row_count.
    CachedTableBuilder& row_count(std::function<size_t()> fn) {
        def_.row_count_fn = std::move(fn);
        return *this;
    }

    CachedTableBuilder& cache_builder(std::function<void(std::vector<RowData>&)> fn) {
        def_.cache_builder_fn = std::move(fn);
        return *this;
    }

    CachedTableBuilder& stateful_cache_builder(
        std::function<void(
            const TransactionHooks::State&, std::vector<RowData>&)> fn) {
        def_.stateful_cache_builder_fn = std::move(fn);
        return *this;
    }

    // Projection-aware builder for query-scoped (no_shared_cache) tables: gets
    // the colUsed bitmask so it can skip materializing unused expensive columns.
    // Used INSTEAD of cache_builder() for non-shared full-scan builds; keep a
    // cache_builder() too as the fallback for any other path.
    CachedTableBuilder& projection_cache_builder(
            std::function<void(std::vector<RowData>&, uint64_t)> fn) {
        def_.projection_cache_builder_fn = std::move(fn);
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

    CachedTableBuilder& transaction_hooks(TransactionHooks hooks) {
        def_.transaction_hooks = std::move(hooks);
        return *this;
    }

    CachedTableBuilder& column(const char* name,
                               ColumnType type,
                               std::function<void(FunctionContext&, const RowData&)> getter) {
        def_.columns.push_back(
            detail::make_row_column<RowData>(name, type, false, std::move(getter)));
        return *this;
    }

    CachedTableBuilder& column_int64(const char* name, std::function<int64_t(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, false, detail::row_getter_int64<RowData>(std::move(getter))));
        return *this;
    }

    CachedTableBuilder& column_int64_rw(const char* name,
                                         std::function<int64_t(const RowData&)> getter,
                                         std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int64<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    CachedTableBuilder& column_int64_rw(const char* name,
                                         std::function<int64_t(const RowData&)> getter,
                                         std::function<bool(RowData&, int64_t)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int64<RowData>(std::move(getter)),
            detail::row_setter_int64<RowData>(std::move(setter))));
        return *this;
    }

    CachedTableBuilder& column_int(const char* name, std::function<int(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, false, detail::row_getter_int<RowData>(std::move(getter))));
        return *this;
    }

    CachedTableBuilder& column_int_rw(const char* name,
                                       std::function<int(const RowData&)> getter,
                                       std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    CachedTableBuilder& column_int_rw(const char* name,
                                       std::function<int(const RowData&)> getter,
                                       std::function<bool(RowData&, int)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int<RowData>(std::move(getter)),
            detail::row_setter_int<RowData>(std::move(setter))));
        return *this;
    }

    CachedTableBuilder& column_text(const char* name, std::function<std::string(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, false, detail::row_getter_text<RowData>(std::move(getter))));
        return *this;
    }

    CachedTableBuilder& column_text_rw(const char* name,
                                        std::function<std::string(const RowData&)> getter,
                                        std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, true,
            detail::row_getter_text<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    CachedTableBuilder& column_text_nullable_rw(
                                        const char* name,
                                        std::function<std::optional<std::string>(const RowData&)> getter,
                                        std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, true,
            detail::row_getter_nullable_text<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    // Writable integer column whose getter may report SQL NULL (returns
    // std::nullopt). Mirrors column_text_nullable_rw for the integer type — used
    // where a mapped-but-unknown value must read back as NULL rather than 0.
    CachedTableBuilder& column_int_nullable_rw(
                                        const char* name,
                                        std::function<std::optional<int>(const RowData&)> getter,
                                        std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_nullable_int<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    // Read-only integer column whose getter may report SQL NULL (std::nullopt).
    // Read-only sibling of column_int_nullable_rw.
    CachedTableBuilder& column_int_nullable(
                                        const char* name,
                                        std::function<std::optional<int>(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, false,
            detail::row_getter_nullable_int<RowData>(std::move(getter))));
        return *this;
    }

    // Read-only text column whose getter may report SQL NULL (std::nullopt).
    // Read-only sibling of column_text_nullable_rw.
    CachedTableBuilder& column_text_nullable(
                                        const char* name,
                                        std::function<std::optional<std::string>(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, false,
            detail::row_getter_nullable_text<RowData>(std::move(getter))));
        return *this;
    }

    CachedTableBuilder& column_text_rw(const char* name,
                                        std::function<std::string(const RowData&)> getter,
                                        std::function<bool(RowData&, const char*)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, true,
            detail::row_getter_text<RowData>(std::move(getter)),
            detail::row_setter_text<RowData>(std::move(setter))));
        return *this;
    }

    CachedTableBuilder& column_double(const char* name, std::function<double(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Real, false, detail::row_getter_double<RowData>(std::move(getter))));
        return *this;
    }

    CachedTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Blob, false, detail::row_getter_blob<RowData>(std::move(getter))));
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

    /**
     * Add a prefix (LIKE) pushdown filter for a text column.
     *
     * Matches `WHERE column LIKE 'prefix%'` constraints. The factory receives the
     * full LIKE pattern (e.g. "webs%") and returns an iterator that yields a
     * SUPERSET of the matching rows -- SQLite re-applies the exact LIKE test
     * afterwards (the constraint is not omitted), so the iterator only needs to be
     * a correct over-approximation (e.g. a case-insensitive prefix scan). This is
     * the right tool when the backing store has no sorted index and the cost is
     * dominated by per-row rendering: the iterator does cheap identity comparisons
     * and only renders the rows that pass the prefix.
     *
     * Default cost (50) sits above EQ filters (so exact lookups win) but well
     * below a full scan.
     */
    CachedTableBuilder& filter_prefix(const char* column_name,
                                       std::function<std::unique_ptr<RowIterator>(const std::string&)> factory,
                                       double cost = 50.0, double est_rows = 20.0) {
        int col_idx = def_.find_column(column_name);
        if (col_idx < 0) return *this;
        int filter_id = static_cast<int>(def_.filters.size()) + 1;
        def_.filters.emplace_back(col_idx, filter_id, cost, est_rows,
            [factory = std::move(factory)](FunctionArg val) -> std::unique_ptr<RowIterator> {
                const char* text = val.as_c_str();
                return factory(text ? std::string(text) : std::string());
            }, SQLITE_INDEX_CONSTRAINT_LIKE);
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

    // Set a stable rowid for full-scan/index cursors (see CachedTableDef::rowid_fn).
    CachedTableBuilder& rowid(std::function<int64_t(const RowData&)> fn) {
        def_.rowid_fn = std::move(fn);
        return *this;
    }

    // Opt in to the pre-mutation snapshot for a rowid_fn table whose rowid encodes
    // a shifting identity (see CachedTableDef::snapshot_mutations). Requires a
    // rowid_fn -- a multi-row UPDATE/DELETE then resolves each rowid against the
    // stable pre-mutation snapshot instead of live (already-shifted) state.
    CachedTableBuilder& snapshot_mutations(bool value = true) {
        def_.snapshot_mutations = value;
        return *this;
    }

    /**
     * Opt OUT of rowid-based UPDATE reconstruction + the SQLITE_NOCHANGE
     * optimization. Set this when the table's rowid is NOT a reliable lookup key
     * for UPDATE: row_lookup() cannot resolve every rowid the scans produce to
     * the exact row (e.g. names -- full-scan rowid is a cache position but
     * row_lookup() expects an ea; ctree_labels -- the func filter iterator's
     * rowid is func-local but row_lookup() expects a global index). Such tables
     * are reconstructed from the real column values in argv, which requires
     * NOCHANGE to stay disabled so unchanged identity columns carry real values.
     * Leave OFF (default) for tables whose row_lookup() resolves their scan
     * rowids and for tables with many/paired writable columns that rely on
     * unchanged columns being skipped during xUpdate.
     */
    CachedTableBuilder& update_from_column_values(bool value = true) {
        def_.update_from_column_values = value;
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
     * INVARIANT: the extractor MUST return the RAW value of the named column --
     * the same int64 SQLite sees for that column, with no transformation (no
     * scaling, masking, remapping, or derived value). Both equality (WHERE col =
     * ?) and range (WHERE col >= ? / BETWEEN) constraints on this column are
     * OMITTED from SQLite's re-check on this basis (see xBestIndex: omit=1), so a
     * non-raw extractor would make the pushed window disagree with the WHERE
     * predicate and return wrong rows. All current consumers satisfy this.
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
constexpr int CONSTRAINT_FILTER_BASE = 2000;

enum class ConstraintOp {
    Eq,
    Gt,
    Le,
    Lt,
    Ge,
    Like
};

inline int sqlite_constraint_op(ConstraintOp op) {
    switch (op) {
        case ConstraintOp::Eq: return SQLITE_INDEX_CONSTRAINT_EQ;
        case ConstraintOp::Gt: return SQLITE_INDEX_CONSTRAINT_GT;
        case ConstraintOp::Le: return SQLITE_INDEX_CONSTRAINT_LE;
        case ConstraintOp::Lt: return SQLITE_INDEX_CONSTRAINT_LT;
        case ConstraintOp::Ge: return SQLITE_INDEX_CONSTRAINT_GE;
        case ConstraintOp::Like: return SQLITE_INDEX_CONSTRAINT_LIKE;
    }
    return SQLITE_INDEX_CONSTRAINT_EQ;
}

struct ConstraintRequest {
    std::string column_name;
    ConstraintOp op = ConstraintOp::Eq;
    bool required = false;
    std::string missing_error;
};

inline ConstraintRequest required_eq(const char* column_name, const char* missing_error) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Eq, true,
                             missing_error ? missing_error : ""};
}

inline ConstraintRequest required_like(const char* column_name, const char* missing_error) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Like, true,
                             missing_error ? missing_error : ""};
}

inline ConstraintRequest optional_eq(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Eq, false, ""};
}

inline ConstraintRequest optional_like(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Like, false, ""};
}

inline ConstraintRequest optional_gt(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Gt, false, ""};
}

inline ConstraintRequest optional_ge(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Ge, false, ""};
}

inline ConstraintRequest optional_lt(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Lt, false, ""};
}

inline ConstraintRequest optional_le(const char* column_name) {
    return ConstraintRequest{column_name ? column_name : "", ConstraintOp::Le, false, ""};
}

struct GeneratorConstraintSpec {
    int column_index = -1;
    ConstraintOp op = ConstraintOp::Eq;
    bool required = false;
    std::string missing_error;
};

struct GeneratorConstraintArg {
    int column_index = -1;
    ConstraintOp op = ConstraintOp::Eq;
    FunctionArg value;
};

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
struct ConstraintFilterDef {
    std::vector<GeneratorConstraintSpec> specs;
    int filter_id;
    double estimated_cost;
    double estimated_rows;
    int ordered_column = -1;
    bool ordered_desc = false;
    std::function<std::unique_ptr<Generator<RowData>>(
        const std::vector<GeneratorConstraintArg>&)> create;
};

template<typename RowData>
struct GeneratorTableDef {
    std::string name;
    std::function<size_t()> estimate_rows_fn;
    // Optional exact row count. When set, a bare `SELECT COUNT(*)` (no
    // constraints, no columns) is answered via COUNT_ONLY_SCAN — emitting N
    // phantom rows — instead of materializing every generated row. Lets a
    // backend that can count cheaply (e.g. DIA get_Count, memoized) skip a
    // pathological full enumeration. See generator_table_supports_count_only_scan.
    std::function<size_t()> row_count_fn;
    std::function<std::unique_ptr<Generator<RowData>>()> generator_factory_fn;
    // Optional projection-aware full-scan factory. When set, a full-scan plan
    // carries SQLite's colUsed bitmask (via idxStr, like projection_cache_builder_fn
    // on cached tables) so the generator can skip materializing expensive unused
    // columns (e.g. an undecorated name that requires demangling). Null => the plain
    // generator_factory_fn is used, so existing consumers are unaffected.
    std::function<std::unique_ptr<Generator<RowData>>(uint64_t)> projection_generator_factory_fn;
    std::vector<CachedColumnDef<RowData>> columns;
    std::vector<FilterDef> filters;
    std::vector<ParametricFilterDef<RowData>> parametric_filters;
    std::vector<ConstraintFilterDef<RowData>> constraint_filters;
    std::unordered_set<int> hidden_columns;  // Column indices marked HIDDEN
    std::string full_scan_error;
    std::function<bool(RowData&, int64_t)> row_lookup;
    std::function<bool(RowData&)> delete_row;
    bool supports_delete = false;
    std::function<bool(int argc, FunctionArg* argv)> insert_row;
    bool supports_insert = false;
    std::function<void(const std::string&)> before_modify;
    std::function<void(const std::string&)> after_modify;

    TransactionHooks transaction_hooks;

    std::string schema() const {
        return detail::render_table_schema(name, columns, [this](size_t i) {
            return hidden_columns.count(static_cast<int>(i)) != 0;
        });
    }

    int find_column(const std::string& col_name) const {
        return detail::find_column_index(columns, col_name);
    }

    const FilterDef* find_filter(int col_index) const {
        return detail::find_filter_by_column(filters, col_index);
    }
};

inline std::vector<int> parse_constraint_index_list(const char* idx_str) {
    std::vector<int> result;
    if (!idx_str || !*idx_str) return result;

    std::stringstream ss(idx_str);
    std::string part;
    while (std::getline(ss, part, ',')) {
        if (!part.empty()) {
            result.push_back(std::atoi(part.c_str()));
        }
    }
    return result;
}

template<typename RowData>
struct GeneratorCursor {
    sqlite3_vtab_cursor base;
    const GeneratorTableDef<RowData>* def = nullptr;
    std::unique_ptr<Generator<RowData>> generator;
    bool generator_eof = false;
    std::unique_ptr<RowIterator> iterator;
    bool using_iterator = false;
    bool iterator_eof = false;
    // COUNT_ONLY_SCAN: emit count_only_total phantom (all-NULL) rows without
    // running the generator, so `SELECT COUNT(*)` returns the right count cheaply.
    bool using_count_only = false;
    size_t count_only_total = 0;
    size_t count_only_row = 0;
};

namespace detail {
template<typename RowData>
inline bool generator_table_supports_count_only_scan(const GeneratorTableDef<RowData>* def) {
    // A bare COUNT(*) needs neither columns nor rowids, so an exact row_count_fn
    // is sufficient. (Generator tables have no rowid_fn / scan-driven mutation
    // path to conflict with, unlike CachedTableDef.)
    return def && def->row_count_fn;
}

template<typename RowData>
inline int materialize_count_only_generator(GeneratorCursor<RowData>* cursor,
                                            uint64_t col_used) {
    if (!cursor || !cursor->using_count_only) {
        return to_sqlite_status(Status::ok);
    }

    // colUsed == 0 is shared by COUNT(*) and rowid-only scans. COUNT(*) never
    // asks for xColumn/xRowid, but a rowid scan does; lazily switch that scan to
    // the real generator so it observes backend rowids instead of phantom
    // positional values.
    //
    // That switch IS a full enumeration, so a table that forbids full scans must
    // refuse it here exactly as xFilter's full-scan branch would. Only the
    // count-only answer (which enumerates nothing) is exempt from the guard.
    if (!cursor->def->full_scan_error.empty()) {
        cursor->using_count_only = false;
        cursor->generator_eof = true;
        set_vtab_error(cursor->def->full_scan_error);
        return return_vtab_error(cursor->base.pVtab);
    }

    clear_vtab_error();
    if (cursor->def->projection_generator_factory_fn) {
        cursor->generator =
            cursor->def->projection_generator_factory_fn(col_used);
    } else if (cursor->def->generator_factory_fn) {
        cursor->generator = cursor->def->generator_factory_fn();
    }
    if (!get_vtab_error().empty()) {
        cursor->generator_eof = true;
        return return_vtab_error(cursor->base.pVtab);
    }

    cursor->generator_eof = true;
    for (size_t row = 0; cursor->generator && row <= cursor->count_only_row;
         ++row) {
        clear_vtab_error();
        const bool has_row = cursor->generator->next();
        if (!get_vtab_error().empty()) {
            cursor->generator_eof = true;
            return return_vtab_error(cursor->base.pVtab);
        }
        if (!has_row) {
            cursor->generator_eof = true;
            break;
        }
        cursor->generator_eof = false;
    }

    cursor->using_count_only = false;
    if (!cursor->generator || cursor->generator_eof) {
        set_vtab_error(
            "generator row_count() exceeded the rows produced by generator()");
        return return_vtab_error(cursor->base.pVtab);
    }
    return to_sqlite_status(Status::ok);
}
}  // namespace detail

template<typename RowData>
struct GeneratorVtab {
    sqlite3_vtab base;
    const GeneratorTableDef<RowData>* def = nullptr;
    sqlite3* db = nullptr;
    std::string schema_name;
    std::string table_name;
    detail::TransactionState transaction;
};

// SQLite callbacks for generator tables
template<typename RowData>
inline int generator_vtab_connect(sqlite3* db, void* pAux, int argc,
                                  const char* const* argv,
                                  sqlite3_vtab** ppVtab, char**) {
    const auto* def = static_cast<const GeneratorTableDef<RowData>*>(pAux);
    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (!xsql::is_ok(rc)) return rc;
    auto* vtab = new GeneratorVtab<RowData>();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    vtab->db = db;
    if (argc > 1 && argv && argv[1]) vtab->schema_name = argv[1];
    if (argc > 2 && argv && argv[2]) vtab->table_name = argv[2];
    *ppVtab = &vtab->base;
    detail::write_surface_connected(
        db, argc > 0 && argv ? argv[0] : nullptr,
        vtab->schema_name.c_str(), vtab->table_name.c_str());
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_destroy(sqlite3_vtab* pVtab) {
    auto* vtab = reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    detail::write_surface_destroyed(
        vtab->db, vtab->schema_name.c_str(), vtab->table_name.c_str());
    delete vtab;
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
    cursor->using_count_only = false;
    cursor->count_only_total = 0;
    cursor->count_only_row = 0;
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
    if (cursor->using_count_only) {
        cursor->count_only_row++;
    } else if (cursor->using_iterator && cursor->iterator) {
        clear_vtab_error();
        if (!cursor->iterator->next()) {
            cursor->iterator_eof = true;
        }
        if (!get_vtab_error().empty()) {
            cursor->iterator_eof = true;
            return return_vtab_error(pCursor->pVtab);
        }
    } else {
        clear_vtab_error();
        if (!cursor->generator || !cursor->generator->next()) {
            cursor->generator_eof = true;
        }
        if (!get_vtab_error().empty()) {
            cursor->generator_eof = true;
            return return_vtab_error(pCursor->pVtab);
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (cursor->using_count_only) {
        return cursor->count_only_row >= cursor->count_only_total ? 1 : 0;
    }
    if (cursor->using_iterator) {
        if (!cursor->iterator || cursor->iterator_eof) return 1;
        return cursor->iterator->eof() ? 1 : 0;
    }
    return (!cursor->generator || cursor->generator_eof) ? 1 : 0;
}

template<typename RowData>
inline int generator_vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);
    if (sqlite3_vtab_nochange(ctx)) {
        return to_sqlite_status(Status::ok);
    }

    if (cursor->using_count_only) {
        // The plan said colUsed == 0, yet a column is being read. We cannot know
        // which OTHER columns SQLite will ask for on this same row, and the
        // generator is built exactly once here -- so request every column. A
        // per-column mask would leave a projection-aware generator silently
        // omitting the columns fetched after this first one.
        const int rc =
            detail::materialize_count_only_generator(cursor, ~uint64_t{0});
        if (!xsql::is_ok(rc)) return rc;
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
    if (cursor->using_count_only) {
        const int rc =
            detail::materialize_count_only_generator(cursor, uint64_t{0});
        if (!xsql::is_ok(rc)) return rc;
    }
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
inline int generator_vtab_filter(sqlite3_vtab_cursor* pCursor, int idxNum, const char* idxStr,
                                 int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<GeneratorCursor<RowData>*>(pCursor);

    cursor->generator = nullptr;
    cursor->generator_eof = false;
    cursor->iterator = nullptr;
    cursor->using_iterator = false;
    cursor->iterator_eof = false;
    cursor->using_count_only = false;
    cursor->count_only_total = 0;
    cursor->count_only_row = 0;

    if (idxNum == COUNT_ONLY_SCAN && argc == 0 &&
        detail::generator_table_supports_count_only_scan(cursor->def)) {
        clear_vtab_error();
        cursor->using_count_only = true;
        // row_count_fn is user code on a C-ABI boundary: an exception must not
        // unwind through SQLite's VDBE (same rule as the transaction hooks).
        try {
            cursor->count_only_total = cursor->def->row_count_fn();
        } catch (const std::exception& e) {
            set_vtab_error(e.what());
        } catch (...) {
            set_vtab_error("row_count callback threw");
        }
        if (!get_vtab_error().empty()) {
            return return_vtab_error(pCursor->pVtab);
        }
        return to_sqlite_status(Status::ok);
    }

    if (idxNum == MISSING_REQUIRED_CONSTRAINT) {
        set_vtab_error(idxStr && idxStr[0] ? idxStr : "required constraint missing");
        return return_vtab_error(pCursor->pVtab);
    }

    if (idxNum != FILTER_NONE && argc > 0) {
        // Check constraint filters first (multi-column, multi-operator).
        if (idxNum >= CONSTRAINT_FILTER_BASE) {
            for (const auto& cf : cursor->def->constraint_filters) {
                if (cf.filter_id == idxNum) {
                    std::vector<int> spec_indices = parse_constraint_index_list(idxStr);
                    std::vector<GeneratorConstraintArg> args;
                    args.reserve(static_cast<size_t>(argc));
                    for (int i = 0; i < argc && i < static_cast<int>(spec_indices.size()); i++) {
                        int spec_idx = spec_indices[static_cast<size_t>(i)];
                        if (spec_idx < 0 || static_cast<size_t>(spec_idx) >= cf.specs.size()) {
                            continue;
                        }
                        const auto& spec = cf.specs[static_cast<size_t>(spec_idx)];
                        args.push_back(GeneratorConstraintArg{
                            spec.column_index,
                            spec.op,
                            FunctionArg(argv[i])
                        });
                    }
                    clear_vtab_error();
                    cursor->generator = cf.create(args);
                    if (!get_vtab_error().empty()) {
                        return return_vtab_error(pCursor->pVtab);
                    }
                    clear_vtab_error();
                    cursor->using_iterator = false;
                    cursor->generator_eof = true;
                    if (cursor->generator) {
                        clear_vtab_error();
                        cursor->generator_eof = !cursor->generator->next();
                        if (!get_vtab_error().empty()) {
                            cursor->generator_eof = true;
                            return return_vtab_error(pCursor->pVtab);
                        }
                    }
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Check parametric filters (multi-column, creates a Generator)
        if (idxNum >= PARAMETRIC_FILTER_BASE) {
            for (const auto& pf : cursor->def->parametric_filters) {
                if (pf.filter_id == idxNum) {
                    std::vector<FunctionArg> args;
                    args.reserve(argc);
                    for (int i = 0; i < argc; i++) {
                        args.emplace_back(argv[i]);
                    }
                    clear_vtab_error();
                    cursor->generator = pf.create(args);
                    if (!get_vtab_error().empty()) {
                        return return_vtab_error(pCursor->pVtab);
                    }
                    clear_vtab_error();
                    cursor->using_iterator = false;
                    cursor->generator_eof = true;
                    if (cursor->generator) {
                        clear_vtab_error();
                        cursor->generator_eof = !cursor->generator->next();
                        if (!get_vtab_error().empty()) {
                            cursor->generator_eof = true;
                            return return_vtab_error(pCursor->pVtab);
                        }
                    }
                    return to_sqlite_status(Status::ok);
                }
            }
        }

        // Single-column filters (creates a RowIterator)
        for (const auto& filter : cursor->def->filters) {
            if (filter.filter_id == idxNum) {
                clear_vtab_error();
                cursor->iterator = filter.create(FunctionArg(argv[0]));
                if (!get_vtab_error().empty()) {
                    return return_vtab_error(pCursor->pVtab);
                }
                clear_vtab_error();
                cursor->using_iterator = true;
                cursor->iterator_eof = true;
                if (cursor->iterator) {
                    clear_vtab_error();
                    cursor->iterator_eof = !cursor->iterator->next();
                    if (!get_vtab_error().empty()) {
                        cursor->iterator_eof = true;
                        return return_vtab_error(pCursor->pVtab);
                    }
                }
                return to_sqlite_status(Status::ok);
            }
        }
    }

    if (!cursor->def->full_scan_error.empty()) {
        set_vtab_error(cursor->def->full_scan_error);
        return return_vtab_error(pCursor->pVtab);
    }

    // Full scan - create generator and position to first row. A projection-aware
    // factory (if set) receives colUsed (decoded from idxStr, set on the FILTER_NONE
    // plan) so it can skip expensive unused columns; otherwise the plain factory runs.
    cursor->using_iterator = false;
    cursor->generator_eof = true;
    clear_vtab_error();
    if (cursor->def->projection_generator_factory_fn) {
        uint64_t col_used = ~0ull;  // absent/unparseable => assume all columns (safe)
        if (idxStr && *idxStr) {
            col_used = static_cast<uint64_t>(strtoull(idxStr, nullptr, 10));
        }
        cursor->generator = cursor->def->projection_generator_factory_fn(col_used);
    } else if (cursor->def->generator_factory_fn) {
        cursor->generator = cursor->def->generator_factory_fn();
    }
    if (!get_vtab_error().empty()) {
        cursor->generator_eof = true;
        return return_vtab_error(pCursor->pVtab);
    }
    if (cursor->generator) {
        clear_vtab_error();
        cursor->generator_eof = !cursor->generator->next();
        if (!get_vtab_error().empty()) {
            cursor->generator_eof = true;
            return return_vtab_error(pCursor->pVtab);
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    const auto* def = vtab->def;

    // Bare COUNT(*) (no constraints, no columns referenced): answer via the
    // exact row_count_fn instead of a full generator enumeration. Mirrors the
    // CachedTableDef COUNT_ONLY_SCAN plan. Must precede filter matching, though
    // with nConstraint == 0 no filter would match anyway.
    if (pInfo->nConstraint == 0 && pInfo->colUsed == 0 &&
        detail::generator_table_supports_count_only_scan(def)) {
        // Both callbacks are user code on a C-ABI boundary: a throw must not
        // unwind through SQLite. A failing estimate simply skips the
        // COUNT_ONLY promotion and lets the normal full-scan plan proceed
        // (xFilter re-invokes row_count_fn with its own guard).
        try {
            size_t estimated_rows = def->estimate_rows_fn
                                        ? def->estimate_rows_fn()
                                        : def->row_count_fn();
            pInfo->idxNum = COUNT_ONLY_SCAN;
            pInfo->estimatedCost = static_cast<double>(estimated_rows);
            pInfo->estimatedRows = static_cast<sqlite3_int64>(estimated_rows);
            return to_sqlite_status(Status::ok);
        } catch (...) {
        }
    }

    // First, check constraint filters (multi-column, multi-operator).
    const ConstraintFilterDef<RowData>* best_cf = nullptr;
    std::vector<int> best_matched_constraints;
    bool best_consumes_order = false;
    int best_matched_count = -1;
    std::string missing_required_error;

    for (const auto& cf : def->constraint_filters) {
        std::vector<int> matched_constraints(cf.specs.size(), -1);
        bool all_required_matched = true;
        int matched_count = 0;

        for (size_t s = 0; s < cf.specs.size(); ++s) {
            const auto& spec = cf.specs[s];
            bool found = false;
            const int sqlite_op = sqlite_constraint_op(spec.op);
            for (int i = 0; i < pInfo->nConstraint; i++) {
                const auto& c = pInfo->aConstraint[i];
                if (!c.usable) continue;
                if (c.op != sqlite_op) continue;
                if (c.iColumn == spec.column_index) {
                    matched_constraints[s] = i;
                    found = true;
                    matched_count++;
                    break;
                }
            }
            if (spec.required && !found) {
                all_required_matched = false;
                if (missing_required_error.empty() && !spec.missing_error.empty()) {
                    missing_required_error = spec.missing_error;
                }
                break;
            }
        }

        if (all_required_matched && matched_count > 0) {
            bool consumes_order = false;
            if (cf.ordered_column >= 0 && pInfo->nOrderBy == 1) {
                const auto& order = pInfo->aOrderBy[0];
                if (order.iColumn == cf.ordered_column &&
                    ((order.desc != 0) == cf.ordered_desc)) {
                    consumes_order = true;
                }
            }

            if (!best_cf ||
                cf.estimated_cost < best_cf->estimated_cost ||
                (cf.estimated_cost == best_cf->estimated_cost &&
                 consumes_order && !best_consumes_order) ||
                (cf.estimated_cost == best_cf->estimated_cost &&
                 consumes_order == best_consumes_order &&
                 matched_count > best_matched_count)) {
                best_cf = &cf;
                best_matched_constraints = std::move(matched_constraints);
                best_consumes_order = consumes_order;
                best_matched_count = matched_count;
            }
        }
    }

    if (best_cf) {
        std::ostringstream idx_str;
        int argv_index = 1;
        bool first = true;
        for (size_t s = 0; s < best_matched_constraints.size(); ++s) {
            int constraint_idx = best_matched_constraints[s];
            if (constraint_idx < 0) continue;
            pInfo->aConstraintUsage[constraint_idx].argvIndex = argv_index++;
            pInfo->aConstraintUsage[constraint_idx].omit = 1;
            if (!first) idx_str << ",";
            idx_str << s;
            first = false;
        }

        if (best_consumes_order) {
            pInfo->orderByConsumed = 1;
        }

        pInfo->idxNum = best_cf->filter_id;
        pInfo->idxStr = sqlite3_mprintf("%s", idx_str.str().c_str());
        pInfo->needToFreeIdxStr = 1;
        pInfo->estimatedCost = best_cf->estimated_cost;
        pInfo->estimatedRows = static_cast<sqlite3_int64>(best_cf->estimated_rows);
        return to_sqlite_status(Status::ok);
    }

    // Next, check parametric filters (multi-column, e.g. hidden params for table-valued functions).
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
    } else if (!missing_required_error.empty()) {
        pInfo->idxNum = MISSING_REQUIRED_CONSTRAINT;
        pInfo->idxStr = sqlite3_mprintf("%s", missing_required_error.c_str());
        pInfo->needToFreeIdxStr = 1;
        pInfo->estimatedCost = 1.0;
        pInfo->estimatedRows = 0;
    } else {
        size_t estimated_rows = 1000;
        if (def->estimate_rows_fn) {
            estimated_rows = def->estimate_rows_fn();
        }
        pInfo->idxNum = FILTER_NONE;
        pInfo->estimatedCost = static_cast<double>(estimated_rows);
        pInfo->estimatedRows = estimated_rows;
        // Full-scan plan: carry colUsed to xFilter via idxStr (mirrors
        // projection_cache_builder_fn) so a projection-aware generator can skip
        // expensive unused columns. Only the FILTER_NONE plan reaches xFilter's
        // full-scan branch and uses idxStr (filter/constraint plans return above),
        // so there is no collision. Only allocated for projection-aware tables.
        if (def->projection_generator_factory_fn) {
            if (char* s = sqlite3_mprintf("%llu",
                    static_cast<unsigned long long>(pInfo->colUsed))) {
                pInfo->idxStr = s;
                pInfo->needToFreeIdxStr = 1;
            }
        }
    }
    return to_sqlite_status(Status::ok);
}

template<typename RowData>
inline int generator_vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<GeneratorVtab<RowData>*>(pVtab);
    const auto* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row || !def->row_lookup) {
            return detail::unsupported_delete(pVtab, def->name);
        }

        RowData row{};
        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        clear_vtab_error();
        if (!def->row_lookup(row, raw_rowid)) {
            const std::string& err = get_vtab_error();
            if (!err.empty()) {
                set_vtab_errmsg(pVtab, err.c_str());
            }
            clear_vtab_error();
            return to_sqlite_status(Status::error);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        clear_vtab_error();
        if (!def->delete_row(row)) {
            const std::string& err = get_vtab_error();
            if (!err.empty()) {
                set_vtab_errmsg(pVtab, err.c_str());
            }
            clear_vtab_error();
            return to_sqlite_status(Status::error);
        }

        if (def->after_modify) def->after_modify("DELETE FROM " + def->name);
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        bool has_writable = false;
        for (const auto& col : def->columns) {
            if (col.writable && col.set) {
                has_writable = true;
                break;
            }
        }
        if (!has_writable || !def->row_lookup) {
            return detail::unsupported_update(pVtab, def->name);
        }

        RowData row{};
        const int64_t raw_rowid = sqlite3_value_int64(argv[0]);
        clear_vtab_error();
        if (!def->row_lookup(row, raw_rowid)) {
            const std::string& err = get_vtab_error();
            if (!err.empty()) {
                set_vtab_errmsg(pVtab, err.c_str());
            }
            clear_vtab_error();
            return to_sqlite_status(Status::error);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        // This module's xColumn always honors NOCHANGE (the row is resolved via
        // row_lookup, not argv), so a non-NOCHANGE value on a read-only column is a
        // genuine write.
        const Status st = detail::apply_update_columns(
            pVtab, argc, argv, def->columns.size(), /*nochange_eligible=*/true,
            [&](size_t c) { return def->columns[c].writable && static_cast<bool>(def->columns[c].set); },
            [&](size_t c) -> const std::string& { return def->columns[c].name; },
            [&](size_t c) { return def->columns[c].set(row, FunctionArg(argv[c + 2])); });
        if (st != Status::ok) return to_sqlite_status(st);

        if (def->after_modify) def->after_modify("UPDATE " + def->name);
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    // argc > 1, argv[0] == NULL: INSERT
    if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        if (!def->supports_insert || !def->insert_row) {
            return detail::unsupported_insert(pVtab, def->name);
        }

        vtab->transaction.touched = true;
        if (def->before_modify) {
            def->before_modify("INSERT INTO " + def->name);
        }

        bool ok = false;
        clear_vtab_error();
        detail::with_args(argc - 2, &argv[2], [&](FunctionArg* args) {
            ok = def->insert_row(argc - 2, args);
        });
        if (!ok) {
            const std::string& err = get_vtab_error();
            if (!err.empty()) {
                set_vtab_errmsg(pVtab, err.c_str());
            }
            clear_vtab_error();
            return to_sqlite_status(Status::error);
        }
        clear_vtab_error();

        if (def->after_modify) def->after_modify("INSERT INTO " + def->name);
        vtab->transaction.wrote = true;
        return to_sqlite_status(Status::ok);
    }

    return to_sqlite_status(Status::read_only);
}

inline int generator_vtab_read_only_update(sqlite3_vtab* pVtab, int, sqlite3_value**, sqlite3_int64*) {
    // A fully read-only generator table has no write capability at all. Report
    // the surface as read-only rather than SQLite's generic "attempt to write a
    // readonly database" (which misleadingly implies the whole DB is read-only).
    // (This module is shared across row types, so the table name is not reachable
    // here; the message names the surface generically.)
    set_vtab_errmsg(pVtab, "this table is read-only (no write capability)");
    return to_sqlite_status(Status::error);
}

template<typename RowData>
inline sqlite3_module create_generator_module() {
    sqlite3_module mod = {};
    mod.iVersion = 3;
    mod.xCreate = generator_vtab_connect<RowData>;
    mod.xConnect = generator_vtab_connect<RowData>;
    mod.xBestIndex = generator_vtab_best_index<RowData>;
    mod.xDisconnect = generator_vtab_disconnect<RowData>;
    mod.xDestroy = generator_vtab_destroy<RowData>;
    mod.xOpen = generator_vtab_open<RowData>;
    mod.xClose = generator_vtab_close<RowData>;
    mod.xFilter = generator_vtab_filter<RowData>;
    mod.xNext = generator_vtab_next<RowData>;
    mod.xEof = generator_vtab_eof<RowData>;
    mod.xColumn = generator_vtab_column<RowData>;
    mod.xRowid = generator_vtab_rowid<RowData>;
    if constexpr (std::is_default_constructible_v<RowData>) {
        mod.xUpdate = generator_vtab_update<RowData>;
    } else {
        mod.xUpdate = generator_vtab_read_only_update;
    }
    // xBegin enrolls the vtab; xSync runs the fallible hook; xCommit clears
    // state. Inert for read-only generator tables,
    // which never enroll in a write transaction.
    mod.xBegin = vtab_xbegin<GeneratorVtab<RowData>>;
    mod.xSync = vtab_prepare_commit<GeneratorVtab<RowData>>;
    mod.xCommit = vtab_finish_commit<GeneratorVtab<RowData>>;
    mod.xRollback = vtab_xrollback<GeneratorVtab<RowData>>;
    mod.xSavepoint = vtab_xsavepoint<GeneratorVtab<RowData>>;
    mod.xRelease = vtab_xrelease<GeneratorVtab<RowData>>;
    mod.xRollbackTo = vtab_xrollback_to<GeneratorVtab<RowData>>;
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
        // Do NOT delete `owned` here: sqlite3_create_module_v2 invokes the
        // xDestroy callback (destroy_def) even when registration fails, which
        // already deletes the clone. A manual delete would be a double-free.
        // (See SQLite docs: the destructor "is also invoked if the call ...
        // fails" — same rule as sqlite3_create_function_v2.)
        return false;
    }
    return true;
}

template<typename RowData>
inline bool register_generator_vtable(Database& db,
                                      const char* module_name,
                                      const GeneratorTableDef<RowData>* def) {
    if (!detail::register_generator_vtable_sqlite<RowData>(db.sqlite_handle(), module_name, def))
        return false;
    // A generator DELETE/UPDATE additionally requires a row_lookup to resolve the
    // target row (see generator_vtab_update), so those caps are gated on it.
    const bool has_lookup = static_cast<bool>(def->row_lookup);
    db.record_write_surface(module_name,
                            def->supports_insert && static_cast<bool>(def->insert_row),
                            def->supports_delete && static_cast<bool>(def->delete_row) && has_lookup,
                            has_lookup ? detail::def_writable_columns(def)
                                       : std::vector<std::string>{});
    return true;
}

// Generator Table Builder
template<typename RowData>
class GeneratorTableBuilder {
    GeneratorTableDef<RowData> def_;

    void add_hidden_column(const char* name, ColumnType type) {
        int idx = static_cast<int>(def_.columns.size());
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, type, false,
            [](FunctionContext& ctx, const RowData&) { ctx.result_null(); }));
        def_.hidden_columns.insert(idx);
    }

public:
    explicit GeneratorTableBuilder(const char* name) {
        def_.name = name;
    }

    GeneratorTableBuilder& estimate_rows(std::function<size_t()> fn) {
        def_.estimate_rows_fn = std::move(fn);
        return *this;
    }

    // Exact row count for the COUNT_ONLY_SCAN fast path. Supply when the backend
    // can count without a full enumeration (e.g. a memoized DIA get_Count), so a
    // bare `SELECT COUNT(*)` skips materializing every row.
    GeneratorTableBuilder& row_count(std::function<size_t()> fn) {
        def_.row_count_fn = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& generator(std::function<std::unique_ptr<Generator<RowData>>()> fn) {
        def_.generator_factory_fn = std::move(fn);
        return *this;
    }

    // Projection-aware full-scan factory: receives SQLite's colUsed bitmask so the
    // generator can skip materializing expensive unused columns (e.g. an undecorated
    // name that requires demangling). Used for the full-scan plan when set; falls
    // back to generator() otherwise. Additive — existing tables need not set it.
    GeneratorTableBuilder& projection_generator(
            std::function<std::unique_ptr<Generator<RowData>>(uint64_t)> fn) {
        def_.projection_generator_factory_fn = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& full_scan_error(std::string message) {
        def_.full_scan_error = std::move(message);
        return *this;
    }

    GeneratorTableBuilder& on_modify(std::function<void(const std::string&)> fn) {
        def_.before_modify = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& after_modify(std::function<void(const std::string&)> fn) {
        def_.after_modify = std::move(fn);
        return *this;
    }

    GeneratorTableBuilder& transaction_hooks(TransactionHooks hooks) {
        def_.transaction_hooks = std::move(hooks);
        return *this;
    }

    GeneratorTableBuilder& column(const char* name,
                                  ColumnType type,
                                  std::function<void(FunctionContext&, const RowData&)> getter) {
        def_.columns.push_back(
            detail::make_row_column<RowData>(name, type, false, std::move(getter)));
        return *this;
    }

    GeneratorTableBuilder& column_rw(const char* name,
                                     ColumnType type,
                                     std::function<void(FunctionContext&, const RowData&)> getter,
                                     std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(
            detail::make_row_column<RowData>(name, type, true, std::move(getter), std::move(setter)));
        return *this;
    }

    GeneratorTableBuilder& column_int64(const char* name, std::function<int64_t(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, false, detail::row_getter_int64<RowData>(std::move(getter))));
        return *this;
    }

    GeneratorTableBuilder& column_int64_rw(const char* name,
                                           std::function<int64_t(const RowData&)> getter,
                                           std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int64<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    GeneratorTableBuilder& column_int64_rw(const char* name,
                                           std::function<int64_t(const RowData&)> getter,
                                           std::function<bool(RowData&, int64_t)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int64<RowData>(std::move(getter)),
            detail::row_setter_int64<RowData>(std::move(setter))));
        return *this;
    }

    GeneratorTableBuilder& column_int(const char* name, std::function<int(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, false, detail::row_getter_int<RowData>(std::move(getter))));
        return *this;
    }

    GeneratorTableBuilder& column_int_rw(const char* name,
                                         std::function<int(const RowData&)> getter,
                                         std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    GeneratorTableBuilder& column_int_rw(const char* name,
                                         std::function<int(const RowData&)> getter,
                                         std::function<bool(RowData&, int)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Integer, true,
            detail::row_getter_int<RowData>(std::move(getter)),
            detail::row_setter_int<RowData>(std::move(setter))));
        return *this;
    }

    GeneratorTableBuilder& column_text(const char* name, std::function<std::string(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, false, detail::row_getter_text<RowData>(std::move(getter))));
        return *this;
    }

    GeneratorTableBuilder& column_text_rw(const char* name,
                                          std::function<std::string(const RowData&)> getter,
                                          std::function<bool(RowData&, FunctionArg)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, true,
            detail::row_getter_text<RowData>(std::move(getter)), std::move(setter)));
        return *this;
    }

    GeneratorTableBuilder& column_text_rw(const char* name,
                                          std::function<std::string(const RowData&)> getter,
                                          std::function<bool(RowData&, const char*)> setter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Text, true,
            detail::row_getter_text<RowData>(std::move(getter)),
            detail::row_setter_text<RowData>(std::move(setter))));
        return *this;
    }

    GeneratorTableBuilder& column_double(const char* name, std::function<double(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Real, false, detail::row_getter_double<RowData>(std::move(getter))));
        return *this;
    }

    GeneratorTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(const RowData&)> getter) {
        def_.columns.push_back(detail::make_row_column<RowData>(
            name, ColumnType::Blob, false, detail::row_getter_blob<RowData>(std::move(getter))));
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
        add_hidden_column(name, ColumnType::Integer);
        return *this;
    }

    GeneratorTableBuilder& hidden_column_text(const char* name) {
        add_hidden_column(name, ColumnType::Text);
        return *this;
    }

    GeneratorTableBuilder& hidden_column_int(const char* name) {
        add_hidden_column(name, ColumnType::Integer);
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

    GeneratorTableBuilder& constraint_filter(
            std::initializer_list<ConstraintRequest> constraints,
            std::function<std::unique_ptr<Generator<RowData>>(
                const std::vector<GeneratorConstraintArg>&)> factory,
            double cost = 1.0, double est_rows = 100.0) {
        ConstraintFilterDef<RowData> cf;
        for (const auto& req : constraints) {
            int idx = def_.find_column(req.column_name);
            if (idx < 0) return *this;
            cf.specs.push_back(GeneratorConstraintSpec{
                idx,
                req.op,
                req.required,
                req.missing_error
            });
        }
        cf.filter_id = CONSTRAINT_FILTER_BASE + static_cast<int>(def_.constraint_filters.size());
        cf.estimated_cost = cost;
        cf.estimated_rows = est_rows;
        cf.create = std::move(factory);
        def_.constraint_filters.push_back(std::move(cf));
        return *this;
    }

    GeneratorTableBuilder& order_by_consumed(const char* column_name, bool desc = false) {
        if (def_.constraint_filters.empty()) return *this;
        int idx = def_.find_column(column_name ? column_name : "");
        if (idx < 0) return *this;
        auto& cf = def_.constraint_filters.back();
        cf.ordered_column = idx;
        cf.ordered_desc = desc;
        return *this;
    }

    GeneratorTableBuilder& row_lookup(std::function<bool(RowData&, int64_t)> fn) {
        def_.row_lookup = std::move(fn);
        return *this;
    }

    // DELETE handler. The row is resolved via row_lookup() from the rowid, then
    // passed to delete_fn. Requires row_lookup() to be set.
    GeneratorTableBuilder& deletable(std::function<bool(RowData&)> delete_fn) {
        def_.supports_delete = true;
        def_.delete_row = std::move(delete_fn);
        return *this;
    }

    GeneratorTableBuilder& insertable(std::function<bool(int argc, FunctionArg* argv)> insert_fn) {
        def_.supports_insert = true;
        def_.insert_row = std::move(insert_fn);
        return *this;
    }

    GeneratorTableDef<RowData> build() { return std::move(def_); }
};

template<typename RowData>
inline GeneratorTableBuilder<RowData> generator_table(const char* name) {
    return GeneratorTableBuilder<RowData>(name);
}

} // namespace xsql
