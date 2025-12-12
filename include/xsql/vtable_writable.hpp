/**
 * xsql/vtable_writable.hpp - Writable SQLite Virtual Table framework
 *
 * Extends the base vtable framework with UPDATE/DELETE support.
 * Provides hooks for transaction management (e.g., undo points).
 *
 * Key features:
 *   - Live data access (fresh on every query)
 *   - UPDATE support via column setters
 *   - DELETE support via row delete handler
 *   - before_modify hook for undo/transaction integration
 */

#pragma once

#include "types.hpp"
#include <sstream>
#include <cstring>

namespace xsql {

// ============================================================================
// Writable Column Definition
// ============================================================================

struct WritableColumnDef {
    std::string name;
    ColumnType type;
    bool writable;

    // Getter: Fetch value at row index
    std::function<void(sqlite3_context*, size_t)> get;

    // Setter: Update value at row index (for UPDATE support)
    // Returns true on success
    std::function<bool(size_t, sqlite3_value*)> set;

    WritableColumnDef(const char* n, ColumnType t, bool w,
                      std::function<void(sqlite3_context*, size_t)> getter,
                      std::function<bool(size_t, sqlite3_value*)> setter = nullptr)
        : name(n), type(t), writable(w), get(std::move(getter)), set(std::move(setter)) {}
};

// ============================================================================
// Writable Virtual Table Definition
// ============================================================================

struct WritableVTableDef {
    std::string name;

    // Row count (called fresh each time for live data)
    std::function<size_t()> row_count;

    // Columns
    std::vector<WritableColumnDef> columns;

    // DELETE handler: Delete row at index, returns success
    std::function<bool(size_t)> delete_row;
    bool supports_delete = false;

    // Hook called before any modification (UPDATE/DELETE)
    // Use for undo points, logging, validation, etc.
    // Receives operation description (e.g., "DELETE FROM funcs")
    std::function<void(const std::string&)> before_modify;

    std::string schema() const {
        std::ostringstream ss;
        ss << "CREATE TABLE " << name << "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << columns[i].name << " " << column_type_sql(columns[i].type);
        }
        ss << ")";
        return ss.str();
    }
};

// ============================================================================
// SQLite Virtual Table Implementation (Writable)
// ============================================================================

struct WritableVtab {
    sqlite3_vtab base;
    const WritableVTableDef* def;
};

struct WritableCursor {
    sqlite3_vtab_cursor base;
    size_t idx;
    size_t total;
    const WritableVTableDef* def;
};

// xConnect/xCreate
inline int writable_vtab_connect(sqlite3* db, void* pAux, int, const char* const*,
                                  sqlite3_vtab** ppVtab, char**) {
    const WritableVTableDef* def = static_cast<const WritableVTableDef*>(pAux);

    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (rc != SQLITE_OK) return rc;

    auto* vtab = new WritableVtab();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

// xDisconnect/xDestroy
inline int writable_vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<WritableVtab*>(pVtab);
    return SQLITE_OK;
}

// xOpen
inline int writable_vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<WritableVtab*>(pVtab);
    auto* cursor = new WritableCursor();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->idx = 0;
    cursor->total = 0;  // Set in xFilter
    cursor->def = vtab->def;
    *ppCursor = &cursor->base;
    return SQLITE_OK;
}

// xClose
inline int writable_vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<WritableCursor*>(pCursor);
    return SQLITE_OK;
}

// xNext
inline int writable_vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<WritableCursor*>(pCursor);
    cursor->idx++;
    return SQLITE_OK;
}

// xEof
inline int writable_vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<WritableCursor*>(pCursor);
    return cursor->idx >= cursor->total;
}

// xColumn - fetches LIVE data each time
inline int writable_vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<WritableCursor*>(pCursor);
    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    cursor->def->columns[col].get(ctx, cursor->idx);
    return SQLITE_OK;
}

// xRowid
inline int writable_vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<WritableCursor*>(pCursor);
    *pRowid = static_cast<sqlite3_int64>(cursor->idx);
    return SQLITE_OK;
}

// xFilter - get fresh count for iteration
inline int writable_vtab_filter(sqlite3_vtab_cursor* pCursor, int, const char*, int, sqlite3_value**) {
    auto* cursor = reinterpret_cast<WritableCursor*>(pCursor);
    cursor->idx = 0;
    cursor->total = cursor->def->row_count();
    return SQLITE_OK;
}

// xBestIndex
inline int writable_vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<WritableVtab*>(pVtab);
    size_t count = vtab->def->row_count();
    pInfo->estimatedCost = static_cast<double>(count);
    pInfo->estimatedRows = count;
    return SQLITE_OK;
}

// xUpdate - handles INSERT, UPDATE, DELETE
inline int writable_vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<WritableVtab*>(pVtab);
    const WritableVTableDef* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row) {
            return SQLITE_READONLY;
        }

        size_t rowid = static_cast<size_t>(sqlite3_value_int64(argv[0]));

        // Call before_modify hook (e.g., create undo point)
        if (def->before_modify) {
            def->before_modify("DELETE FROM " + def->name);
        }

        if (!def->delete_row(rowid)) {
            return SQLITE_ERROR;
        }
        return SQLITE_OK;
    }

    // argc > 1, argv[0] != NULL: UPDATE
    if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        size_t old_rowid = static_cast<size_t>(sqlite3_value_int64(argv[0]));

        // Call before_modify hook
        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

        // argv[2..n] are the new column values
        for (size_t i = 2; i < static_cast<size_t>(argc) && (i - 2) < def->columns.size(); ++i) {
            size_t col_idx = i - 2;
            const auto& col = def->columns[col_idx];
            if (col.writable && col.set) {
                if (!col.set(old_rowid, argv[i])) {
                    return SQLITE_ERROR;
                }
            }
        }
        return SQLITE_OK;
    }

    // argc > 1, argv[0] == NULL: INSERT (not supported by default)
    return SQLITE_READONLY;
}

// Create module with xUpdate support
inline sqlite3_module create_writable_module() {
    sqlite3_module mod = {};
    mod.iVersion = 0;
    mod.xCreate = writable_vtab_connect;
    mod.xConnect = writable_vtab_connect;
    mod.xBestIndex = writable_vtab_best_index;
    mod.xDisconnect = writable_vtab_disconnect;
    mod.xDestroy = writable_vtab_disconnect;
    mod.xOpen = writable_vtab_open;
    mod.xClose = writable_vtab_close;
    mod.xFilter = writable_vtab_filter;
    mod.xNext = writable_vtab_next;
    mod.xEof = writable_vtab_eof;
    mod.xColumn = writable_vtab_column;
    mod.xRowid = writable_vtab_rowid;
    mod.xUpdate = writable_vtab_update;
    return mod;
}

inline sqlite3_module& get_writable_module() {
    static sqlite3_module mod = create_writable_module();
    return mod;
}

// ============================================================================
// Registration
// ============================================================================

inline bool register_writable_vtable(sqlite3* db, const char* module_name, const WritableVTableDef* def) {
    int rc = sqlite3_create_module_v2(db, module_name, &get_writable_module(),
                                       const_cast<WritableVTableDef*>(def), nullptr);
    return rc == SQLITE_OK;
}

inline bool create_writable_vtable(sqlite3* db, const char* table_name, const char* module_name) {
    std::string sql = "CREATE VIRTUAL TABLE " + std::string(table_name) +
                      " USING " + std::string(module_name) + ";";
    char* err = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (err) sqlite3_free(err);
    return rc == SQLITE_OK;
}

// ============================================================================
// Builder Pattern (Writable)
// ============================================================================

class WritableVTableBuilder {
    WritableVTableDef def_;
public:
    explicit WritableVTableBuilder(const char* name) {
        def_.name = name;
        def_.supports_delete = false;
    }

    WritableVTableBuilder& count(std::function<size_t()> fn) {
        def_.row_count = std::move(fn);
        return *this;
    }

    // Hook called before any modification
    WritableVTableBuilder& on_modify(std::function<void(const std::string&)> fn) {
        def_.before_modify = std::move(fn);
        return *this;
    }

    // Read-only integer column
    WritableVTableBuilder& column_int64(const char* name, std::function<int64_t(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int64(ctx, getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable integer column
    WritableVTableBuilder& column_int64_rw(const char* name,
                                            std::function<int64_t(size_t)> getter,
                                            std::function<bool(size_t, int64_t)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int64(ctx, getter(idx));
            },
            [setter = std::move(setter)](size_t idx, sqlite3_value* val) -> bool {
                return setter(idx, sqlite3_value_int64(val));
            });
        return *this;
    }

    // Read-only text column
    WritableVTableBuilder& column_text(const char* name, std::function<std::string(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                std::string val = getter(idx);
                sqlite3_result_text(ctx, val.c_str(), -1, SQLITE_TRANSIENT);
            },
            nullptr);
        return *this;
    }

    // Writable text column
    WritableVTableBuilder& column_text_rw(const char* name,
                                           std::function<std::string(size_t)> getter,
                                           std::function<bool(size_t, const char*)> setter) {
        def_.columns.emplace_back(name, ColumnType::Text, true,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                std::string val = getter(idx);
                sqlite3_result_text(ctx, val.c_str(), -1, SQLITE_TRANSIENT);
            },
            [setter = std::move(setter)](size_t idx, sqlite3_value* val) -> bool {
                const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                return setter(idx, text ? text : "");
            });
        return *this;
    }

    // Read-only int column
    WritableVTableBuilder& column_int(const char* name, std::function<int(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int(ctx, getter(idx));
            },
            nullptr);
        return *this;
    }

    // Enable DELETE support
    WritableVTableBuilder& deletable(std::function<bool(size_t)> delete_fn) {
        def_.supports_delete = true;
        def_.delete_row = std::move(delete_fn);
        return *this;
    }

    WritableVTableDef build() { return std::move(def_); }
};

inline WritableVTableBuilder writable_table(const char* name) {
    return WritableVTableBuilder(name);
}

} // namespace xsql
