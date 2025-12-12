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
 */

#pragma once

#include <sqlite3.h>
#include <string>
#include <vector>
#include <functional>
#include <sstream>
#include <cstring>

namespace xsql {

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
    std::function<void(sqlite3_context*, size_t)> get;

    // Setter: Update value at row index (optional, for UPDATE support)
    std::function<bool(size_t, sqlite3_value*)> set;

    ColumnDef(const char* n, ColumnType t, bool w,
              std::function<void(sqlite3_context*, size_t)> getter,
              std::function<bool(size_t, sqlite3_value*)> setter = nullptr)
        : name(n), type(t), writable(w), get(std::move(getter)), set(std::move(setter)) {}
};

// ============================================================================
// Virtual Table Definition
// ============================================================================

struct VTableDef {
    std::string name;

    // Row count (called fresh each time for live data)
    std::function<size_t()> row_count;

    // Columns
    std::vector<ColumnDef> columns;

    // DELETE handler: Delete row at index, returns success
    std::function<bool(size_t)> delete_row;
    bool supports_delete = false;

    // Hook called before any modification (UPDATE/DELETE)
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
// SQLite Virtual Table Implementation
// ============================================================================

struct Vtab {
    sqlite3_vtab base;
    const VTableDef* def;
};

struct Cursor {
    sqlite3_vtab_cursor base;
    size_t idx;
    size_t total;
    const VTableDef* def;
};

// xConnect/xCreate
inline int vtab_connect(sqlite3* db, void* pAux, int, const char* const*,
                        sqlite3_vtab** ppVtab, char**) {
    const VTableDef* def = static_cast<const VTableDef*>(pAux);

    int rc = sqlite3_declare_vtab(db, def->schema().c_str());
    if (rc != SQLITE_OK) return rc;

    auto* vtab = new Vtab();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

// xDisconnect/xDestroy
inline int vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<Vtab*>(pVtab);
    return SQLITE_OK;
}

// xOpen
inline int vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    auto* cursor = new Cursor();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->idx = 0;
    cursor->total = 0;
    cursor->def = vtab->def;
    *ppCursor = &cursor->base;
    return SQLITE_OK;
}

// xClose
inline int vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<Cursor*>(pCursor);
    return SQLITE_OK;
}

// xNext
inline int vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    cursor->idx++;
    return SQLITE_OK;
}

// xEof
inline int vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    return cursor->idx >= cursor->total;
}

// xColumn - fetches live data each time
inline int vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    cursor->def->columns[col].get(ctx, cursor->idx);
    return SQLITE_OK;
}

// xRowid
inline int vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    *pRowid = static_cast<sqlite3_int64>(cursor->idx);
    return SQLITE_OK;
}

// xFilter - get fresh count for iteration
inline int vtab_filter(sqlite3_vtab_cursor* pCursor, int, const char*, int, sqlite3_value**) {
    auto* cursor = reinterpret_cast<Cursor*>(pCursor);
    cursor->idx = 0;
    cursor->total = cursor->def->row_count();
    return SQLITE_OK;
}

// xBestIndex
inline int vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    size_t count = vtab->def->row_count();
    pInfo->estimatedCost = static_cast<double>(count);
    pInfo->estimatedRows = count;
    return SQLITE_OK;
}

// xUpdate - handles INSERT, UPDATE, DELETE
inline int vtab_update(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite3_int64*) {
    auto* vtab = reinterpret_cast<Vtab*>(pVtab);
    const VTableDef* def = vtab->def;

    // argc == 1: DELETE
    if (argc == 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        if (!def->supports_delete || !def->delete_row) {
            return SQLITE_READONLY;
        }

        size_t rowid = static_cast<size_t>(sqlite3_value_int64(argv[0]));

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

        if (def->before_modify) {
            def->before_modify("UPDATE " + def->name);
        }

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
inline sqlite3_module create_module() {
    sqlite3_module mod = {};
    mod.iVersion = 0;
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

inline bool register_vtable(sqlite3* db, const char* module_name, const VTableDef* def) {
    int rc = sqlite3_create_module_v2(db, module_name, &get_module(),
                                       const_cast<VTableDef*>(def), nullptr);
    return rc == SQLITE_OK;
}

inline bool create_vtable(sqlite3* db, const char* table_name, const char* module_name) {
    std::string sql = "CREATE VIRTUAL TABLE " + std::string(table_name) +
                      " USING " + std::string(module_name) + ";";
    char* err = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (err) sqlite3_free(err);
    return rc == SQLITE_OK;
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

    // Hook called before any modification
    VTableBuilder& on_modify(std::function<void(const std::string&)> fn) {
        def_.before_modify = std::move(fn);
        return *this;
    }

    // Read-only integer column (int64)
    VTableBuilder& column_int64(const char* name, std::function<int64_t(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int64(ctx, getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable integer column (int64)
    VTableBuilder& column_int64_rw(const char* name,
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

    // Read-only integer column (int)
    VTableBuilder& column_int(const char* name, std::function<int(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Integer, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int(ctx, getter(idx));
            },
            nullptr);
        return *this;
    }

    // Writable integer column (int)
    VTableBuilder& column_int_rw(const char* name,
                                  std::function<int(size_t)> getter,
                                  std::function<bool(size_t, int)> setter) {
        def_.columns.emplace_back(name, ColumnType::Integer, true,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_int(ctx, getter(idx));
            },
            [setter = std::move(setter)](size_t idx, sqlite3_value* val) -> bool {
                return setter(idx, sqlite3_value_int(val));
            });
        return *this;
    }

    // Read-only text column
    VTableBuilder& column_text(const char* name, std::function<std::string(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Text, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                std::string val = getter(idx);
                sqlite3_result_text(ctx, val.c_str(), -1, SQLITE_TRANSIENT);
            },
            nullptr);
        return *this;
    }

    // Writable text column
    VTableBuilder& column_text_rw(const char* name,
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

    // Read-only double column
    VTableBuilder& column_double(const char* name, std::function<double(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Real, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                sqlite3_result_double(ctx, getter(idx));
            },
            nullptr);
        return *this;
    }

    // Read-only blob column
    VTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(size_t)> getter) {
        def_.columns.emplace_back(name, ColumnType::Blob, false,
            [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
                auto val = getter(idx);
                sqlite3_result_blob(ctx, val.data(), static_cast<int>(val.size()), SQLITE_TRANSIENT);
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

} // namespace xsql
