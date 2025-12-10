/**
 * xsql/vtable.hpp - SQLite Virtual Table framework
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * This provides the core virtual table implementation that allows
 * any data source to be exposed as a SQL table.
 */

#pragma once

#include "types.hpp"
#include <sstream>
#include <cstring>

namespace xsql {

// ============================================================================
// Virtual Table Definition
// ============================================================================

struct VTableDef {
    std::string name;
    std::function<size_t()> row_count;
    std::vector<ColumnDef> columns;

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

struct GenericVtab {
    sqlite3_vtab base;
    const VTableDef* def;
};

struct GenericCursor {
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

    auto* vtab = new GenericVtab();
    memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->def = def;
    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

// xDisconnect/xDestroy
inline int vtab_disconnect(sqlite3_vtab* pVtab) {
    delete reinterpret_cast<GenericVtab*>(pVtab);
    return SQLITE_OK;
}

// xOpen
inline int vtab_open(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
    auto* vtab = reinterpret_cast<GenericVtab*>(pVtab);
    auto* cursor = new GenericCursor();
    memset(&cursor->base, 0, sizeof(cursor->base));
    cursor->idx = 0;
    cursor->total = vtab->def->row_count();
    cursor->def = vtab->def;
    *ppCursor = &cursor->base;
    return SQLITE_OK;
}

// xClose
inline int vtab_close(sqlite3_vtab_cursor* pCursor) {
    delete reinterpret_cast<GenericCursor*>(pCursor);
    return SQLITE_OK;
}

// xNext
inline int vtab_next(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<GenericCursor*>(pCursor);
    cursor->idx++;
    return SQLITE_OK;
}

// xEof
inline int vtab_eof(sqlite3_vtab_cursor* pCursor) {
    auto* cursor = reinterpret_cast<GenericCursor*>(pCursor);
    return cursor->idx >= cursor->total;
}

// xColumn
inline int vtab_column(sqlite3_vtab_cursor* pCursor, sqlite3_context* ctx, int col) {
    auto* cursor = reinterpret_cast<GenericCursor*>(pCursor);
    if (col < 0 || static_cast<size_t>(col) >= cursor->def->columns.size()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    cursor->def->columns[col].bind(ctx, cursor->idx);
    return SQLITE_OK;
}

// xRowid
inline int vtab_rowid(sqlite3_vtab_cursor* pCursor, sqlite3_int64* pRowid) {
    auto* cursor = reinterpret_cast<GenericCursor*>(pCursor);
    *pRowid = static_cast<sqlite3_int64>(cursor->idx);
    return SQLITE_OK;
}

// xFilter
inline int vtab_filter(sqlite3_vtab_cursor* pCursor, int, const char*, int, sqlite3_value**) {
    auto* cursor = reinterpret_cast<GenericCursor*>(pCursor);
    cursor->idx = 0;
    cursor->total = cursor->def->row_count();
    return SQLITE_OK;
}

// xBestIndex
inline int vtab_best_index(sqlite3_vtab* pVtab, sqlite3_index_info* pInfo) {
    auto* vtab = reinterpret_cast<GenericVtab*>(pVtab);
    size_t count = vtab->def->row_count();
    pInfo->estimatedCost = static_cast<double>(count);
    pInfo->estimatedRows = count;
    return SQLITE_OK;
}

// Create module definition
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
    return mod;
}

// Global module instance (one is enough since behavior is driven by VTableDef)
inline sqlite3_module& get_module() {
    static sqlite3_module mod = create_module();
    return mod;
}

// ============================================================================
// Registration Helper
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

} // namespace xsql
