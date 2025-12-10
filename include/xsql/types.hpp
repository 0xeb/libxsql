/**
 * xsql/types.hpp - Core types for the xsql virtual table framework
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 */

#pragma once

#include <sqlite3.h>
#include <string>
#include <vector>
#include <functional>

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
    std::function<void(sqlite3_context*, size_t)> bind;  // Bind value at row index

    ColumnDef(const char* n, ColumnType t,
              std::function<void(sqlite3_context*, size_t)> b)
        : name(n), type(t), bind(std::move(b)) {}
};

// Column factory helpers
inline ColumnDef make_column_int64(const char* name,
                                    std::function<int64_t(size_t)> getter) {
    return ColumnDef(name, ColumnType::Integer,
        [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
            sqlite3_result_int64(ctx, getter(idx));
        });
}

inline ColumnDef make_column_int(const char* name,
                                  std::function<int(size_t)> getter) {
    return ColumnDef(name, ColumnType::Integer,
        [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
            sqlite3_result_int(ctx, getter(idx));
        });
}

inline ColumnDef make_column_text(const char* name,
                                   std::function<std::string(size_t)> getter) {
    return ColumnDef(name, ColumnType::Text,
        [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
            std::string val = getter(idx);
            sqlite3_result_text(ctx, val.c_str(), -1, SQLITE_TRANSIENT);
        });
}

inline ColumnDef make_column_double(const char* name,
                                     std::function<double(size_t)> getter) {
    return ColumnDef(name, ColumnType::Real,
        [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
            sqlite3_result_double(ctx, getter(idx));
        });
}

inline ColumnDef make_column_blob(const char* name,
                                   std::function<std::vector<uint8_t>(size_t)> getter) {
    return ColumnDef(name, ColumnType::Blob,
        [getter = std::move(getter)](sqlite3_context* ctx, size_t idx) {
            auto val = getter(idx);
            sqlite3_result_blob(ctx, val.data(), static_cast<int>(val.size()), SQLITE_TRANSIENT);
        });
}

} // namespace xsql
