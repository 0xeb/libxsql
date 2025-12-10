/**
 * xsql/vtable_builder.hpp - Fluent API for building virtual table definitions
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Example usage:
 *
 *   auto def = xsql::table("my_table")
 *       .count([&]() { return data.size(); })
 *       .column_int64("id", [&](size_t i) { return data[i].id; })
 *       .column_text("name", [&](size_t i) { return data[i].name; })
 *       .build();
 */

#pragma once

#include "vtable.hpp"

namespace xsql {

// ============================================================================
// Table Builder (Fluent API)
// ============================================================================

class VTableBuilder {
    VTableDef def_;
public:
    explicit VTableBuilder(const char* name) {
        def_.name = name;
    }

    VTableBuilder& count(std::function<size_t()> fn) {
        def_.row_count = std::move(fn);
        return *this;
    }

    VTableBuilder& column_int64(const char* name, std::function<int64_t(size_t)> getter) {
        def_.columns.push_back(make_column_int64(name, std::move(getter)));
        return *this;
    }

    VTableBuilder& column_int(const char* name, std::function<int(size_t)> getter) {
        def_.columns.push_back(make_column_int(name, std::move(getter)));
        return *this;
    }

    VTableBuilder& column_text(const char* name, std::function<std::string(size_t)> getter) {
        def_.columns.push_back(make_column_text(name, std::move(getter)));
        return *this;
    }

    VTableBuilder& column_double(const char* name, std::function<double(size_t)> getter) {
        def_.columns.push_back(make_column_double(name, std::move(getter)));
        return *this;
    }

    VTableBuilder& column_blob(const char* name, std::function<std::vector<uint8_t>(size_t)> getter) {
        def_.columns.push_back(make_column_blob(name, std::move(getter)));
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
    xsql::make_column_int64(#name, getter)

#define XSQL_COLUMN_INT(name, getter) \
    xsql::make_column_int(#name, getter)

#define XSQL_COLUMN_TEXT(name, getter) \
    xsql::make_column_text(#name, getter)

#define XSQL_COLUMN_DOUBLE(name, getter) \
    xsql::make_column_double(#name, getter)

#define XSQL_COLUMN_BLOB(name, getter) \
    xsql::make_column_blob(#name, getter)

} // namespace xsql
