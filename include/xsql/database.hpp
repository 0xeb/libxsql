/**
 * xsql/database.hpp - RAII SQLite database wrapper with query helpers
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Example usage:
 *
 *   xsql::Database db;
 *   if (!db.open(":memory:")) {
 *       fprintf(stderr, "Error: %s\n", db.last_error().c_str());
 *       return 1;
 *   }
 *
 *   db.register_table(my_table_def);
 *
 *   auto result = db.query("SELECT * FROM my_table WHERE id > 10");
 *   if (!result.ok()) {
 *       fprintf(stderr, "Query error: %s\n", result.error.c_str());
 *       return 1;
 *   }
 *
 *   for (const auto& row : result.rows) {
 *       printf("%s\n", row[0].c_str());
 *   }
 */

#pragma once

#include "vtable.hpp"
#include "functions.hpp"
#include <memory>
#include <utility>

namespace xsql {

// ============================================================================
// Query Result Types
// ============================================================================

struct Row {
    std::vector<std::string> values;

    const std::string& operator[](size_t i) const { return values[i]; }
    std::string& operator[](size_t i) { return values[i]; }
    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }
};

struct Result {
    std::vector<std::string> columns;
    std::vector<Row> rows;
    std::string error;

    bool ok() const { return error.empty(); }
    size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }

    const Row& operator[](size_t i) const { return rows[i]; }

    // Iterator support
    auto begin() { return rows.begin(); }
    auto end() { return rows.end(); }
    auto begin() const { return rows.begin(); }
    auto end() const { return rows.end(); }
};

// ============================================================================
// Database Wrapper
// ============================================================================

class Database {
public:
    Database() { open(":memory:"); }

    /**
     * Constructor with explicit path
     */
    explicit Database(const char* path) { open(path); }
    ~Database() { close(); }

    // Non-copyable
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Movable
    Database(Database&& other) noexcept
        : db_(other.db_), last_error_(std::move(other.last_error_)) {
        other.db_ = nullptr;
    }

    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            last_error_ = std::move(other.last_error_);
            other.db_ = nullptr;
        }
        return *this;
    }

    // ========================================================================
    // Open/Close
    // ========================================================================

    bool open(const char* path = ":memory:") {
        close();
        int rc = sqlite3_open(path, &db_);
        if (rc != SQLITE_OK) {
            last_error_ = db_ ? sqlite3_errmsg(db_) : "Failed to allocate database";
            if (db_) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            return false;
        }
        last_error_.clear();
        return true;
    }

    void close() {
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    bool is_open() const { return db_ != nullptr; }

    // ========================================================================
    // Table Registration
    // ========================================================================

    bool register_table(const VTableDef& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_vtable(db_, def.name.c_str(), &def);
    }

    bool register_table(const char* module_name, const VTableDef* def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_vtable(db_, module_name, def);
    }

    bool create_table(const char* table_name, const char* module_name) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return xsql::create_vtable(db_, table_name, module_name);
    }

    bool register_and_create_table(const VTableDef& def) {
        return register_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    /**
     * Register and create a virtual table with different table name
     */
    bool register_and_create_table(const VTableDef& def, const char* table_name) {
        return register_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    /**
     * Register and create multiple virtual tables at once
     */
    template<typename... Defs>
    bool register_and_create_tables(Defs&... defs) {
        return (register_and_create_table(defs) && ...);
    }

    // ========================================================================
    // Table Registration - Cached Virtual Tables
    // ========================================================================

    template<typename RowData>
    bool register_cached_table(const CachedTableDef<RowData>& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_cached_vtable(db_, def.name.c_str(), &def);
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

    // ========================================================================
    // Table Registration - Generator Virtual Tables
    // ========================================================================

    template<typename RowData>
    bool register_generator_table(const GeneratorTableDef<RowData>& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_generator_vtable(db_, def.name.c_str(), &def);
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

    // ========================================================================
    // Function Registration
    // ========================================================================

    int register_function(const char* name, int argc, SqlScalarFn fn) {
        if (!db_) {
            last_error_ = "Database not open";
            return SQLITE_ERROR;
        }
        return register_scalar_function(db_, name, argc, std::move(fn));
    }

    // ========================================================================
    // Query Execution
    // ========================================================================

    Result query(const char* sql) {
        Result result;

        if (!db_) {
            result.error = "Database not open";
            return result;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            result.error = sqlite3_errmsg(db_);
            return result;
        }

        // Get column names
        int col_count = sqlite3_column_count(stmt);
        result.columns.reserve(col_count);
        for (int i = 0; i < col_count; ++i) {
            const char* name = sqlite3_column_name(stmt, i);
            result.columns.push_back(name ? name : "");
        }

        // Fetch rows
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            Row row;
            row.values.reserve(col_count);
            for (int i = 0; i < col_count; ++i) {
                const char* text = reinterpret_cast<const char*>(
                    sqlite3_column_text(stmt, i));
                row.values.push_back(text ? text : "");
            }
            result.rows.push_back(std::move(row));
        }

        if (rc != SQLITE_DONE) {
            result.error = sqlite3_errmsg(db_);
        }

        sqlite3_finalize(stmt);
        return result;
    }

    Result query(const std::string& sql) {
        return query(sql.c_str());
    }

    /**
     * Get single value (first column of first row)
     */
    std::string scalar(const char* sql) {
        auto result = query(sql);
        if (result.ok() && !result.empty()) {
            return result[0][0];
        }
        return "";
    }

    std::string scalar(const std::string& sql) {
        return scalar(sql.c_str());
    }

    int exec(const char* sql) {
        if (!db_) {
            last_error_ = "Database not open";
            return SQLITE_ERROR;
        }

        char* err = nullptr;
        int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err);
        if (err) {
            last_error_ = err;
            sqlite3_free(err);
        } else if (rc != SQLITE_OK) {
            last_error_ = sqlite3_errmsg(db_);
        } else {
            last_error_.clear();
        }
        return rc;
    }

    int exec(const std::string& sql) {
        return exec(sql.c_str());
    }

    // ========================================================================
    // Direct Access
    // ========================================================================

    sqlite3* handle() const { return db_; }
    const std::string& last_error() const { return last_error_; }

    // ========================================================================
    // Utility
    // ========================================================================

    int64_t last_insert_rowid() const {
        return db_ ? sqlite3_last_insert_rowid(db_) : 0;
    }

    int changes() const {
        return db_ ? sqlite3_changes(db_) : 0;
    }

private:
    sqlite3* db_ = nullptr;
    std::string last_error_;
};

} // namespace xsql
