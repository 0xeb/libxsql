/**
 * xsql/database.hpp - RAII SQLite database wrapper with query helpers
 */

#pragma once

#include "functions.hpp"
#include "script.hpp"
#include "statement.hpp"
#include "status.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace xsql {

struct VTableDef;
template<typename RowData>
struct CachedTableDef;
template<typename RowData>
struct GeneratorTableDef;

class Database;

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

    const std::string& operator[](size_t i) const { return values[i]; }
    std::string& operator[](size_t i) { return values[i]; }
    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }
};

struct QueryOptions {
    int timeout_ms = 0;
    int progress_steps = 1000;
};

struct Result {
    std::vector<std::string> columns;
    std::vector<Row> rows;
    std::string error;
    std::vector<std::string> warnings;
    bool timed_out = false;
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

    Status register_function(const char* name, int argc, ScalarFn fn);

    Statement prepare_statement(const char* sql);
    Statement prepare_statement(const std::string& sql);
    bool is_readonly_statement(const char* sql);
    bool is_readonly_statement(const std::string& sql);

    Result query(const char* sql);
    Result query(const char* sql, const QueryOptions& options);
    Result query(const std::string& sql);
    Result query(const std::string& sql, const QueryOptions& options);

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

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;

    void* native_handle_unsafe() const;

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
};

} // namespace xsql
