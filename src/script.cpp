// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#include <xsql/script.hpp>

#include <xsql/database.hpp>

#include <sqlite3.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>

namespace xsql {

namespace {

std::string trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string escape_text(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 4);
    for (char c : value) {
        if (c == '\'') {
            out += "''";
        } else {
            out += c;
        }
    }
    return out;
}

std::string blob_to_hex(const std::vector<uint8_t>& data) {
    std::ostringstream oss;
    oss << "X'";
    oss << std::hex << std::uppercase << std::setfill('0');
    for (uint8_t byte : data) {
        oss << std::setw(2) << static_cast<int>(byte);
    }
    oss << "'";
    return oss.str();
}

} // namespace

std::string quote_identifier(const std::string& name) {
    std::string out;
    out.reserve(name.size() + 2);
    out.push_back('"');
    for (char c : name) {
        if (c == '"') {
            out.push_back('"');
        }
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

bool collect_statements(const std::string& script,
                        std::vector<std::string>& statements,
                        std::string& error) {
    (void)error;
    std::string current;
    for (char c : script) {
        current.push_back(c);
        if (sqlite3_complete(current.c_str())) {
            auto trimmed = trim_copy(current);
            if (!trimmed.empty()) {
                statements.push_back(std::move(trimmed));
            }
            current.clear();
        }
    }

    auto tail = trim_copy(current);
    if (!tail.empty()) {
        statements.push_back(std::move(tail));
    }
    return true;
}

bool execute_script(Database& db,
                    const std::string& script,
                    std::vector<StatementResult>& results,
                    std::string& error) {
    std::vector<std::string> statements;
    if (!collect_statements(script, statements, error)) {
        return false;
    }

    for (const auto& sql : statements) {
        auto stmt = db.prepare_statement(sql);
        if (!stmt.valid()) {
            error = stmt.error().empty() ? db.last_error() : stmt.error();
            return false;
        }

        StatementResult result;
        const int col_count = stmt.column_count();
        if (col_count > 0) {
            result.columns.reserve(static_cast<size_t>(col_count));
            for (int i = 0; i < col_count; ++i) {
                result.columns.push_back(stmt.column_name(i));
            }
        }

        while (true) {
            StepResult step = stmt.step();
            if (step == StepResult::row) {
                std::vector<std::string> row;
                row.reserve(static_cast<size_t>(col_count));
                for (int i = 0; i < col_count; ++i) {
                    row.push_back(stmt.column_is_null(i) ? "NULL" : stmt.text(i));
                }
                result.rows.push_back(std::move(row));
                continue;
            }
            if (step == StepResult::done) {
                if (col_count > 0) {
                    results.push_back(std::move(result));
                }
                break;
            }
            error = stmt.error().empty() ? db.last_error() : stmt.error();
            return false;
        }
    }

    error.clear();
    return true;
}

bool export_tables(Database& db,
                   const std::vector<std::string>& requested_tables,
                   const std::string& output_path,
                   std::string& error) {
    std::vector<std::string> tables = requested_tables;

    if (tables.empty()) {
        auto names = db.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;");
        if (!names.ok()) {
            error = names.error;
            return false;
        }
        for (const auto& row : names.rows) {
            if (!row.empty()) {
                tables.push_back(row[0]);
            }
        }
    }

    std::ofstream out(output_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        error = "Cannot open output file: " + output_path;
        return false;
    }

    out << "-- SQL Export\n";
    out << "-- Tables: " << tables.size() << "\n\n";

    for (const auto& table : tables) {
        const std::string quoted_table = quote_identifier(table);

        struct Column {
            std::string name;
            std::string type;
            bool notnull = false;
            bool pk = false;
            std::string dflt;
        };
        std::vector<Column> columns;

        auto col_stmt = db.prepare_statement("PRAGMA table_info(" + quoted_table + ");");
        if (!col_stmt.valid()) {
            error = col_stmt.error().empty() ? db.last_error() : col_stmt.error();
            return false;
        }

        while (col_stmt.step() == StepResult::row) {
            Column col;
            col.name = col_stmt.text(1);
            col.type = col_stmt.column_is_null(2) ? std::string() : col_stmt.text(2);
            col.notnull = col_stmt.int_value(3) != 0;
            col.dflt = col_stmt.column_is_null(4) ? std::string() : col_stmt.text(4);
            col.pk = col_stmt.int_value(5) != 0;
            columns.push_back(std::move(col));
        }
        if (!col_stmt.error().empty()) {
            error = col_stmt.error();
            return false;
        }

        if (columns.empty()) {
            continue;
        }

        out << "-- Table: " << table << "\n";
        out << "DROP TABLE IF EXISTS " << quoted_table << ";\n";
        out << "CREATE TABLE " << quoted_table << " (\n";
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& col = columns[i];
            out << "    " << quote_identifier(col.name);
            if (!col.type.empty()) {
                out << " " << col.type;
            }
            if (col.pk) {
                out << " PRIMARY KEY";
            }
            if (col.notnull) {
                out << " NOT NULL";
            }
            if (!col.dflt.empty()) {
                out << " DEFAULT " << col.dflt;
            }
            if (i + 1 < columns.size()) {
                out << ",";
            }
            out << "\n";
        }
        out << ");\n\n";

        auto data_stmt = db.prepare_statement("SELECT * FROM " + quoted_table + ";");
        if (!data_stmt.valid()) {
            error = data_stmt.error().empty() ? db.last_error() : data_stmt.error();
            return false;
        }

        size_t row_count = 0;
        while (true) {
            StepResult step = data_stmt.step();
            if (step == StepResult::done) {
                break;
            }
            if (step != StepResult::row) {
                error = data_stmt.error().empty() ? db.last_error() : data_stmt.error();
                return false;
            }

            out << "INSERT INTO " << quoted_table << " VALUES (";
            const int col_count = data_stmt.column_count();
            for (int i = 0; i < col_count; ++i) {
                switch (data_stmt.column_type(i)) {
                    case SQLITE_NULL:
                        out << "NULL";
                        break;
                    case SQLITE_INTEGER:
                        out << data_stmt.int64_value(i);
                        break;
                    case SQLITE_FLOAT: {
                        std::ostringstream oss;
                        oss << std::setprecision(std::numeric_limits<double>::digits10 + 1)
                            << data_stmt.double_value(i);
                        out << oss.str();
                        break;
                    }
                    case SQLITE_BLOB:
                        out << blob_to_hex(data_stmt.blob(i));
                        break;
                    case SQLITE_TEXT:
                    default:
                        out << "'" << escape_text(data_stmt.text(i)) << "'";
                        break;
                }
                if (i + 1 < col_count) {
                    out << ", ";
                }
            }
            out << ");\n";
            ++row_count;
        }

        out << "-- " << row_count << " rows exported\n\n";
    }

    error.clear();
    return true;
}

} // namespace xsql
