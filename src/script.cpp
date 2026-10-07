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
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <system_error>
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

namespace {

struct ExportColumn {
    std::string name;
    std::string type;
    bool notnull = false;
    bool pk = false;
    std::string dflt;
};

// SQLite's planner text when every plan a virtual table offered was refused
// (its xBestIndex returned SQLITE_CONSTRAINT for each one).
constexpr const char* kNoQuerySolution = "no query solution";

// True when the last call on `h` failed because the table cannot be read
// without constraints: a libxsql table refuses the scan with SQLITE_CONSTRAINT
// (see return_vtab_refusal), and a table whose xBestIndex rejects every plan
// fails to prepare with "no query solution". A plain SELECT can produce neither
// any other way, so this never swallows a real error.
bool is_constraint_refusal(sqlite3* h, const std::string& message) {
    if (!h) {
        return false;
    }
    const int code = sqlite3_errcode(h) & 0xff;
    return code == SQLITE_CONSTRAINT ||
           (code == SQLITE_ERROR && message == kNoQuerySolution);
}

std::string refusal_error(const std::string& table, const std::string& reason) {
    std::string message = "Table '" + table +
                          "' cannot be exported: it requires constraints (WHERE) to be read";
    if (!reason.empty()) {
        message += " (" + reason + ")";
    }
    return message;
}

void write_row(std::ostream& out, Statement& stmt, const std::string& quoted_table) {
    out << "INSERT INTO " << quoted_table << " VALUES (";
    const int col_count = stmt.column_count();
    for (int i = 0; i < col_count; ++i) {
        switch (stmt.column_type(i)) {
            case SQLITE_NULL:
                out << "NULL";
                break;
            case SQLITE_INTEGER:
                out << stmt.int64_value(i);
                break;
            case SQLITE_FLOAT: {
                std::ostringstream oss;
                oss << std::setprecision(std::numeric_limits<double>::digits10 + 1)
                    << stmt.double_value(i);
                out << oss.str();
                break;
            }
            case SQLITE_BLOB:
                out << blob_to_hex(stmt.blob(i));
                break;
            case SQLITE_TEXT:
            default:
                out << "'" << escape_text(stmt.text(i)) << "'";
                break;
        }
        if (i + 1 < col_count) {
            out << ", ";
        }
    }
    out << ");\n";
}

enum class TableOutcome { exported, empty, refused, failed };

// Write one table's section to `out`. Whether the table can be read is settled
// by the first step of its SELECT, before any of its text is written, so a
// refused table leaves nothing behind.
TableOutcome export_one_table(Database& db, sqlite3* handle, const std::string& table,
                              std::ostream& out, std::string& error) {
    const std::string quoted_table = quote_identifier(table);

    std::vector<ExportColumn> columns;
    auto col_stmt = db.prepare_statement("PRAGMA table_info(" + quoted_table + ");");
    if (!col_stmt.valid()) {
        error = col_stmt.error().empty() ? db.last_error() : col_stmt.error();
        return TableOutcome::failed;
    }
    while (col_stmt.step() == StepResult::row) {
        ExportColumn col;
        col.name = col_stmt.text(1);
        col.type = col_stmt.column_is_null(2) ? std::string() : col_stmt.text(2);
        col.notnull = col_stmt.int_value(3) != 0;
        col.dflt = col_stmt.column_is_null(4) ? std::string() : col_stmt.text(4);
        col.pk = col_stmt.int_value(5) != 0;
        columns.push_back(std::move(col));
    }
    if (!col_stmt.error().empty()) {
        error = col_stmt.error();
        return TableOutcome::failed;
    }
    if (columns.empty()) {
        return TableOutcome::empty;
    }

    auto data_stmt = db.prepare_statement("SELECT * FROM " + quoted_table + ";");
    if (!data_stmt.valid()) {
        error = data_stmt.error().empty() ? db.last_error() : data_stmt.error();
        return is_constraint_refusal(handle, error) ? TableOutcome::refused
                                                                : TableOutcome::failed;
    }
    StepResult step = data_stmt.step();
    if (step != StepResult::row && step != StepResult::done) {
        error = data_stmt.error().empty() ? db.last_error() : data_stmt.error();
        return is_constraint_refusal(handle, error) ? TableOutcome::refused
                                                                : TableOutcome::failed;
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

    size_t row_count = 0;
    while (step == StepResult::row) {
        write_row(out, data_stmt, quoted_table);
        ++row_count;
        step = data_stmt.step();
    }
    if (step != StepResult::done) {
        // A failure after the table was opened is a real error, whatever its code.
        error = data_stmt.error().empty() ? db.last_error() : data_stmt.error();
        return TableOutcome::failed;
    }

    out << "-- " << row_count << " rows exported\n\n";
    return TableOutcome::exported;
}

} // namespace

bool export_tables(Database& db,
                   const std::vector<std::string>& requested_tables,
                   const std::string& output_path,
                   std::string& error) {
    const bool explicit_request = !requested_tables.empty();
    std::vector<std::string> tables = requested_tables;

    if (!explicit_request) {
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

    // The header reports how many tables were exported and which were skipped,
    // which is only known at the end. Table sections stream to a sibling body
    // file (memory stays at one row), then header and body are joined into
    // `output_path`. A failed export leaves `output_path` untouched.
    const std::string body_path = output_path + ".body.tmp";
    std::ofstream body(body_path, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!body.is_open()) {
        error = "Cannot open output file: " + output_path;
        return false;
    }
    auto fail = [&](std::string message) {
        if (body.is_open()) {
            body.close();
        }
        std::remove(body_path.c_str());
        error = std::move(message);
        return false;
    };

    size_t exported = 0;
    std::vector<std::string> skipped;
    for (const auto& table : tables) {
        std::string table_error;
        switch (export_one_table(db, db.sqlite_handle(), table, body, table_error)) {
            case TableOutcome::exported:
                ++exported;
                break;
            case TableOutcome::empty:
                break;
            case TableOutcome::refused:
                if (explicit_request) {
                    return fail(refusal_error(table, table_error));
                }
                skipped.push_back(table);
                break;
            case TableOutcome::failed:
                return fail(table_error);
        }
        if (!body) {
            return fail("Cannot write output file: " + output_path);
        }
    }
    body.close();
    if (body.fail()) {
        return fail("Cannot write output file: " + output_path);
    }

    // Assembled beside the target and renamed over it, so a failure while
    // writing never leaves a partial `output_path`.
    const std::string staged_path = output_path + ".tmp";
    auto fail_staged = [&](std::string message) {
        std::remove(staged_path.c_str());
        return fail(std::move(message));
    };
    std::ofstream out(staged_path, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!out.is_open()) {
        return fail("Cannot open output file: " + output_path);
    }
    out << "-- SQL Export\n";
    out << "-- Tables: " << exported << "\n";
    if (!skipped.empty()) {
        out << "-- Skipped (requires constraints): ";
        for (size_t i = 0; i < skipped.size(); ++i) {
            out << (i ? ", " : "") << skipped[i];
        }
        out << "\n";
    }
    out << "\n";
    {
        std::ifstream in(body_path, std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            out.close();
            return fail_staged("Cannot read temporary export file: " + body_path);
        }
        if (in.peek() != std::ifstream::traits_type::eof()) {
            out << in.rdbuf();
        }
    }
    out.close();
    if (out.fail()) {
        return fail_staged("Cannot write output file: " + output_path);
    }
    std::error_code rename_error;
    std::filesystem::rename(staged_path, output_path, rename_error);
    if (rename_error) {
        return fail_staged("Cannot write output file: " + output_path + ": " +
                           rename_error.message());
    }
    std::remove(body_path.c_str());

    error.clear();
    return true;
}

} // namespace xsql
