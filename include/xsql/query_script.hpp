// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

/**
 * xsql/query_script.hpp - Canonical multi-statement SQL orchestrator.
 *
 * One uniform "script" envelope across every xsql-family product. Single
 * statement = array of one. See script_result_to_json() for the on-the-wire
 * shape. Callers plug in their own per-statement executor (templated) and
 * receive aggregated results, fail-fast or continue-on-error.
 */

#include <xsql/database.hpp>
#include <xsql/json.hpp>
#include <xsql/script.hpp>

#include <cstddef>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace xsql {

// ----- Options ---------------------------------------------------------------

struct ScriptOptions {
    bool continue_on_error = false;
    bool include_sql       = false;
};

// ----- Per-statement record --------------------------------------------------

struct ScriptStatementResult {
    std::size_t statement_index = 0;
    bool success = false;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
    std::size_t row_count = 0;
    double elapsed_ms = 0.0;
    std::string error;   // empty when success
    std::string sql;     // populated only when ScriptOptions::include_sql

    bool is_null_cell(std::size_t row_index, std::size_t col_index) const {
        if (row_index >= rows.size()) return false;
        if (col_index >= rows[row_index].size()) return false;
        return rows[row_index][col_index] == "NULL";
    }
};

// ----- Aggregate result ------------------------------------------------------

struct ScriptResult {
    bool success = false;
    std::size_t statement_count = 0;
    std::vector<ScriptStatementResult> results;
    std::size_t row_count_total = 0;
    double elapsed_ms_total = 0.0;
    std::optional<std::size_t> first_error_index;
    std::string parse_error;   // empty unless the splitter failed
};

// ----- Orchestrator ----------------------------------------------------------
//
// Executor signature (any of these forms work):
//   void(const std::string& sql, ScriptStatementResult& out)
//   ScriptStatementResult(const std::string& sql)
//
// Required output fields the executor must populate:
//   out.columns, out.rows, out.row_count, out.elapsed_ms, out.success, out.error
//
// run_script sets out.statement_index and (conditionally) out.sql.

namespace detail {

// Adapter: call executor with (sql, out) regardless of whether the executor
// returns a value or writes through the out parameter.
template <class Executor>
auto invoke_executor(Executor& exec, const std::string& sql,
                     ScriptStatementResult& out)
    -> decltype(exec(sql, out), void())
{
    exec(sql, out);
}

template <class Executor>
auto invoke_executor(Executor& exec, const std::string& sql,
                     ScriptStatementResult& out)
    -> decltype(out = exec(sql), void())
{
    out = exec(sql);
}

} // namespace detail

template <class Executor>
inline ScriptResult run_script(const std::string& script,
                               const ScriptOptions& options,
                               Executor exec)
{
    ScriptResult agg;

    std::vector<std::string> statements;
    std::string split_error;
    if (!collect_statements(script, statements, split_error)) {
        agg.parse_error = split_error.empty()
            ? std::string("Failed to parse SQL script")
            : split_error;
        return agg;
    }

    agg.statement_count = statements.size();
    agg.results.reserve(statements.size());

    bool stop = false;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        if (stop) break;

        ScriptStatementResult out;
        out.statement_index = i;

        const std::string& sql = statements[i];
        detail::invoke_executor(exec, sql, out);

        out.row_count = out.rows.size();
        if (options.include_sql) {
            out.sql = sql;
        }

        if (!out.success) {
            if (!agg.first_error_index.has_value()) {
                agg.first_error_index = i;
            }
            if (!options.continue_on_error) {
                stop = true;
            }
        }

        agg.row_count_total  += out.row_count;
        agg.elapsed_ms_total += out.elapsed_ms;
        agg.results.push_back(std::move(out));
    }

    agg.success = !agg.first_error_index.has_value();
    return agg;
}

// ----- JSON formatter --------------------------------------------------------

namespace detail {

inline void append_json_string(std::string& out, const std::string& s) {
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x",
                                  static_cast<unsigned int>(static_cast<unsigned char>(c)));
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

inline void append_json_number(std::string& out, double v) {
    // Drop trailing zeros for whole numbers; integer-cast when v has no
    // fractional part.
    if (v == static_cast<double>(static_cast<long long>(v))) {
        out += std::to_string(static_cast<long long>(v));
    } else {
        std::ostringstream ss;
        ss << v;
        out += ss.str();
    }
}

inline void append_statement_json(std::string& out,
                                  const ScriptStatementResult& r,
                                  bool include_sql)
{
    out.push_back('{');
    out += "\"statement_index\":";
    out += std::to_string(r.statement_index);

    out += ",\"success\":";
    out += r.success ? "true" : "false";

    if (include_sql && !r.sql.empty()) {
        out += ",\"sql\":";
        append_json_string(out, r.sql);
    }

    out += ",\"columns\":[";
    for (std::size_t i = 0; i < r.columns.size(); ++i) {
        if (i) out.push_back(',');
        append_json_string(out, r.columns[i]);
    }
    out.push_back(']');

    out += ",\"rows\":[";
    for (std::size_t i = 0; i < r.rows.size(); ++i) {
        if (i) out.push_back(',');
        out.push_back('[');
        const auto& row = r.rows[i];
        for (std::size_t j = 0; j < row.size(); ++j) {
            if (j) out.push_back(',');
            if (row[j] == "NULL") {
                out += "null";
            } else {
                append_json_string(out, row[j]);
            }
        }
        out.push_back(']');
    }
    out.push_back(']');

    out += ",\"row_count\":";
    out += std::to_string(r.row_count);

    out += ",\"elapsed_ms\":";
    append_json_number(out, r.elapsed_ms);

    out += ",\"error\":";
    if (r.error.empty()) {
        out += "null";
    } else {
        append_json_string(out, r.error);
    }

    out.push_back('}');
}

} // namespace detail

inline std::string script_result_to_json(const ScriptResult& r,
                                         bool include_sql = false)
{
    std::string out;
    out.reserve(256 + r.results.size() * 64);

    if (!r.parse_error.empty()) {
        out += "{\"success\":false";
        out += ",\"statement_count\":0";
        out += ",\"results\":[]";
        out += ",\"row_count_total\":0";
        out += ",\"elapsed_ms_total\":0";
        out += ",\"first_error_index\":null";
        out += ",\"parse_error\":";
        detail::append_json_string(out, r.parse_error);
        out.push_back('}');
        return out;
    }

    out += "{\"success\":";
    out += r.success ? "true" : "false";

    out += ",\"statement_count\":";
    out += std::to_string(r.statement_count);

    out += ",\"results\":[";
    for (std::size_t i = 0; i < r.results.size(); ++i) {
        if (i) out.push_back(',');
        detail::append_statement_json(out, r.results[i], include_sql);
    }
    out.push_back(']');

    out += ",\"row_count_total\":";
    out += std::to_string(r.row_count_total);

    out += ",\"elapsed_ms_total\":";
    detail::append_json_number(out, r.elapsed_ms_total);

    out += ",\"first_error_index\":";
    if (r.first_error_index.has_value()) {
        out += std::to_string(*r.first_error_index);
    } else {
        out += "null";
    }

    out.push_back('}');
    return out;
}

// ----- JSON parser (inverse of script_result_to_json) ------------------------
//
// Rebuilds a ScriptResult from the canonical envelope. Used by HTTP servers that
// already hold a serialized JSON result (e.g. the thinclient's string-based query
// callback / queue) but want to re-render it as text/csv/tsv. NULL cells (JSON
// `null`) are restored to the "NULL" sentinel so the formatters treat them
// consistently. Tolerant of missing/extra fields; on unparseable input returns a
// failed ScriptResult carrying parse_error.

inline ScriptResult json_to_script_result(const std::string& json_str) {
    ScriptResult r;
    json j = json::parse(json_str, nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded() || !j.is_object()) {
        r.success = false;
        r.parse_error = "json_to_script_result: could not parse result JSON";
        return r;
    }
    r.success         = j.value("success", false);
    r.statement_count = j.value("statement_count", static_cast<std::size_t>(0));
    r.row_count_total = j.value("row_count_total", static_cast<std::size_t>(0));
    r.elapsed_ms_total = j.value("elapsed_ms_total", 0.0);
    if (j.contains("parse_error") && j["parse_error"].is_string()) {
        r.parse_error = j["parse_error"].get<std::string>();
    }
    if (j.contains("first_error_index") && j["first_error_index"].is_number_unsigned()) {
        r.first_error_index = j["first_error_index"].get<std::size_t>();
    }
    if (j.contains("results") && j["results"].is_array()) {
        for (const auto& js : j["results"]) {
            if (!js.is_object()) continue;
            ScriptStatementResult s;
            s.statement_index = js.value("statement_index", static_cast<std::size_t>(0));
            s.success    = js.value("success", false);
            s.row_count  = js.value("row_count", static_cast<std::size_t>(0));
            s.elapsed_ms = js.value("elapsed_ms", 0.0);
            if (js.contains("error") && js["error"].is_string()) {
                s.error = js["error"].get<std::string>();
            }
            if (js.contains("sql") && js["sql"].is_string()) {
                s.sql = js["sql"].get<std::string>();
            }
            if (js.contains("columns") && js["columns"].is_array()) {
                for (const auto& c : js["columns"]) {
                    s.columns.push_back(c.is_string() ? c.get<std::string>() : c.dump());
                }
            }
            if (js.contains("rows") && js["rows"].is_array()) {
                for (const auto& jr : js["rows"]) {
                    std::vector<std::string> row;
                    if (jr.is_array()) {
                        for (const auto& cell : jr) {
                            if (cell.is_null())        row.emplace_back("NULL");
                            else if (cell.is_string()) row.push_back(cell.get<std::string>());
                            else                       row.push_back(cell.dump());
                        }
                    }
                    s.rows.push_back(std::move(row));
                }
            }
            r.results.push_back(std::move(s));
        }
    }
    return r;
}

// ----- Text formatter --------------------------------------------------------

namespace detail {

inline std::string render_statement_table(const ScriptStatementResult& r) {
    if (r.columns.empty()) {
        return "(no result)";
    }
    std::vector<std::size_t> widths(r.columns.size(), 0);
    for (std::size_t i = 0; i < r.columns.size(); ++i) {
        widths[i] = r.columns[i].size();
    }
    for (const auto& row : r.rows) {
        for (std::size_t i = 0; i < row.size() && i < widths.size(); ++i) {
            widths[i] = std::max(widths[i], row[i].size());
        }
    }

    std::ostringstream out;
    auto write_row = [&](const std::vector<std::string>& cells) {
        for (std::size_t i = 0; i < cells.size(); ++i) {
            if (i) out << "  ";
            out << cells[i];
            if (i + 1 < cells.size() && cells[i].size() < widths[i]) {
                out << std::string(widths[i] - cells[i].size(), ' ');
            }
        }
        out << '\n';
    };

    write_row(r.columns);
    std::vector<std::string> sep;
    sep.reserve(widths.size());
    for (auto w : widths) sep.emplace_back(w, '-');
    write_row(sep);
    for (const auto& row : r.rows) {
        write_row(row);
    }
    return out.str();
}

} // namespace detail

inline std::string script_result_to_text(const ScriptResult& r) {
    if (!r.parse_error.empty()) {
        return "PARSE ERROR: " + r.parse_error;
    }

    std::ostringstream out;
    for (std::size_t i = 0; i < r.results.size(); ++i) {
        const auto& s = r.results[i];
        out << "-- statement " << (s.statement_index + 1)
            << '/' << r.statement_count
            << " (" << s.elapsed_ms << " ms, "
            << s.row_count << " row" << (s.row_count == 1 ? "" : "s") << ")\n";
        if (s.success) {
            out << detail::render_statement_table(s);
        } else {
            out << "ERROR: " << s.error << "\n";
        }
        if (i + 1 < r.results.size()) {
            out << '\n';
        }
    }
    return out.str();
}

// ----- CSV / TSV formatters --------------------------------------------------
//
// Delimited renderers for direct terminal / unix-pipe consumption. JSON remains
// the canonical machine format (see script_result_to_json); these are for humans
// and shell tools. NULL sentinels render as empty fields. Single-statement output
// is a pristine table (header row + data rows); multi-statement output prefixes
// each table with a `# statement i/N` comment line and a blank separator.

namespace detail {

inline void append_delimited_field(std::string& out, const std::string& v, bool csv) {
    if (v == "NULL") return;  // NULL -> empty field (matches JSON null semantics)
    if (csv) {
        // RFC 4180: quote when the field contains delimiter, quote, or newline.
        if (v.find_first_of(",\"\n\r") == std::string::npos) {
            out += v;
            return;
        }
        out.push_back('"');
        for (char c : v) {
            if (c == '"') out += "\"\"";
            else out.push_back(c);
        }
        out.push_back('"');
    } else {
        // TSV has no quoting; neutralize embedded tab/newline to keep one record
        // per line.
        for (char c : v) {
            out.push_back((c == '\t' || c == '\n' || c == '\r') ? ' ' : c);
        }
    }
}

inline std::string render_statement_delimited(const ScriptStatementResult& r,
                                              char delim, bool csv) {
    std::string out;
    auto write_row = [&](const std::vector<std::string>& cells) {
        for (std::size_t i = 0; i < cells.size(); ++i) {
            if (i) out.push_back(delim);
            append_delimited_field(out, cells[i], csv);
        }
        out.push_back('\n');
    };
    write_row(r.columns);
    for (const auto& row : r.rows) write_row(row);
    return out;
}

inline std::string script_result_to_delimited(const ScriptResult& r,
                                               char delim, bool csv) {
    if (!r.parse_error.empty()) {
        return "# PARSE ERROR: " + r.parse_error + "\n";
    }
    std::string out;
    const bool multi = r.results.size() > 1;
    for (std::size_t i = 0; i < r.results.size(); ++i) {
        const auto& s = r.results[i];
        if (multi) {
            out += "# statement " + std::to_string(s.statement_index + 1) + "/" +
                   std::to_string(r.statement_count) + "\n";
        }
        if (s.success) {
            out += render_statement_delimited(s, delim, csv);
        } else {
            out += "# ERROR: " + s.error + "\n";
        }
        if (multi && i + 1 < r.results.size()) out.push_back('\n');
    }
    return out;
}

} // namespace detail

inline std::string script_result_to_csv(const ScriptResult& r) {
    return detail::script_result_to_delimited(r, ',', /*csv=*/true);
}

inline std::string script_result_to_tsv(const ScriptResult& r) {
    return detail::script_result_to_delimited(r, '\t', /*csv=*/false);
}

// ----- Convenience: drive run_script directly against an xsql::Database ------
//
// Default executor adapter for products that use xsql::Database. Wraps each
// statement in a single-statement query; populates timing if the Database
// reports it.

inline ScriptResult run_database_script(Database& db,
                                        const std::string& script,
                                        const ScriptOptions& options)
{
    return run_script(script, options,
        [&db](const std::string& sql, ScriptStatementResult& out) {
            const Result r = db.query(sql);
            out.columns = r.columns;
            out.rows.reserve(r.rows.size());
            for (const auto& row : r.rows) {
                out.rows.push_back(row.values);
            }
            out.elapsed_ms = static_cast<double>(r.elapsed_ms);
            out.error = r.error;
            out.success = r.error.empty();
        });
}

} // namespace xsql
