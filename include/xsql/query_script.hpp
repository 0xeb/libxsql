// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

/**
 * xsql/query_script.hpp - Canonical multi-statement SQL orchestrator.
 *
 * One uniform "script" envelope across every xsql-family product. Single
 * statement = array of one. See script_result_to_json() for the on-the-wire
 * shape. Callers plug in their own per-statement executor (templated) and
 * receive aggregated results, fail-fast or continue-on-error.
 */

#include <xsql/cli/table_printer.hpp>
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
    // Per-cell SQL-NULL flags, parallel to `rows` (nonzero => the cell was SQL
    // NULL; its `rows` entry is then an empty/placeholder string). A row with no
    // corresponding `cell_null` entry — or a shorter inner vector — falls back to
    // the legacy `"NULL"`-sentinel interpretation, so older executors that only
    // fill `rows` keep working. When populated, a genuine text value equal to
    // "NULL" (mask 0) is rendered distinctly from a real SQL NULL (mask 1).
    std::vector<std::vector<char>> cell_null;
    std::size_t row_count = 0;
    double elapsed_ms = 0.0;
    std::string error;   // empty when success
    std::string sql;     // populated only when ScriptOptions::include_sql

    // Timeout / partial-result signalling. A statement that hit the query deadline
    // may still `success` with a truncated row set; without these an agent reading
    // the envelope cannot tell a partial answer from a complete one. `warnings`
    // carries any non-fatal notices. Executors that don't populate them leave the
    // defaults, so the emitted JSON simply omits the fields (back-compatible).
    bool timed_out = false;
    bool partial = false;
    std::vector<std::string> warnings;

    bool is_null_cell(std::size_t row_index, std::size_t col_index) const {
        if (row_index >= rows.size()) return false;
        if (col_index >= rows[row_index].size()) return false;
        if (row_index < cell_null.size() && col_index < cell_null[row_index].size())
            return cell_null[row_index][col_index] != 0;
        return rows[row_index][col_index] == "NULL";   // legacy fallback
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

inline bool json_is_utf8_continuation(unsigned char ch) {
    return (ch & 0xC0U) == 0x80U;
}

// Validate the UTF-8 sequence starting at `pos`. On success sets `seq_len` to the
// byte length (1..4) and returns true; on any malformed/overlong/surrogate/
// truncated sequence returns false. Rejects overlong forms and UTF-16 surrogates
// (0xED 0xA0..0xBF) so the emitted JSON is always well-formed UTF-8.
inline bool json_is_valid_utf8_sequence(const std::string& input, size_t pos, size_t& seq_len) {
    seq_len = 0;
    if (pos >= input.size()) return false;
    const unsigned char c0 = static_cast<unsigned char>(input[pos]);
    if (c0 < 0x80U) { seq_len = 1; return true; }

    if (c0 >= 0xC2U && c0 <= 0xDFU) {
        if (pos + 1 >= input.size()) return false;
        if (!json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 1]))) return false;
        seq_len = 2;
        return true;
    }
    if (c0 == 0xE0U) {
        if (pos + 2 >= input.size()) return false;
        const unsigned char c1 = static_cast<unsigned char>(input[pos + 1]);
        const unsigned char c2 = static_cast<unsigned char>(input[pos + 2]);
        if (c1 < 0xA0U || c1 > 0xBFU || !json_is_utf8_continuation(c2)) return false;
        seq_len = 3;
        return true;
    }
    if ((c0 >= 0xE1U && c0 <= 0xECU) || (c0 >= 0xEEU && c0 <= 0xEFU)) {
        if (pos + 2 >= input.size()) return false;
        if (!json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 1]))
            || !json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 2]))) return false;
        seq_len = 3;
        return true;
    }
    if (c0 == 0xEDU) {
        if (pos + 2 >= input.size()) return false;
        const unsigned char c1 = static_cast<unsigned char>(input[pos + 1]);
        const unsigned char c2 = static_cast<unsigned char>(input[pos + 2]);
        if (c1 < 0x80U || c1 > 0x9FU || !json_is_utf8_continuation(c2)) return false;
        seq_len = 3;
        return true;
    }
    if (c0 == 0xF0U) {
        if (pos + 3 >= input.size()) return false;
        const unsigned char c1 = static_cast<unsigned char>(input[pos + 1]);
        const unsigned char c2 = static_cast<unsigned char>(input[pos + 2]);
        const unsigned char c3 = static_cast<unsigned char>(input[pos + 3]);
        if (c1 < 0x90U || c1 > 0xBFU || !json_is_utf8_continuation(c2)
            || !json_is_utf8_continuation(c3)) return false;
        seq_len = 4;
        return true;
    }
    if (c0 >= 0xF1U && c0 <= 0xF3U) {
        if (pos + 3 >= input.size()) return false;
        if (!json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 1]))
            || !json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 2]))
            || !json_is_utf8_continuation(static_cast<unsigned char>(input[pos + 3]))) return false;
        seq_len = 4;
        return true;
    }
    if (c0 == 0xF4U) {
        if (pos + 3 >= input.size()) return false;
        const unsigned char c1 = static_cast<unsigned char>(input[pos + 1]);
        const unsigned char c2 = static_cast<unsigned char>(input[pos + 2]);
        const unsigned char c3 = static_cast<unsigned char>(input[pos + 3]);
        if (c1 < 0x80U || c1 > 0x8FU || !json_is_utf8_continuation(c2)
            || !json_is_utf8_continuation(c3)) return false;
        seq_len = 4;
        return true;
    }
    return false;
}

// Emit a JSON string. Control chars are \u-escaped; valid multi-byte UTF-8 is
// passed through verbatim; any invalid/non-UTF-8 byte is escaped as \u00XX so the
// result is always well-formed JSON (a non-UTF-8 cell -- binary strings, Latin-1
// comments, netnode blobs -- can no longer produce unparseable output).
inline void append_json_string(std::string& out, const std::string& s) {
    out.push_back('"');
    for (size_t i = 0; i < s.size();) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        switch (c) {
            case '"':  out += "\\\""; ++i; break;
            case '\\': out += "\\\\"; ++i; break;
            case '\b': out += "\\b";  ++i; break;
            case '\f': out += "\\f";  ++i; break;
            case '\n': out += "\\n";  ++i; break;
            case '\r': out += "\\r";  ++i; break;
            case '\t': out += "\\t";  ++i; break;
            default:
                if (c < 0x20U) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                    ++i;
                } else if (c < 0x80U) {
                    out.push_back(static_cast<char>(c));
                    ++i;
                } else {
                    size_t seq_len = 0;
                    if (json_is_valid_utf8_sequence(s, i, seq_len)) {
                        out.append(s.data() + i, seq_len);
                        i += seq_len;
                    } else {
                        char buf[8];
                        std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                        out += buf;
                        ++i;
                    }
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
            // Per-cell nullness via is_null_cell so a jagged/short cell_null row
            // still falls back to the "NULL" sentinel exactly like the text path.
            if (r.is_null_cell(i, j)) {
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

    // Only emit the timeout/partial/warning fields when set, so executors that
    // never populate them keep the exact prior envelope shape.
    if (r.timed_out) {
        out += ",\"timed_out\":true";
    }
    if (r.partial) {
        out += ",\"partial\":true";
    }
    if (!r.warnings.empty()) {
        out += ",\"warnings\":[";
        for (std::size_t i = 0; i < r.warnings.size(); ++i) {
            if (i) out.push_back(',');
            append_json_string(out, r.warnings[i]);
        }
        out.push_back(']');
    }

    out.push_back('}');
}

} // namespace detail

inline std::string script_result_to_json(const ScriptResult& r,
                                         bool include_sql = false)
{
    std::string out;
    // Cell-aware size hint: ~24 bytes per rendered cell (quotes, escapes,
    // separators) + fixed per-statement envelope. Growth still works if a
    // result under-fits; this only trims re-allocations on large row sets.
    std::size_t est = 256;
    for (const auto& s : r.results) {
        est += 160 + s.sql.size() + s.columns.size() * 24 +
               s.rows.size() * (8 + s.columns.size() * 24);
    }
    out.reserve(est);

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
// `null`) are recorded in the per-cell `cell_null` mask (distinct from a genuine
// text value "NULL") so the formatters render them correctly. Tolerant of
// missing/extra fields; on unparseable input returns a failed ScriptResult
// carrying parse_error.

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
    // A request-level failure envelope carries a top-level string "error" and
    // (typically) no "results" and no "parse_error" — e.g. {"success":false,
    // "error":"..."}. Without this, such an envelope round-tripped through the
    // ?format=text|csv|tsv path yields an empty ScriptResult -> empty body.
    // Surface it as parse_error so the formatters emit the message.
    if (r.parse_error.empty() && !r.success &&
        j.contains("error") && j["error"].is_string()) {
        r.parse_error = j["error"].get<std::string>();
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
            // Round-trip the timeout/partial/warning signalling emitted by
            // append_statement_json; without these the ?format=text|csv|tsv path
            // (which reparses via this function) would silently drop them and a
            // consumer could not tell a partial answer from a complete one.
            s.timed_out = js.value("timed_out", false);
            s.partial   = js.value("partial", false);
            if (js.contains("warnings") && js["warnings"].is_array()) {
                for (const auto& w : js["warnings"]) {
                    s.warnings.push_back(w.is_string() ? w.get<std::string>() : w.dump());
                }
            }
            if (js.contains("columns") && js["columns"].is_array()) {
                for (const auto& c : js["columns"]) {
                    s.columns.push_back(c.is_string() ? c.get<std::string>() : c.dump());
                }
            }
            if (js.contains("rows") && js["rows"].is_array()) {
                for (const auto& jr : js["rows"]) {
                    std::vector<std::string> row;
                    std::vector<char> rnull;
                    if (jr.is_array()) {
                        for (const auto& cell : jr) {
                            // JSON null -> real SQL NULL (mask 1); a JSON *string*
                            // "NULL" stays an ordinary value (mask 0), so the two
                            // are no longer conflated on the round trip.
                            if (cell.is_null())        { row.emplace_back("");                 rnull.push_back(1); }
                            else if (cell.is_string()) { row.push_back(cell.get<std::string>()); rnull.push_back(0); }
                            else                       { row.push_back(cell.dump());             rnull.push_back(0); }
                        }
                    }
                    s.rows.push_back(std::move(row));
                    s.cell_null.push_back(std::move(rnull));
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
    // Human-readable NULL: a real SQL NULL is shown as the text "NULL" (as before);
    // a genuine text value "NULL" is shown as itself — indistinguishable in this
    // human table (acceptable), but json/csv/tsv keep them distinct via cell_null.
    auto display = [&](std::size_t ri, std::size_t ci) -> std::string {
        return r.is_null_cell(ri, ci) ? std::string("NULL") : r.rows[ri][ci];
    };

    std::vector<std::vector<std::string>> rows;
    rows.reserve(r.rows.size());
    for (std::size_t ri = 0; ri < r.rows.size(); ++ri) {
        std::vector<std::string> row;
        row.reserve(r.rows[ri].size());
        for (std::size_t ci = 0; ci < r.rows[ri].size(); ++ci) {
            row.push_back(display(ri, ci));
        }
        rows.push_back(std::move(row));
    }
    return xsql::cli::print_table(r.columns, rows);
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
// and shell tools. A real SQL NULL renders as an empty field. Single-statement
// output is a pristine table (header row + data rows); multi-statement output
// prefixes each table with a `# statement i/N` comment line and a blank separator.
//
// NULL fidelity (fixed): SQL NULL is tracked by the per-cell `cell_null` mask
// (see ScriptStatementResult), threaded from the executor / Database::query null
// bitmap through the JSON round trip and into these formatters. A genuine text
// value equal to "NULL" is therefore rendered distinctly from a real SQL NULL on
// *every* format — csv/tsv emit the literal "NULL" as a normal field (empty only
// for a true NULL), and script_result_to_json emits the string "NULL" quoted vs
// JSON `null` for a true NULL. Executors that leave `cell_null` empty fall back to
// the legacy `"NULL"`-sentinel behavior (see is_null_cell), preserving old output.

namespace detail {

inline void append_delimited_field(std::string& out, const std::string& v, bool csv,
                                   bool is_null) {
    // A real SQL NULL -> empty field (matches JSON null semantics). A genuine text
    // value (including the literal "NULL") is rendered as itself.
    if (is_null) return;
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
    // A column-less statement (e.g. a successful write) has no header/data row.
    // Mirror the text renderer's "(no result)" instead of emitting a bare
    // newline, which reads as a phantom empty record in csv/tsv.
    if (r.columns.empty()) {
        return "(no result)\n";
    }
    std::string out;
    // Column headers are never SQL NULL (pass a null-vector of all-false).
    for (std::size_t i = 0; i < r.columns.size(); ++i) {
        if (i) out.push_back(delim);
        append_delimited_field(out, r.columns[i], csv, /*is_null=*/false);
    }
    out.push_back('\n');
    for (std::size_t ri = 0; ri < r.rows.size(); ++ri) {
        const auto& row = r.rows[ri];
        for (std::size_t i = 0; i < row.size(); ++i) {
            if (i) out.push_back(delim);
            // Per-cell nullness via is_null_cell so a jagged/short cell_null row
            // still falls back to the "NULL" sentinel exactly like the text path.
            append_delimited_field(out, row[i], csv, r.is_null_cell(ri, i));
        }
        out.push_back('\n');
    }
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
            Result r = db.query(sql);
            out.columns = std::move(r.columns);
            out.rows.reserve(r.rows.size());
            out.cell_null.reserve(r.rows.size());
            for (auto& row : r.rows) {
                // The Result is dead after this loop; move each row's cells
                // instead of re-materializing every cell string.
                out.rows.push_back(std::move(row.values));
                out.cell_null.push_back(std::move(row.nulls));  // parallel SQL-NULL mask
            }
            out.elapsed_ms = static_cast<double>(r.elapsed_ms);
            out.success = r.error.empty();   // before moving r.error
            out.error = std::move(r.error);
        });
}

} // namespace xsql
