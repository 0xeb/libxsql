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
#include <xsql/interruption.hpp>
#include <xsql/json.hpp>
#include <xsql/script.hpp>

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace xsql {

// ----- Options ---------------------------------------------------------------

struct ScriptOptions {
    bool continue_on_error = false;
    bool include_sql       = false;
    // Per-statement wall-clock timeout in milliseconds (0 = no limit). Threaded
    // into each statement's execution: the buffered path passes it as
    // QueryOptions::timeout_ms to Database::query (reusing its progress-handler +
    // interrupt-checker + partial-result machinery); the streaming path enforces
    // the same deadline between emitted rows. A read-only, result-bearing
    // statement that hits the deadline comes back success=true with partial=true,
    // timed_out=true, and a warning — but only when at least one row was already
    // gathered (an empty "partial" would read as a valid truncation, so a zero-row
    // deadline is an error). An interrupted mutation (including DML with
    // RETURNING) is aborted through sqlite3_interrupt so SQLite rolls the
    // statement back, and is an error; a mutation that completes before the abort
    // lands has committed and reports its honest result.
    int timeout_ms = 0;
    // Optional cooperative cancellation predicate. When set and it returns true, an
    // in-flight statement stops ASAP. Read-only result statements return partial
    // rows with a "query cancelled" warning (same shape as a timeout); mutations
    // return an error (see timeout_ms above for the rollback and zero-row
    // semantics, which apply identically here). Threaded into the
    // buffered path (Database::query's interrupt checker) and polled between rows in
    // the streaming path, so an HTTP server can abort a runaway query through
    // POST /cancel even under timeout_ms==0. A streaming HTTP executor can also
    // stop on client disconnect through its output sink; a buffered executor cannot
    // observe a disconnect until it tries to write the completed response. Null =>
    // never cancelled.
    std::function<bool()> should_cancel;
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

struct CooperativeInterruptState {
    std::chrono::steady_clock::time_point started_at{};
    int timeout_ms = 0;
    const std::function<bool()>* cancel = nullptr;
    bool timed_out = false;
    bool cancelled = false;
    std::string cancel_error;

    bool enabled() const {
        return timeout_ms > 0 || (cancel && *cancel);
    }

    bool poll() noexcept {
        if (!cancel_error.empty() || cancelled || timed_out) {
            return true;
        }
        if (cancel && *cancel) {
            try {
                cancelled = (*cancel)();
            } catch (const std::exception& e) {
                cancel_error =
                    std::string("cancellation predicate threw: ") + e.what();
                return true;
            } catch (...) {
                cancel_error =
                    "cancellation predicate threw a non-standard exception";
                return true;
            }
            if (cancelled) {
                return true;
            }
        }
        if (timeout_ms > 0 &&
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started_at)
                    .count() >= timeout_ms) {
            timed_out = true;
            return true;
        }
        return false;
    }
};

// Wrap a cancellation predicate so a single `true` observed by ANY copy is
// permanent for ALL copies (they share one latch). Database::query consumes the
// predicate mid-statement through its own polling; without the shared latch a
// one-shot predicate would be "used up" inside the executor and run_script's
// between-statement poll would miss the cancellation and keep executing the rest
// of the script. Null stays null (never cancelled). Predicate exceptions
// propagate — CooperativeInterruptState::poll() converts them to a sticky error.
inline std::function<bool()> make_sticky_cancel(
    const std::function<bool()>& inner) {
    if (!inner) {
        return {};
    }
    auto latched = std::make_shared<std::atomic<bool>>(false);
    return [latched, inner]() {
        if (latched->load(std::memory_order_relaxed)) {
            return true;
        }
        if (!inner()) {
            return false;
        }
        latched->store(true, std::memory_order_relaxed);
        return true;
    };
}

class StreamingInterruptGuard {
public:
    StreamingInterruptGuard(sqlite3* db, CooperativeInterruptState& state)
        : db_(db), state_(&state), active_(db && state.enabled()) {
        if (!active_) {
            return;
        }
        try {
            progress_.emplace(db_, 1000, &StreamingInterruptGuard::callback,
                              state_);
            checker_.emplace(
                [state = state_]() { return state->poll(); });
        } catch (...) {
            checker_.reset();
            progress_.reset();
            active_ = false;
            throw;
        }
    }

    StreamingInterruptGuard(const StreamingInterruptGuard&) = delete;
    StreamingInterruptGuard& operator=(const StreamingInterruptGuard&) = delete;

    ~StreamingInterruptGuard() = default;

private:
    static int callback(void* user_data) noexcept {
        auto* state = static_cast<CooperativeInterruptState*>(user_data);
        return state && state->poll() ? 1 : 0;
    }

    sqlite3* db_ = nullptr;
    CooperativeInterruptState* state_ = nullptr;
    bool active_ = false;
    std::optional<ScopedProgressHandler> progress_;
    std::optional<ScopedInterruptChecker> checker_;
};

inline std::vector<std::string> unique_json_object_keys(
    const std::vector<std::string>& columns) {
    std::unordered_set<std::string> used;
    std::vector<std::string> keys;
    keys.reserve(columns.size());
    for (const auto& column : columns) {
        std::string candidate = column;
        std::size_t suffix = 2;
        while (used.find(candidate) != used.end()) {
            candidate = column + "#" + std::to_string(suffix++);
        }
        used.insert(candidate);
        keys.push_back(std::move(candidate));
    }
    return keys;
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

    detail::CooperativeInterruptState cancellation;
    cancellation.cancel =
        options.should_cancel ? &options.should_cancel : nullptr;

    bool stop = false;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        if (stop) break;
        // Cooperative cancellation between statements (e.g. an HTTP POST /cancel):
        // don't start a new statement once cancel is requested. Mid-statement
        // cancellation is the executor's job (Database::query honors should_cancel).
        if (cancellation.poll()) {
            ScriptStatementResult out;
            out.statement_index = i;
            out.success = false;
            out.error = cancellation.cancel_error.empty()
                ? std::string("query cancelled")
                : cancellation.cancel_error;
            if (options.include_sql) {
                out.sql = statements[i];
            }
            if (!agg.first_error_index.has_value()) {
                agg.first_error_index = i;
            }
            agg.results.push_back(std::move(out));
            break;
        }

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
        // Surface the timeout / partial-result signalling that the data model
        // carries — emitted ONLY when set, so a normal (no-timeout) result is
        // byte-identical to before. `--`-prefixed to match the `-- statement`
        // convention above so it never collides with a data row.
        for (const auto& w : s.warnings) {
            out << "-- warning: " << w << "\n";
        }
        if (s.timed_out) {
            out << "-- query timed out; results are partial\n";
        } else if (s.partial) {
            out << "-- results are partial\n";
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

// JSON Lines / NDJSON: one self-describing JSON object per row, one per line —
// keyed by column name (the "records" shape a bulk consumer wants, e.g. a symbol
// cache). Cell values are emitted as JSON strings (SQL NULL => null), matching the
// canonical envelope's cell treatment (script_result_to_json) so a numeric column
// like `rva` is "4096" exactly as it is there. A failed/parse-error statement emits
// one `{"statement_index":I,"error":"…"}` line instead of rows; a column-less
// statement (a successful write) emits nothing. No enclosing array/envelope, so the
// output streams and appends cleanly.
inline std::string script_result_to_jsonl(const ScriptResult& r) {
    std::string out;
    if (!r.parse_error.empty()) {
        out += "{\"error\":";
        detail::append_json_string(out, r.parse_error);
        out += "}\n";
        return out;
    }
    for (const auto& s : r.results) {
        if (!s.success) {
            out += "{\"statement_index\":";
            out += std::to_string(s.statement_index);
            out += ",\"error\":";
            detail::append_json_string(out, s.error);
            out += "}\n";
            continue;
        }
        const auto keys = detail::unique_json_object_keys(s.columns);
        for (std::size_t ri = 0; ri < s.rows.size(); ++ri) {
            const auto& row = s.rows[ri];
            out.push_back('{');
            for (std::size_t c = 0; c < s.columns.size(); ++c) {
                if (c) out.push_back(',');
                detail::append_json_string(out, keys[c]);
                out.push_back(':');
                if (c < row.size() && !s.is_null_cell(ri, c)) {
                    detail::append_json_string(out, row[c]);
                } else {
                    out += "null";
                }
            }
            out += "}\n";
        }
        if (s.timed_out || s.partial || !s.warnings.empty()) {
            out += "{\"statement_index\":";
            out += std::to_string(s.statement_index);
            if (s.timed_out) out += ",\"timed_out\":true";
            if (s.partial) out += ",\"partial\":true";
            if (!s.warnings.empty()) {
                out += ",\"warnings\":[";
                for (std::size_t i = 0; i < s.warnings.size(); ++i) {
                    if (i) out.push_back(',');
                    detail::append_json_string(out, s.warnings[i]);
                }
                out.push_back(']');
            }
            out += "}\n";
        }
    }
    return out;
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
    // Latch cancellation across statements: Database::query consumes the
    // predicate mid-statement, so a one-shot `true` must remain visible to
    // run_script's between-statement poll (mirrors the streaming serializers'
    // script-level latch). Both the poll and the executor share one latch.
    ScriptOptions effective = options;
    effective.should_cancel = detail::make_sticky_cancel(options.should_cancel);
    return run_script(script, effective,
        [&db, &effective](const std::string& sql, ScriptStatementResult& out) {
            QueryOptions qopts;
            qopts.timeout_ms = effective.timeout_ms;  // reuse Database::query's timeout
            qopts.should_cancel = effective.should_cancel;  // ...and its cancellation
            Result r = db.query(sql, qopts);
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
            // Round-trip the timeout / partial-result signalling so the envelope
            // can tell a truncated answer from a complete one.
            out.timed_out = r.timed_out;
            out.partial = r.partial;
            out.warnings = std::move(r.warnings);
        });
}

// ----- Streaming JSON serializer (bounded memory) ----------------------------
//
// Emits the script-envelope JSON incrementally via `sink`, stepping each
// statement row-by-row and never accumulating a read-only result set. Peak
// memory is O(one row) for reads. Mutation RETURNING rows are the deliberate
// exception: they remain provisional until SQLITE_DONE, so they are buffered
// until the statement succeeds and discarded if it is interrupted or fails.
// Pair the read path with a chunked HTTP response so the wire is incremental.
//
// This is ADDITIVE and opt-in: run_database_script + script_result_to_json stay
// the default buffered path, byte-for-byte unchanged. The streamed envelope
// carries the SAME key set and values as the buffered form, but the aggregate
// fields that are unknowable until every row is emitted — each statement's
// success/error/row_count/elapsed_ms, and the top-level row_count_total/
// elapsed_ms_total/first_error_index/success — are emitted AFTER the rows they
// summarize. JSON is order-independent for parsers, so a structural compare
// against the buffered output matches (see the round-trip test).
inline void stream_database_script_json(
    Database& db, const std::string& script, const ScriptOptions& options,
    const std::function<bool(const char*, std::size_t)>& sink)
{
    // `sink` returns false once the client has gone away (httplib's DataSink::write
    // reports a disconnected connection this way). We latch that in `aborted` and
    // unwind — the streaming cancel-on-disconnect path. An explicit POST /cancel is
    // handled by options.should_cancel, polled between rows below.
    bool aborted = false;
    auto emit = [&sink, &aborted](const std::string& s) {
        if (aborted || s.empty()) return;
        if (!sink(s.data(), s.size())) aborted = true;
    };

    std::vector<std::string> statements;
    std::string split_error;
    if (!collect_statements(script, statements, split_error)) {
        std::string out =
            "{\"success\":false,\"statement_count\":0,\"results\":[]"
            ",\"row_count_total\":0,\"elapsed_ms_total\":0"
            ",\"first_error_index\":null,\"parse_error\":";
        detail::append_json_string(out, split_error.empty()
            ? std::string("Failed to parse SQL script") : split_error);
        out.push_back('}');
        emit(out);
        return;
    }

    std::string header = "{\"statement_count\":";
    header += std::to_string(statements.size());
    header += ",\"results\":[";
    emit(header);

    std::size_t row_count_total = 0;
    double elapsed_ms_total = 0.0;
    std::optional<std::size_t> first_error_index;
    detail::CooperativeInterruptState script_cancellation;
    script_cancellation.cancel =
        options.should_cancel ? &options.should_cancel : nullptr;

    for (std::size_t i = 0; i < statements.size(); ++i) {
        if (i) emit(",");
        const std::string& sql = statements[i];

        std::string head = "{\"statement_index\":";
        head += std::to_string(i);
        if (options.include_sql) {
            head += ",\"sql\":";
            detail::append_json_string(head, sql);
        }

        // Cancellation is request-terminal. Keep one sticky script-level state
        // so a mid-statement partial cancellation prevents every later
        // statement, even if the user predicate only returned true once.
        if (script_cancellation.poll()) {
            head += ",\"columns\":[],\"rows\":[],\"row_count\":0"
                    ",\"elapsed_ms\":0,\"success\":false,\"error\":";
            detail::append_json_string(
                head, script_cancellation.cancel_error.empty()
                    ? std::string("query cancelled")
                    : script_cancellation.cancel_error);
            head.push_back('}');
            emit(head);
            if (!first_error_index.has_value()) first_error_index = i;
            break;
        }

        const auto t0 = std::chrono::steady_clock::now();
        detail::CooperativeInterruptState interrupt;
        interrupt.started_at = t0;
        interrupt.timeout_ms = options.timeout_ms;
        interrupt.cancel =
            options.should_cancel ? &options.should_cancel : nullptr;
        // Arm before prepare so xBestIndex and other prepare-time callbacks are
        // covered by the same deadline/cancellation contract as sqlite3_step().
        detail::StreamingInterruptGuard interrupt_guard(db.sqlite_handle(),
                                                         interrupt);
        Statement st = db.prepare_statement(sql);
        auto elapsed_ms = [&t0]() {
            return std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - t0).count();
        };

        if (!st) {
            if (interrupt.cancelled) {
                script_cancellation.cancelled = true;
            }
            if (!interrupt.cancel_error.empty()) {
                script_cancellation.cancel_error = interrupt.cancel_error;
            }
            std::string prepare_error = st.error();
            if (!interrupt.cancel_error.empty()) {
                prepare_error = interrupt.cancel_error;
            } else if (interrupt.cancelled) {
                prepare_error = "Query cancelled";
            } else if (interrupt.timed_out) {
                prepare_error = "Query timed out";
            }
            head += ",\"columns\":[],\"rows\":[],\"row_count\":0,\"elapsed_ms\":";
            const double el = elapsed_ms();
            detail::append_json_number(head, el);
            head += ",\"success\":false,\"error\":";
            detail::append_json_string(head, prepare_error);
            if (interrupt.timed_out) {
                head += ",\"timed_out\":true";
            }
            head.push_back('}');
            emit(head);
            elapsed_ms_total += el;
            if (!first_error_index.has_value()) first_error_index = i;
            if (!options.continue_on_error) break;
            continue;
        }

        head += ",\"columns\":[";
        const int ncol = st.column_count();
        const bool readonly = st.is_readonly();
        const bool partial_capable = ncol > 0 && readonly;
        const bool defer_rows = ncol > 0 && !readonly;
        std::vector<std::string> deferred_rows;
        for (int c = 0; c < ncol; ++c) {
            if (c) head.push_back(',');
            detail::append_json_string(head, st.column_name(c));
        }
        head += "],\"rows\":[";
        emit(head);

        std::size_t rows = 0;
        std::string err;
        bool ok = true;
        bool completed = false;
        bool interrupt_sent = false;
        // A mutation whose provisional RETURNING rows are already in flight must
        // NOT be abandoned with a bare break: finalizing a non-errored statement
        // COMPLETES it, and SQLite commits the DML. Ask SQLite to abort instead —
        // that path rolls the statement back — and keep stepping until the abort
        // surfaces as a step error.
        auto request_mutation_abort = [&]() {
            if (!interrupt_sent) {
                sqlite3_interrupt(db.sqlite_handle());
                interrupt_sent = true;
            }
        };
        for (;;) {
            if (interrupt.poll()) {
                if (defer_rows && rows > 0) {
                    request_mutation_abort();
                } else {
                    break;
                }
            }
            const StepResult sr = st.step();
            if (sr == StepResult::row) {
                std::string rowbuf;
                rowbuf.reserve(static_cast<std::size_t>(ncol) * 16 + 2);
                if (rows) rowbuf.push_back(',');  // comma between rows
                rowbuf.push_back('[');
                for (int c = 0; c < ncol; ++c) {
                    if (c) rowbuf.push_back(',');
                    if (st.column_is_null(c)) rowbuf += "null";
                    else detail::append_json_string(rowbuf, st.text(c));
                }
                rowbuf.push_back(']');
                if (defer_rows) {
                    deferred_rows.push_back(rowbuf);
                } else {
                    emit(rowbuf);
                }
                ++rows;
                if (aborted) break;  // client disconnected mid-stream
                if (interrupt.poll()) {
                    if (defer_rows) {
                        request_mutation_abort();
                    } else {
                        break;
                    }
                }
            } else if (sr == StepResult::done) {
                completed = true;
                break;
            } else {
                // A progress callback or cooperative vtable poll latches the
                // cause before sqlite3_step() returns error. Do not invoke a
                // fresh predicate after the step has already completed.
                if (!interrupt.cancel_error.empty() || interrupt.cancelled ||
                    interrupt.timed_out) {
                    break;
                }
                ok = false;
                err = st.error();
                break;
            }
        }
        if (aborted) break;  // stop the statement loop; the footer emit no-ops

        const bool timed_out = interrupt.timed_out;
        const bool cancelled = interrupt.cancelled;
        if (cancelled) script_cancellation.cancelled = true;
        if (!interrupt.cancel_error.empty()) {
            script_cancellation.cancel_error = interrupt.cancel_error;
        }
        // A mutation that reached SQLITE_DONE completed — it has COMMITTED — so a
        // cancel/timeout latched during its final step must not relabel the
        // honest result as a failure (the script-level latch above still stops
        // the NEXT statement). A read-only statement can reach DONE with latched
        // state when a cooperative vtable truncated its scan, so reads still
        // classify: rows gathered are a partial answer; with none it is an error
        // (an empty "partial" would read as a valid truncation of the real one).
        const bool mutation_completed = completed && !readonly;
        if (mutation_completed) {
            // fallthrough: report the committed statement as-is
        } else if (!interrupt.cancel_error.empty()) {
            ok = false;
            err = interrupt.cancel_error;
        } else if (timed_out && !(partial_capable && rows > 0)) {
            ok = false;
            err = "Query timed out";
        } else if (cancelled && !(partial_capable && rows > 0)) {
            ok = false;
            err = "Query cancelled";
        }
        if (defer_rows) {
            if (ok) {
                for (const auto& row : deferred_rows) {
                    emit(row);
                    if (aborted) break;
                }
                if (aborted) break;
            } else {
                rows = 0;
            }
        }

        const double el = elapsed_ms();
        std::string tail = "],\"row_count\":";
        tail += std::to_string(rows);
        tail += ",\"elapsed_ms\":";
        detail::append_json_number(tail, el);
        tail += ",\"success\":";
        tail += ok ? "true" : "false";
        tail += ",\"error\":";
        if (err.empty()) tail += "null";
        else detail::append_json_string(tail, err);
        // A timed-out statement with rows already delivered is a partial success,
        // matching Database::query's timed_out/partial/warnings semantics. With
        // zero rows there is no prefix to keep — the error branch above already
        // reported it — and a committed mutation is not partial at all.
        if (timed_out && !mutation_completed) {
            tail += ",\"timed_out\":true";
            if (ok && partial_capable && rows > 0) {
                tail += ",\"partial\":true,\"warnings\":[";
                detail::append_json_string(
                    tail, "query timed out; returning partial rows");
                tail.push_back(']');
            }
        }
        // An explicit cancel is a partial success too (rows so far are valid), but
        // it is not a timeout, so no timed_out flag — mirrors the buffered path.
        if (cancelled && !mutation_completed && ok && partial_capable && rows > 0) {
            tail += ",\"partial\":true,\"warnings\":[";
            detail::append_json_string(tail, "query cancelled; returning partial rows");
            tail.push_back(']');
        }
        if (!ok && rows > 0 && partial_capable && !timed_out &&
            !cancelled) {
            tail += ",\"partial\":true,\"warnings\":[";
            detail::append_json_string(
                tail, "query failed; returning partial rows");
            tail.push_back(']');
        }
        tail.push_back('}');
        emit(tail);

        row_count_total += rows;
        elapsed_ms_total += el;
        if (!ok) {
            if (!first_error_index.has_value()) first_error_index = i;
            if (!options.continue_on_error) break;
        }
    }

    std::string footer = "],\"row_count_total\":";
    footer += std::to_string(row_count_total);
    footer += ",\"elapsed_ms_total\":";
    detail::append_json_number(footer, elapsed_ms_total);
    footer += ",\"first_error_index\":";
    footer += first_error_index.has_value() ? std::to_string(*first_error_index) : "null";
    footer += ",\"success\":";
    footer += first_error_index.has_value() ? "false" : "true";
    footer.push_back('}');
    emit(footer);
}

// Streaming NDJSON twin of stream_database_script_json: emits ONE keyed JSON object
// per row, one per line (the script_result_to_jsonl "records" shape), with no
// enclosing envelope — ideal for a client that consumes rows as a stream and
// appends them to a file. Read-only results use O(one row) memory; mutation
// RETURNING rows are buffered until successful completion so rolled-back rows
// never reach the sink. Honors options.timeout_ms and options.should_cancel
// between rows, and stops if `sink` reports a disconnect (returns false). A
// failed statement emits one {"statement_index":I,"error":"…"} line; a
// truncated statement (timeout / cancel) emits a trailing
// metadata line carrying `partial`, optional `timed_out`, and `warnings`, so the
// consumer can tell a short answer from a complete one.
inline void stream_database_script_ndjson(
    Database& db, const std::string& script, const ScriptOptions& options,
    const std::function<bool(const char*, std::size_t)>& sink)
{
    bool aborted = false;
    auto emit = [&sink, &aborted](const std::string& s) {
        if (aborted || s.empty()) return;
        if (!sink(s.data(), s.size())) aborted = true;
    };

    std::vector<std::string> statements;
    std::string split_error;
    if (!collect_statements(script, statements, split_error)) {
        std::string out = "{\"error\":";
        detail::append_json_string(out, split_error.empty()
            ? std::string("Failed to parse SQL script") : split_error);
        out += "}\n";
        emit(out);
        return;
    }

    detail::CooperativeInterruptState script_cancellation;
    script_cancellation.cancel =
        options.should_cancel ? &options.should_cancel : nullptr;

    for (std::size_t i = 0; i < statements.size(); ++i) {
        const std::string& sql = statements[i];
        if (script_cancellation.poll()) {
            std::string out = "{\"statement_index\":";
            out += std::to_string(i);
            out += ",\"error\":";
            detail::append_json_string(
                out, script_cancellation.cancel_error.empty()
                    ? std::string("query cancelled")
                    : script_cancellation.cancel_error);
            out += "}\n";
            emit(out);
            break;
        }
        const auto t0 = std::chrono::steady_clock::now();
        detail::CooperativeInterruptState interrupt;
        interrupt.started_at = t0;
        interrupt.timeout_ms = options.timeout_ms;
        interrupt.cancel =
            options.should_cancel ? &options.should_cancel : nullptr;
        detail::StreamingInterruptGuard interrupt_guard(db.sqlite_handle(),
                                                         interrupt);
        Statement st = db.prepare_statement(sql);
        if (!st) {
            if (interrupt.cancelled) {
                script_cancellation.cancelled = true;
            }
            if (!interrupt.cancel_error.empty()) {
                script_cancellation.cancel_error = interrupt.cancel_error;
            }
            std::string prepare_error = st.error();
            if (!interrupt.cancel_error.empty()) {
                prepare_error = interrupt.cancel_error;
            } else if (interrupt.cancelled) {
                prepare_error = "Query cancelled";
            } else if (interrupt.timed_out) {
                prepare_error = "Query timed out";
            }
            std::string out = "{\"statement_index\":";
            out += std::to_string(i);
            out += ",\"error\":";
            detail::append_json_string(out, prepare_error);
            if (interrupt.timed_out) out += ",\"timed_out\":true";
            out += "}\n";
            emit(out);
            if (!options.continue_on_error) break;
            continue;
        }

        const int ncol = st.column_count();
        const bool readonly = st.is_readonly();
        const bool partial_capable = ncol > 0 && readonly;
        const bool defer_rows = ncol > 0 && !readonly;
        std::vector<std::string> deferred_rows;
        std::vector<std::string> colnames;
        colnames.reserve(static_cast<std::size_t>(ncol));
        for (int c = 0; c < ncol; ++c) colnames.push_back(st.column_name(c));
        const auto json_keys = detail::unique_json_object_keys(colnames);

        bool failed = false;
        std::size_t rows = 0;
        std::string err;
        bool completed = false;
        bool interrupt_sent = false;
        // Same contract as the JSON serializer above: an in-flight mutation with
        // provisional RETURNING rows is aborted through SQLite (which rolls the
        // statement back), never abandoned with a bare break (which would commit).
        auto request_mutation_abort = [&]() {
            if (!interrupt_sent) {
                sqlite3_interrupt(db.sqlite_handle());
                interrupt_sent = true;
            }
        };
        for (;;) {
            if (interrupt.poll()) {
                if (defer_rows && rows > 0) {
                    request_mutation_abort();
                } else {
                    break;
                }
            }
            const StepResult sr = st.step();
            if (sr == StepResult::row) {
                std::string rowbuf;
                rowbuf.reserve(static_cast<std::size_t>(ncol) * 24 + 2);
                rowbuf.push_back('{');
                for (int c = 0; c < ncol; ++c) {
                    if (c) rowbuf.push_back(',');
                    detail::append_json_string(rowbuf, json_keys[c]);
                    rowbuf.push_back(':');
                    if (st.column_is_null(c)) rowbuf += "null";
                    else detail::append_json_string(rowbuf, st.text(c));
                }
                rowbuf += "}\n";
                if (defer_rows) {
                    deferred_rows.push_back(rowbuf);
                } else {
                    emit(rowbuf);
                }
                ++rows;
                if (aborted) break;
                if (interrupt.poll()) {
                    if (defer_rows) {
                        request_mutation_abort();
                    } else {
                        break;
                    }
                }
            } else if (sr == StepResult::done) {
                completed = true;
                break;
            } else {
                if (!interrupt.cancel_error.empty() || interrupt.cancelled ||
                    interrupt.timed_out) {
                    break;
                }
                failed = true;
                err = st.error();
                break;
            }
        }
        if (aborted) break;

        const bool timed_out = interrupt.timed_out;
        const bool cancelled = interrupt.cancelled;
        if (cancelled) script_cancellation.cancelled = true;
        if (!interrupt.cancel_error.empty()) {
            script_cancellation.cancel_error = interrupt.cancel_error;
        }
        // See the JSON serializer: a mutation at DONE has COMMITTED (report it
        // honestly; the script-level latch still stops the next statement), a
        // read at DONE may have been cooperatively truncated (classify), and a
        // cancel/timeout with zero rows is an error, not an empty "partial".
        const bool mutation_completed = completed && !readonly;
        if (mutation_completed) {
            // fallthrough: report the committed statement as-is
        } else if (!interrupt.cancel_error.empty()) {
            failed = true;
            err = interrupt.cancel_error;
        } else if (timed_out && !(partial_capable && rows > 0)) {
            failed = true;
            err = "Query timed out";
        } else if (cancelled && !(partial_capable && rows > 0)) {
            failed = true;
            err = "Query cancelled";
        }
        if (defer_rows) {
            if (!failed) {
                for (const auto& row : deferred_rows) {
                    emit(row);
                    if (aborted) break;
                }
                if (aborted) break;
            } else {
                rows = 0;
            }
        }

        if (failed) {
            std::string out = "{\"statement_index\":";
            out += std::to_string(i);
            out += ",\"error\":";
            detail::append_json_string(out, err);
            if (timed_out) out += ",\"timed_out\":true";
            if (rows > 0 && partial_capable) {
                out += ",\"partial\":true,\"warnings\":[";
                detail::append_json_string(
                    out, "query failed; returning partial rows");
                out.push_back(']');
            }
            out += "}\n";
            emit(out);
            if (!options.continue_on_error) break;
        } else if ((timed_out || cancelled) && !mutation_completed &&
                   partial_capable && rows > 0) {
            std::string out = "{\"statement_index\":";
            out += std::to_string(i);
            if (timed_out) out += ",\"timed_out\":true";
            out += ",\"partial\":true,\"warnings\":[";
            detail::append_json_string(
                out, timed_out
                    ? std::string("query timed out; returning partial rows")
                    : std::string("query cancelled; returning partial rows"));
            out.push_back(']');
            out += "}\n";
            emit(out);
        }
    }
}

} // namespace xsql
