// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <algorithm>
#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

namespace xsql::cli {

// Table rendering style. `borderless` is the historical output (two-space
// columns, dashed rule); `boxed` is the classic +---+ / | cell | box (what
// pdbsql's — and idasql's — bespoke CLI printers hand-rolled), now shareable.
enum class TableStyle {
    borderless = 0,
    boxed,
};

struct TablePrintOptions {
    std::string no_result = "(no result)";
    bool newline_after_no_result = false;
    // Appended + defaulted so every existing caller (which passes {} or nothing)
    // is byte-identical: default style is `borderless` and the boxed-only footer
    // toggle is inert unless `style == boxed`.
    TableStyle style = TableStyle::borderless;
    // Boxed style only: append a `N row(s)\n` footer after the box (pdbsql prints
    // it; callers can disable the separator explicitly, hence default true).
    bool boxed_row_count_footer = true;
};

inline std::string print_table(const std::vector<std::string>& columns,
                               const std::vector<std::vector<std::string>>& rows,
                               const TablePrintOptions& options = {}) {
    if (columns.empty()) {
        // Boxed callers (pdbsql) print NOTHING for a column-less / empty result.
        if (options.style == TableStyle::boxed) return {};
        return options.newline_after_no_result
            ? options.no_result + "\n"
            : options.no_result;
    }

    std::vector<std::size_t> widths(columns.size(), 0);
    for (std::size_t i = 0; i < columns.size(); ++i) {
        widths[i] = columns[i].size();
    }
    for (const auto& row : rows) {
        for (std::size_t i = 0; i < row.size() && i < widths.size(); ++i) {
            // Parenthesize the function name so it survives a consumer that pulls
            // in <windows.h> without NOMINMAX (where `max` is a function-like
            // macro that would otherwise clobber std::max — e.g. pdbsql/DIA).
            widths[i] = (std::max)(widths[i], row[i].size());
        }
    }

    std::ostringstream out;

    if (options.style == TableStyle::boxed) {
        // `+` + (width+2) dashes per column + `+` — the +2 covers the leading and
        // trailing space around each cell.
        std::string rule = "+";
        for (std::size_t w : widths) rule += std::string(w + 2, '-') + "+";

        // `| ` then, per column, the value left-padded to width + ` | ` — a short
        // or missing cell is padded so the column count stays fixed. The trailing
        // space after the final `|` is load-bearing (matches the historical box).
        auto box_line = [&](const std::vector<std::string>& cells) {
            out << "| ";
            for (std::size_t i = 0; i < widths.size(); ++i) {
                const std::string& v = i < cells.size() ? cells[i] : std::string();
                out << v;
                if (v.size() < widths[i]) out << std::string(widths[i] - v.size(), ' ');
                out << " | ";
            }
            out << '\n';
        };

        out << rule << '\n';
        box_line(columns);
        out << rule << '\n';
        for (const auto& row : rows) box_line(row);
        out << rule << '\n';
        if (options.boxed_row_count_footer) out << rows.size() << " row(s)\n";
        return out.str();
    }

    // ---- borderless (default): unchanged, byte-for-byte ----
    auto write_cells = [&](const std::vector<std::string>& cells) {
        for (std::size_t i = 0; i < cells.size(); ++i) {
            if (i) out << "  ";
            out << cells[i];
            if (i + 1 < cells.size() && i < widths.size() && cells[i].size() < widths[i]) {
                out << std::string(widths[i] - cells[i].size(), ' ');
            }
        }
        out << '\n';
    };

    write_cells(columns);
    std::vector<std::string> separator;
    separator.reserve(widths.size());
    for (std::size_t width : widths) {
        separator.emplace_back(width, '-');
    }
    write_cells(separator);
    for (const auto& row : rows) {
        write_cells(row);
    }
    return out.str();
}

}  // namespace xsql::cli
