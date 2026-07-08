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

struct TablePrintOptions {
    std::string no_result = "(no result)";
    bool newline_after_no_result = false;
};

inline std::string print_table(const std::vector<std::string>& columns,
                               const std::vector<std::vector<std::string>>& rows,
                               const TablePrintOptions& options = {}) {
    if (columns.empty()) {
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
