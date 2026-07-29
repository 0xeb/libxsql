// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <xsql/vtable.hpp>

namespace xsql {

struct SqlCapability {
    std::string name;
    bool is_supported = false;
    std::string notes;
};

// Build the one canonical capability surface used throughout the family:
//
//   sql_capabilities(name TEXT, is_supported INTEGER, notes TEXT)
//
// Rows are sorted for deterministic output and duplicate/empty names fail at
// definition time instead of leaving consumers with ambiguous feature answers.
inline CachedTableDef<SqlCapability>
define_sql_capabilities(std::vector<SqlCapability> capabilities) {
    std::sort(
        capabilities.begin(), capabilities.end(),
        [](const SqlCapability& left, const SqlCapability& right) {
            return left.name < right.name;
        });
    for (std::size_t index = 0; index < capabilities.size(); ++index) {
        if (capabilities[index].name.empty()) {
            throw std::invalid_argument(
                "sql_capabilities contains an empty capability name");
        }
        if (index != 0 &&
            capabilities[index - 1].name == capabilities[index].name) {
            throw std::invalid_argument(
                "sql_capabilities contains duplicate capability '" +
                capabilities[index].name + "'");
        }
    }

    auto rows = std::make_shared<const std::vector<SqlCapability>>(
        std::move(capabilities));
    return xsql::cached_table<SqlCapability>("sql_capabilities")
        .row_count([rows]() { return rows->size(); })
        .estimate_rows([rows]() { return rows->size(); })
        .cache_builder(
            [rows](std::vector<SqlCapability>& output) {
                output = *rows;
            })
        .column_text(
            "name", [](const SqlCapability& row) { return row.name; })
        .column_int(
            "is_supported",
            [](const SqlCapability& row) {
                return row.is_supported ? 1 : 0;
            })
        .column_text(
            "notes", [](const SqlCapability& row) { return row.notes; })
        .build();
}

} // namespace xsql
