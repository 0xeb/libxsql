// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

/**
 * xsql/script.hpp - SQL script execution and table export utilities
 */

#pragma once

#include <string>
#include <vector>

namespace xsql {

class Database;

struct StatementResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

std::string quote_identifier(const std::string& name);

bool collect_statements(const std::string& script,
                        std::vector<std::string>& statements,
                        std::string& error);

bool execute_script(Database& db,
                    const std::string& script,
                    std::vector<StatementResult>& results,
                    std::string& error);

// Write `requested_tables` (or, when empty, every table in sqlite_master,
// ordered by name) to `output_path` as a replayable SQL script: per table a
// "-- Table:" comment, DROP TABLE IF EXISTS, CREATE TABLE from PRAGMA
// table_info, one INSERT per row and a "-- N rows exported" comment.
//
// The header is "-- SQL Export" then "-- Tables: N", N being the tables
// actually written. A table that cannot be read without constraints (a
// virtual table needing a WHERE, such as one with a required constraint or a
// forbidden full scan) is skipped when exporting every table and listed on a
// "-- Skipped (requires constraints): a, b" header line; requested by name,
// it fails the export with "Table 'a' cannot be exported: it requires
// constraints (WHERE) to be read (<the table's reason>)". Any other error
// fails the export. On failure `output_path` is left untouched and `error`
// says why.
bool export_tables(Database& db,
                   const std::vector<std::string>& requested_tables,
                   const std::string& output_path,
                   std::string& error);

} // namespace xsql
