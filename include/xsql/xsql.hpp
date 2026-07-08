// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

/**
 * xsql/xsql.hpp - Master include for libxsql
 *
 * libxsql - A generic SQLite virtual table framework
 *
 * Include this single header to get all libxsql functionality:
 *   - VTableDef, VTableBuilder - Define virtual tables (read-only or writable)
 *   - Database - RAII database wrapper with query helpers
 *   - SQL function registration utilities
 *
 * Example (read-only):
 *
 *   #include <xsql/xsql.hpp>
 *
 *   std::vector<int> data = {10, 20, 30};
 *
 *   auto def = xsql::table("numbers")
 *       .count([&]() { return data.size(); })
 *       .column_int64("value", [&](size_t i) { return data[i]; })
 *       .build();
 *
 * Example (writable with hook):
 *
 *   auto def = xsql::table("items")
 *       .count([&]() { return items.size(); })
 *       .on_modify([](const std::string& op) { log(op); })
 *       .column_text_rw("name", getter, setter)
 *       .deletable(delete_fn)
 *       .build();
 */

#pragma once

#include "cli/table_printer.hpp"
#include "vtable.hpp"
#include "functions.hpp"
#include "statement.hpp"
#include "database.hpp"
#include "aggregates.hpp"
#include "runtime_settings.hpp"
