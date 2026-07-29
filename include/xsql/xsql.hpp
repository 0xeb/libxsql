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
 *   - define_sql_capabilities - the canonical sql_capabilities table
 *   - xsql::graph - dominators, natural loops, SCC, topological order
 *   - xsql::runtime - the transactional runtime_settings table
 *
 */

#pragma once

#include "cli/table_printer.hpp"
#include "vtable.hpp"
#include "functions.hpp"
#include "statement.hpp"
#include "database.hpp"
#include "aggregates.hpp"
#include "capabilities.hpp"
#include "graph.hpp"
#include "runtime_settings.hpp"
#include "runtime_settings_table.hpp"
