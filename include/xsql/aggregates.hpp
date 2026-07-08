// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

namespace xsql {

class Database;

// Register libxsql's built-in aggregates on the given Database. Called
// automatically by Database::open(). Currently registers:
//   - blob_concat(value): concatenates BLOBs and/or single-byte INTs (0-255)
//     into one BLOB. NULL inputs are skipped. TEXT or out-of-range INTs cause
//     a query-level error. Zero rows or all-NULL input yields NULL; a non-NULL
//     input that contributes no bytes (e.g. x'') yields a zero-length BLOB.
bool register_builtin_aggregates(Database& db);

} // namespace xsql
