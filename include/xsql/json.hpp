// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once
/// @file json.hpp
/// @brief JSON library alias for xsql projects
///
/// Provides a namespace alias for the JSON library used by all xsql projects
/// (idasql, pdbsql, clangsql). Currently wraps nlohmann/json.
/// This indirection allows future library swaps without changing user code.

#include <nlohmann/json.hpp>

namespace xsql {

/// JSON type alias
using json = nlohmann::json;

/// Ordered JSON (preserves insertion order)
using ordered_json = nlohmann::ordered_json;

} // namespace xsql
