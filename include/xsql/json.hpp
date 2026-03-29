// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
