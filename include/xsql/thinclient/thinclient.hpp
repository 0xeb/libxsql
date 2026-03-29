// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

/**
 * @file thinclient.hpp
 * @brief Master include for thin client support
 *
 * Includes CLI parsing, HTTP server, and HTTP client.
 * Enable with XSQL_WITH_THINCLIENT CMake option.
 */

#include <xsql/thinclient/cli.hpp>
#include <xsql/thinclient/server.hpp>
#include <xsql/thinclient/client.hpp>
#include <xsql/thinclient/clipboard.hpp>
