// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

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
