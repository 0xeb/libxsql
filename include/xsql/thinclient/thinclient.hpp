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
