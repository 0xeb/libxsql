#
# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
#
# This file is licensed under the Human-Origin Source License v1.0.
# See LICENSE.
#

cmake_minimum_required(VERSION 3.20)

# Enable CMAKE_MSVC_RUNTIME_LIBRARY support (required for IDA SDK compatibility)
cmake_policy(SET CMP0091 NEW)

project(libxsql VERSION 1.0.13 LANGUAGES C CXX)

# Detect if we're being built standalone or as part of a larger project
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(XSQL_STANDALONE ON)
else()
    set(XSQL_STANDALONE OFF)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ============================================================================
# SQLite
# ============================================================================

add_subdirectory(external/sqlite)

# ============================================================================
# nlohmann/json (vendored single-include, v3.11.3)
# ============================================================================

add_subdirectory(external/nlohmann)

# ============================================================================
# libxsql
# ============================================================================

add_library(xsql STATIC
    src/aggregates.cpp
    src/database.cpp
    src/functions.cpp
    src/script.cpp
    src/statement.cpp
)
add_library(xsql::xsql ALIAS xsql)

target_include_directories(xsql PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(xsql PUBLIC sqlite3 nlohmann_json::nlohmann_json)

target_compile_features(xsql PUBLIC cxx_std_17)

# Expose the project version to headers (e.g. thinclient/cli.hpp --version) so the
# printed version stays in sync with project(... VERSION ...) automatically.
target_compile_definitions(xsql PUBLIC XSQL_VERSION_STRING="${PROJECT_VERSION}")

# Enable PIC so the static library can be linked into shared objects (e.g. IDA plugins)
set_target_properties(xsql PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# Thin Client (optional HTTP server/client)
# ============================================================================

option(XSQL_WITH_THINCLIENT "Build thin client support (HTTP server/client)" OFF)

if(XSQL_WITH_THINCLIENT)
    if(NOT TARGET httplib::httplib)
        add_subdirectory(external/httplib)
    endif()
    target_compile_definitions(xsql PUBLIC XSQL_HAS_THINCLIENT)
    target_link_libraries(xsql PUBLIC httplib::httplib)
endif()

# Make find_package(xsql) work from build tree.
#
# Only generate the build-tree export when libxsql owns all of xsql's PUBLIC
# link dependencies. When an outer project (e.g. bnsql's fastmcpp
# FetchContent) already provided nlohmann_json, that target lives in a
# different export set and CMake refuses to export xsql against it. In that
# case the outer project consumes xsql::xsql directly as an in-tree target
# and the build-tree export is unnecessary anyway.
if(XSQL_OWNS_NLOHMANN_JSON)
    set(XSQL_EXPORT_TARGETS xsql sqlite3 nlohmann_json)
    if(XSQL_WITH_THINCLIENT)
        list(APPEND XSQL_EXPORT_TARGETS httplib)
    endif()

    export(TARGETS ${XSQL_EXPORT_TARGETS}
        NAMESPACE xsql::
        FILE "${CMAKE_CURRENT_BINARY_DIR}/xsqlTargets.cmake"
    )
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/xsqlConfig.cmake"
        "include(\"\${CMAKE_CURRENT_LIST_DIR}/xsqlTargets.cmake\")\n"
    )
    set(xsql_DIR "${CMAKE_CURRENT_BINARY_DIR}" CACHE PATH "xsql config directory" FORCE)
endif()

# ============================================================================
# Examples (optional)
# ============================================================================

option(XSQL_BUILD_EXAMPLES "Build libxsql examples" ${XSQL_STANDALONE})
if(XSQL_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# ============================================================================
# Install (for standalone builds)
# ============================================================================

if(XSQL_STANDALONE)
    include(GNUInstallDirs)

    set(XSQL_INSTALL_TARGETS xsql sqlite3)
    if(XSQL_OWNS_NLOHMANN_JSON)
        list(APPEND XSQL_INSTALL_TARGETS nlohmann_json)
    endif()

    install(TARGETS ${XSQL_INSTALL_TARGETS}
        EXPORT xsqlTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )

    install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(EXPORT xsqlTargets
        FILE xsqlConfig.cmake
        NAMESPACE xsql::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xsql
    )
endif()
