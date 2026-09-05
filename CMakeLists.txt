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

project(libxsql VERSION 1.0.17 LANGUAGES C CXX)

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
        # zlibstatic: httplib's optional gzip-compression dependency
        # (CPPHTTPLIB_ZLIB_SUPPORT, external/httplib/CMakeLists.txt) --
        # httplib now links it, so when OUR add_subdirectory(external/httplib)
        # is what defines httplib::httplib, zlibstatic must be in the same
        # build-tree export set or `export(TARGETS httplib ...)` below fails
        # ("requires target zlibstatic that is not in any export set").
        #
        # Guard on TARGET zlibstatic, not just XSQL_WITH_THINCLIENT: a
        # consumer can pre-define httplib::httplib itself before this file's
        # `if(NOT TARGET httplib::httplib) add_subdirectory(external/httplib)`
        # runs. In that case our external/httplib/CMakeLists.txt -- and its zlib
        # FetchContent -- never runs, so zlibstatic never exists here even
        # though XSQL_WITH_THINCLIENT is ON. Exporting a nonexistent target is a
        # hard configure error, not a graceful skip.
        #
        # No known consumer does this any more: a build that pre-fetched a
        # second httplib dropped it on 2026-09-02 when this vendored copy was
        # bumped to 0.16.3 (see external/httplib/VENDORED.md "Why 0.16.3"). The
        # guard stays because it protects a supported integration shape, not
        # because anything here still relies on it.
        list(APPEND XSQL_EXPORT_TARGETS httplib)
        if(TARGET zlibstatic)
            list(APPEND XSQL_EXPORT_TARGETS zlibstatic)
        endif()
    endif()

    export(TARGETS ${XSQL_EXPORT_TARGETS}
        NAMESPACE xsql::
        FILE "${CMAKE_CURRENT_BINARY_DIR}/xsqlTargets.cmake"
    )
    set(XSQL_CONFIG_PREAMBLE "")
    if(XSQL_WITH_THINCLIENT)
        string(APPEND XSQL_CONFIG_PREAMBLE
            "include(CMakeFindDependencyMacro)\nfind_dependency(Threads)\n")
    endif()
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/xsqlConfig.cmake"
        "${XSQL_CONFIG_PREAMBLE}include(\"\${CMAKE_CURRENT_LIST_DIR}/xsqlTargets.cmake\")\n")
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
    if(XSQL_WITH_THINCLIENT)
        # xsql publicly links the header-only httplib target, so an installed
        # xsqlTargets.cmake must export it too. When this standalone build owns
        # the vendored httplib target it also owns zlibstatic; make that target
        # relocatable and install the two public zlib headers it exposes.
        list(APPEND XSQL_INSTALL_TARGETS httplib)
        # Only when OUR external/httplib actually ran do we own zlibstatic and
        # know where its headers are. A consumer that pre-defined
        # httplib::httplib leaves zlibstatic defined by someone else (or not at
        # all) and these two vars unset -- installing "/zlib.h" would be a hard
        # configure error, so require both the target AND the paths.
        if(TARGET zlibstatic AND DEFINED XSQL_HTTPLIB_ZLIB_SOURCE_DIR
                             AND DEFINED XSQL_HTTPLIB_ZLIB_BINARY_DIR)
            # APPEND, never overwrite: zlibstatic is a third-party FetchContent
            # target shared with every other consumer in the build tree, and a
            # plain set_property() would discard whatever zlib's own CMakeLists
            # put there. We are only adding the INSTALL_INTERFACE view needed to
            # make the exported target relocatable.
            set_property(TARGET zlibstatic APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                "$<BUILD_INTERFACE:${XSQL_HTTPLIB_ZLIB_SOURCE_DIR}>"
                "$<BUILD_INTERFACE:${XSQL_HTTPLIB_ZLIB_BINARY_DIR}>"
                "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
            list(APPEND XSQL_INSTALL_TARGETS zlibstatic)
            install(FILES
                "${XSQL_HTTPLIB_ZLIB_SOURCE_DIR}/zlib.h"
                "${XSQL_HTTPLIB_ZLIB_BINARY_DIR}/zconf.h"
                DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
        endif()
        install(FILES external/httplib/httplib.h
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
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
    install(FILES external/sqlite/sqlite3.h external/sqlite/sqlite3ext.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    if(XSQL_OWNS_NLOHMANN_JSON)
        install(DIRECTORY external/nlohmann/include/
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    endif()

    install(EXPORT xsqlTargets
        FILE xsqlTargets.cmake
        NAMESPACE xsql::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xsql
    )
    set(XSQL_INSTALL_CONFIG_PREAMBLE "")
    if(XSQL_WITH_THINCLIENT)
        string(APPEND XSQL_INSTALL_CONFIG_PREAMBLE
            "include(CMakeFindDependencyMacro)\nfind_dependency(Threads)\n")
    endif()
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/xsqlConfig-install.cmake"
        "${XSQL_INSTALL_CONFIG_PREAMBLE}include(\"\${CMAKE_CURRENT_LIST_DIR}/xsqlTargets.cmake\")\n")
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/xsqlConfig-install.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xsql
        RENAME xsqlConfig.cmake)
endif()
