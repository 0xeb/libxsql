#
# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
#
# This file is licensed under the Human-Origin Source License v1.0.
# See LICENSE.
#

# libxsql examples

# Basic read-only table
add_executable(example_basic_table basic_table.cpp)
target_link_libraries(example_basic_table PRIVATE xsql::xsql)

# Writable table with UPDATE/DELETE
add_executable(example_writable_table writable_table.cpp)
target_link_libraries(example_writable_table PRIVATE xsql::xsql)

# Cached table with constraint pushdown
add_executable(example_cached_table cached_table.cpp)
target_link_libraries(example_cached_table PRIVATE xsql::xsql)

