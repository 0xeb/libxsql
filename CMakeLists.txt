cmake_minimum_required(VERSION 3.20)
project(libxsql VERSION 1.0.0 LANGUAGES C CXX)

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
# libxsql - Header-only library with SQLite dependency
# ============================================================================

add_library(xsql INTERFACE)
add_library(xsql::xsql ALIAS xsql)

target_include_directories(xsql INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(xsql INTERFACE sqlite3)

target_compile_features(xsql INTERFACE cxx_std_17)

# ============================================================================
# Tests (optional)
# ============================================================================

option(XSQL_BUILD_TESTS "Build libxsql tests" ${XSQL_STANDALONE})
if(XSQL_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
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

    install(TARGETS xsql sqlite3
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
