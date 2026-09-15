# cpp-httplib v0.16.3 — https://github.com/yhirose/cpp-httplib
# Vendored single-header distribution (MIT License)

add_library(httplib INTERFACE)
add_library(httplib::httplib ALIAS httplib)

target_include_directories(httplib INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(httplib INTERFACE cxx_std_11)

# Platform-specific link dependencies
find_package(Threads REQUIRED)
target_link_libraries(httplib INTERFACE Threads::Threads)
if(WIN32)
    target_link_libraries(httplib INTERFACE ws2_32 crypt32)
endif()

# Optional gzip response compression (CPPHTTPLIB_ZLIB_SUPPORT). Live-verified
# gap, not a guess: pdbsql's /query never compressed a response even with
# Accept-Encoding: gzip sent (a 10,000-row JSON pull, 1.08 MB uncompressed,
# came back with no Content-Encoding header at all) -- every prior
# performance fix this project has shipped reduced CPU/DIA-side cost,
# measured over loopback; this is the first one that helps actual network
# transfer time for a client on a real link, where JSON/CSV/NDJSON bulk
# exports typically compress 5-10x. No system zlib is reliably present for
# this MSVC toolchain, so it is fetched via FetchContent like the project's
# other small, stable C/C++ dependencies (fastmcpp, googletest) rather than
# assumed installed. Applies to every *sql tool's HTTP server uniformly --
# this is the shared httplib wrapper, not a pdbsql-specific file.
include(FetchContent)
FetchContent_Declare(
    zlib_httplib
    GIT_REPOSITORY https://github.com/madler/zlib.git
    GIT_TAG v1.3.1
)
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SKIP_INSTALL_ALL ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(zlib_httplib)

# zlib's own CMakeLists never marks zlibstatic position-independent, so linking
# the archive into any consumer's SHARED library fails on ELF x86_64:
#
#   relocation R_X86_64_PC32 against symbol `_length_code' can not be used when
#   making a shared object; recompile with -fPIC
#
# Every tool in this family ships a plugin as a .so, so each one hits this the
# moment it takes a libxsql carrying zlib (zlib arrived in v1.0.15; anything
# pinned older never saw it). Caught by bnsql's Linux CI on the v0.0.12 release
# when it moved off a v1.0.10 pin. PE and Mach-O do not have the distinction,
# so Windows and macOS build clean and the failure looks Linux-specific.
#
# Set it here, where we own the fetched target, rather than in each consumer.
# libxsql already does the same for its own xsql and sqlite3 targets. Guarded
# because a parent project may have supplied httplib (and zlib) itself.
if(TARGET zlibstatic)
    set_target_properties(zlibstatic PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

# Export the fetched header locations to libxsql's parent CMake scope so the
# standalone install rules can make the public httplib/zlib dependency graph
# relocatable. FetchContent's variables otherwise remain local to this wrapper
# subdirectory.
set(XSQL_HTTPLIB_ZLIB_SOURCE_DIR "${zlib_httplib_SOURCE_DIR}" PARENT_SCOPE)
set(XSQL_HTTPLIB_ZLIB_BINARY_DIR "${zlib_httplib_BINARY_DIR}" PARENT_SCOPE)

target_compile_definitions(httplib INTERFACE CPPHTTPLIB_ZLIB_SUPPORT)
target_link_libraries(httplib INTERFACE zlibstatic)
target_include_directories(httplib INTERFACE
    $<BUILD_INTERFACE:${zlib_httplib_SOURCE_DIR}>
    $<BUILD_INTERFACE:${zlib_httplib_BINARY_DIR}>
)
