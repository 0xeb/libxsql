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
