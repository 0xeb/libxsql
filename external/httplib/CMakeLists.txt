# cpp-httplib v0.15.3 — https://github.com/yhirose/cpp-httplib
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
