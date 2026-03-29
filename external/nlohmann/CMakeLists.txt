# nlohmann/json v3.11.3 — https://github.com/nlohmann/json
# Vendored single-include distribution (MIT License)

add_library(nlohmann_json INTERFACE)
add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)

target_include_directories(nlohmann_json INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(nlohmann_json INTERFACE cxx_std_11)
