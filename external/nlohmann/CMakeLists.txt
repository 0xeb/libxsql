# nlohmann/json v3.11.3 — https://github.com/nlohmann/json
# Vendored single-include distribution (MIT License)

# Skip if another consumer (e.g. fastmcpp's FetchContent, vcpkg, or an outer
# project) already provided the target. Matches the defensive pattern
# fastmcpp itself uses, so multiple xsql-family modules can coexist in one
# build tree without target collisions.
if(NOT TARGET nlohmann_json)
    add_library(nlohmann_json INTERFACE)
    if(NOT TARGET nlohmann_json::nlohmann_json)
        add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
    endif()

    target_include_directories(nlohmann_json INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )

    target_compile_features(nlohmann_json INTERFACE cxx_std_11)

    # Signal to the parent that libxsql owns this target — only then is it
    # safe to include it in xsql's export/install set.
    set(XSQL_OWNS_NLOHMANN_JSON TRUE PARENT_SCOPE)
else()
    set(XSQL_OWNS_NLOHMANN_JSON FALSE PARENT_SCOPE)
endif()
