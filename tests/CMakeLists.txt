# libxsql tests - GoogleTest-based test suite

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

# Test sources
set(XSQL_TEST_SOURCES
    test_vtable.cpp
    test_database.cpp
    test_protocol.cpp
)

# Add thinclient tests if enabled
if(XSQL_WITH_THINCLIENT)
    list(APPEND XSQL_TEST_SOURCES test_thinclient.cpp)
endif()

# Test executable
add_executable(xsql_tests ${XSQL_TEST_SOURCES})

target_link_libraries(xsql_tests PRIVATE
    GTest::gtest_main
    xsql::xsql
)

include(GoogleTest)
gtest_discover_tests(xsql_tests PROPERTIES TIMEOUT 20)
