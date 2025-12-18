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

# TCP server/client
add_executable(example_server_client server_client.cpp)
target_link_libraries(example_server_client PRIVATE xsql::xsql)
if(WIN32)
    target_link_libraries(example_server_client PRIVATE ws2_32)
endif()
