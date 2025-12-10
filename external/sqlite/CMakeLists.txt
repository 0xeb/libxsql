add_library(sqlite3 STATIC
    sqlite3.c
)

target_include_directories(sqlite3 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# SQLite compile options
target_compile_definitions(sqlite3 PRIVATE
    SQLITE_ENABLE_FTS5
    SQLITE_ENABLE_RTREE
    SQLITE_ENABLE_JSON1
    SQLITE_ENABLE_COLUMN_METADATA
)

if(WIN32)
    target_link_libraries(sqlite3 PRIVATE)
else()
    target_link_libraries(sqlite3 PRIVATE pthread dl m)
endif()
