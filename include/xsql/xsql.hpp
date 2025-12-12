/**
 * xsql/xsql.hpp - Master include for libxsql
 *
 * libxsql - A generic SQLite virtual table framework
 *
 * Include this single header to get all libxsql functionality:
 *   - VTableDef, VTableBuilder - Define virtual tables
 *   - Database - RAII database wrapper with query helpers
 *   - SQL function registration utilities
 *
 * Example:
 *
 *   #include <xsql/xsql.hpp>
 *
 *   std::vector<int> data = {10, 20, 30};
 *
 *   auto def = xsql::table("numbers")
 *       .count([&]() { return data.size(); })
 *       .column_int64("value", [&](size_t i) { return data[i]; })
 *       .build();
 *
 *   xsql::Database db;
 *   db.open();
 *   db.register_table("numbers_mod", &def);
 *   db.create_table("numbers", "numbers_mod");
 *
 *   auto result = db.query("SELECT * FROM numbers WHERE value > 15");
 *   for (const auto& row : result) {
 *       printf("%s\n", row[0].c_str());
 *   }
 */

#pragma once

#include "types.hpp"
#include "vtable.hpp"
#include "vtable_writable.hpp"
#include "vtable_builder.hpp"
#include "functions.hpp"
#include "database.hpp"
