/**
 * xsql/script.hpp - SQL script execution and table export utilities
 */

#pragma once

#include <string>
#include <vector>

namespace xsql {

class Database;

struct StatementResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

std::string quote_identifier(const std::string& name);

bool collect_statements(const std::string& script,
                        std::vector<std::string>& statements,
                        std::string& error);

bool execute_script(Database& db,
                    const std::string& script,
                    std::vector<StatementResult>& results,
                    std::string& error);

bool export_tables(Database& db,
                   const std::vector<std::string>& requested_tables,
                   const std::string& output_path,
                   std::string& error);

} // namespace xsql
