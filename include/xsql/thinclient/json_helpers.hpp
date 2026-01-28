#pragma once

/**
 * @file json_helpers.hpp
 * @brief Shared JSON helper functions for HTTP responses
 *
 * Provides common JSON utilities used by all *sql HTTP servers:
 * - json_escape(): Escape strings for JSON
 * - make_error_json(): Create error response
 * - make_success_json(): Create success response
 * - result_to_json(): Convert query results to JSON (template)
 */

#include <string>
#include <sstream>
#include <iomanip>
#include <vector>

namespace xsql::thinclient {

/**
 * Escape a string for JSON output.
 * Handles quotes, backslashes, control characters.
 * Uses ostringstream for portability (works with IDA SDK which blocks snprintf).
 */
inline std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 10);
    for (char ch : s) {
        switch (ch) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20) {
                    std::ostringstream oss;
                    oss << "\\u" << std::hex << std::setfill('0') << std::setw(4)
                        << static_cast<unsigned>(static_cast<unsigned char>(ch));
                    out += oss.str();
                } else {
                    out += ch;
                }
        }
    }
    return out;
}

/**
 * Create a JSON error response.
 * @param error Error message
 * @return JSON string: {"success":false,"error":"message"}
 */
inline std::string make_error_json(const std::string& error) {
    return R"({"success":false,"error":")" + json_escape(error) + R"("})";
}

/**
 * Create a simple JSON success response.
 * @param message Optional message
 * @return JSON string: {"success":true} or {"success":true,"message":"msg"}
 */
inline std::string make_success_json(const std::string& message = "") {
    if (message.empty()) {
        return R"({"success":true})";
    }
    return R"({"success":true,"message":")" + json_escape(message) + R"("})";
}

/**
 * Create a JSON status/health response.
 * @param tool Tool name (e.g., "bnsql", "idasql")
 * @param extra_json Additional JSON fields without enclosing braces (e.g., "\"functions\":42")
 * @return JSON string: {"success":true,"status":"ok","tool":"toolname",...}
 */
inline std::string make_status_json(const std::string& tool, const std::string& extra_json = "") {
    std::string json = R"({"success":true,"status":"ok","tool":")" + json_escape(tool) + R"(")";
    if (!extra_json.empty()) {
        json += "," + extra_json;
    }
    json += "}";
    return json;
}

/**
 * Convert a query result to JSON.
 * Works with result types that have:
 * - success (bool)
 * - columns (vector<string>)
 * - rows (vector<vector<string>>)
 * - error (string) - used when !success
 *
 * @tparam ResultT Query result type
 * @param result Query result object
 * @return JSON string with standard response format
 */
template<typename ResultT>
std::string result_to_json(const ResultT& result) {
    std::ostringstream out;
    out << "{";
    out << "\"success\":" << (result.success ? "true" : "false");

    if (result.success) {
        // Columns
        out << ",\"columns\":[";
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) out << ",";
            out << "\"" << json_escape(result.columns[i]) << "\"";
        }
        out << "]";

        // Rows
        out << ",\"rows\":[";
        for (size_t i = 0; i < result.rows.size(); i++) {
            if (i > 0) out << ",";
            out << "[";
            for (size_t j = 0; j < result.rows[i].size(); j++) {
                if (j > 0) out << ",";
                out << "\"" << json_escape(result.rows[i][j]) << "\"";
            }
            out << "]";
        }
        out << "]";

        // Row count
        out << ",\"row_count\":" << result.rows.size();
    } else {
        // Error message
        out << ",\"error\":\"" << json_escape(result.error) << "\"";
    }

    out << "}";
    return out.str();
}

}  // namespace xsql::thinclient
