/**
 * @file protocol.hpp
 * @brief JSON protocol helpers for xsql socket server/client
 *
 * Defines the wire protocol used between *sql CLI tools and servers.
 * Protocol: length-prefixed JSON over TCP
 *
 * Request:  {"sql": "SELECT ..."}
 * Response: {"success": true, "columns": [...], "rows": [[...], ...], "row_count": N}
 *           {"success": false, "error": "message"}
 */

#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <cstdint>

namespace xsql::socket {

//=============================================================================
// Query Result (for server-side)
//=============================================================================

struct QueryResult {
    bool success = false;
    std::string error;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;

    size_t row_count() const { return rows.size(); }
    size_t column_count() const { return columns.size(); }

    static QueryResult ok() {
        QueryResult r;
        r.success = true;
        return r;
    }

    static QueryResult fail(const std::string& msg) {
        QueryResult r;
        r.success = false;
        r.error = msg;
        return r;
    }
};

//=============================================================================
// JSON Helpers
//=============================================================================

inline std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 10);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // Control character - escape as \uXXXX
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
                break;
        }
    }
    return out;
}

inline std::string json_unescape(const std::string& s, size_t& pos) {
    std::string out;
    while (pos < s.size() && s[pos] != '"') {
        if (s[pos] == '\\' && pos + 1 < s.size()) {
            pos++;
            switch (s[pos]) {
                case 'n': out += '\n'; break;
                case 'r': out += '\r'; break;
                case 't': out += '\t'; break;
                case '"': out += '"'; break;
                case '\\': out += '\\'; break;
                case 'u':
                    // Unicode escape \uXXXX - simplified handling
                    if (pos + 4 < s.size()) {
                        pos += 4;
                    }
                    out += '?';  // Simplified - just put placeholder
                    break;
                default: out += s[pos]; break;
            }
        } else {
            out += s[pos];
        }
        pos++;
    }
    return out;
}

//=============================================================================
// Result Serialization
//=============================================================================

inline std::string result_to_json(const QueryResult& result) {
    std::ostringstream json;
    json << "{";
    json << "\"success\":" << (result.success ? "true" : "false");

    if (result.success) {
        json << ",\"columns\":[";
        for (size_t i = 0; i < result.columns.size(); i++) {
            if (i > 0) json << ",";
            json << "\"" << json_escape(result.columns[i]) << "\"";
        }
        json << "]";

        json << ",\"rows\":[";
        for (size_t r = 0; r < result.rows.size(); r++) {
            if (r > 0) json << ",";
            json << "[";
            for (size_t c = 0; c < result.rows[r].size(); c++) {
                if (c > 0) json << ",";
                json << "\"" << json_escape(result.rows[r][c]) << "\"";
            }
            json << "]";
        }
        json << "]";
        json << ",\"row_count\":" << result.row_count();
    } else {
        json << ",\"error\":\"" << json_escape(result.error) << "\"";
    }

    json << "}";
    return json.str();
}

inline std::string extract_sql_from_request(const std::string& json) {
    auto pos = json.find("\"sql\"");
    if (pos == std::string::npos) return "";

    pos = json.find("\"", pos + 5);
    if (pos == std::string::npos) return "";
    pos++;

    return json_unescape(json, pos);
}

//=============================================================================
// Remote Result (for client-side parsing)
//=============================================================================

struct RemoteRow {
    std::vector<std::string> values;
    const std::string& operator[](size_t i) const { return values[i]; }
    size_t size() const { return values.size(); }
};

struct RemoteResult {
    std::vector<std::string> columns;
    std::vector<RemoteRow> rows;
    std::string error;
    bool success = false;

    size_t row_count() const { return rows.size(); }
    size_t column_count() const { return columns.size(); }
    bool empty() const { return rows.empty(); }
};

inline RemoteResult parse_response(const std::string& json) {
    RemoteResult result;

    result.success = json.find("\"success\":true") != std::string::npos;

    if (!result.success) {
        auto pos = json.find("\"error\":\"");
        if (pos != std::string::npos) {
            pos += 9;
            result.error = json_unescape(json, pos);
        }
        return result;
    }

    // Parse columns
    auto cols_start = json.find("\"columns\":[");
    if (cols_start != std::string::npos) {
        cols_start += 11;
        while (cols_start < json.size() && json[cols_start] != ']') {
            if (json[cols_start] == '"') {
                cols_start++;
                result.columns.push_back(json_unescape(json, cols_start));
            }
            cols_start++;
        }
    }

    // Parse rows
    auto rows_start = json.find("\"rows\":[");
    if (rows_start != std::string::npos) {
        rows_start += 8;
        while (rows_start < json.size()) {
            if (json[rows_start] == ']' && (rows_start + 1 >= json.size() || json[rows_start + 1] != '[')) {
                break;
            }
            if (json[rows_start] == '[') {
                rows_start++;
                RemoteRow row;
                while (rows_start < json.size() && json[rows_start] != ']') {
                    if (json[rows_start] == '"') {
                        rows_start++;
                        row.values.push_back(json_unescape(json, rows_start));
                    }
                    rows_start++;
                }
                result.rows.push_back(std::move(row));
            }
            rows_start++;
        }
    }

    return result;
}

}  // namespace xsql::socket
