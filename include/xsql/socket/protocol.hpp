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
#include <cstdio>

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

namespace detail {

inline bool is_ws(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

inline bool is_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

inline uint8_t hex_value(char c) {
    if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(10 + (c - 'a'));
    return static_cast<uint8_t>(10 + (c - 'A'));
}

inline bool append_utf8(std::string& out, uint32_t codepoint) {
    if (codepoint > 0x10FFFF) return false;

    if (codepoint <= 0x7F) {
        out.push_back(static_cast<char>(codepoint));
    } else if (codepoint <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else if (codepoint <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }

    return true;
}

inline bool unescape_json_string_body(const std::string& s, size_t& pos, std::string& out) {
    out.clear();
    while (pos < s.size() && s[pos] != '"') {
        char c = s[pos++];
        if (static_cast<unsigned char>(c) < 0x20) return false;

        if (c != '\\') {
            out.push_back(c);
            continue;
        }

        if (pos >= s.size()) return false;
        char e = s[pos++];
        switch (e) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            case 'u': {
                if (pos + 4 > s.size()) return false;
                uint32_t code = 0;
                for (int i = 0; i < 4; ++i) {
                    char h = s[pos + i];
                    if (!is_hex(h)) return false;
                    code = (code << 4) | hex_value(h);
                }
                pos += 4;

                if (code >= 0xD800 && code <= 0xDBFF) {
                    if (pos + 6 > s.size()) return false;
                    if (s[pos] != '\\' || s[pos + 1] != 'u') return false;
                    pos += 2;
                    uint32_t low = 0;
                    for (int i = 0; i < 4; ++i) {
                        char h = s[pos + i];
                        if (!is_hex(h)) return false;
                        low = (low << 4) | hex_value(h);
                    }
                    pos += 4;
                    if (low < 0xDC00 || low > 0xDFFF) return false;

                    code = 0x10000 + (((code - 0xD800) << 10) | (low - 0xDC00));
                } else if (code >= 0xDC00 && code <= 0xDFFF) {
                    return false;
                }

                if (!append_utf8(out, code)) return false;
                break;
            }
            default:
                return false;
        }
    }
    return true;
}

class JsonReader {
    const std::string& s_;
    size_t pos_ = 0;
    int depth_ = 0;

public:
    explicit JsonReader(const std::string& s) : s_(s) {}

    size_t pos() const { return pos_; }

    void skip_ws() {
        while (pos_ < s_.size() && is_ws(s_[pos_])) ++pos_;
    }

    bool consume(char c) {
        skip_ws();
        if (pos_ >= s_.size() || s_[pos_] != c) return false;
        ++pos_;
        return true;
    }

    bool expect(char c) { return consume(c); }

    bool begin_object() { return consume('{'); }
    bool begin_array() { return consume('['); }

    bool at_end() {
        skip_ws();
        return pos_ >= s_.size();
    }

    bool parse_string(std::string& out) {
        skip_ws();
        if (pos_ >= s_.size() || s_[pos_] != '"') return false;
        ++pos_;
        if (!unescape_json_string_body(s_, pos_, out)) return false;
        if (pos_ >= s_.size() || s_[pos_] != '"') return false;
        ++pos_;
        return true;
    }

    bool parse_bool(bool& out) {
        skip_ws();
        if (s_.compare(pos_, 4, "true") == 0) {
            pos_ += 4;
            out = true;
            return true;
        }
        if (s_.compare(pos_, 5, "false") == 0) {
            pos_ += 5;
            out = false;
            return true;
        }
        return false;
    }

    bool parse_null() {
        skip_ws();
        if (s_.compare(pos_, 4, "null") != 0) return false;
        pos_ += 4;
        return true;
    }

    bool parse_number() {
        skip_ws();
        if (pos_ >= s_.size()) return false;

        size_t start = pos_;
        if (s_[pos_] == '-') ++pos_;
        if (pos_ >= s_.size()) return false;

        if (s_[pos_] == '0') {
            ++pos_;
        } else if (s_[pos_] >= '1' && s_[pos_] <= '9') {
            while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
        } else {
            return false;
        }

        if (pos_ < s_.size() && s_[pos_] == '.') {
            ++pos_;
            if (pos_ >= s_.size() || s_[pos_] < '0' || s_[pos_] > '9') return false;
            while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
        }

        if (pos_ < s_.size() && (s_[pos_] == 'e' || s_[pos_] == 'E')) {
            ++pos_;
            if (pos_ < s_.size() && (s_[pos_] == '+' || s_[pos_] == '-')) ++pos_;
            if (pos_ >= s_.size() || s_[pos_] < '0' || s_[pos_] > '9') return false;
            while (pos_ < s_.size() && s_[pos_] >= '0' && s_[pos_] <= '9') ++pos_;
        }

        return pos_ > start;
    }

    bool skip_value() {
        skip_ws();
        if (pos_ >= s_.size()) return false;
        if (++depth_ > 64) return false;

        char c = s_[pos_];
        bool ok = false;
        if (c == '"') {
            std::string tmp;
            ok = parse_string(tmp);
        } else if (c == '{') {
            ok = skip_object();
        } else if (c == '[') {
            ok = skip_array();
        } else if (c == 't' || c == 'f') {
            bool b = false;
            ok = parse_bool(b);
        } else if (c == 'n') {
            ok = parse_null();
        } else if (c == '-' || (c >= '0' && c <= '9')) {
            ok = parse_number();
        }

        --depth_;
        return ok;
    }

private:
    bool skip_object() {
        if (!consume('{')) return false;
        skip_ws();
        if (consume('}')) return true;

        while (true) {
            std::string key;
            if (!parse_string(key)) return false;
            if (!expect(':')) return false;
            if (!skip_value()) return false;
            skip_ws();
            if (consume('}')) return true;
            if (!expect(',')) return false;
        }
    }

    bool skip_array() {
        if (!consume('[')) return false;
        skip_ws();
        if (consume(']')) return true;

        while (true) {
            if (!skip_value()) return false;
            skip_ws();
            if (consume(']')) return true;
            if (!expect(',')) return false;
        }
    }
};

inline bool extract_top_level_string_field(const std::string& json, const char* field, std::string& out) {
    out.clear();
    if (!field || !*field) return false;

    JsonReader r(json);
    if (!r.begin_object()) return false;

    r.skip_ws();
    if (r.consume('}')) return r.at_end();

    while (true) {
        std::string key;
        if (!r.parse_string(key) || !r.expect(':')) return false;

        if (key == field) {
            if (!r.parse_string(out)) return false;
        } else {
            if (!r.skip_value()) return false;
        }

        r.skip_ws();
        if (r.consume('}')) break;
        if (!r.expect(',')) return false;
    }

    return r.at_end();
}

}  // namespace detail

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
    if (!detail::unescape_json_string_body(s, pos, out)) return "";
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

inline std::string extract_string_field(const std::string& json, const char* field) {
    if (!field || !*field) return "";
    std::string value;
    if (!detail::extract_top_level_string_field(json, field, value)) return "";
    return value;
}

inline std::string extract_sql_from_request(const std::string& json) {
    return extract_string_field(json, "sql");
}

inline std::string extract_token_from_request(const std::string& json) {
    return extract_string_field(json, "token");
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

#if 0
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
#endif

inline RemoteResult parse_response(const std::string& json) {
    RemoteResult result;

    detail::JsonReader r(json);
    if (!r.begin_object()) {
        result.success = false;
        result.error = "Invalid JSON response";
        return result;
    }

    bool have_success = false;

    r.skip_ws();
    if (r.consume('}')) {
        result.success = false;
        result.error = "Invalid JSON response";
        return result;
    }

    while (true) {
        std::string key;
        if (!r.parse_string(key) || !r.expect(':')) {
            result.success = false;
            result.error = "Invalid JSON response";
            return result;
        }

        if (key == "success") {
            if (!r.parse_bool(result.success)) {
                result.success = false;
                result.error = "Invalid JSON response";
                return result;
            }
            have_success = true;
        } else if (key == "error") {
            if (!r.parse_string(result.error)) {
                result.success = false;
                result.error = "Invalid JSON response";
                return result;
            }
        } else if (key == "columns") {
            if (!r.begin_array()) {
                result.success = false;
                result.error = "Invalid JSON response";
                return result;
            }

            r.skip_ws();
            if (!r.consume(']')) {
                while (true) {
                    std::string col;
                    if (!r.parse_string(col)) {
                        result.success = false;
                        result.error = "Invalid JSON response";
                        return result;
                    }
                    result.columns.push_back(std::move(col));

                    r.skip_ws();
                    if (r.consume(']')) break;
                    if (!r.expect(',')) {
                        result.success = false;
                        result.error = "Invalid JSON response";
                        return result;
                    }
                }
            }
        } else if (key == "rows") {
            if (!r.begin_array()) {
                result.success = false;
                result.error = "Invalid JSON response";
                return result;
            }

            r.skip_ws();
            if (!r.consume(']')) {
                while (true) {
                    if (!r.begin_array()) {
                        result.success = false;
                        result.error = "Invalid JSON response";
                        return result;
                    }

                    RemoteRow row;
                    r.skip_ws();
                    if (!r.consume(']')) {
                        while (true) {
                            std::string v;
                            if (!r.parse_string(v)) {
                                result.success = false;
                                result.error = "Invalid JSON response";
                                return result;
                            }
                            row.values.push_back(std::move(v));

                            r.skip_ws();
                            if (r.consume(']')) break;
                            if (!r.expect(',')) {
                                result.success = false;
                                result.error = "Invalid JSON response";
                                return result;
                            }
                        }
                    }

                    result.rows.push_back(std::move(row));

                    r.skip_ws();
                    if (r.consume(']')) break;
                    if (!r.expect(',')) {
                        result.success = false;
                        result.error = "Invalid JSON response";
                        return result;
                    }
                }
            }
        } else {
            if (!r.skip_value()) {
                result.success = false;
                result.error = "Invalid JSON response";
                return result;
            }
        }

        r.skip_ws();
        if (r.consume('}')) break;
        if (!r.expect(',')) {
            result.success = false;
            result.error = "Invalid JSON response";
            return result;
        }
    }

    if (!have_success || !r.at_end()) {
        result.success = false;
        if (result.error.empty()) result.error = "Invalid JSON response";
        return result;
    }

    if (!result.success && result.error.empty()) {
        result.error = "Unknown error";
    }

    return result;
}

}  // namespace xsql::socket
