/**
 * @file test_thinclient.cpp
 * @brief Tests for thin client server/client
 */

#include <gtest/gtest.h>

#ifdef XSQL_HAS_THINCLIENT

#include <xsql/thinclient/thinclient.hpp>
#include <xsql/database.hpp>
#include <thread>
#include <chrono>
#include <sstream>

using namespace xsql;
using namespace xsql::thinclient;

// Helper to convert Result to CSV
static std::string result_to_csv(const Result& result) {
    std::ostringstream oss;
    // Header
    for (size_t i = 0; i < result.columns.size(); ++i) {
        if (i > 0) oss << ",";
        oss << result.columns[i];
    }
    oss << "\n";
    // Rows
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) oss << ",";
            oss << row[i];
        }
        oss << "\n";
    }
    return oss.str();
}

// Global database for tests (shared across lambdas)
static Database* g_test_db = nullptr;

class ThinclientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create in-memory database with test table
        db_.open(":memory:");
        db_.exec("CREATE TABLE test (id INTEGER, name TEXT)");
        db_.exec("INSERT INTO test VALUES (1, 'alice')");
        db_.exec("INSERT INTO test VALUES (2, 'bob')");
        db_.exec("INSERT INTO test VALUES (3, 'charlie')");
        g_test_db = &db_;
    }

    void TearDown() override {
        g_test_db = nullptr;
        db_.close();
    }

    Database db_;
};

// Query handler that uses global db - returns CSV format
static std::string handle_query(const std::string& sql) {
    if (!g_test_db) {
        throw std::runtime_error("No database");
    }
    auto result = g_test_db->query(sql);
    if (!result.ok()) {
        throw std::runtime_error(result.error);
    }
    return result_to_csv(result);
}

// Setup routes using the new API
static void setup_test_routes(httplib::Server& svr) {
    svr.Post("/query", [](const httplib::Request& req, httplib::Response& res) {
        try {
            std::string result = handle_query(req.body);
            res.set_content(result, "text/csv");
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(std::string("Error: ") + e.what(), "text/plain");
        }
    });

    svr.Get("/status", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{\"status\": \"ok\"}", "application/json");
    });
}

TEST_F(ThinclientTest, ServerStartsAndStops) {
    server_config config;
    config.port = 18080;  // Use high port for tests
    config.setup_routes = setup_test_routes;

    server srv(config);
    srv.run_async();

    EXPECT_TRUE(srv.is_running());
    EXPECT_EQ(srv.port(), 18080);

    srv.stop();
    EXPECT_FALSE(srv.is_running());
}

TEST_F(ThinclientTest, ClientCanQuery) {
    server_config config;
    config.port = 18081;
    config.setup_routes = setup_test_routes;

    server srv(config);
    srv.run_async();

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    client_config client_cfg;
    client_cfg.port = 18081;
    client cli(client_cfg);

    std::string result = cli.query("SELECT COUNT(*) as cnt FROM test");
    EXPECT_NE(result.find("cnt"), std::string::npos);
    EXPECT_NE(result.find("3"), std::string::npos);

    srv.stop();
}

TEST_F(ThinclientTest, ClientHandlesError) {
    server_config config;
    config.port = 18082;
    config.setup_routes = setup_test_routes;

    server srv(config);
    srv.run_async();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    client_config client_cfg;
    client_cfg.port = 18082;
    client cli(client_cfg);

    EXPECT_THROW(cli.query("SELECT * FROM nonexistent"), std::runtime_error);

    srv.stop();
}

TEST_F(ThinclientTest, ServerStatus) {
    server_config config;
    config.port = 18083;
    config.setup_routes = [](httplib::Server& svr) {
        svr.Get("/status", [](const httplib::Request&, httplib::Response& res) {
            res.set_content("{\"status\": \"test\"}", "application/json");
        });
    };

    server srv(config);
    srv.run_async();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    client_config client_cfg;
    client_cfg.port = 18083;
    client cli(client_cfg);

    std::string status = cli.status();
    EXPECT_NE(status.find("test"), std::string::npos);

    srv.stop();
}

TEST_F(ThinclientTest, ClientPing) {
    server_config config;
    config.port = 18084;
    config.setup_routes = [](httplib::Server& svr) {
        svr.Get("/status", [](const httplib::Request&, httplib::Response& res) {
            res.set_content("OK", "text/plain");
        });
    };

    server srv(config);
    srv.run_async();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    client_config client_cfg;
    client_cfg.port = 18084;
    client cli(client_cfg);

    EXPECT_TRUE(cli.ping());

    srv.stop();

    // After stop, ping should fail
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(cli.ping());
}

// ============================================================================
// CLI Parser Tests
// ============================================================================

TEST(CliParserTest, ParseDirectMode) {
    const char* argv[] = {"test", "-s", "db.i64", "-c", "SELECT 1"};
    auto args = parse_args(5, const_cast<char**>(argv), "test", "Test");

    ASSERT_TRUE(args.has_value());
    EXPECT_EQ(args->mode, cli_mode::direct);
    EXPECT_EQ(args->database, "db.i64");
    EXPECT_EQ(args->query, "SELECT 1");
}

TEST(CliParserTest, ParseServeMode) {
    const char* argv[] = {"test", "-s", "db.i64", "--serve", "--port", "8080"};
    auto args = parse_args(6, const_cast<char**>(argv), "test", "Test");

    ASSERT_TRUE(args.has_value());
    EXPECT_EQ(args->mode, cli_mode::serve);
    EXPECT_EQ(args->database, "db.i64");
    EXPECT_EQ(args->port, 8080);
    EXPECT_TRUE(args->serve);
}

TEST(CliParserTest, ParseClientMode) {
    const char* argv[] = {"test", "--port", "8080", "-c", "SELECT 1"};
    auto args = parse_args(5, const_cast<char**>(argv), "test", "Test");

    ASSERT_TRUE(args.has_value());
    EXPECT_EQ(args->mode, cli_mode::client);
    EXPECT_EQ(args->port, 8080);
    EXPECT_EQ(args->query, "SELECT 1");
}

TEST(CliParserTest, ParseFileOption) {
    const char* argv[] = {"test", "-s", "db.i64", "-f", "query.sql"};
    auto args = parse_args(5, const_cast<char**>(argv), "test", "Test");

    ASSERT_TRUE(args.has_value());
    EXPECT_EQ(args->query_file, "query.sql");
}

TEST(CliParserTest, HelpReturnsNullopt) {
    const char* argv[] = {"test", "--help"};
    auto args = parse_args(2, const_cast<char**>(argv), "test", "Test");

    EXPECT_FALSE(args.has_value());
}

// ============================================================================
// JSON Helpers Tests
// ============================================================================

#include <xsql/thinclient/json_helpers.hpp>

TEST(JsonHelpersTest, JsonEscapeBasic) {
    using xsql::thinclient::json_escape;

    EXPECT_EQ(json_escape("hello"), "hello");
    EXPECT_EQ(json_escape(""), "");
    EXPECT_EQ(json_escape("hello world"), "hello world");
}

TEST(JsonHelpersTest, JsonEscapeQuotes) {
    using xsql::thinclient::json_escape;

    EXPECT_EQ(json_escape("say \"hello\""), "say \\\"hello\\\"");
    EXPECT_EQ(json_escape("path\\to\\file"), "path\\\\to\\\\file");
}

TEST(JsonHelpersTest, JsonEscapeControlChars) {
    using xsql::thinclient::json_escape;

    EXPECT_EQ(json_escape("line1\nline2"), "line1\\nline2");
    EXPECT_EQ(json_escape("col1\tcol2"), "col1\\tcol2");
    EXPECT_EQ(json_escape("text\r\n"), "text\\r\\n");
}

TEST(JsonHelpersTest, JsonEscapeLowAscii) {
    using xsql::thinclient::json_escape;

    // Control characters < 0x20 should be escaped as \uXXXX
    std::string input = "a";
    input += '\x01';  // SOH
    input += "b";
    std::string expected = "a\\u0001b";
    EXPECT_EQ(json_escape(input), expected);
}

TEST(JsonHelpersTest, MakeErrorJson) {
    using xsql::thinclient::make_error_json;

    EXPECT_EQ(make_error_json("not found"),
              R"({"success":false,"error":"not found"})");
    // Test with quotes in message - escaped quotes become \" in JSON
    EXPECT_EQ(make_error_json("query \"failed\""),
              "{\"success\":false,\"error\":\"query \\\"failed\\\"\"}");
}

TEST(JsonHelpersTest, MakeSuccessJson) {
    using xsql::thinclient::make_success_json;

    EXPECT_EQ(make_success_json(), R"({"success":true})");
    EXPECT_EQ(make_success_json("done"),
              R"({"success":true,"message":"done"})");
}

TEST(JsonHelpersTest, MakeStatusJson) {
    using xsql::thinclient::make_status_json;

    EXPECT_EQ(make_status_json("bnsql"),
              R"({"success":true,"status":"ok","tool":"bnsql"})");
    EXPECT_EQ(make_status_json("idasql", "\"functions\":42"),
              R"({"success":true,"status":"ok","tool":"idasql","functions":42})");
}

// Mock result type for testing result_to_json
struct MockResult {
    bool success = true;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
    std::string error;
};

TEST(JsonHelpersTest, ResultToJsonSuccess) {
    using xsql::thinclient::result_to_json;

    MockResult result;
    result.success = true;
    result.columns = {"id", "name"};
    result.rows = {{"1", "alice"}, {"2", "bob"}};

    std::string json = result_to_json(result);

    EXPECT_NE(json.find("\"success\":true"), std::string::npos);
    EXPECT_NE(json.find("\"columns\":[\"id\",\"name\"]"), std::string::npos);
    EXPECT_NE(json.find("\"row_count\":2"), std::string::npos);
    EXPECT_NE(json.find("\"alice\""), std::string::npos);
    EXPECT_NE(json.find("\"bob\""), std::string::npos);
}

TEST(JsonHelpersTest, ResultToJsonError) {
    using xsql::thinclient::result_to_json;

    MockResult result;
    result.success = false;
    result.error = "no such table";

    std::string json = result_to_json(result);

    EXPECT_NE(json.find("\"success\":false"), std::string::npos);
    EXPECT_NE(json.find("\"error\":\"no such table\""), std::string::npos);
}

TEST(JsonHelpersTest, ResultToJsonEmpty) {
    using xsql::thinclient::result_to_json;

    MockResult result;
    result.success = true;
    result.columns = {"count"};
    result.rows = {};

    std::string json = result_to_json(result);

    EXPECT_NE(json.find("\"success\":true"), std::string::npos);
    EXPECT_NE(json.find("\"rows\":[]"), std::string::npos);
    EXPECT_NE(json.find("\"row_count\":0"), std::string::npos);
}

#else  // !XSQL_HAS_THINCLIENT

TEST(ThinclientTest, Disabled) {
    GTEST_SKIP() << "Thin client not enabled (XSQL_WITH_THINCLIENT=OFF)";
}

#endif  // XSQL_HAS_THINCLIENT
