/**
 * test_protocol.cpp - Tests for xsql socket JSON protocol helpers
 */

#include <gtest/gtest.h>

#include <xsql/socket/protocol.hpp>

TEST(ProtocolTest, ExtractSqlAndToken) {
    std::string req = "{ \"token\": \"t\", \"sql\": \"SELECT 1\" }";
    EXPECT_EQ(xsql::socket::extract_sql_from_request(req), "SELECT 1");
    EXPECT_EQ(xsql::socket::extract_token_from_request(req), "t");
}

TEST(ProtocolTest, ExtractHandlesEscapesAndUnicode) {
    std::string req = "{\"sql\":\"A\\u00e9\\uD83D\\uDE00\\\\\\\"\\n\"}";
    std::string sql = xsql::socket::extract_sql_from_request(req);
    EXPECT_EQ(sql, std::string("A\xC3\xA9\xF0\x9F\x98\x80\\\"\n"));
}

TEST(ProtocolTest, ExtractRejectsInvalidJson) {
    EXPECT_EQ(xsql::socket::extract_sql_from_request("{\"sql\": \"x\""), "");
    EXPECT_EQ(xsql::socket::extract_sql_from_request("[\"sql\"]"), "");
}

TEST(ProtocolTest, ParseResponseSuccess) {
    std::string resp =
        "{\"success\":true,\"columns\":[\"a\",\"b\"],\"rows\":[[\"1\",\"2\"],[\"x\",\"y\"]],\"row_count\":2}";

    auto r = xsql::socket::parse_response(resp);
    ASSERT_TRUE(r.success) << r.error;
    ASSERT_EQ(r.columns.size(), 2u);
    EXPECT_EQ(r.columns[0], "a");
    EXPECT_EQ(r.columns[1], "b");
    ASSERT_EQ(r.rows.size(), 2u);
    ASSERT_EQ(r.rows[0].values.size(), 2u);
    EXPECT_EQ(r.rows[0].values[0], "1");
    EXPECT_EQ(r.rows[0].values[1], "2");
}

TEST(ProtocolTest, ParseResponseFailureWithExtraFields) {
    std::string resp = "{\"success\":false,\"error\":\"nope\",\"extra\":{\"a\":[1,true,null]}}";
    auto r = xsql::socket::parse_response(resp);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.error, "nope");
}

TEST(ProtocolTest, ParseResponseRejectsBadTypes) {
    auto r = xsql::socket::parse_response("{\"success\":true,\"columns\":[1]}");
    EXPECT_FALSE(r.success);
    EXPECT_FALSE(r.error.empty());
}
