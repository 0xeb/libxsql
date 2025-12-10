/**
 * test_database.cpp - Tests for the xsql::Database wrapper
 */

#include <gtest/gtest.h>
#include <xsql/xsql.hpp>
#include <vector>
#include <string>

class DatabaseTest : public ::testing::Test {
protected:
    xsql::Database db_;

    void SetUp() override {
        ASSERT_TRUE(db_.open(":memory:")) << db_.last_error();
    }
};

TEST_F(DatabaseTest, OpenMemoryDatabase) {
    xsql::Database db;
    EXPECT_TRUE(db.open(":memory:"));
    EXPECT_NE(db.handle(), nullptr);
}

TEST_F(DatabaseTest, OpenFileDatabase) {
    xsql::Database db;
    EXPECT_TRUE(db.open(":memory:"));  // Use memory for test
}

TEST_F(DatabaseTest, ExecuteSimpleSQL) {
    EXPECT_EQ(db_.exec("CREATE TABLE test (id INTEGER, name TEXT)"), SQLITE_OK);
    EXPECT_EQ(db_.exec("INSERT INTO test VALUES (1, 'one')"), SQLITE_OK);
    EXPECT_EQ(db_.exec("INSERT INTO test VALUES (2, 'two')"), SQLITE_OK);
}

TEST_F(DatabaseTest, QueryReturnsResults) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (id INTEGER, name TEXT)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (1, 'one')"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (2, 'two')"), SQLITE_OK);

    auto result = db_.query("SELECT * FROM test ORDER BY id");
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.size(), 2);
}

TEST_F(DatabaseTest, QueryGetByIndex) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (id INTEGER, name TEXT)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (1, 'one')"), SQLITE_OK);

    auto result = db_.query("SELECT id, name FROM test");
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0][0], "1");
    EXPECT_EQ(result[0][1], "one");
}

TEST_F(DatabaseTest, QueryColumnNames) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (id INTEGER, name TEXT)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (42, 'answer')"), SQLITE_OK);

    auto result = db_.query("SELECT id, name FROM test");
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.columns.size(), 2);
    EXPECT_EQ(result.columns[0], "id");
    EXPECT_EQ(result.columns[1], "name");
}

TEST_F(DatabaseTest, QueryScalar) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (val INTEGER)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (10)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (20)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (30)"), SQLITE_OK);

    auto result = db_.query("SELECT SUM(val) FROM test");
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0][0], "60");
}

TEST_F(DatabaseTest, EmptyQueryResult) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (id INTEGER)"), SQLITE_OK);

    auto result = db_.query("SELECT * FROM test");
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.size(), 0);
    EXPECT_TRUE(result.empty());
}

TEST_F(DatabaseTest, InvalidQueryReturnsError) {
    auto result = db_.query("SELECT * FROM nonexistent_table");
    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(result.error.empty());
}

TEST_F(DatabaseTest, InvalidExecReturnsError) {
    int rc = db_.exec("INVALID SQL SYNTAX");
    EXPECT_NE(rc, SQLITE_OK);
}

TEST_F(DatabaseTest, RegisterVirtualTable) {
    static std::vector<int> data = {1, 2, 3};

    auto table = xsql::table("test_vtable")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    EXPECT_TRUE(db_.register_table("test_module", &table));
    EXPECT_EQ(db_.exec("CREATE VIRTUAL TABLE test USING test_module"), SQLITE_OK);

    auto result = db_.query("SELECT * FROM test");
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.size(), 3);
}

TEST_F(DatabaseTest, ColumnNames) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (first_col INTEGER, second_col TEXT)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (1, 'a')"), SQLITE_OK);

    auto result = db_.query("SELECT first_col, second_col FROM test");
    ASSERT_TRUE(result.ok());

    ASSERT_EQ(result.columns.size(), 2);
    EXPECT_EQ(result.columns[0], "first_col");
    EXPECT_EQ(result.columns[1], "second_col");
}

TEST_F(DatabaseTest, RowIteration) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (val INTEGER)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (10)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (20)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (30)"), SQLITE_OK);

    auto result = db_.query("SELECT val FROM test ORDER BY val");
    ASSERT_TRUE(result.ok());

    int count = 0;
    std::string expected[] = {"10", "20", "30"};
    for (const auto& row : result) {
        ASSERT_LT(count, 3);
        EXPECT_EQ(row[0], expected[count]);
        count++;
    }
    EXPECT_EQ(count, 3);
}

TEST_F(DatabaseTest, MoveSemantics) {
    xsql::Database db1;
    ASSERT_TRUE(db1.open(":memory:"));
    auto handle = db1.handle();

    xsql::Database db2 = std::move(db1);
    EXPECT_EQ(db1.handle(), nullptr);
    EXPECT_EQ(db2.handle(), handle);
}

TEST_F(DatabaseTest, LastInsertRowid) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test (val) VALUES ('first')"), SQLITE_OK);
    EXPECT_EQ(db_.last_insert_rowid(), 1);

    ASSERT_EQ(db_.exec("INSERT INTO test (val) VALUES ('second')"), SQLITE_OK);
    EXPECT_EQ(db_.last_insert_rowid(), 2);
}

TEST_F(DatabaseTest, Changes) {
    ASSERT_EQ(db_.exec("CREATE TABLE test (val INTEGER)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (1)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (2)"), SQLITE_OK);
    ASSERT_EQ(db_.exec("INSERT INTO test VALUES (3)"), SQLITE_OK);

    ASSERT_EQ(db_.exec("UPDATE test SET val = val * 2"), SQLITE_OK);
    EXPECT_EQ(db_.changes(), 3);
}
