/**
 * test_vtable.cpp - Tests for the xsql virtual table framework
 */

#include <gtest/gtest.h>
#include <xsql/xsql.hpp>
#include <vector>
#include <string>
#include <atomic>

namespace {

class NeverEofIterator : public xsql::RowIterator {
    int current_ = -1;

public:
    bool next() override {
        ++current_;
        return current_ < 2;
    }

    bool eof() const override {
        return false;
    }

    void column(sqlite3_context* ctx, int col) override {
        if (current_ < 0 || current_ >= 2) {
            sqlite3_result_null(ctx);
            return;
        }

        switch (col) {
            case 0: sqlite3_result_int(ctx, 123); break;
            case 1: sqlite3_result_int(ctx, current_); break;
            default: sqlite3_result_null(ctx); break;
        }
    }

    int64_t rowid() const override {
        return current_;
    }
};

struct ProgressLimiter {
    int calls = 0;
    int max_calls = 0;
};

int progress_handler(void* p) {
    auto* limiter = static_cast<ProgressLimiter*>(p);
    limiter->calls++;
    return limiter->calls > limiter->max_calls ? 1 : 0;
}

std::vector<std::vector<std::string>> query_with_result_code(sqlite3* db, const std::string& sql, int& rc) {
    std::vector<std::vector<std::string>> results;

    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return results;

    int cols = sqlite3_column_count(stmt);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        std::vector<std::string> row;
        row.reserve(cols);
        for (int i = 0; i < cols; ++i) {
            const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
            row.push_back(text ? text : "");
        }
        results.push_back(std::move(row));
    }

    sqlite3_finalize(stmt);
    return results;
}

} // namespace

class VTableTest : public ::testing::Test {
protected:
    sqlite3* db_ = nullptr;

    void SetUp() override {
        int rc = sqlite3_open(":memory:", &db_);
        ASSERT_EQ(rc, SQLITE_OK) << "Failed to open SQLite database";
    }

    void TearDown() override {
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    // Helper to execute SQL and return results
    std::vector<std::vector<std::string>> query(const std::string& sql) {
        std::vector<std::vector<std::string>> results;
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) return results;

        int cols = sqlite3_column_count(stmt);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::vector<std::string> row;
            for (int i = 0; i < cols; ++i) {
                const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                row.push_back(text ? text : "");
            }
            results.push_back(row);
        }
        sqlite3_finalize(stmt);
        return results;
    }
};

TEST_F(VTableTest, CreateSimpleTable) {
    static std::vector<std::pair<int, std::string>> test_data = {
        {1, "one"},
        {2, "two"},
        {3, "three"}
    };

    auto test_table = xsql::table("test_table")
        .count([]() { return test_data.size(); })
        .column_int("id", [](size_t i) { return test_data[i].first; })
        .column_text("name", [](size_t i) { return test_data[i].second; })
        .build();

    EXPECT_TRUE(xsql::register_vtable(db_, "test_module", &test_table));
    EXPECT_TRUE(xsql::create_vtable(db_, "test", "test_module"));

    auto results = query("SELECT * FROM test");
    EXPECT_EQ(results.size(), 3);
}

TEST_F(VTableTest, ColumnTypesWork) {
    static std::vector<int64_t> numbers = {100, 200, 300};

    auto num_table = xsql::table("numbers")
        .count([]() { return numbers.size(); })
        .column_int64("value", [](size_t i) { return numbers[i]; })
        .column_int64("doubled", [](size_t i) { return numbers[i] * 2; })
        .build();

    xsql::register_vtable(db_, "num_module", &num_table);
    xsql::create_vtable(db_, "nums", "num_module");

    auto results = query("SELECT value, doubled FROM nums WHERE value = 200");
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0][0], "200");
    EXPECT_EQ(results[0][1], "400");
}

TEST_F(VTableTest, LimitWorks) {
    static std::vector<int> data(100);
    for (int i = 0; i < 100; i++) data[i] = i;

    auto large_table = xsql::table("large")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "large_module", &large_table);
    xsql::create_vtable(db_, "large", "large_module");

    auto results = query("SELECT * FROM large LIMIT 10");
    EXPECT_EQ(results.size(), 10);
}

TEST_F(VTableTest, OffsetWorks) {
    static std::vector<int> data(100);
    for (int i = 0; i < 100; i++) data[i] = i;

    auto table = xsql::table("offset_test")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "offset_module", &table);
    xsql::create_vtable(db_, "offset_test", "offset_module");

    auto results = query("SELECT n FROM offset_test LIMIT 5 OFFSET 10");
    ASSERT_EQ(results.size(), 5);
    EXPECT_EQ(results[0][0], "10");
    EXPECT_EQ(results[4][0], "14");
}

TEST_F(VTableTest, OrderByWorks) {
    static std::vector<std::pair<int, std::string>> data = {
        {3, "charlie"},
        {1, "alice"},
        {2, "bob"}
    };

    auto table = xsql::table("sort_test")
        .count([]() { return data.size(); })
        .column_int("id", [](size_t i) { return data[i].first; })
        .column_text("name", [](size_t i) { return data[i].second; })
        .build();

    xsql::register_vtable(db_, "sort_module", &table);
    xsql::create_vtable(db_, "sort_test", "sort_module");

    auto results = query("SELECT name FROM sort_test ORDER BY id ASC");
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0], "alice");
    EXPECT_EQ(results[1][0], "bob");
    EXPECT_EQ(results[2][0], "charlie");
}

TEST_F(VTableTest, AggregationWorks) {
    static std::vector<int> values = {10, 20, 30, 40, 50};

    auto table = xsql::table("agg_test")
        .count([]() { return values.size(); })
        .column_int("val", [](size_t i) { return values[i]; })
        .build();

    xsql::register_vtable(db_, "agg_module", &table);
    xsql::create_vtable(db_, "agg_test", "agg_module");

    auto sum_result = query("SELECT SUM(val) FROM agg_test");
    ASSERT_EQ(sum_result.size(), 1);
    EXPECT_EQ(sum_result[0][0], "150");

    auto count_result = query("SELECT COUNT(*) FROM agg_test");
    ASSERT_EQ(count_result.size(), 1);
    EXPECT_EQ(count_result[0][0], "5");
}

TEST_F(VTableTest, EmptyTableWorks) {
    static std::vector<int> empty_data;

    auto table = xsql::table("empty_test")
        .count([]() { return empty_data.size(); })
        .column_int("n", [](size_t i) { return empty_data[i]; })
        .build();

    xsql::register_vtable(db_, "empty_module", &table);
    xsql::create_vtable(db_, "empty_test", "empty_module");

    auto results = query("SELECT * FROM empty_test");
    EXPECT_EQ(results.size(), 0);
}

TEST_F(VTableTest, DoubleColumnWorks) {
    static std::vector<double> doubles = {1.5, 2.5, 3.5};

    auto table = xsql::table("double_test")
        .count([]() { return doubles.size(); })
        .column_double("val", [](size_t i) { return doubles[i]; })
        .build();

    xsql::register_vtable(db_, "double_module", &table);
    xsql::create_vtable(db_, "double_test", "double_module");

    auto results = query("SELECT val FROM double_test ORDER BY val");
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0], "1.5");
    EXPECT_EQ(results[1][0], "2.5");
    EXPECT_EQ(results[2][0], "3.5");
}

TEST_F(VTableTest, SchemaGeneration) {
    auto table = xsql::table("schema_test")
        .count([]() { return 0; })
        .column_int64("id", [](size_t) { return 0; })
        .column_text("name", [](size_t) { return ""; })
        .column_double("value", [](size_t) { return 0.0; })
        .build();

    std::string schema = table.schema();
    EXPECT_TRUE(schema.find("id INTEGER") != std::string::npos);
    EXPECT_TRUE(schema.find("name TEXT") != std::string::npos);
    EXPECT_TRUE(schema.find("value REAL") != std::string::npos);
}

TEST_F(VTableTest, RowCountCalledOncePerQuery) {
    static std::vector<int> data = {1, 2, 3};

    std::atomic<int> row_count_calls = 0;

    auto table = xsql::table("row_count_test")
        .count([&]() {
            row_count_calls++;
            return data.size();
        })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "row_count_module", &table);
    xsql::create_vtable(db_, "row_count_test", "row_count_module");

    auto results = query("SELECT * FROM row_count_test");
    ASSERT_EQ(results.size(), data.size());
    EXPECT_EQ(row_count_calls.load(), 1);
}

TEST_F(VTableTest, IteratorTerminationUsesNextReturnValue) {
    auto table = xsql::table("iter_test")
        .count([]() { return 0; })
        .column_int("a", [](size_t) { return 0; })
        .column_int("b", [](size_t) { return 0; })
        .filter_eq("a", [](int64_t) -> std::unique_ptr<xsql::RowIterator> {
            return std::make_unique<NeverEofIterator>();
        })
        .build();

    xsql::register_vtable(db_, "iter_module", &table);
    xsql::create_vtable(db_, "iter_test", "iter_module");

    ProgressLimiter limiter;
    limiter.max_calls = 10'000;
    sqlite3_progress_handler(db_, 1'000, progress_handler, &limiter);

    int rc = SQLITE_OK;
    auto results = query_with_result_code(db_, "SELECT a, b FROM iter_test WHERE a = 123", rc);

    sqlite3_progress_handler(db_, 0, nullptr, nullptr);

    ASSERT_EQ(rc, SQLITE_DONE) << "SQLite did not reach EOF (rc=" << rc << ")";
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0][0], "123");
    EXPECT_EQ(results[0][1], "0");
    EXPECT_EQ(results[1][0], "123");
    EXPECT_EQ(results[1][1], "1");
}

TEST_F(VTableTest, CachedIteratorTerminationUsesNextReturnValue) {
    auto table = xsql::cached_table<int>("cached_iter_test")
        .estimate_rows([]() { return 0; })
        .cache_builder([](std::vector<int>&) {})
        .column_int("a", [](const int&) { return 0; })
        .column_int("b", [](const int&) { return 0; })
        .filter_eq("a", [](int64_t) -> std::unique_ptr<xsql::RowIterator> {
            return std::make_unique<NeverEofIterator>();
        })
        .build();

    xsql::register_cached_vtable(db_, "cached_iter_module", &table);
    xsql::create_vtable(db_, "cached_iter_test", "cached_iter_module");

    ProgressLimiter limiter;
    limiter.max_calls = 10'000;
    sqlite3_progress_handler(db_, 1'000, progress_handler, &limiter);

    int rc = SQLITE_OK;
    auto results = query_with_result_code(db_, "SELECT a, b FROM cached_iter_test WHERE a = 123", rc);

    sqlite3_progress_handler(db_, 0, nullptr, nullptr);

    ASSERT_EQ(rc, SQLITE_DONE) << "SQLite did not reach EOF (rc=" << rc << ")";
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0][0], "123");
    EXPECT_EQ(results[0][1], "0");
    EXPECT_EQ(results[1][0], "123");
    EXPECT_EQ(results[1][1], "1");
}
