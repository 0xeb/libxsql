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

struct GenRow {
    int64_t key = 0;
    int64_t n = 0;
};

class RangeGenerator : public xsql::Generator<GenRow> {
    std::atomic<int>* next_calls_ = nullptr;
    int64_t current_ = -1;
    int64_t end_ = 0;
    mutable GenRow row_;

public:
    RangeGenerator(std::atomic<int>* next_calls, int64_t end)
        : next_calls_(next_calls), end_(end) {}

    bool next() override {
        next_calls_->fetch_add(1);
        ++current_;
        return current_ < end_;
    }

    const GenRow& current() const override {
        row_.key = current_;
        row_.n = current_;
        return row_;
    }

    sqlite3_int64 rowid() const override {
        return current_;
    }
};

class SingleRowIterator : public xsql::RowIterator {
    bool started_ = false;
    bool valid_ = false;
    int64_t key_ = 0;

public:
    explicit SingleRowIterator(int64_t key)
        : key_(key) {}

    bool next() override {
        if (!started_) {
            started_ = true;
            valid_ = true;
            return true;
        }
        valid_ = false;
        return false;
    }

    bool eof() const override {
        return started_ && !valid_;
    }

    void column(sqlite3_context* ctx, int col) override {
        if (!valid_) {
            sqlite3_result_null(ctx);
            return;
        }

        switch (col) {
            case 0: sqlite3_result_int64(ctx, key_); break;
            case 1: sqlite3_result_int64(ctx, key_); break;
            default: sqlite3_result_null(ctx); break;
        }
    }

    int64_t rowid() const override {
        return key_;
    }
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

TEST_F(VTableTest, GeneratorTableLimitStopsEarly) {
    std::atomic<int> next_calls = 0;
    std::atomic<int> factory_calls = 0;

    auto table = xsql::generator_table<GenRow>("gen_table")
        .estimate_rows([]() { return 1000; })
        .generator([&]() -> std::unique_ptr<xsql::Generator<GenRow>> {
            factory_calls.fetch_add(1);
            return std::make_unique<RangeGenerator>(&next_calls, 1000);
        })
        .column_int64("key", [](const GenRow& r) { return r.key; })
        .column_int64("n", [](const GenRow& r) { return r.n; })
        .build();

    EXPECT_TRUE(xsql::register_generator_vtable(db_, "gen_module", &table));
    EXPECT_TRUE(xsql::create_vtable(db_, "gen", "gen_module"));

    auto results = query("SELECT n FROM gen LIMIT 10");
    ASSERT_EQ(results.size(), 10);

    // xFilter positions once, then xNext advances per row; allow a small buffer.
    EXPECT_EQ(factory_calls.load(), 1);
    EXPECT_LE(next_calls.load(), 25);
}

TEST_F(VTableTest, GeneratorTableFiltersBypassGenerator) {
    std::atomic<int> next_calls = 0;
    std::atomic<int> factory_calls = 0;

    auto table = xsql::generator_table<GenRow>("gen_filter_table")
        .estimate_rows([]() { return 1000; })
        .generator([&]() -> std::unique_ptr<xsql::Generator<GenRow>> {
            factory_calls.fetch_add(1);
            return std::make_unique<RangeGenerator>(&next_calls, 1000);
        })
        .column_int64("key", [](const GenRow& r) { return r.key; })
        .column_int64("n", [](const GenRow& r) { return r.n; })
        .filter_eq("key", [](int64_t key) -> std::unique_ptr<xsql::RowIterator> {
            return std::make_unique<SingleRowIterator>(key);
        }, 1.0, 1.0)
        .build();

    EXPECT_TRUE(xsql::register_generator_vtable(db_, "gen_filter_module", &table));
    EXPECT_TRUE(xsql::create_vtable(db_, "gen_filter", "gen_filter_module"));

    auto results = query("SELECT key, n FROM gen_filter WHERE key = 42");
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0][0], "42");
    EXPECT_EQ(results[0][1], "42");

    // Filter path should not even create a generator.
    EXPECT_EQ(factory_calls.load(), 0);
    EXPECT_EQ(next_calls.load(), 0);
}

// ============================================================================
// CTE (Common Table Expression) Tests
// ============================================================================

TEST_F(VTableTest, SimpleCTE) {
    static std::vector<int> data = {1, 2, 3, 4, 5};

    auto table = xsql::table("cte_source")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "cte_source_module", &table);
    xsql::create_vtable(db_, "cte_source", "cte_source_module");

    auto results = query(
        "WITH doubled AS (SELECT n, n * 2 as n2 FROM cte_source) "
        "SELECT n, n2 FROM doubled WHERE n > 2"
    );
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0], "3");
    EXPECT_EQ(results[0][1], "6");
}

TEST_F(VTableTest, MultipleCTEs) {
    static std::vector<int> data = {10, 20, 30, 40, 50};

    auto table = xsql::table("multi_cte_source")
        .count([]() { return data.size(); })
        .column_int("val", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "multi_cte_module", &table);
    xsql::create_vtable(db_, "multi_cte_source", "multi_cte_module");

    auto results = query(
        "WITH "
        "  big AS (SELECT val FROM multi_cte_source WHERE val > 25), "
        "  small AS (SELECT val FROM multi_cte_source WHERE val <= 25) "
        "SELECT 'big' as type, COUNT(*) as cnt FROM big "
        "UNION ALL "
        "SELECT 'small', COUNT(*) FROM small"
    );
    ASSERT_EQ(results.size(), 2);
    // big: 30, 40, 50 = 3
    // small: 10, 20 = 2
}

TEST_F(VTableTest, RecursiveCTE) {
    // Test recursive CTE (generates numbers 1-10)
    auto results = query(
        "WITH RECURSIVE cnt(x) AS ("
        "  VALUES(1) "
        "  UNION ALL "
        "  SELECT x+1 FROM cnt WHERE x < 10"
        ") "
        "SELECT x FROM cnt"
    );
    ASSERT_EQ(results.size(), 10);
    EXPECT_EQ(results[0][0], "1");
    EXPECT_EQ(results[9][0], "10");
}

TEST_F(VTableTest, RecursiveCTEWithVTable) {
    static std::vector<std::pair<int, int>> edges = {
        {1, 2}, {2, 3}, {3, 4}, {1, 5}, {5, 6}
    };

    auto table = xsql::table("graph_edges")
        .count([]() { return edges.size(); })
        .column_int("from_node", [](size_t i) { return edges[i].first; })
        .column_int("to_node", [](size_t i) { return edges[i].second; })
        .build();

    xsql::register_vtable(db_, "graph_module", &table);
    xsql::create_vtable(db_, "graph_edges", "graph_module");

    // Find all nodes reachable from node 1
    auto results = query(
        "WITH RECURSIVE reachable(node, depth) AS ("
        "  SELECT 1, 0 "
        "  UNION "
        "  SELECT e.to_node, r.depth + 1 "
        "  FROM reachable r "
        "  JOIN graph_edges e ON e.from_node = r.node "
        "  WHERE r.depth < 5"
        ") "
        "SELECT DISTINCT node FROM reachable ORDER BY node"
    );
    ASSERT_GE(results.size(), 4);  // At least 1, 2, 3, 4 reachable
    EXPECT_EQ(results[0][0], "1");
}

// ============================================================================
// JOIN Tests
// ============================================================================

TEST_F(VTableTest, InnerJoinTwoTables) {
    static std::vector<std::pair<int, std::string>> users = {
        {1, "alice"}, {2, "bob"}, {3, "charlie"}
    };
    static std::vector<std::pair<int, int>> orders = {
        {1, 100}, {1, 200}, {2, 150}
    };

    auto users_table = xsql::table("users")
        .count([]() { return users.size(); })
        .column_int("id", [](size_t i) { return users[i].first; })
        .column_text("name", [](size_t i) { return users[i].second; })
        .build();

    auto orders_table = xsql::table("orders")
        .count([]() { return orders.size(); })
        .column_int("user_id", [](size_t i) { return orders[i].first; })
        .column_int("amount", [](size_t i) { return orders[i].second; })
        .build();

    xsql::register_vtable(db_, "users_module", &users_table);
    xsql::register_vtable(db_, "orders_module", &orders_table);
    xsql::create_vtable(db_, "users", "users_module");
    xsql::create_vtable(db_, "orders", "orders_module");

    auto results = query(
        "SELECT u.name, o.amount "
        "FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "ORDER BY u.name, o.amount"
    );
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0], "alice");
    EXPECT_EQ(results[0][1], "100");
}

TEST_F(VTableTest, LeftJoinWithNulls) {
    static std::vector<std::pair<int, std::string>> left_data = {
        {1, "a"}, {2, "b"}, {3, "c"}
    };
    static std::vector<std::pair<int, std::string>> right_data = {
        {1, "x"}, {3, "z"}
    };

    auto left_table = xsql::table("left_tbl")
        .count([]() { return left_data.size(); })
        .column_int("id", [](size_t i) { return left_data[i].first; })
        .column_text("val", [](size_t i) { return left_data[i].second; })
        .build();

    auto right_table = xsql::table("right_tbl")
        .count([]() { return right_data.size(); })
        .column_int("id", [](size_t i) { return right_data[i].first; })
        .column_text("val", [](size_t i) { return right_data[i].second; })
        .build();

    xsql::register_vtable(db_, "left_module", &left_table);
    xsql::register_vtable(db_, "right_module", &right_table);
    xsql::create_vtable(db_, "left_tbl", "left_module");
    xsql::create_vtable(db_, "right_tbl", "right_module");

    auto results = query(
        "SELECT l.id, l.val, r.val "
        "FROM left_tbl l "
        "LEFT JOIN right_tbl r ON l.id = r.id "
        "ORDER BY l.id"
    );
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0], "1");
    EXPECT_EQ(results[0][2], "x");
    EXPECT_EQ(results[1][0], "2");
    EXPECT_EQ(results[1][2], "");  // NULL becomes empty string
    EXPECT_EQ(results[2][0], "3");
    EXPECT_EQ(results[2][2], "z");
}

TEST_F(VTableTest, SelfJoin) {
    static std::vector<std::pair<int, int>> hierarchy = {
        {1, 0}, {2, 1}, {3, 1}, {4, 2}, {5, 2}
    };

    auto table = xsql::table("tree")
        .count([]() { return hierarchy.size(); })
        .column_int("id", [](size_t i) { return hierarchy[i].first; })
        .column_int("parent_id", [](size_t i) { return hierarchy[i].second; })
        .build();

    xsql::register_vtable(db_, "tree_module", &table);
    xsql::create_vtable(db_, "tree", "tree_module");

    // Find all children of node 2
    auto results = query(
        "SELECT child.id "
        "FROM tree parent "
        "JOIN tree child ON child.parent_id = parent.id "
        "WHERE parent.id = 2 "
        "ORDER BY child.id"
    );
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0][0], "4");
    EXPECT_EQ(results[1][0], "5");
}

TEST_F(VTableTest, ThreeTableJoin) {
    static std::vector<std::pair<int, std::string>> a_data = {{1, "a1"}, {2, "a2"}};
    static std::vector<std::pair<int, std::string>> b_data = {{1, "b1"}, {2, "b2"}};
    static std::vector<std::pair<int, std::string>> c_data = {{1, "c1"}, {2, "c2"}};

    auto a_table = xsql::table("tbl_a")
        .count([]() { return a_data.size(); })
        .column_int("id", [](size_t i) { return a_data[i].first; })
        .column_text("val", [](size_t i) { return a_data[i].second; })
        .build();

    auto b_table = xsql::table("tbl_b")
        .count([]() { return b_data.size(); })
        .column_int("id", [](size_t i) { return b_data[i].first; })
        .column_text("val", [](size_t i) { return b_data[i].second; })
        .build();

    auto c_table = xsql::table("tbl_c")
        .count([]() { return c_data.size(); })
        .column_int("id", [](size_t i) { return c_data[i].first; })
        .column_text("val", [](size_t i) { return c_data[i].second; })
        .build();

    xsql::register_vtable(db_, "a_module", &a_table);
    xsql::register_vtable(db_, "b_module", &b_table);
    xsql::register_vtable(db_, "c_module", &c_table);
    xsql::create_vtable(db_, "tbl_a", "a_module");
    xsql::create_vtable(db_, "tbl_b", "b_module");
    xsql::create_vtable(db_, "tbl_c", "c_module");

    auto results = query(
        "SELECT a.val, b.val, c.val "
        "FROM tbl_a a "
        "JOIN tbl_b b ON a.id = b.id "
        "JOIN tbl_c c ON b.id = c.id "
        "WHERE a.id = 1"
    );
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0][0], "a1");
    EXPECT_EQ(results[0][1], "b1");
    EXPECT_EQ(results[0][2], "c1");
}

// ============================================================================
// Edge Cases and Stress Tests
// ============================================================================

TEST_F(VTableTest, NullColumnValues) {
    static std::vector<std::pair<int, std::string>> data = {
        {1, "value"}, {2, ""}, {3, "another"}
    };

    auto table = xsql::table("nullable")
        .count([]() { return data.size(); })
        .column_int("id", [](size_t i) { return data[i].first; })
        .column_text("val", [](size_t i) { return data[i].second; })
        .build();

    xsql::register_vtable(db_, "nullable_module", &table);
    xsql::create_vtable(db_, "nullable", "nullable_module");

    auto results = query("SELECT id, val FROM nullable WHERE val = ''");
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0][0], "2");
}

TEST_F(VTableTest, LargeRowCount) {
    static constexpr size_t LARGE_SIZE = 10000;
    static std::vector<int64_t> large_data;
    if (large_data.empty()) {
        large_data.resize(LARGE_SIZE);
        for (size_t i = 0; i < LARGE_SIZE; ++i) large_data[i] = static_cast<int64_t>(i);
    }

    auto table = xsql::table("large_table")
        .count([]() { return large_data.size(); })
        .column_int64("n", [](size_t i) { return large_data[i]; })
        .build();

    xsql::register_vtable(db_, "large_table_module", &table);
    xsql::create_vtable(db_, "large_table", "large_table_module");

    auto count_result = query("SELECT COUNT(*) FROM large_table");
    ASSERT_EQ(count_result.size(), 1);
    EXPECT_EQ(count_result[0][0], "10000");

    auto sum_result = query("SELECT SUM(n) FROM large_table");
    ASSERT_EQ(sum_result.size(), 1);
    // Sum of 0..9999 = 9999*10000/2 = 49995000
    EXPECT_EQ(sum_result[0][0], "49995000");
}

TEST_F(VTableTest, SubqueryInWhere) {
    static std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    auto table = xsql::table("subq_source")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "subq_module", &table);
    xsql::create_vtable(db_, "subq_source", "subq_module");

    // Select values greater than average
    auto results = query(
        "SELECT n FROM subq_source "
        "WHERE n > (SELECT AVG(n) FROM subq_source) "
        "ORDER BY n"
    );
    ASSERT_EQ(results.size(), 5);  // 6, 7, 8, 9, 10 > 5.5
    EXPECT_EQ(results[0][0], "6");
}

TEST_F(VTableTest, SubqueryInSelect) {
    static std::vector<std::pair<int, std::string>> data = {
        {1, "a"}, {2, "b"}, {3, "c"}
    };

    auto table = xsql::table("subq_select")
        .count([]() { return data.size(); })
        .column_int("id", [](size_t i) { return data[i].first; })
        .column_text("val", [](size_t i) { return data[i].second; })
        .build();

    xsql::register_vtable(db_, "subq_select_module", &table);
    xsql::create_vtable(db_, "subq_select", "subq_select_module");

    auto results = query(
        "SELECT id, val, (SELECT MAX(id) FROM subq_select) as max_id "
        "FROM subq_select"
    );
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][2], "3");
    EXPECT_EQ(results[1][2], "3");
    EXPECT_EQ(results[2][2], "3");
}

TEST_F(VTableTest, GroupByHaving) {
    static std::vector<std::pair<std::string, int>> data = {
        {"a", 10}, {"a", 20}, {"b", 5}, {"b", 15}, {"b", 25}, {"c", 100}
    };

    auto table = xsql::table("group_test")
        .count([]() { return data.size(); })
        .column_text("category", [](size_t i) { return data[i].first; })
        .column_int("value", [](size_t i) { return data[i].second; })
        .build();

    xsql::register_vtable(db_, "group_module", &table);
    xsql::create_vtable(db_, "group_test", "group_module");

    auto results = query(
        "SELECT category, SUM(value) as total "
        "FROM group_test "
        "GROUP BY category "
        "HAVING COUNT(*) > 1 "
        "ORDER BY total DESC"
    );
    ASSERT_EQ(results.size(), 2);  // 'a' and 'b' have >1 rows
    EXPECT_EQ(results[0][0], "b");  // b has 45 total
    EXPECT_EQ(results[0][1], "45");
}

TEST_F(VTableTest, WindowFunction) {
    static std::vector<int> data = {10, 20, 30, 40, 50};

    auto table = xsql::table("window_test")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "window_module", &table);
    xsql::create_vtable(db_, "window_test", "window_module");

    auto results = query(
        "SELECT n, SUM(n) OVER (ORDER BY n) as running_sum "
        "FROM window_test "
        "ORDER BY n"
    );
    ASSERT_EQ(results.size(), 5);
    EXPECT_EQ(results[0][1], "10");   // 10
    EXPECT_EQ(results[1][1], "30");   // 10+20
    EXPECT_EQ(results[2][1], "60");   // 10+20+30
    EXPECT_EQ(results[3][1], "100");  // 10+20+30+40
    EXPECT_EQ(results[4][1], "150");  // 10+20+30+40+50
}

TEST_F(VTableTest, CaseExpression) {
    static std::vector<int> data = {5, 15, 25, 35, 45};

    auto table = xsql::table("case_test")
        .count([]() { return data.size(); })
        .column_int("n", [](size_t i) { return data[i]; })
        .build();

    xsql::register_vtable(db_, "case_module", &table);
    xsql::create_vtable(db_, "case_test", "case_module");

    auto results = query(
        "SELECT n, "
        "  CASE "
        "    WHEN n < 10 THEN 'small' "
        "    WHEN n < 30 THEN 'medium' "
        "    ELSE 'large' "
        "  END as size "
        "FROM case_test "
        "ORDER BY n"
    );
    ASSERT_EQ(results.size(), 5);
    EXPECT_EQ(results[0][1], "small");
    EXPECT_EQ(results[1][1], "medium");
    EXPECT_EQ(results[2][1], "medium");
    EXPECT_EQ(results[3][1], "large");
    EXPECT_EQ(results[4][1], "large");
}

TEST_F(VTableTest, CoalesceAndIfnull) {
    static std::vector<std::pair<int, std::string>> data = {
        {1, "value"}, {2, ""}, {3, "another"}
    };

    auto table = xsql::table("coalesce_test")
        .count([]() { return data.size(); })
        .column_int("id", [](size_t i) { return data[i].first; })
        .column_text("val", [](size_t i) { return data[i].second; })
        .build();

    xsql::register_vtable(db_, "coalesce_module", &table);
    xsql::create_vtable(db_, "coalesce_test", "coalesce_module");

    auto results = query(
        "SELECT id, COALESCE(NULLIF(val, ''), 'default') as result "
        "FROM coalesce_test "
        "ORDER BY id"
    );
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][1], "value");
    EXPECT_EQ(results[1][1], "default");  // empty string -> NULL -> default
    EXPECT_EQ(results[2][1], "another");
}
