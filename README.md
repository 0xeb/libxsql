
# libxsql

**libxsql** is a modern C++17 header-only library that lets you expose your C++ data structures as high-performance SQLite virtual tables with minimal boilerplate.

- Effortlessly map C++ containers to SQL tables
- Build custom, writable, and streaming tables in just a few lines
- Ideal for CLI tools, scripting, and rapid data exploration

Turn your in-memory data into a full-featured SQL database—no dependencies, no hassle.

## Features

- **Fluent builder API** - Define tables in 20-50 lines instead of 250-400
- **Three table patterns** - Index-based, cached, and generator (streaming)
- **Writable tables** - UPDATE and DELETE support via column setters
- **Constraint pushdown** - O(1) lookups with `filter_eq()`
- **Socket protocol** - TCP server/client for remote queries
- **Zero dependencies** - Header-only, uses vendored SQLite

## Installation

### As Git Submodule

```bash
git submodule add https://github.com/0xeb/libxsql external/libxsql
```

```cmake
add_subdirectory(external/libxsql)
target_link_libraries(myapp PRIVATE xsql::xsql)
```

### Header-Only

Copy `include/xsql/` and `external/sqlite/` to your project.

## Quick Start

```cpp
#include <xsql/database.hpp>
#include <xsql/vtable.hpp>

// Data source
std::vector<std::pair<int, std::string>> items = {
    {1, "apple"}, {2, "banana"}, {3, "cherry"}
};

// Define table
auto def = xsql::table("items")
    .count([&]() { return items.size(); })
    .column_int("id", [&](size_t i) { return items[i].first; })
    .column_text("name", [&](size_t i) { return items[i].second; })
    .build();

// Use it
xsql::Database db;
db.open(":memory:");
db.register_and_create_table(def);

auto result = db.query("SELECT * FROM items WHERE id > 1");
for (const auto& row : result) {
    printf("%s: %s\n", row[0].c_str(), row[1].c_str());
}
```

## Table Patterns

### Index-Based Table

For data with direct index access. Row count is computed on each query.

```cpp
auto def = xsql::table("funcs")
    .count([&]() { return get_func_qty(); })
    .column_int64("ea", [&](size_t i) { return get_func(i)->start_ea; })
    .column_text("name", [&](size_t i) { return get_func_name(i); })
    .build();
```

### Cached Table

For data that requires enumeration. Cache is built per-query and freed when query completes.

```cpp
struct XrefInfo { uint64_t from; uint64_t to; };

auto def = xsql::cached_table<XrefInfo>("xrefs")
    .estimate_rows([]() { return 10000; })
    .cache_builder([](std::vector<XrefInfo>& cache) {
        // Enumerate and populate cache
        for (auto& xref : all_xrefs())
            cache.push_back({xref.from, xref.to});
    })
    .column_int64("from_ea", [](const XrefInfo& r) { return r.from; })
    .column_int64("to_ea", [](const XrefInfo& r) { return r.to; })
    .build();
```

### Generator Table

For expensive data sources where LIMIT should stop work early.

```cpp
struct DecompRow { uint64_t func_ea; std::string pseudocode; };

class DecompGenerator : public xsql::Generator<DecompRow> {
    size_t idx_ = 0;
    DecompRow current_;
public:
    bool next() override {
        if (idx_ >= get_func_qty()) return false;
        current_.func_ea = get_func(idx_)->start_ea;
        current_.pseudocode = decompile(idx_);  // Expensive!
        idx_++;
        return true;
    }
    const DecompRow& current() const override { return current_; }
    sqlite3_int64 rowid() const override { return idx_ - 1; }
};

auto def = xsql::generator_table<DecompRow>("decompiled")
    .estimate_rows([]() { return get_func_qty(); })
    .generator([]() { return std::make_unique<DecompGenerator>(); })
    .column_int64("func_ea", [](const DecompRow& r) { return r.func_ea; })
    .column_text("pseudocode", [](const DecompRow& r) { return r.pseudocode; })
    .build();
```

## Writable Tables

Support UPDATE and DELETE with column setters and `deletable()`.

```cpp
auto def = xsql::table("names")
    .count([&]() { return names.size(); })
    .on_modify([](const std::string& op) {
        // Called before any modification
        create_undo_point();
    })
    .column_int64("ea", [&](size_t i) { return names[i].ea; })
    .column_text_rw("name",
        [&](size_t i) { return names[i].name; },           // getter
        [&](size_t i, const char* v) {                     // setter
            names[i].name = v;
            return true;
        })
    .deletable([&](size_t i) {
        names.erase(names.begin() + i);
        return true;
    })
    .build();
```

```sql
UPDATE names SET name = 'new_name' WHERE ea = 0x401000;
DELETE FROM names WHERE ea = 0x402000;
```

## Constraint Pushdown

Optimize `WHERE column = value` queries with `filter_eq()`.

```cpp
// Custom iterator for efficient lookups
class XrefsToIterator : public xsql::RowIterator {
    uint64_t target_;
    xrefblk_t xref_;
    bool valid_ = false;
public:
    XrefsToIterator(int64_t target) : target_(target) {
        valid_ = xref_.first_to(target_, XREF_ALL);
    }
    bool next() override {
        if (!valid_) return false;
        bool had = valid_;
        valid_ = xref_.next_to();
        return had;
    }
    bool eof() const override { return !valid_; }
    void column(sqlite3_context* ctx, int col) override {
        if (col == 0) sqlite3_result_int64(ctx, xref_.from);
        else sqlite3_result_int64(ctx, target_);
    }
    int64_t rowid() const override { return xref_.from; }
};

auto def = xsql::cached_table<XrefInfo>("xrefs")
    .cache_builder([](std::vector<XrefInfo>& c) { /* ... */ })
    .column_int64("from_ea", [](const XrefInfo& r) { return r.from; })
    .column_int64("to_ea", [](const XrefInfo& r) { return r.to; })
    .filter_eq("to_ea", [](int64_t target) {
        return std::make_unique<XrefsToIterator>(target);
    }, 10.0, 5.0)  // cost=10, estimated_rows=5
    .build();
```

With this filter, `SELECT * FROM xrefs WHERE to_ea = 0x401000` uses the native xref API instead of scanning all rows.

## Socket Server/Client

Serve tables over TCP with length-prefixed JSON protocol.

### Server

```cpp
#include <xsql/socket/socket.hpp>

xsql::socket::Server server;
// Optional: require an auth token from clients
// xsql::socket::ServerConfig cfg;
// cfg.auth_token = "secret";
// server.set_config(cfg);

server.set_query_handler([&](const std::string& sql) {
    auto result = db.query(sql);
    xsql::socket::QueryResult qr;
    qr.success = result.ok();
    qr.error = result.error;
    qr.columns = result.columns;
    for (const auto& row : result) {
        qr.rows.push_back(row.values);
    }
    return qr;
});
server.run(12345);  // Blocking
```

### Client

```cpp
#include <xsql/socket/socket.hpp>

xsql::socket::Client client;
// Optional: if server requires a token
// client.set_auth_token("secret");
if (client.connect("localhost", 12345)) {
    auto result = client.query("SELECT * FROM items LIMIT 10");
    if (result.success) {
        for (const auto& row : result.rows) {
            printf("%s\n", row[0].c_str());
        }
    }
}
```

## API Reference

### Column Types

| Method | Type | Writable Version |
|--------|------|------------------|
| `column_int(name, getter)` | INTEGER | `column_int_rw(name, getter, setter)` |
| `column_int64(name, getter)` | INTEGER | `column_int64_rw(name, getter, setter)` |
| `column_text(name, getter)` | TEXT | `column_text_rw(name, getter, setter)` |
| `column_double(name, getter)` | REAL | - |
| `column_blob(name, getter)` | BLOB | - |

### Builder Methods

| Method | Description |
|--------|-------------|
| `count(fn)` | Row count function (required for index-based) |
| `estimate_rows(fn)` | Cheap row estimate for query planner |
| `cache_builder(fn)` | Populate cache (cached_table only) |
| `generator(fn)` | Generator factory (generator_table only) |
| `on_modify(fn)` | Hook called before UPDATE/DELETE |
| `deletable(fn)` | Enable DELETE support |
| `filter_eq(col, factory, cost, rows)` | Constraint pushdown for int64 |
| `filter_eq_text(col, factory, cost, rows)` | Constraint pushdown for text |

### Database Class

```cpp
xsql::Database db;
db.open(":memory:");                        // Open database
db.register_and_create_table(def);         // Register and create table
auto result = db.query("SELECT ...");       // Execute query
db.exec("UPDATE ...");                      // Execute statement
db.close();                                 // Close (automatic in destructor)
```

## Requirements

- C++17 or later
- SQLite 3.x (vendored in `external/sqlite/`)

## Author

**Elias Bachaalany** ([@0xeb](https://github.com/0xeb))

## License

MIT License - see [LICENSE](LICENSE) for details.
