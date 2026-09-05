# libxsql

> **License Notice:** This is source-available software, not open-source software.
> By using, building, distributing, or contributing to this repository, you are relying on the [Human-Origin Source License v1.0](LICENSE).
> Unmodified dependency use is allowed, including commercial use.
> Forks, ports, rebrands, derivative implementations, and AI-assisted implementation mining require prior written permission.

**libxsql** is a modern C++17 library that exposes C++ data structures as
SQLite virtual tables. The virtual-table framework, HTTP thinclient, runtime
settings, and graph utilities are header-based; the database, statement,
function, aggregate, and script layers are compiled sources. Note that the
virtual-table headers call into the compiled layer (the prepare-time
write-surface authorizer), so every consumer links the `xsql` library — the
headers are not usable stand-alone.

SQL is the universal query language. By exposing your application's data as SQL tables, you make it instantly accessible to scripts, CLI pipelines, and AI coding agents. No proprietary API to learn. No SDK to integrate. Just SQL.

Build a CLI tool with libxsql, and any agent (Claude Code, Codex, Copilot) can query your application's internals with zero additional work.

> **Rust port:** [libxsql-rs](https://github.com/0xeb/libxsql-rs) — the same fluent virtual-table API, idiomatic Rust.

## License and Terms of Use

In short: you may read, build, evaluate, benchmark, package, and use unmodified libxsql as a dependency, including commercially, if you preserve notices and follow the license terms. You may fork or patch it to prepare bug fixes, optimizations, features, tests, or documentation improvements for contribution back within the license's contribution-purpose rules.

You may not maintain a divergent private fork, port, rebrand, clone, API-compatible replacement, competing implementation, or use libxsql as AI input to recreate or improve a derivative implementation without prior written permission from Elias Bachaalany. Independent implementations that are not copied from, materially derived from, or substantially informed by libxsql in the license's defined sense are not prohibited.

Permission requests: open a GitHub issue at [0xeb/libxsql/issues](https://github.com/0xeb/libxsql/issues).

If libxsql materially informs a distributed project, preserve the human origin: credit libxsql and Elias Bachaalany visibly in your README/docs and in About/credits UI when applicable. The license includes an examples/FAQ section for common allowed and permission-required uses. Vendored dependencies under `external/` remain under their own licenses; see [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

## Features

- **Fluent builder API** - Define tables in 20-50 lines instead of 250-400
- **Three table patterns** - Index-based, cached, and generator (streaming)
- **Writable tables** - UPDATE and DELETE support via column setters
- **Constraint pushdown** - O(1) lookups with `filter_eq()`
- **HTTP thinclient** - HTTP server/client for remote queries (JSON, JSONL,
  text, CSV, TSV output; cooperative cancellation via `POST /cancel`)
- **Transactional runtime settings** - a writable `runtime_settings` table
- **Capabilities table** - a self-describing `sql_capabilities` surface
- **Graph utilities** - dominators, natural loops, SCC, topological order
- **No external dependencies** - SQLite, cpp-httplib, and nlohmann-json are
  vendored under `external/`

## Installation

### As Git Submodule

```bash
git submodule add https://github.com/0xeb/libxsql external/libxsql
```

```cmake
add_subdirectory(external/libxsql)
target_link_libraries(myapp PRIVATE xsql::xsql)
```

### Vendored Copy

Copy `include/xsql/`, `src/`, `external/sqlite/`, and `external/nlohmann/` to
your project and add the `src/*.cpp` files (plus the SQLite amalgamation) to
your build; the script/serializer layer includes the vendored nlohmann-json
headers. Add `external/httplib/` as well if you use the HTTP thinclient. The
virtual-table framework and thinclient are header-based, but they call into
the compiled `Database`/`Statement`/function-registration layer, so the
`src/*.cpp` files are always required.

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
    .row_count([&]() { return items.size(); })
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
    .row_count([&]() { return get_func_qty(); })
    .column_int64("addr", [&](size_t i) { return get_func(i)->start_ea; })
    .column_text("name", [&](size_t i) { return get_func_name(i); })
    .build();
```

### Cached Table

For data that requires enumeration. Cache is built per-query and freed when query completes.

```cpp
struct XrefInfo { uint64_t from; uint64_t to; };

auto def = xsql::cached_table<XrefInfo>("xrefs")
    .estimate_rows([]() { return 10000; })
    .row_count([]() { return exact_xref_count(); })  // Optional: optimizes COUNT(*)
    .cache_builder([](std::vector<XrefInfo>& cache) {
        // Enumerate and populate cache
        for (auto& xref : all_xrefs())
            cache.push_back({xref.from, xref.to});
    })
    .column_int64("from_addr", [](const XrefInfo& r) { return r.from; })
    .column_int64("to_addr", [](const XrefInfo& r) { return r.to; })
    .build();
```

### Generator Table

For expensive data sources where LIMIT should stop work early.

```cpp
struct DecompRow { uint64_t func_addr; std::string pseudocode; };

class DecompGenerator : public xsql::Generator<DecompRow> {
    size_t idx_ = 0;
    DecompRow current_;
public:
    bool next() override {
        if (idx_ >= get_func_qty()) return false;
        current_.func_addr = get_func(idx_)->start_ea;
        current_.pseudocode = decompile(idx_);  // Expensive!
        idx_++;
        return true;
    }
    const DecompRow& current() const override { return current_; }
    int64_t rowid() const override { return idx_ - 1; }
};

auto def = xsql::generator_table<DecompRow>("decompiled")
    .estimate_rows([]() { return get_func_qty(); })
    .generator([]() { return std::make_unique<DecompGenerator>(); })
    .column_int64("func_addr", [](const DecompRow& r) { return r.func_addr; })
    .column_text("pseudocode", [](const DecompRow& r) { return r.pseudocode; })
    .build();
```

Generator tables also support UPDATE (via `*_rw` column setters) and DELETE (via
`deletable()`); both resolve the target row through `row_lookup()`, so set a
`row_lookup()` when enabling either.

## Writable Tables

Support UPDATE and DELETE with column setters and `deletable()`.

```cpp
auto def = xsql::table("names")
    .row_count([&]() { return names.size(); })
    .on_modify([](const std::string& op) {
        // Called before any modification
        create_undo_point();
    })
    .column_int64("addr", [&](size_t i) { return names[i].addr; })
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
UPDATE names SET name = 'new_name' WHERE addr = 0x401000;
DELETE FROM names WHERE addr = 0x402000;
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
    void column(xsql::FunctionContext& ctx, int col) override {
        if (col == 0) ctx.result_int64(xref_.from);
        else ctx.result_int64(target_);
    }
    int64_t rowid() const override { return xref_.from; }
};

auto def = xsql::cached_table<XrefInfo>("xrefs")
    .cache_builder([](std::vector<XrefInfo>& c) { /* ... */ })
    .column_int64("from_addr", [](const XrefInfo& r) { return r.from; })
    .column_int64("to_addr", [](const XrefInfo& r) { return r.to; })
    .filter_eq("to_addr", [](int64_t target) {
        return std::make_unique<XrefsToIterator>(target);
    }, 10.0, 5.0)  // cost=10, estimated_rows=5
    .build();
```

With this filter, `SELECT * FROM xrefs WHERE to_addr = 0x401000` uses the native xref API instead of scanning all rows.

## Runtime Settings

Runtime configuration is exposed through the writable `runtime_settings` virtual
table (`key`, `value`, `type`, `scope`). Read a setting with
`SELECT value FROM runtime_settings WHERE key = ?` and change it with
`UPDATE runtime_settings SET value = ? WHERE key = ?`. Multiple
updates in one SQL transaction commit atomically and roll back together.
Only the `value` column is writable; `INSERT` and `DELETE` are rejected
(the key set is fixed by registration), as are `NULL` and empty values.

Common keys shipped by `RuntimeSettingsCore` (products register their own on
the same core with `register_bool_setting` / `register_integer_setting` /
`register_string_setting`; string values are capped at 4 KiB):

| Key | Type | Default | Range | Writable |
|-----|------|---------|-------|----------|
| `query_timeout_ms` | int | 60000 | 0 – 3600000 | yes |
| `queue_admission_timeout_ms` | int | 120000 | 0 – 3600000 | yes |
| `max_queue` | int | 64 | 0 – 10000 | yes |
| `hints_enabled` | bool | 1 | – | yes |
| `timeout_stack_depth` | int | live stack depth | – | no (read-only) |
| `max_timeout_stack_depth` | int | 64 | – | no (read-only) |

`scope` is `common` for the shared keys, the product prefix (e.g. `idasql`)
for product keys, and `action` for the two imperative verbs below.

Older value-bearing `PRAGMA` commands are retired; reads and writes go through
`runtime_settings`, and native SQLite PRAGMAs are reserved for SQLite itself.
The single sanctioned exception is the imperative timeout stack —
`PRAGMA <prefix>.timeout_push = <ms>` / `PRAGMA <prefix>.timeout_pop` — which
is a verb, not a value, and therefore stays a PRAGMA. A staged
`query_timeout_ms` write and an active timeout stack are mutually exclusive
(the error message names the conflict and how to clear it).

## Scripts, Streaming, Timeouts, and Cancellation

`run_database_script(db, script, options)` executes a multi-statement SQL
script and returns one aggregate `ScriptResult` (per-statement rows, errors,
timing). `ScriptOptions` carries:

- `timeout_ms` — per-statement wall-clock deadline (0 = none).
- `should_cancel` — a cooperative cancellation predicate polled between rows
  and via SQLite's progress handler; once it fires, the rest of the script is
  cancelled too.
- `continue_on_error`, `include_sql` — aggregation controls.

Semantics on cancel/timeout: a read-only statement keeps rows already gathered
(`partial=true` plus a warning) — but only when at least one row landed; with
zero rows it is an error (`partial` is never set on an empty result, so an
empty successful result is always complete). A mutation — including
`DML ... RETURNING` — is aborted through `sqlite3_interrupt`, so SQLite rolls
the statement back, and reports an error; if it completes before the abort
lands, it has committed and reports its honest result.

Serializers over a `ScriptResult`: `script_result_to_json` (canonical
envelope), `script_result_to_jsonl` (one keyed JSON object per row — NDJSON
"records"), `script_result_to_text`, `_to_csv`, `_to_tsv`.

Streaming twins with O(one row) memory for reads:
`stream_database_script_json` and `stream_database_script_ndjson` emit through
a sink callback (pair them with a chunked HTTP response); mutation RETURNING
rows stay buffered until the statement succeeds so rolled-back rows never
reach the sink.

The HTTP server exposes cancellation as `POST /cancel` (whole-script executor
configurations only): it cooperatively cancels the queries whose execution
started before the request arrived — queued-but-unstarted work is not
affected.

Adapters that can block inside a downstream engine call may set the server
configuration's `cancel_fn`. The server advances its own cancel epoch before
invoking that hook, allowing the adapter to forward an out-of-band cancellation
request while the normal query channel is occupied.

With curl, frame the empty POST body explicitly:

```bash
curl -X POST -d '' http://127.0.0.1:8080/cancel
```

`-d ''` sends `Content-Length: 0`. A bare `curl -X POST` can leave the request
body unframed, causing the server to wait for a body boundary before route
dispatch. Buffered queries require this explicit cancellation request (or a
timeout); only the opt-in streaming path can notice a client disconnect while
the query is still producing rows.

## Capabilities Table

`capabilities.hpp` provides the one canonical capability surface used across
the family: `define_sql_capabilities(rows)` builds a read-only
`sql_capabilities(name TEXT, is_supported INTEGER, notes TEXT)` table a tool
registers so agents can discover what the surface supports (by convention the
`mutation.<table>.<leaf>` names that write-denial errors also cite) with
`SELECT * FROM sql_capabilities`. Rows are sorted for deterministic output;
duplicate or empty names fail at definition time.

## Graph Utilities

`#include <xsql/graph.hpp>` (also part of the `xsql.hpp` umbrella) is a stock
directed-graph algorithm library over opaque integer node ids in
`[0, node_count)` — no reverse-engineering concepts; map your own objects onto
ids. `DirectedGraph` stores the edges (`add_edge` validates ids and permits
parallel edges and self-loops); the algorithms are free functions:

| Function | Result |
|----------|--------|
| `immediate_dominators(g, entry)` | idom per node (`kNoNode` if unreachable) |
| `dominator_sets(g, entry)` | full sorted dominator sets — O(n²) output; prefer `immediate_dominators` at scale |
| `immediate_post_dominators(g)` | ipdom relative to the graph's sinks via a virtual exit |
| `natural_loops(g, entry)` | back-edge natural loops (header, latch, sorted body); parallel back edges deduped |
| `strongly_connected_components(g)` | Tarjan SCC component ids |
| `topological_order(g)` | Kahn order (smallest-id-first), `std::nullopt` on a cycle |

## HTTP Thinclient

Serve tables over HTTP with `xsql::thinclient::server` and query them with standard HTTP clients.

### Server

```cpp
#include <xsql/thinclient/server.hpp>

xsql::thinclient::server_config cfg;
cfg.port = 8081;
cfg.bind_address = "127.0.0.1";
cfg.auth_token = "secret";
cfg.setup_routes = [&](httplib::Server& svr) {
    svr.Post("/query", [&](const httplib::Request& req, httplib::Response& res) {
        auto result = db.query(req.body);
        res.status = result.ok() ? 200 : 400;
        res.set_content(result.to_string(), "text/plain");
    });
};

xsql::thinclient::server server(cfg);
server.run();
```

### Client

```cpp
#include <xsql/thinclient/client.hpp>

xsql::thinclient::client_config cfg;
cfg.host = "127.0.0.1";
cfg.port = 8081;
cfg.auth_token = "secret";
xsql::thinclient::client client(cfg);
std::string result = client.query("SELECT * FROM items LIMIT 10");
printf("%s\n", result.c_str());
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
| `row_count(fn)` | Exact row count (required for index-based; optional `COUNT(*)` fast path for cached/generator tables) |
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

## CLI Tools and AI Agents

libxsql is designed for building CLI tools that AI coding agents can query directly.

### The Pattern

1. Your application exposes data as virtual tables
2. A thin client (or curl) sends SQL over HTTP
3. Agents invoke the CLI and parse results

```
┌─────────────────┐      SQL over HTTP    ┌─────────────┐
│  Your App       │◄────────────────────►│  CLI Client │
│  (libxsql)      │                      │  (thin)     │
│                 │                      └──────┬──────┘
│  - funcs table  │                             │
│  - strings table│                             ▼
│  - xrefs table  │                      ┌─────────────┐
└─────────────────┘                      │  AI Agent   │
                                         │  (invokes   │
                                         │   CLI)      │
                                         └─────────────┘
```

### Why This Works

- **SQL is universal** - Every agent understands SQL. No tool definitions needed.
- **Self-describing** - `SELECT * FROM sqlite_master` lists available tables.
- **Composable** - Agents can JOIN, filter, aggregate without learning your API.
- **Portable** - Same queries work in scripts, notebooks, CI pipelines.

### Example: Reverse Engineering Tool

```cpp
// Expose IDA-like data structures
auto funcs = xsql::table("funcs")
    .row_count([&]() { return functions.size(); })
    .column_text("name", [&](size_t i) { return functions[i].name; })
    .column_int64("addr", [&](size_t i) { return functions[i].addr; })
    .column_int("size", [&](size_t i) { return functions[i].size; })
    .build();

// Start HTTP server
xsql::thinclient::server_config cfg;
cfg.port = 8081;
cfg.setup_routes = [&](httplib::Server& svr) { /* ... */ };
xsql::thinclient::server server(cfg);
server.run();
```

Now an agent can:

```bash
# Find largest functions
curl -X POST http://localhost:8081/query -d "SELECT name, size FROM funcs ORDER BY size DESC LIMIT 10"

# Search for patterns
curl -X POST http://localhost:8081/query -d "SELECT * FROM strings WHERE content LIKE '%password%'"
```

The agent writes SQL. Your tool executes it. No glue code required.

## Requirements

- C++17 or later
- SQLite 3.x (vendored in `external/sqlite/`)

## Author

**Elias Bachaalany** ([@0xeb](https://github.com/0xeb))

## License

First-party libxsql material is source-available under the [Human-Origin Source License v1.0](LICENSE). This custom license is derived in structure from MPL-2.0, but it adds binding human-origin attribution, provenance, contribution-back, no-divergent-fork, and no-AI-cloning terms. It is not MPL-only and is not plain OSI-open-source.

For additional permissions, including commercial modification, redistribution beyond the public grant, derivative licensing, private/internal modification rights, long-term maintenance forks, ports, derivative implementations, API-compatible replacements, rebrands, competing implementations, AI-assisted implementation mining, or other uses not granted by the public license, contact Elias Bachaalany for separate written permission.

Permission requests: open a GitHub issue at [0xeb/libxsql/issues](https://github.com/0xeb/libxsql/issues).

Contributors do not receive authority to grant license exceptions, private licenses, or derivative-use rights merely by submitting contributions unless Elias Bachaalany separately authorizes them in writing.

See the `LICENSE` examples/FAQ section for common allowed uses, including package-manager distribution, benchmarking, AI-generated tests and AI-assisted patch preparation for contribution-back work, and independent development.

Third-party vendored dependencies remain under their own licenses; see [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
