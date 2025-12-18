/**
 * cached_table.cpp - Query-scoped cache with constraint pushdown
 *
 * Demonstrates cached_table<T>() for data requiring enumeration,
 * and filter_eq() for optimized lookups.
 */

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>
#include <cstdio>
#include <map>
#include <vector>

// Simulated cross-reference data
struct Xref {
    uint64_t from;
    uint64_t to;
    int type;
};

// Global xref storage (simulating external data source)
std::vector<Xref> g_xrefs = {
    {0x1000, 0x2000, 1},
    {0x1004, 0x2000, 1},
    {0x1008, 0x3000, 2},
    {0x100C, 0x2000, 1},
    {0x2000, 0x3000, 1},
    {0x2004, 0x4000, 2},
    {0x3000, 0x4000, 1},
};

// Index for fast lookups by target address
std::multimap<uint64_t, size_t> g_xrefs_to_index;

void build_index() {
    g_xrefs_to_index.clear();
    for (size_t i = 0; i < g_xrefs.size(); i++) {
        g_xrefs_to_index.emplace(g_xrefs[i].to, i);
    }
}

// Custom iterator for filter_eq("to")
class XrefsToIterator : public xsql::RowIterator {
    uint64_t target_;
    std::multimap<uint64_t, size_t>::iterator it_, end_;
    bool valid_ = false;

public:
    XrefsToIterator(int64_t target) : target_(target) {
        auto range = g_xrefs_to_index.equal_range(target_);
        it_ = range.first;
        end_ = range.second;
        valid_ = (it_ != end_);
    }

    bool next() override {
        if (!valid_) return false;
        ++it_;
        valid_ = (it_ != end_);
        return true;
    }

    bool eof() const override { return !valid_; }

    void column(sqlite3_context* ctx, int col) override {
        if (it_ == end_) {
            sqlite3_result_null(ctx);
            return;
        }
        const auto& xref = g_xrefs[it_->second];
        switch (col) {
            case 0: sqlite3_result_int64(ctx, xref.from); break;
            case 1: sqlite3_result_int64(ctx, xref.to); break;
            case 2: sqlite3_result_int(ctx, xref.type); break;
        }
    }

    int64_t rowid() const override {
        return it_ != end_ ? static_cast<int64_t>(it_->second) : 0;
    }
};

int main() {
    // Build lookup index
    build_index();

    // Define cached table with filter
    auto def = xsql::cached_table<Xref>("xrefs")
        .estimate_rows([]() { return g_xrefs.size(); })
        .cache_builder([](std::vector<Xref>& cache) {
            printf("[Cache] Building xref cache (%zu items)...\n", g_xrefs.size());
            cache = g_xrefs;  // Copy all xrefs
        })
        .column_int64("from_ea", [](const Xref& r) { return r.from; })
        .column_int64("to_ea", [](const Xref& r) { return r.to; })
        .column_int("type", [](const Xref& r) { return r.type; })
        .filter_eq("to_ea", [](int64_t target) {
            printf("[Filter] Using optimized lookup for to_ea = 0x%llx\n",
                   (unsigned long long)target);
            return std::make_unique<XrefsToIterator>(target);
        }, 10.0, 3.0)
        .build();

    // Open database
    xsql::Database db;
    db.open(":memory:");
    xsql::register_cached_vtable(db.handle(), def.name.c_str(), &def);
    db.create_table("xrefs", def.name.c_str());

    // Full scan query (builds cache)
    printf("Query 1: Full scan\n");
    auto result = db.query("SELECT printf('0x%X', from_ea), printf('0x%X', to_ea) FROM xrefs");
    for (const auto& row : result) {
        printf("  %s -> %s\n", row[0].c_str(), row[1].c_str());
    }

    // Filtered query (uses index, no cache build)
    printf("\nQuery 2: Filtered by to_ea = 0x2000\n");
    result = db.query("SELECT printf('0x%X', from_ea) FROM xrefs WHERE to_ea = 0x2000");
    for (const auto& row : result) {
        printf("  %s\n", row[0].c_str());
    }

    // Another filtered query
    printf("\nQuery 3: Filtered by to_ea = 0x3000\n");
    result = db.query("SELECT printf('0x%X', from_ea) FROM xrefs WHERE to_ea = 0x3000");
    for (const auto& row : result) {
        printf("  %s\n", row[0].c_str());
    }

    return 0;
}
