// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

/**
 * xsql/cache_registry.hpp - connection-wide invalidation of cached tables
 *
 * A query-scoped cached table keeps its rows for the life of one SQLite cursor
 * and rebuilds only after a write THROUGH the table (see WriteGeneration in
 * vtable.hpp). A SQL scalar function that changes the engine behind a table --
 * renames a symbol, re-runs analysis, redefines types -- is not such a write,
 * and usually holds no handle to the tables whose rows it changes: registration
 * clones each definition, and many tools drop their own copy.
 *
 * This registry keeps, per SQLite connection, one invalidator per registered
 * cached table (the registered clone itself), removed when SQLite destroys the
 * module. invalidate_cached_tables(db) runs each one: exactly what a write
 * through that table does, so a scan already in flight keeps its rows and every
 * later probe or statement rebuilds from the live engine.
 */
#pragma once

#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

struct sqlite3;

namespace xsql {
namespace detail {

struct CachedTableRegistry {
    std::mutex mu;
    // connection -> (registered def clone, invalidator)
    std::unordered_map<sqlite3*, std::vector<std::pair<const void*, std::function<void()>>>> by_db;
};

inline CachedTableRegistry& cached_table_registry() {
    static CachedTableRegistry registry;
    return registry;
}

inline void register_cached_table_invalidator(sqlite3* db, const void* def,
                                              std::function<void()> invalidate) {
    auto& registry = cached_table_registry();
    std::lock_guard<std::mutex> lock(registry.mu);
    registry.by_db[db].emplace_back(def, std::move(invalidate));
}

// Called when SQLite destroys a cached module (connection close, re-register).
inline void unregister_cached_table_invalidator(const void* def) {
    auto& registry = cached_table_registry();
    std::lock_guard<std::mutex> lock(registry.mu);
    for (auto it = registry.by_db.begin(); it != registry.by_db.end();) {
        auto& entries = it->second;
        for (auto e = entries.begin(); e != entries.end();) {
            e = (e->first == def) ? entries.erase(e) : e + 1;
        }
        it = entries.empty() ? registry.by_db.erase(it) : std::next(it);
    }
}

}  // namespace detail

// Invalidate every cached table registered on `db`, as a write through each of
// them would. Call it from any SQL function that mutates the engine state the
// tables read. Safe mid-statement: the invalidators are copied under the lock
// and run outside it, so one may itself take table locks.
inline void invalidate_cached_tables(sqlite3* db) {
    std::vector<std::function<void()>> invalidators;
    {
        auto& registry = detail::cached_table_registry();
        std::lock_guard<std::mutex> lock(registry.mu);
        auto it = registry.by_db.find(db);
        if (it == registry.by_db.end()) return;
        invalidators.reserve(it->second.size());
        for (const auto& entry : it->second) invalidators.push_back(entry.second);
    }
    for (const auto& invalidate : invalidators) invalidate();
}

}  // namespace xsql
