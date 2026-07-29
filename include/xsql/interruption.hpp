// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <sqlite3.h>

#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace xsql {

// SQLite's progress handler only runs between VDBE opcodes. Long-running
// virtual-table and streaming loops use this thread-local checker to poll the
// same timeout/cancellation state while they remain inside one opcode.
// thread_local is CORRECT here (unlike the progress-handler registry below,
// which mirrors connection-global SQLite state): the checker is only ever
// polled by the thread executing the query, so per-thread scoping is exactly
// the isolation wanted — do not "fix" it to a process-wide slot.
inline thread_local std::function<bool()> g_vtab_interrupt_check;

inline void set_interrupt_checker(std::function<bool()> fn) {
    g_vtab_interrupt_check = std::move(fn);
}

inline void clear_interrupt_checker() {
    g_vtab_interrupt_check = nullptr;
}

/// Temporarily install an interrupt checker and restore the previous checker
/// when the scope exits. Nested queries and embedders that install their own
/// checker therefore retain the outer callback.
class ScopedInterruptChecker {
public:
    explicit ScopedInterruptChecker(std::function<bool()> fn)
        : previous_(std::move(g_vtab_interrupt_check)), active_(true) {
        g_vtab_interrupt_check = std::move(fn);
    }

    ScopedInterruptChecker(const ScopedInterruptChecker&) = delete;
    ScopedInterruptChecker& operator=(const ScopedInterruptChecker&) = delete;

    ScopedInterruptChecker(ScopedInterruptChecker&& other) noexcept
        : previous_(std::move(other.previous_)), active_(other.active_) {
        other.active_ = false;
    }

    ~ScopedInterruptChecker() {
        if (active_) {
            g_vtab_interrupt_check = std::move(previous_);
        }
    }

private:
    std::function<bool()> previous_;
    bool active_ = false;
};

/// Install a SQLite progress handler for one scope and restore a handler
/// installed by an outer libxsql scope on the same connection. SQLite exposes
/// only a setter, not a getter, so libxsql tracks its own shadow registry.
///
/// The registry is PROCESS-GLOBAL (mutex-guarded), because the resource it
/// mirrors — sqlite3_progress_handler — is per-CONNECTION state visible from
/// every thread. A per-thread shadow would make a guard constructed on one
/// thread invisible to another thread's guard on the same connection, whose
/// destructor would then silently uninstall the first thread's live handler.
///
/// Semantics: guards on one connection form a LIFO stack. Same-thread nesting
/// is fully supported. Cross-thread guards on the SAME connection are safe when
/// their lifetimes are properly nested/serialized; non-LIFO destruction is
/// detected (the registry entry no longer matches what this guard installed)
/// and degrades to a no-op rather than clobbering the newer live handler.
class ScopedProgressHandler {
public:
    using Callback = int (*)(void*);

    ScopedProgressHandler(sqlite3* db, int steps, Callback callback,
                          void* user_data)
        : db_(db), installed_{steps, callback, user_data} {
        if (!db_) return;
        std::lock_guard<std::mutex> lock(registry_mutex());
        auto& handlers = registry();
        auto it = handlers.find(db_);
        if (it != handlers.end()) previous_ = it->second;
        handlers[db_] = installed_;
        sqlite3_progress_handler(db_, steps, callback, user_data);
        active_ = true;
    }

    ScopedProgressHandler(const ScopedProgressHandler&) = delete;
    ScopedProgressHandler& operator=(const ScopedProgressHandler&) = delete;

    ~ScopedProgressHandler() {
        if (!active_) return;
        // A destructor is implicitly noexcept; a std::system_error out of the
        // lock would terminate. Swallow it — worst case the connection keeps a
        // stale progress handler until the next guard replaces it.
        try {
            std::lock_guard<std::mutex> lock(registry_mutex());
            auto& handlers = registry();
            auto it = handlers.find(db_);
            // Restore only if we are still the installed handler; a newer guard
            // (non-LIFO destruction) must not be clobbered with older state.
            if (it == handlers.end() || !(it->second == installed_)) return;
            if (previous_) {
                it->second = *previous_;
                sqlite3_progress_handler(db_, previous_->steps,
                                         previous_->callback,
                                         previous_->user_data);
            } else {
                handlers.erase(it);
                sqlite3_progress_handler(db_, 0, nullptr, nullptr);
            }
        } catch (...) {
        }
    }

private:
    struct Entry {
        int steps = 0;
        Callback callback = nullptr;
        void* user_data = nullptr;

        bool operator==(const Entry& other) const {
            return steps == other.steps && callback == other.callback &&
                   user_data == other.user_data;
        }
    };

    // Intentionally leaked: guards may run during static teardown (e.g. from a
    // detached shutdown thread), which must never touch a destroyed registry.
    // Magic statics make first use thread-safe.
    static std::unordered_map<sqlite3*, Entry>& registry() {
        static auto* handlers = new std::unordered_map<sqlite3*, Entry>();
        return *handlers;
    }

    static std::mutex& registry_mutex() {
        static auto* m = new std::mutex();
        return *m;
    }

    sqlite3* db_ = nullptr;
    Entry installed_{};
    std::optional<Entry> previous_;
    bool active_ = false;
};

// A custom checker must never unwind through SQLite's C callbacks. Treat a
// checker failure as an interruption; query-owned checkers record the detailed
// predicate error separately for the result envelope.
inline bool vtab_interrupted() noexcept {
    if (!g_vtab_interrupt_check) {
        return false;
    }
    try {
        return g_vtab_interrupt_check();
    } catch (...) {
        return true;
    }
}

}  // namespace xsql
