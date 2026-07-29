// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

// Canonical transactional `runtime_settings(key, value, type, scope)` table.
//
// A table definition owns one connection-local staged overlay. UPDATE validates
// and canonicalizes values immediately, but does not mutate the shared settings
// core. Reads through the same connection merge that overlay over committed
// values; other connections and direct getters continue to see committed state.
// xCommit swaps every staged value under one core lock (no callbacks and no
// allocation), while rollback simply discards the overlay.

#include <cstddef>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <xsql/functions.hpp>
#include <xsql/runtime_settings.hpp>
#include <xsql/vtable.hpp>

namespace xsql::runtime {

namespace detail {

using RuntimeSettingsOverlay = std::unordered_map<std::string, std::string>;

struct RuntimeSettingsTableTransaction {
    std::mutex mutex;
    RuntimeSettingsOverlay staged;
    std::unordered_map<int, RuntimeSettingsOverlay> savepoints;
    bool owns_staged_query_timeout = false;
};

inline void finish_runtime_settings_transaction(
    RuntimeSettingsCore& settings,
    const std::shared_ptr<RuntimeSettingsTableTransaction>& transaction,
    bool commit) noexcept {
    // noexcept is load-bearing (called from the noexcept xCommit/xRollback
    // hooks), but mutex::lock() can throw std::system_error — swallow rather
    // than terminate. Everything past the lock is genuinely non-throwing
    // (noexcept callees, clear(), a bool store).
    try {
        std::lock_guard<std::mutex> lock(transaction->mutex);
        if (commit) {
            settings.commit_staged(
                transaction->staged, transaction->owns_staged_query_timeout);
        } else {
            settings.discard_staged(transaction->owns_staged_query_timeout);
        }
        transaction->staged.clear();
        transaction->savepoints.clear();
        transaction->owns_staged_query_timeout = false;
    } catch (...) {
    }
}

inline Status save_runtime_settings_savepoint(
    const std::shared_ptr<RuntimeSettingsTableTransaction>& transaction,
    int savepoint) {
    std::lock_guard<std::mutex> lock(transaction->mutex);
    transaction->savepoints[savepoint] = transaction->staged;
    return Status::ok;
}

inline Status release_runtime_settings_savepoint(
    const std::shared_ptr<RuntimeSettingsTableTransaction>& transaction,
    int savepoint) {
    std::lock_guard<std::mutex> lock(transaction->mutex);
    for (auto it = transaction->savepoints.begin();
         it != transaction->savepoints.end();) {
        if (it->first >= savepoint) {
            it = transaction->savepoints.erase(it);
        } else {
            ++it;
        }
    }
    return Status::ok;
}

inline Status rollback_runtime_settings_to_savepoint(
    RuntimeSettingsCore& settings,
    const std::shared_ptr<RuntimeSettingsTableTransaction>& transaction,
    int savepoint) {
    std::lock_guard<std::mutex> lock(transaction->mutex);

    RuntimeSettingsOverlay target;
    const auto snapshot = transaction->savepoints.find(savepoint);
    if (snapshot != transaction->savepoints.end()) {
        target = snapshot->second;
    }
    // If the table was first enlisted after SAVEPOINT, SQLite could not have
    // delivered xSavepoint to it. A missing snapshot therefore means the empty
    // pre-enlistment state.

    const bool target_has_query_timeout =
        target.find("query_timeout_ms") != target.end();
    const bool current_has_query_timeout =
        transaction->owns_staged_query_timeout;

    if (!current_has_query_timeout && target_has_query_timeout &&
        !settings.begin_staged_query_timeout()) {
        set_vtab_error(
            "runtime_settings: cannot restore staged query_timeout_ms while "
            "the timeout stack is active (drain it with timeout_pop first; "
            "the stack is process-wide, so it may belong to another session)");
        return Status::error;
    }

    transaction->staged = std::move(target);
    transaction->owns_staged_query_timeout = target_has_query_timeout;
    if (current_has_query_timeout && !target_has_query_timeout) {
        settings.discard_staged(true);
    }

    for (auto it = transaction->savepoints.begin();
         it != transaction->savepoints.end();) {
        if (it->first > savepoint) {
            it = transaction->savepoints.erase(it);
        } else {
            ++it;
        }
    }
    return Status::ok;
}

} // namespace detail

// `settings` must outlive the returned definition. Product-specific settings
// are registered on the same RuntimeSettingsCore before this helper is called.
inline CachedTableDef<RuntimeSettingEntry>
define_runtime_settings_table(RuntimeSettingsCore& settings,
                              std::string product_prefix) {
    TransactionHooks hooks;
    hooks.state_factory = [] {
        return std::make_shared<
            detail::RuntimeSettingsTableTransaction>();
    };
    hooks.commit = [&settings](
                       const TransactionHooks::State& state) noexcept {
        const auto transaction = std::static_pointer_cast<
            detail::RuntimeSettingsTableTransaction>(state);
        detail::finish_runtime_settings_transaction(
            settings, transaction, true);
    };
    hooks.rollback = [&settings](
                         const TransactionHooks::State& state) noexcept {
        const auto transaction = std::static_pointer_cast<
            detail::RuntimeSettingsTableTransaction>(state);
        detail::finish_runtime_settings_transaction(
            settings, transaction, false);
    };
    hooks.savepoint = [](
                          const TransactionHooks::State& state,
                          int savepoint) {
        const auto transaction = std::static_pointer_cast<
            detail::RuntimeSettingsTableTransaction>(state);
        return detail::save_runtime_settings_savepoint(
            transaction, savepoint);
    };
    hooks.release = [](
                        const TransactionHooks::State& state,
                        int savepoint) {
        const auto transaction = std::static_pointer_cast<
            detail::RuntimeSettingsTableTransaction>(state);
        return detail::release_runtime_settings_savepoint(
            transaction, savepoint);
    };
    hooks.rollback_to = [&settings](
                            const TransactionHooks::State& state,
                            int savepoint) {
        const auto transaction = std::static_pointer_cast<
            detail::RuntimeSettingsTableTransaction>(state);
        return detail::rollback_runtime_settings_to_savepoint(
            settings, transaction, savepoint);
    };

    return xsql::cached_table<RuntimeSettingEntry>("runtime_settings")
        .no_shared_cache()
        .estimate_rows([&settings]() -> std::size_t {
            return settings.specs().size();
        })
        .stateful_cache_builder(
            [&settings](
                const TransactionHooks::State& state,
                std::vector<RuntimeSettingEntry>& rows) {
                const auto transaction = std::static_pointer_cast<
                    detail::RuntimeSettingsTableTransaction>(state);
                rows = settings.enumerate();
                std::lock_guard<std::mutex> lock(transaction->mutex);
                for (auto& row : rows) {
                    row.connection_state = state;
                    const auto staged =
                        transaction->staged.find(row.key);
                    if (staged != transaction->staged.end()) {
                        row.value = staged->second;
                    }
                }
            })
        .transaction_hooks(std::move(hooks))
        .column_text(
            "key", [](const RuntimeSettingEntry& row) { return row.key; })
        .column_text_rw(
            "value",
            [](const RuntimeSettingEntry& row) { return row.value; },
            [&settings, product_prefix](
                RuntimeSettingEntry& row, xsql::FunctionArg value) -> bool {
                if (value.is_nochange()) {
                    return true;
                }
                if (value.is_null()) {
                    xsql::set_vtab_error(
                        "runtime_settings: '" + row.key +
                        "' cannot be set to NULL");
                    return false;
                }
                if (row.scope == "action") {
                    xsql::set_vtab_error(
                        "runtime_settings: '" + row.key +
                        "' is a PRAGMA verb, not a settable value");
                    return false;
                }

                const RuntimePragmaReply normalized = settings.normalize(
                    row.key, value.as_text(), product_prefix);
                if (!normalized.handled) {
                    xsql::set_vtab_error(
                        "runtime_settings: unknown key '" + row.key + "'");
                    return false;
                }
                if (!normalized.success) {
                    xsql::set_vtab_error(normalized.error);
                    return false;
                }

                const auto transaction = std::static_pointer_cast<
                    detail::RuntimeSettingsTableTransaction>(
                    row.connection_state);
                if (!transaction) {
                    xsql::set_vtab_error(
                        "runtime_settings: missing connection-local "
                        "transaction state");
                    return false;
                }
                std::lock_guard<std::mutex> lock(transaction->mutex);
                const bool first_query_timeout =
                    row.key == "query_timeout_ms" &&
                    transaction->staged.find(row.key) ==
                        transaction->staged.end();
                if (first_query_timeout &&
                    !settings.begin_staged_query_timeout()) {
                    xsql::set_vtab_error(
                        "runtime_settings: query_timeout_ms cannot be staged "
                        "while the timeout stack is active (drain it with " +
                        product_prefix + ".timeout_pop first; the stack is "
                        "process-wide, so it may belong to another session)");
                    return false;
                }

                try {
                    transaction->staged[row.key] = normalized.value;
                } catch (...) {
                    if (first_query_timeout) {
                        transaction->staged.erase(row.key);
                        settings.discard_staged(true);
                    }
                    xsql::set_vtab_error(
                        "runtime_settings: out of memory while staging '" +
                        row.key + "'");
                    return false;
                }
                if (first_query_timeout) {
                    transaction->owns_staged_query_timeout = true;
                }
                row.value = normalized.value;
                return true;
            })
        .column_text(
            "type", [](const RuntimeSettingEntry& row) { return row.type; })
        .column_text(
            "scope", [](const RuntimeSettingEntry& row) { return row.scope; })
        .build();
}

} // namespace xsql::runtime
