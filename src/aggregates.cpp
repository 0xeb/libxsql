// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <xsql/aggregates.hpp>
#include <xsql/database.hpp>
#include <xsql/functions.hpp>

#include <sqlite3.h>

#include <cstdint>
#include <vector>

namespace xsql {

namespace {

struct BlobConcatState {
    std::vector<uint8_t> bytes;
    bool any_input = false;  // distinguishes "no rows" from "rows that were all NULL"
    bool errored = false;
};

BlobConcatState* fetch_state(AggregateContext& ctx) {
    void** slot = ctx.state_ptr();
    if (!slot) return nullptr;
    if (*slot == nullptr) {
        *slot = new BlobConcatState();
    }
    return static_cast<BlobConcatState*>(*slot);
}

void blob_concat_step(AggregateContext& ctx, int argc, FunctionArg* argv) {
    auto* state = fetch_state(ctx);
    if (!state || state->errored) return;
    if (argc < 1) return;

    const FunctionArg& arg = argv[0];
    const int type = arg.type();

    if (type == SQLITE_NULL) {
        // NULL inputs are skipped, like SQLite's built-in aggregates.
        return;
    }

    state->any_input = true;

    if (type == SQLITE_BLOB) {
        const void* data = arg.as_blob();
        const int len = arg.bytes();
        if (data && len > 0) {
            const auto* bytes = static_cast<const uint8_t*>(data);
            state->bytes.insert(state->bytes.end(), bytes, bytes + len);
        }
        return;
    }

    if (type == SQLITE_INTEGER) {
        const int64_t value = arg.as_int64();
        if (value < 0 || value > 255) {
            state->errored = true;
            ctx.result_error("blob_concat: INTEGER input out of byte range [0, 255]");
            return;
        }
        state->bytes.push_back(static_cast<uint8_t>(value & 0xFF));
        return;
    }

    // SQLITE_TEXT, SQLITE_FLOAT, or anything else: error.
    state->errored = true;
    ctx.result_error("blob_concat: input must be BLOB, INTEGER 0-255, or NULL");
}

void blob_concat_final(AggregateContext& ctx) {
    void** slot = ctx.state_ptr();
    if (!slot || *slot == nullptr) {
        // No step calls happened (zero-row aggregation): emit NULL.
        ctx.result_null();
        return;
    }
    auto* state = static_cast<BlobConcatState*>(*slot);
    if (!state->errored) {
        if (!state->any_input) {
            // No non-NULL input was seen (zero rows or all-NULL inputs): NULL,
            // matching SQLite's built-in aggregates (e.g. SUM over no rows).
            ctx.result_null();
        } else if (state->bytes.empty()) {
            // At least one non-NULL input, but it contributed no bytes (e.g. a
            // zero-length BLOB x''). A zero-length BLOB is a non-NULL value and
            // is distinct from NULL, so emit an empty BLOB rather than NULL.
            ctx.result_blob("", 0);
        } else {
            ctx.result_blob(state->bytes.data(), state->bytes.size());
        }
    }
    // If state->errored, the error was already raised in step(); don't override.
    delete state;
    *slot = nullptr;
}

} // namespace

bool register_builtin_aggregates(Database& db) {
    Status s = db.register_aggregate("blob_concat", 1, blob_concat_step, blob_concat_final);
    return s == Status::ok;
}

} // namespace xsql
