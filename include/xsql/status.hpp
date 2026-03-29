// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

namespace xsql {

enum class Status : int {
    ok = 0,
    error = 1,
    internal = 2,
    perm = 3,
    abort = 4,
    busy = 5,
    locked = 6,
    no_memory = 7,
    read_only = 8,
    interrupt = 9,
    io_error = 10,
    corrupt = 11,
    not_found = 12,
    full = 13,
    cant_open = 14,
    protocol = 15,
    empty = 16,
    schema = 17,
    too_big = 18,
    constraint = 19,
    mismatch = 20,
    misuse = 21,
    no_lfs = 22,
    auth = 23,
    format = 24,
    range = 25,
    not_a_db = 26,
    notice = 27,
    warning = 28,
    row = 100,
    done = 101
};

constexpr int to_sqlite_status(Status status) {
    return static_cast<int>(status);
}

constexpr Status to_status(int sqlite_status) {
    return static_cast<Status>(sqlite_status);
}

constexpr bool is_ok(Status status) {
    return status == Status::ok;
}

constexpr bool is_ok(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::ok);
}

constexpr bool is_row(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::row);
}

constexpr bool is_done(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::done);
}

} // namespace xsql
