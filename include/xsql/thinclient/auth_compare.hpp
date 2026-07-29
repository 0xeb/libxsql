// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

#pragma once

#include <cstddef>
#include <string>

namespace xsql::thinclient::detail {

// Constant-time bearer-token comparison shared by every thinclient server.
// std::string::operator== short-circuits on the first differing byte, which
// turns an authenticated endpoint into a remote timing oracle for the secret's
// matching prefix. This accumulates differences over the presented token's
// full length instead. The secret's LENGTH is not concealed (a length mismatch
// is unequal), only prefix-match timing.
inline bool timing_safe_equal(const std::string& presented,
                              const std::string& expected) {
    unsigned char acc = presented.size() == expected.size() ? 0 : 1;
    for (std::size_t i = 0; i < presented.size(); ++i) {
        const unsigned char e =
            expected.empty()
                ? 0
                : static_cast<unsigned char>(expected[i % expected.size()]);
        acc |= static_cast<unsigned char>(
            static_cast<unsigned char>(presented[i]) ^ e);
    }
    return acc == 0;
}

}  // namespace xsql::thinclient::detail
