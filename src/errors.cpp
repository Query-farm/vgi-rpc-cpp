// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/errors.h"

namespace vgi_rpc {

std::string exception_type_of(const std::exception& e) {
    // A kinded error names itself; nothing else about its C++ class should
    // decide what the client sees.
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) {
        return kinded->exception_type();
    }
    // invalid_argument and out_of_range both derive from logic_error, so the
    // narrow cases have to come first or every one of them reports TypeError.
    if (dynamic_cast<const std::invalid_argument*>(&e)) return "ValueError";
    if (dynamic_cast<const std::out_of_range*>(&e)) return "IndexError";
    if (dynamic_cast<const std::logic_error*>(&e)) return "TypeError";
    return "RuntimeError";
}

std::string error_kind_of(const std::exception& e) {
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) return kinded->kind();
    return "";
}

}  // namespace vgi_rpc
