// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The value a StateRef shares between its copies.  Private to the library:
// CallContext builds them, StateRef::decode reads them.
#pragma once

#include "vgi_rpc/state_ref.h"

#include <memory>
#include <string>

namespace vgi_rpc {

struct StateRef::Impl {
    // The value's serialized bytes.  For a record ref, its Arrow IPC stream.
    std::shared_ptr<const std::string> bytes;
    // The original record while the ref is in memory, so a typed resolve
    // returns the very instance the stream method handed to state_ref().
    std::shared_ptr<arrow::RecordBatch> record;
    // Read from an encoded inline ref rather than made by state_ref().
    bool decoded = false;
};

}  // namespace vgi_rpc
