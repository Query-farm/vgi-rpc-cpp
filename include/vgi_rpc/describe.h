// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Introspection support for the __describe__ built-in method.
/// register_describe() adds a handler that returns method schemas, types,
/// and documentation for all registered methods on the server.
#pragma once

#include <string>
#include <unordered_map>

namespace vgi_rpc {

struct MethodInfo;

// Register the __describe__ handler in the method map.
// Called by ServerBuilder::build() when describe is enabled.
void register_describe(
    std::unordered_map<std::string, MethodInfo>& methods,
    const std::string& protocol_name,
    const std::string& server_id);

}  // namespace vgi_rpc
