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

// Compute the protocol_hash: a SHA-256 hex digest over the canonical describe
// payload (describe_version, request_version, protocol_name, and per-method
// name/type/flags/schemas).  Excludes whatever is in `methods` at call time, so
// callers must invoke it before registering the synthetic __describe__ method.
// Mirrors Python's introspect.compute_protocol_hash.
std::string compute_protocol_hash(const std::string& protocol_name,
                                  const std::unordered_map<std::string, MethodInfo>& methods);

// Register the __describe__ handler in the method map.
// Called by ServerBuilder::build() when describe is enabled.
// protocol_version (canonical semver, may be empty) is surfaced under
// vgi_rpc.protocol_version in the describe response metadata.
void register_describe(std::unordered_map<std::string, MethodInfo>& methods,
                       const std::string& protocol_name, const std::string& server_id,
                       const std::string& protocol_version = "");

}  // namespace vgi_rpc
