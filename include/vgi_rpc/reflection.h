#pragma once

// vgi_rpc.Reflection.v1 -- discovery as an ordinary co-hosted protocol.
//
// See src/reflection.cpp for why introspection is a protocol here rather than a
// hardcoded method name.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/result.h>
#include <arrow/type.h>

#include "vgi_rpc/export.h"
#include "vgi_rpc/server.h"

namespace vgi_rpc {

/// The wire name of the reflection protocol.
///
/// Fixed, and the one protocol name a client may know a priori: it is the
/// bootstrap, so there is nothing to discover it with.
inline constexpr const char* kReflectionProtocolName = "vgi_rpc.Reflection.v1";

/// One hosted protocol, as it appears in `list_protocols`.
struct VGI_RPC_EXPORT ProtocolSummary {
  std::string protocol;
  std::string version;
  std::string hash;
};

/// Whether a method belongs to the application protocol rather than the framework.
VGI_RPC_EXPORT bool IsApplicationMethod(const std::string& name);

/// Whether a method returns a value to its caller.
VGI_RPC_EXPORT bool UnaryHasReturn(const MethodInfo& info);

/// The stream kind, or "" for a unary method.
VGI_RPC_EXPORT std::string StreamKindFor(const MethodInfo& info);

/// One protocol's canonical fingerprint.
VGI_RPC_EXPORT arrow::Result<std::string> BindingHash(
    const std::string& name, const std::unordered_map<std::string, MethodInfo>& methods);

/// Build the single-row `ProtocolList` payload, serialized as an IPC stream.
VGI_RPC_EXPORT arrow::Result<std::string> BuildProtocolList(
    const std::string& server_id, const std::string& server_version,
    const std::string& request_version, const std::vector<ProtocolSummary>& protocols);

/// Build the single-row `ServiceDescription` payload, serialized as an IPC stream.
VGI_RPC_EXPORT arrow::Result<std::string> BuildServiceDescription(
    const std::string& protocol, const std::string& version, const std::string& hash,
    const std::unordered_map<std::string, MethodInfo>& methods);

}  // namespace vgi_rpc
