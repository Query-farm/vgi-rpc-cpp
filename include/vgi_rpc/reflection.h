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
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/server.h"

namespace vgi_rpc {

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

/// The reflection protocol's own method table.
///
/// Built from the same `MethodInfo` shape every other protocol uses, so the
/// hash and the description read one table rather than two hand-kept copies
/// that can drift.  Reflection is not special-cased: it appears in its own
/// `list_protocols` output, and `describe("vgi_rpc.Reflection.v1")` must return
/// the two methods it answers -- a client that discovers a server the
/// documented way learns how to call reflection from reflection itself.
///
/// `handler` is left empty: these are dispatched by `Server::serve_reflection`,
/// which answers them from the server's own binding table rather than through
/// the generic handler signature.
VGI_RPC_EXPORT std::unordered_map<std::string, MethodInfo> ReflectionMethods();

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
