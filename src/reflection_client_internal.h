// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Package-internal: the one classification of "this server does not host
/// reflection", shared by the byte-stream and HTTP clients.
#pragma once

#include <string>

namespace vgi_rpc::detail {

/// Whether an error answering `list_protocols` says reflection is not hosted.
///
/// Only meaningful for `list_protocols`, which is always hosted when
/// reflection is: a "not supported" answer to it can only be about the
/// protocol. (`describe` answers `protocol_not_supported` for an unknown
/// *argument*, which is why `describe_protocol` lists first.)
///
/// A current server without reflection answers `protocol_not_supported`; one
/// older than multi-protocol hosting ignores the protocol key and answers an
/// unknown method; both carry `UNIMPLEMENTED` when the server sends a code at
/// all. A server that sends no error kind still names the exception. The bare
/// HTTP 404 of a server older than protocol-scoped routes is classified by the
/// HTTP client itself, since only it sees a non-Arrow status.
inline bool reflection_not_hosted(const std::string& error_kind, const std::string& error_code,
                                  const std::string& exception_type) {
    return error_kind == "protocol_not_supported" || error_kind == "method_not_implemented" ||
           error_code == "UNIMPLEMENTED" || exception_type == "ProtocolNotSupportedError" ||
           exception_type == "MethodNotImplementedError";
}

}  // namespace vgi_rpc::detail
