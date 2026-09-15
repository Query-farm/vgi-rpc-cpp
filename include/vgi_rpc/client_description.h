// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Client-side decoding of discovery replies.
///
/// Two encodings meet here.  `ServiceDescription` is a *client-side view* --
/// the shape a caller wants -- and was only ever a wire format by accident of
/// there being one encoding.  `vgi_rpc.Reflection.v1` is the wire format now,
/// and `decode_service_description` adapts it into that same view, which is
/// what lets every existing call site move to reflection unchanged.
///
/// Decoding is deliberately *tolerant*, and that is normative rather than
/// convenient: fields are read by name, unknown columns are ignored, and an
/// absent column takes its default.  A v1.1 server answering a v1.0 client
/// therefore decodes, and so does the reverse.  The one thing that is an error
/// is a column that is absent *and* has no default -- silently zero-filling a
/// required field would hand a caller a description that is wrong rather than
/// missing.  The rule that follows for every port: a field added in a minor
/// version must carry a default, or the addition is a breaking change wearing
/// a minor version number.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/type_fwd.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct MethodDescription {
    std::string name;
    std::string method_type;
    bool has_return = false;
    std::shared_ptr<arrow::Schema> params_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    bool has_header = false;
    std::shared_ptr<arrow::Schema> header_schema;
    std::optional<bool> is_exchange;
};

struct ServiceDescription {
    std::string protocol_name;
    std::string request_version;
    std::string describe_version;
    std::string protocol_hash;
    std::string server_id;
    std::string protocol_version;
    std::unordered_map<std::string, MethodDescription> methods;

    const MethodDescription* method(const std::string& name) const noexcept;
};

/// Parse and validate one version-4 __describe__ data batch. Throws
/// std::runtime_error for an invalid response rather than returning a partial
/// description that a generated or dynamic client could misinterpret.
///
/// Retired wire format, kept only as the decoder for this port's own legacy
/// `__describe__` handler; new callers go through reflection.
VGI_RPC_EXPORT ServiceDescription parse_service_description(const AnnotatedBatch& response);

/// One hosted protocol, as `vgi_rpc.Reflection.v1` reports it.
///
/// Enough to decide whether to fetch the full description: a client that
/// already knows a hash can skip the second round trip entirely.
struct ReflectedProtocol {
    std::string protocol;
    std::string protocol_version;
    std::string protocol_hash;
    bool deprecated = false;
    std::string deprecation_message;
    std::vector<std::string> features;
};

/// What one server hosts, as `list_protocols` reports it.
///
/// Server identity lives here rather than on a description, because two
/// processes serving the same protocol must describe it identically -- if they
/// do not, the description is not a property of the protocol.
struct ProtocolListing {
    std::string server_id;
    std::string server_version;
    std::string request_version;
    std::vector<ReflectedProtocol> protocols;

    /// The first hosted protocol that is not framework surface, or nullptr.
    ///
    /// Reflection and identity are co-hosted under the reserved `vgi_rpc.`
    /// prefix; what a caller means by "the" protocol is the application one.
    const ReflectedProtocol* application() const noexcept;
};

/// Decode a `list_protocols` reply.
VGI_RPC_EXPORT ProtocolListing decode_protocol_list(const AnnotatedBatch& response);

/// Decode a `describe` reply into this module's client-side view.
///
/// `listing`, when supplied, carries the server identity and wire-envelope
/// version that a description deliberately does not. Pass nullptr when the
/// caller named a protocol outright and so never made the `list_protocols`
/// call; the two identity fields are then left empty rather than invented.
VGI_RPC_EXPORT ServiceDescription decode_service_description(
    const AnnotatedBatch& response, const ProtocolListing* listing = nullptr);

}  // namespace vgi_rpc
