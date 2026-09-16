// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <memory>
#include <arrow/type_fwd.h>

#include "vgi_rpc/export.h"

namespace arrow {
class Schema;
class KeyValueMetadata;
}  // namespace arrow

namespace vgi_rpc {

// Wire protocol metadata keys
namespace keys {

inline constexpr const char* METHOD = "vgi_rpc.method";

/// Names the protocol a request addresses -- the routing key.
///
/// Dispatch resolves the pair `(protocol, method)`: a server hosts one or more
/// protocols and method names may collide across them, which is what lets
/// protocols be authored independently. Required on every request, including
/// against a server hosting exactly one protocol -- an exemption would let an
/// intermediary that rebuilds a request and drops the field land silently on
/// whichever protocol happened to be first, rather than being told.
///
/// The major version is part of the protocol name (`vgi_rpc.Reflection.v1`), so
/// an incompatible major is a routing failure rather than a parse failure, and
/// v1 and v2 can be served side by side while clients migrate.
inline constexpr const char* PROTOCOL = "vgi_rpc.protocol";
inline constexpr const char* REQUEST_VERSION = "vgi_rpc.request_version";
inline constexpr const char* LOG_LEVEL = "vgi_rpc.log_level";
inline constexpr const char* LOG_MESSAGE = "vgi_rpc.log_message";
inline constexpr const char* LOG_EXTRA = "vgi_rpc.log_extra";
inline constexpr const char* SERVER_ID = "vgi_rpc.server_id";
inline constexpr const char* REQUEST_ID = "vgi_rpc.request_id";
inline constexpr const char* STREAM_STATE = "vgi_rpc.stream_state";
inline constexpr const char* LOCATION = "vgi_rpc.location";
inline constexpr const char* LOCATION_SHA256 = "vgi_rpc.location.sha256";
inline constexpr const char* LOCATION_SOURCE = "vgi_rpc.location.source";
inline constexpr const char* LOCATION_FETCH_MS = "vgi_rpc.location.fetch_ms";
inline constexpr const char* SHM_OFFSET = "vgi_rpc.shm_offset";
inline constexpr const char* SHM_LENGTH = "vgi_rpc.shm_length";
inline constexpr const char* SHM_SOURCE = "vgi_rpc.shm_source";
inline constexpr const char* SHM_SEGMENT_NAME = "vgi_rpc.shm_segment_name";
inline constexpr const char* SHM_SEGMENT_SIZE = "vgi_rpc.shm_segment_size";

// Transport capability namespace for the __transport_options__ handshake.
// Matched by prefix and open-ended: an unknown key is ignored, so a future
// capability needs no protocol-version bump.  A feature is used only when
// both peers advertise it.
inline constexpr const char* TRANSPORT_PREFIX = "vgi_rpc.transport.";
inline constexpr const char* TRANSPORT_SHM = "vgi_rpc.transport.shm";
inline constexpr const char* PROTOCOL_NAME = "vgi_rpc.protocol_name";
inline constexpr const char* PROTOCOL_HASH = "vgi_rpc.protocol_hash";
inline constexpr const char* PROTOCOL_VERSION = "vgi_rpc.protocol_version";
inline constexpr const char* CANCEL = "vgi_rpc.cancel";
inline constexpr const char* TRACEPARENT = "traceparent";
inline constexpr const char* TRACESTATE = "tracestate";

// Stream state travels as two tokens split by lifetime: the call token is
// minted once by /init and names the fixed half (the request and the resolved
// schemas), while the cursor token is re-minted every turn.  Packing both into
// one would re-serialize and re-parse the fixed half on every continuation,
// which for a typical stream is most of the payload.
inline constexpr const char* STATE_B64 = "vgi_rpc.stream_state#b64";
inline constexpr const char* CALL_STATE_B64 = "vgi_rpc.call_state#b64";

// Machine-readable error class, alongside the human-facing error_type.
// Clients branch on this; "session_lost" and "server_draining" are the values
// a sticky-aware client must recognize.
inline constexpr const char* ERROR_KIND = "vgi_rpc.error_kind";

}  // namespace keys

// Well-known error_kind values.
inline constexpr const char* ERROR_KIND_SESSION_LOST = "session_lost";
inline constexpr const char* ERROR_KIND_SERVER_DRAINING = "server_draining";
inline constexpr const char* ERROR_KIND_METHOD_NOT_IMPLEMENTED = "method_not_implemented";
// Routing outcomes a client depends on telling apart: "you do not speak this
// protocol" is a different answer from "you speak it but lack this method",
// and a capability probe reads both.
inline constexpr const char* ERROR_KIND_PROTOCOL_NOT_SUPPORTED = "protocol_not_supported";
inline constexpr const char* ERROR_KIND_PROTOCOL_NOT_SPECIFIED = "protocol_not_specified";

/// The wire name of the reflection protocol.
///
/// Fixed, and the one protocol name either side may know a priori: it is the
/// bootstrap, so there is nothing to discover it with.  It lives here rather
/// than beside the server-side builders because the client needs it too --
/// discovery is the one call a client makes before it knows anything else.
inline constexpr const char* kReflectionProtocolName = "vgi_rpc.Reflection.v1";

// Protocol constants
inline constexpr const char* REQUEST_VERSION_VALUE = "1";

/// The introspection format `ServiceDescription` presents to a caller.
///
/// Vestigial as a wire value: the wire format is `vgi_rpc.Reflection.v1`, whose
/// version rides its protocol name.  Kept because `ServiceDescription` is a
/// client-side *view*, and a caller that branches on the shape it was handed
/// still needs a number for it.
///
/// It never crosses the wire, which is exactly why it drifted: a Python client
/// describing this server stamps its own constant, so no server-role test can
/// see the number this client reports.  It is cross-port shared state and must
/// equal the reference's `DESCRIBE_VERSION`; retiring `__describe__` moved that
/// to 5 and this was left at 4.
inline constexpr const char* DESCRIBE_VERSION_VALUE = "5";

/// Retired in the multi-service revamp, and kept only so the refusal can say
/// where introspection went.
///
/// A stale client told merely "no such method" cannot tell "retired" from "this
/// server was built without introspection", and the two need opposite fixes:
/// update the client, or reconfigure the server.  Naming it here is what lets
/// dispatch answer the first without inventing a whole capability table.
inline constexpr const char* RETIRED_DESCRIBE_METHOD = "__describe__";

/// What a server answers a `__describe__` request with.
///
/// Spelled out rather than assembled at the call site because two transports
/// refuse it and a stale client must get the same sentence from either.
inline constexpr const char* RETIRED_DESCRIBE_MESSAGE =
    "'__describe__' was retired. Introspection is now the 'vgi_rpc.Reflection.v1' protocol: "
    "call 'list_protocols' for what this server hosts, then 'describe' for one protocol's "
    "methods.";

inline constexpr const char* TRANSPORT_OPTIONS_METHOD_NAME = "__transport_options__";

// Empty schema — used for void results, protocol errors, producer tick input
VGI_RPC_EXPORT const std::shared_ptr<arrow::Schema>& empty_schema();

// Create a zero-row batch on the given schema
VGI_RPC_EXPORT std::shared_ptr<arrow::RecordBatch> make_empty_batch(
    const std::shared_ptr<arrow::Schema>& schema);

// Generate a random hex string of given length
VGI_RPC_EXPORT std::string random_hex(size_t length);

// Get or create metadata value from a KeyValueMetadata
VGI_RPC_EXPORT std::string get_metadata_value(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata, const std::string& key,
    const std::string& default_value = "");

}  // namespace vgi_rpc
