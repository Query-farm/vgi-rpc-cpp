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
}

namespace vgi_rpc {

// Wire protocol metadata keys
namespace keys {

inline constexpr const char* METHOD = "vgi_rpc.method";
inline constexpr const char* REQUEST_VERSION = "vgi_rpc.request_version";
inline constexpr const char* LOG_LEVEL = "vgi_rpc.log_level";
inline constexpr const char* LOG_MESSAGE = "vgi_rpc.log_message";
inline constexpr const char* LOG_EXTRA = "vgi_rpc.log_extra";
inline constexpr const char* SERVER_ID = "vgi_rpc.server_id";
inline constexpr const char* REQUEST_ID = "vgi_rpc.request_id";
inline constexpr const char* STREAM_STATE = "vgi_rpc.stream_state";
inline constexpr const char* LOCATION = "vgi_rpc.location";
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
inline constexpr const char* DESCRIBE_VERSION = "vgi_rpc.describe_version";
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

// Protocol constants
inline constexpr const char* REQUEST_VERSION_VALUE = "1";
inline constexpr const char* DESCRIBE_VERSION_VALUE = "4";
inline constexpr const char* DESCRIBE_METHOD_NAME = "__describe__";
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
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
    const std::string& key,
    const std::string& default_value = "");

}  // namespace vgi_rpc
