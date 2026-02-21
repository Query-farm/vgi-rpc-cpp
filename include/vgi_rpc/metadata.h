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
inline constexpr const char* PROTOCOL_NAME = "vgi_rpc.protocol_name";
inline constexpr const char* DESCRIBE_VERSION = "vgi_rpc.describe_version";
inline constexpr const char* TRACEPARENT = "traceparent";
inline constexpr const char* TRACESTATE = "tracestate";

}  // namespace keys

// Protocol constants
inline constexpr const char* REQUEST_VERSION_VALUE = "1";
inline constexpr const char* DESCRIBE_VERSION_VALUE = "2";
inline constexpr const char* DESCRIBE_METHOD_NAME = "__describe__";

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
