// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// JSONL access-log emitter for the vgi_rpc.access channel.
/// One record per completed RPC call, conforming to docs/access-log-spec.md
/// (machine-checkable form: vgi_rpc/access_log.schema.json).  Enabled via the
/// worker's --access-log <path> flag; suppressed when no path is configured.
#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

// Default per-record byte ceiling, matching the Python reference.  Downstream
// log shippers drop over-long lines silently (Vector defaults to 100 KiB,
// Fluent Bit to 256 KiB), so an uncapped emitter loses whole records rather
// than fields.  See docs/access-log-spec.md §5b.
inline constexpr int64_t kDefaultMaxRecordBytes = 1048576;

// One completed-call access-log record (pipe/subprocess transport fields).
struct AccessRecord {
    // The protocol that owns the dispatched method, and that protocol's
    // canonical digest.
    //
    // docs/access-log-spec.md §3 makes `protocol` the wire name of the owning
    // protocol -- "not a server-wide default" -- and `protocol_hash` "the
    // registry key when decoding archived records".  A framework endpoint owned
    // by no protocol (`__transport_options__`) logs the server's primary, which
    // the spec prescribes rather than tolerates.
    //
    // Both are constructor arguments, and the writer keeps no copy to fall back
    // on, so an emit site cannot leave them to a server-wide default by
    // omission.  That omission is the failure this pair exists to prevent, and
    // it is the only one in the whole area that fails *silently*: a record
    // naming one protocol and carrying another's digest is well-formed, passes
    // the schema, and feeds a plausible dashboard while a consumer decodes it
    // against the wrong description.
    AccessRecord(std::string owning_protocol, std::string owning_protocol_hash)
        : protocol(std::move(owning_protocol)), protocol_hash(std::move(owning_protocol_hash)) {}

    std::string protocol;
    std::string protocol_hash;
    std::string method;
    bool is_stream = false;
    std::string status = "ok";  // "ok" | "error"
    std::string error_type;     // "" when status == "ok"
    std::string error_message;  // non-empty when status == "error"
    double duration_ms = 0.0;
    std::string request_id;        // per-request correlation id
    std::string stream_id;         // 32 lowercase hex; set when is_stream
    bool cancelled = false;        // client cancelled a stream
    std::string request_data_b64;  // base64 IPC of the request batch
    bool has_request_data = false;
    // Base64 of the *decrypted* stream state, in this server's own encoding,
    // per docs/access-log-spec.md §4.4: `request_state` on a continuation,
    // `response_state` on an init and on any continuation that handed a
    // further token back -- absent on the terminal one, which is what makes
    // "this stream is over" readable from the record alone.
    std::string request_state_b64;
    bool has_request_state = false;
    std::string response_state_b64;
    bool has_response_state = false;
    // Set instead of request_data_b64 when the payload was too large to carry.
    // Reports the character length of the base64 string that was dropped.
    int64_t original_request_bytes = -1;
};

// base64-encode bytes (RFC 4648, padding required).
VGI_RPC_EXPORT std::string base64_encode(const uint8_t* data, size_t len);

// Character length of the base64 encoding of `len` bytes, without encoding it.
VGI_RPC_EXPORT int64_t base64_encoded_length(int64_t len);

// Writes one JSON line per call to the configured path.  Concurrent emitters
// are serialized so threaded HTTP dispatch cannot interleave JSON records.
class VGI_RPC_EXPORT AccessLogWriter {
public:
    // No protocol identity here on purpose: it belongs to the binding that owns
    // the dispatched method, which only the emit site knows.  A writer-held
    // default is how three ports came to stamp every record with their primary.
    AccessLogWriter(const std::string& path, std::string server_id,
                    int64_t max_record_bytes = kDefaultMaxRecordBytes);

    bool enabled() const noexcept { return enabled_; }
    int64_t max_record_bytes() const noexcept { return max_record_bytes_; }

    // True when a base64 payload of `b64_len` characters could plausibly fit
    // under the cap.  Callers use this to skip materializing a payload they
    // would only have to throw away — the difference between a few hundred
    // bytes of accounting and several gigabytes of string.
    bool payload_fits(int64_t b64_len) const noexcept;

    void emit(const AccessRecord& rec);

private:
    bool enabled_ = false;
    std::ofstream out_;
    std::string server_id_;
    int64_t max_record_bytes_ = kDefaultMaxRecordBytes;
    std::mutex mutex_;
};

}  // namespace vgi_rpc
