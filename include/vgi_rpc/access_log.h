// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// JSONL access-log emitter for the vgi_rpc.access channel.
/// One record per completed RPC call, conforming to docs/access-log-spec.md
/// (machine-checkable form: vgi_rpc/access_log.schema.json).  Enabled via the
/// worker's --access-log <path> flag; suppressed when no path is configured.
#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

// One completed-call access-log record (pipe/subprocess transport fields).
struct AccessRecord {
    std::string method;
    bool is_stream = false;
    std::string status = "ok";       // "ok" | "error"
    std::string error_type;          // "" when status == "ok"
    std::string error_message;       // non-empty when status == "error"
    double duration_ms = 0.0;
    std::string stream_id;           // 32 lowercase hex; set when is_stream
    bool cancelled = false;          // client cancelled a stream
    std::string request_data_b64;    // base64 IPC of the request batch
    bool has_request_data = false;
};

// base64-encode bytes (RFC 4648, padding required).
VGI_RPC_EXPORT std::string base64_encode(const uint8_t* data, size_t len);

// Writes one JSON line per call to the configured path.  NOT thread-safe;
// designed for the single-threaded pipe server.
class VGI_RPC_EXPORT AccessLogWriter {
public:
    AccessLogWriter(const std::string& path,
                    std::string server_id,
                    std::string protocol_name,
                    std::string protocol_hash);

    bool enabled() const noexcept { return enabled_; }
    void emit(const AccessRecord& rec);

private:
    bool enabled_ = false;
    std::ofstream out_;
    std::string server_id_;
    std::string protocol_name_;
    std::string protocol_hash_;
};

}  // namespace vgi_rpc
