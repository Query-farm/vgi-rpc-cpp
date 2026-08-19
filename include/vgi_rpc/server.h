// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/type.h>

#include "vgi_rpc/access_log.h"
#include "vgi_rpc/call_context.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/http_config.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/shm.h"
#include "vgi_rpc/stream.h"

namespace vgi_rpc {

enum class MethodType {
    UNARY,
    STREAM,
};

struct MethodInfo {
    std::string name;
    MethodType method_type;
    std::shared_ptr<arrow::Schema> params_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    std::function<Result(const Request&, CallContext&)> handler;
    std::string doc;
    bool has_return = true;

    // For streaming
    std::shared_ptr<arrow::Schema> input_schema;   // nullptr for unary
    std::shared_ptr<arrow::Schema> output_schema;  // nullptr for unary
    std::shared_ptr<arrow::Schema> header_schema;  // nullptr if no header
    std::function<Stream(const Request&, CallContext&)> stream_factory;
    // true = exchange (bidi), false = producer.  Used to pick the stream
    // dispatch shape; not surfaced in __describe__ (which reports null).
    bool is_exchange = false;
};

class Server;

// Populate `rec`'s request_data — or, when the payload would blow the writer's
// per-record cap, its `original_request_bytes` accounting instead.  Measures
// before serializing, so an over-cap payload is never materialized.
VGI_RPC_EXPORT void fill_request_data(const AccessLogWriter& log, AccessRecord& rec,
                                      const std::shared_ptr<arrow::RecordBatch>& batch);

class VGI_RPC_EXPORT ServerBuilder {
public:
    ServerBuilder() = default;

    // Register a unary method
    ServerBuilder& add_unary(const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
                             std::shared_ptr<arrow::Schema> result_schema,
                             std::function<Result(const Request&, CallContext&)> handler,
                             const std::string& doc = "");

    // Register a void unary method
    ServerBuilder& add_void(const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
                            std::function<void(const Request&, CallContext&)> handler,
                            const std::string& doc = "");

    // Register a producer stream method
    ServerBuilder& add_producer(const std::string& name,
                                std::shared_ptr<arrow::Schema> params_schema,
                                std::shared_ptr<arrow::Schema> output_schema,
                                std::function<Stream(const Request&, CallContext&)> factory,
                                const std::string& doc = "",
                                std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Register an exchange stream method
    ServerBuilder& add_exchange(const std::string& name,
                                std::shared_ptr<arrow::Schema> params_schema,
                                std::shared_ptr<arrow::Schema> input_schema,
                                std::shared_ptr<arrow::Schema> output_schema,
                                std::function<Stream(const Request&, CallContext&)> factory,
                                const std::string& doc = "",
                                std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Set a deterministic server ID (defaults to random_hex(12) if not set).
    ServerBuilder& server_id(std::string id);

    // Answer __transport_options__ (WIRE_PROTOCOL §15), advertising whether
    // the shared-memory side channel is usable.  A worker that implements SHM
    // MUST answer this method, or clients will never negotiate it; a worker
    // that omits it returns method_not_implemented and clients stay on the
    // pipe, which is also conformant.
    ServerBuilder& enable_transport_options(bool enabled = true);

    // Enable __describe__ introspection.
    // The describe response is a snapshot captured at build() time.
    ServerBuilder& enable_describe(const std::string& protocol_name = "");

    // Declare the application protocol surface version (canonical semver
    // MAJOR.MINOR.PATCH).  Surfaced in the __describe__ response under
    // vgi_rpc.protocol_version so version-aware clients can discover it.
    ServerBuilder& protocol_version(std::string version);

    // Install an optional process-local lifecycle hook.  It runs once, with
    // the concrete transport kind, before that transport dispatches its first
    // request.  A throwing hook leaves startup uncommitted so the next request
    // retries it.
    ServerBuilder& on_serve_start(std::function<void(TransportKind)> hook);

    // Enable the vgi_rpc.access JSONL access log, written to `path`.  One
    // record per completed call.  Empty path (the default) disables it.
    // `max_record_bytes` caps one emitted line; over-cap records shed fields
    // per docs/access-log-spec.md §5b rather than being dropped downstream.
    ServerBuilder& access_log(std::string path, int64_t max_record_bytes = kDefaultMaxRecordBytes);

    // Build the server
    std::unique_ptr<Server> build();

private:
    void check_duplicate(const std::string& name) const;

    std::vector<MethodInfo> methods_;
    bool describe_enabled_ = false;
    bool built_ = false;
    std::string protocol_name_;
    std::string server_id_;
    std::string protocol_version_;
    std::string access_log_path_;
    bool transport_options_enabled_ = false;
    std::function<void(TransportKind)> on_serve_start_;
    int64_t access_log_max_record_bytes_ = kDefaultMaxRecordBytes;
};

// NOT thread-safe.  Designed for single-threaded pipe-based operation
// (one request at a time on stdin/stdout).
class VGI_RPC_EXPORT Server {
    friend class ServerBuilder;

public:
    void run();

    // Serve over HTTP (cpp-httplib) instead of stdin/stdout.  Blocks until the
    // server stops.  Prints "PORT:<n>" to stdout once bound, so a caller that
    // asked for port 0 learns which one it got.
    void serve_http(const HttpConfig& config);

    // Convenience overload for the common host/port/cap case.
    void serve_http(const std::string& host, int port, int64_t max_response_bytes = -1);

    // Serve over a Unix domain socket, printing "UNIX:<path>" once bound.
    void serve_unix(const std::string& path);

    // Serve over TCP with the same raw Arrow-IPC framing as the Unix socket
    // (no HTTP envelope), printing "TCP:<host>:<port>" once bound.  Carries no
    // auth or TLS — trusted networks only.
    void serve_tcp(const std::string& host, int port);

    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& protocol_name() const noexcept { return protocol_name_; }
    const std::unordered_map<std::string, MethodInfo>& methods() const noexcept { return methods_; }
    // The reason a request's declared application protocol version is
    // incompatible with this server's, or empty when it is fine.
    //
    // Enforced here rather than left to the client because a mismatch means
    // the two sides disagree about what the *payloads* mean: caught at the
    // dispatch boundary it is one clear error, and caught later it is a
    // schema mismatch somewhere inside a method. Compared on major and minor
    // only — a patch release does not change the surface. A server that
    // declared no version enforces nothing.
    std::string protocol_version_error(
        const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) const;

    // Returns false on EOF (clean shutdown), true when a request was served.
    bool serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                   const std::shared_ptr<arrow::io::OutputStream>& output);

    // Explicit-kind overload for raw-transport integrations.  The original
    // overload remains source and binary compatible and means PIPE.
    bool serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                   const std::shared_ptr<arrow::io::OutputStream>& output,
                   TransportKind transport_kind);

    // Notify the lifecycle hook for a transport.  Public for custom transport
    // adapters; normal users call one of the serve_* entry points instead.
    void notify_serve_start(TransportKind transport_kind);

    // Unary dispatch driven by a caller-supplied context, so the HTTP
    // transport can install its per-request sticky machinery.  Returns true
    // when the method raised — the caller needs that to set X-VGI-RPC-Error,
    // which is the only thing distinguishing a failure from a result on a
    // response that is 200 either way.
    bool serve_unary_http(const MethodInfo& method_info, const Request& request,
                          const std::string& request_id,
                          const std::shared_ptr<arrow::io::OutputStream>& output, CallContext& ctx);

private:
    Server(std::unordered_map<std::string, MethodInfo> methods, std::string server_id,
           std::string protocol_name, std::string protocol_hash, std::string protocol_version,
           const std::string& access_log_path, int64_t access_log_max_record_bytes,
           std::function<void(TransportKind)> on_serve_start);

    void serve_unary(const MethodInfo& method_info, const Request& request,
                     const std::string& request_id,
                     const std::shared_ptr<arrow::io::OutputStream>& output,
                     TransportKind transport_kind);

    void serve_stream(const MethodInfo& method_info, const Request& request,
                      const std::string& request_id,
                      const std::shared_ptr<arrow::io::InputStream>& input,
                      const std::shared_ptr<arrow::io::OutputStream>& output,
                      TransportKind transport_kind);

    // Attach (or reuse) the peer-owned segment this request advertises.
    // Cached per connection because a segment is process-level, not per-call.
    void refresh_shm(const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata);

    std::unordered_map<std::string, MethodInfo> methods_;
    std::string server_id_;
    std::string protocol_name_;
    std::string protocol_hash_;
    std::string protocol_version_;
    std::unique_ptr<AccessLogWriter> access_log_;
    std::function<void(TransportKind)> on_serve_start_;
    std::once_flag serve_start_once_;
    std::optional<TransportKind> transport_kind_;

    // Segment the peer advertised, and the segment for the call in flight.
    // The second is set only when the client signalled SHM for *this* call, so
    // a response is never routed through a channel the caller is not reading.
    std::shared_ptr<ShmSegment> shm_;
    std::string shm_name_;
    std::shared_ptr<ShmSegment> call_shm_;
};

}  // namespace vgi_rpc
