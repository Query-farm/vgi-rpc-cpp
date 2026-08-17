// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>
#include <memory>
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
    std::shared_ptr<arrow::Schema> output_schema;   // nullptr for unary
    std::shared_ptr<arrow::Schema> header_schema;   // nullptr if no header
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
    ServerBuilder& add_unary(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> result_schema,
        std::function<Result(const Request&, CallContext&)> handler,
        const std::string& doc = "");

    // Register a void unary method
    ServerBuilder& add_void(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::function<void(const Request&, CallContext&)> handler,
        const std::string& doc = "");

    // Register a producer stream method
    ServerBuilder& add_producer(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> output_schema,
        std::function<Stream(const Request&, CallContext&)> factory,
        const std::string& doc = "",
        std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Register an exchange stream method
    ServerBuilder& add_exchange(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> input_schema,
        std::shared_ptr<arrow::Schema> output_schema,
        std::function<Stream(const Request&, CallContext&)> factory,
        const std::string& doc = "",
        std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Set a deterministic server ID (defaults to random_hex(12) if not set).
    ServerBuilder& server_id(std::string id);

    // Enable __describe__ introspection.
    // The describe response is a snapshot captured at build() time.
    ServerBuilder& enable_describe(const std::string& protocol_name = "");

    // Declare the application protocol surface version (canonical semver
    // MAJOR.MINOR.PATCH).  Surfaced in the __describe__ response under
    // vgi_rpc.protocol_version so version-aware clients can discover it.
    ServerBuilder& protocol_version(std::string version);

    // Enable the vgi_rpc.access JSONL access log, written to `path`.  One
    // record per completed call.  Empty path (the default) disables it.
    // `max_record_bytes` caps one emitted line; over-cap records shed fields
    // per docs/access-log-spec.md §5b rather than being dropped downstream.
    ServerBuilder& access_log(std::string path,
                              int64_t max_record_bytes = kDefaultMaxRecordBytes);

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

    // Returns false on EOF (clean shutdown), true when a request was served.
    bool serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                   const std::shared_ptr<arrow::io::OutputStream>& output);

    // Unary dispatch driven by a caller-supplied context, so the HTTP
    // transport can install its per-request sticky machinery.  Returns true
    // when the method raised — the caller needs that to set X-VGI-RPC-Error,
    // which is the only thing distinguishing a failure from a result on a
    // response that is 200 either way.
    bool serve_unary_http(const MethodInfo& method_info,
                          const Request& request,
                          const std::string& request_id,
                          const std::shared_ptr<arrow::io::OutputStream>& output,
                          CallContext& ctx);

private:
    Server(std::unordered_map<std::string, MethodInfo> methods,
           std::string server_id,
           std::string protocol_name,
           std::string protocol_hash,
           const std::string& access_log_path,
           int64_t access_log_max_record_bytes = kDefaultMaxRecordBytes);

    void serve_unary(const MethodInfo& method_info,
                     const Request& request,
                     const std::string& request_id,
                     const std::shared_ptr<arrow::io::OutputStream>& output);

    void serve_stream(const MethodInfo& method_info,
                      const Request& request,
                      const std::string& request_id,
                      const std::shared_ptr<arrow::io::InputStream>& input,
                      const std::shared_ptr<arrow::io::OutputStream>& output);

    std::unordered_map<std::string, MethodInfo> methods_;
    std::string server_id_;
    std::string protocol_name_;
    std::string protocol_hash_;
    std::unique_ptr<AccessLogWriter> access_log_;
};

}  // namespace vgi_rpc
