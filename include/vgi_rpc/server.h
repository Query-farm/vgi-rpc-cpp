// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
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
#include "vgi_rpc/proxy_protocol_v2.h"
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
    // dispatch shape; reflection reports it as `stream_kind`.
    bool is_exchange = false;
};

class Server;
class IdentityImpl;

/// One hosted protocol's wire name and canonical digest, carried together.
///
/// Together because an access record that names one protocol and carries
/// another's digest is worse than either field being wrong alone:
/// docs/access-log-spec.md §3 makes `protocol_hash` "the registry key when
/// decoding archived records", so such a record is decoded against the wrong
/// description -- and it is well-formed, passes the schema, and looks right.
/// Splitting the pair across two lookups is how that divergence arose in the
/// reference, so here there is one lookup and it returns both.
struct VGI_RPC_EXPORT ProtocolIdentity {
    std::string name;
    std::string hash;
};

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

    // Declare this server's application protocol -- its routing key.
    //
    // Requests name the protocol they address and dispatch resolves the pair
    // `(protocol, method)`, so this is what makes the application's methods
    // addressable: over HTTP they are served at `{prefix}/{protocol}/{method}`,
    // and on the raw transports a request must carry the name in
    // `vgi_rpc.protocol`.  The major version belongs in the name
    // (`MyService.v2`), which is what lets two majors be served side by side.
    //
    // A server that declares none keeps the pre-multi-service shape: flat
    // `{prefix}/{method}` routes, and no routing key required.  There is then
    // exactly one namespace and no name for it -- workable, but an
    // intermediary that rebuilds a request cannot be told it landed in the
    // wrong place.
    ServerBuilder& protocol(std::string protocol_name);

    // Host vgi_rpc.Identity.v1.  Absent by default, and absent rather than
    // routed-and-refusing when omitted: that is what keeps a dependency
    // upgrade from growing a credential-to-identity oracle on every existing
    // worker.  Only the methods whose hooks the deployment configured are
    // hosted, and the protocol hash narrows with them.
    ServerBuilder& identity(std::shared_ptr<IdentityImpl> impl);

    // Declare the application protocol surface version (canonical semver
    // MAJOR.MINOR.PATCH).  Reported by `vgi_rpc.Reflection.v1` as
    // `protocol_version` so version-aware clients can discover it.
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
    bool built_ = false;
    std::string protocol_name_;
    std::string server_id_;
    std::string protocol_version_;
    std::string access_log_path_;
    std::shared_ptr<IdentityImpl> identity_;
    bool transport_options_enabled_ = false;
    std::function<void(TransportKind)> on_serve_start_;
    int64_t access_log_max_record_bytes_ = kDefaultMaxRecordBytes;
};

// Pipe operation dispatches one request at a time. HTTP and the Unix/TCP
// listeners may invoke unrelated handlers concurrently; implementations that
// share mutable state between calls must provide their own synchronization.
// Calls remain ordered within one raw connection. Stream turns and sticky
// session calls are serialized per state object by the HTTP transport.
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

    // Raw TCP behind a trusted PROXY v2 sender. The original overload remains
    // unchanged and consumes VGI framing immediately.
    void serve_tcp(const std::string& host, int port, const TcpServerOptions& options);

    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& protocol_name() const noexcept { return protocol_name_; }

    /// Serve one call to the co-hosted reflection protocol.
    ///
    /// `errored`, when supplied, reports whether the reply carries an error
    /// batch rather than a result. The raw transports do not need it -- the
    /// error rides the stream either way -- but HTTP answers 200 for both and
    /// distinguishes them only by the `X-VGI-RPC-Error` header.
    bool serve_reflection(const std::shared_ptr<arrow::io::OutputStream>& output,
                          const std::string& method_name,
                          const std::shared_ptr<arrow::RecordBatch>& request_batch,
                          const std::string& request_id, bool* errored = nullptr);

    /// Serve one call to the co-hosted identity protocol.
    ///
    /// Takes the connection's AuthContext rather than reading one: every guard
    /// here turns on *who is asking*, and a transport that cannot say has no
    /// authenticated principal, which is the answer that fails closed.
    /// `errored` reports an error reply, for the same reason as above.
    bool serve_identity(const std::shared_ptr<arrow::io::OutputStream>& output,
                        const std::string& method_name,
                        const std::shared_ptr<arrow::RecordBatch>& request_batch,
                        const std::string& request_id, const AuthContext& auth,
                        bool* errored = nullptr);

    /// The hosted identity implementation, or null when the protocol is absent.
    const std::shared_ptr<IdentityImpl>& identity() const noexcept { return identity_; }

    /// The binding that owns this server's application surface.
    ///
    /// Public because a transport that drives dispatch itself has to name an
    /// owning binding to build an access record at all, and the one thing it
    /// must not do is invent one.  Every application method and every stream
    /// belongs here; so do the framework endpoints owned by no protocol
    /// (`__transport_options__`, `__upload_url__`), which
    /// docs/access-log-spec.md §3 prescribes log the server's primary rather
    /// than merely tolerating it.
    const ProtocolIdentity& application_binding() const noexcept { return application_binding_; }

    /// The configured access-log writer, or nullptr when none was configured.
    ///
    /// Public because HTTP drives a stream's turns itself: `serve_stream` never
    /// runs there, so the Server never sees an `init` or a continuation and
    /// cannot file their records.  Handing the writer out cannot reintroduce
    /// the server-wide protocol default this area exists to prevent -- the
    /// writer holds no protocol identity, and `AccessRecord` has no default
    /// constructor, so a caller still has to name a binding to build a record.
    AccessLogWriter* access_log() noexcept { return access_log_.get(); }
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

    // Identity-aware overload for custom connection transports (for example
    // Iroh or a trusted proxy adapter). The adapter resolves/authenticates the
    // peer once and supplies the immutable connection snapshot here; every
    // handler and stream turn then observes the same values.
    bool serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                   const std::shared_ptr<arrow::io::OutputStream>& output,
                   TransportKind transport_kind, const AuthContext& auth,
                   const PeerEvidenceSet& peer_evidence);

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
    struct ConnectionState {
        std::shared_ptr<ShmSegment> shm;
        std::string shm_name;
        std::shared_ptr<ShmSegment> call_shm;
    };

    // The protocol hash is not a parameter: a Server computes the canonical
    // digest of every binding it hosts, which is the same value reflection
    // reports.  Passing one in is what let the access log carry a digest the
    // wire never advertised.
    Server(std::unordered_map<std::string, MethodInfo> methods, std::string server_id,
           std::string protocol_name, std::string protocol_version,
           const std::string& access_log_path, int64_t access_log_max_record_bytes,
           std::function<void(TransportKind)> on_serve_start,
           std::shared_ptr<IdentityImpl> identity);

    // Emit one access record for a unary call to a co-hosted framework
    // protocol.  `owner` is the binding that owns the method, never this
    // server's primary: reflection and identity are protocols in their own
    // right, and a record filed under the application's name merges two
    // protocols' traffic with nothing to show it happened.
    void log_framework_call(const ProtocolIdentity& owner, const std::string& method,
                            const std::string& request_id,
                            const std::shared_ptr<arrow::RecordBatch>& request_batch,
                            std::chrono::steady_clock::time_point started,
                            const std::string& error_type, const std::string& error_message);

    void serve_unary(const MethodInfo& method_info, const Request& request,
                     const std::string& request_id,
                     const std::shared_ptr<arrow::io::OutputStream>& output,
                     TransportKind transport_kind, const std::shared_ptr<ShmSegment>& call_shm,
                     const AuthContext& auth, const PeerEvidenceSet& peer_evidence);

    bool serve_unary_impl(const MethodInfo& method_info, const Request& request,
                          const std::string& request_id,
                          const std::shared_ptr<arrow::io::OutputStream>& output, CallContext& ctx,
                          const std::shared_ptr<ShmSegment>& call_shm);

    void serve_stream(const MethodInfo& method_info, const Request& request,
                      const std::string& request_id,
                      const std::shared_ptr<arrow::io::InputStream>& input,
                      const std::shared_ptr<arrow::io::OutputStream>& output,
                      TransportKind transport_kind, ConnectionState& connection,
                      const AuthContext& auth, const PeerEvidenceSet& peer_evidence);

    bool serve_one_with_state(const std::shared_ptr<arrow::io::InputStream>& input,
                              const std::shared_ptr<arrow::io::OutputStream>& output,
                              TransportKind transport_kind, ConnectionState& connection,
                              const AuthContext& auth = AuthContext::anonymous(),
                              const PeerEvidenceSet& peer_evidence = PeerEvidenceSet(),
                              std::function<void()> first_frame_complete = {});

    void serve_socket_fd(
        int fd, TransportKind transport_kind, const AuthContext& auth = AuthContext::anonymous(),
        const PeerEvidenceSet& peer_evidence = PeerEvidenceSet(),
        std::chrono::steady_clock::time_point first_frame_deadline =
            std::chrono::steady_clock::time_point::max(),
        std::chrono::milliseconds idle_read_timeout = std::chrono::milliseconds::max(),
        std::chrono::milliseconds write_timeout = std::chrono::milliseconds::max());

    // Attach (or reuse) the peer-owned segment this request advertises.
    // Cached per connection because a segment is process-level, not per-call.
    void refresh_shm(ConnectionState& connection,
                     const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata);

    std::unordered_map<std::string, MethodInfo> methods_;
    std::string server_id_;
    std::string protocol_name_;
    std::string protocol_version_;

    // Every binding this server hosts, fingerprinted once at construction.
    //
    // One computation feeds both the `list_protocols` reply and the access
    // log, so the digest an operator reads out of an archived record is the
    // same string a client compared against -- which is the only way the hash
    // is usable as a registry key.  `identity_binding_.name` is empty when no
    // identity implementation was configured, because the protocol is then
    // absent rather than hosted-and-empty.
    ProtocolIdentity application_binding_;
    ProtocolIdentity reflection_binding_;
    ProtocolIdentity identity_binding_;
    std::unordered_map<std::string, MethodInfo> identity_methods_;
    std::unique_ptr<AccessLogWriter> access_log_;
    std::shared_ptr<IdentityImpl> identity_;
    std::function<void(TransportKind)> on_serve_start_;
    std::once_flag serve_start_once_;
    std::optional<TransportKind> transport_kind_;

    // State for the public serve_one API's one logical pipe connection. Socket
    // listeners allocate a separate state object per accepted connection.
    ConnectionState default_connection_state_;
};

}  // namespace vgi_rpc
