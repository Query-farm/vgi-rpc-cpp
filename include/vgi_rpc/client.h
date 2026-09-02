// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Blocking native client for the raw Arrow IPC transports.
///
/// A raw transport is a trusted, ordered byte stream: subprocess stdio, a
/// caller-supplied pair of Arrow streams, an AF_UNIX socket, or plain TCP.
/// Raw TCP deliberately provides neither authentication nor TLS; use
/// HttpClient when either is required.
#pragma once

#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/client_description.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/log.h"

namespace vgi_rpc {

enum class ClientStderrMode {
    INHERIT,
    DISCARD,
};

struct SubprocessTransportOptions {
    ClientStderrMode stderr_mode = ClientStderrMode::INHERIT;
    std::chrono::milliseconds close_grace{2000};
    std::chrono::milliseconds terminate_grace{2000};
};

struct SocketTransportOptions {
    // A finite default prevents a dead peer/address from pinning construction.
    // One monotonic budget covers every address attempted by connect_tcp plus
    // SOCKS negotiation when proxy is set. Elapsed blocking system-resolver
    // time is charged to that budget, but getaddrinfo itself is not preemptible.
    // nullopt deliberately requests an unbounded connection setup.
    std::optional<std::chrono::milliseconds> connect_timeout{std::chrono::milliseconds{5000}};
    std::optional<std::chrono::milliseconds> read_timeout;
    std::optional<std::chrono::milliseconds> write_timeout;
    // Optional SOCKS5 proxy URI, e.g. socks5h://127.0.0.1:1055. Only
    // proxy-side hostname resolution and the NO AUTH method are supported;
    // URI userinfo is rejected. C++ callers must supply Unicode target names as
    // pre-encoded ASCII IDNA A-labels. A proxy failure never falls back direct.
    std::optional<std::string> proxy;
};

inline constexpr const char* IROH_ARROW_MUX_ALPN = "vgi-rpc/arrow-mux/1";
inline constexpr const char* IROH_HTTP_ALPN = "iroh-http/2";

enum class IrohErrorStage : uint32_t {
    PARSE = 1,
    BIND = 2,
    RESOLVE = 3,
    CONNECT = 4,
    ALPN = 5,
    OPEN_STREAM = 6,
    WRITE = 7,
    READ = 8,
    CANCEL = 9,
    CLOSE = 10,
    INTERNAL = 11,
};
enum class IrohErrorCategory : uint32_t {
    INVALID_INPUT = 1,
    UNSUPPORTED = 2,
    UNAVAILABLE = 3,
    TIMEOUT = 4,
    PROTOCOL = 5,
    CONNECTION_RESET = 6,
    CANCELLED = 7,
    AUTHENTICATION = 8,
    RESOURCE_EXHAUSTED = 9,
    INTERNAL = 10,
};
enum class IrohDispatchCertainty : uint32_t { NOT_SENT = 0, UNKNOWN = 1, SENT = 2 };

/// Portable Iroh failure dimensions; dispatch certainty controls safe retries.
class VGI_RPC_EXPORT IrohTransportError : public std::runtime_error {
public:
    IrohTransportError(std::string message, IrohErrorStage stage, IrohErrorCategory category,
                       IrohDispatchCertainty dispatch_certainty)
        : std::runtime_error(std::move(message)),
          stage_(stage),
          category_(category),
          dispatch_certainty_(dispatch_certainty) {}
    IrohErrorStage stage() const noexcept { return stage_; }
    IrohErrorCategory category() const noexcept { return category_; }
    IrohDispatchCertainty dispatch_certainty() const noexcept { return dispatch_certainty_; }

private:
    IrohErrorStage stage_;
    IrohErrorCategory category_;
    IrohDispatchCertainty dispatch_certainty_;
};

/// Arrow statuses from native I/O retain the same structured C-ABI fields.
class VGI_RPC_EXPORT IrohStatusDetail : public arrow::StatusDetail {
public:
    IrohStatusDetail(IrohErrorStage stage, IrohErrorCategory category,
                     IrohDispatchCertainty dispatch_certainty, std::string message = {})
        : stage_(stage),
          category_(category),
          dispatch_certainty_(dispatch_certainty),
          message_(std::move(message)) {}
    const char* type_id() const override { return "vgi_rpc::IrohStatusDetail"; }
    std::string ToString() const override;
    IrohErrorStage stage() const noexcept { return stage_; }
    IrohErrorCategory category() const noexcept { return category_; }
    IrohDispatchCertainty dispatch_certainty() const noexcept { return dispatch_certainty_; }

private:
    IrohErrorStage stage_;
    IrohErrorCategory category_;
    IrohDispatchCertainty dispatch_certainty_;
    std::string message_;
};

struct IrohEndpoint {
    enum class Scheme { IROH, HTTPI };
    Scheme scheme;
    std::string endpoint_id;
    std::array<uint8_t, 32> endpoint_id_bytes{};
    std::string base_path;
    std::string alpn;

    static IrohEndpoint parse(const std::string& uri);
};

struct IrohTransportOptions {
    std::optional<std::array<uint8_t, 32>> secret_key;
    std::vector<std::string> relay_urls;
    bool no_relay = false;
    /// Optional route hints for the remote endpoint (not local relay selection).
    std::optional<std::string> remote_relay_url;
    std::vector<std::string> direct_addresses;
    std::chrono::milliseconds connect_timeout{30000};
    std::chrono::milliseconds io_timeout{300000};
    /// Called by blocking native operations; true cancels only the current logical operation.
    std::function<bool()> cancel_check;
};

/// Owning, move-only pair of raw input/output streams.
///
/// `from_streams` takes shared ownership of both Arrow streams and closes them
/// with the transport. `spawn` executes argv directly (never through a shell).
class VGI_RPC_EXPORT ClientTransport {
public:
    ~ClientTransport();
    ClientTransport(ClientTransport&&) noexcept;
    ClientTransport& operator=(ClientTransport&&) noexcept;
    ClientTransport(const ClientTransport&) = delete;
    ClientTransport& operator=(const ClientTransport&) = delete;

    static ClientTransport from_streams(std::shared_ptr<arrow::io::InputStream> input,
                                        std::shared_ptr<arrow::io::OutputStream> output);
    static ClientTransport spawn(const std::vector<std::string>& argv,
                                 const SubprocessTransportOptions& options = {});
    // Genuine AF_UNIX on both POSIX and Windows (Windows 10 1803+ has real
    // kernel AF_UNIX support, via afunix.h) - NOT the same thing as
    // connect_pipe below. Use this for an explicit unix:// location
    // against a worker that binds a real AF_UNIX socket (confirmed
    // working: vgi-go's --unix flag does this on Windows too).
    static ClientTransport connect_unix(const std::string& path,
                                        const SocketTransportOptions& options = {});
    // Windows named pipe (\\.\pipe\<name>) - the AF_UNIX worker launcher
    // protocol's actual Windows rendezvous mechanism (docs/launcher-
    // protocol.md's own "Platform: Windows" section: CPython has no
    // socket.AF_UNIX on Windows, so vgi_rpc's own worker CLI serves a
    // named pipe there, not an AF_UNIX socket, despite --unix's name).
    // POSIX-only builds throw - AF_UNIX (connect_unix above) already
    // covers the equivalent need there, a named pipe has no POSIX
    // analog worth adding one for.
    static ClientTransport connect_pipe(const std::string& pipe_name,
                                        const SocketTransportOptions& options = {});
    // Raw TCP currently follows the server and is POSIX-only. It has no TLS or
    // authentication and must be limited to a trusted network.
    static ClientTransport connect_tcp(const std::string& host, uint16_t port,
                                       const SocketTransportOptions& options = {});

    bool is_open() const noexcept;
    void close();

private:
    class Impl;
    explicit ClientTransport(std::unique_ptr<Impl> impl);

    std::shared_ptr<arrow::io::InputStream> input() const;
    std::shared_ptr<arrow::io::OutputStream> output() const;

    std::unique_ptr<Impl> impl_;

    friend class RpcClient;
    friend class ClientStream;
};

/// Optional native implementation seam, normally backed by the version-matched
/// vgi-iroh C ABI static library. No connector is downloaded or spawned.
using IrohTransportProvider =
    std::function<ClientTransport(const IrohEndpoint&, const IrohTransportOptions&)>;

VGI_RPC_EXPORT ClientTransport connect_iroh_transport(const std::string& endpoint,
                                                      const IrohTransportProvider& provider,
                                                      const IrohTransportOptions& options = {});

/// Return the linked vgi-iroh C ABI provider. Throws a clear unsupported error
/// when the library was built without VGI_RPC_WITH_IROH_CABI.
VGI_RPC_EXPORT IrohTransportProvider native_iroh_transport_provider();
VGI_RPC_EXPORT bool native_iroh_transport_available() noexcept;
/// Return the process-shared provider's stable local EndpointId, creating it if necessary.
VGI_RPC_EXPORT std::string native_iroh_endpoint_id(const IrohTransportOptions& options = {});

/// A remote exception envelope, preserving both human- and machine-readable
/// fields from the wire.
class VGI_RPC_EXPORT RpcException : public std::runtime_error {
public:
    RpcException(std::string exception_type, std::string message, std::string error_kind = {},
                 std::string server_id = {}, std::string request_id = {});

    const std::string& exception_type() const noexcept { return exception_type_; }
    const std::string& error_kind() const noexcept { return error_kind_; }
    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& request_id() const noexcept { return request_id_; }

private:
    std::string exception_type_;
    std::string error_kind_;
    std::string server_id_;
    std::string request_id_;
};

using ClientLogHandler = std::function<void(const Message&)>;

struct RpcClientOptions {
    std::string protocol_version;
    ClientLogHandler on_log;
    // Zero disables SHM. A positive value performs __transport_options__ at
    // construction and creates a POSIX segment only when both peers support it.
    size_t shared_memory_bytes = 0;
};

struct ClientTransportOptions {
    bool shm = false;
    std::shared_ptr<arrow::KeyValueMetadata> raw;
};

enum class ClientStreamKind {
    PRODUCER,
    EXCHANGE,
};

/// One live producer/exchange call. The raw protocol is lockstep and stateful;
/// this object is its continuation handle (there is no stateless raw resume
/// endpoint or cursor token).
class VGI_RPC_EXPORT ClientStream {
public:
    ~ClientStream();
    ClientStream(ClientStream&&) noexcept;
    ClientStream& operator=(ClientStream&&) noexcept;
    ClientStream(const ClientStream&) = delete;
    ClientStream& operator=(const ClientStream&) = delete;

    ClientStreamKind kind() const noexcept;
    const std::optional<AnnotatedBatch>& header() const noexcept;
    bool finished() const noexcept;

    std::optional<AnnotatedBatch> tick();
    std::optional<AnnotatedBatch> exchange(
        const std::shared_ptr<arrow::RecordBatch>& input,
        std::shared_ptr<arrow::KeyValueMetadata> metadata = nullptr);
    // Explicit cancellation/close performs the protocol drain and may block
    // up to the transport's configured I/O deadline. Destruction never drains:
    // abandoning a live stream aborts its connection instead.
    void cancel();
    void close();

private:
    class Impl;
    explicit ClientStream(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;

    friend class RpcClient;
};

/// Dynamic, schema-first client for raw Arrow IPC transports.
///
/// RpcClient is intentionally single-call-at-a-time. A live ClientStream
/// reserves the connection until it is closed, cancelled, or destroyed. Raw
/// clients reject external-location and stateless-resume envelopes; those are
/// HTTP transport features.
class VGI_RPC_EXPORT RpcClient {
public:
    explicit RpcClient(ClientTransport transport, const RpcClientOptions& options = {});
    ~RpcClient();
    RpcClient(RpcClient&&) noexcept;
    RpcClient& operator=(RpcClient&&) noexcept;
    RpcClient(const RpcClient&) = delete;
    RpcClient& operator=(const RpcClient&) = delete;

    static RpcClient spawn(const std::vector<std::string>& argv,
                           const RpcClientOptions& client_options = {},
                           const SubprocessTransportOptions& transport_options = {});
    static RpcClient connect_unix(const std::string& path, const RpcClientOptions& options = {},
                                  const SocketTransportOptions& transport_options = {});
    static RpcClient connect_pipe(const std::string& pipe_name,
                                  const RpcClientOptions& options = {},
                                  const SocketTransportOptions& transport_options = {});
    static RpcClient connect_tcp(const std::string& host, uint16_t port,
                                 const RpcClientOptions& options = {},
                                 const SocketTransportOptions& transport_options = {});
    static RpcClient connect_iroh(const std::string& endpoint,
                                  const IrohTransportProvider& provider,
                                  const RpcClientOptions& options = {},
                                  const IrohTransportOptions& transport_options = {});

    AnnotatedBatch call_unary(const std::string& method,
                              const std::shared_ptr<arrow::RecordBatch>& params,
                              std::shared_ptr<arrow::KeyValueMetadata> metadata = nullptr);

    ClientStream open_producer(const std::string& method,
                               const std::shared_ptr<arrow::RecordBatch>& params,
                               bool has_header = false,
                               std::shared_ptr<arrow::KeyValueMetadata> metadata = nullptr);
    ClientStream open_exchange(const std::string& method,
                               const std::shared_ptr<arrow::RecordBatch>& params,
                               bool has_header = false,
                               std::shared_ptr<arrow::KeyValueMetadata> metadata = nullptr);

    ServiceDescription describe();
    ClientTransportOptions transport_options();
    bool enable_shared_memory(size_t bytes);
    bool shared_memory_enabled() const noexcept;
    uint32_t shared_memory_live_allocations() const noexcept;

    void close();

private:
    class Impl;

    ClientStream open_stream(const std::string& method,
                             const std::shared_ptr<arrow::RecordBatch>& params, bool has_header,
                             std::shared_ptr<arrow::KeyValueMetadata> metadata,
                             ClientStreamKind kind);

    std::shared_ptr<Impl> impl_;

    friend class ClientStream;
};

}  // namespace vgi_rpc
