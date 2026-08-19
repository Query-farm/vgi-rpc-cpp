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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
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
    // It applies to each resolved address attempted by connect_tcp.
    // DNS resolution follows the platform resolver and is outside this timer;
    // nullopt deliberately requests an unbounded socket connect.
    std::optional<std::chrono::milliseconds> connect_timeout{std::chrono::milliseconds{5000}};
    std::optional<std::chrono::milliseconds> read_timeout;
    std::optional<std::chrono::milliseconds> write_timeout;
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
    // Unix sockets are POSIX-only.
    static ClientTransport connect_unix(const std::string& path,
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
    static RpcClient connect_tcp(const std::string& host, uint16_t port,
                                 const RpcClientOptions& options = {},
                                 const SocketTransportOptions& transport_options = {});

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
