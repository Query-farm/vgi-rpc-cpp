// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Native HTTP client for unary and typed exchange RPCs.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/type_fwd.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct HttpServerCapabilities {
    bool sticky_enabled = false;
    std::optional<int64_t> sticky_default_ttl;
    std::vector<std::string> sticky_echo_headers;
    bool upload_url_support = false;
    std::optional<int64_t> max_request_bytes;
    std::optional<int64_t> max_response_bytes;
    std::optional<int64_t> max_externalized_response_bytes;
    bool externalization_enabled = false;
    std::optional<int64_t> max_upload_bytes;
    // An absent advertisement is the legacy-server answer: zstd.  A
    // present-but-empty header replaces this with an empty vector.
    std::vector<std::string> supported_encodings = {"zstd"};
};

struct HttpClientConfig {
    // RPC mount prefix.  Empty means bare method paths.
    std::string prefix = "/vgi";
    // Optional application protocol version stamped on every request.
    std::string protocol_version;
    // Independent mandatory local allocation guards.  Both must be positive;
    // the client never offers an unbounded buffering mode.
    int64_t max_request_bytes = 64LL * 1024 * 1024;
    // Compatibility ceiling used for both encoded and decoded responses unless
    // either more-specific limit below is positive.
    int64_t max_response_bytes = 256LL * 1024 * 1024;
    int connection_timeout_seconds = 10;
    int read_timeout_seconds = 30;
    int write_timeout_seconds = 30;
    bool keep_alive = true;
    // Sending credentials over plain HTTP is unsafe and therefore refused by
    // default.  Set this only for an already-protected local/private channel.
    bool allow_insecure_credentials = false;
    std::map<std::string, std::string> headers;
    // Non-exception log batches are not returned as data.  Install this hook
    // to observe them; leaving it empty discards them.
    std::function<void(const AnnotatedBatch&)> on_log;
    // zstd request-body compression level.  nullopt sends identity request
    // bodies.  The client always supports bounded zstd response decoding.
    std::optional<int> compression_level = 3;
    // Independent wire and post-decompression response limits.  Zero inherits
    // max_response_bytes, preserving existing configurations and guarantees.
    int64_t max_encoded_response_bytes = 0;
    int64_t max_decoded_response_bytes = 0;
};

class VGI_RPC_EXPORT HttpClientError : public std::runtime_error {
public:
    HttpClientError(std::string message, int http_status = 0)
        : std::runtime_error(std::move(message)), http_status_(http_status) {}

    int http_status() const noexcept { return http_status_; }

private:
    int http_status_;
};

class VGI_RPC_EXPORT RpcRemoteError : public HttpClientError {
public:
    RpcRemoteError(std::string exception_type, std::string message, std::string error_kind,
                   std::string server_id, std::string request_id, int http_status = 200);

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

class HttpExchangeSession;
class HttpClientState;

class VGI_RPC_EXPORT HttpClient {
public:
    // This implementation intentionally accepts plain http:// origins only.
    // TLS, external-location fetching and producer iteration are not implied
    // by this focused unary/exchange API.
    explicit HttpClient(std::string base_url, HttpClientConfig config = {});
    ~HttpClient();

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) noexcept = default;
    HttpClient& operator=(HttpClient&&) noexcept = default;

    // Invoke a unary method.  The request batch's declared schema is written
    // verbatim; expected_output_schema, when present, is enforced exactly.
    AnnotatedBatch call(const std::string& method, const AnnotatedBatch& request,
                        std::shared_ptr<arrow::Schema> expected_output_schema = nullptr) const;

    // Query OPTIONS {prefix}/health once and cache its advertised transport
    // capabilities.  Ordinary RPC responses refine the same cache.
    HttpServerCapabilities capabilities() const;

    // Initialize a bidirectional exchange.  Every input and output batch is
    // checked against the declared schemas before it crosses the API boundary.
    HttpExchangeSession open_exchange(const std::string& method, const AnnotatedBatch& request,
                                      std::shared_ptr<arrow::Schema> input_schema,
                                      std::shared_ptr<arrow::Schema> output_schema) const;

private:
    std::shared_ptr<HttpClientState> state_;
};

class VGI_RPC_EXPORT HttpExchangeSession {
public:
    ~HttpExchangeSession();
    HttpExchangeSession(HttpExchangeSession&&) noexcept;
    HttpExchangeSession& operator=(HttpExchangeSession&&) noexcept;
    HttpExchangeSession(const HttpExchangeSession&) = delete;
    HttpExchangeSession& operator=(const HttpExchangeSession&) = delete;

    AnnotatedBatch exchange(const AnnotatedBatch& input);
    // Local and idempotent: no network I/O, including from the destructor.
    void close() noexcept;
    // Best-effort network cancellation followed by local close.  Idempotent.
    void cancel() noexcept;
    bool active() const noexcept;

private:
    friend class HttpClient;
    class Impl;
    explicit HttpExchangeSession(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace vgi_rpc
