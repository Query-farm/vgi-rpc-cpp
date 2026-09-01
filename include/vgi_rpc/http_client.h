// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Native HTTP client for unary and typed exchange RPCs.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <vector>

#include <arrow/type_fwd.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/client_description.h"
#include "vgi_rpc/client_external.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

enum class HttpClientErrorKind {
    TRANSPORT,
    TLS,
    TIMEOUT,
    CANCELLED,
    HTTP_STATUS,
    AUTHENTICATION,
    PROTOCOL,
    LIMIT,
    REMOTE,
};

struct RetryPolicy {
    uint32_t max_attempts = 3;
    std::chrono::milliseconds initial_backoff{100};
    std::chrono::milliseconds max_backoff{10'000};
    double multiplier = 2.0;
    double jitter = 0.2;
    // Empty by default: status retries require both an explicit status list
    // and CallOptions::idempotent. Connection retries use the same gate.
    std::vector<int> retryable_status_codes;

    static RetryPolicy disabled();
};

struct CallOptions {
    std::map<std::string, std::string> headers;
    std::optional<std::chrono::steady_clock::time_point> deadline;
    std::optional<std::string> request_id;
    std::stop_token stop_token;
    // POST transport failures are ambiguous and are not retried unless the
    // caller explicitly declares the logical operation idempotent.
    bool idempotent = false;

    static CallOptions with_timeout(std::chrono::milliseconds timeout);
};

struct HttpAuthRequest {
    std::string method;
    std::string path;
    std::string request_id;
};

using HttpAuthCallback =
    std::function<std::map<std::string, std::string>(const HttpAuthRequest& request)>;

struct TlsOptions {
    // Empty means OpenSSL's system trust store.
    std::string ca_file;
    std::string client_certificate_file;
    std::string client_private_key_file;
    // Deliberately loud and test-scoped. Never enable in production.
    bool insecure_skip_verification_for_testing = false;
};

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

struct HttpUploadUrl {
    std::string upload_url;
    std::string download_url;
    std::optional<int64_t> expires_at_us;
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
    // Optional explicit SOCKS5h proxy for the VGI RPC origin. The origin
    // hostname is resolved by the proxy, NO AUTH is the only supported method,
    // and failure never falls back to a direct connection. Externalized object
    // upload/download URLs retain ClientExternalHttp's separate validation and
    // transport policy; this option does not proxy those auxiliary requests.
    // Kept at the end to preserve positional aggregate initialization of the
    // pre-existing configuration fields.
    std::optional<std::string> tcp_proxy;
};

class VGI_RPC_EXPORT HttpClientError : public std::runtime_error {
public:
    HttpClientError(std::string message, int http_status = 0);
    HttpClientError(HttpClientErrorKind kind, std::string message, int http_status,
                    std::string method, std::string request_id, std::string response_body = {},
                    std::string retry_after = {}, std::string auth_reason = {});

    int http_status() const noexcept { return http_status_; }
    int status_code() const noexcept { return http_status_; }
    HttpClientErrorKind kind() const noexcept { return kind_; }
    const std::string& method() const noexcept { return method_; }
    const std::string& request_id() const noexcept { return request_id_; }
    const std::string& response_body() const noexcept { return response_body_; }
    const std::string& retry_after() const noexcept { return retry_after_; }
    const std::string& auth_reason() const noexcept { return auth_reason_; }

private:
    HttpClientErrorKind kind_ = HttpClientErrorKind::PROTOCOL;
    int http_status_;
    std::string method_;
    std::string request_id_;
    std::string response_body_;
    std::string retry_after_;
    std::string auth_reason_;
};

class VGI_RPC_EXPORT HttpAuthenticationError : public HttpClientError {
public:
    HttpAuthenticationError(std::string message, int http_status, std::string method,
                            std::string request_id, std::string response_body,
                            std::string www_authenticate, std::string retry_after = {},
                            std::string auth_reason = {});

    const std::string& www_authenticate() const noexcept { return www_authenticate_; }

private:
    std::string www_authenticate_;
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

class VGI_RPC_EXPORT HttpSessionLostError : public RpcRemoteError {
public:
    HttpSessionLostError(std::string message, std::string error_kind, std::string server_id,
                         std::string request_id, int http_status = 200);
};

class HttpExchangeSession;
class HttpStreamSession;
class HttpSessionView;
class HttpClientState;
class HttpStickySessionState;
class HttpClient;

class VGI_RPC_EXPORT HttpClientBuilder {
public:
    explicit HttpClientBuilder(std::string base_url);
    ~HttpClientBuilder();
    HttpClientBuilder(const HttpClientBuilder&);
    HttpClientBuilder& operator=(const HttpClientBuilder&);
    HttpClientBuilder(HttpClientBuilder&&) noexcept;
    HttpClientBuilder& operator=(HttpClientBuilder&&) noexcept;

    HttpClientBuilder& config(HttpClientConfig config);
    HttpClientBuilder& prefix(std::string prefix);
    HttpClientBuilder& protocol_version(std::string version);
    HttpClientBuilder& header(std::string name, std::string value);
    HttpClientBuilder& compression_level(std::optional<int> level);
    HttpClientBuilder& response_limits(int64_t max_encoded_bytes, int64_t max_decoded_bytes);
    HttpClientBuilder& retry_policy(RetryPolicy policy);
    HttpClientBuilder& auth_callback(HttpAuthCallback callback);
    HttpClientBuilder& tls_options(TlsOptions options);
    HttpClientBuilder& custom_ca_file(std::string path);
    HttpClientBuilder& client_certificate(std::string certificate_file,
                                          std::string private_key_file);
    HttpClientBuilder& tcp_proxy(std::string proxy_uri);
    HttpClientBuilder& dangerous_disable_tls_verification_for_testing(bool disabled = true);
    // External-location resolution is securely enabled by default. Override
    // limits/policy here; LOOPBACK_HTTP_TEST is only for local conformance.
    HttpClientBuilder& external_http_options(ClientExternalHttpOptions options);
    HttpClientBuilder& disable_external_locations(bool disabled = true);

    HttpClient build() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class VGI_RPC_EXPORT HttpClient {
public:
    static HttpClientBuilder builder(std::string base_url);

    // Compatibility adapter. New code should use builder(), which exposes TLS,
    // retry, authentication and per-call controls without growing this ABI.
    [[deprecated("use HttpClient::builder(base_url).config(config).build()")]] explicit HttpClient(
        std::string base_url, HttpClientConfig config = {});
    ~HttpClient();

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) noexcept = default;
    HttpClient& operator=(HttpClient&&) noexcept = default;

    // Invoke a unary method.  The request batch's declared schema is written
    // verbatim; expected_output_schema, when present, is enforced exactly.
    AnnotatedBatch call(const std::string& method, const AnnotatedBatch& request,
                        std::shared_ptr<arrow::Schema> expected_output_schema = nullptr,
                        const CallOptions& options = {}) const;

    // Query OPTIONS {prefix}/health once and cache its advertised transport
    // capabilities.  Ordinary RPC responses refine the same cache.
    HttpServerCapabilities capabilities(const CallOptions& options = {}) const;

    // Fetch and validate the protocol's version-4 introspection document.
    ServiceDescription describe(const CallOptions& options = {}) const;

    // Request one or more method-bound external upload/download URL pairs.
    std::vector<HttpUploadUrl> request_upload_urls(int64_t count,
                                                   const CallOptions& options = {}) const;

    // Initialize a bidirectional exchange.  Every input and output batch is
    // checked against the declared schemas before it crosses the API boundary.
    HttpExchangeSession open_exchange(const std::string& method, const AnnotatedBatch& request,
                                      std::shared_ptr<arrow::Schema> input_schema,
                                      std::shared_ptr<arrow::Schema> output_schema,
                                      const CallOptions& options = {}) const;

    // Initialize a producer stream. Application batches emitted during init
    // are queued and returned before the first continuation request.
    HttpStreamSession open_producer(const std::string& method, const AnnotatedBatch& request,
                                    std::shared_ptr<arrow::Schema> output_schema,
                                    bool has_header = false, const CallOptions& options = {}) const;

    // General stream API for exchanges that may terminate without a final
    // data batch. Existing open_exchange remains the strict compatibility API.
    HttpStreamSession open_stream_exchange(const std::string& method, const AnnotatedBatch& request,
                                           std::shared_ptr<arrow::Schema> input_schema,
                                           std::shared_ptr<arrow::Schema> output_schema,
                                           bool has_header = false,
                                           const CallOptions& options = {}) const;

    // Resume a token-addressed HTTP producer without replaying /init.
    HttpStreamSession resume_stream(const std::string& method,
                                    const std::vector<uint8_t>& resume_token,
                                    std::shared_ptr<arrow::Schema> output_schema) const;

    // Create an independent sticky-session view over this transport. The view
    // opts into session creation, tracks token rotation and routing echo
    // headers, and tears down a still-live session on destruction.
    HttpSessionView with_session_token(std::optional<std::string> token = std::nullopt,
                                       std::map<std::string, std::string> echo_headers = {}) const;

private:
    friend class HttpClientBuilder;
    friend class HttpSessionView;
    explicit HttpClient(std::shared_ptr<HttpClientState> state,
                        std::shared_ptr<HttpStickySessionState> sticky_session = nullptr);
    std::shared_ptr<HttpClientState> state_;
    std::shared_ptr<HttpStickySessionState> sticky_session_;
};

class VGI_RPC_EXPORT HttpSessionView {
public:
    ~HttpSessionView();
    HttpSessionView(HttpSessionView&&) noexcept;
    HttpSessionView& operator=(HttpSessionView&&) noexcept;
    HttpSessionView(const HttpSessionView&) = delete;
    HttpSessionView& operator=(const HttpSessionView&) = delete;

    AnnotatedBatch call(const std::string& method, const AnnotatedBatch& request,
                        std::shared_ptr<arrow::Schema> expected_output_schema = nullptr,
                        const CallOptions& options = {}) const;
    HttpServerCapabilities capabilities(const CallOptions& options = {}) const;
    ServiceDescription describe(const CallOptions& options = {}) const;
    std::vector<HttpUploadUrl> request_upload_urls(int64_t count,
                                                   const CallOptions& options = {}) const;
    HttpExchangeSession open_exchange(const std::string& method, const AnnotatedBatch& request,
                                      std::shared_ptr<arrow::Schema> input_schema,
                                      std::shared_ptr<arrow::Schema> output_schema,
                                      const CallOptions& options = {}) const;
    HttpStreamSession open_producer(const std::string& method, const AnnotatedBatch& request,
                                    std::shared_ptr<arrow::Schema> output_schema,
                                    bool has_header = false, const CallOptions& options = {}) const;
    HttpStreamSession open_stream_exchange(const std::string& method, const AnnotatedBatch& request,
                                           std::shared_ptr<arrow::Schema> input_schema,
                                           std::shared_ptr<arrow::Schema> output_schema,
                                           bool has_header = false,
                                           const CallOptions& options = {}) const;
    HttpStreamSession resume_stream(const std::string& method,
                                    const std::vector<uint8_t>& resume_token,
                                    std::shared_ptr<arrow::Schema> output_schema) const;

    std::optional<std::string> current_session_token() const;
    std::map<std::string, std::string> current_echo_headers() const;
    // Forget and return the token without deleting it, for explicit handoff.
    std::optional<std::string> detach();
    // Idempotently release local state and best-effort DELETE a live token.
    void close() noexcept;
    bool active() const noexcept;

private:
    friend class HttpClient;
    HttpSessionView(std::shared_ptr<HttpClientState> state, std::optional<std::string> token,
                    std::map<std::string, std::string> echo_headers);
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class VGI_RPC_EXPORT HttpExchangeSession {
public:
    ~HttpExchangeSession();
    HttpExchangeSession(HttpExchangeSession&&) noexcept;
    HttpExchangeSession& operator=(HttpExchangeSession&&) noexcept;
    HttpExchangeSession(const HttpExchangeSession&) = delete;
    HttpExchangeSession& operator=(const HttpExchangeSession&) = delete;

    AnnotatedBatch exchange(const AnnotatedBatch& input, const CallOptions& options = {});
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

enum class HttpStreamKind {
    PRODUCER,
    EXCHANGE,
};

struct HttpStreamBatch {
    AnnotatedBatch value;
    // Opaque portable blob containing the cursor and optional call-state.
    // It may be persisted and supplied to resume_stream on another client.
    std::vector<uint8_t> resume_token;
};

class VGI_RPC_EXPORT HttpStreamSession {
public:
    class Impl;
    ~HttpStreamSession();
    HttpStreamSession(HttpStreamSession&&) noexcept;
    HttpStreamSession& operator=(HttpStreamSession&&) noexcept;
    HttpStreamSession(const HttpStreamSession&) = delete;
    HttpStreamSession& operator=(const HttpStreamSession&) = delete;

    HttpStreamKind kind() const noexcept;
    const std::optional<AnnotatedBatch>& header() const noexcept;
    bool finished() const noexcept;

    std::optional<AnnotatedBatch> tick(const CallOptions& options = {});
    std::optional<HttpStreamBatch> next_with_token(const CallOptions& options = {});
    std::optional<AnnotatedBatch> exchange(const AnnotatedBatch& input,
                                           const CallOptions& options = {});
    void seek_to_token(const std::vector<uint8_t>& resume_token);

    // Local-only close. cancel() makes one best-effort cancellation request.
    void close() noexcept;
    void cancel() noexcept;

private:
    friend class HttpClient;
    explicit HttpStreamSession(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace vgi_rpc
