// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Secure client-side HTTP transfer for ExternalLocation payloads.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

enum class ExternalUrlPolicy {
    /// HTTPS only; every resolved address must be globally routable.
    PUBLIC_HTTPS,
    /// Explicit test-only escape hatch: HTTP only and every address loopback.
    LOOPBACK_HTTP_TEST,
};

struct ClientExternalHttpOptions {
    ExternalUrlPolicy url_policy = ExternalUrlPolicy::PUBLIC_HTTPS;
    // Hard protocol ceiling. Values outside [0, 5] are rejected.
    int max_redirects = 5;
    int64_t max_encoded_bytes = 256LL * 1024 * 1024;
    int64_t max_decoded_bytes = 4LL * 1024 * 1024 * 1024;
    int64_t max_upload_bytes = 256LL * 1024 * 1024;
    int64_t max_upload_response_bytes = 64LL * 1024;
    std::chrono::milliseconds connect_timeout{10000};
    std::chrono::milliseconds read_timeout{60000};
    std::chrono::milliseconds write_timeout{60000};
};

class VGI_RPC_EXPORT ExternalHttpError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

using ExternalLogHandler = std::function<void(const AnnotatedBatch&)>;

/// Remove userinfo, query, and fragment data from a URL used in diagnostics.
/// Invalid input becomes "<invalid-url>".
VGI_RPC_EXPORT std::string redact_external_url(const std::string& url) noexcept;

/// Reusable policy/configuration object for credential-free object transfers.
///
/// Each request resolves the hostname, validates the resolved addresses, and
/// pins one validated address into cpp-httplib. The original hostname remains
/// the HTTP Host, TLS SNI, and certificate-verification identity.
class VGI_RPC_EXPORT ClientExternalHttp {
public:
    explicit ClientExternalHttp(const ClientExternalHttpOptions& options = {});
    ~ClientExternalHttp();
    ClientExternalHttp(ClientExternalHttp&&) noexcept;
    ClientExternalHttp& operator=(ClientExternalHttp&&) noexcept;
    ClientExternalHttp(const ClientExternalHttp&) = delete;
    ClientExternalHttp& operator=(const ClientExternalHttp&) = delete;

    /// GET, manually revalidating at most five redirect targets. Content is
    /// decoded only for identity and zstd. The optional digest covers decoded
    /// bytes and must be a 64-character SHA-256 hex string.
    std::string fetch(const std::string& url, const std::string& expected_sha256 = {}) const;

    /// Validate and resolve a URL under the configured policy without making
    /// an HTTP request. This is used to fail closed on download URLs before a
    /// request body points a server at them.
    void validate_url(const std::string& url) const;

    /// Resolve exactly one ExternalLocation pointer payload. The fetched Arrow
    /// stream may contain logs plus exactly one data batch; nested pointers are
    /// rejected. Non-pointer batches are returned unchanged.
    AnnotatedBatch resolve_pointer(const AnnotatedBatch& pointer,
                                   ExternalLogHandler on_log = {}) const;

    /// Credential-free PUT for a method-bound pre-signed upload URL. No
    /// Authorization, cookies, or caller-controlled headers are attached and
    /// redirects are rejected rather than replaying the upload elsewhere.
    void put(const std::string& url, const std::string& body,
             const std::string& content_type = "application/vnd.apache.arrow.stream") const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace vgi_rpc
