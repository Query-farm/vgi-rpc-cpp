// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <httplib.h>

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>

namespace vgi_rpc {

struct CallOptions;
struct TlsOptions;

enum class SocksHttpFailureKind { TRANSPORT, TLS, TIMEOUT, CANCELLED, LIMIT };

class SocksHttpFailure final : public std::runtime_error {
public:
    SocksHttpFailure(SocksHttpFailureKind kind, std::string message)
        : std::runtime_error(std::move(message)), kind_(kind) {}

    SocksHttpFailureKind kind() const noexcept { return kind_; }

private:
    SocksHttpFailureKind kind_;
};

// A deliberately small libcurl-backed request engine used only when an
// explicit SOCKS5h proxy is configured. cpp-httplib remains the ordinary HTTP
// transport; it has no SOCKS hook, while libcurl provides proxy-side DNS for
// both HTTP and HTTPS without changing the public RPC surface.
class SocksHttpClient final {
public:
    SocksHttpClient(std::string base_url, std::string proxy_uri, int connect_timeout_seconds,
                    int read_timeout_seconds, int write_timeout_seconds,
                    const TlsOptions& tls_options);

    httplib::Response send(const std::string& method, const std::string& path,
                           const httplib::Headers& headers, const std::string& body,
                           int64_t maximum_encoded_response_bytes,
                           int64_t maximum_identity_response_bytes,
                           const CallOptions& options) const;

private:
    std::string base_url_;
    std::string proxy_uri_;
    int connect_timeout_seconds_;
    int read_timeout_seconds_;
    int write_timeout_seconds_;
    std::string ca_file_;
    std::string client_certificate_file_;
    std::string client_private_key_file_;
    bool insecure_skip_verification_for_testing_ = false;
};

void validate_socks5h_http_proxy_uri(const std::string& proxy_uri);

}  // namespace vgi_rpc
