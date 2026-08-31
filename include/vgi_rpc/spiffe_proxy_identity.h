// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"

namespace vgi_rpc {

struct VGI_RPC_EXPORT SpiffeProxyOptions {
    std::vector<std::string> trust_domains;
    std::vector<std::string> trusted_proxy_addresses;
    size_t max_header_bytes = 16'384;
};

struct VGI_RPC_EXPORT SpiffeX509HeaderOptions : SpiffeProxyOptions {
    std::string certificate_header = "X-SSL-Client-Cert";
    std::string verification_header;
    std::string verification_value = "true";
    std::string evidence_source = "verified_certificate_header";
};

// The generic certificate adapter always requires a positive per-request
// verification header in addition to the exact immediate-proxy allowlist.
VGI_RPC_EXPORT PeerIdentityProvider spiffe_x509_header_provider(SpiffeX509HeaderOptions options);
VGI_RPC_EXPORT PeerIdentityProvider nginx_spiffe_provider(SpiffeProxyOptions options);
// This profile is valid only when the operator has configured ALB mTLS verify
// mode; passthrough mode is not accepted evidence.
VGI_RPC_EXPORT PeerIdentityProvider aws_alb_spiffe_provider(SpiffeProxyOptions options);
VGI_RPC_EXPORT PeerIdentityProvider
azure_application_gateway_spiffe_provider(SpiffeProxyOptions options);

struct VGI_RPC_EXPORT GcpSpiffeOptions : SpiffeProxyOptions {
    std::string spiffe_id_header = "X-Client-Cert-Spiffe-Id";
    std::string present_header = "X-Client-Cert-Present";
    std::string chain_verified_header = "X-Client-Cert-Chain-Verified";
    std::string error_header = "X-Client-Cert-Error";
};

VGI_RPC_EXPORT PeerIdentityProvider gcp_load_balancer_spiffe_provider(GcpSpiffeOptions options);

struct VGI_RPC_EXPORT EnvoyXfccSpiffeOptions : SpiffeProxyOptions {
    std::string header = "X-Forwarded-Client-Cert";
};

// Accepts one text-format SANITIZE_SET XFCC element. Forward/append chains and
// ambiguous fields fail closed.
VGI_RPC_EXPORT PeerIdentityProvider envoy_xfcc_spiffe_provider(EnvoyXfccSpiffeOptions options);

// Returns the trust domain after validating canonical SPIFFE syntax and the
// configured trust-domain allowlist.
VGI_RPC_EXPORT std::string validate_spiffe_id(const std::string& value,
                                              const std::vector<std::string>& trust_domains);

}  // namespace vgi_rpc
