// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"

namespace vgi_rpc {

struct VGI_RPC_EXPORT TailscaleServeOptions {
    std::string issuer;
    std::vector<std::string> trusted_proxy_addresses;
    size_t max_header_bytes = 16'384;
};

// Consumes the identity headers emitted by one of the exact, adjacent
// Tailscale Serve proxy addresses configured above.
VGI_RPC_EXPORT PeerIdentityProvider
tailscale_serve_identity_provider(TailscaleServeOptions options);

struct VGI_RPC_EXPORT TailscaleLocalAPIOptions {
    std::string issuer;
    // Configure at most one. If neither is set, Unix platforms use the
    // conventional tailscaled socket. Endpoint must be a plain HTTP origin;
    // it is intended for a local same-user-proof endpoint, not a remote API.
    std::string unix_socket;
    std::string endpoint;
    std::string password;
    std::chrono::milliseconds timeout{5'000};
    size_t max_response_bytes = 65'536;
    size_t max_response_header_bytes = 32'768;
};

// Performs a fresh LocalAPI WhoIs request for every resolution. It never
// invokes the Tailscale CLI, consults proxy environment variables, or caches.
VGI_RPC_EXPORT PeerIdentityProvider
tailscale_localapi_identity_provider(TailscaleLocalAPIOptions options);

}  // namespace vgi_rpc
