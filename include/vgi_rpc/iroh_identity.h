// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"

namespace vgi_rpc {

inline constexpr char IROH_FORWARDED_ENDPOINT_HEADER[] = "VGI-Forwarded-Iroh-Endpoint";

struct VGI_RPC_EXPORT IrohForwardedHeaderOptions {
    std::string issuer;
    std::vector<std::string> trusted_proxy_addresses;
};

// Resolve one canonical lowercase EndpointId only across an explicitly
// configured exact-IP HTTP proxy boundary. The bridge authenticates Iroh; this
// provider authenticates the forwarding hop and retains that distinction in
// assurance/original_assurance.
VGI_RPC_EXPORT PeerIdentityProvider
iroh_forwarded_header_provider(IrohForwardedHeaderOptions options);

}  // namespace vgi_rpc
