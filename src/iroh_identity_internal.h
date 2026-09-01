// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <string>

#include "vgi_rpc/identity.h"

namespace vgi_rpc::iroh_internal {

void validate_issuer(const std::string& issuer);
bool canonical_endpoint(const std::string& endpoint) noexcept;
PeerIdentity forwarded_identity(const std::string& endpoint, const std::string& issuer,
                                const std::string& transport, const std::string& evidence_source,
                                const std::string& proxy_address);

}  // namespace vgi_rpc::iroh_internal
