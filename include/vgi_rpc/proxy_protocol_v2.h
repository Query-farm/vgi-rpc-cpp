// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"

namespace vgi_rpc {

// Deliberately closed: raw VGI accepts only a relayed TCP connection with a
// concrete IPv4 or IPv6 address. LOCAL, UNSPEC, UDP and Unix-family preambles
// cannot establish caller evidence and are rejected.
struct VGI_RPC_EXPORT ProxyProtocolV2Address {
    std::string source_address;
    std::string destination_address;
    uint16_t source_port = 0;
    uint16_t destination_port = 0;
};

class VGI_RPC_EXPORT ProxyProtocolV2Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Returns the complete preamble size from the fixed 16-byte prefix. The
// caller must enforce its allocation cap before reading the variable body.
VGI_RPC_EXPORT size_t proxy_protocol_v2_size(std::span<const uint8_t> prefix,
                                             size_t maximum_bytes = 536);

// Parse one exact, complete preamble. Unknown TLVs are structurally validated
// and ignored. No caller-controlled TLV is treated as identity evidence.
VGI_RPC_EXPORT ProxyProtocolV2Address parse_proxy_protocol_v2(std::span<const uint8_t> preamble,
                                                              size_t maximum_bytes = 536);

struct VGI_RPC_EXPORT TcpServerOptions {
    // Persistent raw connections consume one worker each. Admission is
    // reject-on-full, with a hard total cap of active + pending connections.
    // These defaults are deliberately finite; tune them from measured worker
    // concurrency and memory use rather than the socket listen backlog.
    size_t maximum_active_connections = 32;
    size_t maximum_pending_connections = 128;

    // One absolute budget from accept through PROXY parsing, identity
    // resolution, and the complete first VGI request frame. Subsequent socket
    // reads each receive an independent idle budget. Long-running handlers do
    // not consume either read budget while they are not reading the socket.
    std::chrono::milliseconds connection_setup_timeout{5000};
    std::chrono::milliseconds idle_read_timeout{60000};
    std::chrono::milliseconds write_timeout{60000};

    bool proxy_protocol_v2_required = false;
    // Exact normalized IP addresses only. This intentionally does not accept
    // CIDRs: trusting a broad network is an operator policy decision outside
    // the parser. Loopback must still be listed explicitly.
    std::vector<std::string> trusted_proxy_addresses;
    std::chrono::milliseconds proxy_preamble_timeout{1000};
    size_t maximum_proxy_preamble_bytes = 536;
    std::string service_name;
    std::chrono::milliseconds identity_resolution_timeout{1000};

    struct ResolvedIdentity {
        AuthContext auth = AuthContext::anonymous();
        PeerEvidenceSet evidence;
    };
    // Runs once after the trusted preamble is parsed and before any VGI bytes
    // are dispatched. The callback must honor context.deadline; a blocked
    // callback consumes one of the listener's bounded connection workers.
    // Provider timeout/capacity outcomes should be represented as an
    // UNAVAILABLE PeerIdentityResult and passed to the configured policy;
    // throwing is a fail-closed connection rejection.
    std::function<ResolvedIdentity(const PeerResolutionContext&)> resolve_identity;
};

}  // namespace vgi_rpc
