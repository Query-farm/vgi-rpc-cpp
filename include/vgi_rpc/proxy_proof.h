// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Proxy proof: HMAC-SHA256 proof-of-hop letting a worker refuse any request
/// that did not arrive through its trusted proxy.  See docs/proxy-proof-spec.md.
///
/// It proves the hop, never the user — user identity stays with whatever
/// bearer/JWT/mTLS authentication runs alongside.
#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <unordered_map>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

enum class ProofMode {
    OFF,      // gate not installed; zero per-request cost
    ALLOW,    // verified and recorded, but never denies — the rollout lever
    REQUIRE,  // verification failure is a 401
};

// Reason codes from spec §6.  Every one collapses to `proxy_required` on the
// wire; they exist for logs and metrics, and MUST NOT reach a response body.
enum class ProofReason {
    OK,
    NO_PROOF,
    MALFORMED,
    UNKNOWN_KID,
    EXPIRED,
    NOT_YET_VALID,
    BAD_MAC,
    REPLAYED,
};

VGI_RPC_EXPORT const char* proof_reason_name(ProofReason reason);

struct ProofResult {
    ProofReason reason = ProofReason::NO_PROOF;
    std::string proxy_label;  // set only when verified
    std::string claimed_kid;  // informational; attacker-controlled until verified
    bool verified() const noexcept { return reason == ProofReason::OK; }
};

class VGI_RPC_EXPORT ProofVerifier {
public:
    // `secrets` maps kid -> 32-byte secret, hex-encoded (64 chars).  Throws on
    // a malformed secret or a missing origin id in allow/require mode: a lax
    // parse turns a typo into a 100% rejection outage with no diagnostic, so
    // this fails closed at startup rather than degrading.
    ProofVerifier(ProofMode mode, std::string origin_id, const std::string& secrets_spec,
                  int skew_seconds, bool replay_cache, size_t replay_capacity = 100000);

    ProofMode mode() const noexcept { return mode_; }

    // Verify the value of a single VGI-Proxy-Proof header.  `count` is how many
    // instances the request carried; anything but 1 is malformed (0 when the
    // header is absent is NO_PROOF).
    ProofResult verify(const std::string& header_value, int count);

    // Parse "kid:hex,kid2:hex" into a kid -> raw-secret map, throwing on any
    // entry that is not exactly 64 hex characters.
    static std::map<std::string, std::string> parse_secrets(const std::string& spec);

private:
    bool seen_nonce(const std::string& nonce);

    ProofMode mode_;
    std::string origin_id_;
    std::map<std::string, std::string> secrets_;  // kid -> 32 raw bytes
    int skew_seconds_;
    bool replay_cache_;
    size_t replay_capacity_;
    std::unordered_map<std::string, int64_t> nonces_;  // nonce -> first-seen unix
    std::deque<std::string> nonce_order_;
};

}  // namespace vgi_rpc
