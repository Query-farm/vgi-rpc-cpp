// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/proxy_proof.h"

#include <stdexcept>
#include <vector>

#include "vgi_rpc/crypto.h"

namespace vgi_rpc {

namespace {

// Domain-separating prefix for the MAC input (spec §4).  Distinct from every
// other keyed use in the framework, which is what stops a state token and a
// proof from being cross-forged.
constexpr const char kProofLabel[] = "vgi.proxy.proof.v1";

// Hard ceiling applied before parsing, per spec §3.
constexpr size_t kMaxHeaderBytes = 512;

bool is_kid_char(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
           c == '_' || c == '-';
}

bool is_b64url_char(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
           c == '-' || c == '_';
}

bool valid_kid(const std::string& s) {
    if (s.empty() || s.size() > 64) return false;
    for (char c : s) {
        if (!is_kid_char(c)) return false;
    }
    return true;
}

bool valid_ts(const std::string& s) {
    if (s.empty() || s.size() > 20) return false;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

bool valid_b64url_fixed(const std::string& s, size_t len) {
    if (s.size() != len) return false;
    for (char c : s) {
        if (!is_b64url_char(c)) return false;
    }
    return true;
}

bool valid_origin_id(const std::string& s) {
    if (s.empty() || s.size() > 255) return false;
    for (char c : s) {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' ||
                        c == '/' || c == '-';
        if (!ok) return false;
    }
    return true;
}

// Left-split on '.' into exactly `n` fields; false for any other count.
bool split_exact(const std::string& s, char sep, size_t n, std::vector<std::string>* out) {
    out->clear();
    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == sep) {
            out->push_back(s.substr(start, i - start));
            if (out->size() > n) return false;
            start = i + 1;
        }
    }
    return out->size() == n;
}

int64_t unix_now() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace

const char* proof_reason_name(ProofReason reason) {
    switch (reason) {
        case ProofReason::OK: return "ok";
        case ProofReason::NO_PROOF: return "no_proof";
        case ProofReason::MALFORMED: return "malformed";
        case ProofReason::UNKNOWN_KID: return "unknown_kid";
        case ProofReason::EXPIRED: return "expired";
        case ProofReason::NOT_YET_VALID: return "not_yet_valid";
        case ProofReason::BAD_MAC: return "bad_mac";
        case ProofReason::REPLAYED: return "replayed";
    }
    return "unauthorized";
}

std::map<std::string, std::string> ProofVerifier::parse_secrets(const std::string& spec) {
    std::map<std::string, std::string> out;
    size_t start = 0;
    while (start <= spec.size()) {
        size_t comma = spec.find(',', start);
        if (comma == std::string::npos) comma = spec.size();
        const std::string entry = spec.substr(start, comma - start);
        start = comma + 1;
        if (entry.empty()) continue;

        const size_t colon = entry.find(':');
        if (colon == std::string::npos) {
            throw std::invalid_argument("proxy-proof secret entry is not 'kid:hex': " + entry);
        }
        const std::string kid = entry.substr(0, colon);
        const std::string hex = entry.substr(colon + 1);
        if (!valid_kid(kid)) {
            throw std::invalid_argument("proxy-proof kid is not [A-Za-z0-9_-]{1,64}: " + kid);
        }
        if (hex.size() != 64) {
            throw std::invalid_argument(
                "proxy-proof secret for '" + kid + "' must be 64 hex characters, got " +
                std::to_string(hex.size()));
        }
        auto raw = crypto::hex_decode(hex);
        if (!raw) {
            throw std::invalid_argument("proxy-proof secret for '" + kid + "' is not hex");
        }
        out[kid] = std::string(reinterpret_cast<const char*>(raw->data()), raw->size());
    }
    return out;
}

ProofVerifier::ProofVerifier(ProofMode mode, std::string origin_id,
                             const std::string& secrets_spec, int skew_seconds,
                             bool replay_cache, size_t replay_capacity)
    : mode_(mode)
    , origin_id_(std::move(origin_id))
    , skew_seconds_(skew_seconds > 0 ? skew_seconds : 30)
    , replay_cache_(replay_cache)
    , replay_capacity_(replay_capacity) {
    if (mode_ == ProofMode::OFF) return;

    if (!valid_origin_id(origin_id_)) {
        throw std::invalid_argument(
            "proxy-proof origin_id is required in allow/require mode and must match "
            "[A-Za-z0-9._:/-]{1,255}");
    }
    secrets_ = parse_secrets(secrets_spec);
    if (secrets_.empty()) {
        throw std::invalid_argument(
            "proxy-proof mode is allow/require but no secrets are configured");
    }
}

bool ProofVerifier::seen_nonce(const std::string& nonce) {
    if (!replay_cache_) return false;
    const int64_t now = unix_now();

    // Drop entries that have aged past the window: a nonce older than the skew
    // can no longer verify anyway, so keeping it buys nothing.
    while (!nonce_order_.empty()) {
        auto it = nonces_.find(nonce_order_.front());
        if (it != nonces_.end() && now - it->second <= skew_seconds_) break;
        if (it != nonces_.end()) nonces_.erase(it);
        nonce_order_.pop_front();
    }

    if (nonces_.count(nonce) != 0) return true;

    // Hard capacity cap on top of the TTL.  Without it, distinct nonces at line
    // rate are a remote memory-exhaustion vector; evicting oldest keeps a
    // traffic burst from becoming an outage, and the time window still bounds
    // the exposure.
    while (nonces_.size() >= replay_capacity_ && !nonce_order_.empty()) {
        nonces_.erase(nonce_order_.front());
        nonce_order_.pop_front();
    }
    nonces_[nonce] = now;
    nonce_order_.push_back(nonce);
    return false;
}

ProofResult ProofVerifier::verify(const std::string& header_value, int count) {
    ProofResult result;
    if (mode_ == ProofMode::OFF) {
        result.reason = ProofReason::OK;
        return result;
    }

    // Steps 1-4 involve no MAC computation and run first, so a malformed
    // header costs nothing.
    if (count == 0) {
        result.reason = ProofReason::NO_PROOF;
        return result;
    }
    if (count != 1 || header_value.empty() || header_value.size() > kMaxHeaderBytes) {
        result.reason = ProofReason::MALFORMED;
        return result;
    }

    std::vector<std::string> parts;
    if (!split_exact(header_value, '.', 5, &parts) || parts[0] != "v1") {
        result.reason = ProofReason::MALFORMED;
        return result;
    }
    const std::string& kid = parts[1];
    const std::string& ts = parts[2];
    const std::string& nonce = parts[3];
    const std::string& mac = parts[4];

    // Charsets are checked before decoding, not left to the base64 decoder:
    // decoders disagree about invalid input, so a port that relied on its own
    // would report bad_mac where another reports malformed.
    if (!valid_kid(kid) || !valid_ts(ts) || !valid_b64url_fixed(nonce, 22) ||
        !valid_b64url_fixed(mac, 43)) {
        result.reason = ProofReason::MALFORMED;
        return result;
    }
    result.claimed_kid = kid;

    auto secret_it = secrets_.find(kid);
    if (secret_it == secrets_.end()) {
        result.reason = ProofReason::UNKNOWN_KID;
        return result;
    }

    int64_t ts_value = 0;
    try {
        ts_value = std::stoll(ts);
    } catch (const std::exception&) {
        result.reason = ProofReason::MALFORMED;
        return result;
    }

    // Two-sided window.  Checking only the upper bound would let a far-future
    // timestamp verify forever.
    const int64_t now = unix_now();
    if (now - ts_value > skew_seconds_) {
        result.reason = ProofReason::EXPIRED;
        return result;
    }
    if (ts_value - now > skew_seconds_) {
        result.reason = ProofReason::NOT_YET_VALID;
        return result;
    }

    // origin_id is folded in from configuration, never from the wire — that is
    // what makes a proof minted for one worker invalid at every other, even if
    // secrets were misconfigured to overlap.
    std::string canonical(kProofLabel, sizeof(kProofLabel) - 1);
    canonical.push_back('\0');
    canonical += kid;
    canonical.push_back('\0');
    canonical += ts;
    canonical.push_back('\0');
    canonical += nonce;
    canonical.push_back('\0');
    canonical += origin_id_;

    auto expected = crypto::hmac_sha256(secret_it->second, canonical);
    const std::string expected_b64 = crypto::base64url_encode(expected.data(), expected.size());
    // Selecting one candidate secret by kid is a legitimate branch — kid is
    // public — but the comparison itself must not leak position.
    if (!crypto::constant_time_equal(expected_b64, mac)) {
        result.reason = ProofReason::BAD_MAC;
        return result;
    }

    if (seen_nonce(nonce)) {
        result.reason = ProofReason::REPLAYED;
        return result;
    }

    result.reason = ProofReason::OK;
    result.proxy_label = kid;
    return result;
}

}  // namespace vgi_rpc
