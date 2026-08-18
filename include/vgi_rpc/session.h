// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Sticky sessions: per-worker registry of live handle-bearing objects, keyed
/// by an AEAD-sealed token the client echoes.  See docs/sticky-sessions-spec.md.
///
/// State lives in this process's memory and is never serialized, replicated,
/// or persisted — that is the point of the feature, not a limitation of it.
/// A token therefore carries only an identifier plus the bindings that decide
/// whether this worker may honour it.
#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

// Anything a method wants to keep alive across the requests of one session.
// `close()` runs on explicit close, TTL eviction, and graceful drain — never
// on a process crash, which the spec states outright.
class VGI_RPC_EXPORT SessionState {
public:
    virtual ~SessionState() = default;
    virtual void close() {}
};

// Why a token did not resolve.  Every value surfaces to the client as the same
// `session_lost` error: telling a caller which check failed turns the endpoint
// into an oracle for whose sessions exist.
enum class SessionLookup {
    OK,
    NOT_PRESENTED,  // no VGI-Session header
    UNSEALABLE,     // wrong key, wrong principal (AAD), or tampered bytes
    WRONG_WORKER,   // sealed by a peer sharing our key, but not by us
    MISSING,        // never existed, or already evicted
    EXPIRED,        // past its TTL
};

// The AAD tail that binds a token to the identity that opened it.  Only the
// stable identity feeds it — never claims, which churn as a credential
// refreshes and would evict live sessions for no reason.
VGI_RPC_EXPORT std::string session_aad(const std::string& domain, const std::string& principal,
                                       bool authenticated);

class VGI_RPC_EXPORT SessionRegistry {
public:
    SessionRegistry(std::array<uint8_t, crypto::kAeadKeyBytes> key, std::string server_id,
                    int default_ttl_seconds);

    // Register `state` and return the token the response should carry.
    std::string open(std::shared_ptr<SessionState> state, const std::string& aad,
                     std::optional<int> ttl_seconds = std::nullopt);

    // Resolve a token presented under `aad`.  On OK, `out_state` and
    // `out_session_id` are populated.
    SessionLookup resolve(const std::string& token, const std::string& aad,
                          std::shared_ptr<SessionState>* out_state, std::string* out_session_id);

    // Drop a session and run its close hook.  Idempotent.
    bool close(const std::string& token, const std::string& aad);

    // Evict everything whose TTL has passed, running each close hook.
    void sweep();

    // Flip the drain flag: open() then refuses, while live sessions keep serving.
    void drain() { draining_ = true; }
    // Only a conformance run needs this: a deployment drains once and exits.
    void undrain() { draining_ = false; }
    bool draining() const noexcept { return draining_; }

    // Run every live session's close hook and empty the registry.
    void shutdown();

    int default_ttl_seconds() const noexcept { return default_ttl_seconds_; }

private:
    struct Entry {
        std::shared_ptr<SessionState> state;
        std::chrono::steady_clock::time_point expires_at;
    };

    // Decode a token to its session id, verifying the seal, the AAD, and that
    // this worker is the one that minted it.
    SessionLookup unseal(const std::string& token, const std::string& aad,
                         std::string* out_session_id) const;

    std::array<uint8_t, crypto::kAeadKeyBytes> key_;
    std::string server_id_;
    int default_ttl_seconds_;
    bool draining_ = false;
    std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace vgi_rpc
