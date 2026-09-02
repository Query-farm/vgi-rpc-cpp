// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Operator configuration for the HTTP transport.  Every field is off or
/// unbounded by default: a server built with `HttpConfig{}` is byte-identical
/// on the wire to the framework before any of these features existed.
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"
#include "vgi_rpc/proxy_proof.h"

namespace vgi_rpc {

// How an authenticate hook refused a request.  The closed set of
// docs/unauthorized-spec.md §3 — implementations must not widen it, because a
// client switching on the code needs to know it will not grow underneath them.
enum class AuthReason {
    NONE,  // not a refusal
    MISSING_CREDENTIAL,
    INVALID_CREDENTIAL,
    EXPIRED_CREDENTIAL,
    INSUFFICIENT_SCOPE,
    PROXY_REQUIRED,
    UNAUTHORIZED,
};

VGI_RPC_EXPORT const char* auth_reason_name(AuthReason reason);
VGI_RPC_EXPORT std::optional<AuthReason> auth_reason_from_name(const std::string& name);

// Resolved identity for one request.  `domain` and `principal` are the stable
// identity the sticky AAD binds; they are deliberately not derived from the
// credential, which may rotate mid-session.
struct AuthIdentity {
    bool authenticated = false;
    std::string domain;
    std::string principal;
};

struct HttpConfig {
    std::string host = "127.0.0.1";
    int port = 0;

    // RPC mount prefix.  Operator configuration, not part of the wire
    // contract; the transport also serves the bare paths so a client
    // configured either way reaches the same methods.
    std::string prefix = "/vgi";

    // Response caps.  Negative means unbounded.
    int64_t max_response_bytes = -1;
    int64_t max_externalized_response_bytes = -1;
    // Inline request cap, applied independently to encoded and decoded bytes.
    // Negative means unbounded and omits the capability advertisement.
    int64_t max_request_bytes = -1;

    // Externalization: payloads at or above the threshold are uploaded to the
    // configured backend and replaced by a pointer batch.  Empty URL = off.
    // Accepts s3://bucket/prefix, gs://bucket/prefix, or an http(s) base URL.
    std::string external_storage_url;
    // Lifetime of the pre-signed URLs a pointer batch carries: long enough for
    // a client to finish a fetch, short enough that a leaked pointer expires.
    int signed_url_ttl_seconds = 3600;
    // S3 only.  Empty means the SDK's own credential and region resolution.
    std::string external_storage_region;
    std::string external_storage_endpoint;
    // GCS only; see ExternalStorageConfig::signing_account.
    std::string external_storage_signing_account;
    int64_t externalize_threshold = 4096;
    // Coding applied to an externalized payload before upload; empty = none.
    // The integrity digest covers the payload *before* this is applied.
    std::string externalize_compression;
    // External pointer fetch safety.  These are independent encoded and
    // decoded limits; the URL policy is evaluated before every network hop.
    int64_t max_fetch_bytes = 256LL * 1024 * 1024;
    int64_t max_decompressed_fetch_bytes = 4LL * 1024 * 1024 * 1024;
    int max_external_redirects = 5;
    std::function<void(const std::string&)> external_url_validator;

    // Produce the mandatory zstd and gzip HTTP response codings. False means
    // the server positively states it produces none, which is distinct from
    // not advertising at all. Request decoding remains available either way.
    bool compression = true;

    // CORS.  Empty origin = off; a server with no CORS configured must emit
    // no CORS headers at all.
    std::string cors_origin;

    // Sticky sessions.
    bool sticky = false;
    int sticky_default_ttl = 300;
    // Headers the client must replay for the life of a session, emitted once
    // on the session-opening response as VGI-Echo-<name>.  Their use is
    // client-driven routing: a platform that routes by header (Fly, Railway,
    // an Envoy filter) can then reach the owning worker with no LB config.
    std::map<std::string, std::string> sticky_echo_headers;
    // Fixture affordance: resolve the principal named by X-Conformance-Principal.
    bool sticky_header_auth = false;

    // Master key for state and session tokens.  Random per process unless the
    // operator shares one, which multi-worker deployments must do.
    std::array<uint8_t, crypto::kAeadKeyBytes> token_key = crypto::random_key();

    // Serve stream continuations from a cached resolved call.  Turning it off
    // forces every continuation onto the cold path.
    bool call_state_cache = true;

    // Test-only admin endpoint for flipping the drain flag over the wire, so a
    // conformance run can exercise drain without SIGTERM killing the worker.
    // Never enable in production.
    bool test_drain_endpoint = false;

    // Token introspection route.  Off unless explicitly enabled — the guard
    // that stops a worker growing a credential-to-identity oracle.
    bool token_introspection = false;

    // Reject every RPC request with this reason.  Health stays reachable.
    AuthReason reject_all = AuthReason::NONE;
    // Fixture affordance: honour X-Conformance-Auth-Reason on a rejected
    // request.  Never enable in production — a request must not steer the
    // reason its rejection reports.
    bool honour_requested_auth_reason = false;

    // Proxy proof.
    ProofMode proof_mode = ProofMode::OFF;
    std::string proof_origin_id;
    std::string proof_secrets;
    int proof_skew_seconds = 30;
    bool proof_replay_cache = true;

    // Off-wire transport identity. Providers receive the accepted HTTP peer,
    // original header multiplicity, and logical destination. Evidence is
    // resolved once per request and passed to CallContext; no VGI wire change
    // is involved. These fields remain at the end to preserve positional
    // aggregate initialization of the pre-existing configuration fields.
    std::vector<PeerIdentityProvider> peer_identity_providers;
    PeerAuthenticationPolicy peer_authentication_policy;
    std::optional<std::string> peer_service_name;
    // Cooperative budget exposed to synchronous providers through
    // PeerResolutionContext::deadline. Custom providers must honor that
    // deadline and return promptly; the server cannot preempt a callback.
    // Built-in header-only providers do not perform blocking I/O.
    std::chrono::milliseconds peer_identity_resolution_timeout{1000};

    // Provider-neutral hosting ceilings and advisory response target. These
    // stay at the end for positional aggregate source compatibility.
    int64_t hosting_max_request_bytes = -1;
    int64_t hosting_max_response_bytes = -1;
    int64_t preferred_response_bytes = -1;
};

}  // namespace vgi_rpc
