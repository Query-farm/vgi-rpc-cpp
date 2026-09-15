// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

/// `vgi_rpc.Identity.v1` -- resolving a credential, and minting a grant.
///
/// Identity lives at the RPC layer rather than in any application protocol: a
/// bearer token is not an application concept, the auth primitives it builds on
/// (`AuthContext`, the transport identity providers) are already here, and
/// implementing it once is the whole point.  It was previously an HTTP JSON
/// route, `POST {prefix}/__introspect_token__`, which meant it existed only on
/// one transport and had to be hand-written in every port.
///
/// Two methods share this file's guards, and they are guarded *differently* on
/// purpose.
///
/// `introspect_token` answers "which principal is this credential" for a
/// reverse proxy that terminates the only public listener.  The answer is an
/// identity assertion made by the thing being protected, which the asker then
/// acts on using credentials the worker does not hold -- storage credentials,
/// entitlement lookups, policy-tier selection.  "Trust it as much as you trust
/// the worker" is the wrong frame: it must be trusted *more*.  So every
/// rejection is uniform, the caller must be on an allowlist with no permissive
/// default, a JWS-shaped subject never reaches the resolver, and the whole
/// thing is rate limited.
///
/// `issue_grant` mints a credential for the *calling* user, so it is not an
/// oracle about anybody else.  It therefore needs no allowlist and no rate
/// limit, and its rejections are deliberately *actionable*: a console that
/// cannot tell "your login is too old" from "no" cannot know to re-prompt.
///
/// Errors carry a stable `error_kind`.  That is load-bearing rather than
/// decorative: these used to be a bespoke HTTP route whose callers classified
/// definitive-vs-transient on the HTTP status (404 vs 503).  As protocol
/// methods every handler exception surfaces the same way, so `error_kind` is
/// now the *only* signal a caller has.  A caller that negative-caches a
/// transient failure locks out valid users; one that retries a definitive
/// rejection hammers the worker.

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/type.h>

#include "vgi_rpc/errors.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"
#include "vgi_rpc/server.h"

namespace vgi_rpc {

/// The wire name of the identity protocol.
///
/// Framework-owned, under the reserved `vgi_rpc.` prefix, so an application
/// cannot register a protocol that impersonates it.
inline constexpr const char* kIdentityProtocolName = "vgi_rpc.Identity.v1";

/// Cap on a credential we will even attempt to resolve.
///
/// Anything longer is not a bearer token; refusing early keeps a resolver from
/// being handed megabytes.
inline constexpr size_t kMaxTokenChars = 4096;

/// Introspections allowed per caller per window, unless configured otherwise.
inline constexpr int kDefaultIntrospectRateLimit = 20;

/// How recently a caller must have authenticated to mint a grant, in seconds.
inline constexpr double kDefaultMaxAuthAge = 900.0;

/// How long a caller should wait before retrying a transient identity failure.
inline constexpr int kDefaultIdentityRetryAfter = 5;

/// A SHA-256 hex digest of `token`, for diagnostics.
///
/// The credential itself must never reach a log, a span, or an error message.
/// A digest is stable enough to correlate one credential's failures across
/// records without being the credential.
VGI_RPC_EXPORT std::string token_digest(const std::string& token);

/// The caller may not introspect.
///
/// Definitive: a caller may cache this.  Authentication is not the same
/// capability as introspection -- a deployment where any valid credential may
/// introspect lets any user test guesses of any other user's credential at
/// unlimited rate, and resolve a stolen one to its owner.
class VGI_RPC_EXPORT IntrospectionRefusedError : public KindedError {
public:
    explicit IntrospectionRefusedError(const std::string& what)
        : KindedError("introspection_refused", "IntrospectionRefusedError", what) {}
};

/// The subject credential did not resolve.
///
/// Definitive, and deliberately uniform: unknown, expired and malformed are one
/// answer, because reporting which would confirm that a guessed credential
/// exists.
class VGI_RPC_EXPORT TokenUnresolvedError : public KindedError {
public:
    explicit TokenUnresolvedError(const std::string& what = "unresolved")
        : KindedError("token_unresolved", "TokenUnresolvedError", what) {}
};

/// The caller has not authenticated recently enough to mint a grant.
///
/// Definitive but *actionable*, unlike the introspection rejections: this is
/// always about the caller themselves, so naming the reason leaks nothing and
/// is the only way a console learns to re-prompt.
class VGI_RPC_EXPORT StaleAuthError : public KindedError {
public:
    explicit StaleAuthError(const std::string& what)
        : KindedError("stale_auth", "StaleAuthError", what) {}
};

/// The worker declined to mint this grant.
///
/// Definitive.  The worker holds the policy; the framework only asked.
class VGI_RPC_EXPORT GrantRefusedError : public KindedError {
public:
    explicit GrantRefusedError(const std::string& what)
        : KindedError("grant_refused", "GrantRefusedError", what) {}
};

/// The answer is not *knowable* -- a backing store is down, a 5xx upstream.
///
/// Transient, and distinct from a definitive rejection: a caller that
/// negative-caches "unknown" must not cache this.
///
/// Deliberately **not** a `std::invalid_argument`.  That is this port's
/// equivalent of the Python hazard the contract names: `exception_type_of()`
/// maps `std::invalid_argument` to `ValueError`, which is the wire class a
/// caller reads as "your input was wrong, do not retry".  An authenticator
/// chain that advances on a not-my-credential signal would then treat a
/// thirty-second sidecar outage as "try the next one" and end the chain in a
/// 401, restarting every session in the fleet.  It derives from
/// `KindedError`/`std::runtime_error` instead, and names itself on the wire.
class VGI_RPC_EXPORT IdentityUnavailableError : public KindedError {
public:
    explicit IdentityUnavailableError(const std::string& detail = "",
                                      int retry_after = kDefaultIdentityRetryAfter)
        : KindedError("identity_unavailable", "IdentityUnavailableError",
                      detail.empty() ? "identity lookup unavailable" : detail),
          detail_(detail),
          retry_after_(retry_after) {}

    /// How long the caller should wait before asking again.
    int retry_after() const noexcept { return retry_after_; }
    const std::string& detail() const noexcept { return detail_; }

private:
    std::string detail_;
    int retry_after_;
};

/// Fixed-window request limiter, keyed by caller.
///
/// Present because introspection is a credential-to-identity oracle even when
/// correctly restricted: an allowlisted caller whose own credential leaks can
/// still test guesses.  Rate limiting does not close that, it bounds it.
///
/// Fixed-window rather than a token bucket: a window admits at most twice the
/// rate across a boundary, which is a rounding error here, and the state is two
/// integers per caller rather than a float that has to be aged.
///
/// Thread-safe.  This port's HTTP and socket listeners dispatch unrelated calls
/// concurrently, so an unsynchronized counter would both race and -- far worse
/// for a limiter -- undercount, which is the direction that fails open.
class VGI_RPC_EXPORT RateLimiter {
public:
    explicit RateLimiter(int per_window, double window_seconds = 1.0);

    /// Whether `key` may make a request in the current window, using the
    /// monotonic clock.
    bool allow(const std::string& key);

    /// Whether `key` may make a request in the window containing `now`.
    /// Seconds on a monotonic scale; taken explicitly so tests can drive the
    /// window without sleeping.
    bool allow(const std::string& key, double now);

    /// How many callers the map is currently holding.  Exposed so the
    /// whole-map-reset property can be asserted rather than assumed.
    size_t tracked_keys() const;

private:
    mutable std::mutex mutex_;
    int per_window_;
    double window_seconds_;
    double window_start_ = 0.0;
    std::unordered_map<std::string, int> counts_;
};

/// Validate the introspector allowlist.
///
/// Throws `std::invalid_argument` when the allowlist is missing or empty.
/// There is no permissive default: "any authenticated caller" is precisely the
/// configuration that turns introspection into an open oracle, so it cannot be
/// reached by omission.
VGI_RPC_EXPORT std::set<std::string> normalise_principals(
    const std::vector<std::string>& principals);

/// Return the caller principal, or refuse.
///
/// Checked before anything touches the subject credential: an unauthorized
/// caller must not learn anything about it, including how long it took.
VGI_RPC_EXPORT std::string check_introspector(const AuthContext& auth,
                                              const std::set<std::string>& principals);

/// Refuse a JWS-shaped subject before it reaches a resolver.
///
/// The shape test runs against the **whitespace-trimmed** credential, while the
/// resolver still receives exactly what the caller sent.  Trimming can only add
/// refusals, never remove one, and it closes a padding bypass -- without it
/// `"a.b.c\n"` is not JWS-shaped to a strict matcher and gets routed onward,
/// which is what this guard exists to stop.  Anchor semantics are the least
/// portable corner of seven regex dialects, so the rule deliberately does not
/// depend on them.
///
/// Trimming is for the shape test *only*.  Rewriting a credential before
/// resolving it would make the worker answer about a string the caller never
/// sent.
///
/// Also refuses an empty, whitespace-only, or over-long credential, and does so
/// with the same error: unknown, expired and malformed are one answer.  The
/// length cap applies to the original, since that is what a resolver would be
/// handed.
VGI_RPC_EXPORT void reject_jws_shaped(const std::string& token);

/// Return the caller's `auth_time`, or refuse if it is missing or stale.
///
/// A credential with no verifiable `auth_time` cannot mint.  That single rule
/// is what stops a grant being used to mint another grant: a grant is not an
/// IdP-issued token, so it carries no `auth_time`, so the lineage cannot escape
/// the identity provider.  It also makes the pipe, Unix and TCP transports fail
/// closed for free -- there is no authenticated principal there at all.
///
/// A static bearer proves a machine holds a secret, never that a human just
/// authenticated, so it is refused here too.
///
/// Warning: `auth_time` is an OIDC claim meaning *when this session began*,
/// which can be arbitrarily old while still present and cryptographically
/// valid.  Requiring it is not the same as requiring a recent login: the
/// deployment must send `max_age` (or an appropriate `acr`) at the authorize
/// endpoint for this guard to mean what it says.
VGI_RPC_EXPORT double check_freshness(const AuthContext& auth, double max_auth_age,
                                      std::optional<double> now = std::nullopt);

/// The identity an opaque credential authenticates as.
///
/// **Never carries claims.**  A pass-through claims field would let a worker
/// choose its caller's tenant routing, its row scope, and its policy branch,
/// and the asker derives everything it needs from the principal alone.
///
/// Field order is the schema, and the schema is part of the protocol hash, so
/// these three declarations are a wire contract rather than a layout choice.
struct VGI_RPC_EXPORT TokenIdentity {
    /// The canonical principal, in the exact form the worker itself would
    /// derive -- so an asker that normalises differently does not authorize as
    /// one identity while the worker serves another.
    std::string principal;
    /// Human-readable name for the credential, for audit trails.  Never the
    /// credential.
    std::string token_name;
    /// How long the answer may be cached.  The caller does the caching.  Treat
    /// it as an authorization window: for any path the asker serves without
    /// re-presenting the credential it is exactly that, and therefore also the
    /// revocation lag.
    int64_t ttl_seconds = 300;

    static const std::shared_ptr<arrow::Schema>& arrow_schema();
    /// This value as a one-row Arrow IPC stream.
    std::string serialize_to_bytes() const;
};

/// A standing delegation credential.
struct VGI_RPC_EXPORT IssuedGrant {
    /// The credential.  **Opaque to the framework** -- the worker owns the
    /// format entirely (a sealed envelope, a database row, or a credential
    /// brokered from the IdP are all equally valid and equally invisible here).
    /// Never parsed, never logged.
    std::string token;
    /// Unix timestamp after which the worker will stop honouring the grant.
    /// Required *because* the framework cannot enforce it: the real lifetime
    /// lives inside the opaque token, so this is a declaration rather than an
    /// enforcement.  A worker that must state a lifetime has thought about one.
    double expires_at = 0.0;
    /// Correlation handle for the audit trail.  Not a credential and not secret
    /// -- it is what ties a mint record to later use.
    std::string grant_id;

    static const std::shared_ptr<arrow::Schema>& arrow_schema();
    /// This value as a one-row Arrow IPC stream.
    std::string serialize_to_bytes() const;
};

/// `(token) -> TokenIdentity`.  An empty optional means the store answered and
/// the credential is unknown; throw `IdentityUnavailableError` for "not
/// knowable".
using ResolveTokenHook = std::function<std::optional<TokenIdentity>(const std::string& token)>;

/// `(principal, purpose, scopes, ttl_seconds) -> IssuedGrant`.
using MintGrantHook =
    std::function<IssuedGrant(const std::string& principal, const std::string& purpose,
                              const std::vector<std::string>& scopes, int64_t ttl_seconds)>;

/// Deployment configuration for `vgi_rpc.Identity.v1`.
struct VGI_RPC_EXPORT IdentityOptions {
    ResolveTokenHook resolve_token;
    MintGrantHook mint_grant;
    /// Who may call `introspect_token`.  Required whenever `resolve_token` is
    /// supplied; there is no permissive default.
    std::vector<std::string> introspect_principals;
    int introspect_rate_limit = kDefaultIntrospectRateLimit;
    double max_auth_age = kDefaultMaxAuthAge;
};

/// Applies this file's guards, then delegates to worker-supplied hooks.
///
/// The framework owns the guards and owns none of the policy.  It decides who
/// may ask, how often, and what shape of credential is refused outright; the
/// worker decides what a credential resolves to and whether a grant is minted.
/// That split is deliberate -- the guards are the part that is identical in
/// every deployment and catastrophic to get wrong, and the policy is the part
/// that is different in every deployment and cannot be guessed.
///
/// **A method whose hook is absent is not hosted at all**, so the protocol a
/// server hosts describes what it actually does.  A worker that resolves
/// credentials but does not mint grants hosts `introspect_token` and not
/// `issue_grant`, and a client discovers that through ordinary reflection
/// rather than by calling and reading an error.  Absent beats
/// routed-and-refusing: it is what keeps a dependency upgrade from growing a
/// credential-to-identity oracle on every existing worker.
class VGI_RPC_EXPORT IdentityImpl {
public:
    /// Throws `std::invalid_argument` when `resolve_token` was supplied without
    /// an allowlist.  Validated at construction, not at first call: a worker
    /// that would refuse every introspection should fail to start rather than
    /// serve traffic until someone tries.
    explicit IdentityImpl(IdentityOptions options);

    /// The methods this deployment can actually answer.
    std::set<std::string> offered_methods() const;

    /// Resolve `token`, after checking the caller may ask.
    TokenIdentity introspect_token(const std::string& token, const AuthContext& auth);

    /// Mint a grant for the caller, after checking they authenticated recently.
    ///
    /// **There is no subject parameter.**  The subject is always the caller's
    /// authenticated principal, so cross-subject minting is closed by
    /// construction rather than by a check that could be forgotten in one of
    /// several ports.
    IssuedGrant issue_grant(const std::string& purpose, const std::vector<std::string>& scopes,
                            int64_t ttl_seconds, const AuthContext& auth);

private:
    ResolveTokenHook resolve_token_;
    MintGrantHook mint_grant_;
    std::set<std::string> principals_;
    double max_auth_age_;
    RateLimiter limiter_;
};

/// The identity protocol's method table, narrowed to `offered`.
///
/// Built from the same `MethodInfo` shape every other protocol uses, so the
/// hash and the description read the same table rather than two hand-kept
/// copies that can drift.  A method the deployment did not configure is simply
/// not in it -- which is what makes the narrowing visible in `protocol_hash`,
/// and therefore to a client doing ordinary reflection, rather than only in an
/// error returned to someone who called it.
///
/// `handler` is left empty: these are dispatched by `Server::serve_identity`,
/// which has to pass the connection's `AuthContext` to every guard, and the
/// generic handler signature carries a `CallContext` the raw transports build
/// per call rather than the connection identity the guards turn on.
VGI_RPC_EXPORT std::unordered_map<std::string, MethodInfo> IdentityMethods(
    const std::set<std::string>& offered);

}  // namespace vgi_rpc
