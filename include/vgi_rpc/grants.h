// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

/// Sealed grants: the framework's own credential format for `issue_grant`.
///
/// `issue_grant` mints a credential meant to be presented later, by unattended
/// automation, as an ordinary bearer.  Until this file nothing accepted one, so
/// the loop was open.  With grant keys configured the framework mints these
/// itself (unless the worker supplies `mint_grant`) and the HTTP transport
/// accepts them back as bearer credentials (WIRE_PROTOCOL.md §16, "Accepting
/// identity credentials"; IDENTITY_V1_SPEC.md §9).  With no key configured,
/// nothing changes.
///
/// Token: `"vgig1." + base64url_nopad(kid(8) || envelope)`.
///
/// - `kid = SHA-256("vgi_rpc.grant.kid.v1\0" || key)[0:8]`, in the clear so a
///   verifier selects the key without trial decryption, and bound by the AAD.
/// - `envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(payload) || tag(16)`,
///   the same envelope the stream-state tokens use.
/// - `aad = "vgi_rpc.grant.v1\0" || kid || UTF-8(audience)`, so two deployments
///   that (against advice) share a key still cannot accept each other's grants.
/// - The payload is a fixed little-endian binary record rather than JSON, so the
///   published vectors are byte-exact in every language.
///
/// Grants are not individually revocable.  No storage is the point; expiry is
/// the revocation, which is why the default ceiling is short (seven days) and
/// why removing a key revokes everything it minted.

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// Token prefix.  The version lives in the prefix, so an incompatible format is
/// a different prefix -- routed elsewhere, never half-parsed.
inline constexpr const char* kGrantTokenPrefix = "vgig1.";

/// Environment a worker reads its grant configuration from.
inline constexpr const char* kGrantKeysEnv = "VGI_RPC_GRANT_KEYS";
inline constexpr const char* kGrantAudienceEnv = "VGI_RPC_GRANT_AUDIENCE";
inline constexpr const char* kGrantMaxTtlEnv = "VGI_RPC_GRANT_MAX_TTL_SECONDS";

/// Longest lifetime a minted grant may have unless the deployment says
/// otherwise.  Short on purpose: expiry is the only revocation a grant has.
inline constexpr int64_t kDefaultGrantMaxTtlSeconds = 7 * 24 * 3600;

/// Allowance for the minting and verifying workers' clocks disagreeing.
inline constexpr int64_t kDefaultGrantClockSkewSeconds = 60;

/// Longest token text considered at all -- the cap `introspect_token` applies
/// to a credential, so a verifier is never handed megabytes.
inline constexpr size_t kMaxGrantTokenChars = 4096;

/// A grant key: exactly 32 bytes.
using GrantKey = std::array<uint8_t, crypto::kAeadKeyBytes>;

/// A `vgig1.` credential that could not be accepted.
///
/// One type for every cause -- malformed, wrong key, wrong audience, tampered,
/// expired -- so a caller cannot tell a forged token from a stale one except by
/// `expired()`, which is only true once the token was proven authentic and so
/// reveals nothing a forger could use.
class VGI_RPC_EXPORT GrantInvalidError : public std::runtime_error {
public:
    explicit GrantInvalidError(const std::string& detail, bool expired = false)
        : std::runtime_error(detail), expired_(expired) {}

    /// Authentic, but outside its lifetime (expired, or not yet valid).
    bool expired() const noexcept { return expired_; }

private:
    bool expired_;
};

/// The 8-byte key id a token names its sealing key with.
VGI_RPC_EXPORT std::string grant_key_id(const GrantKey& key);

/// A deployment's grant configuration.
///
/// The first key mints; every key verifies.  Rotation: add the new key first,
/// keep the old one after it until every grant it minted has expired, then drop
/// it.
struct VGI_RPC_EXPORT GrantKeys {
    /// Minting key first.
    std::vector<GrantKey> keys;
    /// Bound into every token's associated data.
    std::string audience;
    /// Ceiling on a grant's lifetime, at minting and at verification.
    int64_t max_ttl_seconds = kDefaultGrantMaxTtlSeconds;
    /// Tolerance applied to `issued_at` and `expires_at`.
    int64_t clock_skew_seconds = kDefaultGrantClockSkewSeconds;

    /// Throws `std::invalid_argument` for a configuration that could not mint
    /// or verify safely: no key, two keys with one id, a non-positive
    /// lifetime, a negative skew, or an audience too long to encode.
    void validate() const;

    /// Build from standard base64 key text (padding optional), minting key
    /// first, and validate.  A key that is not base64 of exactly 32 bytes
    /// throws `std::invalid_argument`: a worker refuses to start rather than
    /// run with a key it misread.
    static GrantKeys parse(const std::vector<std::string>& encoded_keys, std::string audience = "",
                           int64_t max_ttl_seconds = kDefaultGrantMaxTtlSeconds,
                           int64_t clock_skew_seconds = kDefaultGrantClockSkewSeconds);

    /// Read `VGI_RPC_GRANT_KEYS` (comma-separated base64, minting key first),
    /// `VGI_RPC_GRANT_AUDIENCE` and `VGI_RPC_GRANT_MAX_TTL_SECONDS`.  `nullopt`
    /// when no key is set -- grants off.  Throws `std::invalid_argument` for a
    /// malformed key or lifetime.
    ///
    /// `getenv` replaces the process environment, for tests.
    static std::optional<GrantKeys> from_env(
        const std::function<std::optional<std::string>(const char*)>& getenv = {});

    /// `"vgi_rpc.grant.v1\0" || kid || UTF-8(audience)`.
    std::string aad(const std::string& kid) const;
};

/// What a verified grant says.
struct VGI_RPC_EXPORT GrantClaims {
    std::string principal;
    std::vector<std::string> scopes;
    std::string purpose;
    std::string grant_id;
    int64_t issued_at = 0;
    int64_t expires_at = 0;
};

/// The little-endian payload record: `issued_at i64, expires_at i64,
/// grant_id, principal, purpose` as u16-length UTF-8, then a u16 scope count
/// and each scope the same way.  Throws `std::invalid_argument` for a field or
/// scope list over 65535.
VGI_RPC_EXPORT std::string encode_grant_payload(const GrantClaims& claims);

/// Strict inverse: exact lengths, valid UTF-8, no trailing bytes.  Throws
/// `GrantInvalidError`.
VGI_RPC_EXPORT GrantClaims decode_grant_payload(const std::string& payload);

/// Overrides for `mint_grant_token`.  Every field is for tests and published
/// vectors; production leaves them all unset.
struct VGI_RPC_EXPORT GrantMintOverrides {
    std::optional<int64_t> now;
    std::optional<std::string> grant_id;
    /// A fixed nonce.  Reusing a nonce under one key destroys
    /// XChaCha20-Poly1305 -- **test vectors only**.
    std::optional<std::array<uint8_t, crypto::kAeadNonceBytes>> nonce;
};

struct VGI_RPC_EXPORT MintedGrantToken {
    std::string token;
    GrantClaims claims;
};

/// Mint a sealed grant with the first configured key.  The lifetime is
/// `min(ttl_seconds, max_ttl_seconds)`.  Throws `std::invalid_argument` for a
/// non-positive lifetime or a field too long to encode.
VGI_RPC_EXPORT MintedGrantToken mint_grant_token(const GrantKeys& keys,
                                                 const std::string& principal,
                                                 const std::vector<std::string>& scopes,
                                                 const std::string& purpose, int64_t ttl_seconds,
                                                 const GrantMintOverrides& overrides = {});

/// Verify a sealed grant and return its claims.
///
/// Order, normative: prefix, length, canonical base64url, key id, AEAD open,
/// payload, then lifetime -- the lifetime is inside the ciphertext, so it is
/// only trusted after the tag verified.  Throws `GrantInvalidError` for every
/// cause; `expired()` is set only for an authentic grant outside its lifetime.
///
/// `now` overrides the clock (seconds since the epoch), for tests.
VGI_RPC_EXPORT GrantClaims verify_grant_token(const GrantKeys& keys, const std::string& token,
                                              std::optional<double> now = std::nullopt);

}  // namespace vgi_rpc
