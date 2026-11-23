// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Protocol 3.4 large values (vgi-rpc-python `tools/cross-port/specs/STATE_REF_V1.md`):
/// `StateRef`, the standard token codec, and argument references.
///
/// **This port is single-replica in 3.4 (§5.4).**  The HTTP transport keeps
/// every stream in server memory and seals only a call id into its tokens, so
/// stream state is never serialized.  A `StateRef` here is therefore always
/// held in memory: `CallContext::state_ref` / `CallContext::resolve` work on
/// every transport (worker code stays portable to the SDKs that do serialize),
/// and nothing is ever stored, sealed per turn or fetched for one.  The stored
/// form, the cursor rule and tag expiry do not apply.
///
/// What does apply in full is the server side of argument references: the
/// `__upload_url__/init` grant mints a sealed `ref` per upload
/// (`mint_argument_ref`), and `resolve_argument_ref` opens one for the
/// presenting principal, fetches the object by id through the object cache and
/// verifies it.  The vgi layer resolves `BindRequest.arguments_location` with it.
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/errors.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/identity.h"

namespace arrow {
class RecordBatch;
}  // namespace arrow

namespace vgi_rpc {

class ObjectCache;
class ObjectStore;

// ---------------------------------------------------------------------------
// Errors (STATE_REF_V1.md §6; argument-reference kinds per decision 10)
// ---------------------------------------------------------------------------

/// Unknown form or codec, bad layout, or a stored ref (never valid here).
class VGI_RPC_EXPORT InvalidStateRefError : public KindedError {
public:
    explicit InvalidStateRefError(const std::string& what)
        : KindedError("invalid_state_ref", "InvalidStateRefError", what, Code::INVALID_ARGUMENT) {}
};

/// `state_ref()` called anywhere but the stream method itself (§2.1).
class VGI_RPC_EXPORT StateRefMisuseError : public KindedError {
public:
    explicit StateRefMisuseError(const std::string& what)
        : KindedError("state_ref_misuse", "StateRefMisuseError", what, Code::FAILED_PRECONDITION) {}
};

/// An argument reference does not open for this caller, does not match the
/// location it was sent with, or cannot be resolved by this server.
class VGI_RPC_EXPORT InvalidArgumentsRefError : public KindedError {
public:
    explicit InvalidArgumentsRefError(const std::string& what)
        : KindedError("invalid_arguments_ref", "InvalidArgumentsRefError", what,
                      Code::INVALID_ARGUMENT) {}
};

/// The fetched argument object's length or SHA-256 differs from the sealed values.
class VGI_RPC_EXPORT ArgumentsRefIntegrityError : public KindedError {
public:
    explicit ArgumentsRefIntegrityError(const std::string& what)
        : KindedError("arguments_ref_integrity", "ArgumentsRefIntegrityError", what,
                      Code::DATA_LOSS) {}
};

/// Storage reports the argument object absent (deleted by lifecycle, never
/// uploaded).  The vgi layer re-raises it with its prepared-statement message.
class VGI_RPC_EXPORT ArgumentsRefMissingError : public KindedError {
public:
    explicit ArgumentsRefMissingError(
        const std::string& what = "the uploaded argument object is gone (expired or deleted)")
        : KindedError("arguments_ref_missing", "ArgumentsRefMissingError", what, Code::NOT_FOUND) {}
};

/// A transient storage failure (5xx, timeout, refused connection) on `get`.
class VGI_RPC_EXPORT StorageUnavailableError : public KindedError {
public:
    explicit StorageUnavailableError(const std::string& what)
        : KindedError("storage_unavailable", "StorageUnavailableError", what, Code::UNAVAILABLE) {}
};

// ---------------------------------------------------------------------------
// The standard token codec (§7)
// ---------------------------------------------------------------------------

/// `payload := codec (1 byte) || body`, compressed before anything is sealed.
///
/// This port's tokens carry only a stream's method name and call id, so no
/// token of its own has a payload to compress in 3.4 (§7: C++ adopts the
/// codec for its tokens when it goes stateless).  The codec is used for the
/// inline `StateRef` encoding, the one place a payload is serialized.
namespace token_codec {

inline constexpr uint8_t RAW = 0x00;
inline constexpr uint8_t ZSTD = 0x01;
/// Reserved for runtimes without zstd.  This writer never emits it, so this
/// reader refuses it like any unknown tag (§7: a reader accepts every codec
/// its own writer can emit).
inline constexpr uint8_t DEFLATE = 0x02;
/// Bound on a decompressed token (§7).  A `StateRef` value may be larger: its
/// bound is the larger of this and the server's `max_fetch_bytes` (§3.1).
inline constexpr int64_t MAX_TOKEN_BYTES = int64_t{64} << 20;

/// zstd (level 3) when that is smaller, else raw: a small payload never grows.
VGI_RPC_EXPORT std::string encode(std::string_view plaintext);

/// Inverse of `encode`, bounded at `max_bytes` of output.  Throws
/// `std::invalid_argument` for an empty payload, an unknown codec, a body that
/// does not decompress, or one over the bound (a frame's declared content size
/// is checked before anything is inflated).
VGI_RPC_EXPORT std::string decode(std::string_view payload, int64_t max_bytes = MAX_TOKEN_BYTES);

}  // namespace token_codec

// ---------------------------------------------------------------------------
// StateRef (§2, §3)
// ---------------------------------------------------------------------------

/// A reference to an immutable per-stream value: create it with
/// `CallContext::state_ref` in a stream method, keep it in the stream's state,
/// read it back on any turn with `CallContext::resolve`.
///
/// Cheap to copy (copies share the value).  In this single-replica port the
/// value always stays in process memory; `encode()` renders the §3.2 inline
/// form (`0x01 || codec || body`) for callers that persist state for their own
/// reasons, and `decode()` reads it back.  A stored form (`0x02`) never
/// appears here and is refused with `invalid_state_ref`.
class VGI_RPC_EXPORT StateRef {
public:
    /// An empty ref; `valid()` is false and resolving it throws.
    StateRef() = default;

    bool valid() const noexcept { return impl_ != nullptr; }

    /// `memory` for a ref made by `state_ref()` (never serialized into a
    /// token: always, in this port), `inline` for one read by `decode()`.
    const char* form() const noexcept;

    /// The §3.2 inline encoding.  Does not change `form()`.
    std::string encode() const;

    /// Read an encoded ref.  Inline only; anything else is
    /// `InvalidStateRefError`.  The value is decompressed now, bounded at
    /// `max(64 MiB, max_value_bytes)`.
    static StateRef decode(std::string_view encoded, int64_t max_value_bytes = 0);

private:
    friend class CallContext;
    struct Impl;
    explicit StateRef(std::shared_ptr<const Impl> impl) : impl_(std::move(impl)) {}
    std::shared_ptr<const Impl> impl_;
};

// ---------------------------------------------------------------------------
// Sealing scopes (§4) and argument references (§10.2)
// ---------------------------------------------------------------------------

/// The scope of a reference bound to the principal only: `__upload_url__`
/// lives at the server prefix, not under a protocol.
VGI_RPC_EXPORT const std::string& server_scope();

/// `BASE_AAD(auth, protocol)`: the Python reference's cursor-token AAD,
/// `vgi_rpc.state.v6\0` (or `.v7\0` when peer authentication contributed a
/// `peer_evidence_binding` claim) `|| tail || \0 || protocol`.
VGI_RPC_EXPORT std::string base_aad(const AuthContext& auth, const std::string& protocol);

/// `ARGREF_AAD`: `vgi_rpc.argref.v1\0 || BASE_AAD(auth, server_scope())`.
VGI_RPC_EXPORT std::string argument_ref_aad(const AuthContext& auth);

/// Seal one grant's argument reference from the *claimed* size and digest
/// (`0x01 || nonce || AEAD(0x01 || size u64le || sha256[32] || id_len u16le
/// || object_id, ARGREF_AAD)`).  No expiry: on an unauthenticated server a
/// reference is a bearer capability for as long as the object lives.  Throws
/// `std::invalid_argument` on a size < 1, a digest that is not 64 lowercase
/// hex, or an object id that is empty or over 1024 bytes.
VGI_RPC_EXPORT std::string mint_argument_ref(const std::array<uint8_t, crypto::kAeadKeyBytes>& key,
                                             const AuthContext& auth, int64_t size,
                                             const std::string& sha256_hex,
                                             const std::string& object_id);

/// What an argument reference seals.
struct ArgumentRefClaims {
    int64_t size = 0;
    std::string sha256_hex;
    std::string object_id;
};

/// Open a reference for the presenting principal.  Every failure (wrong key,
/// other principal, tampered byte, bad layout) is `InvalidArgumentsRefError`,
/// with no distinction between them.
VGI_RPC_EXPORT ArgumentRefClaims
open_argument_ref(const std::array<uint8_t, crypto::kAeadKeyBytes>& key, const AuthContext& auth,
                  const std::string& ref);

/// Resolve a reference (§10.2): check the claimed location, open the ref, check
/// its sealed size and digest equal the claimed ones, then the object cache by
/// `object_id`, on a miss a single-flight `store->get`, verify length and
/// SHA-256, return.  Errors: `InvalidArgumentsRefError` (location, no store,
/// ref, mismatch, over `max_fetch_bytes`), `ArgumentsRefIntegrityError`,
/// `ArgumentsRefMissingError`, `StorageUnavailableError`.
VGI_RPC_EXPORT std::shared_ptr<const std::string> resolve_argument_ref(
    const std::string& ref, int64_t size, const std::string& sha256_hex,
    const std::array<uint8_t, crypto::kAeadKeyBytes>& key, const AuthContext& auth,
    ObjectStore* store, int64_t max_fetch_bytes, ObjectCache& cache);

/// What a transport hands a `CallContext` so it can resolve argument
/// references: the HTTP transport's token key, object store and fetch cap.
/// Absent on the byte-stream transports, which vend no grants.
struct StateRefRuntime {
    std::array<uint8_t, crypto::kAeadKeyBytes> token_key{};
    ObjectStore* store = nullptr;  // owned by the transport; outlives every request
    int64_t max_fetch_bytes = 0;
    ObjectCache* cache = nullptr;  // null = default_object_cache()
};

}  // namespace vgi_rpc
