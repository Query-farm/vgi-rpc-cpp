// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Primitives for the HTTP transport's authenticated surfaces: state-token
/// sealing (XChaCha20-Poly1305), proxy-proof MACs (HMAC-SHA256), and the
/// encodings both ride on.
///
/// libsodium underneath, not our own arithmetic. It is the only common library
/// that ships XChaCha20-Poly1305 — OpenSSL has the IETF 12-byte-nonce variant
/// and not this one, and the 24-byte nonce is the whole reason the envelope
/// can generate nonces at random. It is also what the Python reference reaches
/// through PyNaCl, so the two ends agree by construction rather than by both
/// having been written from the same RFC.
///
/// Only the base64url and hex encodings are ours, because libsodium's base64
/// writes the padded variant and this wire carries unpadded.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"

namespace vgi_rpc::crypto {

// --- Digests ---------------------------------------------------------------

// Streaming SHA-256 (FIPS 180-4). libsodium underneath.
class VGI_RPC_EXPORT Sha256 {
public:
    Sha256() { reset(); }

    void update(const uint8_t* data, size_t len);
    void update(const std::string& s) {
        update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }
    void update_byte(uint8_t b) { update(&b, 1); }

    // Both finalize; the object must not be reused afterwards.
    std::array<uint8_t, 32> digest();
    std::string hex_digest();

private:
    void reset();

    // libsodium's `crypto_hash_sha256_state` by value, without putting
    // <sodium.h> in this header — a consumer of the SDK should not have to
    // find libsodium's includes to use a digest. Sized with room to spare and
    // static_assert'd against the real type in crypto.cpp, so a libsodium that
    // grew its state fails to compile rather than to run.
    alignas(8) unsigned char state_[128]{};
};

VGI_RPC_EXPORT std::array<uint8_t, 32> sha256(const uint8_t* data, size_t len);

// HMAC-SHA256 (RFC 2104).
VGI_RPC_EXPORT std::array<uint8_t, 32> hmac_sha256(const uint8_t* key, size_t key_len,
                                                   const uint8_t* msg, size_t msg_len);
VGI_RPC_EXPORT std::array<uint8_t, 32> hmac_sha256(const std::string& key, const std::string& msg);

// --- AEAD ------------------------------------------------------------------

// XChaCha20-Poly1305: HChaCha20 key derivation over the first 16 nonce bytes,
// then RFC 8439 ChaCha20-Poly1305 with the remaining 8 as the low half of a
// 12-byte nonce.  A 24-byte nonce is what makes random nonces safe at the
// volumes a token minter reaches.
inline constexpr size_t kAeadKeyBytes = 32;
inline constexpr size_t kAeadNonceBytes = 24;
inline constexpr size_t kAeadTagBytes = 16;

// Returns nonce || ciphertext || tag.  The nonce is generated internally from
// the system CSPRNG; a caller cannot supply one and so cannot repeat one.
VGI_RPC_EXPORT std::string aead_seal(const std::array<uint8_t, kAeadKeyBytes>& key,
                                     const std::string& plaintext, const std::string& aad);

// Inverse of aead_seal.  Returns nullopt on any failure — a wrong key, a
// wrong AAD, a truncated envelope, or a tampered byte — with no distinction
// between them, deliberately.
VGI_RPC_EXPORT std::optional<std::string> aead_open(const std::array<uint8_t, kAeadKeyBytes>& key,
                                                    const std::string& sealed,
                                                    const std::string& aad);

// --- Encodings and comparison ---------------------------------------------

// Base64url without padding (RFC 4648 §5), the header-safe encoding every
// token in this protocol uses.
VGI_RPC_EXPORT std::string base64url_encode(const uint8_t* data, size_t len);
VGI_RPC_EXPORT std::string base64url_encode(const std::string& s);
VGI_RPC_EXPORT std::optional<std::string> base64url_decode(const std::string& s);

VGI_RPC_EXPORT std::string hex_encode(const uint8_t* data, size_t len);
VGI_RPC_EXPORT std::optional<std::vector<uint8_t>> hex_decode(const std::string& s);

// Length-independent only for equal-length inputs; unequal lengths return
// false immediately, which leaks nothing a MAC's fixed width does not.
VGI_RPC_EXPORT bool constant_time_equal(const std::string& a, const std::string& b);

// --- Randomness ------------------------------------------------------------

// Cryptographically secure bytes from the OS.  Throws if the OS cannot
// supply them: proceeding with a predictable nonce is worse than not starting.
VGI_RPC_EXPORT std::vector<uint8_t> random_bytes(size_t n);
VGI_RPC_EXPORT std::array<uint8_t, kAeadKeyBytes> random_key();

}  // namespace vgi_rpc::crypto
