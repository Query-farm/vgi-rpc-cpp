// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Every primitive here is libsodium's. This file is the envelope layout and
// the encodings, not an implementation of any algorithm.
//
// libsodium rather than OpenSSL because the wire's AEAD is XChaCha20-Poly1305
// — a 24-byte nonce, which is what makes randomly-generated nonces safe at the
// volumes a token minter reaches — and OpenSSL ships only the IETF 12-byte
// variant. It is also what the Python reference reaches through PyNaCl, so the
// two implementations agree by construction rather than by both having been
// written from the same RFC.

#include "vgi_rpc/crypto.h"

#include <cstring>
#include <stdexcept>

#include <sodium.h>

namespace vgi_rpc::crypto {

namespace {

// `sodium_init` must complete before any other call, may be called more than
// once, and is thread-safe only after the first has returned. A function-local
// static gives exactly that guarantee with no ordering assumptions.
void require_sodium() {
    static const bool ready = [] {
        if (sodium_init() < 0) {
            throw std::runtime_error("vgi_rpc: libsodium failed to initialise");
        }
        return true;
    }();
    (void)ready;
}

}  // namespace

// --- Digests ---------------------------------------------------------------

void Sha256::reset() {
    require_sodium();
    static_assert(sizeof(state_) >= sizeof(crypto_hash_sha256_state),
                  "Sha256::state_ is too small for libsodium's state");
    crypto_hash_sha256_init(reinterpret_cast<crypto_hash_sha256_state*>(state_));
}

void Sha256::update(const uint8_t* data, size_t len) {
    crypto_hash_sha256_update(reinterpret_cast<crypto_hash_sha256_state*>(state_), data, len);
}

std::array<uint8_t, 32> Sha256::digest() {
    std::array<uint8_t, 32> out{};
    crypto_hash_sha256_final(reinterpret_cast<crypto_hash_sha256_state*>(state_), out.data());
    return out;
}

std::string Sha256::hex_digest() {
    const auto d = digest();
    return hex_encode(d.data(), d.size());
}

std::array<uint8_t, 32> sha256(const uint8_t* data, size_t len) {
    require_sodium();
    std::array<uint8_t, 32> out{};
    crypto_hash_sha256(out.data(), data, len);
    return out;
}

std::array<uint8_t, 32> hmac_sha256(const uint8_t* key, size_t key_len, const uint8_t* msg,
                                    size_t msg_len) {
    require_sodium();
    // The `_init` form rather than the one-shot, because the one-shot demands
    // a 32-byte key and callers here supply operator-chosen secrets of any
    // length. `_init` does HMAC's own key preparation: hash if longer than the
    // block, zero-pad if shorter.
    crypto_auth_hmacsha256_state state;
    crypto_auth_hmacsha256_init(&state, key, key_len);
    crypto_auth_hmacsha256_update(&state, msg, msg_len);
    std::array<uint8_t, 32> out{};
    crypto_auth_hmacsha256_final(&state, out.data());
    return out;
}

std::array<uint8_t, 32> hmac_sha256(const std::string& key, const std::string& msg) {
    return hmac_sha256(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                       reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
}

// --- AEAD ------------------------------------------------------------------

static_assert(kAeadKeyBytes == crypto_aead_xchacha20poly1305_ietf_KEYBYTES);
static_assert(kAeadNonceBytes == crypto_aead_xchacha20poly1305_ietf_NPUBBYTES);
static_assert(kAeadTagBytes == crypto_aead_xchacha20poly1305_ietf_ABYTES);

std::string aead_seal(const std::array<uint8_t, kAeadKeyBytes>& key, const std::string& plaintext,
                      const std::string& aad) {
    require_sodium();
    // nonce || ciphertext || tag. The nonce is generated here and never taken
    // from the caller, so a caller cannot repeat one.
    std::string out(kAeadNonceBytes + plaintext.size() + kAeadTagBytes, '\0');
    auto* nonce = reinterpret_cast<uint8_t*>(out.data());
    randombytes_buf(nonce, kAeadNonceBytes);

    unsigned long long sealed_len = 0;
    crypto_aead_xchacha20poly1305_ietf_encrypt(
        nonce + kAeadNonceBytes, &sealed_len, reinterpret_cast<const uint8_t*>(plaintext.data()),
        plaintext.size(), reinterpret_cast<const uint8_t*>(aad.data()), aad.size(),
        /*nsec=*/nullptr, nonce, key.data());
    out.resize(kAeadNonceBytes + static_cast<size_t>(sealed_len));
    return out;
}

std::optional<std::string> aead_open(const std::array<uint8_t, kAeadKeyBytes>& key,
                                     const std::string& sealed, const std::string& aad) {
    require_sodium();
    if (sealed.size() < kAeadNonceBytes + kAeadTagBytes) return std::nullopt;

    const auto* nonce = reinterpret_cast<const uint8_t*>(sealed.data());
    const auto* body = nonce + kAeadNonceBytes;
    const size_t body_len = sealed.size() - kAeadNonceBytes;

    std::string out(body_len - kAeadTagBytes, '\0');
    unsigned long long out_len = 0;
    // One `nullopt` for every failure — wrong key, wrong AAD, truncation, a
    // flipped bit — deliberately, so nothing downstream can branch on which.
    if (crypto_aead_xchacha20poly1305_ietf_decrypt(
            out.empty() ? nullptr : reinterpret_cast<uint8_t*>(out.data()), &out_len,
            /*nsec=*/nullptr, body, body_len, reinterpret_cast<const uint8_t*>(aad.data()),
            aad.size(), nonce, key.data()) != 0) {
        return std::nullopt;
    }
    out.resize(static_cast<size_t>(out_len));
    return out;
}

// --- Encodings and comparison ---------------------------------------------
//
// Not libsodium's: `sodium_bin2base64` writes the padded variant, and this
// wire carries unpadded base64url. Kept as they were.

namespace {

constexpr char kB64Url[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int b64url_value(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '-') return 62;
    if (c == '_') return 63;
    return -1;
}

}  // namespace

std::string base64url_encode(const uint8_t* data, size_t len) {
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 3 <= len; i += 3) {
        const uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                           (static_cast<uint32_t>(data[i + 1]) << 8) |
                           static_cast<uint32_t>(data[i + 2]);
        out.push_back(kB64Url[(n >> 18) & 0x3F]);
        out.push_back(kB64Url[(n >> 12) & 0x3F]);
        out.push_back(kB64Url[(n >> 6) & 0x3F]);
        out.push_back(kB64Url[n & 0x3F]);
    }
    if (i + 1 == len) {
        const uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        out.push_back(kB64Url[(n >> 18) & 0x3F]);
        out.push_back(kB64Url[(n >> 12) & 0x3F]);
    } else if (i + 2 == len) {
        const uint32_t n =
            (static_cast<uint32_t>(data[i]) << 16) | (static_cast<uint32_t>(data[i + 1]) << 8);
        out.push_back(kB64Url[(n >> 18) & 0x3F]);
        out.push_back(kB64Url[(n >> 12) & 0x3F]);
        out.push_back(kB64Url[(n >> 6) & 0x3F]);
    }
    return out;
}

std::string base64url_encode(const std::string& s) {
    return base64url_encode(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

std::optional<std::string> base64url_decode(const std::string& s) {
    // Strict: reject any character outside the alphabet rather than skipping
    // it.  A lenient decoder turns a tampered token into a different valid
    // one, which is how two ports end up reporting different reason codes for
    // the same bytes.
    std::string in = s;
    while (!in.empty() && in.back() == '=') in.pop_back();
    if (in.size() % 4 == 1) return std::nullopt;

    std::string out;
    out.reserve((in.size() / 4) * 3 + 2);
    uint32_t acc = 0;
    int bits = 0;
    for (char c : in) {
        const int v = b64url_value(c);
        if (v < 0) return std::nullopt;
        acc = (acc << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((acc >> bits) & 0xFF));
        }
    }
    return out;
}

std::string hex_encode(const uint8_t* data, size_t len) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string out(len * 2, '0');
    for (size_t i = 0; i < len; ++i) {
        out[i * 2] = hex[data[i] >> 4];
        out[i * 2 + 1] = hex[data[i] & 0x0F];
    }
    return out;
}

std::optional<std::vector<uint8_t>> hex_decode(const std::string& s) {
    if (s.size() % 2 != 0) return std::nullopt;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::vector<uint8_t> out;
    out.reserve(s.size() / 2);
    for (size_t i = 0; i < s.size(); i += 2) {
        const int hi = nibble(s[i]), lo = nibble(s[i + 1]);
        if (hi < 0 || lo < 0) return std::nullopt;
        out.push_back(static_cast<uint8_t>((hi << 4) | lo));
    }
    return out;
}

bool constant_time_equal(const std::string& a, const std::string& b) {
    // The length check leaks only the length, which is not the secret. Beyond
    // that, `sodium_memcmp` is the constant-time compare — the obvious
    // `a == b` short-circuits on the first differing byte and leaks the
    // position of the mismatch through timing.
    if (a.size() != b.size()) return false;
    if (a.empty()) return true;
    return sodium_memcmp(a.data(), b.data(), a.size()) == 0;
}

// --- Randomness ------------------------------------------------------------

std::vector<uint8_t> random_bytes(size_t n) {
    require_sodium();
    std::vector<uint8_t> out(n);
    if (n) randombytes_buf(out.data(), n);
    return out;
}

std::array<uint8_t, kAeadKeyBytes> random_key() {
    require_sodium();
    std::array<uint8_t, kAeadKeyBytes> key{};
    crypto_aead_xchacha20poly1305_ietf_keygen(key.data());
    return key;
}

}  // namespace vgi_rpc::crypto
