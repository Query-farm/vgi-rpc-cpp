// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/crypto.h"

#include <cstring>
#include <stdexcept>

#ifdef _WIN32
  // windows.h defines min/max as macros and ERROR as a constant, all of
  // which collide with ordinary C++ spellings; these three keep it to itself.
  #define WIN32_LEAN_AND_MEAN
  #define NOMINMAX
  #define NOGDI
  #include <windows.h>
  #include <bcrypt.h>
#else
  #include <fcntl.h>
  #include <unistd.h>
#endif

namespace vgi_rpc::crypto {

namespace {

inline uint32_t rotl32(uint32_t x, int n) { return (x << n) | (x >> (32 - n)); }
inline uint32_t rotr32(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }

inline uint32_t load32_le(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

inline void store32_le(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}

}  // namespace

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4)
// ---------------------------------------------------------------------------

void Sha256::reset() {
    state_[0] = 0x6a09e667; state_[1] = 0xbb67ae85;
    state_[2] = 0x3c6ef372; state_[3] = 0xa54ff53a;
    state_[4] = 0x510e527f; state_[5] = 0x9b05688c;
    state_[6] = 0x1f83d9ab; state_[7] = 0x5be0cd19;
    bitlen_ = 0;
    buflen_ = 0;
}

void Sha256::update(const uint8_t* data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        buf_[buflen_++] = data[i];
        if (buflen_ == 64) {
            transform(buf_);
            bitlen_ += 512;
            buflen_ = 0;
        }
    }
}

void Sha256::transform(const uint8_t* chunk) {
    static constexpr uint32_t k[64] = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(chunk[i * 4]) << 24) |
               (static_cast<uint32_t>(chunk[i * 4 + 1]) << 16) |
               (static_cast<uint32_t>(chunk[i * 4 + 2]) << 8) |
               (static_cast<uint32_t>(chunk[i * 4 + 3]));
    }
    for (int i = 16; i < 64; ++i) {
        uint32_t s0 = rotr32(w[i - 15], 7) ^ rotr32(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = rotr32(w[i - 2], 17) ^ rotr32(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
    uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
    for (int i = 0; i < 64; ++i) {
        uint32_t S1 = rotr32(e, 6) ^ rotr32(e, 11) ^ rotr32(e, 25);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t t1 = h + S1 + ch + k[i] + w[i];
        uint32_t S0 = rotr32(a, 2) ^ rotr32(a, 13) ^ rotr32(a, 22);
        uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t t2 = S0 + maj;
        h = g; g = f; f = e; e = d + t1;
        d = c; c = b; b = a; a = t1 + t2;
    }
    state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
    state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
}

std::array<uint8_t, 32> Sha256::digest() {
    uint64_t total_bits = bitlen_ + static_cast<uint64_t>(buflen_) * 8;
    update_byte(0x80);
    while (buflen_ != 56) {
        uint8_t z = 0;
        update(&z, 1);
    }
    uint8_t len_be[8];
    for (int i = 0; i < 8; ++i) {
        len_be[i] = static_cast<uint8_t>(total_bits >> (56 - i * 8));
    }
    update(len_be, 8);

    std::array<uint8_t, 32> out{};
    for (int i = 0; i < 8; ++i) {
        out[i * 4] = static_cast<uint8_t>(state_[i] >> 24);
        out[i * 4 + 1] = static_cast<uint8_t>(state_[i] >> 16);
        out[i * 4 + 2] = static_cast<uint8_t>(state_[i] >> 8);
        out[i * 4 + 3] = static_cast<uint8_t>(state_[i]);
    }
    return out;
}

std::string Sha256::hex_digest() {
    auto d = digest();
    return hex_encode(d.data(), d.size());
}

std::array<uint8_t, 32> sha256(const uint8_t* data, size_t len) {
    Sha256 h;
    h.update(data, len);
    return h.digest();
}

std::array<uint8_t, 32> hmac_sha256(const uint8_t* key, size_t key_len,
                                    const uint8_t* msg, size_t msg_len) {
    // RFC 2104: keys longer than the block size are hashed first, shorter ones
    // zero-padded to it.
    uint8_t k0[64] = {};
    if (key_len > 64) {
        auto hk = sha256(key, key_len);
        std::memcpy(k0, hk.data(), hk.size());
    } else {
        std::memcpy(k0, key, key_len);
    }

    uint8_t ipad[64], opad[64];
    for (size_t i = 0; i < 64; ++i) {
        ipad[i] = static_cast<uint8_t>(k0[i] ^ 0x36);
        opad[i] = static_cast<uint8_t>(k0[i] ^ 0x5c);
    }

    Sha256 inner;
    inner.update(ipad, 64);
    inner.update(msg, msg_len);
    auto inner_digest = inner.digest();

    Sha256 outer;
    outer.update(opad, 64);
    outer.update(inner_digest.data(), inner_digest.size());
    return outer.digest();
}

std::array<uint8_t, 32> hmac_sha256(const std::string& key, const std::string& msg) {
    return hmac_sha256(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                       reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
}

// ---------------------------------------------------------------------------
// ChaCha20 / Poly1305 / XChaCha20-Poly1305 (RFC 8439 + the XChaCha draft)
// ---------------------------------------------------------------------------

namespace {

void chacha20_block(const uint32_t in[16], uint8_t out[64]) {
    uint32_t x[16];
    std::memcpy(x, in, sizeof(x));
    auto quarter = [&](int a, int b, int c, int d) {
        x[a] += x[b]; x[d] = rotl32(x[d] ^ x[a], 16);
        x[c] += x[d]; x[b] = rotl32(x[b] ^ x[c], 12);
        x[a] += x[b]; x[d] = rotl32(x[d] ^ x[a], 8);
        x[c] += x[d]; x[b] = rotl32(x[b] ^ x[c], 7);
    };
    for (int i = 0; i < 10; ++i) {
        quarter(0, 4, 8, 12);  quarter(1, 5, 9, 13);
        quarter(2, 6, 10, 14); quarter(3, 7, 11, 15);
        quarter(0, 5, 10, 15); quarter(1, 6, 11, 12);
        quarter(2, 7, 8, 13);  quarter(3, 4, 9, 14);
    }
    for (int i = 0; i < 16; ++i) store32_le(out + i * 4, x[i] + in[i]);
}

void chacha20_init(uint32_t st[16], const uint8_t key[32], const uint8_t nonce[12],
                   uint32_t counter) {
    st[0] = 0x61707865; st[1] = 0x3320646e; st[2] = 0x79622d32; st[3] = 0x6b206574;
    for (int i = 0; i < 8; ++i) st[4 + i] = load32_le(key + i * 4);
    st[12] = counter;
    for (int i = 0; i < 3; ++i) st[13 + i] = load32_le(nonce + i * 4);
}

// XOR `len` bytes of ChaCha20 keystream (starting at `counter`) into `data`.
void chacha20_xor(const uint8_t key[32], const uint8_t nonce[12], uint32_t counter,
                  uint8_t* data, size_t len) {
    uint32_t st[16];
    chacha20_init(st, key, nonce, counter);
    uint8_t block[64];
    size_t off = 0;
    while (off < len) {
        chacha20_block(st, block);
        const size_t n = (len - off < 64) ? (len - off) : 64;
        for (size_t i = 0; i < n; ++i) data[off + i] ^= block[i];
        off += n;
        ++st[12];
    }
}

// HChaCha20: the XChaCha20 key-derivation step.  Same rounds as ChaCha20, but
// the output is the first and last four words of the *unfed-forward* state.
void hchacha20(const uint8_t key[32], const uint8_t nonce16[16], uint8_t out[32]) {
    uint32_t x[16];
    x[0] = 0x61707865; x[1] = 0x3320646e; x[2] = 0x79622d32; x[3] = 0x6b206574;
    for (int i = 0; i < 8; ++i) x[4 + i] = load32_le(key + i * 4);
    for (int i = 0; i < 4; ++i) x[12 + i] = load32_le(nonce16 + i * 4);

    auto quarter = [&](int a, int b, int c, int d) {
        x[a] += x[b]; x[d] = rotl32(x[d] ^ x[a], 16);
        x[c] += x[d]; x[b] = rotl32(x[b] ^ x[c], 12);
        x[a] += x[b]; x[d] = rotl32(x[d] ^ x[a], 8);
        x[c] += x[d]; x[b] = rotl32(x[b] ^ x[c], 7);
    };
    for (int i = 0; i < 10; ++i) {
        quarter(0, 4, 8, 12);  quarter(1, 5, 9, 13);
        quarter(2, 6, 10, 14); quarter(3, 7, 11, 15);
        quarter(0, 5, 10, 15); quarter(1, 6, 11, 12);
        quarter(2, 7, 8, 13);  quarter(3, 4, 9, 14);
    }
    for (int i = 0; i < 4; ++i) store32_le(out + i * 4, x[i]);
    for (int i = 0; i < 4; ++i) store32_le(out + 16 + i * 4, x[12 + i]);
}

// Poly1305 one-shot (RFC 8439 §2.5) over 130-bit arithmetic in 26-bit limbs.
class Poly1305 {
public:
    Poly1305(const uint8_t key[32]) {
        r_[0] = (load32_le(key + 0)) & 0x3ffffff;
        r_[1] = (load32_le(key + 3) >> 2) & 0x3ffff03;
        r_[2] = (load32_le(key + 6) >> 4) & 0x3ffc0ff;
        r_[3] = (load32_le(key + 9) >> 6) & 0x3f03fff;
        r_[4] = (load32_le(key + 12) >> 8) & 0x00fffff;
        for (int i = 0; i < 4; ++i) pad_[i] = load32_le(key + 16 + i * 4);
    }

    void update(const uint8_t* m, size_t len) {
        while (len > 0) {
            const size_t take = (len < 16 - buflen_) ? len : 16 - buflen_;
            std::memcpy(buf_ + buflen_, m, take);
            buflen_ += take;
            m += take;
            len -= take;
            if (buflen_ == 16) {
                block(buf_, /*final=*/false);
                buflen_ = 0;
            }
        }
    }

    void finish(uint8_t tag[16]) {
        if (buflen_ > 0) {
            buf_[buflen_++] = 1;
            while (buflen_ < 16) buf_[buflen_++] = 0;
            block(buf_, /*final=*/true);
        }
        carry();

        // Compute h + -p and pick it if there was no borrow (i.e. h >= p).
        uint32_t g[5];
        uint32_t c = 5;
        for (int i = 0; i < 5; ++i) {
            c += h_[i];
            g[i] = c & 0x3ffffff;
            c >>= 26;
        }
        g[4] -= (1u << 26);
        const uint32_t mask_g = (c ^ 1) - 1;  // all-ones when c == 1
        const uint32_t mask_h = ~mask_g;
        for (int i = 0; i < 5; ++i) h_[i] = (h_[i] & mask_h) | (g[i] & mask_g);

        uint32_t f0 = (h_[0] | (h_[1] << 26)) & 0xffffffff;
        uint32_t f1 = ((h_[1] >> 6) | (h_[2] << 20)) & 0xffffffff;
        uint32_t f2 = ((h_[2] >> 12) | (h_[3] << 14)) & 0xffffffff;
        uint32_t f3 = ((h_[3] >> 18) | (h_[4] << 8)) & 0xffffffff;

        uint64_t t = static_cast<uint64_t>(f0) + pad_[0];
        store32_le(tag + 0, static_cast<uint32_t>(t));
        t = static_cast<uint64_t>(f1) + pad_[1] + (t >> 32);
        store32_le(tag + 4, static_cast<uint32_t>(t));
        t = static_cast<uint64_t>(f2) + pad_[2] + (t >> 32);
        store32_le(tag + 8, static_cast<uint32_t>(t));
        t = static_cast<uint64_t>(f3) + pad_[3] + (t >> 32);
        store32_le(tag + 12, static_cast<uint32_t>(t));
    }

private:
    void block(const uint8_t m[16], bool final_block) {
        const uint32_t hibit = final_block ? 0 : (1u << 24);
        h_[0] += (load32_le(m + 0)) & 0x3ffffff;
        h_[1] += (load32_le(m + 3) >> 2) & 0x3ffffff;
        h_[2] += (load32_le(m + 6) >> 4) & 0x3ffffff;
        h_[3] += (load32_le(m + 9) >> 6) & 0x3ffffff;
        h_[4] += (load32_le(m + 12) >> 8) | hibit;

        uint64_t d[5];
        const uint32_t s1 = r_[1] * 5, s2 = r_[2] * 5, s3 = r_[3] * 5, s4 = r_[4] * 5;
        d[0] = static_cast<uint64_t>(h_[0]) * r_[0] + static_cast<uint64_t>(h_[1]) * s4 +
               static_cast<uint64_t>(h_[2]) * s3 + static_cast<uint64_t>(h_[3]) * s2 +
               static_cast<uint64_t>(h_[4]) * s1;
        d[1] = static_cast<uint64_t>(h_[0]) * r_[1] + static_cast<uint64_t>(h_[1]) * r_[0] +
               static_cast<uint64_t>(h_[2]) * s4 + static_cast<uint64_t>(h_[3]) * s3 +
               static_cast<uint64_t>(h_[4]) * s2;
        d[2] = static_cast<uint64_t>(h_[0]) * r_[2] + static_cast<uint64_t>(h_[1]) * r_[1] +
               static_cast<uint64_t>(h_[2]) * r_[0] + static_cast<uint64_t>(h_[3]) * s4 +
               static_cast<uint64_t>(h_[4]) * s3;
        d[3] = static_cast<uint64_t>(h_[0]) * r_[3] + static_cast<uint64_t>(h_[1]) * r_[2] +
               static_cast<uint64_t>(h_[2]) * r_[1] + static_cast<uint64_t>(h_[3]) * r_[0] +
               static_cast<uint64_t>(h_[4]) * s4;
        d[4] = static_cast<uint64_t>(h_[0]) * r_[4] + static_cast<uint64_t>(h_[1]) * r_[3] +
               static_cast<uint64_t>(h_[2]) * r_[2] + static_cast<uint64_t>(h_[3]) * r_[1] +
               static_cast<uint64_t>(h_[4]) * r_[0];

        uint64_t c = 0;
        for (int i = 0; i < 5; ++i) {
            d[i] += c;
            h_[i] = static_cast<uint32_t>(d[i]) & 0x3ffffff;
            c = d[i] >> 26;
        }
        h_[0] += static_cast<uint32_t>(c) * 5;
        h_[1] += h_[0] >> 26;
        h_[0] &= 0x3ffffff;
    }

    void carry() {
        uint32_t c = h_[1] >> 26;
        h_[1] &= 0x3ffffff;
        h_[2] += c; c = h_[2] >> 26; h_[2] &= 0x3ffffff;
        h_[3] += c; c = h_[3] >> 26; h_[3] &= 0x3ffffff;
        h_[4] += c; c = h_[4] >> 26; h_[4] &= 0x3ffffff;
        h_[0] += c * 5; c = h_[0] >> 26; h_[0] &= 0x3ffffff;
        h_[1] += c;
    }

    uint32_t r_[5]{};
    uint32_t h_[5]{};
    uint32_t pad_[4]{};
    uint8_t buf_[16]{};
    size_t buflen_ = 0;
};

// The RFC 8439 MAC input: aad ‖ pad16 ‖ ciphertext ‖ pad16 ‖ len(aad) ‖ len(ct).
void poly1305_aead_tag(const uint8_t poly_key[32], const std::string& aad,
                       const uint8_t* ct, size_t ct_len, uint8_t tag[16]) {
    static const uint8_t zeros[16] = {};
    Poly1305 poly(poly_key);
    poly.update(reinterpret_cast<const uint8_t*>(aad.data()), aad.size());
    if (aad.size() % 16 != 0) poly.update(zeros, 16 - (aad.size() % 16));
    poly.update(ct, ct_len);
    if (ct_len % 16 != 0) poly.update(zeros, 16 - (ct_len % 16));

    uint8_t lengths[16];
    uint64_t a = aad.size(), c = ct_len;
    for (int i = 0; i < 8; ++i) lengths[i] = static_cast<uint8_t>(a >> (i * 8));
    for (int i = 0; i < 8; ++i) lengths[8 + i] = static_cast<uint8_t>(c >> (i * 8));
    poly.update(lengths, 16);
    poly.finish(tag);
}

// Split a 24-byte XChaCha nonce into the derived subkey and the 12-byte
// ChaCha20 nonce (four zero bytes followed by the trailing eight).
void xchacha_derive(const uint8_t key[32], const uint8_t nonce24[24],
                    uint8_t subkey[32], uint8_t nonce12[12]) {
    hchacha20(key, nonce24, subkey);
    std::memset(nonce12, 0, 4);
    std::memcpy(nonce12 + 4, nonce24 + 16, 8);
}

}  // namespace

std::string aead_seal(const std::array<uint8_t, kAeadKeyBytes>& key,
                      const std::string& plaintext, const std::string& aad) {
    auto nonce = random_bytes(kAeadNonceBytes);

    uint8_t subkey[32], nonce12[12];
    xchacha_derive(key.data(), nonce.data(), subkey, nonce12);

    // Counter 0 produces the one-time Poly1305 key; the payload starts at 1.
    uint8_t poly_key[64] = {};
    chacha20_xor(subkey, nonce12, 0, poly_key, sizeof(poly_key));

    std::string ct = plaintext;
    if (!ct.empty()) {
        chacha20_xor(subkey, nonce12, 1, reinterpret_cast<uint8_t*>(ct.data()), ct.size());
    }

    uint8_t tag[kAeadTagBytes];
    poly1305_aead_tag(poly_key, aad, reinterpret_cast<const uint8_t*>(ct.data()), ct.size(),
                      tag);

    std::string out;
    out.reserve(kAeadNonceBytes + ct.size() + kAeadTagBytes);
    out.append(reinterpret_cast<const char*>(nonce.data()), kAeadNonceBytes);
    out.append(ct);
    out.append(reinterpret_cast<const char*>(tag), kAeadTagBytes);
    return out;
}

std::optional<std::string> aead_open(const std::array<uint8_t, kAeadKeyBytes>& key,
                                     const std::string& sealed, const std::string& aad) {
    if (sealed.size() < kAeadNonceBytes + kAeadTagBytes) return std::nullopt;

    const auto* raw = reinterpret_cast<const uint8_t*>(sealed.data());
    const size_t ct_len = sealed.size() - kAeadNonceBytes - kAeadTagBytes;
    const uint8_t* ct = raw + kAeadNonceBytes;
    const uint8_t* tag = ct + ct_len;

    uint8_t subkey[32], nonce12[12];
    xchacha_derive(key.data(), raw, subkey, nonce12);

    uint8_t poly_key[64] = {};
    chacha20_xor(subkey, nonce12, 0, poly_key, sizeof(poly_key));

    uint8_t expected[kAeadTagBytes];
    poly1305_aead_tag(poly_key, aad, ct, ct_len, expected);

    // Verify before decrypting: releasing unauthenticated plaintext, even
    // internally, is the mistake the whole construction exists to prevent.
    if (!constant_time_equal(std::string(reinterpret_cast<const char*>(expected), kAeadTagBytes),
                             std::string(reinterpret_cast<const char*>(tag), kAeadTagBytes))) {
        return std::nullopt;
    }

    std::string pt(reinterpret_cast<const char*>(ct), ct_len);
    if (!pt.empty()) {
        chacha20_xor(subkey, nonce12, 1, reinterpret_cast<uint8_t*>(pt.data()), pt.size());
    }
    return pt;
}

// ---------------------------------------------------------------------------
// Encodings
// ---------------------------------------------------------------------------

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
        const uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                           (static_cast<uint32_t>(data[i + 1]) << 8);
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
    if (a.size() != b.size()) return false;
    uint8_t diff = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        diff |= static_cast<uint8_t>(a[i] ^ b[i]);
    }
    return diff == 0;
}

// ---------------------------------------------------------------------------
// Randomness
// ---------------------------------------------------------------------------

std::vector<uint8_t> random_bytes(size_t n) {
    std::vector<uint8_t> out(n);
    if (n == 0) return out;
#ifdef _WIN32
    if (BCryptGenRandom(nullptr, out.data(), static_cast<ULONG>(n),
                        BCRYPT_USE_SYSTEM_PREFERRED_RNG) != 0) {
        throw std::runtime_error("vgi_rpc: BCryptGenRandom failed");
    }
#else
    int fd = ::open("/dev/urandom", O_RDONLY);
    if (fd < 0) throw std::runtime_error("vgi_rpc: cannot open /dev/urandom");
    size_t got = 0;
    while (got < n) {
        const auto r = ::read(fd, out.data() + got, n - got);
        if (r < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            throw std::runtime_error("vgi_rpc: read from /dev/urandom failed");
        }
        if (r == 0) {
            ::close(fd);
            throw std::runtime_error("vgi_rpc: /dev/urandom returned EOF");
        }
        got += static_cast<size_t>(r);
    }
    ::close(fd);
#endif
    return out;
}

std::array<uint8_t, kAeadKeyBytes> random_key() {
    auto bytes = random_bytes(kAeadKeyBytes);
    std::array<uint8_t, kAeadKeyBytes> key{};
    std::memcpy(key.data(), bytes.data(), kAeadKeyBytes);
    return key;
}

}  // namespace vgi_rpc::crypto
