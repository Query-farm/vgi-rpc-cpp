// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Test vectors, not behaviour reimplemented in the test.  Every primitive here
// has a published answer, and a hand-written crypto implementation that agrees
// with itself is worth nothing.

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/crypto.h"

using namespace vgi_rpc::crypto;

namespace {
std::string bytes(const std::vector<uint8_t>& v) {
    return std::string(reinterpret_cast<const char*>(v.data()), v.size());
}
}  // namespace

TEST_CASE("sha256: FIPS 180-4 vectors", "[crypto]") {
    Sha256 empty;
    REQUIRE(empty.hex_digest() ==
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    Sha256 abc;
    abc.update(std::string("abc"));
    REQUIRE(abc.hex_digest() ==
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST_CASE("hmac_sha256: RFC 4231 vectors", "[crypto]") {
    // Case 1: 20 bytes of 0x0b, "Hi There".
    std::string key(20, '\x0b');
    auto mac = hmac_sha256(key, "Hi There");
    REQUIRE(hex_encode(mac.data(), mac.size()) ==
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");

    // Case 2: short key, "what do ya want for nothing?".
    auto mac2 = hmac_sha256("Jefe", "what do ya want for nothing?");
    REQUIRE(hex_encode(mac2.data(), mac2.size()) ==
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");

    // Case 6: key longer than the 64-byte block, so it is hashed first.
    std::string long_key(131, '\xaa');
    auto mac3 = hmac_sha256(long_key, "Test Using Larger Than Block-Size Key - Hash Key First");
    REQUIRE(hex_encode(mac3.data(), mac3.size()) ==
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

TEST_CASE("base64url: RFC 4648 vectors, unpadded", "[crypto]") {
    REQUIRE(base64url_encode(std::string("")) == "");
    REQUIRE(base64url_encode(std::string("f")) == "Zg");
    REQUIRE(base64url_encode(std::string("fo")) == "Zm8");
    REQUIRE(base64url_encode(std::string("foo")) == "Zm9v");
    REQUIRE(base64url_encode(std::string("foob")) == "Zm9vYg");
    REQUIRE(base64url_encode(std::string("fooba")) == "Zm9vYmE");
    REQUIRE(base64url_encode(std::string("foobar")) == "Zm9vYmFy");

    // The url alphabet: 0xfb 0xff encodes to '-' and '_', never '+' and '/'.
    const uint8_t high[] = {0xfb, 0xff, 0xbf};
    REQUIRE(base64url_encode(high, sizeof(high)) == "-_-_");

    for (const char* s : {"", "f", "fo", "foo", "foob", "fooba", "foobar"}) {
        auto round = base64url_decode(base64url_encode(std::string(s)));
        REQUIRE(round.has_value());
        REQUIRE(*round == std::string(s));
    }

    // Padding is tolerated on decode, junk is not.
    REQUIRE(base64url_decode("Zm9vYg==").value() == "foob");
    REQUIRE_FALSE(base64url_decode("Zm9v*g").has_value());
    REQUIRE_FALSE(base64url_decode("Zm9vY").has_value());  // 4n+1 is impossible
}

TEST_CASE("aead: seals and opens, and refuses anything altered", "[crypto]") {
    auto key = random_key();
    const std::string pt = "the quick brown fox";
    const std::string aad = "vgi_rpc.state.v4\x00binding";

    auto sealed = aead_seal(key, pt, aad);
    REQUIRE(sealed.size() == kAeadNonceBytes + pt.size() + kAeadTagBytes);
    REQUIRE(sealed.find(pt) == std::string::npos);  // not plaintext on the wire

    auto opened = aead_open(key, sealed, aad);
    REQUIRE(opened.has_value());
    REQUIRE(*opened == pt);

    SECTION("a different key does not open it") {
        REQUIRE_FALSE(aead_open(random_key(), sealed, aad).has_value());
    }
    SECTION("a different AAD does not open it") {
        REQUIRE_FALSE(aead_open(key, sealed, aad + "x").has_value());
    }
    SECTION("a flipped ciphertext bit does not open it") {
        auto tampered = sealed;
        tampered[kAeadNonceBytes] = static_cast<char>(tampered[kAeadNonceBytes] ^ 0x01);
        REQUIRE_FALSE(aead_open(key, tampered, aad).has_value());
    }
    SECTION("a flipped tag bit does not open it") {
        auto tampered = sealed;
        tampered.back() = static_cast<char>(tampered.back() ^ 0x80);
        REQUIRE_FALSE(aead_open(key, tampered, aad).has_value());
    }
    SECTION("a flipped nonce bit does not open it") {
        auto tampered = sealed;
        tampered[0] = static_cast<char>(tampered[0] ^ 0x01);
        REQUIRE_FALSE(aead_open(key, tampered, aad).has_value());
    }
    SECTION("a truncated envelope does not open it") {
        REQUIRE_FALSE(aead_open(key, sealed.substr(0, sealed.size() - 1), aad).has_value());
        REQUIRE_FALSE(aead_open(key, "", aad).has_value());
        REQUIRE_FALSE(aead_open(key, std::string(kAeadNonceBytes, 'x'), aad).has_value());
    }
}

TEST_CASE("aead: opens an envelope sealed by an independent implementation", "[crypto]") {
    // The round-trip tests above prove only self-consistency, which any
    // reversible transform satisfies.  This envelope was produced by Python's
    // `cryptography` package — HChaCha20 subkey derivation followed by
    // ChaCha20-Poly1305 — so opening it is what establishes that this really
    // is XChaCha20-Poly1305 rather than a private variant of it.
    const std::array<uint8_t, kAeadKeyBytes> key = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
        0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
        0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f};
    const uint8_t envelope[] = {
        0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70,
        0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x0a, 0x14,
        0x90, 0x9d, 0x8d, 0x5e, 0xea, 0xa2, 0xe4, 0x42, 0xf2, 0x4c, 0xd5, 0x42, 0xde,
        0xa1, 0x3e, 0xd8, 0x39, 0x51, 0x94, 0x40, 0xab, 0xd9, 0xe2, 0x21, 0x01, 0xdf,
        0x5f, 0x72, 0x58, 0x63, 0xdb, 0x67, 0x2d, 0x10, 0x45, 0xd8, 0x90, 0xe7, 0x9a,
        0x67, 0x16, 0x68, 0x74, 0x39, 0x66, 0x34, 0x4e, 0xd3, 0xe4, 0x32, 0xfd, 0xcb,
        0x40, 0xc3, 0xef, 0x6d, 0x67, 0x27, 0x98, 0xe3, 0x9e, 0x0e, 0x39, 0xa6, 0xaa,
        0x81, 0xb1, 0x1c, 0xdd, 0x03, 0x36, 0xde, 0xe1, 0xd1, 0xd9, 0xa7, 0xdd, 0x88,
        0x7e, 0x0a, 0xe7, 0xda, 0x45, 0x11, 0x1b, 0x40};
    const std::string aad = std::string("vgi_rpc.state.v4", 16) +
                            std::string("\x00\x01""domain\x00""principal", 18);
    const std::string expected =
        "vgi-rpc cross-check plaintext, spanning past one 64-byte block boundary.";

    auto opened = aead_open(
        key, std::string(reinterpret_cast<const char*>(envelope), sizeof(envelope)), aad);
    REQUIRE(opened.has_value());
    REQUIRE(*opened == expected);
}

TEST_CASE("aead: nonces do not repeat, so the same plaintext seals differently", "[crypto]") {
    auto key = random_key();
    auto a = aead_seal(key, "same", "same");
    auto b = aead_seal(key, "same", "same");
    REQUIRE(a != b);
    REQUIRE(aead_open(key, a, "same").value() == "same");
    REQUIRE(aead_open(key, b, "same").value() == "same");
}

TEST_CASE("aead: empty plaintext and empty aad round-trip", "[crypto]") {
    auto key = random_key();
    auto sealed = aead_seal(key, "", "");
    auto opened = aead_open(key, sealed, "");
    REQUIRE(opened.has_value());
    REQUIRE(opened->empty());
}

TEST_CASE("aead: payloads spanning several ChaCha20 blocks round-trip", "[crypto]") {
    // 64 bytes is one block and 16 is one Poly1305 block; sizes either side of
    // both boundaries catch an off-by-one in the counter or the padding.
    auto key = random_key();
    for (size_t n : {1u, 15u, 16u, 17u, 63u, 64u, 65u, 128u, 1000u}) {
        const std::string pt(n, 'z');
        auto sealed = aead_seal(key, pt, "aad");
        auto opened = aead_open(key, sealed, "aad");
        REQUIRE(opened.has_value());
        REQUIRE(*opened == pt);
    }
}

TEST_CASE("hex: round-trips and rejects non-hex", "[crypto]") {
    const uint8_t raw[] = {0x00, 0x0f, 0xff, 0xa5};
    REQUIRE(hex_encode(raw, sizeof(raw)) == "000fffa5");
    REQUIRE(bytes(hex_decode("000fffa5").value()) == std::string(reinterpret_cast<const char*>(raw), 4));
    REQUIRE(hex_decode("00FFAA").has_value());  // uppercase accepted
    REQUIRE_FALSE(hex_decode("abc").has_value());   // odd length
    REQUIRE_FALSE(hex_decode("zz").has_value());    // non-hex
}

TEST_CASE("constant_time_equal: agrees with ==", "[crypto]") {
    REQUIRE(constant_time_equal("abc", "abc"));
    REQUIRE_FALSE(constant_time_equal("abc", "abd"));
    REQUIRE_FALSE(constant_time_equal("abc", "ab"));
    REQUIRE(constant_time_equal("", ""));
}

TEST_CASE("random_bytes: right length, and not obviously stuck", "[crypto]") {
    REQUIRE(random_bytes(0).empty());
    REQUIRE(random_bytes(32).size() == 32);
    REQUIRE(random_bytes(32) != random_bytes(32));
}
