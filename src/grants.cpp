// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/grants.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <set>

#include "identity_provider_internal.h"

namespace vgi_rpc {

namespace {

constexpr size_t kKidBytes = 8;
constexpr uint8_t kEnvelopeVersion = 1;
constexpr size_t kMaxField = 0xFFFF;
// Both domain strings end in a NUL that is part of the preimage, so they are
// spelled with an explicit length rather than as C strings.
const std::string kKidDomain("vgi_rpc.grant.kid.v1\0", 21);
const std::string kAadDomain("vgi_rpc.grant.v1\0", 17);

std::string trim(const std::string& text) {
    const auto first = text.find_first_not_of(" \t\r\n\f\v");
    if (first == std::string::npos) return "";
    const auto last = text.find_last_not_of(" \t\r\n\f\v");
    return text.substr(first, last - first + 1);
}

int standard_b64_value(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

/// Standard (not url-safe) base64, padding optional.  Only the alphabet is
/// accepted: a key that decoded leniently would be a different key.
std::optional<std::string> decode_standard_base64(std::string text) {
    size_t padding = 0;
    while (!text.empty() && text.back() == '=') {
        text.pop_back();
        ++padding;
    }
    if (text.size() % 4 == 1 || padding > 2) return std::nullopt;
    if (padding != 0 && (text.size() + padding) % 4 != 0) return std::nullopt;
    std::string out;
    uint32_t acc = 0;
    int bits = 0;
    for (const char c : text) {
        const int v = standard_b64_value(c);
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

bool is_b64url_char(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' ||
           c == '_';
}

/// Decode unpadded base64url, refusing any non-canonical spelling.  Re-encoding
/// and comparing rejects non-zero trailing bits, so one grant has exactly one
/// spelling.
std::string b64url_strict(const std::string& text) {
    if (text.empty() || !std::all_of(text.begin(), text.end(), is_b64url_char) ||
        text.size() % 4 == 1) {
        throw GrantInvalidError("grant token is not unpadded base64url");
    }
    auto raw = crypto::base64url_decode(text);
    if (!raw) throw GrantInvalidError("grant token is not unpadded base64url");
    if (crypto::base64url_encode(*raw) != text) {
        throw GrantInvalidError("grant token is not canonical base64url");
    }
    return *raw;
}

void put_u16(std::string& out, size_t value) {
    out.push_back(static_cast<char>(value & 0xFF));
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
}

void put_i64(std::string& out, int64_t value) {
    const auto bits = static_cast<uint64_t>(value);
    for (int shift = 0; shift < 64; shift += 8) {
        out.push_back(static_cast<char>((bits >> shift) & 0xFF));
    }
}

void put_text(std::string& out, const std::string& value) {
    if (value.size() > kMaxField) {
        throw std::invalid_argument("grant field longer than 65535 bytes");
    }
    put_u16(out, value.size());
    out += value;
}

class PayloadReader {
public:
    explicit PayloadReader(const std::string& payload) : payload_(payload) {}

    std::string take(size_t n) {
        if (n > payload_.size() - pos_) throw GrantInvalidError("grant payload is truncated");
        std::string chunk = payload_.substr(pos_, n);
        pos_ += n;
        return chunk;
    }

    size_t u16() {
        const std::string raw = take(2);
        return static_cast<size_t>(static_cast<uint8_t>(raw[0])) |
               (static_cast<size_t>(static_cast<uint8_t>(raw[1])) << 8);
    }

    int64_t i64() {
        const std::string raw = take(8);
        uint64_t bits = 0;
        for (int i = 7; i >= 0; --i) {
            bits = (bits << 8) | static_cast<uint8_t>(raw[static_cast<size_t>(i)]);
        }
        return static_cast<int64_t>(bits);
    }

    std::string text() {
        const size_t length = u16();
        std::string value = take(length);
        if (!identity_internal::valid_utf8(value)) {
            throw GrantInvalidError("grant payload is not UTF-8");
        }
        return value;
    }

    bool done() const noexcept { return pos_ == payload_.size(); }

private:
    const std::string& payload_;
    size_t pos_ = 0;
};

double wall_clock_seconds() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

std::string grant_key_id(const GrantKey& key) {
    crypto::Sha256 hash;
    hash.update(kKidDomain);
    hash.update(key.data(), key.size());
    const auto digest = hash.digest();
    return std::string(reinterpret_cast<const char*>(digest.data()), kKidBytes);
}

void GrantKeys::validate() const {
    if (keys.empty()) throw std::invalid_argument("grant configuration needs at least one key");
    std::set<std::string> ids;
    for (const auto& key : keys) ids.insert(grant_key_id(key));
    if (ids.size() != keys.size()) throw std::invalid_argument("grant keys must be distinct");
    if (max_ttl_seconds <= 0) throw std::invalid_argument("max_ttl_seconds must be positive");
    if (clock_skew_seconds < 0) {
        throw std::invalid_argument("clock_skew_seconds must not be negative");
    }
    if (audience.size() > kMaxField) throw std::invalid_argument("audience is too long");
}

GrantKeys GrantKeys::parse(const std::vector<std::string>& encoded_keys, std::string audience,
                           int64_t max_ttl_seconds, int64_t clock_skew_seconds) {
    GrantKeys config;
    for (size_t index = 0; index < encoded_keys.size(); ++index) {
        const auto key = decode_standard_base64(trim(encoded_keys[index]));
        const std::string ordinal = "grant key #" + std::to_string(index + 1);
        if (!key) throw std::invalid_argument(ordinal + " is not valid base64");
        if (key->size() != crypto::kAeadKeyBytes) {
            throw std::invalid_argument(ordinal + " decodes to " + std::to_string(key->size()) +
                                        " bytes; exactly 32 are required");
        }
        GrantKey bytes{};
        std::copy(key->begin(), key->end(), bytes.begin());
        config.keys.push_back(bytes);
    }
    config.audience = std::move(audience);
    config.max_ttl_seconds = max_ttl_seconds;
    config.clock_skew_seconds = clock_skew_seconds;
    config.validate();
    return config;
}

std::optional<GrantKeys> GrantKeys::from_env(
    const std::function<std::optional<std::string>(const char*)>& getenv) {
    const auto read = [&](const char* name) -> std::optional<std::string> {
        if (getenv) return getenv(name);
        const char* value = std::getenv(name);
        if (value == nullptr) return std::nullopt;
        return std::string(value);
    };
    const std::string raw = trim(read(kGrantKeysEnv).value_or(""));
    if (raw.empty()) return std::nullopt;

    const std::string ttl_raw = trim(read(kGrantMaxTtlEnv).value_or(""));
    int64_t max_ttl = kDefaultGrantMaxTtlSeconds;
    if (!ttl_raw.empty()) {
        size_t consumed = 0;
        try {
            max_ttl = std::stoll(ttl_raw, &consumed);
        } catch (const std::exception&) {
            consumed = 0;
        }
        if (consumed != ttl_raw.size()) {
            throw std::invalid_argument(std::string(kGrantMaxTtlEnv) + "='" + ttl_raw +
                                        "' is not an integer");
        }
    }

    std::vector<std::string> parts;
    size_t begin = 0;
    while (true) {
        const auto end = raw.find(',', begin);
        std::string part = raw.substr(begin, end == std::string::npos ? end : end - begin);
        if (!trim(part).empty()) parts.push_back(std::move(part));
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return parse(parts, read(kGrantAudienceEnv).value_or(""), max_ttl);
}

std::string GrantKeys::aad(const std::string& kid) const {
    return kAadDomain + kid + audience;
}

std::string encode_grant_payload(const GrantClaims& claims) {
    if (claims.scopes.size() > kMaxField) throw std::invalid_argument("too many scopes");
    std::string out;
    put_i64(out, claims.issued_at);
    put_i64(out, claims.expires_at);
    put_text(out, claims.grant_id);
    put_text(out, claims.principal);
    put_text(out, claims.purpose);
    put_u16(out, claims.scopes.size());
    for (const auto& scope : claims.scopes) put_text(out, scope);
    return out;
}

GrantClaims decode_grant_payload(const std::string& payload) {
    PayloadReader reader(payload);
    GrantClaims claims;
    // One statement per field: the read order *is* the format.
    claims.issued_at = reader.i64();
    claims.expires_at = reader.i64();
    claims.grant_id = reader.text();
    claims.principal = reader.text();
    claims.purpose = reader.text();
    const size_t count = reader.u16();
    for (size_t i = 0; i < count; ++i) claims.scopes.push_back(reader.text());
    if (!reader.done()) throw GrantInvalidError("grant payload has trailing bytes");
    return claims;
}

MintedGrantToken mint_grant_token(const GrantKeys& keys, const std::string& principal,
                                  const std::vector<std::string>& scopes,
                                  const std::string& purpose, int64_t ttl_seconds,
                                  const GrantMintOverrides& overrides) {
    if (ttl_seconds <= 0) throw std::invalid_argument("ttl_seconds must be positive");
    if (keys.keys.empty())
        throw std::invalid_argument("grant configuration needs at least one key");

    GrantClaims claims;
    claims.principal = principal;
    claims.scopes = scopes;
    claims.purpose = purpose;
    if (overrides.grant_id) {
        claims.grant_id = *overrides.grant_id;
    } else {
        const auto random = crypto::random_bytes(16);
        claims.grant_id = crypto::hex_encode(random.data(), random.size());
    }
    claims.issued_at =
        overrides.now
            ? *overrides.now
            : static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count());
    claims.expires_at = claims.issued_at + std::min(ttl_seconds, keys.max_ttl_seconds);

    const GrantKey& key = keys.keys.front();
    const std::string kid = grant_key_id(key);
    const std::string payload = encode_grant_payload(claims);
    const std::string aad = keys.aad(kid);
    std::string sealed = overrides.nonce
                             ? crypto::aead_seal_with_nonce(key, payload, aad, *overrides.nonce)
                             : crypto::aead_seal(key, payload, aad);
    std::string raw = kid;
    raw.push_back(static_cast<char>(kEnvelopeVersion));
    raw += sealed;
    MintedGrantToken minted;
    minted.token = std::string(kGrantTokenPrefix) + crypto::base64url_encode(raw);
    minted.claims = std::move(claims);
    return minted;
}

GrantClaims verify_grant_token(const GrantKeys& keys, const std::string& token,
                               std::optional<double> now) {
    const std::string prefix(kGrantTokenPrefix);
    if (token.compare(0, prefix.size(), prefix) != 0) {
        throw GrantInvalidError("not a sealed grant");
    }
    if (token.size() > kMaxGrantTokenChars) throw GrantInvalidError("grant token is too long");
    const std::string raw = b64url_strict(token.substr(prefix.size()));
    const std::string kid = raw.substr(0, std::min(raw.size(), kKidBytes));
    const std::string envelope = raw.size() > kKidBytes ? raw.substr(kKidBytes) : std::string();

    const GrantKey* key = nullptr;
    for (const auto& candidate : keys.keys) {
        if (grant_key_id(candidate) == kid) {
            key = &candidate;
            break;
        }
    }
    if (key == nullptr) {
        throw GrantInvalidError("grant was sealed with a key this deployment does not hold");
    }
    if (envelope.empty() || static_cast<uint8_t>(envelope[0]) != kEnvelopeVersion) {
        throw GrantInvalidError("grant failed verification");
    }
    const auto payload = crypto::aead_open(*key, envelope.substr(1), keys.aad(kid));
    if (!payload) throw GrantInvalidError("grant failed verification");

    GrantClaims claims = decode_grant_payload(*payload);
    if (claims.principal.empty()) throw GrantInvalidError("grant names no principal");
    // Subtraction only after the ordering check, so it cannot overflow.
    if (claims.expires_at <= claims.issued_at ||
        static_cast<uint64_t>(claims.expires_at) - static_cast<uint64_t>(claims.issued_at) >
            static_cast<uint64_t>(keys.max_ttl_seconds)) {
        throw GrantInvalidError("grant lifetime exceeds this deployment's maximum");
    }
    const double current = now ? *now : wall_clock_seconds();
    const auto skew = static_cast<double>(keys.clock_skew_seconds);
    if (static_cast<double>(claims.issued_at) > current + skew) {
        throw GrantInvalidError("grant is not yet valid", /*expired=*/true);
    }
    if (current >= static_cast<double>(claims.expires_at) + skew) {
        throw GrantInvalidError("grant has expired", /*expired=*/true);
    }
    return claims;
}

}  // namespace vgi_rpc
