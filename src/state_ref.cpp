// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/state_ref.h"

#include "vgi_rpc/external.h"
#include "vgi_rpc/object_cache.h"
#include "state_ref_internal.h"

#include <zstd.h>

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace vgi_rpc {

// ---------------------------------------------------------------------------
// Token codec
// ---------------------------------------------------------------------------

namespace token_codec {

std::string encode(std::string_view plaintext) {
    std::string out;
    const size_t bound = ZSTD_compressBound(plaintext.size());
    std::string compressed(bound, '\0');
    const size_t written =
        ZSTD_compress(compressed.data(), bound, plaintext.data(), plaintext.size(), 3);
    if (!ZSTD_isError(written) && written < plaintext.size()) {
        out.reserve(1 + written);
        out.push_back(static_cast<char>(ZSTD));
        out.append(compressed.data(), written);
        return out;
    }
    out.reserve(1 + plaintext.size());
    out.push_back(static_cast<char>(RAW));
    out.append(plaintext.data(), plaintext.size());
    return out;
}

std::string decode(std::string_view payload, int64_t max_bytes) {
    if (payload.empty()) throw std::invalid_argument("token payload is empty");
    const auto codec = static_cast<uint8_t>(payload.front());
    const std::string_view body = payload.substr(1);
    if (codec == RAW) {
        if (static_cast<int64_t>(body.size()) > max_bytes) {
            throw std::invalid_argument("token payload exceeds its decompression bound");
        }
        return std::string(body);
    }
    if (codec != ZSTD) throw std::invalid_argument("unknown token codec");

    // A frame that declares its size is checked before anything is inflated.
    const unsigned long long declared = ZSTD_getFrameContentSize(body.data(), body.size());
    if (declared == ZSTD_CONTENTSIZE_ERROR) throw std::invalid_argument("invalid zstd token body");
    if (declared != ZSTD_CONTENTSIZE_UNKNOWN &&
        declared > static_cast<unsigned long long>(max_bytes)) {
        throw std::invalid_argument("token payload exceeds its decompression bound");
    }
    using DStream = std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)>;
    DStream stream(ZSTD_createDStream(), &ZSTD_freeDStream);
    if (!stream || ZSTD_isError(ZSTD_initDStream(stream.get()))) {
        throw std::runtime_error("zstd decoder initialization failed");
    }
    ZSTD_inBuffer input{body.data(), body.size(), 0};
    std::string decoded;
    if (declared != ZSTD_CONTENTSIZE_UNKNOWN) decoded.reserve(static_cast<size_t>(declared));
    std::string chunk(64 * 1024, '\0');
    size_t remaining = 1;
    while (input.pos < input.size || remaining != 0) {
        ZSTD_outBuffer output{chunk.data(), chunk.size(), 0};
        remaining = ZSTD_decompressStream(stream.get(), &output, &input);
        if (ZSTD_isError(remaining)) throw std::invalid_argument("invalid zstd token body");
        if (static_cast<int64_t>(decoded.size() + output.pos) > max_bytes) {
            throw std::invalid_argument("token payload exceeds its decompression bound");
        }
        decoded.append(chunk.data(), output.pos);
        if (input.pos == input.size && remaining != 0 && output.pos == 0) {
            throw std::invalid_argument("truncated zstd token body");
        }
    }
    return decoded;
}

}  // namespace token_codec

// ---------------------------------------------------------------------------
// StateRef
// ---------------------------------------------------------------------------

namespace {

constexpr uint8_t kFormInline = 0x01;
constexpr uint8_t kFormStored = 0x02;
constexpr uint8_t kSealVersion = 0x01;
constexpr uint8_t kArgRefVersion = 0x01;
constexpr size_t kMaxObjectIdBytes = 1024;

}  // namespace

const char* StateRef::form() const noexcept {
    if (!impl_) return "memory";
    return impl_->decoded ? "inline" : "memory";
}

std::string StateRef::encode() const {
    if (!impl_) throw InvalidStateRefError("cannot encode an empty StateRef");
    std::string out(1, static_cast<char>(kFormInline));
    out += token_codec::encode(*impl_->bytes);
    return out;
}

StateRef StateRef::decode(std::string_view encoded, int64_t max_value_bytes) {
    if (encoded.empty()) throw InvalidStateRefError("StateRef is empty");
    const auto form = static_cast<uint8_t>(encoded.front());
    if (form == kFormStored) {
        throw InvalidStateRefError(
            "stored StateRefs are not supported by this single-replica server (STATE_REF_V1.md "
            "§5.4)");
    }
    if (form != kFormInline) throw InvalidStateRefError("unknown StateRef form");
    auto impl = std::make_shared<Impl>();
    try {
        impl->bytes = std::make_shared<const std::string>(token_codec::decode(
            encoded.substr(1), std::max(token_codec::MAX_TOKEN_BYTES, max_value_bytes)));
    } catch (const std::invalid_argument& e) {
        throw InvalidStateRefError(std::string("invalid inline StateRef: ") + e.what());
    }
    impl->decoded = true;
    return StateRef(std::move(impl));
}

// ---------------------------------------------------------------------------
// AAD and argument references
// ---------------------------------------------------------------------------

const std::string& server_scope() {
    static const std::string scope("\0server", 7);
    return scope;
}

std::string base_aad(const AuthContext& auth, const std::string& protocol) {
    std::string binding;
    if (auto it = auth.claims.find("peer_evidence_binding");
        it != auth.claims.end() && it->is_string()) {
        binding = it->get<std::string>();
    }
    std::string aad = binding.empty() ? std::string("vgi_rpc.state.v6\0", 17)
                                      : std::string("vgi_rpc.state.v7\0", 17);
    if (!auth.authenticated) {
        aad += std::string("\0anonymous", 10);
    } else {
        aad.push_back('\x01');
        aad += auth.domain;
        aad.push_back('\0');
        aad += auth.principal.value_or("");
        if (!binding.empty()) {
            aad.push_back('\0');
            aad += binding;
        }
    }
    aad.push_back('\0');
    aad += protocol;
    return aad;
}

std::string argument_ref_aad(const AuthContext& auth) {
    return std::string("vgi_rpc.argref.v1\0", 18) + base_aad(auth, server_scope());
}

namespace {

bool is_lower_hex_sha256(const std::string& value) {
    return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

void put_le(std::string& out, uint64_t value, int bytes) {
    for (int i = 0; i < bytes; ++i) out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

uint64_t get_le(const std::string& in, size_t offset, int bytes) {
    uint64_t value = 0;
    for (int i = 0; i < bytes; ++i) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(in[offset + i])) << (8 * i);
    }
    return value;
}

}  // namespace

std::string mint_argument_ref(const std::array<uint8_t, crypto::kAeadKeyBytes>& key,
                              const AuthContext& auth, int64_t size, const std::string& sha256_hex,
                              const std::string& object_id) {
    if (size < 1) throw std::invalid_argument("argument ref size must be an integer >= 1");
    if (!is_lower_hex_sha256(sha256_hex)) {
        throw std::invalid_argument("argument ref sha256 must be 64 lowercase hex characters");
    }
    if (object_id.empty() || object_id.size() > kMaxObjectIdBytes) {
        throw std::invalid_argument("argument ref object_id must be 1-1024 bytes");
    }
    const auto digest = crypto::hex_decode(sha256_hex);
    std::string plaintext(1, static_cast<char>(kArgRefVersion));
    put_le(plaintext, static_cast<uint64_t>(size), 8);
    plaintext.append(reinterpret_cast<const char*>(digest->data()), digest->size());
    put_le(plaintext, object_id.size(), 2);
    plaintext += object_id;
    std::string sealed(1, static_cast<char>(kSealVersion));
    sealed += crypto::aead_seal(key, plaintext, argument_ref_aad(auth));
    return sealed;
}

ArgumentRefClaims open_argument_ref(const std::array<uint8_t, crypto::kAeadKeyBytes>& key,
                                    const AuthContext& auth, const std::string& ref) {
    const InvalidArgumentsRefError refused("argument reference is invalid for this caller");
    if (ref.empty() || static_cast<uint8_t>(ref.front()) != kSealVersion) throw refused;
    const auto opened = crypto::aead_open(key, ref.substr(1), argument_ref_aad(auth));
    if (!opened) throw refused;
    const std::string& plain = *opened;
    constexpr size_t kFixed = 1 + 8 + 32 + 2;
    if (plain.size() < kFixed || static_cast<uint8_t>(plain[0]) != kArgRefVersion) throw refused;
    const auto id_len = static_cast<size_t>(get_le(plain, 41, 2));
    if (id_len == 0 || id_len > kMaxObjectIdBytes || plain.size() != kFixed + id_len) {
        throw refused;
    }
    ArgumentRefClaims claims;
    claims.size = static_cast<int64_t>(get_le(plain, 1, 8));
    claims.sha256_hex = crypto::hex_encode(reinterpret_cast<const uint8_t*>(plain.data() + 9), 32);
    claims.object_id = plain.substr(kFixed);
    return claims;
}

std::shared_ptr<const std::string> resolve_argument_ref(
    const std::string& ref, int64_t size, const std::string& sha256_hex,
    const std::array<uint8_t, crypto::kAeadKeyBytes>& key, const AuthContext& auth,
    ObjectStore* store, int64_t max_fetch_bytes, ObjectCache& cache) {
    if (size < 1) {
        throw InvalidArgumentsRefError("argument location size must be an integer >= 1, got " +
                                       std::to_string(size));
    }
    if (!is_lower_hex_sha256(sha256_hex)) {
        throw InvalidArgumentsRefError(
            "argument location sha256 must be 64 lowercase hex characters");
    }
    if (store == nullptr) {
        throw InvalidArgumentsRefError(
            "this server has no object storage configured to resolve argument references");
    }
    const ArgumentRefClaims claims = open_argument_ref(key, auth, ref);
    if (claims.size != size || claims.sha256_hex != sha256_hex) {
        throw InvalidArgumentsRefError(
            "argument reference does not match the size and sha256 it was sent with");
    }
    if (max_fetch_bytes >= 0 && size > max_fetch_bytes) {
        throw InvalidArgumentsRefError("argument object is " + std::to_string(size) +
                                       " bytes, over this server's max_fetch_bytes (" +
                                       std::to_string(max_fetch_bytes) + ")");
    }
    const auto expected = crypto::hex_decode(sha256_hex);
    auto fetch = [&]() -> std::string {
        std::string data;
        try {
            data = store->get(claims.object_id, max_fetch_bytes);
        } catch (const ObjectMissingError&) {
            throw ArgumentsRefMissingError();
        } catch (const KindedError&) {
            throw;
        } catch (const std::exception& e) {
            // Never the URL or the id: the backends redact, and this adds none.
            throw StorageUnavailableError(std::string("argument object fetch failed: ") + e.what());
        }
        if (static_cast<int64_t>(data.size()) != size) {
            throw ArgumentsRefIntegrityError("argument object size mismatch: expected " +
                                             std::to_string(size) + ", got " +
                                             std::to_string(data.size()));
        }
        const auto digest =
            crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
        if (!std::equal(digest.begin(), digest.end(), expected->begin())) {
            throw ArgumentsRefIntegrityError("argument object sha256 mismatch");
        }
        return data;
    };
    auto [value, hit] = cache.get_or_fetch(claims.object_id, fetch);
    if (static_cast<int64_t>(value->size()) != size) {
        // A cached value passed the checks above for this id; a different
        // length means the id was reused, which storage never does.
        throw ArgumentsRefIntegrityError("argument object size mismatch: expected " +
                                         std::to_string(size) + ", got " +
                                         std::to_string(value->size()));
    }
    (void)hit;
    return value;
}

}  // namespace vgi_rpc
