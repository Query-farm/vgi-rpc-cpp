// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/describe.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/result.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace vgi_rpc {

namespace {

// ---------------------------------------------------------------------------
// Minimal self-contained SHA-256 (public-domain reference algorithm) used to
// compute vgi_rpc.protocol_hash without pulling in an external crypto library.
// ---------------------------------------------------------------------------
class Sha256 {
public:
    Sha256() { reset(); }

    void update(const uint8_t* data, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            buf_[buflen_++] = data[i];
            if (buflen_ == 64) {
                transform(buf_);
                bitlen_ += 512;
                buflen_ = 0;
            }
        }
    }
    void update(const std::string& s) {
        update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }
    void update_byte(uint8_t b) { update(&b, 1); }

    std::string hex_digest() {
        std::array<uint8_t, 32> digest{};
        finalize(digest.data());
        static constexpr char hex[] = "0123456789abcdef";
        std::string out(64, '0');
        for (size_t i = 0; i < 32; ++i) {
            out[i * 2] = hex[digest[i] >> 4];
            out[i * 2 + 1] = hex[digest[i] & 0x0F];
        }
        return out;
    }

private:
    uint32_t state_[8];
    uint64_t bitlen_;
    uint8_t buf_[64];
    size_t buflen_;

    static uint32_t rotr(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }

    void reset() {
        state_[0] = 0x6a09e667;
        state_[1] = 0xbb67ae85;
        state_[2] = 0x3c6ef372;
        state_[3] = 0xa54ff53a;
        state_[4] = 0x510e527f;
        state_[5] = 0x9b05688c;
        state_[6] = 0x1f83d9ab;
        state_[7] = 0x5be0cd19;
        bitlen_ = 0;
        buflen_ = 0;
    }

    void transform(const uint8_t* chunk) {
        static constexpr uint32_t k[64] = {
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2};

        uint32_t w[64];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(chunk[i * 4]) << 24) |
                   (static_cast<uint32_t>(chunk[i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(chunk[i * 4 + 2]) << 8) |
                   (static_cast<uint32_t>(chunk[i * 4 + 3]));
        }
        for (int i = 16; i < 64; ++i) {
            uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }

        uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
        uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
        for (int i = 0; i < 64; ++i) {
            uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            uint32_t ch = (e & f) ^ (~e & g);
            uint32_t t1 = h + S1 + ch + k[i] + w[i];
            uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            uint32_t t2 = S0 + maj;
            h = g;
            g = f;
            f = e;
            e = d + t1;
            d = c;
            c = b;
            b = a;
            a = t1 + t2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }

    void finalize(uint8_t* out) {
        uint64_t total_bits = bitlen_ + static_cast<uint64_t>(buflen_) * 8;
        // Append 0x80, pad with zeros to a 56-byte boundary, then the
        // 64-bit big-endian message length.
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
        // buflen_ is now 0 and the final block has been transformed.
        for (int i = 0; i < 8; ++i) {
            out[i * 4] = static_cast<uint8_t>(state_[i] >> 24);
            out[i * 4 + 1] = static_cast<uint8_t>(state_[i] >> 16);
            out[i * 4 + 2] = static_cast<uint8_t>(state_[i] >> 8);
            out[i * 4 + 3] = static_cast<uint8_t>(state_[i]);
        }
    }
};

// Introspection format version 4 schema (matches Python's _DESCRIBE_SCHEMA).
std::shared_ptr<arrow::Schema> describe_schema() {
    static auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("method_type", arrow::utf8()),
        arrow::field("has_return", arrow::boolean()),
        arrow::field("params_schema_ipc", arrow::binary()),
        arrow::field("result_schema_ipc", arrow::binary()),
        arrow::field("has_header", arrow::boolean()),
        arrow::field("header_schema_ipc", arrow::binary(), /*nullable=*/true),
        arrow::field("is_exchange", arrow::boolean(), /*nullable=*/true),
    });
    return schema;
}

std::shared_ptr<arrow::Buffer> serialize_schema(const std::shared_ptr<arrow::Schema>& schema) {
    return unwrap(arrow::ipc::SerializeSchema(*schema), "Failed to serialize schema");
}

}  // anonymous namespace

std::string compute_protocol_hash(const std::string& protocol_name,
                                  const std::unordered_map<std::string, MethodInfo>& methods) {
    std::vector<std::string> sorted_names;
    for (const auto& [name, _] : methods) sorted_names.push_back(name);
    std::sort(sorted_names.begin(), sorted_names.end());

    Sha256 hasher;
    hasher.update("vgi_rpc.describe.v");
    hasher.update(std::string(DESCRIBE_VERSION_VALUE));
    hasher.update_byte('|');
    hasher.update(std::string(REQUEST_VERSION_VALUE));
    hasher.update_byte('|');
    hasher.update(protocol_name);
    hasher.update_byte('|');

    for (const auto& method_name : sorted_names) {
        const auto& info = methods.at(method_name);
        const char* method_type_str = info.method_type == MethodType::UNARY ? "unary" : "stream";
        bool has_header = info.header_schema != nullptr;
        auto params_buf = serialize_schema(info.params_schema);
        auto result_schema = info.result_schema ? info.result_schema : empty_schema();
        auto result_buf = serialize_schema(result_schema);
        std::shared_ptr<arrow::Buffer> header_buf;
        if (has_header) header_buf = serialize_schema(info.header_schema);

        hasher.update_byte(0x1f);
        hasher.update(info.name);
        hasher.update_byte(0x1e);
        hasher.update(std::string(method_type_str));
        hasher.update_byte(0x1e);
        hasher.update_byte(info.has_return ? '1' : '0');
        hasher.update_byte(0x1e);
        hasher.update_byte(has_header ? '1' : '0');
        hasher.update_byte(0x1e);
        hasher.update_byte('-');  // is_exchange == None
        hasher.update_byte(0x1e);
        hasher.update(params_buf->data(), static_cast<size_t>(params_buf->size()));
        hasher.update_byte(0x1e);
        hasher.update(result_buf->data(), static_cast<size_t>(result_buf->size()));
        hasher.update_byte(0x1e);
        if (header_buf) {
            hasher.update(header_buf->data(), static_cast<size_t>(header_buf->size()));
        }
    }
    return hasher.hex_digest();
}

void register_describe(std::unordered_map<std::string, MethodInfo>& methods,
                       const std::string& protocol_name, const std::string& server_id,
                       const std::string& protocol_version) {
    // Collect methods sorted by name.  __describe__ is registered *after*
    // this call returns, so it is intentionally excluded from the response.
    std::vector<std::string> sorted_names;
    for (const auto& [name, _] : methods) {
        sorted_names.push_back(name);
    }
    std::sort(sorted_names.begin(), sorted_names.end());

    arrow::StringBuilder name_builder;
    arrow::StringBuilder method_type_builder;
    arrow::BooleanBuilder has_return_builder;
    arrow::BinaryBuilder params_schema_builder;
    arrow::BinaryBuilder result_schema_builder;
    arrow::BooleanBuilder has_header_builder;
    arrow::BinaryBuilder header_schema_builder;
    arrow::BooleanBuilder is_exchange_builder;

    // Accumulate the canonical protocol-hash payload as we go.  The hash
    // algorithm mirrors Python's introspect.compute_protocol_hash so ports
    // that produce identical describe payloads produce identical hashes.
    for (const auto& method_name : sorted_names) {
        const auto& info = methods.at(method_name);
        const char* method_type_str = info.method_type == MethodType::UNARY ? "unary" : "stream";

        VGI_RPC_THROW_NOT_OK(name_builder.Append(info.name));
        VGI_RPC_THROW_NOT_OK(method_type_builder.Append(method_type_str));
        VGI_RPC_THROW_NOT_OK(has_return_builder.Append(info.has_return));

        auto params_buf = serialize_schema(info.params_schema);
        VGI_RPC_THROW_NOT_OK(params_schema_builder.Append(
            params_buf->data(), static_cast<int32_t>(params_buf->size())));

        // For stream methods result_schema reflects the (empty) Protocol-level
        // return type; for unary it is the actual result schema.
        auto result_schema = info.result_schema ? info.result_schema : empty_schema();
        auto result_buf = serialize_schema(result_schema);
        VGI_RPC_THROW_NOT_OK(result_schema_builder.Append(
            result_buf->data(), static_cast<int32_t>(result_buf->size())));

        bool has_header = info.header_schema != nullptr;
        VGI_RPC_THROW_NOT_OK(has_header_builder.Append(has_header));

        if (has_header) {
            auto header_buf = serialize_schema(info.header_schema);
            VGI_RPC_THROW_NOT_OK(header_schema_builder.Append(
                header_buf->data(), static_cast<int32_t>(header_buf->size())));
        } else {
            VGI_RPC_THROW_NOT_OK(header_schema_builder.AppendNull());
        }

        // is_exchange is always null: a Protocol cannot statically distinguish
        // producer from exchange streams (matches the Python reference).
        VGI_RPC_THROW_NOT_OK(is_exchange_builder.AppendNull());
    }

    auto batch = arrow::RecordBatch::Make(
        describe_schema(), static_cast<int64_t>(sorted_names.size()),
        {unwrap(name_builder.Finish()), unwrap(method_type_builder.Finish()),
         unwrap(has_return_builder.Finish()), unwrap(params_schema_builder.Finish()),
         unwrap(result_schema_builder.Finish()), unwrap(has_header_builder.Finish()),
         unwrap(header_schema_builder.Finish()), unwrap(is_exchange_builder.Finish())});

    std::string protocol_hash = compute_protocol_hash(protocol_name, methods);

    auto describe_md = std::make_shared<arrow::KeyValueMetadata>();
    describe_md->Append(keys::PROTOCOL_NAME, protocol_name);
    describe_md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    describe_md->Append(keys::DESCRIBE_VERSION, DESCRIBE_VERSION_VALUE);
    describe_md->Append(keys::PROTOCOL_HASH, protocol_hash);
    describe_md->Append(keys::SERVER_ID, server_id);
    if (!protocol_version.empty()) {
        describe_md->Append(keys::PROTOCOL_VERSION, protocol_version);
    }

    auto captured_batch = std::move(batch);
    auto captured_md = std::move(describe_md);
    auto schema = describe_schema();

    MethodInfo describe_info;
    describe_info.name = DESCRIBE_METHOD_NAME;
    describe_info.method_type = MethodType::UNARY;
    describe_info.params_schema = empty_schema();
    describe_info.result_schema = schema;
    describe_info.has_return = true;
    describe_info.doc = "Return machine-readable metadata about all server methods.";
    describe_info.handler = [captured_batch, captured_md](const Request&, CallContext&) -> Result {
        AnnotatedBatch ab;
        ab.batch = captured_batch;
        ab.custom_metadata = captured_md;
        return Result::from_annotated_batch(std::move(ab));
    };

    methods[DESCRIBE_METHOD_NAME] = std::move(describe_info);
}

}  // namespace vgi_rpc
