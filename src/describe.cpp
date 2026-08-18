// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/crypto.h"
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

    crypto::Sha256 hasher;
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
