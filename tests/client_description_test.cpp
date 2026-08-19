// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client_description.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <catch2/catch_test_macros.hpp>

namespace {

std::shared_ptr<arrow::Buffer> schema_bytes(const std::shared_ptr<arrow::Schema>& schema) {
    return vgi_rpc::unwrap(arrow::ipc::SerializeSchema(*schema));
}

vgi_rpc::AnnotatedBatch description_batch(const std::vector<std::string>& names) {
    auto params = arrow::schema({arrow::field("value", arrow::int64(), false)});
    auto result = arrow::schema({arrow::field("result", arrow::utf8())});
    auto params_ipc = schema_bytes(params);
    auto result_ipc = schema_bytes(result);

    arrow::StringBuilder name_builder;
    arrow::StringBuilder method_type_builder;
    arrow::BooleanBuilder has_return_builder;
    arrow::BinaryBuilder params_builder;
    arrow::BinaryBuilder result_builder;
    arrow::BooleanBuilder has_header_builder;
    arrow::BinaryBuilder header_builder;
    arrow::BooleanBuilder exchange_builder;
    for (const auto& name : names) {
        REQUIRE(name_builder.Append(name).ok());
        REQUIRE(method_type_builder.Append("unary").ok());
        REQUIRE(has_return_builder.Append(true).ok());
        REQUIRE(params_builder.Append(params_ipc->data(), params_ipc->size()).ok());
        REQUIRE(result_builder.Append(result_ipc->data(), result_ipc->size()).ok());
        REQUIRE(has_header_builder.Append(false).ok());
        REQUIRE(header_builder.AppendNull().ok());
        REQUIRE(exchange_builder.AppendNull().ok());
    }

    auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("method_type", arrow::utf8()),
        arrow::field("has_return", arrow::boolean()),
        arrow::field("params_schema_ipc", arrow::binary()),
        arrow::field("result_schema_ipc", arrow::binary()),
        arrow::field("has_header", arrow::boolean()),
        arrow::field("header_schema_ipc", arrow::binary(), true),
        arrow::field("is_exchange", arrow::boolean(), true),
    });
    auto batch = arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(names.size()),
        {vgi_rpc::unwrap(name_builder.Finish()), vgi_rpc::unwrap(method_type_builder.Finish()),
         vgi_rpc::unwrap(has_return_builder.Finish()), vgi_rpc::unwrap(params_builder.Finish()),
         vgi_rpc::unwrap(result_builder.Finish()), vgi_rpc::unwrap(has_header_builder.Finish()),
         vgi_rpc::unwrap(header_builder.Finish()), vgi_rpc::unwrap(exchange_builder.Finish())});
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(vgi_rpc::keys::PROTOCOL_NAME, "example");
    metadata->Append(vgi_rpc::keys::REQUEST_VERSION, vgi_rpc::REQUEST_VERSION_VALUE);
    metadata->Append(vgi_rpc::keys::DESCRIBE_VERSION, vgi_rpc::DESCRIBE_VERSION_VALUE);
    metadata->Append(vgi_rpc::keys::PROTOCOL_HASH,
                     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    metadata->Append(vgi_rpc::keys::SERVER_ID, "server-1");
    metadata->Append(vgi_rpc::keys::PROTOCOL_VERSION, "1.2.3");
    return vgi_rpc::AnnotatedBatch::with_metadata(batch, metadata);
}

}  // namespace

TEST_CASE("client describe parser preserves exact schemas") {
    const auto description = vgi_rpc::parse_service_description(description_batch({"lookup"}));
    REQUIRE(description.protocol_name == "example");
    REQUIRE(description.protocol_version == "1.2.3");
    REQUIRE(description.methods.size() == 1);
    const auto* method = description.method("lookup");
    REQUIRE(method != nullptr);
    REQUIRE(method->method_type == "unary");
    REQUIRE(method->params_schema->field(0)->name() == "value");
    REQUIRE(method->params_schema->field(0)->nullable() == false);
    REQUIRE(method->result_schema->field(0)->type()->Equals(arrow::utf8()));
    REQUIRE(method->header_schema == nullptr);
    REQUIRE_FALSE(method->is_exchange.has_value());
}

TEST_CASE("client describe parser rejects duplicate method names") {
    REQUIRE_THROWS(vgi_rpc::parse_service_description(description_batch({"lookup", "lookup"})));
}
