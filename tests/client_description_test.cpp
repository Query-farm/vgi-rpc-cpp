// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client_description.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <utility>
#include <vector>

// ── vgi_rpc.Reflection.v1 ────────────────────────────────────────────
//
// Tolerant decoding is normative, not a convenience: a version-mismatched
// client calls reflection precisely to learn *what* mismatched, so a decoder
// that failed on minor skew would fail exactly when it is needed.  The
// conformance suite cannot reach this -- it can only produce whatever payload
// the reference port emits today -- so the property is asserted here against
// deliberately skewed schemas.

namespace {

std::shared_ptr<arrow::Buffer> schema_bytes(const std::shared_ptr<arrow::Schema>& schema) {
    return vgi_rpc::unwrap(arrow::ipc::SerializeSchema(*schema));
}

/// Wrap a payload batch the way a reflection reply carries it: one row, one
/// `result` binary column holding a complete IPC stream.
vgi_rpc::AnnotatedBatch reflection_reply(const std::shared_ptr<arrow::RecordBatch>& payload) {
    auto sink = vgi_rpc::unwrap(arrow::io::BufferOutputStream::Create());
    auto writer = vgi_rpc::unwrap(arrow::ipc::MakeStreamWriter(sink, payload->schema()));
    REQUIRE(writer->WriteRecordBatch(*payload).ok());
    REQUIRE(writer->Close().ok());
    auto buffer = vgi_rpc::unwrap(sink->Finish());

    arrow::BinaryBuilder builder;
    REQUIRE(builder.Append(buffer->data(), static_cast<int32_t>(buffer->size())).ok());
    auto schema = arrow::schema({arrow::field("result", arrow::binary(), false)});
    return vgi_rpc::AnnotatedBatch::data(
        arrow::RecordBatch::Make(schema, 1, {vgi_rpc::unwrap(builder.Finish())}));
}

std::shared_ptr<arrow::Array> one_utf8(const std::string& value) {
    arrow::StringBuilder builder;
    REQUIRE(builder.Append(value).ok());
    return vgi_rpc::unwrap(builder.Finish());
}

std::shared_ptr<arrow::Array> one_bool(bool value) {
    arrow::BooleanBuilder builder;
    REQUIRE(builder.Append(value).ok());
    return vgi_rpc::unwrap(builder.Finish());
}

std::shared_ptr<arrow::Array> one_binary(const std::shared_ptr<arrow::Buffer>& value) {
    arrow::BinaryBuilder builder;
    REQUIRE(builder.Append(value->data(), static_cast<int32_t>(value->size())).ok());
    return vgi_rpc::unwrap(builder.Finish());
}

/// A one-row list holding every element of `values`.
std::shared_ptr<arrow::Array> one_row_list(const std::shared_ptr<arrow::Array>& values) {
    arrow::Int32Builder offsets;
    REQUIRE(offsets.Append(0).ok());
    REQUIRE(offsets.Append(static_cast<int32_t>(values->length())).ok());
    auto offset_array = vgi_rpc::unwrap(offsets.Finish());
    auto item = arrow::field("item", values->type(), true);
    return std::make_shared<arrow::ListArray>(
        arrow::list(item), 1, std::static_pointer_cast<arrow::Int32Array>(offset_array)->values(),
        values);
}

using NamedColumns = std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>;

std::shared_ptr<arrow::StructArray> struct_of(const NamedColumns& columns) {
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    for (const auto& [name, array] : columns) {
        fields.push_back(arrow::field(name, array->type(), false));
        arrays.push_back(array);
    }
    return std::make_shared<arrow::StructArray>(arrow::struct_(fields), 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> batch_of(const NamedColumns& columns) {
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    for (const auto& [name, array] : columns) {
        fields.push_back(arrow::field(name, array->type(), false));
        arrays.push_back(array);
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), 1, arrays);
}

constexpr const char* kHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// One `describe` payload, minus `omit` and plus any `extra` columns.
///
/// The method struct likewise drops `omit_method` and gains `extra_method`, so
/// one helper covers both directions of minor skew at both nesting levels.
vgi_rpc::AnnotatedBatch describe_payload(const std::string& omit = "",
                                         const std::string& omit_method = "", bool extra = false) {
    auto params = arrow::schema({arrow::field("value", arrow::int64(), false)});
    auto params_ipc = schema_bytes(params);

    NamedColumns method{{"name", one_utf8("lookup")},
                        {"method_type", one_utf8("stream")},
                        {"has_return", one_bool(false)},
                        {"has_header", one_bool(false)},
                        {"stream_kind", one_utf8("exchange")},
                        {"params_schema_ipc", one_binary(params_ipc)},
                        {"result_schema_ipc", one_binary(arrow::Buffer::FromString(""))},
                        {"header_schema_ipc", one_binary(arrow::Buffer::FromString(""))},
                        {"idempotency", one_utf8("no_side_effects")},
                        {"deprecated", one_bool(false)},
                        {"deprecation_message", one_utf8("")}};
    if (!omit_method.empty()) {
        std::erase_if(method, [&](const auto& c) { return c.first == omit_method; });
    }
    if (extra) method.emplace_back("minted_in_a_later_minor", one_utf8("ignore me"));

    NamedColumns top{{"protocol", one_utf8("example")},
                     {"protocol_version", one_utf8("1.2.3")},
                     {"protocol_hash", one_utf8(kHash)},
                     {"deprecated", one_bool(false)},
                     {"deprecation_message", one_utf8("")},
                     {"features", one_row_list(one_utf8("resumable"))},
                     {"methods", one_row_list(struct_of(method))}};
    if (!omit.empty()) {
        std::erase_if(top, [&](const auto& c) { return c.first == omit; });
    }
    if (extra) top.emplace_back("minted_in_a_later_minor", one_utf8("ignore me"));
    return reflection_reply(batch_of(top));
}

}  // namespace

TEST_CASE("reflection describe decodes into the client-side view") {
    const auto description = vgi_rpc::decode_service_description(describe_payload());
    REQUIRE(description.protocol_name == "example");
    REQUIRE(description.protocol_version == "1.2.3");
    REQUIRE(description.protocol_hash == kHash);
    // Server identity is not a property of a protocol, so a description that
    // was fetched without a listing reports none rather than inventing one.
    REQUIRE(description.server_id.empty());
    REQUIRE(description.request_version.empty());

    const auto* method = description.method("lookup");
    REQUIRE(method != nullptr);
    REQUIRE(method->method_type == "stream");
    REQUIRE(method->is_exchange == true);
    REQUIRE(method->params_schema->field(0)->name() == "value");
    REQUIRE(method->header_schema == nullptr);
}

TEST_CASE("reflection decoding survives minor skew in both directions") {
    // A later minor answering this client: columns it has never heard of are
    // ignored rather than fatal.
    const auto newer = vgi_rpc::decode_service_description(
        describe_payload(/*omit=*/"", /*omit_method=*/"", /*extra=*/true));
    REQUIRE(newer.method("lookup") != nullptr);

    // An earlier minor answering it: a column with a default takes the default.
    const auto older = vgi_rpc::decode_service_description(describe_payload("", "idempotency"));
    REQUIRE(older.method("lookup") != nullptr);
}

TEST_CASE("reflection decoding refuses a field that is absent and undefaulted") {
    // The rule this makes normative for every port: a field added in a minor
    // version must carry a default.  Zero-filling a required field would hand
    // a caller a description that is wrong rather than missing, and a caller
    // acts on a description.
    REQUIRE_THROWS(vgi_rpc::decode_service_description(describe_payload("protocol_hash")));
    REQUIRE_THROWS(vgi_rpc::decode_service_description(describe_payload("methods")));
    REQUIRE_THROWS(vgi_rpc::decode_service_description(describe_payload("", "stream_kind")));
    REQUIRE_THROWS(vgi_rpc::decode_service_description(describe_payload("", "name")));
}

TEST_CASE("reflection list_protocols separates framework surface from the application") {
    NamedColumns summary{
        {"protocol", one_utf8("example")},     {"protocol_version", one_utf8("1.2.3")},
        {"protocol_hash", one_utf8(kHash)},    {"deprecated", one_bool(false)},
        {"deprecation_message", one_utf8("")}, {"features", one_row_list(one_utf8("resumable"))}};
    NamedColumns reflection{{"protocol", one_utf8(vgi_rpc::kReflectionProtocolName)},
                            {"protocol_version", one_utf8("")},
                            {"protocol_hash", one_utf8(kHash)},
                            {"deprecated", one_bool(false)},
                            {"deprecation_message", one_utf8("")},
                            {"features", one_row_list(one_utf8(""))}};
    // Reflection first, so "the application protocol" cannot be "the first one".
    auto protocols =
        vgi_rpc::unwrap(arrow::Concatenate({struct_of(reflection), struct_of(summary)}));

    const auto listing = vgi_rpc::decode_protocol_list(
        reflection_reply(batch_of({{"server_id", one_utf8("server-1")},
                                   {"server_version", one_utf8("0.2.0")},
                                   {"request_version", one_utf8(vgi_rpc::REQUEST_VERSION_VALUE)},
                                   {"protocols", one_row_list(protocols)}})));
    REQUIRE(listing.server_id == "server-1");
    REQUIRE(listing.protocols.size() == 2);
    const auto* application = listing.application();
    REQUIRE(application != nullptr);
    REQUIRE(application->protocol == "example");
    REQUIRE(application->features == std::vector<std::string>{"resumable"});
}
