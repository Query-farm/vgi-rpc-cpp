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

#include <nlohmann/json.hpp>

#include <algorithm>
#include <vector>

namespace vgi_rpc {

namespace {

// The describe response schema (matches Python's _DESCRIBE_SCHEMA)
std::shared_ptr<arrow::Schema> describe_schema() {
    static auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("method_type", arrow::utf8()),
        arrow::field("doc", arrow::utf8()),        // nullable
        arrow::field("has_return", arrow::boolean()),
        arrow::field("params_schema_ipc", arrow::binary()),
        arrow::field("result_schema_ipc", arrow::binary()),
        arrow::field("param_types_json", arrow::utf8()),    // nullable
        arrow::field("param_defaults_json", arrow::utf8()), // nullable
        arrow::field("has_header", arrow::boolean()),
        arrow::field("header_schema_ipc", arrow::binary()), // nullable
    });
    return schema;
}

std::shared_ptr<arrow::Buffer> serialize_schema(const std::shared_ptr<arrow::Schema>& schema) {
    return unwrap(arrow::ipc::SerializeSchema(*schema), "Failed to serialize schema");
}

}  // anonymous namespace

void register_describe(
    std::unordered_map<std::string, MethodInfo>& methods,
    const std::string& protocol_name,
    const std::string& server_id) {

    // Build the describe response once at registration time.  This is
    // intentionally a snapshot of the current method table — the response
    // is captured in a lambda and served identically on every __describe__
    // call, matching the Python implementation's behaviour.

    // Collect methods sorted by name (excluding __describe__ itself)
    std::vector<std::string> sorted_names;
    for (const auto& [name, _] : methods) {
        sorted_names.push_back(name);
    }
    std::sort(sorted_names.begin(), sorted_names.end());

    // Build arrays for the describe batch
    arrow::StringBuilder name_builder;
    arrow::StringBuilder method_type_builder;
    arrow::StringBuilder doc_builder;
    arrow::BooleanBuilder has_return_builder;
    arrow::BinaryBuilder params_schema_builder;
    arrow::BinaryBuilder result_schema_builder;
    arrow::StringBuilder param_types_builder;
    arrow::StringBuilder param_defaults_builder;
    arrow::BooleanBuilder has_header_builder;
    arrow::BinaryBuilder header_schema_builder;

    for (const auto& method_name : sorted_names) {
        const auto& info = methods.at(method_name);

        VGI_RPC_THROW_NOT_OK(name_builder.Append(info.name));
        VGI_RPC_THROW_NOT_OK(method_type_builder.Append(
            info.method_type == MethodType::UNARY ? "unary" : "stream"));

        if (info.doc.empty()) {
            VGI_RPC_THROW_NOT_OK(doc_builder.AppendNull());
        } else {
            VGI_RPC_THROW_NOT_OK(doc_builder.Append(info.doc));
        }

        VGI_RPC_THROW_NOT_OK(has_return_builder.Append(info.has_return));

        auto params_buf = serialize_schema(info.params_schema);
        VGI_RPC_THROW_NOT_OK(params_schema_builder.Append(
            params_buf->data(), static_cast<int32_t>(params_buf->size())));

        auto result_buf = serialize_schema(info.result_schema);
        VGI_RPC_THROW_NOT_OK(result_schema_builder.Append(
            result_buf->data(), static_cast<int32_t>(result_buf->size())));

        // param_types_json and param_defaults_json
        // For C++, we don't have type annotations, so we emit null for now
        VGI_RPC_THROW_NOT_OK(param_types_builder.AppendNull());
        VGI_RPC_THROW_NOT_OK(param_defaults_builder.AppendNull());

        // has_header
        bool has_header = info.header_schema != nullptr;
        VGI_RPC_THROW_NOT_OK(has_header_builder.Append(has_header));

        if (has_header) {
            auto header_buf = serialize_schema(info.header_schema);
            VGI_RPC_THROW_NOT_OK(header_schema_builder.Append(
                header_buf->data(), static_cast<int32_t>(header_buf->size())));
        } else {
            VGI_RPC_THROW_NOT_OK(header_schema_builder.AppendNull());
        }
    }

    // Finish arrays
    auto name_arr = unwrap(name_builder.Finish());
    auto method_type_arr = unwrap(method_type_builder.Finish());
    auto doc_arr = unwrap(doc_builder.Finish());
    auto has_return_arr = unwrap(has_return_builder.Finish());
    auto params_schema_arr = unwrap(params_schema_builder.Finish());
    auto result_schema_arr = unwrap(result_schema_builder.Finish());
    auto param_types_arr = unwrap(param_types_builder.Finish());
    auto param_defaults_arr = unwrap(param_defaults_builder.Finish());
    auto has_header_arr = unwrap(has_header_builder.Finish());
    auto header_schema_arr = unwrap(header_schema_builder.Finish());

    auto batch = arrow::RecordBatch::Make(
        describe_schema(),
        static_cast<int64_t>(sorted_names.size()),
        {name_arr, method_type_arr, doc_arr, has_return_arr,
         params_schema_arr, result_schema_arr, param_types_arr,
         param_defaults_arr, has_header_arr, header_schema_arr});

    // Describe response metadata
    auto describe_md = std::make_shared<arrow::KeyValueMetadata>();
    describe_md->Append(keys::PROTOCOL_NAME, protocol_name);
    describe_md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    describe_md->Append(keys::DESCRIBE_VERSION, DESCRIBE_VERSION_VALUE);
    describe_md->Append(keys::SERVER_ID, server_id);

    // Capture the batch and metadata for the handler
    auto captured_batch = std::move(batch);
    auto captured_md = std::move(describe_md);
    auto schema = describe_schema();

    // Register the __describe__ method
    MethodInfo describe_info;
    describe_info.name = DESCRIBE_METHOD_NAME;
    describe_info.method_type = MethodType::UNARY;
    describe_info.params_schema = empty_schema();
    describe_info.result_schema = schema;
    describe_info.has_return = true;
    describe_info.doc = "Return machine-readable metadata about all server methods.";
    describe_info.handler = [captured_batch, captured_md](
        const Request&, CallContext&) -> Result {
        AnnotatedBatch ab;
        ab.batch = captured_batch;
        ab.custom_metadata = captured_md;
        return Result::from_annotated_batch(std::move(ab));
    };

    methods[DESCRIBE_METHOD_NAME] = std::move(describe_info);
}

}  // namespace vgi_rpc
