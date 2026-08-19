// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client_description.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <cctype>
#include <stdexcept>
#include <string>

namespace vgi_rpc {

namespace {

template <typename ArrayType>
std::shared_ptr<ArrayType> required_column(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const char* name, arrow::Type::type type) {
    const auto column = batch->GetColumnByName(name);
    if (!column)
        throw std::runtime_error(std::string("describe response is missing '") + name + "'");
    if (column->type_id() != type) {
        throw std::runtime_error(std::string("describe response column '") + name +
                                 "' has the wrong Arrow type");
    }
    return std::static_pointer_cast<ArrayType>(column);
}

std::string required_metadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                              const char* key) {
    const std::string value = get_metadata_value(metadata, key);
    if (value.empty()) {
        throw std::runtime_error(std::string("describe response is missing metadata '") + key +
                                 "'");
    }
    return value;
}

std::shared_ptr<arrow::Schema> read_schema(const uint8_t* data, int32_t size) {
    auto buffer = std::make_shared<arrow::Buffer>(data, size);
    arrow::io::BufferReader input(buffer);
    arrow::ipc::DictionaryMemo dictionary_memo;
    return unwrap(arrow::ipc::ReadSchema(&input, &dictionary_memo),
                  "invalid schema in describe response");
}

std::shared_ptr<arrow::Schema> read_schema(const std::shared_ptr<arrow::BinaryArray>& array,
                                           int64_t row) {
    int32_t size = 0;
    const uint8_t* data = array->GetValue(row, &size);
    return read_schema(data, size);
}

void require_non_null(const std::shared_ptr<arrow::Array>& array, int64_t row, const char* name) {
    if (array->IsNull(row)) {
        throw std::runtime_error(std::string("describe response contains null '") + name + "'");
    }
}

bool is_sha256_hex(const std::string& value) {
    if (value.size() != 64) return false;
    for (const unsigned char character : value) {
        if (!std::isxdigit(character)) return false;
    }
    return true;
}

}  // namespace

const MethodDescription* ServiceDescription::method(const std::string& name) const noexcept {
    const auto it = methods.find(name);
    return it == methods.end() ? nullptr : &it->second;
}

ServiceDescription parse_service_description(const AnnotatedBatch& response) {
    if (!response.batch) throw std::runtime_error("describe response has no record batch");

    const auto& batch = response.batch;
    const auto names = required_column<arrow::StringArray>(batch, "name", arrow::Type::STRING);
    const auto method_types =
        required_column<arrow::StringArray>(batch, "method_type", arrow::Type::STRING);
    const auto has_returns =
        required_column<arrow::BooleanArray>(batch, "has_return", arrow::Type::BOOL);
    const auto params =
        required_column<arrow::BinaryArray>(batch, "params_schema_ipc", arrow::Type::BINARY);
    const auto results =
        required_column<arrow::BinaryArray>(batch, "result_schema_ipc", arrow::Type::BINARY);
    const auto has_headers =
        required_column<arrow::BooleanArray>(batch, "has_header", arrow::Type::BOOL);
    const auto headers =
        required_column<arrow::BinaryArray>(batch, "header_schema_ipc", arrow::Type::BINARY);
    const auto exchanges =
        required_column<arrow::BooleanArray>(batch, "is_exchange", arrow::Type::BOOL);

    ServiceDescription description;
    description.protocol_name = required_metadata(response.custom_metadata, keys::PROTOCOL_NAME);
    description.request_version =
        required_metadata(response.custom_metadata, keys::REQUEST_VERSION);
    description.describe_version =
        required_metadata(response.custom_metadata, keys::DESCRIBE_VERSION);
    description.protocol_hash = required_metadata(response.custom_metadata, keys::PROTOCOL_HASH);
    description.server_id = required_metadata(response.custom_metadata, keys::SERVER_ID);
    description.protocol_version =
        get_metadata_value(response.custom_metadata, keys::PROTOCOL_VERSION);

    if (description.request_version != REQUEST_VERSION_VALUE) {
        throw std::runtime_error("unsupported request version in describe response: " +
                                 description.request_version);
    }
    if (description.describe_version != DESCRIBE_VERSION_VALUE) {
        throw std::runtime_error("unsupported describe version: " + description.describe_version);
    }
    if (!is_sha256_hex(description.protocol_hash)) {
        throw std::runtime_error("describe response contains an invalid protocol hash");
    }

    description.methods.reserve(static_cast<size_t>(batch->num_rows()));
    for (int64_t row = 0; row < batch->num_rows(); ++row) {
        require_non_null(names, row, "name");
        require_non_null(method_types, row, "method_type");
        require_non_null(has_returns, row, "has_return");
        require_non_null(params, row, "params_schema_ipc");
        require_non_null(results, row, "result_schema_ipc");
        require_non_null(has_headers, row, "has_header");

        MethodDescription method;
        method.name = names->GetString(row);
        method.method_type = method_types->GetString(row);
        if (method.name.empty())
            throw std::runtime_error("describe response contains an empty method name");
        if (method.method_type != "unary" && method.method_type != "stream") {
            throw std::runtime_error("describe response contains an invalid method type for '" +
                                     method.name + "'");
        }
        method.has_return = has_returns->Value(row);
        method.params_schema = read_schema(params, row);
        method.result_schema = read_schema(results, row);
        method.has_header = has_headers->Value(row);
        if (method.has_header) {
            require_non_null(headers, row, "header_schema_ipc");
            method.header_schema = read_schema(headers, row);
        } else if (!headers->IsNull(row)) {
            throw std::runtime_error(
                "describe response has a header schema when has_header is false");
        }
        if (!exchanges->IsNull(row)) method.is_exchange = exchanges->Value(row);

        if (!description.methods.emplace(method.name, std::move(method)).second) {
            throw std::runtime_error("describe response contains duplicate method names");
        }
    }
    return description;
}

}  // namespace vgi_rpc
