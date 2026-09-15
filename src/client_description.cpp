// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client_description.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <cctype>
#include <stdexcept>
#include <string>
#include <vector>

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

// ── vgi_rpc.Reflection.v1 ─────────────────────────────────────────────
//
// The reply to a reflection method rides the framework's ordinary convention
// for a structured return: a one-row batch whose single `result` binary column
// holds a complete Arrow IPC stream of the payload struct.  Reflection is an
// ordinary co-hosted protocol, so it gets no exemption from that -- which is
// also why its replies are subject to externalization and response caps like
// any other method's.

namespace {

/// Unwrap the `result` column of a reflection reply into its payload batch.
std::shared_ptr<arrow::RecordBatch> reflection_payload(const AnnotatedBatch& response,
                                                       const char* what) {
    if (!response.batch) {
        throw std::runtime_error(std::string(what) + " reply has no record batch");
    }
    const auto column = response.batch->GetColumnByName("result");
    const auto bytes = std::dynamic_pointer_cast<arrow::BinaryArray>(column);
    if (!bytes || bytes->length() < 1 || bytes->IsNull(0)) {
        throw std::runtime_error(std::string(what) + " reply has no 'result' payload");
    }
    int32_t size = 0;
    const uint8_t* data = bytes->GetValue(0, &size);
    auto buffer = std::make_shared<arrow::Buffer>(data, size);
    arrow::io::BufferReader input(buffer);
    const std::string context = std::string("invalid IPC in ") + what + " reply";
    auto reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(&input), context.c_str());
    std::shared_ptr<arrow::RecordBatch> batch;
    VGI_RPC_THROW_NOT_OK(reader->ReadNext(&batch));
    if (!batch || batch->num_rows() < 1) {
        throw std::runtime_error(std::string(what) + " reply payload is empty");
    }
    return batch;
}

/// Look up a field by name, or return nullptr when the producer did not send it.
///
/// By name and not by position: a port that appends a field in a minor version
/// must not shift every field a positional decoder reads.
std::shared_ptr<arrow::Array> field_of(const arrow::RecordBatch& batch, const char* name) {
    return batch.GetColumnByName(name);
}

std::shared_ptr<arrow::Array> field_of(const arrow::StructArray& values, const char* name) {
    return values.GetFieldByName(name);
}

[[noreturn]] void missing_required(const char* what, const char* name) {
    // Absent-and-undefaulted is the one intolerable case: a caller acts on a
    // description, so a zero-filled required field is worse than no answer.
    throw std::runtime_error(std::string(what) + " reply is missing required field '" + name + "'");
}

[[noreturn]] void wrong_type(const char* what, const char* name) {
    throw std::runtime_error(std::string(what) + " reply field '" + name +
                             "' has the wrong Arrow type");
}

std::string take_string(const std::shared_ptr<arrow::Array>& column, int64_t row, const char* what,
                        const char* name, const std::string* fallback) {
    if (!column) {
        if (fallback) return *fallback;
        missing_required(what, name);
    }
    const auto strings = std::dynamic_pointer_cast<arrow::StringArray>(column);
    if (!strings) wrong_type(what, name);
    if (strings->IsNull(row)) return fallback ? *fallback : std::string();
    return strings->GetString(row);
}

bool take_bool(const std::shared_ptr<arrow::Array>& column, int64_t row, const char* what,
               const char* name, const bool* fallback) {
    if (!column) {
        if (fallback) return *fallback;
        missing_required(what, name);
    }
    const auto flags = std::dynamic_pointer_cast<arrow::BooleanArray>(column);
    if (!flags) wrong_type(what, name);
    if (flags->IsNull(row)) return fallback ? *fallback : false;
    return flags->Value(row);
}

std::string take_binary(const std::shared_ptr<arrow::Array>& column, int64_t row, const char* what,
                        const char* name) {
    if (!column) missing_required(what, name);
    const auto bytes = std::dynamic_pointer_cast<arrow::BinaryArray>(column);
    if (!bytes) wrong_type(what, name);
    if (bytes->IsNull(row)) return std::string();
    int32_t size = 0;
    const uint8_t* data = bytes->GetValue(row, &size);
    return std::string(reinterpret_cast<const char*>(data), static_cast<size_t>(size));
}

std::vector<std::string> take_string_list(const std::shared_ptr<arrow::Array>& column, int64_t row,
                                          const char* what, const char* name) {
    std::vector<std::string> out;
    if (!column) missing_required(what, name);
    const auto lists = std::dynamic_pointer_cast<arrow::ListArray>(column);
    if (!lists) wrong_type(what, name);
    if (lists->IsNull(row)) return out;
    const auto items = std::dynamic_pointer_cast<arrow::StringArray>(lists->value_slice(row));
    if (!items) wrong_type(what, name);
    out.reserve(static_cast<size_t>(items->length()));
    for (int64_t index = 0; index < items->length(); ++index) {
        out.push_back(items->IsNull(index) ? std::string() : items->GetString(index));
    }
    return out;
}

/// The struct elements of a one-row list column, or an empty array when absent.
std::shared_ptr<arrow::StructArray> take_struct_list(const std::shared_ptr<arrow::Array>& column,
                                                     int64_t row, const char* what,
                                                     const char* name) {
    if (!column) missing_required(what, name);
    const auto lists = std::dynamic_pointer_cast<arrow::ListArray>(column);
    if (!lists) wrong_type(what, name);
    if (lists->IsNull(row)) return nullptr;
    const auto values = std::dynamic_pointer_cast<arrow::StructArray>(lists->value_slice(row));
    if (!values) wrong_type(what, name);
    return values;
}

/// Read a schema from IPC bytes, treating empty as the empty schema.
///
/// Empty rather than null throughout the reflection payloads: a nullable
/// column costs every port a null check on a value it will only ever treat as
/// absent.
std::shared_ptr<arrow::Schema> schema_from_ipc(const std::string& bytes) {
    if (bytes.empty()) return empty_schema();
    return read_schema(reinterpret_cast<const uint8_t*>(bytes.data()),
                       static_cast<int32_t>(bytes.size()));
}

ReflectedProtocol decode_summary(const arrow::StructArray& values, int64_t row, const char* what) {
    ReflectedProtocol summary;
    summary.protocol = take_string(field_of(values, "protocol"), row, what, "protocol", nullptr);
    summary.protocol_version =
        take_string(field_of(values, "protocol_version"), row, what, "protocol_version", nullptr);
    summary.protocol_hash =
        take_string(field_of(values, "protocol_hash"), row, what, "protocol_hash", nullptr);
    summary.deprecated =
        take_bool(field_of(values, "deprecated"), row, what, "deprecated", nullptr);
    summary.deprecation_message = take_string(field_of(values, "deprecation_message"), row, what,
                                              "deprecation_message", nullptr);
    summary.features = take_string_list(field_of(values, "features"), row, what, "features");
    return summary;
}

}  // namespace

const ReflectedProtocol* ProtocolListing::application() const noexcept {
    for (const auto& summary : protocols) {
        if (summary.protocol.rfind("vgi_rpc.", 0) != 0) return &summary;
    }
    return nullptr;
}

ProtocolListing decode_protocol_list(const AnnotatedBatch& response) {
    static constexpr const char* kWhat = "list_protocols";
    const auto batch = reflection_payload(response, kWhat);

    ProtocolListing listing;
    listing.server_id = take_string(field_of(*batch, "server_id"), 0, kWhat, "server_id", nullptr);
    listing.server_version =
        take_string(field_of(*batch, "server_version"), 0, kWhat, "server_version", nullptr);
    listing.request_version =
        take_string(field_of(*batch, "request_version"), 0, kWhat, "request_version", nullptr);
    if (listing.request_version != REQUEST_VERSION_VALUE) {
        throw std::runtime_error("unsupported request version in list_protocols reply: " +
                                 listing.request_version);
    }

    const auto values = take_struct_list(field_of(*batch, "protocols"), 0, kWhat, "protocols");
    if (values) {
        listing.protocols.reserve(static_cast<size_t>(values->length()));
        for (int64_t row = 0; row < values->length(); ++row) {
            listing.protocols.push_back(decode_summary(*values, row, kWhat));
        }
    }
    return listing;
}

ServiceDescription decode_service_description(const AnnotatedBatch& response,
                                              const ProtocolListing* listing) {
    static constexpr const char* kWhat = "describe";
    const auto batch = reflection_payload(response, kWhat);

    ServiceDescription description;
    description.protocol_name =
        take_string(field_of(*batch, "protocol"), 0, kWhat, "protocol", nullptr);
    description.protocol_version =
        take_string(field_of(*batch, "protocol_version"), 0, kWhat, "protocol_version", nullptr);
    description.protocol_hash =
        take_string(field_of(*batch, "protocol_hash"), 0, kWhat, "protocol_hash", nullptr);
    if (!is_sha256_hex(description.protocol_hash)) {
        throw std::runtime_error("describe reply contains an invalid protocol hash");
    }
    // Neither identity field is a property of a protocol, so neither is on the
    // description. They come from the listing when there was one, and are left
    // empty -- not invented -- when the caller named a protocol outright.
    if (listing) {
        description.server_id = listing->server_id;
        description.request_version = listing->request_version;
    }
    // The client-side view keeps reporting the describe format it presents,
    // which is this one; the wire format's version rides its protocol name.
    description.describe_version = DESCRIBE_VERSION_VALUE;

    const auto methods = take_struct_list(field_of(*batch, "methods"), 0, kWhat, "methods");
    if (!methods) return description;
    description.methods.reserve(static_cast<size_t>(methods->length()));
    for (int64_t row = 0; row < methods->length(); ++row) {
        MethodDescription method;
        method.name = take_string(field_of(*methods, "name"), row, kWhat, "name", nullptr);
        if (method.name.empty()) {
            throw std::runtime_error("describe reply contains an empty method name");
        }
        method.method_type =
            take_string(field_of(*methods, "method_type"), row, kWhat, "method_type", nullptr);
        if (method.method_type != "unary" && method.method_type != "stream") {
            throw std::runtime_error("describe reply contains an invalid method type for '" +
                                     method.name + "'");
        }
        method.has_return =
            take_bool(field_of(*methods, "has_return"), row, kWhat, "has_return", nullptr);
        method.has_header =
            take_bool(field_of(*methods, "has_header"), row, kWhat, "has_header", nullptr);
        method.params_schema = schema_from_ipc(
            take_binary(field_of(*methods, "params_schema_ipc"), row, kWhat, "params_schema_ipc"));
        method.result_schema = schema_from_ipc(
            take_binary(field_of(*methods, "result_schema_ipc"), row, kWhat, "result_schema_ipc"));
        const auto header_ipc =
            take_binary(field_of(*methods, "header_schema_ipc"), row, kWhat, "header_schema_ipc");
        if (method.has_header) method.header_schema = schema_from_ipc(header_ipc);
        // Three-valued on the wire and three-valued here: a port that decides
        // producer-vs-exchange from the returned stream cannot say ahead of the
        // call, and "I cannot say" is a different answer from "producer".
        const auto stream_kind =
            take_string(field_of(*methods, "stream_kind"), row, kWhat, "stream_kind", nullptr);
        if (stream_kind == "exchange") method.is_exchange = true;
        if (stream_kind == "producer") method.is_exchange = false;

        if (!description.methods.emplace(method.name, std::move(method)).second) {
            throw std::runtime_error("describe reply contains duplicate method names");
        }
    }
    return description;
}

}  // namespace vgi_rpc
