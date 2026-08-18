// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/request.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/type.h>
#include <stdexcept>

namespace vgi_rpc {

Request::Request(std::shared_ptr<arrow::RecordBatch> batch,
                 std::shared_ptr<arrow::KeyValueMetadata> metadata)
    : batch_(std::move(batch))
    , metadata_(std::move(metadata)) {}

std::string Request::method_name() const {
    return get_metadata_value(metadata_, keys::METHOD);
}

std::string Request::request_id() const {
    return get_metadata_value(metadata_, keys::REQUEST_ID);
}

std::string Request::request_version() const {
    return get_metadata_value(metadata_, keys::REQUEST_VERSION);
}

const std::shared_ptr<arrow::Schema>& Request::schema() const {
    return batch_->schema();
}

std::shared_ptr<arrow::Array> Request::get_column(std::string_view name) const {
    return batch_->GetColumnByName(std::string(name));
}

bool Request::has_param(std::string_view name) const {
    return get_column(name) != nullptr;
}

namespace {

// ── Column extraction helpers ───────────────────────────────────────

std::shared_ptr<arrow::Array> require_column(
    const std::shared_ptr<arrow::RecordBatch>& batch, std::string_view name) {
    auto col = batch->GetColumnByName(std::string(name));
    if (!col) throw std::runtime_error("Parameter not found: " + std::string(name));
    if (col->length() == 0)
        throw std::runtime_error("Empty batch, cannot get parameter: " + std::string(name));
    return col;
}

template <typename ArrowArrayType>
auto get_value(const std::shared_ptr<arrow::RecordBatch>& batch,
               std::string_view name) {
    auto col = require_column(batch, name);
    auto typed = std::dynamic_pointer_cast<ArrowArrayType>(col);
    if (!typed) throw std::runtime_error("Type mismatch for parameter: " + std::string(name));
    if (typed->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));
    return typed->Value(0);
}

template <typename ArrowArrayType>
auto get_optional_value(const std::shared_ptr<arrow::RecordBatch>& batch,
                        std::string_view name)
    -> std::optional<decltype(std::declval<ArrowArrayType>().Value(0))> {
    auto col = batch->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    auto typed = std::dynamic_pointer_cast<ArrowArrayType>(col);
    if (!typed) throw std::runtime_error("Type mismatch for parameter: " + std::string(name));
    if (typed->IsNull(0)) return std::nullopt;
    return typed->Value(0);
}

// Try StringArray then LargeStringArray; returns the string at row 0.
std::string extract_string(const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    if (auto typed = std::dynamic_pointer_cast<arrow::StringArray>(col)) {
        if (typed->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));
        return typed->GetString(0);
    }
    if (auto typed = std::dynamic_pointer_cast<arrow::LargeStringArray>(col)) {
        if (typed->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));
        return typed->GetString(0);
    }
    throw std::runtime_error("Type mismatch for string parameter: " + std::string(name));
}

// Try BinaryArray then LargeBinaryArray; returns bytes at row 0.
std::vector<uint8_t> extract_binary(const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    auto to_vec = [](auto view) {
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(view.data()),
            reinterpret_cast<const uint8_t*>(view.data()) + view.size());
    };
    if (auto typed = std::dynamic_pointer_cast<arrow::BinaryArray>(col)) {
        if (typed->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));
        return to_vec(typed->GetView(0));
    }
    if (auto typed = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(col)) {
        if (typed->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));
        return to_vec(typed->GetView(0));
    }
    throw std::runtime_error("Type mismatch for bytes parameter: " + std::string(name));
}

// Optional variants
std::optional<std::string> get_optional_string(
    const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    if (auto typed = std::dynamic_pointer_cast<arrow::StringArray>(col)) {
        if (typed->IsNull(0)) return std::nullopt;
        return typed->GetString(0);
    }
    if (auto typed = std::dynamic_pointer_cast<arrow::LargeStringArray>(col)) {
        if (typed->IsNull(0)) return std::nullopt;
        return typed->GetString(0);
    }
    throw std::runtime_error("Type mismatch for string parameter: " + std::string(name));
}

std::optional<std::vector<uint8_t>> get_optional_binary(
    const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    auto to_vec = [](auto view) {
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(view.data()),
            reinterpret_cast<const uint8_t*>(view.data()) + view.size());
    };
    if (auto typed = std::dynamic_pointer_cast<arrow::BinaryArray>(col)) {
        if (typed->IsNull(0)) return std::nullopt;
        return to_vec(typed->GetView(0));
    }
    if (auto typed = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(col)) {
        if (typed->IsNull(0)) return std::nullopt;
        return to_vec(typed->GetView(0));
    }
    throw std::runtime_error("Type mismatch for bytes parameter: " + std::string(name));
}

// Generic list extraction helper: extracts vector<T> from a typed list array.
template <typename ListArrayType, typename ValueArrayType, typename T>
bool try_get_list_values(const std::shared_ptr<arrow::Array>& col,
                         std::string_view name,
                         const char* expected_type,
                         std::vector<T>& out) {
    auto list_arr = std::dynamic_pointer_cast<ListArrayType>(col);
    if (!list_arr) return false;
    if (list_arr->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));

    auto values = std::dynamic_pointer_cast<ValueArrayType>(list_arr->values());
    if (!values) throw std::runtime_error(
        std::string("Type mismatch: expected list<") + expected_type + "> for parameter: " + std::string(name));

    auto start = list_arr->value_offset(0);
    auto end = list_arr->value_offset(1);
    out.reserve(end - start);
    for (auto i = start; i < end; ++i) {
        out.push_back(values->Value(i));
    }
    return true;
}

// Try ListArray then LargeListArray → vector<T>
template <typename ValueArrayType, typename T>
std::vector<T> get_list_values(const std::shared_ptr<arrow::Array>& col,
                               std::string_view name,
                               const char* expected_type) {
    std::vector<T> result;
    if (try_get_list_values<arrow::ListArray, ValueArrayType, T>(col, name, expected_type, result))
        return result;
    if (try_get_list_values<arrow::LargeListArray, ValueArrayType, T>(col, name, expected_type, result))
        return result;
    throw std::runtime_error("Type mismatch: expected list for parameter: " + std::string(name));
}

// String list extraction helper: extracts vector<string> from a typed list array.
// Tries both StringArray and LargeStringArray for child values.
template <typename ListArrayType>
bool try_get_string_list_values(const std::shared_ptr<arrow::Array>& col,
                                std::string_view name,
                                std::vector<std::string>& out) {
    auto list_arr = std::dynamic_pointer_cast<ListArrayType>(col);
    if (!list_arr) return false;
    if (list_arr->IsNull(0)) throw std::runtime_error("Parameter is null: " + std::string(name));

    auto start = list_arr->value_offset(0);
    auto end = list_arr->value_offset(1);

    if (auto values = std::dynamic_pointer_cast<arrow::StringArray>(list_arr->values())) {
        out.reserve(end - start);
        for (auto i = start; i < end; ++i) out.push_back(values->GetString(i));
        return true;
    }
    if (auto values = std::dynamic_pointer_cast<arrow::LargeStringArray>(list_arr->values())) {
        out.reserve(end - start);
        for (auto i = start; i < end; ++i) out.push_back(values->GetString(i));
        return true;
    }
    throw std::runtime_error("Type mismatch: expected list<string> for parameter: " + std::string(name));
}

// Try ListArray then LargeListArray for string lists
std::vector<std::string> get_string_list_values(
    const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    std::vector<std::string> result;
    if (try_get_string_list_values<arrow::ListArray>(col, name, result))
        return result;
    if (try_get_string_list_values<arrow::LargeListArray>(col, name, result))
        return result;
    throw std::runtime_error("Type mismatch: expected list for parameter: " + std::string(name));
}

// Optional list extraction: returns nullopt on null instead of throwing.
template <typename ValueArrayType, typename T>
std::optional<std::vector<T>> get_optional_list_values(
    const std::shared_ptr<arrow::Array>& col, std::string_view name,
    const char* expected_type) {
    // Try ListArray
    if (auto list_arr = std::dynamic_pointer_cast<arrow::ListArray>(col)) {
        if (list_arr->IsNull(0)) return std::nullopt;
        auto values = std::dynamic_pointer_cast<ValueArrayType>(list_arr->values());
        if (!values) throw std::runtime_error(
            std::string("Type mismatch: expected list<") + expected_type + "> for parameter: " + std::string(name));
        auto start = list_arr->value_offset(0);
        auto end = list_arr->value_offset(1);
        std::vector<T> result;
        result.reserve(end - start);
        for (auto i = start; i < end; ++i) result.push_back(values->Value(i));
        return result;
    }
    // Try LargeListArray
    if (auto list_arr = std::dynamic_pointer_cast<arrow::LargeListArray>(col)) {
        if (list_arr->IsNull(0)) return std::nullopt;
        auto values = std::dynamic_pointer_cast<ValueArrayType>(list_arr->values());
        if (!values) throw std::runtime_error(
            std::string("Type mismatch: expected list<") + expected_type + "> for parameter: " + std::string(name));
        auto start = list_arr->value_offset(0);
        auto end = list_arr->value_offset(1);
        std::vector<T> result;
        result.reserve(end - start);
        for (auto i = start; i < end; ++i) result.push_back(values->Value(i));
        return result;
    }
    throw std::runtime_error("Type mismatch: expected list for parameter: " + std::string(name));
}

// Optional string list extraction: returns nullopt on null.
std::optional<std::vector<std::string>> get_optional_string_list_values(
    const std::shared_ptr<arrow::Array>& col, std::string_view name) {
    auto extract = [&](auto list_arr) -> std::optional<std::vector<std::string>> {
        if (list_arr->IsNull(0)) return std::nullopt;
        auto start = list_arr->value_offset(0);
        auto end = list_arr->value_offset(1);
        if (auto values = std::dynamic_pointer_cast<arrow::StringArray>(list_arr->values())) {
            std::vector<std::string> result;
            result.reserve(end - start);
            for (auto i = start; i < end; ++i) result.push_back(values->GetString(i));
            return result;
        }
        if (auto values = std::dynamic_pointer_cast<arrow::LargeStringArray>(list_arr->values())) {
            std::vector<std::string> result;
            result.reserve(end - start);
            for (auto i = start; i < end; ++i) result.push_back(values->GetString(i));
            return result;
        }
        throw std::runtime_error("Type mismatch: expected list<string> for parameter: " + std::string(name));
    };

    if (auto list_arr = std::dynamic_pointer_cast<arrow::ListArray>(col))
        return extract(list_arr);
    if (auto list_arr = std::dynamic_pointer_cast<arrow::LargeListArray>(col))
        return extract(list_arr);
    throw std::runtime_error("Type mismatch: expected list for parameter: " + std::string(name));
}

}  // anonymous namespace

// =========================================================================
// get<T>() specializations
// =========================================================================

template <>
double Request::get<double>(std::string_view name) const {
    return get_value<arrow::DoubleArray>(batch_, name);
}

template <>
float Request::get<float>(std::string_view name) const {
    return get_value<arrow::FloatArray>(batch_, name);
}

template <>
int64_t Request::get<int64_t>(std::string_view name) const {
    return get_value<arrow::Int64Array>(batch_, name);
}

template <>
int32_t Request::get<int32_t>(std::string_view name) const {
    return get_value<arrow::Int32Array>(batch_, name);
}

template <>
bool Request::get<bool>(std::string_view name) const {
    return get_value<arrow::BooleanArray>(batch_, name);
}

template <>
std::string Request::get<std::string>(std::string_view name) const {
    return extract_string(require_column(batch_, name), name);
}

template <>
std::vector<uint8_t> Request::get<std::vector<uint8_t>>(std::string_view name) const {
    return extract_binary(require_column(batch_, name), name);
}

// =========================================================================
// Complex type get<T>() — list types
// =========================================================================

template <>
std::vector<std::string> Request::get<std::vector<std::string>>(std::string_view name) const {
    return get_string_list_values(require_column(batch_, name), name);
}

template <>
std::vector<int64_t> Request::get<std::vector<int64_t>>(std::string_view name) const {
    return get_list_values<arrow::Int64Array, int64_t>(
        require_column(batch_, name), name, "int64");
}

template <>
std::vector<double> Request::get<std::vector<double>>(std::string_view name) const {
    return get_list_values<arrow::DoubleArray, double>(
        require_column(batch_, name), name, "double");
}

// =========================================================================
// get_optional<T>() specializations
// =========================================================================

template <>
std::optional<double> Request::get_optional<double>(std::string_view name) const {
    return get_optional_value<arrow::DoubleArray>(batch_, name);
}

template <>
std::optional<float> Request::get_optional<float>(std::string_view name) const {
    return get_optional_value<arrow::FloatArray>(batch_, name);
}

template <>
std::optional<int64_t> Request::get_optional<int64_t>(std::string_view name) const {
    return get_optional_value<arrow::Int64Array>(batch_, name);
}

template <>
std::optional<int32_t> Request::get_optional<int32_t>(std::string_view name) const {
    return get_optional_value<arrow::Int32Array>(batch_, name);
}

template <>
std::optional<bool> Request::get_optional<bool>(std::string_view name) const {
    return get_optional_value<arrow::BooleanArray>(batch_, name);
}

template <>
std::optional<std::string> Request::get_optional<std::string>(std::string_view name) const {
    auto col = batch_->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    return get_optional_string(col, name);
}

template <>
std::optional<std::vector<uint8_t>> Request::get_optional<std::vector<uint8_t>>(std::string_view name) const {
    auto col = batch_->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    return get_optional_binary(col, name);
}

// =========================================================================
// get_optional<T>() — list types
// =========================================================================

template <>
std::optional<std::vector<std::string>>
Request::get_optional<std::vector<std::string>>(std::string_view name) const {
    auto col = batch_->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    return get_optional_string_list_values(col, name);
}

template <>
std::optional<std::vector<int64_t>>
Request::get_optional<std::vector<int64_t>>(std::string_view name) const {
    auto col = batch_->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    return get_optional_list_values<arrow::Int64Array, int64_t>(col, name, "int64");
}

template <>
std::optional<std::vector<double>>
Request::get_optional<std::vector<double>>(std::string_view name) const {
    auto col = batch_->GetColumnByName(std::string(name));
    if (!col || col->length() == 0) return std::nullopt;
    return get_optional_list_values<arrow::DoubleArray, double>(col, name, "double");
}

}  // namespace vgi_rpc
