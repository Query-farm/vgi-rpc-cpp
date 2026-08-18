// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Typed read-only view over an incoming RPC request batch.
/// Wraps a single-row Arrow RecordBatch and its custom metadata, providing
/// get<T>(name) / get_optional<T>(name) for extracting typed parameters by
/// column name. Throws on type mismatch or missing required parameters.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

class VGI_RPC_EXPORT Request {
public:
    Request(std::shared_ptr<arrow::RecordBatch> batch,
            std::shared_ptr<arrow::KeyValueMetadata> metadata);

    std::string method_name() const;
    std::string request_id() const;
    std::string request_version() const;
    const std::shared_ptr<arrow::Schema>& schema() const;
    const std::shared_ptr<arrow::RecordBatch>& batch() const noexcept { return batch_; }
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata() const noexcept { return metadata_; }

    // Typed parameter access (reads column by name, row 0).
    // Throws on missing column, null value, or type mismatch.
    template <typename T>
    T get(std::string_view name) const;

    // Like get<T>() but returns nullopt when the column is missing or the value
    // is null. Throws only on type mismatch.
    template <typename T>
    std::optional<T> get_optional(std::string_view name) const;

    // Raw column accessor — returns nullptr if column not found.
    std::shared_ptr<arrow::Array> get_column(std::string_view name) const;

    bool has_param(std::string_view name) const;

private:
    std::shared_ptr<arrow::RecordBatch> batch_;
    std::shared_ptr<arrow::KeyValueMetadata> metadata_;
};

// Template specializations for get<T>() — defined in request.cpp
template <> double Request::get<double>(std::string_view name) const;
template <> float Request::get<float>(std::string_view name) const;
template <> int64_t Request::get<int64_t>(std::string_view name) const;
template <> int32_t Request::get<int32_t>(std::string_view name) const;
template <> bool Request::get<bool>(std::string_view name) const;
template <> std::string Request::get<std::string>(std::string_view name) const;
template <> std::vector<uint8_t> Request::get<std::vector<uint8_t>>(std::string_view name) const;

// Complex type get<T>() specializations — list types
template <> std::vector<std::string> Request::get<std::vector<std::string>>(std::string_view name) const;
template <> std::vector<int64_t> Request::get<std::vector<int64_t>>(std::string_view name) const;
template <> std::vector<double> Request::get<std::vector<double>>(std::string_view name) const;

// Template specializations for get_optional<T>()
template <> std::optional<double> Request::get_optional<double>(std::string_view name) const;
template <> std::optional<float> Request::get_optional<float>(std::string_view name) const;
template <> std::optional<int64_t> Request::get_optional<int64_t>(std::string_view name) const;
template <> std::optional<int32_t> Request::get_optional<int32_t>(std::string_view name) const;
template <> std::optional<bool> Request::get_optional<bool>(std::string_view name) const;
template <> std::optional<std::string> Request::get_optional<std::string>(std::string_view name) const;
template <> std::optional<std::vector<uint8_t>> Request::get_optional<std::vector<uint8_t>>(std::string_view name) const;

// Optional list type specializations
template <> std::optional<std::vector<std::string>> Request::get_optional<std::vector<std::string>>(std::string_view name) const;
template <> std::optional<std::vector<int64_t>> Request::get_optional<std::vector<int64_t>>(std::string_view name) const;
template <> std::optional<std::vector<double>> Request::get_optional<std::vector<double>>(std::string_view name) const;

}  // namespace vgi_rpc
