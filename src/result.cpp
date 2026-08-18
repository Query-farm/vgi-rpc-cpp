// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/result.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/log.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <nlohmann/json.hpp>

#include <stdexcept>
#include <vector>

namespace vgi_rpc {

Result Result::value(std::shared_ptr<arrow::RecordBatch> batch) {
    AnnotatedBatch ab;
    ab.batch = std::move(batch);
    ab.custom_metadata = nullptr;
    return Result(std::move(ab));
}

Result Result::from_annotated_batch(AnnotatedBatch ab) {
    return Result(std::move(ab));
}

Result Result::value(std::shared_ptr<arrow::Schema> schema,
                     std::vector<std::shared_ptr<arrow::Array>> arrays) {
    int64_t num_rows = arrays.empty() ? 0 : arrays[0]->length();
    auto batch = arrow::RecordBatch::Make(std::move(schema), num_rows,
                                          std::move(arrays));
    return Result::value(std::move(batch));
}

Result Result::void_result() {
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(empty_schema());
    ab.custom_metadata = nullptr;
    return Result(std::move(ab));
}

std::shared_ptr<arrow::KeyValueMetadata> make_error_metadata(
    const std::string& exception_type,
    const std::string& message,
    const std::string& server_id,
    const std::string& request_id,
    const std::string& error_kind) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, log_level_to_string(LogLevel::EXCEPTION));
    md->Append(keys::LOG_MESSAGE, message);

    nlohmann::json extra;
    extra["exception_type"] = exception_type;
    extra["exception_message"] = message;
    md->Append(keys::LOG_EXTRA, extra.dump());

    if (!server_id.empty()) {
        md->Append(keys::SERVER_ID, server_id);
    }
    if (!request_id.empty()) {
        md->Append(keys::REQUEST_ID, request_id);
    }
    if (!error_kind.empty()) {
        md->Append(keys::ERROR_KIND, error_kind);
    }
    return md;
}

Result Result::error(std::shared_ptr<arrow::Schema> schema,
                     const std::string& exception_type,
                     const std::string& message,
                     const std::string& server_id,
                     const std::string& request_id,
                     const std::string& error_kind) {
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(schema);
    ab.custom_metadata =
        make_error_metadata(exception_type, message, server_id, request_id, error_kind);
    return Result(std::move(ab));
}

const std::shared_ptr<arrow::Schema>& Result::schema() const {
    return batch_.batch->schema();
}

}  // namespace vgi_rpc
