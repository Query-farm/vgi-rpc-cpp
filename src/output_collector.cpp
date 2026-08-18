// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/metadata.h"

#include <arrow/util/key_value_metadata.h>

#include <stdexcept>

namespace vgi_rpc {

OutputCollector::OutputCollector(std::shared_ptr<arrow::Schema> output_schema,
                                 bool producer_mode,
                                 const std::string& server_id,
                                 const std::string& request_id)
    : output_schema_(std::move(output_schema))
    , producer_mode_(producer_mode)
    , server_id_(server_id)
    , request_id_(request_id) {}

void OutputCollector::emit_batch(std::shared_ptr<arrow::RecordBatch> batch) {
    if (data_batch_idx_) {
        throw std::runtime_error("Only one data batch may be emitted per call");
    }
    if (!batch->schema()->Equals(*output_schema_)) {
        throw std::runtime_error("emit_batch: schema mismatch");
    }
    data_batch_idx_ = batches_.size();
    AnnotatedBatch ab;
    ab.batch = std::move(batch);
    ab.custom_metadata = nullptr;
    batches_.push_back(std::move(ab));
}

void OutputCollector::emit_batch(std::shared_ptr<arrow::RecordBatch> batch,
                                 std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    emit_batch(std::move(batch));
    // The delegated call either appended or threw, so the batch just appended
    // is the last one; attaching afterwards keeps the validation in the
    // single-argument overload rather than duplicating it here.
    if (metadata) {
        batches_.back().custom_metadata = std::move(metadata);
    }
}

void OutputCollector::emit_arrays(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
    int64_t num_rows = arrays.empty() ? 0 : arrays[0]->length();
    auto batch = arrow::RecordBatch::Make(output_schema_, num_rows, arrays);
    emit_batch(std::move(batch));
}

void OutputCollector::client_log(LogLevel level, std::string_view message) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, log_level_to_string(level));
    md->Append(keys::LOG_MESSAGE, std::string(message));

    if (!server_id_.empty()) {
        md->Append(keys::SERVER_ID, server_id_);
    }
    if (!request_id_.empty()) {
        md->Append(keys::REQUEST_ID, request_id_);
    }

    auto batch = make_empty_batch(output_schema_);
    AnnotatedBatch ab;
    ab.batch = std::move(batch);
    ab.custom_metadata = std::move(md);
    batches_.push_back(std::move(ab));
}

void OutputCollector::finish() {
    if (!producer_mode_) {
        throw std::runtime_error(
            "finish() is not allowed on exchange streams; "
            "exchange streams must emit exactly one data batch per call");
    }
    finished_ = true;
}

}  // namespace vgi_rpc
