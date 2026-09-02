// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Accumulates output batches and log messages during a single stream tick.
/// Created per-tick by the stream loop; use emit_batch()/emit_arrays() to
/// produce data, client_log() for in-band logs, and finish() to signal
/// stream completion (producer mode only).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/log.h"

namespace vgi_rpc {

// NOT thread-safe.  One OutputCollector is created per stream tick/exchange.
class VGI_RPC_EXPORT OutputCollector {
public:
    OutputCollector(std::shared_ptr<arrow::Schema> output_schema, bool producer_mode,
                    const std::string& server_id = "", const std::string& request_id = "",
                    std::optional<int64_t> response_limit_bytes = std::nullopt,
                    std::optional<int64_t> preferred_response_bytes = std::nullopt);

    // Emit a pre-built data batch
    void emit_batch(std::shared_ptr<arrow::RecordBatch> batch);

    // Emit a data batch carrying per-batch custom metadata.
    //
    // Distinct from schema metadata, which describes the shape: this describes
    // *this batch*, which is what an application protocol needs when a batch
    // says something about itself — a cache advertisement, a row-provenance
    // map, a batch index.
    void emit_batch(std::shared_ptr<arrow::RecordBatch> batch,
                    std::shared_ptr<arrow::KeyValueMetadata> metadata);

    // Emit a data batch from arrays
    void emit_arrays(const std::vector<std::shared_ptr<arrow::Array>>& arrays);

    // Emit a log message
    void client_log(LogLevel level, std::string_view message);

    // Signal stream completion (producer mode only)
    void finish();

    bool is_finished() const noexcept { return finished_; }

    // Get all accumulated batches (logs + data)
    const std::vector<AnnotatedBatch>& batches() const noexcept { return batches_; }

    const std::shared_ptr<arrow::Schema>& output_schema() const noexcept { return output_schema_; }
    std::optional<int64_t> response_limit_bytes() const noexcept { return response_limit_bytes_; }
    std::optional<int64_t> preferred_response_bytes() const noexcept {
        return preferred_response_bytes_;
    }

private:
    std::shared_ptr<arrow::Schema> output_schema_;
    bool producer_mode_;
    bool finished_ = false;
    std::string server_id_;
    std::string request_id_;
    std::vector<AnnotatedBatch> batches_;
    std::optional<size_t> data_batch_idx_;
    std::optional<int64_t> response_limit_bytes_;
    std::optional<int64_t> preferred_response_bytes_;
};

}  // namespace vgi_rpc
