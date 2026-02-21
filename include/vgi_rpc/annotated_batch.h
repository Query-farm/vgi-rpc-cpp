/// A RecordBatch paired with optional per-batch custom metadata.
/// AnnotatedBatch is the primary unit passed through the IPC wire protocol —
/// use the static factories data() and with_metadata() to construct, and
/// type() to classify a received batch (DATA, LOG, ERROR, etc.).
#pragma once

#include <memory>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

namespace vgi_rpc {

enum class BatchType;  // forward-declared; defined in wire.h

struct AnnotatedBatch {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::KeyValueMetadata> custom_metadata;

    // Convenience factories
    static AnnotatedBatch data(std::shared_ptr<arrow::RecordBatch> b) {
        return {std::move(b), nullptr};
    }

    static AnnotatedBatch with_metadata(
        std::shared_ptr<arrow::RecordBatch> b,
        std::shared_ptr<arrow::KeyValueMetadata> md) {
        return {std::move(b), std::move(md)};
    }

    // Classify this batch (delegates to classify_batch in wire.h).
    // Defined in wire.cpp to avoid circular include.
    BatchType type() const;
};

}  // namespace vgi_rpc
