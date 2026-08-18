// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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

    // Fold `md` into whatever this batch already carries, `md` winning on a
    // key conflict. A transport that stamps its own key on an outgoing batch
    // must merge rather than assign: the handler may have attached metadata of
    // its own, and overwriting it drops it silently.
    void merge_metadata(const std::shared_ptr<arrow::KeyValueMetadata>& md) {
        if (!md) return;
        if (!custom_metadata) {
            custom_metadata = md->Copy();
            return;
        }
        auto merged = custom_metadata->Copy();
        for (int64_t i = 0; i < md->size(); ++i) {
            if (const auto index = merged->FindKey(md->key(i)); index >= 0) {
                (void)merged->Delete(index);
            }
            merged->Append(md->key(i), md->value(i));
        }
        custom_metadata = std::move(merged);
    }

    // Classify this batch (delegates to classify_batch in wire.h).
    // Defined in wire.cpp to avoid circular include.
    BatchType type() const;
};

}  // namespace vgi_rpc
