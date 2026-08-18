// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <string>

namespace vgi_rpc {

// Initial-call parameters are an exact wire contract. Unlike exchange input,
// parameter batches are not cast or reordered: generated clients construct
// the declared schema verbatim, and accepting a different one can make a
// handler read the wrong column or silently ignore caller data.
inline std::string parameter_contract_error(const std::shared_ptr<arrow::RecordBatch>& batch,
                                            const std::shared_ptr<arrow::Schema>& expected) {
    if (expected->num_fields() > 0 && batch->num_rows() != 1) {
        return "Expected 1 row in request batch, got " + std::to_string(batch->num_rows());
    }

    const auto& actual = batch->schema();
    if (actual->num_fields() != expected->num_fields()) {
        return "Parameter schema mismatch: expected " + std::to_string(expected->num_fields()) +
               " fields, got " + std::to_string(actual->num_fields());
    }
    for (int i = 0; i < expected->num_fields(); ++i) {
        const auto& got = actual->field(i);
        const auto& want = expected->field(i);
        if (got->name() != want->name() || !got->type()->Equals(*want->type()) ||
            got->nullable() != want->nullable()) {
            return "Parameter schema mismatch at field " + std::to_string(i) + ": expected " +
                   want->ToString() + ", got " + got->ToString();
        }
    }
    return {};
}

}  // namespace vgi_rpc
