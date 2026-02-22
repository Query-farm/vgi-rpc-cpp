// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/metadata.h"
#include "vgi_rpc/arrow_utils.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

#include <random>

namespace vgi_rpc {

const std::shared_ptr<arrow::Schema>& empty_schema() {
    static const auto schema = arrow::schema({});
    return schema;
}

std::shared_ptr<arrow::RecordBatch> make_empty_batch(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); ++i) {
        arrays.push_back(
            unwrap(arrow::MakeEmptyArray(schema->field(i)->type()),
                   "make_empty_batch"));
    }
    return arrow::RecordBatch::Make(schema, 0, std::move(arrays));
}

std::string random_hex(size_t length) {
    static constexpr char hex_chars[] = "0123456789abcdef";
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, 15);
    std::string result(length, '\0');
    for (size_t i = 0; i < length; ++i) {
        result[i] = hex_chars[dist(gen)];
    }
    return result;
}

std::string get_metadata_value(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
    const std::string& key,
    const std::string& default_value) {
    if (!metadata) return default_value;
    auto idx = metadata->FindKey(key);
    if (idx < 0) return default_value;
    return metadata->value(idx);
}

}  // namespace vgi_rpc
