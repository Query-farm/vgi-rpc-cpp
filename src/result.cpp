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

Result Result::error(std::shared_ptr<arrow::Schema> schema,
                     const std::string& exception_type,
                     const std::string& message,
                     const std::string& server_id,
                     const std::string& request_id) {
    auto batch = make_empty_batch(schema);

    // Build error metadata
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

    AnnotatedBatch ab;
    ab.batch = std::move(batch);
    ab.custom_metadata = std::move(md);
    return Result(std::move(ab));
}

const std::shared_ptr<arrow::Schema>& Result::schema() const {
    return batch_.batch->schema();
}

}  // namespace vgi_rpc
