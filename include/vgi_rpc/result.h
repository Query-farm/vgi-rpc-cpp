#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

class VGI_RPC_EXPORT Result {
public:
    // Create a result with a pre-built batch (1 row for values, 0 rows for void/error)
    static Result value(std::shared_ptr<arrow::RecordBatch> batch);

    // Create a result from an AnnotatedBatch (batch + custom metadata)
    static Result from_annotated_batch(AnnotatedBatch ab);

    // Create a result from schema + arrays (builds a 1-row batch)
    static Result value(std::shared_ptr<arrow::Schema> schema,
                        std::vector<std::shared_ptr<arrow::Array>> arrays);

    // Void result (0-row batch on empty schema)
    static Result void_result();

    // Error result (0-row batch with EXCEPTION metadata)
    static Result error(std::shared_ptr<arrow::Schema> schema,
                        const std::string& exception_type,
                        const std::string& message,
                        const std::string& server_id = "",
                        const std::string& request_id = "");

    const AnnotatedBatch& annotated_batch() const noexcept { return batch_; }
    const std::shared_ptr<arrow::Schema>& schema() const;

private:
    explicit Result(AnnotatedBatch batch) : batch_(std::move(batch)) {}
    AnnotatedBatch batch_;
};

}  // namespace vgi_rpc
