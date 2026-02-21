/// Buffered collector for log messages emitted during request handling.
/// Messages are accumulated via emit() and flushed as AnnotatedBatch vectors
/// (zero-row batches with log metadata) to be sent ahead of the result.
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/type_fwd.h>
#include <nlohmann/json.hpp>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/log.h"

namespace vgi_rpc {

// NOT thread-safe.  Assumed to be used within a single request's scope.
class VGI_RPC_EXPORT LogSink {
public:
    LogSink(std::string server_id, std::string request_id);

    void emit(LogLevel level, std::string_view message,
              const nlohmann::json& extra = {});

    void emit(const Message& msg);

    std::vector<AnnotatedBatch> flush(const std::shared_ptr<arrow::Schema>& schema);

private:
    std::string server_id_;
    std::string request_id_;
    std::vector<std::shared_ptr<arrow::KeyValueMetadata>> buffered_;
};

}  // namespace vgi_rpc
