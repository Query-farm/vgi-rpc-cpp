#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <stdexcept>

namespace vgi_rpc {

LogSink::LogSink(std::string server_id, std::string request_id)
    : server_id_(std::move(server_id))
    , request_id_(std::move(request_id)) {}

void LogSink::emit(LogLevel level, std::string_view message,
                   const nlohmann::json& extra) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, log_level_to_string(level));
    md->Append(keys::LOG_MESSAGE, std::string(message));

    if (!extra.is_null() && !extra.empty()) {
        md->Append(keys::LOG_EXTRA, extra.dump());
    }

    if (!server_id_.empty()) {
        md->Append(keys::SERVER_ID, server_id_);
    }
    if (!request_id_.empty()) {
        md->Append(keys::REQUEST_ID, request_id_);
    }

    buffered_.push_back(std::move(md));
}

void LogSink::emit(const Message& msg) {
    emit(msg.level, msg.message, msg.extra);
}

std::vector<AnnotatedBatch> LogSink::flush(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<AnnotatedBatch> result;
    result.reserve(buffered_.size());

    auto empty_batch = make_empty_batch(schema);

    for (auto& md : buffered_) {
        AnnotatedBatch ab;
        ab.batch = empty_batch;
        ab.custom_metadata = std::move(md);
        result.push_back(std::move(ab));
    }
    buffered_.clear();
    return result;
}

}  // namespace vgi_rpc
