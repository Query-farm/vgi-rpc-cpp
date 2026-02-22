// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/call_context.h"

namespace vgi_rpc {

CallContext::CallContext(std::shared_ptr<LogSink> sink,
                         std::string server_id,
                         std::string request_id)
    : sink_(std::move(sink))
    , server_id_(std::move(server_id))
    , request_id_(std::move(request_id)) {}

void CallContext::client_log(LogLevel level, std::string_view message,
                             const nlohmann::json& extra) {
    sink_->emit(level, message, extra);
}

void CallContext::client_log(const Message& msg) {
    sink_->emit(msg);
}

}  // namespace vgi_rpc
