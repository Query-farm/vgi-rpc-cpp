#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "vgi_rpc/export.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/log_sink.h"

namespace vgi_rpc {

class VGI_RPC_EXPORT CallContext {
public:
    CallContext(std::shared_ptr<LogSink> sink,
                std::string server_id,
                std::string request_id);

    void client_log(LogLevel level, std::string_view message,
                    const nlohmann::json& extra = {});
    void client_log(const Message& msg);

    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& request_id() const noexcept { return request_id_; }

    std::shared_ptr<LogSink> log_sink() const noexcept { return sink_; }

private:
    std::shared_ptr<LogSink> sink_;
    std::string server_id_;
    std::string request_id_;
};

}  // namespace vgi_rpc
