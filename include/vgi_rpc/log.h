/// Log severity levels and the Message struct for client-visible logging.
/// Levels range from TRACE to EXCEPTION; EXCEPTION signals an error response.
/// Use log_level_to_string/log_level_from_string for wire-format conversion.
#pragma once

#include <string>
#include <nlohmann/json.hpp>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    EXCEPTION
};

VGI_RPC_EXPORT const char* log_level_to_string(LogLevel level);
VGI_RPC_EXPORT LogLevel log_level_from_string(const std::string& s);

struct Message {
    LogLevel level;
    std::string message;
    nlohmann::json extra = {};
};

}  // namespace vgi_rpc
