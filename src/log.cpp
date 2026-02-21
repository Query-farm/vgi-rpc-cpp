#include "vgi_rpc/log.h"
#include <stdexcept>

namespace vgi_rpc {

const char* log_level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::TRACE:     return "TRACE";
        case LogLevel::DEBUG:     return "DEBUG";
        case LogLevel::INFO:      return "INFO";
        case LogLevel::WARN:      return "WARN";
        case LogLevel::ERROR:     return "ERROR";
        case LogLevel::EXCEPTION: return "EXCEPTION";
    }
    return "UNKNOWN";
}

LogLevel log_level_from_string(const std::string& s) {
    if (s == "TRACE")     return LogLevel::TRACE;
    if (s == "DEBUG")     return LogLevel::DEBUG;
    if (s == "INFO")      return LogLevel::INFO;
    if (s == "WARN")      return LogLevel::WARN;
    if (s == "ERROR")     return LogLevel::ERROR;
    if (s == "EXCEPTION") return LogLevel::EXCEPTION;
    throw std::runtime_error("Unknown log level: " + s);
}

}  // namespace vgi_rpc
