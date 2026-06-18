// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/access_log.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cmath>
#include <ctime>
#include <ios>

namespace vgi_rpc {

std::string base64_encode(const uint8_t* data, size_t len) {
    static constexpr char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 3 <= len; i += 3) {
        uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                     (static_cast<uint32_t>(data[i + 1]) << 8) |
                     static_cast<uint32_t>(data[i + 2]);
        out.push_back(tbl[(n >> 18) & 0x3F]);
        out.push_back(tbl[(n >> 12) & 0x3F]);
        out.push_back(tbl[(n >> 6) & 0x3F]);
        out.push_back(tbl[n & 0x3F]);
    }
    if (i + 1 == len) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        out.push_back(tbl[(n >> 18) & 0x3F]);
        out.push_back(tbl[(n >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (i + 2 == len) {
        uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                     (static_cast<uint32_t>(data[i + 1]) << 8);
        out.push_back(tbl[(n >> 18) & 0x3F]);
        out.push_back(tbl[(n >> 12) & 0x3F]);
        out.push_back(tbl[(n >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

namespace {

// RFC 3339 UTC with millisecond precision: YYYY-MM-DDTHH:MM:SS.sssZ
std::string utc_timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto since_epoch = now.time_since_epoch();
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch - secs);
    std::time_t t = static_cast<std::time_t>(secs.count());
    std::tm tm_utc{};
#ifdef _WIN32
    gmtime_s(&tm_utc, &t);
#else
    gmtime_r(&t, &tm_utc);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);
    char out[40];
    std::snprintf(out, sizeof(out), "%s.%03dZ", buf, static_cast<int>(ms.count()));
    return std::string(out);
}

}  // namespace

AccessLogWriter::AccessLogWriter(const std::string& path,
                                 std::string server_id,
                                 std::string protocol_name,
                                 std::string protocol_hash)
    : server_id_(std::move(server_id))
    , protocol_name_(std::move(protocol_name))
    , protocol_hash_(std::move(protocol_hash)) {
    if (path.empty()) return;
    out_.open(path, std::ios::out | std::ios::app);
    enabled_ = out_.is_open();
}

void AccessLogWriter::emit(const AccessRecord& rec) {
    if (!enabled_) return;

    nlohmann::json j;
    j["timestamp"] = utc_timestamp_ms();
    j["level"] = "INFO";
    j["logger"] = "vgi_rpc.access";
    j["message"] =
        protocol_name_ + "." + rec.method + (rec.status == "ok" ? " ok" : " error");
    j["server_id"] = server_id_;
    j["protocol"] = protocol_name_;
    j["protocol_hash"] = protocol_hash_;
    j["method"] = rec.method;
    j["method_type"] = rec.is_stream ? "stream" : "unary";
    j["principal"] = "";
    j["auth_domain"] = "";
    j["authenticated"] = false;
    j["remote_addr"] = "";
    j["duration_ms"] = std::round(rec.duration_ms * 100.0) / 100.0;
    j["status"] = rec.status;
    j["error_type"] = rec.error_type;
    if (rec.status == "error") {
        j["error_message"] =
            rec.error_message.empty() ? std::string("error") : rec.error_message;
    }
    if (rec.is_stream) {
        j["stream_id"] = rec.stream_id;
        if (rec.cancelled) j["cancelled"] = true;
    }
    if (rec.has_request_data) {
        j["request_data"] = rec.request_data_b64;
    }

    out_ << j.dump() << '\n';
    out_.flush();
}

}  // namespace vgi_rpc
