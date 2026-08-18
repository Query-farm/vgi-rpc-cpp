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
                     (static_cast<uint32_t>(data[i + 1]) << 8) | static_cast<uint32_t>(data[i + 2]);
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
        uint32_t n =
            (static_cast<uint32_t>(data[i]) << 16) | (static_cast<uint32_t>(data[i + 1]) << 8);
        out.push_back(tbl[(n >> 18) & 0x3F]);
        out.push_back(tbl[(n >> 12) & 0x3F]);
        out.push_back(tbl[(n >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

int64_t base64_encoded_length(int64_t len) {
    if (len <= 0) return 0;
    return ((len + 2) / 3) * 4;
}

namespace {

// Envelope keys that survive every rung of the truncation ladder: the four
// framing fields plus the twelve always-required ones.  §5b's sentinel form
// keeps exactly these (plus error_message when the call failed).
const char* const kEnvelopeKeys[] = {
    "timestamp",     "level",       "logger",      "message",    "server_id",   "protocol",
    "protocol_hash", "method",      "method_type", "principal",  "auth_domain", "authenticated",
    "remote_addr",   "duration_ms", "status",      "error_type",
};

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

AccessLogWriter::AccessLogWriter(const std::string& path, std::string server_id,
                                 std::string protocol_name, std::string protocol_hash,
                                 int64_t max_record_bytes)
    : server_id_(std::move(server_id)),
      protocol_name_(std::move(protocol_name)),
      protocol_hash_(std::move(protocol_hash)),
      max_record_bytes_(max_record_bytes > 0 ? max_record_bytes : kDefaultMaxRecordBytes) {
    if (path.empty()) return;
    out_.open(path, std::ios::out | std::ios::app);
    enabled_ = out_.is_open();
}

bool AccessLogWriter::payload_fits(int64_t b64_len) const noexcept {
    return b64_len < max_record_bytes_;
}

void AccessLogWriter::emit(const AccessRecord& rec) {
    if (!enabled_) return;

    nlohmann::json j;
    j["timestamp"] = utc_timestamp_ms();
    j["level"] = "INFO";
    j["logger"] = "vgi_rpc.access";
    j["message"] = protocol_name_ + "." + rec.method + (rec.status == "ok" ? " ok" : " error");
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
        j["error_message"] = rec.error_message.empty() ? std::string("error") : rec.error_message;
    }
    if (!rec.request_id.empty()) {
        // Must equal the X-Request-ID the response carried.  An id that
        // appears on the response but differs in the log is worse than none:
        // it looks like a working trail right up to the moment someone
        // tries to follow it.
        j["request_id"] = rec.request_id;
    }
    if (rec.is_stream) {
        j["stream_id"] = rec.stream_id;
        if (rec.cancelled) j["cancelled"] = true;
    }
    if (rec.has_request_data) {
        j["request_data"] = rec.request_data_b64;
    } else if (rec.original_request_bytes >= 0) {
        // The caller already declined to materialize an over-cap payload;
        // report what was dropped, per rung 1 of the §5b ladder.
        j["original_request_bytes"] = rec.original_request_bytes;
        j["truncated"] = true;
    }

    // §5b truncation ladder.  Rung 1 (drop request_data) is applied here for
    // payloads the caller did materialize; rung 2 (claims) has no C++ analogue
    // yet since this emitter carries no claims; rung 3 is the sentinel form.
    std::string line = j.dump();
    if (static_cast<int64_t>(line.size()) > max_record_bytes_ && j.contains("request_data")) {
        j["original_request_bytes"] = static_cast<int64_t>(rec.request_data_b64.size());
        j.erase("request_data");
        j["truncated"] = true;
        line = j.dump();
    }
    if (static_cast<int64_t>(line.size()) > max_record_bytes_) {
        // Sentinel form: envelope fields plus error_message, which is never
        // truncated — an operator debugging a failure needs it whole.
        nlohmann::json s;
        for (const char* key : kEnvelopeKeys) {
            if (j.contains(key)) s[key] = j[key];
        }
        if (rec.status == "error") s["error_message"] = j["error_message"];
        s["truncated"] = "record_too_large";
        line = s.dump();
    }

    out_ << line << '\n';
    out_.flush();
}

}  // namespace vgi_rpc
