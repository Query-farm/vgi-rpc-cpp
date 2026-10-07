// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/access_log.h"
#include "vgi_rpc/errors.h"

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
                                 int64_t max_record_bytes)
    : server_id_(std::move(server_id)),
      max_record_bytes_(max_record_bytes > 0 ? max_record_bytes : kDefaultMaxRecordBytes) {
    if (path.empty()) return;
    out_.open(path, std::ios::out | std::ios::app);
    enabled_ = out_.is_open();
}

void AccessLogWriter::emit(const AccessRecord& rec) {
    if (!enabled_) return;
    std::lock_guard<std::mutex> lock(mutex_);

    nlohmann::json j;
    j["timestamp"] = utc_timestamp_ms();
    j["level"] = "INFO";
    j["logger"] = "vgi_rpc.access";
    j["message"] = rec.protocol + "." + rec.method + (rec.status == "ok" ? " ok" : " error");
    j["server_id"] = server_id_;
    // The owning binding's, never a server-wide default -- see AccessRecord.
    j["protocol"] = rec.protocol;
    j["protocol_hash"] = rec.protocol_hash;
    j["method"] = rec.method;
    j["method_type"] = rec.is_stream ? "stream" : "unary";
    j["principal"] = rec.principal;
    j["auth_domain"] = rec.auth_domain;
    j["authenticated"] = rec.authenticated;
    j["remote_addr"] = "";
    j["duration_ms"] = std::round(rec.duration_ms * 100.0) / 100.0;
    j["status"] = rec.status;
    j["error_type"] = rec.error_type;
    if (rec.status == "error") {
        j["error_message"] = rec.error_message.empty() ? std::string("error") : rec.error_message;
        // What an operator alerts on ("page on UNAVAILABLE"): the code the
        // client received.  A site that did not record one is classified the
        // way the wire would have been, from its type; absent on success.
        j["error_code"] = rec.error_code.empty()
                              ? std::string(code_name(default_error_code(rec.error_type, "")))
                              : rec.error_code;
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
    // The request's shape and the state tokens' sizes -- never a payload
    // value, at any level (docs/access-log-spec.md §4.3, §4.4).
    if (rec.request_rows >= 0) {
        nlohmann::json fields = nlohmann::json::array();
        for (const auto& field : rec.request_fields) {
            nlohmann::json entry = nlohmann::json::object();
            entry["name"] = field.name;
            entry["type"] = field.type;
            fields.push_back(std::move(entry));
        }
        j["request_fields"] = std::move(fields);
        j["request_rows"] = rec.request_rows;
    }
    if (!rec.is_stream) {
        // Transitional: vgi-rpc 0.50.0's schema requires `request_data` on a
        // unary record unless it is marked truncated, and the current schema
        // accepts "payload_omitted" as legacy -- so this marker passes both.
        // Nothing is lost: payloads are never logged.  Remove once CI
        // validates against vgi-rpc >= 0.50.1.
        j["truncated"] = "payload_omitted";
    }
    if (rec.request_state_bytes >= 0) j["request_state_bytes"] = rec.request_state_bytes;
    if (rec.response_state_bytes >= 0) j["response_state_bytes"] = rec.response_state_bytes;

    // §5b truncation ladder.  There is no payload to shed; rung 1 (claims) has
    // no C++ analogue yet since this emitter carries no claims, so an over-cap
    // record goes straight to the sentinel form.
    std::string line = j.dump();
    if (static_cast<int64_t>(line.size()) > max_record_bytes_) {
        // Sentinel form: envelope fields plus error_message, which is never
        // truncated — an operator debugging a failure needs it whole.
        nlohmann::json s;
        for (const char* key : kEnvelopeKeys) {
            if (j.contains(key)) s[key] = j[key];
        }
        if (rec.status == "error") {
            s["error_message"] = j["error_message"];
            s["error_code"] = j["error_code"];
        }
        s["truncated"] = "record_too_large";
        line = s.dump();
    }

    out_ << line << '\n';
    out_.flush();
}

}  // namespace vgi_rpc
