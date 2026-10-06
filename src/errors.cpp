// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/errors.h"

#include <arrow/util/key_value_metadata.h>

#include <array>
#include <cmath>
#include <set>
#include <utility>

namespace vgi_rpc {

// ---------------------------------------------------------------------------
// Codes
// ---------------------------------------------------------------------------

namespace {

struct CodeEntry {
    Code code;
    const char* name;
};

constexpr std::array<CodeEntry, 16> kCodes{{
    {Code::CANCELLED, "CANCELLED"},
    {Code::UNKNOWN, "UNKNOWN"},
    {Code::INVALID_ARGUMENT, "INVALID_ARGUMENT"},
    {Code::DEADLINE_EXCEEDED, "DEADLINE_EXCEEDED"},
    {Code::NOT_FOUND, "NOT_FOUND"},
    {Code::ALREADY_EXISTS, "ALREADY_EXISTS"},
    {Code::PERMISSION_DENIED, "PERMISSION_DENIED"},
    {Code::RESOURCE_EXHAUSTED, "RESOURCE_EXHAUSTED"},
    {Code::FAILED_PRECONDITION, "FAILED_PRECONDITION"},
    {Code::ABORTED, "ABORTED"},
    {Code::OUT_OF_RANGE, "OUT_OF_RANGE"},
    {Code::UNIMPLEMENTED, "UNIMPLEMENTED"},
    {Code::INTERNAL, "INTERNAL"},
    {Code::UNAVAILABLE, "UNAVAILABLE"},
    {Code::DATA_LOSS, "DATA_LOSS"},
    {Code::UNAUTHENTICATED, "UNAUTHENTICATED"},
}};

}  // namespace

const char* code_name(Code code) noexcept {
    for (const auto& entry : kCodes) {
        if (entry.code == code) return entry.name;
    }
    return "UNKNOWN";
}

std::optional<Code> code_from_name(std::string_view name) noexcept {
    for (const auto& entry : kCodes) {
        if (name == entry.name) return entry.code;
    }
    return std::nullopt;
}

Code parse_code(std::string_view name) noexcept {
    return code_from_name(name).value_or(Code::UNKNOWN);
}

Code default_error_code(std::string_view exception_type, std::string_view kind) noexcept {
    // The kind table first: the kind is the classification, and it names
    // exactly one code (WIRE_PROTOCOL.md §8, §16).
    struct KindEntry {
        const char* kind;
        Code code;
    };
    static constexpr KindEntry kKinds[] = {
        {"method_not_implemented", Code::UNIMPLEMENTED},
        {"protocol_not_supported", Code::UNIMPLEMENTED},
        {"protocol_not_specified", Code::INVALID_ARGUMENT},
        {"protocol_version_mismatch", Code::FAILED_PRECONDITION},
        {"session_lost", Code::ABORTED},
        {"server_draining", Code::UNAVAILABLE},
        {"identity_unavailable", Code::UNAVAILABLE},
        {"stale_auth", Code::UNAUTHENTICATED},
        {"introspection_refused", Code::PERMISSION_DENIED},
        {"grant_refused", Code::PERMISSION_DENIED},
        {"token_unresolved", Code::NOT_FOUND},
    };
    if (!kind.empty()) {
        for (const auto& entry : kKinds) {
            if (kind == entry.kind) return entry.code;
        }
        return Code::UNKNOWN;
    }
    // Framework error types that carry no kind but are still classified.
    // A response over the cap fails again against the same limits, so it is
    // RESOURCE_EXHAUSTED *without* RetryInfo: not retryable.
    if (exception_type == "ResponseTooLargeError") return Code::RESOURCE_EXHAUSTED;
    if (exception_type == "AuthUnavailableError") return Code::UNAVAILABLE;
    return Code::UNKNOWN;
}

// ---------------------------------------------------------------------------
// The detail catalog
// ---------------------------------------------------------------------------

namespace {

using json = nlohmann::json;

// String fields absent from an object read as ""; present, they must be strings.
bool read_str(const json& obj, const char* key, std::string& out) {
    auto it = obj.find(key);
    if (it == obj.end()) {
        out.clear();
        return true;
    }
    if (!it->is_string()) return false;
    out = it->get<std::string>();
    return true;
}

// Arrays absent from an object read as []; present, they must be arrays of objects.
bool read_objects(const json& obj, const char* key, const json*& out) {
    static const json empty = json::array();
    auto it = obj.find(key);
    if (it == obj.end()) {
        out = &empty;
        return true;
    }
    if (!it->is_array()) return false;
    for (const auto& item : *it) {
        if (!item.is_object()) return false;
    }
    out = &*it;
    return true;
}

json delay_value(double seconds) {
    // A whole number travels as an integer so the common case reads the same
    // in every language's JSON encoder ("7", not "7.0").
    if (std::isfinite(seconds) && std::floor(seconds) == seconds && std::fabs(seconds) < 9.0e15) {
        return json(static_cast<int64_t>(seconds));
    }
    return json(seconds);
}

}  // namespace

json ErrorInfo::to_json() const {
    json meta = json::object();
    for (const auto& [k, v] : metadata) meta[k] = v;
    return json{{"@type", TYPE}, {"metadata", std::move(meta)}};
}

std::optional<ErrorInfo> ErrorInfo::from_json(const json& obj) {
    ErrorInfo out;
    auto it = obj.find("metadata");
    if (it == obj.end()) return out;
    if (!it->is_object()) return std::nullopt;
    for (const auto& [k, v] : it->items()) {
        if (!v.is_string()) return std::nullopt;
        out.metadata[k] = v.get<std::string>();
    }
    return out;
}

json RetryInfo::to_json() const {
    return json{{"@type", TYPE}, {"retry_delay_seconds", delay_value(retry_delay_seconds)}};
}

std::optional<RetryInfo> RetryInfo::from_json(const json& obj) {
    auto it = obj.find("retry_delay_seconds");
    if (it == obj.end() || !it->is_number()) return std::nullopt;
    const double value = it->get<double>();
    if (!std::isfinite(value) || value < 0) return std::nullopt;
    return RetryInfo{value};
}

json BadRequest::to_json() const {
    json items = json::array();
    for (const auto& v : field_violations) {
        items.push_back(json{{"field", v.field}, {"description", v.description}});
    }
    return json{{"@type", TYPE}, {"field_violations", std::move(items)}};
}

std::optional<BadRequest> BadRequest::from_json(const json& obj) {
    const json* items = nullptr;
    if (!read_objects(obj, "field_violations", items)) return std::nullopt;
    BadRequest out;
    for (const auto& item : *items) {
        FieldViolation v;
        if (!read_str(item, "field", v.field) || !read_str(item, "description", v.description)) {
            return std::nullopt;
        }
        out.field_violations.push_back(std::move(v));
    }
    return out;
}

json PreconditionFailure::to_json() const {
    json items = json::array();
    for (const auto& v : violations) {
        items.push_back(
            json{{"type", v.type}, {"subject", v.subject}, {"description", v.description}});
    }
    return json{{"@type", TYPE}, {"violations", std::move(items)}};
}

std::optional<PreconditionFailure> PreconditionFailure::from_json(const json& obj) {
    const json* items = nullptr;
    if (!read_objects(obj, "violations", items)) return std::nullopt;
    PreconditionFailure out;
    for (const auto& item : *items) {
        PreconditionViolation v;
        if (!read_str(item, "type", v.type) || !read_str(item, "subject", v.subject) ||
            !read_str(item, "description", v.description)) {
            return std::nullopt;
        }
        out.violations.push_back(std::move(v));
    }
    return out;
}

json QuotaFailure::to_json() const {
    json items = json::array();
    for (const auto& v : violations) {
        items.push_back(json{{"subject", v.subject}, {"description", v.description}});
    }
    return json{{"@type", TYPE}, {"violations", std::move(items)}};
}

std::optional<QuotaFailure> QuotaFailure::from_json(const json& obj) {
    const json* items = nullptr;
    if (!read_objects(obj, "violations", items)) return std::nullopt;
    QuotaFailure out;
    for (const auto& item : *items) {
        QuotaViolation v;
        if (!read_str(item, "subject", v.subject) ||
            !read_str(item, "description", v.description)) {
            return std::nullopt;
        }
        out.violations.push_back(std::move(v));
    }
    return out;
}

json ResourceInfo::to_json() const {
    return json{{"@type", TYPE},
                {"resource_type", resource_type},
                {"resource_name", resource_name},
                {"owner", owner},
                {"description", description}};
}

std::optional<ResourceInfo> ResourceInfo::from_json(const json& obj) {
    ResourceInfo out;
    if (!read_str(obj, "resource_type", out.resource_type) ||
        !read_str(obj, "resource_name", out.resource_name) || !read_str(obj, "owner", out.owner) ||
        !read_str(obj, "description", out.description)) {
        return std::nullopt;
    }
    return out;
}

json Help::to_json() const {
    json items = json::array();
    for (const auto& v : links) {
        items.push_back(json{{"description", v.description}, {"url", v.url}});
    }
    return json{{"@type", TYPE}, {"links", std::move(items)}};
}

std::optional<Help> Help::from_json(const json& obj) {
    const json* items = nullptr;
    if (!read_objects(obj, "links", items)) return std::nullopt;
    Help out;
    for (const auto& item : *items) {
        HelpLink v;
        if (!read_str(item, "description", v.description) || !read_str(item, "url", v.url)) {
            return std::nullopt;
        }
        out.links.push_back(std::move(v));
    }
    return out;
}

json LocalizedMessage::to_json() const {
    return json{{"@type", TYPE}, {"locale", locale}, {"message", message}};
}

std::optional<LocalizedMessage> LocalizedMessage::from_json(const json& obj) {
    LocalizedMessage out;
    if (!read_str(obj, "locale", out.locale) || !read_str(obj, "message", out.message)) {
        return std::nullopt;
    }
    return out;
}

json detail_to_json(const ErrorDetail& detail) {
    return std::visit([](const auto& d) { return d.to_json(); }, detail);
}

namespace {

template <typename T>
std::optional<ErrorDetail> parse_as(const json& obj) {
    if (auto parsed = T::from_json(obj)) return ErrorDetail{std::move(*parsed)};
    return std::nullopt;
}

bool in_catalog(const std::string& type) {
    return type == ErrorInfo::TYPE || type == RetryInfo::TYPE || type == BadRequest::TYPE ||
           type == PreconditionFailure::TYPE || type == QuotaFailure::TYPE ||
           type == ResourceInfo::TYPE || type == Help::TYPE || type == LocalizedMessage::TYPE;
}

}  // namespace

std::optional<ErrorDetail> parse_error_detail(const json& obj) {
    if (!obj.is_object()) return std::nullopt;
    auto it = obj.find("@type");
    if (it == obj.end() || !it->is_string()) return std::nullopt;
    const auto type = it->get<std::string>();
    try {
        if (type == ErrorInfo::TYPE) return parse_as<ErrorInfo>(obj);
        if (type == RetryInfo::TYPE) return parse_as<RetryInfo>(obj);
        if (type == BadRequest::TYPE) return parse_as<BadRequest>(obj);
        if (type == PreconditionFailure::TYPE) return parse_as<PreconditionFailure>(obj);
        if (type == QuotaFailure::TYPE) return parse_as<QuotaFailure>(obj);
        if (type == ResourceInfo::TYPE) return parse_as<ResourceInfo>(obj);
        if (type == Help::TYPE) return parse_as<Help>(obj);
        if (type == LocalizedMessage::TYPE) return parse_as<LocalizedMessage>(obj);
    } catch (const json::exception&) {
        return std::nullopt;
    }
    return std::nullopt;
}

bool error_details_valid(const json& details) {
    if (!details.is_array()) return false;
    std::set<std::string> seen;
    for (const auto& obj : details) {
        if (!obj.is_object()) return false;
        auto it = obj.find("@type");
        if (it == obj.end() || !it->is_string()) return false;
        const auto type = it->get<std::string>();
        if (type.empty()) return false;
        // Each type at most once, as AIP-193 requires.
        if (!seen.insert(type).second) return false;
        // The reserved space holds the catalog and nothing else: a protocol's
        // own detail types live under the protocol's own name.
        if (type.rfind(kReservedProtocolPrefix, 0) == 0 && !in_catalog(type)) return false;
        if (type.find('.') == std::string::npos) return false;
    }
    return true;
}

std::optional<std::string> encode_error_details(const json& details) {
    if (!details.is_array() || details.empty()) return std::nullopt;
    if (!error_details_valid(details)) return std::nullopt;
    std::string text;
    try {
        // Compact, UTF-8 (not ASCII-escaped).  Invalid UTF-8 in a value is a
        // broken detail, and a broken array is omitted like any other.
        text = details.dump(-1, ' ', false, json::error_handler_t::strict);
    } catch (const json::exception&) {
        return std::nullopt;
    }
    // Measured in bytes, as emitted.  Over the cap the whole array goes, never
    // a prefix: a client cannot tell a partial list from a complete one.
    if (text.size() > kMaxErrorDetailsBytes) return std::nullopt;
    return text;
}

json decode_error_details(std::string_view raw) {
    json out = json::array();
    json decoded;
    try {
        decoded = json::parse(raw.begin(), raw.end());
    } catch (const json::exception&) {
        return out;
    }
    if (!decoded.is_array()) return out;
    for (auto& item : decoded) {
        if (item.is_object()) out.push_back(std::move(item));
    }
    return out;
}

bool is_retryable(std::string_view code, const json& details) {
    const Code parsed = parse_code(code);
    if (parsed == Code::UNAVAILABLE) return true;
    if (parsed == Code::RESOURCE_EXHAUSTED && details.is_array()) {
        for (const auto& obj : details) {
            auto detail = parse_error_detail(obj);
            if (detail && std::holds_alternative<RetryInfo>(*detail)) return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

namespace {

json details_json(const std::vector<ErrorDetail>& details) {
    json out = json::array();
    for (const auto& d : details) out.push_back(detail_to_json(d));
    return out;
}

json validated(json details) {
    if (!details.is_array()) {
        throw std::invalid_argument("error details must be a JSON array of objects");
    }
    if (!error_details_valid(details)) {
        throw std::invalid_argument(
            "error details break a catalog rule: every detail must name a qualified '@type', "
            "each type may appear once, and the reserved 'vgi_rpc.' prefix holds only the "
            "catalog (a protocol-defined type lives under its own protocol's name)");
    }
    return details;
}

}  // namespace

StatusError::StatusError(const std::string& message, Code code, std::string kind,
                         std::vector<ErrorDetail> details)
    : KindedError(std::move(kind), "StatusError", message, code, validated(details_json(details))) {
}

StatusError::StatusError(const std::string& message, Code code, std::string kind,
                         nlohmann::json details)
    : KindedError(std::move(kind), "StatusError", message, code, validated(std::move(details))) {}

std::string exception_type_of(const std::exception& e) {
    // A kinded error names itself; nothing else about its C++ class should
    // decide what the client sees.
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) {
        return kinded->exception_type();
    }
    // invalid_argument and out_of_range both derive from logic_error, so the
    // narrow cases have to come first or every one of them reports TypeError.
    if (dynamic_cast<const std::invalid_argument*>(&e)) return "ValueError";
    if (dynamic_cast<const std::out_of_range*>(&e)) return "IndexError";
    if (dynamic_cast<const std::logic_error*>(&e)) return "TypeError";
    return "RuntimeError";
}

std::string error_kind_of(const std::exception& e) {
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) return kinded->kind();
    return "";
}

Code error_code_of(const std::exception& e) {
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) return kinded->code();
    return Code::UNKNOWN;
}

json error_details_of(const std::exception& e) {
    if (const auto* kinded = dynamic_cast<const KindedError*>(&e)) return kinded->details();
    return json::array();
}

ErrorExtras error_extras_of(const std::exception& e, std::string traceback) {
    ErrorExtras extras;
    extras.code = code_name(error_code_of(e));
    extras.details = error_details_of(e);
    extras.traceback = std::move(traceback);
    return extras;
}

std::string cpp_traceback(const std::string& exception_type, const std::string& message,
                          const std::string& where) {
    std::string out = "Traceback (C++ exception; no stack captured):\n";
    if (!where.empty()) out += "  in " + where + "\n";
    out += exception_type + ": " + message;
    return out;
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

RemoteStatus::RemoteStatus(std::string error_code, std::string error_kind,
                           nlohmann::json error_details, std::string remote_traceback)
    : error_code_(std::move(error_code)),
      error_kind_(std::move(error_kind)),
      error_details_(error_details.is_array() ? std::move(error_details) : json::array()),
      remote_traceback_(std::move(remote_traceback)) {}

std::vector<ErrorDetail> RemoteStatus::details() const {
    std::vector<ErrorDetail> out;
    for (const auto& obj : error_details_) {
        if (auto detail = parse_error_detail(obj)) out.push_back(std::move(*detail));
    }
    return out;
}

template <typename T>
std::optional<T> RemoteStatus::first() const {
    for (const auto& obj : error_details_) {
        auto detail = parse_error_detail(obj);
        if (detail && std::holds_alternative<T>(*detail)) return std::get<T>(std::move(*detail));
    }
    return std::nullopt;
}

std::optional<ErrorInfo> RemoteStatus::error_info() const {
    return first<ErrorInfo>();
}
std::optional<RetryInfo> RemoteStatus::retry_info() const {
    return first<RetryInfo>();
}
std::optional<BadRequest> RemoteStatus::bad_request() const {
    return first<BadRequest>();
}
std::optional<PreconditionFailure> RemoteStatus::precondition_failure() const {
    return first<PreconditionFailure>();
}
std::optional<QuotaFailure> RemoteStatus::quota_failure() const {
    return first<QuotaFailure>();
}
std::optional<ResourceInfo> RemoteStatus::resource_info() const {
    return first<ResourceInfo>();
}
std::optional<Help> RemoteStatus::help() const {
    return first<Help>();
}
std::optional<LocalizedMessage> RemoteStatus::localized_message() const {
    return first<LocalizedMessage>();
}

RemoteStatus decode_remote_status(const arrow::KeyValueMetadata* metadata, const json& extra) {
    auto top = [&](const char* key) -> std::optional<std::string> {
        if (metadata == nullptr) return std::nullopt;
        const int index = metadata->FindKey(key);
        if (index < 0) return std::nullopt;
        return metadata->value(index);
    };
    auto mirrored = [&](const char* key) -> std::string {
        if (extra.is_object()) {
            auto it = extra.find(key);
            if (it != extra.end() && it->is_string()) return it->get<std::string>();
        }
        return "";
    };

    std::string code = top(keys::ERROR_CODE).value_or(mirrored("error_code"));
    std::string kind = top(keys::ERROR_KIND).value_or(mirrored("error_kind"));
    json details = json::array();
    if (auto raw = top(keys::ERROR_DETAILS)) {
        details = decode_error_details(*raw);
    } else if (extra.is_object()) {
        auto it = extra.find("error_details");
        if (it != extra.end() && it->is_array()) {
            for (const auto& item : *it) {
                if (item.is_object()) details.push_back(item);
            }
        }
    }
    return RemoteStatus(std::move(code), std::move(kind), std::move(details),
                        mirrored("traceback"));
}

}  // namespace vgi_rpc
