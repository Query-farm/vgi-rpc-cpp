// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/vendor.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "external_internal.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/http_config.h"

namespace vgi_rpc {

using namespace external_internal;

namespace {

constexpr const char* kPutPath = "/v1/blobs/put";
constexpr const char* kGetPath = "/v1/blobs/get";
constexpr const char* kInfoPath = "/v1/info";
constexpr int kMaxBackoffMs = 5000;
constexpr int64_t kDefaultThreshold = 1'048'576;
constexpr int64_t kDefaultMaxFetchBytes = 256LL * 1024 * 1024;
constexpr size_t kMaxVendorResponseBytes = 16U << 20U;

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

std::string trim(const std::string& value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string env(const char* name) {
    const char* raw = std::getenv(name);
    return raw ? trim(raw) : std::string{};
}

// A parsed absolute URL: just the parts the pin compares.
struct ParsedUrl {
    std::string scheme;
    bool userinfo = false;
    std::string host;  // lowercased, IPv6 without brackets
    int port = 0;      // 0: none given
    std::string path;  // as written (escaped), "" when absent
};

bool parse_port(const std::string& text, int& port) {
    if (text.empty() || text.size() > 5) return false;
    for (char c : text)
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    port = std::stoi(text);
    return port >= 1 && port <= 65535;
}

// Parses "authority" ("[user@]host[:port]") into `out`.  False when malformed.
bool parse_authority(std::string authority, ParsedUrl& out) {
    const auto at = authority.rfind('@');
    if (at != std::string::npos) {
        out.userinfo = true;
        authority.erase(0, at + 1);
    }
    std::string port;
    if (!authority.empty() && authority.front() == '[') {
        const auto close = authority.find(']');
        if (close == std::string::npos) return false;
        out.host = authority.substr(1, close - 1);
        const std::string rest = authority.substr(close + 1);
        if (!rest.empty()) {
            if (rest.front() != ':') return false;
            port = rest.substr(1);
            if (port.empty()) return false;
        }
    } else {
        const auto colon = authority.rfind(':');
        if (colon != std::string::npos) {
            port = authority.substr(colon + 1);
            authority.erase(colon);
            if (port.empty()) return false;
        }
        out.host = authority;
    }
    if (out.host.empty()) return false;
    out.host = lower(out.host);
    if (!port.empty() && !parse_port(port, out.port)) return false;
    return true;
}

bool parse_url(const std::string& url, ParsedUrl& out) {
    const auto sep = url.find("://");
    if (sep == std::string::npos || sep == 0) return false;
    out.scheme = lower(url.substr(0, sep));
    const auto authority_start = sep + 3;
    const auto authority_end = url.find_first_of("/?#", authority_start);
    if (!parse_authority(url.substr(authority_start, authority_end - authority_start), out))
        return false;
    if (authority_end != std::string::npos && url[authority_end] == '/') {
        const auto path_end = url.find_first_of("?#", authority_end);
        out.path = url.substr(authority_end, path_end - authority_end);
    }
    return true;
}

int default_port(const std::string& scheme) {
    return scheme == "https" ? 443 : 80;
}

bool is_loopback_host(const std::string& host) {
    const std::string h = lower(host);
    return h == "localhost" || h == "::1" || h.rfind("127.", 0) == 0;
}

// The vendor's error body, `{error, message}`, as (code, ": code: message").
std::pair<std::string, std::string> vendor_error_detail(const std::string& body) {
    try {
        const auto parsed = nlohmann::json::parse(body);
        if (!parsed.is_object()) return {"", ""};
        std::string code = parsed.value("error", std::string{});
        std::string message = parsed.value("message", std::string{});
        if (code.size() > 64) code.resize(64);
        if (message.size() > 200) message.resize(200);
        std::string detail;
        for (const auto* part : {&code, &message})
            if (!part->empty()) detail += ": " + *part;
        return {code, detail};
    } catch (const std::exception&) {
        return {"", ""};
    }
}

// An S3-style `<Code>...</Code>`, when it is a plain alphanumeric word.
std::string s3_error_code(const std::string& body) {
    const auto start = body.find("<Code>");
    if (start == std::string::npos) return "";
    const auto end = body.find("</Code>", start);
    if (end == std::string::npos) return "";
    std::string code = body.substr(start + 6, end - start - 6);
    if (code.empty() || code.size() > 64) return "";
    for (char c : code)
        if (!std::isalnum(static_cast<unsigned char>(c))) return "";
    return code;
}

std::string json_string(const nlohmann::json& obj, const char* key, const char* op, bool required) {
    const auto found = obj.find(key);
    if (found == obj.end() || found->is_null()) {
        if (!required) return "";
        throw VendorError(std::string("blob vendor ") + op + " response is missing \"" + key +
                          "\"");
    }
    if (!found->is_string() || (required && found->get<std::string>().empty())) {
        throw VendorError(std::string("blob vendor ") + op + " response has a malformed \"" + key +
                          "\"");
    }
    return found->get<std::string>();
}

int64_t json_size(const nlohmann::json& obj, const char* key, const char* op) {
    const auto found = obj.find(key);
    if (found == obj.end() || !found->is_number_integer() || found->get<int64_t>() < 0) {
        throw VendorError(std::string("blob vendor ") + op + " response has a malformed \"" + key +
                          "\"");
    }
    return found->get<int64_t>();
}

std::vector<std::string> json_string_list(const nlohmann::json& obj, const char* key) {
    std::vector<std::string> out;
    const auto found = obj.find(key);
    if (found == obj.end() || found->is_null()) return out;
    if (!found->is_array())
        throw VendorError(std::string("blob vendor info has a malformed \"") + key + "\"");
    for (const auto& item : *found) {
        if (!item.is_string() || item.get<std::string>().empty())
            throw VendorError(std::string("blob vendor info has a malformed \"") + key + "\"");
        out.push_back(item.get<std::string>());
    }
    return out;
}

struct PutGrant {
    std::string object_id;
    std::string upload_url;
    std::map<std::string, std::string> required_headers;
    std::string download_url;
};

struct BlobLocation {
    std::string download_url;
    int64_t size = 0;
    std::string content_encoding;
};

struct HttpOutcome {
    int status = 0;
    std::string body;
    std::string content_encoding;
};

int64_t positive_env(const char* name) {
    const std::string raw = env(name);
    if (raw.empty()) return 0;
    size_t used = 0;
    long long value = 0;
    try {
        value = std::stoll(raw, &used);
    } catch (const std::exception&) {
        used = 0;
    }
    if (used != raw.size()) {
        throw std::invalid_argument(std::string(name) + " must be an integer, got '" + raw + "'");
    }
    if (value <= 0) {
        throw std::invalid_argument(std::string(name) + " must be positive, got " + raw);
    }
    return value;
}

}  // namespace

// ---------------------------------------------------------------------------
// URL pinning
// ---------------------------------------------------------------------------

std::function<void(const std::string&)> pinned_hosts_validator(
    const std::vector<std::string>& hosts, bool allow_http,
    const std::vector<std::string>& prefixes) {
    std::vector<std::string> schemes{"https"};
    if (allow_http) schemes.push_back("http");
    std::set<std::pair<std::string, int>> allowed;
    for (const auto& entry : hosts) {
        ParsedUrl parsed;
        const std::string text = trim(entry);
        if (text.empty() || text.find_first_of("/?#") != std::string::npos ||
            !parse_authority(text, parsed) || parsed.userinfo) {
            throw std::invalid_argument("invalid pinned host entry: " + entry);
        }
        if (parsed.port == 0) {
            for (const auto& scheme : schemes) allowed.emplace(parsed.host, default_port(scheme));
        } else {
            allowed.emplace(parsed.host, parsed.port);
        }
    }
    if (allowed.empty()) throw std::invalid_argument("pinned_hosts_validator needs a host");
    std::vector<std::string> url_prefixes;
    for (const auto& prefix : prefixes)
        if (!prefix.empty()) url_prefixes.push_back(prefix);
    return [schemes, allowed, url_prefixes](const std::string& url) {
        ParsedUrl parsed;
        if (!parse_url(url, parsed)) throw std::invalid_argument("URL is malformed");
        if (std::find(schemes.begin(), schemes.end(), parsed.scheme) == schemes.end()) {
            throw std::invalid_argument("URL scheme '" + parsed.scheme + "' not allowed");
        }
        if (parsed.userinfo) throw std::invalid_argument("URL userinfo is not allowed");
        const int port = parsed.port == 0 ? default_port(parsed.scheme) : parsed.port;
        if (!allowed.count({parsed.host, port})) {
            throw std::invalid_argument("URL host is not a pinned storage host: " +
                                        redact_url(url));
        }
        if (!url_prefixes.empty()) {
            const auto authority_start = url.find("://") + 3;
            const auto authority_end = url.find_first_of("/?#", authority_start);
            const std::string bare = url.substr(0, authority_end) + parsed.path;
            bool dotdot = false;
            size_t start = 0;
            while (start <= parsed.path.size()) {
                const auto slash = parsed.path.find('/', start);
                const auto segment = parsed.path.substr(
                    start, slash == std::string::npos ? std::string::npos : slash - start);
                if (segment == "..") dotdot = true;
                if (slash == std::string::npos) break;
                start = slash + 1;
            }
            const bool matched =
                std::any_of(url_prefixes.begin(), url_prefixes.end(),
                            [&](const std::string& p) { return bare.rfind(p, 0) == 0; });
            if (dotdot || !matched) {
                throw std::invalid_argument("URL is outside the pinned storage prefixes: " +
                                            redact_url(url));
            }
        }
    };
}

// ---------------------------------------------------------------------------
// VendorStorage
// ---------------------------------------------------------------------------

struct VendorStorage::Impl {
    std::string base_url;
    std::string token;
    bool loopback_http = false;
    VendorOptions options;

    std::mutex pin_mutex;
    std::function<void(const std::string&)> pin;
    std::chrono::steady_clock::time_point pin_fetched_at;

    // cpp-httplib's read/write timeouts bound each socket operation, not the
    // request: a stall timeout.  Signed-URL transfers (`storage`) use the
    // storage stall timeout and no whole-request deadline, so a slow transfer
    // that keeps moving is never cut off; the vendor's small JSON calls use
    // timeout_seconds.
    httplib::Client client_for(const std::string& origin, bool storage) const {
        httplib::Client client(origin);
        const int idle = storage
                             ? (options.stall_timeout_seconds > 0 ? options.stall_timeout_seconds
                                                                  : storage_stall_timeout_seconds())
                             : options.timeout_seconds;
        client.set_connection_timeout(
            options.connect_timeout_seconds > 0 ? options.connect_timeout_seconds : 30, 0);
        client.set_read_timeout(idle, 0);
        client.set_write_timeout(idle, 0);
        // A redirect could carry the token or a signed URL somewhere unvetted.
        client.set_follow_location(false);
        client.set_decompress(false);
        return client;
    }

    // One request, retrying transport errors, 429 and 5xx with backoff.
    // `body_cap` bounds what is read of the response body.
    HttpOutcome send(const std::string& method, const std::string& url, const std::string& op,
                     httplib::Headers headers, const std::string* body,
                     const std::string& content_type, size_t body_cap) const {
        auto [origin, target] = split_url(url);
        std::string last_error;
        int status = 0;
        static thread_local std::mt19937 rng{std::random_device{}()};
        for (int attempt = 0; attempt < options.max_attempts; ++attempt) {
            if (attempt > 0) {
                int delay = std::min(options.backoff_ms * (1 << (attempt - 1)), kMaxBackoffMs);
                delay =
                    static_cast<int>(delay * (0.5 + std::uniform_real_distribution<>(0, 0.5)(rng)));
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }
            auto client = client_for(origin, op == "upload" || op == "download");
            HttpOutcome outcome;
            bool over_cap = false;
            httplib::Result res{nullptr, httplib::Error::Unknown};
            if (method == "GET") {
                res = client.Get(
                    target, headers,
                    [&](const httplib::Response& response) {
                        outcome.status = response.status;
                        outcome.content_encoding = response.get_header_value("Content-Encoding");
                        return true;
                    },
                    [&](const char* data, size_t length) {
                        if (outcome.body.size() + length > body_cap) {
                            over_cap = true;
                            return false;
                        }
                        outcome.body.append(data, length);
                        return true;
                    });
            } else if (method == "POST") {
                res = client.Post(target, headers, body ? *body : std::string{}, content_type);
            } else {
                res = client.Put(target, headers, body ? *body : std::string{}, content_type);
            }
            if (over_cap) {
                throw VendorError("blob vendor " + op + " response exceeds the " +
                                  std::to_string(body_cap) + "-byte cap: " + redact_url(url));
            }
            if (!res) {
                // Never the library's text: it can embed the full signed URL.
                status = 0;
                last_error =
                    res.error() == httplib::Error::Read || res.error() == httplib::Error::Write
                        ? "timeout or connection reset"
                        : "transport error";
                continue;
            }
            if (method != "GET") {
                outcome.status = res->status;
                outcome.body =
                    res->body.size() > body_cap ? res->body.substr(0, body_cap) : res->body;
                outcome.content_encoding = res->get_header_value("Content-Encoding");
            }
            if (outcome.status == 429 || outcome.status >= 500) {
                status = outcome.status;
                last_error = "HTTP " + std::to_string(outcome.status);
                continue;
            }
            return outcome;
        }
        throw VendorError("blob vendor " + op + " failed after " +
                              std::to_string(options.max_attempts) + " attempts (" + last_error +
                              "): " + redact_url(url),
                          status);
    }

    nlohmann::json call(const std::string& method, const std::string& path, const std::string& op,
                        const nlohmann::json* payload) const {
        httplib::Headers headers{{"Authorization", "Bearer " + token},
                                 {"Accept", "application/json"},
                                 {"Accept-Encoding", "identity"}};
        std::string body;
        if (payload) body = payload->dump();
        const auto outcome = send(method, base_url + path, op, headers, payload ? &body : nullptr,
                                  "application/json", kMaxVendorResponseBytes);
        if (outcome.status < 200 || outcome.status >= 300) {
            auto [code, detail] = vendor_error_detail(outcome.body);
            throw VendorError(
                "blob vendor " + op + " failed: HTTP " + std::to_string(outcome.status) + detail,
                outcome.status, code);
        }
        if (outcome.body.empty()) return nlohmann::json::object();
        try {
            auto parsed = nlohmann::json::parse(outcome.body);
            if (!parsed.is_object()) {
                throw VendorError("blob vendor " + op + " returned a non-object JSON body",
                                  outcome.status);
            }
            return parsed;
        } catch (const nlohmann::json::exception&) {
            throw VendorError("blob vendor " + op + " returned non-JSON", outcome.status);
        }
    }

    PutGrant put(int64_t size, const std::string& sha256_b64, const std::string& content_encoding,
                 const std::string& origin) const {
        nlohmann::json body = {{"size", size}, {"sha256_b64", sha256_b64}, {"purpose", "rpc"}};
        if (!content_encoding.empty()) body["content_encoding"] = content_encoding;
        if (!origin.empty()) body["origin"] = origin;
        const auto resp = call("POST", kPutPath, "put", &body);
        if (auto method = resp.find("upload_method");
            method != resp.end() && !(method->is_string() && method->get<std::string>() == "PUT")) {
            throw VendorError("blob vendor put asked for an unsupported upload_method");
        }
        const auto headers = resp.find("required_headers");
        if (headers == resp.end() || !headers->is_object()) {
            throw VendorError("blob vendor put response has malformed 'required_headers'");
        }
        PutGrant grant;
        for (const auto& [name, value] : headers->items()) {
            if (!value.is_string()) {
                throw VendorError("blob vendor put response has malformed 'required_headers'");
            }
            grant.required_headers[name] = value.get<std::string>();
        }
        grant.object_id = json_string(resp, "object_id", "put", true);
        grant.upload_url = json_string(resp, "upload_url", "put", true);
        grant.download_url = json_string(resp, "download_url", "put", false);
        return grant;
    }

    BlobLocation get(const std::string& object_id) const {
        const nlohmann::json body = {{"object_id", object_id}};
        const auto resp = call("POST", kGetPath, "get", &body);
        BlobLocation location;
        location.download_url = json_string(resp, "download_url", "get", true);
        location.size = json_size(resp, "size", "get");
        (void)json_string(resp, "sha256_b64", "get", true);
        location.content_encoding = json_string(resp, "content_encoding", "get", false);
        return location;
    }

    // PUT `data` to a granted URL with exactly its required headers.
    void put_object(const PutGrant& grant, const std::string& data,
                    const std::string& content_encoding) const {
        httplib::Headers headers{{"Accept-Encoding", "identity"}};
        bool has_encoding = false;
        std::string content_type;
        for (const auto& [name, value] : grant.required_headers) {
            const std::string key = lower(name);
            if (key == "content-length") {
                if (value != std::to_string(data.size())) {
                    throw VendorError("blob vendor granted Content-Length " + value + " for a " +
                                      std::to_string(data.size()) + "-byte object");
                }
                continue;  // httplib sets it from the body
            }
            if (key == "content-type") {
                content_type = value;
                continue;
            }
            if (key == "content-encoding") has_encoding = true;
            headers.emplace(name, value);
        }
        if (!content_encoding.empty() && !has_encoding)
            headers.emplace("Content-Encoding", content_encoding);
        const auto outcome =
            send("PUT", grant.upload_url, "upload", headers, &data, content_type, 1U << 16U);
        if (outcome.status < 200 || outcome.status >= 300) {
            const std::string code = s3_error_code(outcome.body);
            throw VendorError("signed upload failed: HTTP " + std::to_string(outcome.status) +
                                  (code.empty() ? "" : " " + code) + ": " +
                                  redact_url(grant.upload_url),
                              outcome.status, code);
        }
    }

    // GET a signed URL, pinned and capped.  Returns the body and its coding.
    HttpOutcome download(const std::string& url, int64_t max_bytes) {
        validate(url);
        auto outcome =
            send("GET", url, "download", httplib::Headers{{"Accept-Encoding", "zstd, identity"}},
                 nullptr, "", static_cast<size_t>(max_bytes));
        if (outcome.status != 200) {
            throw VendorError(
                "download failed: HTTP " + std::to_string(outcome.status) + ": " + redact_url(url),
                outcome.status, s3_error_code(outcome.body));
        }
        return outcome;
    }

    std::function<void(const std::string&)> current_pin() {
        std::lock_guard<std::mutex> lock(pin_mutex);
        const auto now = std::chrono::steady_clock::now();
        if (pin && now - pin_fetched_at < std::chrono::seconds(options.info_ttl_seconds)) {
            return pin;
        }
        try {
            const auto info = call("GET", kInfoPath, "info", nullptr);
            auto hosts = json_string_list(info, "upload_hosts");
            const auto download_hosts = json_string_list(info, "download_hosts");
            hosts.insert(hosts.end(), download_hosts.begin(), download_hosts.end());
            pin = pinned_hosts_validator(hosts, loopback_http,
                                         json_string_list(info, "url_prefixes"));
            pin_fetched_at = now;
            return pin;
        } catch (const std::exception& error) {
            if (pin) {
                // Keep the last known-good pin through a vendor blip.
                std::cerr << "vgi_rpc: blob vendor info refresh failed; keeping cached hosts: "
                          << error.what() << "\n";
                pin_fetched_at = now;
                return pin;
            }
            throw std::invalid_argument(std::string("cannot pin storage hosts: ") + error.what());
        }
    }

    void validate(const std::string& url) { current_pin()(url); }

    // A URL the vendor returned, checked against its own pin.
    void check(const std::string& url) {
        try {
            validate(url);
        } catch (const std::exception& error) {
            throw VendorError(std::string("blob vendor returned a URL off its pinned hosts: ") +
                              error.what());
        }
    }
};

VendorStorage::VendorStorage(const std::string& base_url, std::string token, VendorOptions options)
    : impl_(std::make_shared<Impl>()), options_(options) {
    ParsedUrl parsed;
    if (!parse_url(base_url, parsed) || (parsed.scheme != "https" && parsed.scheme != "http")) {
        throw std::invalid_argument("blob vendor URL must be an absolute https URL: " +
                                    redact_url(base_url));
    }
    if (parsed.scheme == "http" && !is_loopback_host(parsed.host)) {
        // The bearer token would cross the network in plain text.
        throw std::invalid_argument(
            "blob vendor URL must use https (http is allowed only for loopback): " +
            redact_url(base_url));
    }
    if (trim(token).empty()) throw std::invalid_argument("blob vendor token is empty");
    if (options.max_attempts <= 0) options.max_attempts = 3;
    if (options.timeout_seconds <= 0) options.timeout_seconds = 30;
    if (options.backoff_ms <= 0) options.backoff_ms = 250;
    if (options.info_ttl_seconds <= 0) options.info_ttl_seconds = 600;
    options_ = options;
    impl_->base_url = base_url;
    while (!impl_->base_url.empty() && impl_->base_url.back() == '/') impl_->base_url.pop_back();
    impl_->token = trim(token);
    impl_->loopback_http = parsed.scheme == "http";
    impl_->options = options;
}

VendorStorage::~VendorStorage() = default;

void VendorStorage::validate_url(const std::string& url) {
    impl_->validate(url);
}

std::function<void(const std::string&)> VendorStorage::url_validator() {
    std::weak_ptr<Impl> weak = impl_;
    return [weak](const std::string& url) {
        auto impl = weak.lock();
        if (!impl) throw std::invalid_argument("blob vendor storage is gone");
        impl->validate(url);
    };
}

std::string VendorStorage::upload(const std::string& data, const std::string& content_encoding) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    const auto grant = impl_->put(static_cast<int64_t>(data.size()),
                                  base64_std(digest.data(), digest.size()), content_encoding, "");
    impl_->check(grant.upload_url);
    impl_->put_object(grant, data, content_encoding);
    std::string download_url = grant.download_url;
    if (download_url.empty()) download_url = impl_->get(grant.object_id).download_url;
    impl_->check(download_url);
    return download_url;
}

std::string VendorStorage::fetch(const std::string& url) {
    const auto outcome = impl_->download(url, options_.max_fetch_bytes);
    const std::string coding = lower(outcome.content_encoding);
    if (coding == "zstd")
        return zstd_decompress_bounded(outcome.body, options_.max_decompressed_bytes);
    if (!coding.empty() && coding != "identity") {
        throw VendorError("external location has unsupported Content-Encoding: " + coding);
    }
    return outcome.body;
}

UploadUrl VendorStorage::generate_upload_url(int64_t size, const std::string& sha256_hex) {
    const auto grant = impl_->put(size, sha256_hex_to_base64(sha256_hex), "", CLIENT_UPLOAD_ORIGIN);
    if (grant.download_url.empty()) {
        throw VendorError("blob vendor put response for an rpc object has no 'download_url'");
    }
    impl_->check(grant.upload_url);
    impl_->check(grant.download_url);
    UploadUrl url;
    url.upload_url = grant.upload_url;
    url.download_url = grant.download_url;
    url.object_id = grant.object_id;
    url.expires_at_us = expiry_us(3600);
    for (const auto& [name, value] : grant.required_headers) {
        if (lower(name) == "content-length") {
            if (value != std::to_string(size)) {
                throw VendorError("blob vendor granted Content-Length " + value + " for a " +
                                  std::to_string(size) + "-byte upload");
            }
            continue;
        }
        url.headers[name] = value;
    }
    return url;
}

std::string VendorStorage::put_object(const std::string& data) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    const auto grant = impl_->put(static_cast<int64_t>(data.size()),
                                  base64_std(digest.data(), digest.size()), "", STATE_REF_ORIGIN);
    impl_->check(grant.upload_url);
    impl_->put_object(grant, data, "");
    return grant.object_id;
}

std::string VendorStorage::get(const std::string& object_id, int64_t max_bytes) {
    if (max_bytes <= 0) max_bytes = kDefaultMaxFetchBytes;
    BlobLocation location;
    try {
        location = impl_->get(object_id);
    } catch (const VendorError& error) {
        if (error.code() == "not_found" || error.code() == "not_uploaded" ||
            error.status() == 404) {
            throw ObjectMissingError(
                "blob vendor object is gone (" +
                (error.code().empty() ? std::to_string(error.status()) : error.code()) + ")");
        }
        throw;
    }
    if (location.size > max_bytes) {
        throw VendorError("blob vendor object is " + std::to_string(location.size) +
                          " bytes, over max_fetch_bytes (" + std::to_string(max_bytes) + ")");
    }
    if (!location.content_encoding.empty() && location.content_encoding != "identity") {
        throw VendorError("blob vendor object has unsupported content_encoding '" +
                          location.content_encoding + "'");
    }
    try {
        auto outcome = impl_->download(location.download_url, max_bytes);
        const std::string coding = lower(outcome.content_encoding);
        if (!coding.empty() && coding != "identity") {
            throw VendorError("storage object has unrequested Content-Encoding: " + coding);
        }
        return std::move(outcome.body);
    } catch (const VendorError& error) {
        if (error.status() == 404)
            throw ObjectMissingError("blob vendor object is gone (storage 404)");
        throw;
    }
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

std::shared_ptr<VendorStorage> vendor_storage_from_env() {
    const std::string url = env(vendor_env::BLOB_VENDOR_URL);
    if (url.empty()) return nullptr;
    std::string token = env(vendor_env::BLOB_VENDOR_TOKEN);
    if (token.empty()) token = env(vendor_env::BLOB_VENDOR_TOKEN_FALLBACK);
    if (token.empty()) {
        throw std::invalid_argument(std::string(vendor_env::BLOB_VENDOR_URL) +
                                    " is set but neither " + vendor_env::BLOB_VENDOR_TOKEN +
                                    " nor " + vendor_env::BLOB_VENDOR_TOKEN_FALLBACK + " is");
    }
    return std::make_shared<VendorStorage>(url, token);
}

std::shared_ptr<VendorStorage> configure_http_from_env(HttpConfig& config) {
    const int64_t max_request = positive_env(vendor_env::MAX_REQUEST_BYTES);
    if (max_request > 0) config.max_request_bytes = max_request;
    auto storage = vendor_storage_from_env();
    if (!storage) return nullptr;

    const int64_t threshold = positive_env(vendor_env::EXTERNALIZE_THRESHOLD_BYTES);
    const std::string compression = lower(env(vendor_env::EXTERNAL_COMPRESSION));
    std::string coding;
    if (compression == "zstd" || compression.rfind("zstd:", 0) == 0) {
        coding = "zstd";
    } else if (!compression.empty() && compression != "none" && compression != "off" &&
               compression != "identity") {
        throw std::invalid_argument(std::string(vendor_env::EXTERNAL_COMPRESSION) +
                                    " must be none or zstd, got '" + compression + "'");
    }
    const int64_t fetch = positive_env(vendor_env::MAX_FETCH_BYTES);
    const int64_t upload = positive_env(vendor_env::MAX_UPLOAD_BYTES);
    if (fetch > 0 && upload > fetch) {
        throw std::invalid_argument(
            std::string(vendor_env::MAX_UPLOAD_BYTES) + " (" + std::to_string(upload) +
            ") exceeds " + vendor_env::MAX_FETCH_BYTES + " (" + std::to_string(fetch) +
            "): clients would upload requests the server then refuses to fetch");
    }
    const int64_t max_fetch = fetch > 0 ? fetch : std::max(kDefaultMaxFetchBytes, upload);

    storage->set_max_fetch_bytes(max_fetch);
    config.external_storage = storage;
    config.external_storage_url.clear();
    config.externalize_threshold = threshold > 0 ? threshold : kDefaultThreshold;
    config.externalize_compression = coding;
    config.max_fetch_bytes = max_fetch;
    // 0 (unset) advertises the fetch cap itself.
    config.max_upload_bytes = upload > 0 ? upload : max_fetch;
    if (!config.external_url_validator) config.external_url_validator = storage->url_validator();
    return storage;
}

}  // namespace vgi_rpc
