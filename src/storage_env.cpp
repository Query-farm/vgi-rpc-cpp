// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/storage_env.h"

#include "external_internal.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

namespace vgi_rpc {

namespace {

using namespace storage_env;

constexpr int kDefaultUrlTtl = 3600;
// V4 signed URLs (S3 and GCS) live at most seven days.
constexpr int kMaxUrlTtl = 7 * 24 * 3600;

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string get(const char* name) {
    const char* value = std::getenv(name);
    return value ? trim(value) : std::string();
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

bool boolean(const char* name, bool fallback) {
    const std::string raw = lower(get(name));
    if (raw.empty()) return fallback;
    if (raw == "1" || raw == "true" || raw == "yes" || raw == "on") return true;
    if (raw == "0" || raw == "false" || raw == "no" || raw == "off") return false;
    throw std::invalid_argument(std::string(name) + " must be true or false, got '" + raw + "'");
}

std::string scheme_of(const std::string& url) {
    const auto colon = url.find("://");
    return colon == std::string::npos ? std::string() : lower(url.substr(0, colon));
}

// "host[:port]" of an absolute URL, lowercased.
std::string authority_of(const std::string& url) {
    const auto start = url.find("://");
    if (start == std::string::npos) return {};
    const auto rest = url.substr(start + 3);
    return lower(rest.substr(0, rest.find_first_of("/?#")));
}

// An absolute http(s) URL without a trailing slash, or empty when unset.
std::string url(const char* name) {
    std::string raw = get(name);
    if (raw.empty()) return {};
    const std::string scheme = scheme_of(raw);
    if ((scheme != "http" && scheme != "https") || authority_of(raw).empty()) {
        throw std::invalid_argument(std::string(name) +
                                    " must be an absolute http:// or https:// URL, got '" +
                                    external_internal::redact_url(raw) + "'");
    }
    while (!raw.empty() && raw.back() == '/') raw.pop_back();
    return raw;
}

int url_ttl() {
    const std::string raw = get(URL_TTL_SECONDS);
    if (raw.empty()) return kDefaultUrlTtl;
    long long value = 0;
    try {
        size_t used = 0;
        value = std::stoll(raw, &used);
        if (used != raw.size()) throw std::invalid_argument(raw);
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string(URL_TTL_SECONDS) +
                                    " must be an integer number of seconds, got '" + raw + "'");
    }
    if (value < 1 || value > kMaxUrlTtl) {
        throw std::invalid_argument(std::string(URL_TTL_SECONDS) + " must be between 1 and " +
                                    std::to_string(kMaxUrlTtl) + " (7 days), got " +
                                    std::to_string(value));
    }
    return static_cast<int>(value);
}

// Addendum A.4: everything this server stores goes under `<prefix>rpc/`.
std::string with_prefix(const std::string& scheme, const std::string& bucket,
                        const std::string& prefix) {
    return scheme + "://" + bucket + "/" + prefix + "rpc";
}

// RFC 3339 (`2026-11-25T18:00:00Z`, fractional seconds and +hh:mm allowed)
// or epoch seconds.
int64_t timestamp(const char* name) {
    const std::string raw = get(name);
    if (raw.empty()) return 0;
    const bool digits =
        std::all_of(raw.begin(), raw.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
    if (digits) return std::stoll(raw);
    int y = 0, mo = 0, d = 0, h = 0, mi = 0, s = 0, used = 0;
    if (std::sscanf(raw.c_str(), "%4d-%2d-%2dT%2d:%2d:%2d%n", &y, &mo, &d, &h, &mi, &s, &used) !=
            6 ||
        mo < 1 || mo > 12 || d < 1 || d > 31) {
        throw std::invalid_argument(std::string(name) +
                                    " must be an RFC 3339 timestamp or epoch seconds, got '" + raw +
                                    "'");
    }
    std::string rest = raw.substr(static_cast<size_t>(used));
    if (!rest.empty() && rest[0] == '.') {
        const auto end = rest.find_first_not_of("0123456789", 1);
        rest = end == std::string::npos ? std::string() : rest.substr(end);
    }
    int64_t offset = 0;
    if (rest == "Z" || rest == "z") {
        offset = 0;
    } else if (rest.size() == 6 && (rest[0] == '+' || rest[0] == '-') && rest[3] == ':') {
        offset = (std::stoi(rest.substr(1, 2)) * 3600 + std::stoi(rest.substr(4, 2)) * 60) *
                 (rest[0] == '-' ? -1 : 1);
    } else {
        throw std::invalid_argument(std::string(name) + " needs a UTC offset (Z or +hh:mm), got '" +
                                    raw + "'");
    }
    // days_from_civil (Howard Hinnant).
    const int64_t yy = y - (mo <= 2 ? 1 : 0);
    const int64_t era = (yy >= 0 ? yy : yy - 399) / 400;
    const int64_t yoe = yy - era * 400;
    const int64_t doy = (153 * (mo + (mo > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    const int64_t days = era * 146097 + doe - 719468;
    return days * 86400 + h * 3600 + mi * 60 + s - offset;
}

}  // namespace

StorageBackend storage_backend_from_env() {
    const std::string explicit_backend = lower(get(BACKEND));
    if (!explicit_backend.empty()) {
        if (explicit_backend == "vendor") return StorageBackend::Vendor;
        if (explicit_backend == "s3") return StorageBackend::S3;
        if (explicit_backend == "gcs") return StorageBackend::Gcs;
        if (explicit_backend == "none") return StorageBackend::None;
        throw std::invalid_argument(std::string(BACKEND) +
                                    " must be vendor, s3, gcs or none, got '" + explicit_backend +
                                    "'");
    }
    std::vector<std::string> present;
    for (const char* name : {"VGI_BLOB_VENDOR_URL", S3_BUCKET, GCS_BUCKET}) {
        if (!get(name).empty()) present.emplace_back(name);
    }
    if (present.size() > 1) {
        std::string names;
        for (const auto& name : present) names += (names.empty() ? "" : ", ") + name;
        throw std::invalid_argument("Conflicting storage configuration: " + names +
                                    " are all set; set " + BACKEND +
                                    " to vendor, s3, gcs or none to choose one");
    }
    if (present.empty()) return StorageBackend::None;
    if (present[0] == "VGI_BLOB_VENDOR_URL") return StorageBackend::Vendor;
    return present[0] == S3_BUCKET ? StorageBackend::S3 : StorageBackend::Gcs;
}

std::optional<DirectStorageSettings> direct_storage_settings_from_env() {
    const StorageBackend backend = storage_backend_from_env();
    if (backend != StorageBackend::S3 && backend != StorageBackend::Gcs) return std::nullopt;
    DirectStorageSettings out;
    out.backend = backend;
    out.allow_plain_http = boolean(ALLOW_PLAIN_HTTP, false);
    out.config.signed_url_ttl_seconds = url_ttl();
    std::string signed_endpoint;
    if (backend == StorageBackend::S3) {
        const std::string bucket = get(S3_BUCKET);
        if (bucket.empty()) {
            throw std::invalid_argument(std::string(BACKEND) + "=s3 needs " + S3_BUCKET);
        }
        out.config.uri = with_prefix("s3", bucket, get(S3_PREFIX));
        out.config.endpoint_url = url(S3_ENDPOINT);
        out.config.s3_public_endpoint_url = url(S3_PUBLIC_ENDPOINT);
        std::string region = get(S3_REGION);
        if (region.empty()) region = get("AWS_REGION");
        if (region.empty()) region = get("AWS_DEFAULT_REGION");
        out.config.region = region.empty() ? "us-east-1" : region;
        out.config.s3_addressing_style =
            boolean(S3_FORCE_PATH_STYLE, !out.config.endpoint_url.empty()) ? "path" : "virtual";
        const std::string checksum = lower(get(S3_CHECKSUM));
        if (!checksum.empty() && checksum != "sha256" && checksum != "none") {
            throw std::invalid_argument(std::string(S3_CHECKSUM) +
                                        " must be sha256 or none, got '" + checksum + "'");
        }
        out.config.s3_checksum = checksum.empty() ? "sha256" : checksum;
        out.upload_grants = out.config.s3_checksum == "sha256";
        out.config.s3_access_key_id = get(S3_ACCESS_KEY_ID);
        out.config.s3_secret_access_key = get(S3_SECRET_ACCESS_KEY);
        out.config.s3_session_token = get(S3_SESSION_TOKEN);
        if (out.config.s3_access_key_id.empty() != out.config.s3_secret_access_key.empty()) {
            throw std::invalid_argument(std::string(S3_ACCESS_KEY_ID) + " and " +
                                        S3_SECRET_ACCESS_KEY + " must be set together");
        }
        if (!out.config.s3_session_token.empty() && out.config.s3_access_key_id.empty()) {
            throw std::invalid_argument(std::string(S3_SESSION_TOKEN) + " needs " +
                                        S3_ACCESS_KEY_ID + " and " + S3_SECRET_ACCESS_KEY);
        }
        out.config.s3_bucket_check = boolean(S3_VERIFY, false)
                                         ? ExternalStorageConfig::BucketCheck::Startup
                                         : ExternalStorageConfig::BucketCheck::FirstUse;
        out.config.s3_session_expires_epoch = timestamp(S3_SESSION_EXPIRES);
        if (!out.config.s3_session_token.empty() && out.config.s3_session_expires_epoch == 0) {
            out.warnings.push_back(
                std::string(S3_SESSION_TOKEN) + " is set without " + S3_SESSION_EXPIRES +
                ": pre-signed URL lifetimes cannot be capped at the session's expiry; set it, or "
                "use credential_process or the standard AWS chain, which refresh themselves");
        }
        signed_endpoint = out.config.s3_public_endpoint_url.empty()
                              ? out.config.endpoint_url
                              : out.config.s3_public_endpoint_url;
    } else {
        const std::string bucket = get(GCS_BUCKET);
        if (bucket.empty()) {
            throw std::invalid_argument(std::string(BACKEND) + "=gcs needs " + GCS_BUCKET);
        }
        out.config.uri = with_prefix("gs", bucket, get(GCS_PREFIX));
        out.config.gcs_endpoint_url = url(GCS_ENDPOINT);
        out.config.gcs_credentials_file = get(GCS_CREDENTIALS_FILE);
        out.config.gcs_verify_at_startup = boolean(GCS_VERIFY, false);
        signed_endpoint = out.config.gcs_endpoint_url;
    }
    if (!signed_endpoint.empty() && scheme_of(signed_endpoint) == "http" && !out.allow_plain_http) {
        throw std::invalid_argument(
            "Storage URLs would use plain http (" + external_internal::redact_url(signed_endpoint) +
            "); set " + ALLOW_PLAIN_HTTP +
            "=1 to allow it (the extension needs its vgi_storage_allow_plain_http setting too), "
            "or use https");
    }
    return out;
}

std::string DirectStorageSettings::describe() const {
    const bool s3 = backend == StorageBackend::S3;
    std::string endpoint =
        s3 ? (config.s3_public_endpoint_url.empty() ? config.endpoint_url
                                                    : config.s3_public_endpoint_url)
           : config.gcs_endpoint_url;
    std::string out = std::string(s3 ? "s3" : "gcs") + " " + config.uri +
                      " endpoint=" + (endpoint.empty() ? "default" : endpoint);
    if (s3) {
        out += " region=" + config.region + " addressing=" + config.s3_addressing_style +
               " credentials=" + (config.s3_access_key_id.empty() ? "default chain" : "explicit");
    }
    out += " url_ttl=" + std::to_string(config.signed_url_ttl_seconds) +
           "s upload_grants=" + (upload_grants ? "on" : "off (checksum=none)");
    return out;
}

std::function<void(const std::string&)> direct_storage_url_validator(
    const DirectStorageSettings& settings) {
    // The hosts pre-signed URLs carry: the (public) endpoint, path-style or
    // with the bucket as a subdomain; AWS's regional and global names
    // otherwise.
    std::vector<std::string> hosts;
    const std::string uri = settings.config.uri;
    const auto scheme_end = uri.find("://");
    const std::string rest = scheme_end == std::string::npos ? uri : uri.substr(scheme_end + 3);
    const std::string bucket = lower(rest.substr(0, rest.find('/')));
    if (settings.backend == StorageBackend::S3) {
        const std::string endpoint = settings.config.s3_public_endpoint_url.empty()
                                         ? settings.config.endpoint_url
                                         : settings.config.s3_public_endpoint_url;
        const bool path_style = settings.config.s3_addressing_style == "path";
        if (!endpoint.empty()) {
            const std::string authority = authority_of(endpoint);
            hosts.push_back(path_style ? authority : bucket + "." + authority);
        } else {
            const std::string region = lower(settings.config.region);
            for (const std::string& host :
                 {"s3." + region + ".amazonaws.com", std::string("s3.amazonaws.com"),
                  "s3.dualstack." + region + ".amazonaws.com"}) {
                hosts.push_back(path_style ? host : bucket + "." + host);
            }
        }
    } else {
        hosts.push_back(settings.config.gcs_endpoint_url.empty()
                            ? std::string("storage.googleapis.com")
                            : authority_of(settings.config.gcs_endpoint_url));
    }
    const bool allow_http = settings.allow_plain_http;
    return [hosts, allow_http](const std::string& candidate) {
        const std::string scheme = scheme_of(candidate);
        if (scheme != "https" && !(allow_http && scheme == "http")) {
            throw std::invalid_argument(
                "URL scheme '" + scheme + "' not allowed for storage URLs" +
                (scheme == "http" ? std::string(" (set ") + ALLOW_PLAIN_HTTP + "=1 to allow http)"
                                  : std::string()));
        }
        std::string authority = authority_of(candidate);
        const auto at = authority.rfind('@');
        if (at != std::string::npos) {
            throw std::invalid_argument("storage URLs may not carry credentials");
        }
        // A default port may be written or not.
        for (const char* suffix : {":443", ":80"}) {
            const std::string s(suffix);
            if (authority.size() > s.size() &&
                authority.compare(authority.size() - s.size(), s.size(), s) == 0) {
                const bool https_default = s == ":443" && scheme == "https";
                const bool http_default = s == ":80" && scheme == "http";
                if (https_default || http_default) authority.resize(authority.size() - s.size());
            }
        }
        if (std::find(hosts.begin(), hosts.end(), authority) == hosts.end()) {
            throw std::invalid_argument("URL host '" + authority +
                                        "' is not this worker's storage host");
        }
    };
}

}  // namespace vgi_rpc
