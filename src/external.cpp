// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/external.h"

#include "external_internal.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <zstd.h>

#include <array>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <atomic>
#include <cstdio>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>

#ifdef VGI_RPC_WITH_S3
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <sstream>
#endif

#ifdef VGI_RPC_WITH_GCS
#include <google/cloud/storage/client.h>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/signed_url_options.h>

#include <fstream>
#include <sstream>
#include <chrono>
#endif

namespace vgi_rpc {

int storage_stall_timeout_seconds() {
    const char* text = std::getenv(STORAGE_STALL_TIMEOUT_ENV);
    if (text == nullptr || *text == '\0' || *text == '-') return 30;
    char* end = nullptr;
    errno = 0;
    const unsigned long value = std::strtoul(text, &end, 10);
    if (errno != 0 || *end != '\0' || value < 1 || value > 86400) return 30;
    return static_cast<int>(value);
}

void storage_retry_backoff(int attempt) {
    if (attempt < 1) return;
    static thread_local std::mt19937 rng{std::random_device{}()};
    double delay = 0.25 * static_cast<double>(1 << std::min(attempt - 1, 5));
    delay = std::min(delay, 5.0) * (0.5 + std::uniform_real_distribution<>(0, 0.5)(rng));
    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay * 1000)));
}

// Shared with the blob-vendor backend (src/vendor.cpp): src/external_internal.h.
namespace external_internal {

// Split "scheme://host/path" into (scheme, rest-after-the-slashes).
std::pair<std::string, std::string> split_scheme(const std::string& uri) {
    const size_t sep = uri.find("://");
    if (sep == std::string::npos) return {"", uri};
    return {uri.substr(0, sep), uri.substr(sep + 3)};
}

// Split "bucket/prefix..." into its two halves; the prefix may be empty.
std::pair<std::string, std::string> split_bucket(const std::string& rest) {
    const size_t slash = rest.find('/');
    if (slash == std::string::npos) return {rest, ""};
    return {rest.substr(0, slash), rest.substr(slash + 1)};
}

// Render a URL for diagnostics without bearer query strings, fragments, or
// userinfo.  Invalid input is deliberately not echoed.
std::string redact_url(const std::string& url) {
    const size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) return "<invalid-url>";
    const size_t authority_start = scheme_end + 3;
    const size_t authority_end = url.find_first_of("/?#", authority_start);
    std::string authority = url.substr(authority_start, authority_end - authority_start);
    const size_t at = authority.rfind('@');
    if (at != std::string::npos) authority.erase(0, at + 1);
    std::string path = "/";
    if (authority_end != std::string::npos && url[authority_end] == '/') {
        const size_t path_end = url.find_first_of("?#", authority_end);
        path = url.substr(authority_end, path_end - authority_end);
    }
    return url.substr(0, scheme_end + 3) + authority + path;
}

// Split an absolute HTTP URL into an httplib client origin and request target.
std::pair<std::string, std::string> split_url(const std::string& url) {
    const size_t scheme_end = url.find("://");
    const std::string scheme = scheme_end == std::string::npos ? "" : url.substr(0, scheme_end);
    if (scheme != "http" && scheme != "https") {
        throw std::invalid_argument(
            "external location is not an absolute HTTP URL [url: " + redact_url(url) + "]");
    }
    const size_t authority_start = scheme_end + 3;
    const size_t target_start = url.find_first_of("/?#", authority_start);
    const std::string origin =
        target_start == std::string::npos ? url : url.substr(0, target_start);
    if (origin.size() == authority_start) {
        throw std::invalid_argument(
            "external location URL has no authority [url: " + redact_url(url) + "]");
    }
    if (target_start == std::string::npos || url[target_start] == '#') return {origin, "/"};
    const size_t fragment = url.find('#', target_start);
    std::string target = url.substr(target_start, fragment - target_start);
    if (!target.empty() && target.front() == '?') target.insert(target.begin(), '/');
    return {origin, target};
}

// Decoding is done here, bounded, not by cpp-httplib.  But cpp-httplib still
// advertises every codec it was built with on any request that names no
// Accept-Encoding of its own, so each request below names exactly what this
// backend decodes: a storage server free to compress would otherwise answer
// `/alloc` in a codec nothing here can read.
//
// cpp-httplib's read and write timeouts bound each socket operation (and
// SO_RCVTIMEO/SO_SNDTIMEO each syscall, under TLS too), not the request, so
// they are stall timeouts: a slow transfer that keeps moving never hits them.
// No set_max_timeout: a whole-request deadline would cut such a transfer off.
int stall_seconds(const ExternalStorageConfig& config) {
    return config.stall_timeout_seconds > 0 ? config.stall_timeout_seconds
                                            : storage_stall_timeout_seconds();
}

httplib::Client make_client(const std::string& origin, const ExternalStorageConfig& config) {
    httplib::Client client(origin);
    const int stall = stall_seconds(config);
    client.set_connection_timeout(
        config.connect_timeout_seconds > 0 ? config.connect_timeout_seconds : 30, 0);
    client.set_read_timeout(stall, 0);
    client.set_write_timeout(stall, 0);
    client.set_follow_location(false);
    client.set_decompress(false);
    return client;
}

// Connect errors, resets, stalls, 429 and 5xx: worth another attempt.
bool transient(const httplib::Result& res) {
    return !res || res->status == 429 || res->status >= 500;
}

int attempts(const ExternalStorageConfig& config) {
    return config.max_attempts > 0 ? config.max_attempts : 3;
}

// A response coded in something other than what the request accepted.  Raised
// rather than parsed: compressed bytes read as JSON or IPC fail further on with
// an error that names the wrong cause.
void require_identity_response(const httplib::Response& res, const char* what) {
    std::string encoding = res.get_header_value("Content-Encoding");
    std::transform(encoding.begin(), encoding.end(), encoding.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (!encoding.empty() && encoding != "identity") {
        throw std::runtime_error(std::string(what) +
                                 " response has unrequested Content-Encoding: " + encoding);
    }
}

bool is_redirect(int status) {
    return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
}

std::string resolve_redirect(const std::string& current, const std::string& location) {
    if (location.find("://") != std::string::npos) return location;
    auto [origin, target] = split_url(current);
    if (location.rfind("//", 0) == 0) {
        return current.substr(0, current.find(':')) + ":" + location;
    }
    if (!location.empty() && location.front() == '/') return origin + location;
    const size_t query = target.find('?');
    if (query != std::string::npos) target.erase(query);
    if (!location.empty() && location.front() == '?') return origin + target + location;
    const size_t slash = target.rfind('/');
    return origin + target.substr(0, slash == std::string::npos ? 0 : slash + 1) + location;
}

std::string zstd_decompress_bounded(const std::string& encoded, int64_t max_bytes) {
    using DStream = std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)>;
    DStream stream(ZSTD_createDStream(), &ZSTD_freeDStream);
    if (!stream) throw std::runtime_error("external location zstd decoder allocation failed");
    const size_t initialized = ZSTD_initDStream(stream.get());
    if (ZSTD_isError(initialized)) {
        throw std::runtime_error("external location zstd decoder initialization failed");
    }

    ZSTD_inBuffer input{encoded.data(), encoded.size(), 0};
    std::array<char, 16 * 1024> chunk{};
    std::string decoded;
    size_t remaining = 1;
    while (input.pos < input.size || remaining != 0) {
        ZSTD_outBuffer output{chunk.data(), chunk.size(), 0};
        remaining = ZSTD_decompressStream(stream.get(), &output, &input);
        if (ZSTD_isError(remaining)) {
            throw std::runtime_error("external location zstd decompression failed");
        }
        if (output.pos > static_cast<size_t>(max_bytes) ||
            decoded.size() > static_cast<size_t>(max_bytes) - output.pos) {
            throw std::runtime_error("external location exceeds max_decompressed_bytes (" +
                                     std::to_string(max_bytes) + ")");
        }
        decoded.append(chunk.data(), output.pos);
        if (input.pos == input.size && remaining != 0 && output.pos == 0) {
            throw std::runtime_error("external location contains a truncated zstd payload");
        }
    }
    return decoded;
}

// Expiry of a URL minted now with `ttl_seconds` to live, in microseconds since
// the Unix epoch.
int64_t expiry_us(int ttl_seconds) {
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    return now + static_cast<int64_t>(ttl_seconds) * 1000000;
}

// Standard (padded) base64, the encoding `x-amz-checksum-sha256` takes.
std::string base64_std(const uint8_t* data, size_t len) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((len + 2) / 3 * 4);
    for (size_t i = 0; i < len; i += 3) {
        const uint32_t n = (uint32_t{data[i]} << 16) |
                           (i + 1 < len ? uint32_t{data[i + 1]} << 8 : 0) |
                           (i + 2 < len ? uint32_t{data[i + 2]} : 0);
        out.push_back(kAlphabet[(n >> 18) & 63]);
        out.push_back(kAlphabet[(n >> 12) & 63]);
        out.push_back(i + 1 < len ? kAlphabet[(n >> 6) & 63] : '=');
        out.push_back(i + 2 < len ? kAlphabet[n & 63] : '=');
    }
    return out;
}

// The base64 form of a lowercase-hex SHA-256 the caller already validated.
std::string sha256_hex_to_base64(const std::string& sha256_hex) {
    const auto raw = crypto::hex_decode(sha256_hex);
    if (!raw || raw->size() != 32) {
        throw std::invalid_argument("upload sha256 must be 64 lowercase hex characters");
    }
    return base64_std(raw->data(), raw->size());
}

}  // namespace external_internal

using namespace external_internal;

namespace {

#if defined(VGI_RPC_WITH_S3) || defined(VGI_RPC_WITH_GCS)
// A random object key under the configured prefix.  The extension records the
// coding so a human browsing the bucket can tell what is in there.
//
// Guarded, because every caller is: the HTTP backend takes its keys from the
// server's own upload-URL response, so a default build never mints one.
std::string make_key(const std::string& prefix, const std::string& content_encoding) {
    auto bytes = crypto::random_bytes(16);
    std::string key = prefix;
    if (!key.empty() && key.back() != '/') key += '/';
    key += crypto::hex_encode(bytes.data(), bytes.size());
    key += content_encoding == "zstd" ? ".arrow.zst" : ".arrow";
    return key;
}

// A random key for an object-API write (`put_object`): opaque bytes, not an
// Arrow stream, so it says so.
std::string make_object_key(const std::string& prefix) {
    auto bytes = crypto::random_bytes(16);
    std::string key = prefix;
    if (!key.empty() && key.back() != '/') key += '/';
    key += crypto::hex_encode(bytes.data(), bytes.size());
    key += ".bin";
    return key;
}
#endif

// ---------------------------------------------------------------------------
// HTTP backend
// ---------------------------------------------------------------------------

// Speaks the four-endpoint contract of vgi_rpc.conformance.fake_storage:
// POST /alloc, then PUT / HEAD / GET on the returned object URL.  Real
// deployments front an object store with something equivalent, or use the
// cloud backends below.
class HttpExternalStorage final : public ExternalStorage, public ObjectStore {
public:
    HttpExternalStorage(std::string base_url, ExternalStorageConfig config)
        : base_url_(std::move(base_url)), config_(std::move(config)) {
        while (!base_url_.empty() && base_url_.back() == '/') base_url_.pop_back();
    }

    std::string upload(const std::string& data, const std::string& content_encoding) override {
        const UploadUrl urls = allocate(content_encoding);
        auto [origin, path] = split_url(urls.upload_url);
        auto client = make_client(origin, config_);

        httplib::Headers headers{{"Accept-Encoding", "identity"}};
        if (!content_encoding.empty()) headers.emplace("Content-Encoding", content_encoding);

        auto res = client.Put(path, headers, data, "application/octet-stream");
        for (int attempt = 1; transient(res) && attempt < attempts(config_); ++attempt) {
            storage_retry_backoff(attempt);
            res = client.Put(path, headers, data, "application/octet-stream");
        }
        if (!res || (res->status != 204 && res->status != 200)) {
            throw std::runtime_error(
                "external storage upload failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        return urls.download_url;
    }

    std::string fetch(const std::string& url) override {
        return fetch_url(url, config_.max_fetch_bytes, /*object_get=*/false);
    }

    // The object API (STATE_REF_V1.md §10.1) over the fixture contract:
    // `POST /alloc` with `purpose: "object"` plus the size and digest, then a
    // verified PUT -- the same shape as a client's sized grant.
    std::string put_object(const std::string& data) override {
        const auto digest =
            crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
        nlohmann::json body = nlohmann::json::object();
        body["size"] = static_cast<int64_t>(data.size());
        body["sha256_b64"] = base64_std(digest.data(), digest.size());
        body["purpose"] = "object";
        const UploadUrl urls = allocate(body);
        if (urls.object_id.empty()) {
            throw std::runtime_error("external storage alloc returned no object_id");
        }
        auto [origin, path] = split_url(urls.upload_url);
        auto client = make_client(origin, config_);
        httplib::Headers headers{{"Accept-Encoding", "identity"}};
        std::string content_type = "application/octet-stream";
        for (const auto& [name, value] : urls.headers) {
            std::string lower = name;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (lower == "content-length") continue;
            if (lower == "content-type") {
                content_type = value;
                continue;
            }
            headers.emplace(name, value);
        }
        auto res = client.Put(path, headers, data, content_type);
        for (int attempt = 1; transient(res) && attempt < attempts(config_); ++attempt) {
            storage_retry_backoff(attempt);
            res = client.Put(path, headers, data, content_type);
        }
        if (!res || (res->status != 204 && res->status != 200)) {
            throw std::runtime_error(
                "external storage object upload failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        return urls.object_id;
    }

    std::string get(const std::string& object_id, int64_t max_bytes) override {
        if (object_id.empty() || object_id.find('/') != std::string::npos ||
            object_id.find('?') != std::string::npos || object_id.find('#') != std::string::npos) {
            throw std::invalid_argument("invalid object id");
        }
        return fetch_url(base_url_ + "/objects/" + object_id, max_bytes, /*object_get=*/true);
    }

    // One bounded GET with per-hop URL validation and redirect handling.
    // `object_get` is the object API's shape: the stored bytes verbatim (no
    // Content-Encoding is requested or accepted) and a final 404 reported as
    // ObjectMissingError rather than a generic failure.
    std::string fetch_url(const std::string& url, int64_t max_fetch, bool object_get) {
        if (max_fetch < 0) max_fetch = config_.max_fetch_bytes;
        std::string current = url;
        for (int redirects = 0;; ++redirects) {
            if (config_.url_validator) {
                try {
                    config_.url_validator(current);
                } catch (...) {
                    throw std::runtime_error(
                        "external location URL rejected [url: " + redact_url(current) + "]");
                }
            }

            auto [origin, path] = split_url(current);
            auto client = make_client(origin, config_);
            std::string body;
            std::string location;
            std::string content_encoding;
            int response_status = 0;
            bool cap_exceeded = false;
            httplib::Result res{nullptr, httplib::Error::Unknown};
            for (int attempt = 0; attempt < attempts(config_); ++attempt) {
                if (attempt > 0) storage_retry_backoff(attempt);
                body.clear();
                location.clear();
                content_encoding.clear();
                response_status = 0;
                // zstd is decoded below; a stored object may carry it whatever the
                // request said, so say so rather than leave the server guessing.
                res = client.Get(
                    path,
                    httplib::Headers{
                        {"Accept-Encoding", object_get ? "identity" : "zstd, identity"}},
                    [&](const httplib::Response& response) {
                        response_status = response.status;
                        location = response.get_header_value("Location");
                        content_encoding = response.get_header_value("Content-Encoding");
                        return true;
                    },
                    [&](const char* data, size_t length) {
                        if (is_redirect(response_status)) return true;
                        if (length > static_cast<size_t>(max_fetch) ||
                            body.size() > static_cast<size_t>(max_fetch) - length) {
                            cap_exceeded = true;
                            return false;
                        }
                        body.append(data, length);
                        return true;
                    });
                if (cap_exceeded || !transient(res)) break;
            }

            if (cap_exceeded) {
                throw std::runtime_error("external location exceeds max_fetch_bytes (" +
                                         std::to_string(max_fetch) + ")");
            }
            if (!res) {
                throw std::runtime_error(
                    "external location fetch failed [url: " + redact_url(current) + "]");
            }
            if (is_redirect(res->status)) {
                if (redirects >= config_.max_redirects) {
                    throw std::runtime_error("external location redirect limit exceeded (" +
                                             std::to_string(config_.max_redirects) + ")");
                }
                if (location.empty()) {
                    throw std::runtime_error(
                        "external location redirect has no Location header [url: " +
                        redact_url(current) + "]");
                }
                current = resolve_redirect(current, location);
                continue;
            }
            if (object_get && res->status == 404) {
                throw ObjectMissingError(
                    "storage object is gone (never uploaded, deleted, or "
                    "expired by lifecycle)");
            }
            if (res->status != 200) {
                throw std::runtime_error(
                    "external location fetch failed: " + std::to_string(res->status) +
                    " [url: " + redact_url(current) + "]");
            }

            std::transform(content_encoding.begin(), content_encoding.end(),
                           content_encoding.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (object_get) {
                // Object bytes are stored and verified verbatim; a coding the
                // request did not ask for would change what the digest covers.
                if (!content_encoding.empty() && content_encoding != "identity") {
                    throw std::runtime_error("storage object has unrequested Content-Encoding: " +
                                             content_encoding);
                }
                return body;
            }
            if (content_encoding == "zstd") {
                return zstd_decompress_bounded(body, config_.max_decompressed_bytes);
            }
            if (!content_encoding.empty() && content_encoding != "identity") {
                throw std::runtime_error("external location has unsupported Content-Encoding: " +
                                         content_encoding);
            }
            if (body.size() > static_cast<size_t>(config_.max_decompressed_bytes)) {
                throw std::runtime_error("external location exceeds max_decompressed_bytes (" +
                                         std::to_string(config_.max_decompressed_bytes) + ")");
            }
            return body;
        }
    }

    UploadUrl generate_upload_url(int64_t size, const std::string& sha256_hex) override {
        // The fixture's sized allocation: it then verifies the PUT like
        // S3/R2 and names the checksum header the client must send.
        nlohmann::json body = nlohmann::json::object();
        body["size"] = size;
        body["sha256_b64"] = sha256_hex_to_base64(sha256_hex);
        return allocate(body);
    }

private:
    UploadUrl allocate(const std::string& content_encoding) {
        nlohmann::json body = nlohmann::json::object();
        if (!content_encoding.empty()) body["content_encoding"] = content_encoding;
        return allocate(body);
    }

    UploadUrl allocate(const nlohmann::json& body) {
        auto [origin, _] = split_url(base_url_);
        auto client = make_client(origin, config_);

        auto res = client.Post("/alloc", httplib::Headers{{"Accept-Encoding", "identity"}},
                               body.dump(), "application/json");
        if (!res || res->status != 200) {
            throw std::runtime_error(
                "external storage alloc failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        require_identity_response(*res, "external storage alloc");
        // The storage server's bytes never reach the message: they are not
        // ours to quote, and nlohmann's own error text would.
        try {
            const auto allocation = nlohmann::json::parse(res->body);
            const std::string legacy = allocation.at("object_url").get<std::string>();
            UploadUrl url;
            url.upload_url = allocation.value("upload_url", legacy);
            url.download_url = allocation.value("download_url", legacy);
            url.expires_at_us = expiry_us(config_.signed_url_ttl_seconds);
            // Protocol 3.4: the id the object API and grants name the blob by.
            // Absent from an older storage service; the grant path refuses.
            if (auto id = allocation.find("object_id"); id != allocation.end() && id->is_string()) {
                url.object_id = id->get<std::string>();
            }
            // Headers the storage service signed into the PUT, if any; the
            // fixture's sized allocations name them so a client must send them.
            if (auto found = allocation.find("upload_headers");
                found != allocation.end() && !found->is_null()) {
                for (const auto& [name, value] : found->items()) {
                    url.headers[name] = value.get<std::string>();
                }
            }
            return url;
        } catch (const nlohmann::json::exception&) {
            throw std::runtime_error(
                "external storage alloc returned an invalid allocation (expected a JSON object "
                "with a string object_url)");
        }
    }

    std::string base_url_;
    ExternalStorageConfig config_;
};

// ---------------------------------------------------------------------------
// S3 backend
// ---------------------------------------------------------------------------

#ifdef VGI_RPC_WITH_S3

// The AWS SDK requires process-wide init/shutdown exactly once.  A static
// keeps that tied to first use rather than to a global constructor, which
// would pay the cost even in a build that never touches S3.
struct AwsRuntime {
    AwsRuntime() { Aws::InitAPI(options); }
    ~AwsRuntime() { Aws::ShutdownAPI(options); }
    Aws::SDKOptions options;
};

void ensure_aws_initialized() {
    static AwsRuntime runtime;
}

class S3ExternalStorage final : public ExternalStorage, public ObjectStore {
public:
    S3ExternalStorage(std::string bucket, std::string prefix, const ExternalStorageConfig& config)
        : bucket_(std::move(bucket)),
          prefix_(std::move(prefix)),
          ttl_(config.signed_url_ttl_seconds),
          config_(config) {
        ensure_aws_initialized();
        if (config.s3_checksum != "sha256" && config.s3_checksum != "none") {
            throw std::invalid_argument("s3_checksum must be \"sha256\" or \"none\"");
        }
        checksum_ = config.s3_checksum == "sha256";
        if (config.s3_access_key_id.empty() != config.s3_secret_access_key.empty()) {
            throw std::invalid_argument(
                "S3 access key id and secret access key must be set together");
        }
        if (!config.s3_session_token.empty() && config.s3_access_key_id.empty()) {
            throw std::invalid_argument("an S3 session token needs an access key id and secret");
        }
        // Explicit keys (with an optional session token), else the SDK's
        // default chain: env (AWS_ACCESS_KEY_ID / _SECRET_ACCESS_KEY /
        // _SESSION_TOKEN), the shared profile (role_arn + source_profile,
        // credential_process), SSO, web identity (AWS_WEB_IDENTITY_TOKEN_FILE
        // + AWS_ROLE_ARN), container credentials (ECS, EKS Pod Identity) and
        // IMDSv2.  The chain's providers refresh temporary credentials ahead
        // of expiry; nothing here caches them.
        if (!config.s3_access_key_id.empty()) {
            credentials_ = Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
                "vgi_rpc", Aws::String(config.s3_access_key_id.c_str()),
                Aws::String(config.s3_secret_access_key.c_str()),
                Aws::String(config.s3_session_token.c_str()));
        } else {
            credentials_ =
                Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("vgi_rpc");
        }
        Aws::Client::ClientConfiguration client_config;
        if (!config.region.empty()) client_config.region = config.region;
        // Connect, then a stall timeout: for the curl client requestTimeoutMs
        // is CURLOPT_LOW_SPEED_TIME (below lowSpeedLimit = 1 byte/s for that
        // long), and httpRequestTimeoutMs = 0 leaves no whole-request deadline.
        // The SDK's own retry strategy handles transient failures.
        client_config.connectTimeoutMs =
            1000L * (config.connect_timeout_seconds > 0 ? config.connect_timeout_seconds : 30);
        client_config.requestTimeoutMs = 1000L * stall_seconds(config);
        client_config.httpRequestTimeoutMs = 0;
        client_config.lowSpeedLimit = 1;
        if (!config.s3_addressing_style.empty() && config.s3_addressing_style != "path" &&
            config.s3_addressing_style != "virtual") {
            throw std::invalid_argument("s3_addressing_style must be \"path\" or \"virtual\"");
        }
        if (!config.endpoint_url.empty()) {
            client_config.endpointOverride = config.endpoint_url;
            // Many S3-compatible services reject the SDK's default flexible
            // checksums (an aws-chunked body with a CRC trailer).  Send them
            // only where an operation requires them -- this backend names
            // SHA-256 itself wherever it wants storage to verify -- unless the
            // operator chose otherwise in the environment.
            if (std::getenv("AWS_REQUEST_CHECKSUM_CALCULATION") == nullptr) {
                client_config.checksumConfig.requestChecksumCalculation =
                    Aws::Client::RequestChecksumCalculation::WHEN_REQUIRED;
            }
            if (std::getenv("AWS_RESPONSE_CHECKSUM_VALIDATION") == nullptr) {
                client_config.checksumConfig.responseChecksumValidation =
                    Aws::Client::ResponseChecksumValidation::WHEN_REQUIRED;
            }
        }
        // An S3-compatible service is usually addressed by path rather than
        // by a virtual host, which has no DNS entry locally.
        const bool virtual_addressing = config.s3_addressing_style.empty()
                                            ? config.endpoint_url.empty()
                                            : config.s3_addressing_style == "virtual";
        client_ = std::make_unique<Aws::S3::S3Client>(
            credentials_, client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            virtual_addressing);
        // Pre-signing is local, so a second client pointed at the public
        // endpoint signs the URLs clients receive (its host is part of the
        // signature) without ever sending a request.
        if (!config.s3_public_endpoint_url.empty() &&
            config.s3_public_endpoint_url != config.endpoint_url) {
            Aws::Client::ClientConfiguration public_config = client_config;
            public_config.endpointOverride = config.s3_public_endpoint_url;
            public_client_ = std::make_unique<Aws::S3::S3Client>(
                credentials_, public_config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, virtual_addressing);
        }
        if (config.s3_bucket_check == ExternalStorageConfig::BucketCheck::Startup) verify_bucket();
    }

    // HEAD the bucket once.  A missing bucket, an access refusal and a wrong
    // region each fail with what to fix (the region S3 reported, when it
    // did); never the credentials.
    void verify_bucket() {
        if (config_.s3_bucket_check == ExternalStorageConfig::BucketCheck::Never) return;
        std::call_once(verified_, [this] {
            Aws::S3::Model::HeadBucketRequest request;
            request.SetBucket(bucket_);
            auto outcome = client_->HeadBucket(request);
            if (outcome.IsSuccess()) return;
            const auto& error = outcome.GetError();
            const int status = static_cast<int>(error.GetResponseCode());
            std::string region;
            const auto& headers = error.GetResponseHeaders();
            if (auto found = headers.find("x-amz-bucket-region"); found != headers.end()) {
                region = std::string(found->second.c_str());
            }
            std::string what = "S3 bucket '" + bucket_ + "' check failed";
            if (status == 404) {
                what +=
                    ": the bucket does not exist (create it, or fix VGI_STORAGE_S3_BUCKET / "
                    "VGI_STORAGE_S3_ENDPOINT)";
            } else if (status == 403) {
                what +=
                    ": access denied (the credentials may not use this bucket; check the "
                    "credential source and its s3:ListBucket/GetObject/PutObject grants)";
            } else if (status == 301 || !region.empty()) {
                what += ": the bucket is in another region" +
                        (region.empty() ? std::string()
                                        : " (S3 reports '" + region +
                                              "'; set VGI_STORAGE_S3_REGION=" + region + ")");
            } else {
                what += ": HTTP " + std::to_string(status) + " " +
                        std::string(error.GetExceptionName().c_str()) + " " +
                        std::string(error.GetMessage().c_str());
            }
            verify_error_ = what;
        });
        if (!verify_error_.empty()) throw std::runtime_error(verify_error_);
    }

    std::string upload(const std::string& data, const std::string& content_encoding) override {
        verify_bucket();
        const std::string key = make_key(prefix_, content_encoding);

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_);
        request.SetKey(key);
        request.SetContentType("application/octet-stream");
        if (!content_encoding.empty()) request.SetContentEncoding(content_encoding);
        // A stringstream body rather than a preallocated stream buf: the SDK
        // needs a seekable stream, and the copy is small beside the upload.
        auto body = Aws::MakeShared<Aws::StringStream>("vgi_rpc");
        body->write(data.data(), static_cast<std::streamsize>(data.size()));
        request.SetBody(body);

        auto outcome = client_->PutObject(request);
        if (!outcome.IsSuccess()) {
            throw std::runtime_error("S3 upload failed: " +
                                     std::string(outcome.GetError().GetMessage()));
        }
        return presign_public("GET", key);
    }

    std::string fetch(const std::string& url) override {
        // A pointer's URL is pre-signed HTTPS, so this is a plain GET —
        // the same path a client takes, which keeps the two in agreement.
        return HttpExternalStorage(url, config_).fetch(url);
    }

    UploadUrl generate_upload_url(int64_t size, const std::string& sha256_hex) override {
        if (!checksum_) {
            // Upload v2 needs storage to enforce the SHA-256; without it the
            // server must not vend grants (HttpConfig::upload_grants = false).
            throw std::runtime_error(
                "upload grants are disabled: VGI_STORAGE_S3_CHECKSUM=none (storage does not "
                "enforce x-amz-checksum-sha256)");
        }
        verify_bucket();
        const std::string key = make_key(prefix_, "");
        const uint64_t ttl = effective_ttl(kUploadGrantMinSeconds);
        UploadUrl out;
        // The PUT signs the exact length and the SHA-256, so S3 itself refuses
        // any other body.  The checksum header goes back to the client; the
        // length rides the plain Content-Length it sends anyway.
        const std::string checksum = sha256_hex_to_base64(sha256_hex);
        Aws::Http::HeaderValueCollection signed_headers;
        signed_headers.emplace("content-length", std::to_string(size));
        signed_headers.emplace("x-amz-checksum-sha256", checksum);
        auto put = signer().GeneratePresignedUrl(bucket_, key, Aws::Http::HttpMethod::HTTP_PUT,
                                                 signed_headers, ttl);
        if (put.empty()) throw std::runtime_error("S3 pre-signing produced no URL");
        out.upload_url = std::string(put.c_str());
        // Distinct credentials per verb: a client given an upload URL
        // cannot use it to read anything else in the bucket.
        out.download_url = presign_public("GET", key);
        out.expires_at_us = expiry_us(static_cast<int>(ttl));
        out.headers.emplace("x-amz-checksum-sha256", checksum);
        out.object_id = key;
        return out;
    }

    // STATE_REF_V1.md §10.1: a PutObject signing the length and the SHA-256,
    // so S3 itself refuses a body that is not exactly these bytes.
    std::string put_object(const std::string& data) override {
        verify_bucket();
        const std::string key = make_object_key(prefix_);
        const auto digest =
            crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_);
        request.SetKey(key);
        request.SetContentType("application/octet-stream");
        request.SetContentLength(static_cast<long long>(data.size()));
        // With checksum "none" the service would not enforce it (or would
        // refuse the header); every reader verifies the SHA-256 it was given.
        if (checksum_) request.SetChecksumSHA256(base64_std(digest.data(), digest.size()));
        auto body = Aws::MakeShared<Aws::StringStream>("vgi_rpc");
        body->write(data.data(), static_cast<std::streamsize>(data.size()));
        request.SetBody(body);
        auto outcome = client_->PutObject(request);
        if (!outcome.IsSuccess()) {
            throw std::runtime_error("S3 object upload failed: " +
                                     std::string(outcome.GetError().GetMessage()));
        }
        return key;
    }

    // A presigned GET of the key, fetched through the same bounded, validated
    // HTTP path as every other storage read.  S3 answers 404 for a key that
    // is gone, which that path reports as ObjectMissingError.
    std::string get(const std::string& object_id, int64_t max_bytes) override {
        if (object_id.empty() || object_id.size() > 1024) {
            throw std::invalid_argument("invalid object id");
        }
        verify_bucket();
        // The worker's own fetch: signed for the internal endpoint, and
        // checked against it rather than the client-facing validator.
        const std::string url = presign_with(*client_, "GET", object_id);
        ExternalStorageConfig internal = config_;
        internal.url_validator = nullptr;
        return HttpExternalStorage(url, internal).fetch_url(url, max_bytes, /*object_get=*/true);
    }

private:
    // The client whose endpoint goes into URLs handed to clients.
    Aws::S3::S3Client& signer() { return public_client_ ? *public_client_ : *client_; }

    // contract §4: a pre-signed URL dies with the credentials that signed it,
    // so its lifetime is min(TTL, expiry - now - 60 s).
    // Seconds the signing credentials remain valid, or -1 when they carry
    // no known expiry (static keys; an explicit token without
    // VGI_STORAGE_S3_SESSION_EXPIRES).
    int64_t credential_seconds_left() {
        if (config_.s3_session_expires_epoch > 0) {
            const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            return config_.s3_session_expires_epoch - now;
        }
        const auto credentials = credentials_->GetAWSCredentials();
        const int64_t remaining_ms =
            (credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count();
        // Static keys carry the far-future default; only real expiries count.
        if (remaining_ms > 0 && remaining_ms < int64_t{10} * 365 * 24 * 3600 * 1000) {
            return remaining_ms / 1000;
        }
        return -1;
    }

    uint64_t effective_ttl(int64_t minimum = 0) {
        int64_t ttl = ttl_;
        int64_t left = credential_seconds_left();
        if (left >= 0 && minimum > 0 && left - 60 < minimum &&
            config_.s3_session_expires_epoch == 0) {
            // Addendum A.2: refresh once (the providers renew credentials
            // close to expiry on access), then sign for what remains.
            left = credential_seconds_left();
        }
        if (left >= 0) {
            ttl = std::min<int64_t>(ttl, left - 60);
            if (minimum > 0 && ttl < minimum && !short_warned_.exchange(true)) {
                std::fprintf(stderr,
                             "vgi-rpc: S3 credentials expire in %lld s; upload grants are signed "
                             "for %lld s, under the 15-minute minimum\n",
                             static_cast<long long>(left), static_cast<long long>(ttl));
            }
        }
        return static_cast<uint64_t>(std::max<int64_t>(ttl, 1));
    }

    std::string presign_with(Aws::S3::S3Client& client, const char* method,
                             const std::string& key) {
        auto url = client.GeneratePresignedUrl(bucket_, key,
                                               std::string(method) == "PUT"
                                                   ? Aws::Http::HttpMethod::HTTP_PUT
                                                   : Aws::Http::HttpMethod::HTTP_GET,
                                               effective_ttl());
        if (url.empty()) throw std::runtime_error("S3 pre-signing produced no URL");
        return std::string(url.c_str());
    }

    std::string presign_public(const char* method, const std::string& key) {
        return presign_with(signer(), method, key);
    }

    std::string bucket_;
    std::string prefix_;
    int ttl_;
    // Contract §4: grants must live at least 15 minutes.
    static constexpr int64_t kUploadGrantMinSeconds = 15 * 60;
    bool checksum_ = true;
    std::atomic<bool> short_warned_{false};
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_;
    std::unique_ptr<Aws::S3::S3Client> client_;
    std::unique_ptr<Aws::S3::S3Client> public_client_;
    std::once_flag verified_;
    std::string verify_error_;
    ExternalStorageConfig config_;
};

#endif  // VGI_RPC_WITH_S3

// ---------------------------------------------------------------------------
// GCS backend
// ---------------------------------------------------------------------------

#ifdef VGI_RPC_WITH_GCS

// A stall timeout on uploads and downloads (below one byte per second for
// that long), not a whole-transfer deadline; the library's retry policy
// handles transient failures.
google::cloud::storage::Client make_gcs_client(const ExternalStorageConfig& config) {
    namespace gcs = google::cloud::storage;
    const auto stall = std::chrono::seconds(stall_seconds(config));
    auto options = google::cloud::Options{}
                       .set<gcs::TransferStallTimeoutOption>(stall)
                       .set<gcs::TransferStallMinimumRateOption>(1)
                       .set<gcs::DownloadStallTimeoutOption>(stall)
                       .set<gcs::DownloadStallMinimumRateOption>(1);
    // VGI_STORAGE_GCS_ENDPOINT (an emulator or a private endpoint).
    if (!config.gcs_endpoint_url.empty()) {
        options.set<gcs::RestEndpointOption>(config.gcs_endpoint_url);
    }
    // VGI_STORAGE_GCS_CREDENTIALS_FILE; otherwise Application Default
    // Credentials (GOOGLE_APPLICATION_CREDENTIALS, workload identity, the
    // metadata server).
    if (!config.gcs_credentials_file.empty()) {
        std::ifstream file(config.gcs_credentials_file);
        if (!file) {
            throw std::invalid_argument(
                "cannot read the GCS credentials file named by "
                "VGI_STORAGE_GCS_CREDENTIALS_FILE");
        }
        std::stringstream contents;
        contents << file.rdbuf();
        options.set<google::cloud::UnifiedCredentialsOption>(
            google::cloud::MakeServiceAccountCredentials(contents.str()));
    }
    return gcs::Client(std::move(options));
}

class GcsExternalStorage final : public ExternalStorage, public ObjectStore {
public:
    GcsExternalStorage(std::string bucket, std::string prefix, const ExternalStorageConfig& config)
        : bucket_(std::move(bucket)),
          prefix_(std::move(prefix)),
          ttl_(config.signed_url_ttl_seconds),
          signing_account_(config.signing_account),
          client_(make_gcs_client(config)),
          config_(config) {
        if (config.gcs_verify_at_startup) {
            auto metadata = client_.GetBucketMetadata(bucket_);
            if (!metadata) {
                const auto& status = metadata.status();
                std::string what = "GCS bucket '" + bucket_ + "' check failed";
                if (status.code() == google::cloud::StatusCode::kNotFound) {
                    what +=
                        ": the bucket does not exist (create it, or fix VGI_STORAGE_GCS_BUCKET "
                        "/ VGI_STORAGE_GCS_ENDPOINT)";
                } else if (status.code() == google::cloud::StatusCode::kPermissionDenied) {
                    what +=
                        ": access denied (check the credentials and their storage.buckets.get "
                        "and storage.objects.* permissions)";
                } else {
                    what += ": " + status.message();
                }
                throw std::runtime_error(what);
            }
        }
    }

    std::string upload(const std::string& data, const std::string& content_encoding) override {
        namespace gcs = google::cloud::storage;
        const std::string key = make_key(prefix_, content_encoding);

        auto metadata =
            content_encoding.empty()
                ? client_.InsertObject(bucket_, key, data)
                : client_.InsertObject(
                      bucket_, key, data,
                      gcs::WithObjectMetadata(
                          gcs::ObjectMetadata().set_content_encoding(content_encoding)));
        if (!metadata) {
            throw std::runtime_error("GCS upload failed: " + metadata.status().message());
        }
        return sign("GET", key);
    }

    std::string fetch(const std::string& url) override {
        return HttpExternalStorage(url, config_).fetch(url);
    }

    UploadUrl generate_upload_url(int64_t size, const std::string& sha256_hex) override {
        (void)sha256_hex;  // GCS cannot verify SHA-256 at storage; the server's
                           // pointer check does.
        const std::string key = make_key(prefix_, "");
        // GCS signs the length instead: a range pinned to exactly `size`.
        const std::string range = std::to_string(size) + "," + std::to_string(size);
        UploadUrl out;
        out.upload_url = sign("PUT", key, range);
        out.download_url = sign("GET", key);
        out.expires_at_us = expiry_us(ttl_);
        out.headers.emplace("Content-Type", "application/octet-stream");
        out.headers.emplace("x-goog-content-length-range", range);
        out.object_id = key;
        return out;
    }

    // STATE_REF_V1.md §10.1.  GCS cannot check a SHA-256 at storage; the
    // client library sends the object's MD5 and CRC32C, which GCS verifies,
    // and every reader verifies the SHA-256 it was promised.
    std::string put_object(const std::string& data) override {
        const std::string key = make_object_key(prefix_);
        auto metadata = client_.InsertObject(bucket_, key, data);
        if (!metadata) {
            throw std::runtime_error("GCS object upload failed: " + metadata.status().message());
        }
        return key;
    }

    // A V4-signed GET of the key through the bounded, validated HTTP path;
    // a 404 is ObjectMissingError.
    std::string get(const std::string& object_id, int64_t max_bytes) override {
        if (object_id.empty() || object_id.size() > 1024) {
            throw std::invalid_argument("invalid object id");
        }
        const std::string url = sign("GET", object_id);
        return HttpExternalStorage(url, config_).fetch_url(url, max_bytes, /*object_get=*/true);
    }

private:
    std::string sign(const char* method, const std::string& key,
                     const std::string& content_length_range = {}) {
        namespace gcs = google::cloud::storage;
        // Without an explicit account the library derives the signing email
        // from the ambient credentials, which anonymous or impersonated ones
        // do not carry — hence the knob.
        const auto range_header =
            content_length_range.empty()
                ? gcs::AddExtensionHeaderOption()
                : gcs::AddExtensionHeader("x-goog-content-length-range", content_length_range);
        // A sized PUT also signs its content type, which the client then
        // must send verbatim.
        const auto content_type =
            content_length_range.empty()
                ? gcs::AddExtensionHeaderOption()
                : gcs::AddExtensionHeader("content-type", "application/octet-stream");
        auto url =
            signing_account_.empty()
                ? client_.CreateV4SignedUrl(method, bucket_, key,
                                            gcs::SignedUrlDuration(std::chrono::seconds(ttl_)),
                                            range_header, content_type)
                : client_.CreateV4SignedUrl(
                      method, bucket_, key, gcs::SignedUrlDuration(std::chrono::seconds(ttl_)),
                      gcs::SigningAccount(signing_account_), range_header, content_type);
        if (!url) {
            throw std::runtime_error("GCS pre-signing failed: " + url.status().message());
        }
        return *url;
    }

    std::string bucket_;
    std::string prefix_;
    int ttl_;
    std::string signing_account_;
    google::cloud::storage::Client client_;
    ExternalStorageConfig config_;
};

#endif  // VGI_RPC_WITH_GCS

}  // namespace

bool s3_storage_available() {
#ifdef VGI_RPC_WITH_S3
    return true;
#else
    return false;
#endif
}

bool gcs_storage_available() {
#ifdef VGI_RPC_WITH_GCS
    return true;
#else
    return false;
#endif
}

ExternalUpload upload_ipc_stream(const std::string& ipc_bytes, ExternalStorage& storage,
                                 const std::string& compression) {
    ExternalUpload out;
    // The digest covers the payload *before* compression, so a reader that
    // decompresses and then verifies is checking the same bytes the writer
    // hashed.
    const auto digest =
        crypto::sha256(reinterpret_cast<const uint8_t*>(ipc_bytes.data()), ipc_bytes.size());
    out.sha256 = crypto::hex_encode(digest.data(), digest.size());
    out.raw_bytes = static_cast<int64_t>(ipc_bytes.size());

    std::string encoding;
    if (compression == "zstd") {
        const size_t bound = ZSTD_compressBound(ipc_bytes.size());
        std::string compressed(bound, '\0');
        const size_t written =
            ZSTD_compress(compressed.data(), bound, ipc_bytes.data(), ipc_bytes.size(), 3);
        if (!ZSTD_isError(written)) {
            compressed.resize(written);
            encoding = "zstd";
            out.uploaded_bytes = static_cast<int64_t>(compressed.size());
            out.url = storage.upload(compressed, encoding);
            return out;
        }
    }
    out.uploaded_bytes = out.raw_bytes;
    out.url = storage.upload(ipc_bytes, encoding);
    return out;
}

namespace {

bool is_lower_hex_sha256(const std::string& value) {
    return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

}  // namespace

ExternalRef::ExternalRef(std::string url, std::optional<std::string> sha256)
    : url_(std::move(url)), sha256_(std::move(sha256)) {
    if (url_.empty()) throw std::invalid_argument("ExternalRef url must be non-empty");
    if (sha256_ && !is_lower_hex_sha256(*sha256_)) {
        throw std::invalid_argument(
            "ExternalRef sha256 must be 64 lowercase hex characters (or absent)");
    }
}

AnnotatedBatch ExternalRef::pointer_batch(
    const std::shared_ptr<arrow::Schema>& result_schema) const {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOCATION, url_);
    if (sha256_) md->Append(keys::LOCATION_SHA256, *sha256_);
    return AnnotatedBatch::with_metadata(make_empty_batch(result_schema), std::move(md));
}

ExternalRef publish_external(const std::shared_ptr<arrow::RecordBatch>& batch,
                             ExternalStorage& storage, const std::string& compression,
                             bool include_sha256) {
    if (!batch) throw std::invalid_argument("publish_external requires a result batch");
    if (batch->num_rows() != 1) {
        throw std::invalid_argument("publish_external expects a 1-row result batch, got " +
                                    std::to_string(batch->num_rows()) + " rows");
    }
    if (!compression.empty() && compression != "zstd") {
        throw std::invalid_argument("publish_external: unsupported compression '" + compression +
                                    "' (expected \"zstd\" or empty)");
    }
    // Exactly what the per-call externalizer uploads for a result: one IPC
    // stream holding the schema and this one batch.
    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(sink, batch->schema(), {AnnotatedBatch::data(batch)});
    const auto buffer = unwrap(sink->Finish());
    const std::string ipc_bytes(reinterpret_cast<const char*>(buffer->data()),
                                static_cast<size_t>(buffer->size()));
    const ExternalUpload uploaded = upload_ipc_stream(ipc_bytes, storage, compression);
    return ExternalRef(uploaded.url,
                       include_sha256 ? std::optional<std::string>(uploaded.sha256) : std::nullopt);
}

std::unique_ptr<ExternalStorage> make_external_storage(const ExternalStorageConfig& config) {
    auto [scheme, rest] = split_scheme(config.uri);
    if (config.max_fetch_bytes < 0 || config.max_decompressed_bytes < 0 ||
        config.max_redirects < 0) {
        throw std::invalid_argument("external fetch limits must be non-negative");
    }

    if (scheme == "http" || scheme == "https") {
        return std::make_unique<HttpExternalStorage>(config.uri, config);
    }

    if (scheme == "s3") {
        auto [bucket, prefix] = split_bucket(rest);
        if (bucket.empty()) throw std::invalid_argument("s3:// URI names no bucket: " + config.uri);
#ifdef VGI_RPC_WITH_S3
        return std::make_unique<S3ExternalStorage>(bucket, prefix, config);
#else
        // Refused at startup, not on the first large payload: an operator who
        // configured a bucket should learn now that this binary cannot reach it.
        throw std::invalid_argument(
            "s3:// external storage requires a build with the 's3' vcpkg feature "
            "(VCPKG_MANIFEST_FEATURES=s3 and -DVGI_RPC_WITH_S3=ON)");
#endif
    }

    if (scheme == "gs") {
        auto [bucket, prefix] = split_bucket(rest);
        if (bucket.empty()) throw std::invalid_argument("gs:// URI names no bucket: " + config.uri);
#ifdef VGI_RPC_WITH_GCS
        return std::make_unique<GcsExternalStorage>(bucket, prefix, config);
#else
        throw std::invalid_argument(
            "gs:// external storage requires a build with the 'gcs' vcpkg feature "
            "(VCPKG_MANIFEST_FEATURES=gcs and -DVGI_RPC_WITH_GCS=ON)");
#endif
    }

    throw std::invalid_argument("unsupported external-storage scheme in: " + config.uri);
}

}  // namespace vgi_rpc
