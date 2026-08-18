// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/external.h"

#include "vgi_rpc/crypto.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <stdexcept>
#include <utility>

#ifdef VGI_RPC_WITH_S3
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <sstream>
#endif

#ifdef VGI_RPC_WITH_GCS
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/signed_url_options.h>
#include <chrono>
#endif

namespace vgi_rpc {

namespace {

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

// Split an absolute URL into an httplib client target and the path.
std::pair<std::string, std::string> split_url(const std::string& url) {
    const size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        throw std::invalid_argument("not an absolute URL: " + url);
    }
    const size_t path_start = url.find('/', scheme_end + 3);
    if (path_start == std::string::npos) return {url, "/"};
    return {url.substr(0, path_start), url.substr(path_start)};
}

httplib::Client make_client(const std::string& origin) {
    httplib::Client client(origin);
    client.set_connection_timeout(10, 0);
    client.set_read_timeout(60, 0);
    client.set_write_timeout(60, 0);
    client.set_follow_location(true);
    return client;
}

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
#endif

// ---------------------------------------------------------------------------
// HTTP backend
// ---------------------------------------------------------------------------

// Speaks the four-endpoint contract of vgi_rpc.conformance.fake_storage:
// POST /alloc, then PUT / HEAD / GET on the returned object URL.  Real
// deployments front an object store with something equivalent, or use the
// cloud backends below.
class HttpExternalStorage final : public ExternalStorage {
public:
    explicit HttpExternalStorage(std::string base_url) : base_url_(std::move(base_url)) {
        while (!base_url_.empty() && base_url_.back() == '/') base_url_.pop_back();
    }

    std::string upload(const std::string& data, const std::string& content_encoding) override {
        const std::string url = allocate(content_encoding);
        auto [origin, path] = split_url(url);
        auto client = make_client(origin);

        httplib::Headers headers;
        if (!content_encoding.empty()) headers.emplace("Content-Encoding", content_encoding);

        auto res = client.Put(path, headers, data, "application/octet-stream");
        if (!res || (res->status != 204 && res->status != 200)) {
            throw std::runtime_error(
                "external storage upload failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        return url;
    }

    std::string fetch(const std::string& url) override {
        auto [origin, path] = split_url(url);
        auto client = make_client(origin);
        auto res = client.Get(path);
        if (!res || res->status != 200) {
            throw std::runtime_error(
                "external storage fetch failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        return res->body;
    }

    std::vector<UploadUrlPair> upload_urls(int64_t count) override {
        std::vector<UploadUrlPair> out;
        out.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; ++i) {
            // The store vends one URL used for both verbs; the protocol keeps
            // them separate because a real signer issues one credential per verb.
            const std::string url = allocate("");
            out.push_back(UploadUrlPair{url, url});
        }
        return out;
    }

private:
    std::string allocate(const std::string& content_encoding) {
        auto [origin, _] = split_url(base_url_);
        auto client = make_client(origin);
        nlohmann::json body = nlohmann::json::object();
        if (!content_encoding.empty()) body["content_encoding"] = content_encoding;

        auto res = client.Post("/alloc", body.dump(), "application/json");
        if (!res || res->status != 200) {
            throw std::runtime_error(
                "external storage alloc failed: " +
                (res ? std::to_string(res->status) : std::string("no response")));
        }
        return nlohmann::json::parse(res->body).at("object_url").get<std::string>();
    }

    std::string base_url_;
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

class S3ExternalStorage final : public ExternalStorage {
public:
    S3ExternalStorage(std::string bucket, std::string prefix, const ExternalStorageConfig& config)
        : bucket_(std::move(bucket)),
          prefix_(std::move(prefix)),
          ttl_(config.signed_url_ttl_seconds) {
        ensure_aws_initialized();
        Aws::Client::ClientConfiguration client_config;
        if (!config.region.empty()) client_config.region = config.region;
        if (!config.endpoint_url.empty()) {
            client_config.endpointOverride = config.endpoint_url;
            // An S3-compatible service is usually addressed by path rather
            // than by a virtual host, which has no DNS entry locally.
            client_ = std::make_unique<Aws::S3::S3Client>(
                client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                /*useVirtualAddressing=*/false);
        } else {
            client_ = std::make_unique<Aws::S3::S3Client>(client_config);
        }
    }

    std::string upload(const std::string& data, const std::string& content_encoding) override {
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
        return presign("GET", key);
    }

    std::string fetch(const std::string& url) override {
        // A pointer's URL is pre-signed HTTPS, so this is a plain GET —
        // the same path a client takes, which keeps the two in agreement.
        return HttpExternalStorage(url).fetch(url);
    }

    std::vector<UploadUrlPair> upload_urls(int64_t count) override {
        std::vector<UploadUrlPair> out;
        out.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; ++i) {
            const std::string key = make_key(prefix_, "");
            // Distinct credentials per verb: a client given an upload URL
            // cannot use it to read anything else in the bucket.
            out.push_back(UploadUrlPair{presign("PUT", key), presign("GET", key)});
        }
        return out;
    }

private:
    std::string presign(const char* method, const std::string& key) {
        auto url = client_->GeneratePresignedUrl(bucket_, key,
                                                 std::string(method) == "PUT"
                                                     ? Aws::Http::HttpMethod::HTTP_PUT
                                                     : Aws::Http::HttpMethod::HTTP_GET,
                                                 static_cast<uint64_t>(ttl_));
        if (url.empty()) throw std::runtime_error("S3 pre-signing produced no URL");
        return std::string(url.c_str());
    }

    std::string bucket_;
    std::string prefix_;
    int ttl_;
    std::unique_ptr<Aws::S3::S3Client> client_;
};

#endif  // VGI_RPC_WITH_S3

// ---------------------------------------------------------------------------
// GCS backend
// ---------------------------------------------------------------------------

#ifdef VGI_RPC_WITH_GCS

class GcsExternalStorage final : public ExternalStorage {
public:
    GcsExternalStorage(std::string bucket, std::string prefix, const ExternalStorageConfig& config)
        : bucket_(std::move(bucket)),
          prefix_(std::move(prefix)),
          ttl_(config.signed_url_ttl_seconds),
          signing_account_(config.signing_account),
          client_(google::cloud::storage::Client()) {}

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
        return HttpExternalStorage(url).fetch(url);
    }

    std::vector<UploadUrlPair> upload_urls(int64_t count) override {
        std::vector<UploadUrlPair> out;
        out.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; ++i) {
            const std::string key = make_key(prefix_, "");
            out.push_back(UploadUrlPair{sign("PUT", key), sign("GET", key)});
        }
        return out;
    }

private:
    std::string sign(const char* method, const std::string& key) {
        namespace gcs = google::cloud::storage;
        // Without an explicit account the library derives the signing email
        // from the ambient credentials, which anonymous or impersonated ones
        // do not carry — hence the knob.
        auto url =
            signing_account_.empty()
                ? client_.CreateV4SignedUrl(method, bucket_, key,
                                            gcs::SignedUrlDuration(std::chrono::seconds(ttl_)))
                : client_.CreateV4SignedUrl(method, bucket_, key,
                                            gcs::SignedUrlDuration(std::chrono::seconds(ttl_)),
                                            gcs::SigningAccount(signing_account_));
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

std::unique_ptr<ExternalStorage> make_external_storage(const ExternalStorageConfig& config) {
    auto [scheme, rest] = split_scheme(config.uri);

    if (scheme == "http" || scheme == "https") {
        return std::make_unique<HttpExternalStorage>(config.uri);
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
