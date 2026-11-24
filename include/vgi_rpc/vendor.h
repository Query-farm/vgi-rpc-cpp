// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Blob-vendor client: region-pinned, single-object pre-signed URLs.
///
/// The blob vendor is a small HTTP service that holds the object-storage
/// credentials so workers never do.  A worker asks it for a pre-signed URL for
/// one object of a declared size and SHA-256, uploads straight to storage with
/// that URL, and hands a download URL (or an object id) to whoever needs the
/// bytes.  Storage enforces the signed Content-Length and
/// `x-amz-checksum-sha256`, so whatever sits at the object's key is exactly the
/// declared bytes.
///
/// `VendorStorage` is every storage role protocol 3.4 asks of a backend:
/// `ExternalStorage` (results over the externalization threshold, client
/// upload URLs for requests over the request cap, inbound request pointers)
/// and `ObjectStore` (argument references resolve by object id).
///
/// Every `/v1` request carries `Authorization: Bearer <token>`.  Signed-URL
/// requests carry exactly the headers the grant names, never the token.
/// Neither the token nor a signed query string is ever logged or put in an
/// error: diagnostics name a URL by scheme, host, port and path only.
///
/// Mirrors vgi-rpc-python's `vgi_rpc.vendor` and vgi-rpc-go's `vendor.go`; the
/// wire format is the vendor's (vgi `docs/testing/blob-test-catalog.md`).
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/external.h"

namespace vgi_rpc {

struct HttpConfig;

/// Environment read by `vendor_storage_from_env` and `configure_http_from_env`.
namespace vendor_env {
/// The vendor's base URL.  Unset: no vendor.
inline constexpr const char* BLOB_VENDOR_URL = "VGI_BLOB_VENDOR_URL";
/// The vendor's bearer token.
inline constexpr const char* BLOB_VENDOR_TOKEN = "VGI_BLOB_VENDOR_TOKEN";
/// Consulted when `VGI_BLOB_VENDOR_TOKEN` is unset.
inline constexpr const char* BLOB_VENDOR_TOKEN_FALLBACK = "VGI_CF_DO_TOKEN";
/// Result batches at or above this many bytes are externalized (default 1 MiB).
inline constexpr const char* EXTERNALIZE_THRESHOLD_BYTES = "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES";
/// `none` (default) or `zstd`.
inline constexpr const char* EXTERNAL_COMPRESSION = "VGI_RPC_EXTERNAL_COMPRESSION";
/// The advertised client-upload cap; unset means the inbound fetch cap.
inline constexpr const char* MAX_UPLOAD_BYTES = "VGI_RPC_MAX_UPLOAD_BYTES";
/// The inbound fetch cap; unset means 256 MiB raised to the upload cap.
inline constexpr const char* MAX_FETCH_BYTES = "VGI_RPC_MAX_FETCH_BYTES";
/// The inline request cap a server advertises and enforces.
inline constexpr const char* MAX_REQUEST_BYTES = "VGI_RPC_MAX_REQUEST_BYTES";
}  // namespace vendor_env

/// The advisory `origin` on a grant minted for a client upload
/// (`__upload_url__`).  The vendor ignores it; the fake vendor counts by it.
inline constexpr const char* CLIENT_UPLOAD_ORIGIN = "client_upload";
/// The advisory `origin` on a grant for a stored object (`put_object`).
inline constexpr const char* STATE_REF_ORIGIN = "state_ref";

/// A failed blob-vendor or signed-URL request.  Its message never contains the
/// bearer token or a signed query string.
class VGI_RPC_EXPORT VendorError : public std::runtime_error {
public:
    VendorError(const std::string& message, int status = 0, std::string code = {})
        : std::runtime_error(message), status_(status), code_(std::move(code)) {}
    /// HTTP status of the failing response; 0 for a transport failure.
    int status() const noexcept { return status_; }
    /// The vendor's `error` code or the storage's S3 `<Code>`, when known.
    const std::string& code() const noexcept { return code_; }

private:
    int status_;
    std::string code_;
};

/// Tuning for a vendor client.  The defaults match the other ports.
struct VendorOptions {
    /// Idle timeout of the vendor's own JSON calls (/v1/info, grants).
    int timeout_seconds = 30;
    /// Signed-URL PUT/GET stall timeout: fails only when no bytes move for
    /// this long; no whole-request deadline.  Zero means
    /// VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS, else 30.
    int stall_timeout_seconds = 0;
    /// TCP + TLS connect timeout of every request.
    int connect_timeout_seconds = 30;
    /// Transport errors, 429 and 5xx are retried with exponential backoff.
    int max_attempts = 3;
    int backoff_ms = 250;
    /// How long the `/v1/info` host pin is cached.
    int info_ttl_seconds = 600;
    /// Cap on `fetch` of an inbound pointer (the server sets its fetch cap).
    int64_t max_fetch_bytes = 256LL * 1024 * 1024;
    int64_t max_decompressed_bytes = 4LL * 1024 * 1024 * 1024;
};

/// A URL validator accepting only URLs on `hosts`.  A URL passes when its
/// scheme is https (or http with `allow_http`), it carries no userinfo, and
/// its host and port are exactly one entry ("host", "host:port",
/// "[v6]:port"; no port matches the scheme's default only).  With `prefixes`,
/// the URL without its query must also start with one of them and its path may
/// not contain a ".." segment.  The validator throws `std::invalid_argument`.
VGI_RPC_EXPORT std::function<void(const std::string&)> pinned_hosts_validator(
    const std::vector<std::string>& hosts, bool allow_http,
    const std::vector<std::string>& prefixes = {});

/// `ExternalStorage` + `ObjectStore` backed by the blob vendor.  Safe for
/// concurrent use.  Objects are cleaned up by the bucket lifecycle; nothing
/// here deletes them.
class VGI_RPC_EXPORT VendorStorage final : public ExternalStorage, public ObjectStore {
public:
    /// `base_url` must be https (http only for a loopback host).  Throws
    /// `std::invalid_argument` for a bad URL or an empty token.
    VendorStorage(const std::string& base_url, std::string token, VendorOptions options = {});
    ~VendorStorage() override;

    /// Server-side externalization: a sized, checksummed grant (purpose
    /// `rpc`), a PUT with exactly its required headers, the download URL.
    std::string upload(const std::string& data, const std::string& content_encoding) override;
    /// An inbound request pointer: pinned to the vendor's hosts, bounded.
    std::string fetch(const std::string& url) override;
    /// A client upload grant (`__upload_url__`): purpose `rpc`, origin
    /// `client_upload`, signed for exactly `size` and `sha256_hex`.
    UploadUrl generate_upload_url(int64_t size, const std::string& sha256_hex) override;
    /// Store `data` as one `rpc` object (origin `state_ref`), return its id.
    std::string put_object(const std::string& data) override;
    /// Fetch an object by id: `/v1/blobs/get` mints the download, pinned and
    /// capped at `max_bytes`.  The vendor's `not_found`/`not_uploaded` and a
    /// storage 404 are `ObjectMissingError`.
    std::string get(const std::string& object_id, int64_t max_bytes) override;

    /// Throws `std::invalid_argument` unless `url` is on the vendor's pinned
    /// storage hosts (`/v1/info`, cached; a failed refresh keeps the last pin).
    void validate_url(const std::string& url);
    /// `validate_url` as a callable, for `HttpConfig::external_url_validator`.
    std::function<void(const std::string&)> url_validator();

    /// Sets the inbound fetch cap (the server's `max_fetch_bytes`).
    void set_max_fetch_bytes(int64_t bytes) noexcept { options_.max_fetch_bytes = bytes; }

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
    VendorOptions options_;
};

/// A `VendorStorage` from `VGI_BLOB_VENDOR_URL` and `VGI_BLOB_VENDOR_TOKEN`
/// (else `VGI_CF_DO_TOKEN`); null when the URL is unset.  Throws
/// `std::invalid_argument` when the URL is set but no token is.
VGI_RPC_EXPORT std::shared_ptr<VendorStorage> vendor_storage_from_env();

/// Apply the environment-driven HTTP settings the other ports read
/// (vgi-rpc-go `ConfigureHTTPServerFromEnv`):
/// - `VGI_RPC_MAX_REQUEST_BYTES` sets `max_request_bytes` (advertised and
///   enforced) whenever it is set;
/// - with `VGI_BLOB_VENDOR_URL`, results are externalized through the vendor
///   (`VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`, default 1 MiB;
///   `VGI_RPC_EXTERNAL_COMPRESSION`), client upload URLs are vended from it,
///   inbound pointers are pinned to its hosts, and `VGI-Max-Upload-Bytes` is
///   `VGI_RPC_MAX_UPLOAD_BYTES`, else the inbound fetch cap
///   (`VGI_RPC_MAX_FETCH_BYTES`, default 256 MiB raised to the upload cap).
/// A malformed or contradictory value throws `std::invalid_argument`.
/// Returns the vendor storage it installed, or null.
VGI_RPC_EXPORT std::shared_ptr<VendorStorage> configure_http_from_env(HttpConfig& config);

}  // namespace vgi_rpc
