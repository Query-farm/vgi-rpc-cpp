// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// External-location support: batches above a size threshold are uploaded to
/// object storage and replaced on the wire by a zero-row pointer batch, which
/// the client transparently re-fetches.  See docs/WIRE_PROTOCOL.md §12.
///
/// The URL a pointer batch carries is a **pre-signed HTTPS URL**, never a
/// bucket path.  That is what lets the client fetch the payload with no
/// credentials of its own and no cloud SDK linked in — the same reason the
/// upload-URL endpoint vends pre-signed PUT URLs rather than accepting the
/// bytes itself.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// One vended upload: a pre-signed PUT for exactly one body, and the GET the
/// server later fetches it from (WIRE_PROTOCOL.md §12, upload URLs v2).
struct UploadUrl {
    std::string upload_url;
    std::string download_url;
    /// Expiry of both URLs, microseconds since the Unix epoch (UTC).
    int64_t expires_at_us = 0;
    /// Headers the client MUST send on the PUT, verbatim.  Never contains
    /// Content-Length: the client sends exactly the declared size with a plain
    /// Content-Length, which the backend signature covers.
    std::map<std::string, std::string> headers;
    /// The storage id of the object this PUT creates (protocol 3.4,
    /// STATE_REF_V1.md §10.2): the S3/GCS key, or the fake storage's blob id.
    /// `ObjectStore::get` fetches it back by this id.  Every provider MUST set
    /// it; the server refuses a grant whose provider returned none.
    std::string object_id;
};

/// `ObjectStore::get` found no object under the id: never uploaded, deleted,
/// or expired by the bucket lifecycle.  Distinguishable from a transient
/// failure, which is any other exception.
class VGI_RPC_EXPORT ObjectMissingError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// The object API of protocol 3.4 (STATE_REF_V1.md §10.1): store bytes once
/// and read them back by id, with the download minted server-side.
///
/// A separate interface beside `ExternalStorage`, not new members of it, so a
/// third-party `ExternalStorage` keeps compiling.  "Storage is configured" for
/// argument references means the server's storage implements this; every
/// in-tree backend does.
class VGI_RPC_EXPORT ObjectStore {
public:
    virtual ~ObjectStore() = default;

    /// Store `data` as one `rpc`-purpose object and return its id (an S3/GCS
    /// key, the fake storage's blob id), at most 1024 bytes of UTF-8.  The
    /// write carries the size and SHA-256 of `data` where the backend can
    /// verify them.  Objects carry no TTL: operators set a bucket lifecycle
    /// rule of at least one day on the prefix.
    virtual std::string put_object(const std::string& data) = 0;

    /// Fetch the object `object_id` names, refusing one larger than
    /// `max_bytes` (the server's `max_fetch_bytes`).  The download is minted
    /// here from the id and goes through the same URL validation as every
    /// other storage fetch.  Throws `ObjectMissingError` when storage reports
    /// the object absent; any other failure throws something else.  Callers
    /// verify size and SHA-256 themselves.
    virtual std::string get(const std::string& object_id, int64_t max_bytes) = 0;
};

// Where a backend puts objects, and how long the URLs it hands out live.
struct ExternalStorageConfig {
    // s3://bucket/prefix, gs://bucket/prefix, or an http(s) base URL speaking
    // the four-endpoint contract of vgi_rpc.conformance.fake_storage.
    std::string uri;
    // Lifetime of the pre-signed URLs.  Long enough for a client to finish a
    // fetch, short enough that a leaked pointer batch stops working.
    int signed_url_ttl_seconds = 3600;
    // S3 only.  Empty means the SDK's own resolution (env, profile, IMDS).
    std::string region;
    // S3 only.  Set for an S3-compatible service such as MinIO, Ceph, Wasabi,
    // Backblaze B2, LocalStack or Cloudflare R2 (region "auto").  http:// is
    // accepted.  Pre-signed URLs carry this host.
    std::string endpoint_url;
    // S3 only.  "path" (https://endpoint/bucket/key) or "virtual"
    // (https://bucket.endpoint/key).  Empty: path with endpoint_url (MinIO and
    // Ceph need it; R2 accepts both), virtual for AWS.
    std::string s3_addressing_style;
    // GCS only.  The service account whose key signs URLs.  Normally derived
    // from the ambient credentials; naming it is required when they do not
    // carry a signing email — under impersonation, or against an emulator.
    std::string signing_account;
    // The encoded response and decoded Arrow IPC payload are bounded
    // independently.  A validator, when present, runs before the initial GET
    // and again before every redirect hop.
    int64_t max_fetch_bytes = 256LL * 1024 * 1024;
    int64_t max_decompressed_bytes = 4LL * 1024 * 1024 * 1024;
    int max_redirects = 5;
    std::function<void(const std::string&)> url_validator;
    // Storage HTTP timeouts.  A request fails only when no bytes move for
    // `stall_timeout_seconds` (each socket read or write, and the S3/GCS
    // SDKs' low-speed checks); there is no whole-request deadline, so a slow
    // transfer that keeps progressing is never cut off.  Zero means
    // VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS, else 30.
    int stall_timeout_seconds = 0;
    int connect_timeout_seconds = 30;
    // Attempts per storage request.  Connect errors, resets, stalls, 429 and
    // 5xx are retried with capped exponential backoff (0.25 s doubling, at
    // most 5 s, jittered); a retry resends the whole body.
    int max_attempts = 3;
};

// Overrides the default stall timeout (whole seconds, at least 1).
inline constexpr const char* STORAGE_STALL_TIMEOUT_ENV = "VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS";
// VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS, or 30 when unset or malformed.
VGI_RPC_EXPORT int storage_stall_timeout_seconds();
// Sleep before retry `attempt` (>= 1): 0.25 s doubling, capped at 5 s,
// jittered to [0.5, 1.0) of that.
VGI_RPC_EXPORT void storage_retry_backoff(int attempt);

// One object store.  Uploaded objects persist: configure a lifecycle rule on
// the bucket to expire them, because nothing here deletes them.
class VGI_RPC_EXPORT ExternalStorage {
public:
    virtual ~ExternalStorage() = default;

    // Store `data` and return the URL a client should fetch it from.
    virtual std::string upload(const std::string& data, const std::string& content_encoding) = 0;

    // Retrieve an object previously uploaded — by this server, or by a client
    // through a vended upload URL.
    virtual std::string fetch(const std::string& url) = 0;

    // Vend one upload/download URL pair for a client body of exactly `size`
    // bytes whose SHA-256 is `sha256_hex` (64 lowercase hex characters).
    // Backends that can sign the length and digest do, so storage itself
    // refuses a mismatched body; the server's pointer check catches the rest.
    virtual UploadUrl generate_upload_url(int64_t size, const std::string& sha256_hex) = 0;
};

// Build the backend named by `config.uri`, dispatching on its scheme.  Throws
// when the scheme is unknown, or when it names a backend this binary was not
// built with — failing at startup rather than on the first large payload.
VGI_RPC_EXPORT std::unique_ptr<ExternalStorage> make_external_storage(
    const ExternalStorageConfig& config);

// Whether this build can serve each scheme, for diagnostics and for the
// startup error above.
VGI_RPC_EXPORT bool s3_storage_available();
VGI_RPC_EXPORT bool gcs_storage_available();

// ---------------------------------------------------------------------------
// Uploading a serialized payload
// ---------------------------------------------------------------------------

// What one upload of a serialized IPC stream produced.
struct ExternalUpload {
    // Where a client fetches the object from.
    std::string url;
    // Lowercase hex SHA-256 of the raw IPC bytes, *before* compression -- so a
    // reader that decompresses and then verifies checks the bytes the writer
    // hashed.
    std::string sha256;
    // Byte count before compression.
    int64_t raw_bytes = 0;
    // Byte count actually uploaded: what a client will fetch, and what the
    // externalized-response cap is charged.
    int64_t uploaded_bytes = 0;
};

// Hash, optionally compress, and upload one complete Arrow IPC stream.
//
// The single choke point every server-side externalization path shares -- the
// HTTP transport's per-response and per-cycle externalizers, and
// `publish_external` -- so the bytes a pointer names are always produced the
// same way.  `compression` is "zstd" or empty (none); a zstd failure falls back
// to uploading the raw bytes, uncoded.
VGI_RPC_EXPORT ExternalUpload upload_ipc_stream(const std::string& ipc_bytes,
                                                ExternalStorage& storage,
                                                const std::string& compression);

// ---------------------------------------------------------------------------
// Pre-published references
// ---------------------------------------------------------------------------

/// A reference to an already-published unary result.
///
/// A unary handler may return `Result::from_external_ref(ref)` in place of its
/// value.  The server then answers with the external-location pointer batch for
/// `url()` directly -- a zero-row batch on the method's result schema carrying
/// `vgi_rpc.location`, plus `vgi_rpc.location.sha256` only when the ref has a
/// digest.  Nothing is serialized, compressed or uploaded during the call; the
/// pointer is written on every transport, whether or not the server has
/// external storage configured, regardless of `externalize_threshold`, never
/// through shared memory, and it is not charged against
/// `max_externalized_response_bytes`.  Clients resolve it like any other
/// pointer, so they need no change.
///
/// Build one with `publish_external`, or by hand for an object published out of
/// band.  The object at the URL must be an Arrow IPC stream (optionally
/// `Content-Encoding`-compressed) whose schema is the method's result schema and
/// which holds exactly one 1-row data batch.
///
/// The caller owns caching the ref and the object's lifecycle: a long-lived ref
/// must not point at an object under the short-TTL lifecycle rule used for
/// per-call uploads, and a pre-signed URL expires -- re-sign or rebuild the ref
/// before then.  Only return a ref to callers who are all entitled to the same
/// content.
class VGI_RPC_EXPORT ExternalRef {
public:
    /// Throws std::invalid_argument when `url` is empty or `sha256` is present
    /// but not 64 lowercase hex characters.  A ref without a digest omits
    /// `vgi_rpc.location.sha256`, so clients skip the content check -- the way
    /// to opt out for an object rewritten in place or too large to hash.
    explicit ExternalRef(std::string url, std::optional<std::string> sha256 = std::nullopt);

    const std::string& url() const noexcept { return url_; }
    const std::optional<std::string>& sha256() const noexcept { return sha256_; }

    /// The zero-row pointer batch announcing this ref on `result_schema`.
    AnnotatedBatch pointer_batch(const std::shared_ptr<arrow::Schema>& result_schema) const;

    bool operator==(const ExternalRef&) const = default;

private:
    std::string url_;
    std::optional<std::string> sha256_;
};

/// Publish a unary result batch once and return a reusable reference.
///
/// Serializes `batch` exactly as the per-call externalizer does (an IPC stream
/// of its schema plus this one batch), hashes the raw bytes, compresses when
/// `compression` is "zstd" (pass the server's
/// `HttpConfig::externalize_compression` to match it), and calls
/// `storage.upload` once.  Cache the returned ref and return it from the
/// handler with `Result::from_external_ref`.
///
/// Build `batch` against the method's result schema -- a single `result`
/// column holding the one value.  `include_sha256 = false` leaves the digest
/// off the ref, so clients skip the content check.
///
/// Throws std::invalid_argument when `batch` is null or does not have exactly
/// one row, or when `compression` is neither empty nor "zstd".
VGI_RPC_EXPORT ExternalRef publish_external(const std::shared_ptr<arrow::RecordBatch>& batch,
                                            ExternalStorage& storage,
                                            const std::string& compression = "",
                                            bool include_sha256 = true);

}  // namespace vgi_rpc
