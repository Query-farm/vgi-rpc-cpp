// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Direct object storage configured from the environment (direct-storage
/// contract v1, the same variables in every SDK).
///
/// A self-hosted worker can keep large values (externalized results, upload
/// grants / argument references, StateRef objects) in the operator's own
/// bucket -- AWS S3, any S3-compatible service, or GCS -- instead of the blob
/// vendor.  The worker signs URLs itself.  `configure_http_from_env`
/// (`vgi_rpc/vendor.h`) applies it.
///
/// `VGI_STORAGE_BACKEND` = `vendor` | `s3` | `gcs` | `none`.  Unset: `vendor`
/// if `VGI_BLOB_VENDOR_URL` is set, else `s3` if `VGI_STORAGE_S3_BUCKET` is,
/// else `gcs` if `VGI_STORAGE_GCS_BUCKET` is, else `none`.  More than one of
/// those three set without an explicit backend is refused.
///
/// S3: `VGI_STORAGE_S3_BUCKET`, `_PREFIX`, `_ENDPOINT`, `_REGION` (else
/// `AWS_REGION`, `AWS_DEFAULT_REGION`, `us-east-1`), `_FORCE_PATH_STYLE`
/// (default true with an endpoint), `_PUBLIC_ENDPOINT`, `_CHECKSUM`
/// (`sha256` | `none`), `_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` /
/// `_SESSION_TOKEN` (else the AWS default chain), `_SESSION_EXPIRES` (RFC 3339
/// or epoch seconds; caps URL lifetimes for an explicit token), `_VERIFY`.
/// GCS: `VGI_STORAGE_GCS_BUCKET`, `_PREFIX`, `_ENDPOINT`, `_CREDENTIALS_FILE`,
/// `_VERIFY`.
///
/// Objects go under `<prefix>rpc/` (results, upload grants, StateRef and
/// argument objects).  With `_PUBLIC_ENDPOINT` the worker must still be able
/// to reach the public endpoint: request-upload pointers are fetched by URL
/// (argument references and StateRefs go by object id and are unaffected).
/// Both: `VGI_STORAGE_URL_TTL_SECONDS` (default 3600, at most 7 days) and
/// `VGI_STORAGE_ALLOW_PLAIN_HTTP` (accept `http://` pre-signed URLs).
///
/// The worker never deletes from the bucket: give it a lifecycle rule that
/// expires `<prefix>rpc/` after one day (and `<prefix>fs/`, function storage
/// in SDKs that have it, after two).
#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"
#include "vgi_rpc/external.h"

namespace vgi_rpc {

namespace storage_env {
inline constexpr const char* BACKEND = "VGI_STORAGE_BACKEND";
inline constexpr const char* S3_BUCKET = "VGI_STORAGE_S3_BUCKET";
inline constexpr const char* S3_PREFIX = "VGI_STORAGE_S3_PREFIX";
inline constexpr const char* S3_ENDPOINT = "VGI_STORAGE_S3_ENDPOINT";
inline constexpr const char* S3_REGION = "VGI_STORAGE_S3_REGION";
inline constexpr const char* S3_FORCE_PATH_STYLE = "VGI_STORAGE_S3_FORCE_PATH_STYLE";
inline constexpr const char* S3_PUBLIC_ENDPOINT = "VGI_STORAGE_S3_PUBLIC_ENDPOINT";
inline constexpr const char* S3_CHECKSUM = "VGI_STORAGE_S3_CHECKSUM";
inline constexpr const char* S3_ACCESS_KEY_ID = "VGI_STORAGE_S3_ACCESS_KEY_ID";
inline constexpr const char* S3_SECRET_ACCESS_KEY = "VGI_STORAGE_S3_SECRET_ACCESS_KEY";
inline constexpr const char* S3_SESSION_TOKEN = "VGI_STORAGE_S3_SESSION_TOKEN";
inline constexpr const char* S3_VERIFY = "VGI_STORAGE_S3_VERIFY";
inline constexpr const char* S3_SESSION_EXPIRES = "VGI_STORAGE_S3_SESSION_EXPIRES";
inline constexpr const char* GCS_BUCKET = "VGI_STORAGE_GCS_BUCKET";
inline constexpr const char* GCS_PREFIX = "VGI_STORAGE_GCS_PREFIX";
inline constexpr const char* GCS_ENDPOINT = "VGI_STORAGE_GCS_ENDPOINT";
inline constexpr const char* GCS_CREDENTIALS_FILE = "VGI_STORAGE_GCS_CREDENTIALS_FILE";
inline constexpr const char* GCS_VERIFY = "VGI_STORAGE_GCS_VERIFY";
inline constexpr const char* URL_TTL_SECONDS = "VGI_STORAGE_URL_TTL_SECONDS";
inline constexpr const char* ALLOW_PLAIN_HTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP";
}  // namespace storage_env

enum class StorageBackend { Vendor, S3, Gcs, None };

/// `VGI_STORAGE_BACKEND`, or the backend the set variables imply (contract
/// §1).  Throws `std::invalid_argument` for an unknown value or a conflict.
VGI_RPC_EXPORT StorageBackend storage_backend_from_env();

/// The `s3` / `gcs` settings, parsed and validated but not connected.
struct DirectStorageSettings {
    StorageBackend backend = StorageBackend::None;
    /// Ready for `make_external_storage` (uri `s3://bucket/prefix` or
    /// `gs://bucket/prefix`, endpoints, credentials, checksum, TTL).
    ExternalStorageConfig config;
    /// False with `VGI_STORAGE_S3_CHECKSUM=none`: the server issues no upload
    /// grants (`HttpConfig::upload_grants`).
    bool upload_grants = true;
    /// `VGI_STORAGE_ALLOW_PLAIN_HTTP`.
    bool allow_plain_http = false;
    /// Startup warnings (an explicit session token without an expiry).
    std::vector<std::string> warnings;
    /// One secret-free line for the startup log.
    std::string describe() const;
};

/// The settings for `VGI_STORAGE_BACKEND` `s3` / `gcs`; empty for `vendor`
/// / `none`.  Throws `std::invalid_argument` naming the variable for a
/// missing bucket, a malformed value, credentials set by halves, or
/// pre-signed URLs that would be `http://` without
/// `VGI_STORAGE_ALLOW_PLAIN_HTTP=1`.
VGI_RPC_EXPORT std::optional<DirectStorageSettings> direct_storage_settings_from_env();

/// A URL validator for inbound pointer fetches, pinned to the host(s) the
/// settings sign URLs for; `https` only unless `allow_plain_http`.
VGI_RPC_EXPORT std::function<void(const std::string&)> direct_storage_url_validator(
    const DirectStorageSettings& settings);

}  // namespace vgi_rpc
