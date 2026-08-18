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

#include <memory>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct UploadUrlPair {
    std::string upload_url;
    std::string download_url;
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
    // S3 only.  Set for an S3-compatible service such as MinIO or LocalStack.
    std::string endpoint_url;
    // GCS only.  The service account whose key signs URLs.  Normally derived
    // from the ambient credentials; naming it is required when they do not
    // carry a signing email — under impersonation, or against an emulator.
    std::string signing_account;
};

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

    // Vend `count` upload/download URL pairs for client-side externalization.
    virtual std::vector<UploadUrlPair> upload_urls(int64_t count) = 0;
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

}  // namespace vgi_rpc
