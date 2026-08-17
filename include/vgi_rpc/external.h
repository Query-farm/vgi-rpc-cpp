// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// External-location support: batches above a size threshold are uploaded to
/// object storage and replaced on the wire by a zero-row pointer batch, which
/// the client transparently re-fetches.  See docs/WIRE_PROTOCOL.md §12.
///
/// The backend here speaks the four-endpoint contract of
/// ``vgi_rpc.conformance.fake_storage`` — alloc, PUT, HEAD, GET.  Real
/// deployments point at S3 or GCS pre-signed URLs, which is the same shape.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct UploadUrlPair {
    std::string upload_url;
    std::string download_url;
};

// HTTP object-storage client.  Deliberately minimal: the wire contract is four
// endpoints, and anything richer would be storage-vendor specific.
class VGI_RPC_EXPORT ExternalStorage {
public:
    explicit ExternalStorage(std::string base_url);

    // Reserve an object and return the URL used for both PUT and GET.
    // `content_encoding` is recorded by the store and echoed on fetch.
    std::string allocate(const std::string& content_encoding = "");

    // Upload `data` to a URL returned by allocate().  Throws on failure —
    // a silently-inlined response would defeat the caps the operator set.
    void upload(const std::string& url, const std::string& data,
                const std::string& content_encoding = "");

    // Fetch a previously uploaded object.
    std::string fetch(const std::string& url);

    // Vend `count` upload/download URL pairs for client-side externalization.
    std::vector<UploadUrlPair> upload_urls(int64_t count);

    const std::string& base_url() const noexcept { return base_url_; }

private:
    std::string base_url_;
};

}  // namespace vgi_rpc
