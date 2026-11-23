// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Helpers shared by the storage backends in src/external.cpp and the
/// blob-vendor backend in src/vendor.cpp.  Not installed.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace vgi_rpc::external_internal {

std::pair<std::string, std::string> split_scheme(const std::string& uri);
std::pair<std::string, std::string> split_bucket(const std::string& rest);
/// scheme://authority/path, with userinfo, query and fragment dropped.
std::string redact_url(const std::string& url);
/// (origin, request target) of an absolute http(s) URL; throws otherwise.
std::pair<std::string, std::string> split_url(const std::string& url);
bool is_redirect(int status);
std::string resolve_redirect(const std::string& current, const std::string& location);
std::string zstd_decompress_bounded(const std::string& encoded, int64_t max_bytes);
int64_t expiry_us(int ttl_seconds);
/// Standard padded base64 (`x-amz-checksum-sha256`, the vendor `sha256_b64`).
std::string base64_std(const uint8_t* data, size_t len);
std::string sha256_hex_to_base64(const std::string& sha256_hex);

}  // namespace vgi_rpc::external_internal
