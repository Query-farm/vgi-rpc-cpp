// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/external.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <stdexcept>

namespace vgi_rpc {

namespace {

// Split "http://host:port/path" into a client target and the path, since
// httplib wants the origin and the path separately.
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
    return client;
}

}  // namespace

ExternalStorage::ExternalStorage(std::string base_url) : base_url_(std::move(base_url)) {
    while (!base_url_.empty() && base_url_.back() == '/') base_url_.pop_back();
}

std::string ExternalStorage::allocate(const std::string& content_encoding) {
    auto [origin, _] = split_url(base_url_);
    auto client = make_client(origin);

    nlohmann::json body = nlohmann::json::object();
    if (!content_encoding.empty()) body["content_encoding"] = content_encoding;

    auto res = client.Post("/alloc", body.dump(), "application/json");
    if (!res || res->status != 200) {
        throw std::runtime_error("external storage alloc failed: " +
                                 (res ? std::to_string(res->status) : std::string("no response")));
    }
    auto parsed = nlohmann::json::parse(res->body);
    return parsed.at("object_url").get<std::string>();
}

void ExternalStorage::upload(const std::string& url, const std::string& data,
                             const std::string& content_encoding) {
    auto [origin, path] = split_url(url);
    auto client = make_client(origin);

    httplib::Headers headers;
    if (!content_encoding.empty()) headers.emplace("Content-Encoding", content_encoding);

    auto res = client.Put(path, headers, data, "application/octet-stream");
    if (!res || (res->status != 204 && res->status != 200)) {
        throw std::runtime_error("external storage upload failed: " +
                                 (res ? std::to_string(res->status) : std::string("no response")));
    }
}

std::string ExternalStorage::fetch(const std::string& url) {
    auto [origin, path] = split_url(url);
    auto client = make_client(origin);
    auto res = client.Get(path);
    if (!res || res->status != 200) {
        throw std::runtime_error("external storage fetch failed: " +
                                 (res ? std::to_string(res->status) : std::string("no response")));
    }
    return res->body;
}

std::vector<UploadUrlPair> ExternalStorage::upload_urls(int64_t count) {
    std::vector<UploadUrlPair> out;
    out.reserve(static_cast<size_t>(count));
    for (int64_t i = 0; i < count; ++i) {
        // The store vends one URL used for both directions; the protocol
        // keeps them separate because a real signer issues distinct
        // credentials per verb.
        const std::string url = allocate();
        out.push_back(UploadUrlPair{url, url});
    }
    return out;
}

}  // namespace vgi_rpc
