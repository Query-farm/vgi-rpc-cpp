// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Upload URLs v2 (WIRE_PROTOCOL.md §12): `__upload_url__/init` takes one
// (size, sha256) entry per upload, the server signs each URL for exactly that
// body, request pointers must carry `vgi_rpc.location.sha256`, and the
// capability is advertised as `VGI-Upload-URL-Support: 2`.
//
// The shared Python conformance suite is the definitive test of this surface;
// these cases are the fast in-process loop for the server refusals and the
// client's v1 refusal, which need no reference worker.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/client_external.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/http_client.h"
#include "vgi_rpc/http_config.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/object_cache.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <functional>
#include <optional>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace vgi_rpc;
using Catch::Matchers::ContainsSubstring;

namespace {

constexpr const char* kProtocol = "UploadTest.v1";
constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";
constexpr int64_t kMaxUploadBytes = 1 << 20;

std::string sha256_hex(const std::string& data) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    return crypto::hex_encode(digest.data(), digest.size());
}

std::string sha256_b64(const std::string& data) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    for (size_t i = 0; i < digest.size(); i += 3) {
        const uint32_t n = (uint32_t{digest[i]} << 16) |
                           (i + 1 < digest.size() ? uint32_t{digest[i + 1]} << 8 : 0) |
                           (i + 2 < digest.size() ? uint32_t{digest[i + 2]} : 0);
        out.push_back(kAlphabet[(n >> 18) & 63]);
        out.push_back(kAlphabet[(n >> 12) & 63]);
        out.push_back(i + 1 < digest.size() ? kAlphabet[(n >> 6) & 63] : '=');
        out.push_back(i + 2 < digest.size() ? kAlphabet[n & 63] : '=');
    }
    return out;
}

std::shared_ptr<arrow::Schema> bytes_schema() {
    return arrow::schema({arrow::field("value", arrow::binary(), false)});
}

std::shared_ptr<arrow::Schema> result_schema() {
    return arrow::schema({arrow::field("result", arrow::binary(), false)});
}

std::shared_ptr<arrow::RecordBatch> bytes_batch(const std::string& value) {
    arrow::BinaryBuilder builder;
    REQUIRE(builder.Append(value).ok());
    return arrow::RecordBatch::Make(bytes_schema(), 1, {builder.Finish().ValueOrDie()});
}

std::string ipc_body(const std::shared_ptr<arrow::RecordBatch>& batch,
                     std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    AnnotatedBatch annotated;
    annotated.batch = batch;
    annotated.custom_metadata = std::move(metadata);
    write_ipc_stream(sink, batch->schema(), {annotated});
    auto buffer = sink->Finish().ValueOrDie();
    return buffer->ToString();
}

std::shared_ptr<arrow::KeyValueMetadata> routing(const std::string& method,
                                                 const std::string& protocol = "") {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::METHOD, method);
    metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    if (!protocol.empty()) metadata->Append(keys::PROTOCOL, protocol);
    return metadata;
}

struct Entry {
    int64_t size;
    std::string sha256;
};

/// A v2 `__upload_url__/init` body.
std::string init_v2(const std::vector<Entry>& entries) {
    auto type = arrow::struct_({arrow::field("size", arrow::int64(), false),
                                arrow::field("sha256", arrow::utf8(), false)});
    auto sizes = std::make_shared<arrow::Int64Builder>();
    auto hashes = std::make_shared<arrow::StringBuilder>();
    auto structs = std::make_shared<arrow::StructBuilder>(
        type, arrow::default_memory_pool(),
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{sizes, hashes});
    arrow::ListBuilder list(arrow::default_memory_pool(), structs, arrow::list(type));
    REQUIRE(list.Append().ok());
    for (const auto& entry : entries) {
        REQUIRE(structs->Append().ok());
        REQUIRE(sizes->Append(entry.size).ok());
        REQUIRE(hashes->Append(entry.sha256).ok());
    }
    auto schema = arrow::schema({arrow::field("uploads", arrow::list(type), false)});
    return ipc_body(arrow::RecordBatch::Make(schema, 1, {list.Finish().ValueOrDie()}),
                    routing("__upload_url__"));
}

/// The retired v1 body: a bare `count`.
std::string init_v1(int64_t count) {
    arrow::Int64Builder builder;
    REQUIRE(builder.Append(count).ok());
    auto schema = arrow::schema({arrow::field("count", arrow::int64(), true)});
    return ipc_body(arrow::RecordBatch::Make(schema, 1, {builder.Finish().ValueOrDie()}),
                    routing("__upload_url__"));
}

/// The error message of an RPC error response, or "" when it carries none.
std::string error_message(const std::string& body) {
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(body));
    auto contents = read_ipc_stream(reader);
    if (!contents) return {};
    for (const auto& batch : contents->batches) {
        if (!batch.custom_metadata) continue;
        const int index = batch.custom_metadata->FindKey(keys::LOG_MESSAGE);
        if (index >= 0) return batch.custom_metadata->value(index);
    }
    return {};
}

/// Object storage that verifies a sized PUT the way R2 does: the body must
/// be exactly the declared size and hash to the declared SHA-256.
class VerifyingStorage {
public:
    VerifyingStorage() {
        svr_.Post("/alloc", [this](const httplib::Request& req, httplib::Response& res) {
            const auto request = nlohmann::json::parse(req.body.empty() ? "{}" : req.body);
            const std::string id = std::to_string(next_id_++);
            nlohmann::json body;
            body["object_url"] = url() + "/blob/" + id;
            body["upload_url"] = url() + "/upload/" + id;
            body["download_url"] = url() + "/download/" + id;
            body["object_id"] = id;
            if (request.value("purpose", "") == "object") ++object_puts;
            if (request.contains("size")) {
                std::lock_guard<std::mutex> lock(mutex_);
                expected_[id] = {request.at("size").get<int64_t>(),
                                 request.at("sha256_b64").get<std::string>()};
                body["upload_headers"] = {{"x-amz-checksum-sha256", expected_[id].sha256_b64}};
            }
            res.set_content(body.dump(), "application/json; charset=utf-8");
        });
        svr_.Put(R"(/upload/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            const std::string id = req.matches[1];
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = expected_.find(id);
            const bool sized = found != expected_.end();
            std::string body = req.body;
            if (corrupt_next_ > 0 && !body.empty()) {
                --corrupt_next_;
                body[0] = static_cast<char>(body[0] ^ 0xff);
            }
            if (sized &&
                (static_cast<int64_t>(body.size()) != found->second.size ||
                 req.get_header_value("x-amz-checksum-sha256") != found->second.sha256_b64 ||
                 sha256_b64(body) != found->second.sha256_b64)) {
                ++refusals;
                res.status = 400;
                res.set_content("<Error><Code>BadDigest</Code></Error>", "application/xml");
                return;
            }
            objects_[id] = body;
            ++(sized ? sized_uploads : unsized_uploads);
            res.status = 204;
        });
        svr_.Get(R"(/download/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            ++downloads;
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = objects_.find(req.matches[1]);
            if (found == objects_.end()) {
                res.status = 404;
                return;
            }
            res.set_content(found->second, "application/octet-stream");
        });
        // The object API's `get(object_id)` (fake storage's /objects/{id}).
        svr_.Get(R"(/objects/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            ++object_gets;
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = objects_.find(req.matches[1]);
            if (found == objects_.end()) {
                res.status = 404;
                return;
            }
            res.set_content(found->second, "application/octet-stream");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        REQUIRE(port_ > 0);
        thread_ = std::thread([this]() { (void)svr_.listen_after_bind(); });
        svr_.wait_until_ready();
    }
    ~VerifyingStorage() {
        svr_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    /// Store `data` directly under a fresh id and return its download URL.
    std::string put_object(const std::string& data) {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::string id = std::to_string(next_id_++);
        objects_[id] = data;
        return url() + "/download/" + id;
    }

    void corrupt_next(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        corrupt_next_ = count;
    }

    // Client uploads through a sized URL, and the server's own (unsized)
    // writes of externalized responses.
    std::atomic<int> sized_uploads{0};
    std::atomic<int> unsized_uploads{0};
    std::atomic<int> refusals{0};
    std::atomic<int> downloads{0};
    std::atomic<int> object_puts{0};
    std::atomic<int> object_gets{0};

    void erase(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        objects_.erase(id);
    }

private:
    struct Expected {
        int64_t size;
        std::string sha256_b64;
    };
    httplib::Server svr_;
    std::thread thread_;
    int port_ = 0;
    std::atomic<int> next_id_{1};
    std::mutex mutex_;
    std::map<std::string, Expected> expected_;
    std::map<std::string, std::string> objects_;
    int corrupt_next_ = 0;
};

class UploadServer {
public:
    explicit UploadServer(const std::string& storage_url,
                          std::function<void(HttpConfig&)> configure = {}) {
        ServerBuilder builder;
        builder.protocol(kProtocol);
        // Resolves an argument reference for the calling principal and
        // returns the bytes (protocol 3.4).
        builder.add_unary("resolve_ref",
                          arrow::schema({arrow::field("ref", arrow::binary(), false),
                                         arrow::field("size", arrow::int64(), false),
                                         arrow::field("sha256", arrow::utf8(), false)}),
                          result_schema(), [](const Request& request, CallContext& ctx) -> Result {
                              const auto raw = request.get<std::vector<uint8_t>>("ref");
                              auto data = ctx.resolve_argument_ref(
                                  std::string(raw.begin(), raw.end()), request.get<int64_t>("size"),
                                  request.get<std::string>("sha256"));
                              arrow::BinaryBuilder out;
                              VGI_RPC_THROW_NOT_OK(out.Append(*data));
                              return Result::value(arrow::RecordBatch::Make(
                                  result_schema(), 1, {out.Finish().ValueOrDie()}));
                          });
        builder.add_unary(
            "echo_bytes", bytes_schema(), result_schema(),
            [](const Request& request, CallContext&) -> Result {
                auto value = request.batch()->column(0);
                return Result::value(arrow::RecordBatch::Make(result_schema(), 1, {value}));
            });
        server_ = builder.build();

        HttpConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.prefix = "/vgi";
        cfg.external_storage_url = storage_url;
        cfg.max_upload_bytes = kMaxUploadBytes;
        // Room for a 101-entry init body inline; the 64 KiB payloads below
        // still exceed it and are externalized.
        cfg.max_request_bytes = 32 * 1024;
        cfg.sticky_header_auth = true;
        if (configure) configure(cfg);
        cfg.on_listen = [this](int port) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                port_ = port;
            }
            ready_.notify_all();
        };
        thread_ = std::thread([this, cfg]() { server_->serve_http(cfg); });
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait_for(lock, std::chrono::seconds(10), [this]() { return port_ != 0; });
        REQUIRE(port_ != 0);
    }
    ~UploadServer() { thread_.detach(); }

    int port() const { return port_; }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    httplib::Result post(const std::string& path, const std::string& body) const {
        httplib::Client client("127.0.0.1", port_);
        client.set_read_timeout(10, 0);
        return client.Post(path, body, kArrowContentType);
    }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

HttpClient make_client(const std::string& url) {
    ClientExternalHttpOptions external;
    external.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
    HttpClientConfig config;
    config.prefix = "/vgi";
    config.protocol = kProtocol;
    return HttpClient::builder(url).config(config).external_http_options(external).build();
}

/// A deterministic payload far above the server's 4 KiB inline threshold.
std::string big_payload() {
    std::string payload(64 * 1024, '\0');
    uint32_t state = 0x9e3779b9U;
    for (char& byte : payload) {
        state = state * 1664525U + 1013904223U;
        byte = static_cast<char>(state >> 24);
    }
    return payload;
}

}  // namespace

TEST_CASE("the server advertises VGI-Upload-URL-Support: 2", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    httplib::Client client("127.0.0.1", server.port());
    auto res = client.Options("/vgi/health");
    REQUIRE(res);
    CHECK(res->get_header_value("VGI-Upload-URL-Support") == "2");
    CHECK(res->get_header_value("VGI-Max-Upload-Bytes") == std::to_string(kMaxUploadBytes));
}

TEST_CASE("a v2 request vends one sized URL per entry, in order", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    const std::string a = sha256_hex("a"), b = sha256_hex("bb");
    auto res = server.post("/vgi/__upload_url__/init", init_v2({{1, a}, {2, b}}));
    REQUIRE(res);
    REQUIRE(res->status == 200);
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(res->body));
    auto contents = read_ipc_stream(reader);
    REQUIRE(contents);
    REQUIRE(contents->batches.size() == 1);
    const auto& batch = contents->batches[0].batch;
    REQUIRE(batch->num_rows() == 2);
    REQUIRE(batch->schema()->field_names() ==
            std::vector<std::string>{"upload_url", "download_url", "expires_at", "upload_headers",
                                     "object_id", "ref"});
    CHECK(batch->schema()->field(3)->type()->Equals(arrow::map(arrow::utf8(), arrow::utf8())));
    CHECK_FALSE(batch->schema()->field(3)->nullable());
    auto headers = std::static_pointer_cast<arrow::MapArray>(batch->column(3));
    auto items = std::static_pointer_cast<arrow::StringArray>(headers->items());
    REQUIRE(headers->value_length(0) == 1);
    CHECK(items->GetString(headers->value_offset(0)) == sha256_b64("a"));
    CHECK(items->GetString(headers->value_offset(1)) == sha256_b64("bb"));
    auto expires = std::static_pointer_cast<arrow::TimestampArray>(batch->column(2));
    CHECK(expires->Value(0) > 0);
}

TEST_CASE("the retired count shape is refused", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    auto res = server.post("/vgi/__upload_url__/init", init_v1(1));
    REQUIRE(res);
    CHECK(res->status == 400);
    const std::string message = error_message(res->body);
    CHECK_THAT(message, ContainsSubstring("retired count shape"));
    CHECK_THAT(message, ContainsSubstring("VGI-Upload-URL-Support: 2"));
}

TEST_CASE("malformed v2 entries are refused before any URL is minted", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    const std::string good = sha256_hex("x");
    std::vector<Entry> too_many(101, Entry{1, good});
    const std::vector<std::pair<std::string, std::string>> cases = {
        {init_v2({}), "'uploads' must have 1-100 entries, got 0"},
        {init_v2(too_many), "'uploads' must have 1-100 entries, got 101"},
        {init_v2({{0, good}}), "uploads[0].size must be an integer >= 1, got 0"},
        {init_v2({{-5, good}}), "uploads[0].size must be an integer >= 1, got -5"},
        {init_v2({{1, "ABC"}}), "uploads[0].sha256 must be 64 lowercase hex characters"},
        {init_v2({{1, std::string(64, 'A')}}), "64 lowercase hex"},
        {init_v2({{1, good}, {1, "zz"}}), "uploads[1].sha256"},
        {init_v2({{kMaxUploadBytes + 1, good}}),
         "uploads[0].size 1048577 exceeds VGI-Max-Upload-Bytes (1048576)"},
    };
    for (const auto& [body, expected] : cases) {
        auto res = server.post("/vgi/__upload_url__/init", body);
        REQUIRE(res);
        CHECK(res->status == 400);
        CHECK_THAT(error_message(res->body), ContainsSubstring(expected));
    }
}

TEST_CASE("the client uploads through a sized URL and the pointer carries the sha",
          "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    auto client = make_client(server.url());
    const std::string payload = big_payload();
    auto result =
        client.call("echo_bytes", AnnotatedBatch::data(bytes_batch(payload)), result_schema());
    REQUIRE(result.batch);
    auto column = std::static_pointer_cast<arrow::BinaryArray>(result.batch->column(0));
    CHECK(column->GetString(0) == payload);
    CHECK(storage.sized_uploads.load() == 1);
    CHECK(storage.refusals.load() == 0);
}

TEST_CASE("storage refuses a corrupted upload and the call fails", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    auto client = make_client(server.url());
    storage.corrupt_next(1);
    CHECK_THROWS_AS(client.call("echo_bytes", AnnotatedBatch::data(bytes_batch(big_payload())),
                                result_schema()),
                    HttpClientError);
    CHECK(storage.refusals.load() == 1);
    CHECK(storage.sized_uploads.load() == 0);
}

TEST_CASE("a request pointer must carry and match vgi_rpc.location.sha256", "[upload-url]") {
    VerifyingStorage storage;
    UploadServer server(storage.url());
    const std::string real = ipc_body(bytes_batch("payload"), routing("echo_bytes", kProtocol));
    const std::string url = storage.put_object(real);
    auto pointer = [&](const std::string& sha) {
        auto metadata = routing("echo_bytes", kProtocol);
        metadata->Append(keys::LOCATION, url);
        if (!sha.empty()) metadata->Append(keys::LOCATION_SHA256, sha);
        return ipc_body(make_empty_batch(bytes_schema()), metadata);
    };
    const std::string path = std::string("/vgi/") + kProtocol + "/echo_bytes";

    auto missing = server.post(path, pointer(""));
    REQUIRE(missing);
    CHECK_THAT(error_message(missing->body), ContainsSubstring("missing vgi_rpc.location.sha256"));
    CHECK(storage.downloads.load() == 0);  // refused before any fetch

    auto mismatch = server.post(path, pointer(sha256_hex("something else")));
    REQUIRE(mismatch);
    CHECK_THAT(error_message(mismatch->body), ContainsSubstring("sha256 mismatch"));

    auto ok = server.post(path, pointer(sha256_hex(real)));
    REQUIRE(ok);
    CHECK(error_message(ok->body).empty());
}

TEST_CASE("the client refuses to upload to a v1 server", "[upload-url]") {
    httplib::Server legacy;
    legacy.Options(".*", [](const httplib::Request&, httplib::Response& res) {
        res.set_header("VGI-Upload-URL-Support", "true");
        res.set_header("VGI-Externalization-Enabled", "true");
        res.set_header("VGI-Max-Request-Bytes", "4096");
        res.status = 204;
    });
    std::atomic<int> init_calls{0};
    legacy.Post(".*", [&](const httplib::Request&, httplib::Response& res) {
        ++init_calls;
        res.status = 500;
    });
    const int port = legacy.bind_to_any_port("127.0.0.1");
    REQUIRE(port > 0);
    std::thread thread([&]() { (void)legacy.listen_after_bind(); });
    legacy.wait_until_ready();

    auto client = make_client("http://127.0.0.1:" + std::to_string(port));
    const auto caps = client.capabilities();
    CHECK(caps.upload_url_version == 1);
    CHECK(caps.upload_url_support);  // present, at a version this client refuses
    try {
        (void)client.call("echo_bytes", AnnotatedBatch::data(bytes_batch(big_payload())),
                          result_schema());
        FAIL("the client uploaded to a v1 server");
    } catch (const HttpClientError& error) {
        CHECK_THAT(std::string(error.what()), ContainsSubstring("upload protocol is too old"));
    }
    CHECK_THROWS_WITH(client.request_upload_urls({{16, sha256_hex("x")}}),
                      ContainsSubstring("upload protocol is too old"));
    CHECK(init_calls.load() == 0);
    legacy.stop();
    thread.join();
}

// ---------------------------------------------------------------------------
// Protocol 3.4: grant columns, argument references, fix 1
// ---------------------------------------------------------------------------

namespace {

struct Grant {
    std::string upload_url;
    std::string object_id;
    std::string ref;
};

Grant grant_for(const UploadServer& server, const std::string& payload,
                const std::string& principal) {
    httplib::Client client("127.0.0.1", server.port());
    httplib::Headers headers;
    if (!principal.empty()) headers.emplace("X-Conformance-Principal", principal);
    auto res = client.Post("/vgi/__upload_url__/init", headers,
                           init_v2({{static_cast<int64_t>(payload.size()), sha256_hex(payload)}}),
                           kArrowContentType);
    REQUIRE(res);
    REQUIRE(res->status == 200);
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(res->body));
    auto contents = read_ipc_stream(reader);
    REQUIRE(contents);
    const auto& batch = contents->batches.at(0).batch;
    Grant grant;
    grant.upload_url =
        std::static_pointer_cast<arrow::StringArray>(batch->GetColumnByName("upload_url"))
            ->GetString(0);
    grant.object_id =
        std::static_pointer_cast<arrow::StringArray>(batch->GetColumnByName("object_id"))
            ->GetString(0);
    grant.ref =
        std::static_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("ref"))->GetString(0);
    const std::string rest = grant.upload_url.substr(grant.upload_url.find("://") + 3);
    const std::string origin = "http://" + rest.substr(0, rest.find('/'));
    httplib::Client put(origin);
    auto put_res =
        put.Put(rest.substr(rest.find('/')), {{"x-amz-checksum-sha256", sha256_b64(payload)}},
                payload, "application/octet-stream");
    REQUIRE(put_res);
    REQUIRE(put_res->status == 204);
    return grant;
}

/// Call resolve_ref; returns {status-ok, body-or-error-message}.
std::pair<bool, std::string> resolve_ref(const UploadServer& server, const Grant& grant,
                                         const std::string& payload, const std::string& principal,
                                         std::optional<int64_t> size = std::nullopt) {
    arrow::BinaryBuilder ref;
    arrow::Int64Builder sizes;
    arrow::StringBuilder sha;
    REQUIRE(ref.Append(grant.ref).ok());
    REQUIRE(sizes.Append(size.value_or(static_cast<int64_t>(payload.size()))).ok());
    REQUIRE(sha.Append(sha256_hex(payload)).ok());
    auto schema = arrow::schema({arrow::field("ref", arrow::binary(), false),
                                 arrow::field("size", arrow::int64(), false),
                                 arrow::field("sha256", arrow::utf8(), false)});
    auto batch = arrow::RecordBatch::Make(
        schema, 1,
        {ref.Finish().ValueOrDie(), sizes.Finish().ValueOrDie(), sha.Finish().ValueOrDie()});
    httplib::Client client("127.0.0.1", server.port());
    client.set_decompress(false);
    httplib::Headers headers{{"Accept-Encoding", "identity"},
                             {"X-VGI-Accept-Encoding", "identity"}};
    if (!principal.empty()) headers.emplace("X-Conformance-Principal", principal);
    auto res = client.Post(std::string("/vgi/") + kProtocol + "/resolve_ref", headers,
                           ipc_body(batch, routing("resolve_ref", kProtocol)), kArrowContentType);
    REQUIRE(res);
    if (res->get_header_value("X-VGI-RPC-Error") == "true") {
        return {false, error_message(res->body)};
    }
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(res->body));
    auto contents = read_ipc_stream(reader);
    REQUIRE(contents);
    for (const auto& b : contents->batches) {
        if (b.batch && b.batch->num_rows() == 1) {
            return {true,
                    std::static_pointer_cast<arrow::BinaryArray>(b.batch->column(0))->GetString(0)};
        }
    }
    return {false, "no result row"};
}

}  // namespace

TEST_CASE("a grant's ref resolves for the granting principal only", "[upload-url][argref]") {
    VerifyingStorage storage;
    // Each fixture storage numbers its ids from 1; the process-wide cache
    // must not carry one test's object into the next.
    default_object_cache().clear();
    // The 64 KiB result comes back inline rather than as a pointer batch.
    UploadServer server(storage.url(), [](HttpConfig& cfg) {
        cfg.externalize_threshold = 1 << 20;
        cfg.compression = false;
    });
    const std::string payload = big_payload();
    const Grant grant = grant_for(server, payload, "alice");
    CHECK_FALSE(grant.object_id.empty());
    CHECK_FALSE(grant.ref.empty());

    auto [ok, body] = resolve_ref(server, grant, payload, "alice");
    INFO(body);
    REQUIRE(ok);
    CHECK(body == payload);
    // A second resolve is served from the object cache: one GET in all.
    auto again = resolve_ref(server, grant, payload, "alice");
    CHECK(again.first);
    CHECK(storage.object_gets == 1);

    auto other = resolve_ref(server, grant, payload, "bob");
    CHECK_FALSE(other.first);
    CHECK_THAT(other.second, ContainsSubstring("argument reference is invalid for this caller"));
    auto anonymous = resolve_ref(server, grant, payload, "");
    CHECK_FALSE(anonymous.first);

    auto wrong_size = resolve_ref(server, grant, payload, "alice", payload.size() + 1);
    CHECK_FALSE(wrong_size.first);
    CHECK_THAT(wrong_size.second, ContainsSubstring("does not match the size and sha256"));
}

TEST_CASE("a deleted argument object reports arguments_ref_missing", "[upload-url][argref]") {
    VerifyingStorage storage;
    // Each fixture storage numbers its ids from 1; the process-wide cache
    // must not carry one test's object into the next.
    default_object_cache().clear();
    UploadServer server(storage.url(), [](HttpConfig& cfg) { cfg.compression = false; });
    const std::string payload = "an argument whose object is deleted";
    const Grant grant = grant_for(server, payload, "alice");
    storage.erase(grant.object_id);
    auto [ok, body] = resolve_ref(server, grant, payload, "alice");
    CHECK_FALSE(ok);
    CHECK_THAT(body, ContainsSubstring("argument object is gone"));
}

TEST_CASE("fix 1: the upload cap defaults to the fetch cap and is always advertised",
          "[upload-url][fix1]") {
    VerifyingStorage storage;
    SECTION("defaults: 256 MiB both ways") {
        UploadServer server(storage.url(), [](HttpConfig& cfg) { cfg.max_upload_bytes = -1; });
        httplib::Client client("127.0.0.1", server.port());
        auto res = client.Options("/vgi/health");
        REQUIRE(res);
        CHECK(res->get_header_value("VGI-Max-Upload-Bytes") == std::to_string(256LL << 20));
    }
    SECTION("an explicit fetch cap bounds the default upload cap") {
        UploadServer server(storage.url(), [](HttpConfig& cfg) {
            cfg.max_upload_bytes = -1;
            cfg.max_fetch_bytes = 4096;
        });
        httplib::Client client("127.0.0.1", server.port());
        auto res = client.Options("/vgi/health");
        REQUIRE(res);
        CHECK(res->get_header_value("VGI-Max-Upload-Bytes") == "4096");
    }
    SECTION("a larger explicit upload cap raises the default fetch cap") {
        UploadServer server(storage.url(),
                            [](HttpConfig& cfg) { cfg.max_upload_bytes = int64_t{1} << 30; });
        httplib::Client client("127.0.0.1", server.port());
        auto res = client.Options("/vgi/health");
        REQUIRE(res);
        CHECK(res->get_header_value("VGI-Max-Upload-Bytes") == std::to_string(int64_t{1} << 30));
    }
}

TEST_CASE("fix 1: an explicit fetch cap below an explicit upload cap refuses to start",
          "[upload-url][fix1]") {
    VerifyingStorage storage;
    ServerBuilder builder;
    builder.protocol(kProtocol);
    builder.add_unary("noop", bytes_schema(), result_schema(),
                      [](const Request& request, CallContext&) -> Result {
                          return Result::value(arrow::RecordBatch::Make(
                              result_schema(), 1, {request.batch()->column(0)}));
                      });
    auto server = builder.build();
    HttpConfig cfg;
    cfg.external_storage_url = storage.url();
    cfg.max_fetch_bytes = 1 << 20;
    cfg.max_upload_bytes = 2 << 20;
    CHECK_THROWS_WITH(server->serve_http(cfg), ContainsSubstring("max_upload_bytes"));
}

TEST_CASE("the externalization threshold is not advertised as a request cap",
          "[upload-url][fix1]") {
    VerifyingStorage storage;
    UploadServer server(storage.url(), [](HttpConfig& cfg) { cfg.max_request_bytes = -1; });
    httplib::Client client("127.0.0.1", server.port());
    auto res = client.Options("/vgi/health");
    REQUIRE(res);
    CHECK_FALSE(res->has_header("VGI-Max-Request-Bytes"));
    CHECK(res->get_header_value("VGI-Upload-URL-Support") == "2");
}
