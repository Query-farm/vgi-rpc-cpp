// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Pre-published external references: `ExternalRef`, `publish_external`, and a
// unary handler answering with `Result::from_external_ref`.
//
// The cross-language behaviour (a real client resolving the pointer, the
// publish-once contract against fake storage) is pinned by the shared
// conformance suite's TestExternalRef group.  These cases cover what that suite
// cannot reach: the constructor's validation, the exact bytes handed to
// `ExternalStorage::upload`, and the dispatcher writing the pointer as-is on a
// raw transport and over HTTP with externalization configured to fire.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/crypto.h>
#include <vgi_rpc/external.h>
#include <vgi_rpc/http_config.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/wire.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>
#include <httplib.h>
#include <zstd.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kProtocol = "RefProbe";
constexpr const char* kUrl = "https://objects.example/published/catalog.arrow";
const std::string kDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

std::shared_ptr<arrow::Schema> result_schema() {
    return arrow::schema({arrow::field("result", arrow::utf8(), /*nullable=*/false)});
}

std::shared_ptr<arrow::RecordBatch> string_batch(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(result_schema(), static_cast<int64_t>(values.size()),
                                    {unwrap(builder.Finish())});
}

std::string to_string(const std::shared_ptr<arrow::Buffer>& buffer) {
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

std::string ipc_of(const std::shared_ptr<arrow::Schema>& schema,
                   const std::vector<AnnotatedBatch>& batches) {
    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(sink, schema, batches);
    return to_string(unwrap(sink->Finish()));
}

IpcStreamContents read_ipc(const std::string& bytes) {
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(bytes));
    auto contents = read_ipc_stream(reader);
    REQUIRE(contents.has_value());
    return std::move(*contents);
}

std::string sha256_hex(const std::string& bytes) {
    const auto digest =
        crypto::sha256(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    return crypto::hex_encode(digest.data(), digest.size());
}

/// Records every upload; never fetched.
class RecordingStorage final : public ExternalStorage {
public:
    struct Upload {
        std::string data;
        std::string content_encoding;
    };

    std::string upload(const std::string& data, const std::string& content_encoding) override {
        uploads.push_back({data, content_encoding});
        return "https://objects.example/obj/" + std::to_string(uploads.size());
    }
    std::string fetch(const std::string&) override {
        throw std::logic_error("RecordingStorage does not fetch");
    }
    std::vector<UploadUrlPair> upload_urls(int64_t) override { return {}; }

    std::vector<Upload> uploads;
};

/// The response's final batch -- the result slot, after any log batches.
const AnnotatedBatch& result_slot(const IpcStreamContents& contents) {
    REQUIRE_FALSE(contents.batches.empty());
    return contents.batches.back();
}

void check_pointer(const AnnotatedBatch& ab, const std::string& url,
                   const std::optional<std::string>& sha256) {
    REQUIRE(ab.batch->num_rows() == 0);
    CHECK(ab.batch->schema()->Equals(*result_schema()));
    REQUIRE(ab.custom_metadata);
    CHECK(get_metadata_value(ab.custom_metadata, keys::LOCATION) == url);
    if (sha256) {
        CHECK(get_metadata_value(ab.custom_metadata, keys::LOCATION_SHA256) == *sha256);
    } else {
        CHECK(ab.custom_metadata->FindKey(keys::LOCATION_SHA256) < 0);
    }
    CHECK(ab.custom_metadata->FindKey(keys::LOG_LEVEL) < 0);
}

/// A server whose methods answer with refs, plus one ordinary value method.
std::unique_ptr<Server> make_ref_server() {
    ServerBuilder builder;
    builder.add_unary("ref", empty_schema(), result_schema(), [](const Request&, CallContext&) {
        return Result::from_external_ref(ExternalRef(kUrl, kDigest));
    });
    builder.add_unary(
        "ref_no_digest", empty_schema(), result_schema(),
        [](const Request&, CallContext&) { return Result::from_external_ref(ExternalRef(kUrl)); });
    builder.add_unary("value", empty_schema(), result_schema(), [](const Request&, CallContext&) {
        return Result::value(string_batch({"inline"}));
    });
    builder.protocol(kProtocol);
    return builder.build();
}

std::string request_body(const std::string& method) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, method);
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    md->Append(keys::PROTOCOL, kProtocol);
    return ipc_of(empty_schema(),
                  {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), md)});
}

IpcStreamContents serve_pipe(Server& server, const std::string& method) {
    auto input =
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(request_body(method)));
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    REQUIRE(server.serve_one(input, output));
    return read_ipc(to_string(unwrap(output->Finish())));
}

/// `Server::serve_http` on a background thread, with its bound port.
class HttpRefServer {
public:
    explicit HttpRefServer(HttpConfig cfg) : server_(make_ref_server()) {
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.prefix = "/vgi";
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

    ~HttpRefServer() {
        // As in http_stream_metadata_test: the accept loop lives inside
        // serve_http, so detach and leak rather than free it underneath.
        thread_.detach();
        (void)server_.release();
    }

    httplib::Result post(const std::string& method) const {
        httplib::Client client("127.0.0.1", port_);
        client.set_read_timeout(10, 0);
        httplib::Headers headers{{"Accept-Encoding", "identity"},
                                 {"X-VGI-Accept-Encoding", "identity"}};
        return client.Post(std::string("/vgi/") + kProtocol + "/" + method, headers,
                           request_body(method), "application/vnd.apache.arrow.stream");
    }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

}  // namespace

// ── ExternalRef ──────────────────────────────────────────────────────

TEST_CASE("ExternalRef validates its url and digest", "[external-ref]") {
    CHECK_THROWS_AS(ExternalRef(""), std::invalid_argument);
    CHECK_THROWS_AS(ExternalRef("", kDigest), std::invalid_argument);
    CHECK_THROWS_AS(ExternalRef(kUrl, kDigest.substr(1)), std::invalid_argument);
    CHECK_THROWS_AS(ExternalRef(kUrl, kDigest + "0"), std::invalid_argument);
    std::string upper = kDigest;
    upper[10] = 'A';
    CHECK_THROWS_AS(ExternalRef(kUrl, upper), std::invalid_argument);
    std::string non_hex = kDigest;
    non_hex[0] = 'g';
    CHECK_THROWS_AS(ExternalRef(kUrl, non_hex), std::invalid_argument);
    CHECK_THROWS_AS(ExternalRef(kUrl, std::string()), std::invalid_argument);

    const ExternalRef with(kUrl, kDigest);
    CHECK(with.url() == kUrl);
    CHECK(with.sha256() == kDigest);
    const ExternalRef without(kUrl);
    CHECK_FALSE(without.sha256().has_value());
    CHECK(with != without);
}

TEST_CASE("ExternalRef's pointer batch carries the digest only when it has one", "[external-ref]") {
    check_pointer(ExternalRef(kUrl, kDigest).pointer_batch(result_schema()), kUrl, kDigest);
    check_pointer(ExternalRef(kUrl).pointer_batch(result_schema()), kUrl, std::nullopt);
}

// ── publish_external ─────────────────────────────────────────────────

TEST_CASE("publish_external uploads the serialized result batch once", "[external-ref]") {
    RecordingStorage storage;
    const auto batch = string_batch({"catalog"});
    const ExternalRef ref = publish_external(batch, storage);

    REQUIRE(storage.uploads.size() == 1);
    const auto& upload = storage.uploads[0];
    CHECK(upload.content_encoding.empty());
    // Byte-for-byte what the per-call externalizer would upload for it.
    CHECK(upload.data == ipc_of(result_schema(), {AnnotatedBatch::data(batch)}));
    CHECK(ref.url() == "https://objects.example/obj/1");
    CHECK(ref.sha256() == sha256_hex(upload.data));

    const auto contents = read_ipc(upload.data);
    REQUIRE(contents.batches.size() == 1);
    REQUIRE(contents.batches[0].batch->num_rows() == 1);
    CHECK(std::static_pointer_cast<arrow::StringArray>(contents.batches[0].batch->column(0))
              ->GetString(0) == "catalog");
}

TEST_CASE("publish_external without a digest", "[external-ref]") {
    RecordingStorage storage;
    const ExternalRef ref = publish_external(string_batch({"x"}), storage, "", false);
    CHECK(storage.uploads.size() == 1);
    CHECK_FALSE(ref.sha256().has_value());
}

TEST_CASE("publish_external compresses with zstd and hashes the raw bytes", "[external-ref]") {
    RecordingStorage storage;
    const auto batch = string_batch({std::string(10000, 'z')});
    const ExternalRef ref = publish_external(batch, storage, "zstd");

    REQUIRE(storage.uploads.size() == 1);
    const auto& upload = storage.uploads[0];
    CHECK(upload.content_encoding == "zstd");
    const std::string raw = ipc_of(result_schema(), {AnnotatedBatch::data(batch)});
    CHECK(upload.data.size() < raw.size());

    const auto size = ZSTD_getFrameContentSize(upload.data.data(), upload.data.size());
    REQUIRE(size == raw.size());
    std::string decoded(size, '\0');
    const size_t written =
        ZSTD_decompress(decoded.data(), decoded.size(), upload.data.data(), upload.data.size());
    REQUIRE_FALSE(ZSTD_isError(written));
    CHECK(decoded == raw);
    CHECK(ref.sha256() == sha256_hex(raw));
}

TEST_CASE("publish_external refuses anything but one row, and unknown codings", "[external-ref]") {
    RecordingStorage storage;
    CHECK_THROWS_AS(publish_external(string_batch({}), storage), std::invalid_argument);
    CHECK_THROWS_AS(publish_external(string_batch({"a", "b"}), storage), std::invalid_argument);
    CHECK_THROWS_AS(publish_external(nullptr, storage), std::invalid_argument);
    CHECK_THROWS_AS(publish_external(string_batch({"a"}), storage, "gzip"), std::invalid_argument);
    CHECK(storage.uploads.empty());
}

TEST_CASE("upload_ipc_stream reports raw and uploaded sizes", "[external-ref]") {
    RecordingStorage storage;
    const std::string raw = ipc_of(result_schema(), {AnnotatedBatch::data(string_batch({"v"}))});
    const ExternalUpload plain = upload_ipc_stream(raw, storage, "");
    CHECK(plain.raw_bytes == static_cast<int64_t>(raw.size()));
    CHECK(plain.uploaded_bytes == plain.raw_bytes);
    CHECK(plain.sha256 == sha256_hex(raw));
    const ExternalUpload zstd = upload_ipc_stream(raw, storage, "zstd");
    CHECK(zstd.uploaded_bytes == static_cast<int64_t>(storage.uploads.back().data.size()));
    CHECK(zstd.sha256 == plain.sha256);
}

// ── Dispatch ─────────────────────────────────────────────────────────

TEST_CASE("a raw transport writes a returned ref's pointer as-is", "[external-ref][server]") {
    auto server = make_ref_server();
    const auto with = serve_pipe(*server, "ref");
    REQUIRE(with.schema->Equals(*result_schema()));
    check_pointer(result_slot(with), kUrl, kDigest);

    const auto without = serve_pipe(*server, "ref_no_digest");
    check_pointer(result_slot(without), kUrl, std::nullopt);
}

TEST_CASE("HTTP writes a returned ref's pointer, never externalizing it again",
          "[external-ref][http]") {
    // Externalization is configured to fire on every response, against a
    // backend nothing listens on, and with a zero external cap: anything that
    // tried to upload, or charged the ref against the cap, would fail the call.
    HttpConfig cfg;
    cfg.external_storage_url = "http://127.0.0.1:9";
    cfg.externalize_threshold = 0;
    cfg.max_externalized_response_bytes = 0;
    // The threshold doubles as the inline-request ceiling unless one is set.
    cfg.max_request_bytes = 1 << 20;
    HttpRefServer server(cfg);

    for (const bool digest : {true, false}) {
        auto response = server.post(digest ? "ref" : "ref_no_digest");
        REQUIRE(response);
        REQUIRE(response->status == 200);
        CHECK_FALSE(response->has_header("X-VGI-RPC-Error"));
        const auto contents = read_ipc(response->body);
        REQUIRE(contents.schema->Equals(*result_schema()));
        check_pointer(result_slot(contents), kUrl,
                      digest ? std::optional<std::string>(kDigest) : std::nullopt);
    }

    // Control: an ordinary value on the same server does take the externalizer,
    // which is what makes the two passes above meaningful.
    auto control = server.post("value");
    REQUIRE(control);
    CHECK(control->status == 200);
    CHECK(control->has_header("X-VGI-RPC-Error"));
}

TEST_CASE("HTTP writes a returned ref's pointer with no storage configured",
          "[external-ref][http]") {
    HttpRefServer server{HttpConfig{}};
    auto response = server.post("ref");
    REQUIRE(response);
    REQUIRE(response->status == 200);
    CHECK_FALSE(response->has_header("X-VGI-RPC-Error"));
    check_pointer(result_slot(read_ipc(response->body)), kUrl, kDigest);
}
