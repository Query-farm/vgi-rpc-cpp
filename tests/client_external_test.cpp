// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/client_external.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <httplib.h>
#include <zstd.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

using namespace vgi_rpc;

namespace {

class LoopbackServer {
public:
    explicit LoopbackServer(const std::function<void(httplib::Server&)>& configure) {
        configure(server_);
        port_ = server_.bind_to_any_port("127.0.0.1");
        if (port_ <= 0) throw std::runtime_error("cannot bind external helper test server");
        thread_ = std::thread([this] { (void)server_.listen_after_bind(); });
        for (int attempt = 0; attempt < 1000 && !server_.is_running(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!server_.is_running())
            throw std::runtime_error("external helper test server did not start");
    }

    ~LoopbackServer() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string url(const std::string& path) const {
        return "http://127.0.0.1:" + std::to_string(port_) + path;
    }

private:
    httplib::Server server_;
    int port_ = 0;
    std::thread thread_;
};

ClientExternalHttpOptions loopback_options() {
    ClientExternalHttpOptions options;
    options.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
    options.connect_timeout = std::chrono::milliseconds(1000);
    options.read_timeout = std::chrono::milliseconds(2000);
    options.write_timeout = std::chrono::milliseconds(2000);
    return options;
}

std::string sha256_hex(const std::string& value) {
    crypto::Sha256 digest;
    digest.update(value);
    return digest.hex_digest();
}

std::string zstd_encode(const std::string& value) {
    std::string encoded(ZSTD_compressBound(value.size()), '\0');
    const size_t size =
        ZSTD_compress(encoded.data(), encoded.size(), value.data(), value.size(), 3);
    if (ZSTD_isError(size)) throw std::runtime_error("test zstd compression failed");
    encoded.resize(size);
    return encoded;
}

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::int64())});
}

std::shared_ptr<arrow::RecordBatch> value_batch(int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(value_schema(), 1, {unwrap(builder.Finish())});
}

std::string encode_ipc(const std::shared_ptr<arrow::Schema>& schema,
                       const std::vector<AnnotatedBatch>& batches) {
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, schema, batches);
    const auto buffer = unwrap(output->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

}  // namespace

TEST_CASE("external URL policy rejects local and credential-bearing production targets",
          "[client_external][security]") {
    ClientExternalHttp client;
    for (const std::string& url : {
             "http://example.com/object",
             "https://user:password@example.com/object",
             "https://127.0.0.1/object",
             "https://0.0.0.0/object",
             "https://169.254.169.254/latest/meta-data",
             "https://224.0.0.1/object",
             "https://[::1]/object",
             "https://example.com/object#secret",
         }) {
        REQUIRE_THROWS_AS(client.fetch(url), ExternalHttpError);
    }
    REQUIRE(redact_external_url("https://example.com:8443/a/b?X-Amz-Signature=secret") ==
            "https://example.com:8443/a/b");
    REQUIRE(redact_external_url("https://user:secret@example.com/a?token=secret") ==
            "<invalid-url>");
}

TEST_CASE("loopback test policy fetches and manually revalidates redirects", "[client_external]") {
    LoopbackServer server([](httplib::Server& http) {
        http.Get("/redirect", [](const httplib::Request&, httplib::Response& response) {
            response.status = 302;
            response.set_header("Location", "/data?X-Amz-Signature=redirect-secret");
        });
        http.Get("/data", [](const httplib::Request&, httplib::Response& response) {
            response.set_content("external-data", "application/octet-stream");
        });
        http.Get("/private", [](const httplib::Request&, httplib::Response& response) {
            response.status = 302;
            response.set_header("Location", "http://10.0.0.1/private?token=secret");
        });
    });
    ClientExternalHttp client(loopback_options());
    REQUIRE(client.fetch(server.url("/redirect?initial=secret")) == "external-data");
    try {
        (void)client.fetch(server.url("/private?initial=secret"));
        FAIL("expected redirect URL policy failure");
    } catch (const ExternalHttpError& error) {
        const std::string message = error.what();
        REQUIRE(message.find("secret") == std::string::npos);
    }
}

TEST_CASE("external fetch enforces encoded decoded zstd and checksum boundaries",
          "[client_external]") {
    const std::string decoded(256 * 1024, 'z');
    const std::string encoded = zstd_encode(decoded);
    LoopbackServer server([&](httplib::Server& http) {
        http.Get("/zstd", [&](const httplib::Request&, httplib::Response& response) {
            response.set_header("Content-Encoding", "zstd");
            response.set_content(encoded, "application/octet-stream");
        });
        http.Get("/identity", [](const httplib::Request&, httplib::Response& response) {
            response.set_content(std::string(2048, 'x'), "application/octet-stream");
        });
    });

    auto options = loopback_options();
    ClientExternalHttp client(options);
    REQUIRE(client.fetch(server.url("/zstd"), sha256_hex(decoded)) == decoded);
    REQUIRE_THROWS_AS(client.fetch(server.url("/zstd"), std::string(64, '0')), ExternalHttpError);

    options.max_decoded_bytes = 128 * 1024;
    ClientExternalHttp decoded_limited(options);
    REQUIRE_THROWS_AS(decoded_limited.fetch(server.url("/zstd")), ExternalHttpError);
    options = loopback_options();
    options.max_encoded_bytes = 1024;
    ClientExternalHttp encoded_limited(options);
    REQUIRE_THROWS_AS(encoded_limited.fetch(server.url("/identity")), ExternalHttpError);
}

TEST_CASE("external pointer resolution parses one IPC level and frees signed URL diagnostics",
          "[client_external]") {
    auto log_metadata = std::make_shared<arrow::KeyValueMetadata>();
    log_metadata->Append(keys::LOG_LEVEL, "INFO");
    log_metadata->Append(keys::LOG_MESSAGE, "fetching");
    auto data_metadata = std::make_shared<arrow::KeyValueMetadata>();
    data_metadata->Append("application.tag", "kept");
    const std::string payload =
        encode_ipc(value_schema(),
                   {AnnotatedBatch::with_metadata(make_empty_batch(value_schema()), log_metadata),
                    AnnotatedBatch::with_metadata(value_batch(77), data_metadata)});

    auto nested_metadata = std::make_shared<arrow::KeyValueMetadata>();
    nested_metadata->Append(keys::LOCATION, "https://example.com/nested");
    const std::string nested_payload = encode_ipc(
        value_schema(),
        {AnnotatedBatch::with_metadata(make_empty_batch(value_schema()), nested_metadata)});
    LoopbackServer server([&](httplib::Server& http) {
        http.Get("/pointer", [&](const httplib::Request&, httplib::Response& response) {
            response.set_content(payload, "application/vnd.apache.arrow.stream");
        });
        http.Get("/nested", [&](const httplib::Request&, httplib::Response& response) {
            response.set_content(nested_payload, "application/vnd.apache.arrow.stream");
        });
    });
    ClientExternalHttp client(loopback_options());

    auto pointer_metadata = std::make_shared<arrow::KeyValueMetadata>();
    const std::string signed_url = server.url("/pointer?X-Amz-Signature=top-secret");
    pointer_metadata->Append(keys::LOCATION, signed_url);
    pointer_metadata->Append(keys::LOCATION_SHA256, sha256_hex(payload));
    pointer_metadata->Append(keys::STREAM_STATE, "outer-continuation");
    const AnnotatedBatch pointer =
        AnnotatedBatch::with_metadata(make_empty_batch(value_schema()), pointer_metadata);
    int logs = 0;
    const auto resolved = client.resolve_pointer(pointer, [&](const AnnotatedBatch&) { ++logs; });
    REQUIRE(logs == 1);
    REQUIRE(resolved.batch->num_rows() == 1);
    REQUIRE(std::static_pointer_cast<arrow::Int64Array>(resolved.batch->column(0))->Value(0) == 77);
    REQUIRE(get_metadata_value(resolved.custom_metadata, "application.tag") == "kept");
    REQUIRE(get_metadata_value(resolved.custom_metadata, keys::STREAM_STATE) ==
            "outer-continuation");
    REQUIRE(get_metadata_value(resolved.custom_metadata, keys::LOCATION_SOURCE) ==
            server.url("/pointer"));
    REQUIRE(get_metadata_value(resolved.custom_metadata, keys::LOCATION_SOURCE).find("secret") ==
            std::string::npos);

    auto nested_outer_metadata = std::make_shared<arrow::KeyValueMetadata>();
    nested_outer_metadata->Append(keys::LOCATION, server.url("/nested"));
    const AnnotatedBatch nested_outer =
        AnnotatedBatch::with_metadata(make_empty_batch(value_schema()), nested_outer_metadata);
    REQUIRE_THROWS_AS(client.resolve_pointer(nested_outer), ExternalHttpError);
}

TEST_CASE("external PUT is credential-free capped and never follows redirects",
          "[client_external]") {
    std::atomic<int> uploads{0};
    std::atomic<int> redirected_uploads{0};
    std::string received;
    bool credential_header = false;
    std::string received_content_type;
    LoopbackServer server([&](httplib::Server& http) {
        http.Put("/upload", [&](const httplib::Request& request, httplib::Response& response) {
            ++uploads;
            received = request.body;
            credential_header = request.has_header("Authorization") ||
                                request.has_header("Cookie") ||
                                request.has_header("Proxy-Authorization");
            received_content_type = request.get_header_value("Content-Type");
            response.status = 204;
        });
        http.Put("/redirect-upload", [](const httplib::Request&, httplib::Response& response) {
            response.status = 307;
            response.set_header("Location", "/upload");
        });
        http.Put("/must-not-run", [&](const httplib::Request&, httplib::Response& response) {
            ++redirected_uploads;
            response.status = 204;
        });
    });
    auto options = loopback_options();
    options.max_upload_bytes = 16;
    ClientExternalHttp client(options);
    client.put(server.url("/upload?signature=secret"), "payload", "application/x-test");
    REQUIRE(uploads.load() == 1);
    REQUIRE(received == "payload");
    REQUIRE_FALSE(credential_header);
    REQUIRE(received_content_type == "application/x-test");
    REQUIRE_THROWS_AS(client.put(server.url("/upload"), std::string(17, 'x')), ExternalHttpError);
    REQUIRE_THROWS_AS(client.put(server.url("/redirect-upload"), "payload"), ExternalHttpError);
    REQUIRE(uploads.load() == 1);
    REQUIRE(redirected_uploads.load() == 0);
}
