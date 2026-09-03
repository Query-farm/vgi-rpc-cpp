// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/client.h"
#include "vgi_rpc/http_client.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <algorithm>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>

using namespace vgi_rpc;

namespace {
const std::string ID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

std::string unary_response(int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    auto schema = arrow::schema({arrow::field("value", arrow::int64(), false)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, schema, {AnnotatedBatch::data(std::move(batch))});
    auto buffer = unwrap(output->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

AnnotatedBatch empty_request() {
    return AnnotatedBatch::data(make_empty_batch(empty_schema()));
}
}  // namespace

TEST_CASE("canonical Iroh endpoint parsing") {
    const auto raw = IrohEndpoint::parse("iroh://" + ID);
    CHECK(raw.scheme == IrohEndpoint::Scheme::IROH);
    CHECK(raw.alpn == IROH_ARROW_MUX_ALPN);
    CHECK(raw.endpoint_id_bytes.size() == 32);

    const auto http = IrohEndpoint::parse("httpi://" + ID + "/api/v1");
    CHECK(http.scheme == IrohEndpoint::Scheme::HTTPI);
    CHECK(http.base_path == "/api/v1");
    CHECK(http.alpn == IROH_HTTP_ALPN);
}

TEST_CASE("non-canonical Iroh endpoint rejection") {
    const std::vector<std::string> invalid{
        "iroh://0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        "iroh://" + ID + "/",
        "iroh://" + ID + ":443",
        "iroh://user@" + ID,
        "httpi://" + ID + "/a//b",
        "httpi://" + ID + "/a/../b",
        "httpi://" + ID + "/bad%2",
        "httpi://" + ID + "?x=1"};
    for (const auto& value : invalid)
        CHECK_THROWS_AS(IrohEndpoint::parse(value), IrohTransportError);
}

TEST_CASE("canonical Iroh transport fixture") {
    std::ifstream input(VGI_RPC_IROH_VECTORS);
    REQUIRE(input.good());
    const auto fixture = nlohmann::json::parse(input);
    CHECK(fixture["alpns"]["iroh"] == IROH_ARROW_MUX_ALPN);
    CHECK(fixture["alpns"]["httpi"] == IROH_HTTP_ALPN);
    for (const auto& vector : fixture["uri_cases"]) {
        const auto uri = vector["uri"].get<std::string>();
        if (!vector["valid"].get<bool>()) {
            CHECK_THROWS_AS(IrohEndpoint::parse(uri), IrohTransportError);
            continue;
        }
        const auto endpoint = IrohEndpoint::parse(uri);
        CHECK(endpoint.base_path == vector["base_path"].get<std::string>());
        CHECK((endpoint.scheme == IrohEndpoint::Scheme::IROH ? "iroh" : "httpi") ==
              vector["scheme"].get<std::string>());
    }
    const std::vector<std::string> stages{"parse",  "bind",        "resolve", "connect",
                                          "alpn",   "open_stream", "write",   "read",
                                          "cancel", "close",       "internal"};
    const std::vector<std::string> categories{
        "invalid_input",      "unsupported",      "unavailable", "timeout",
        "protocol",           "connection_reset", "cancelled",   "authentication",
        "resource_exhausted", "internal"};
    const std::vector<std::string> certainties{"not_sent", "unknown", "sent"};
    for (const auto& vector : fixture["error_cases"]) {
        CHECK(std::find(stages.begin(), stages.end(), vector["stage"].get<std::string>()) !=
              stages.end());
        CHECK(std::find(categories.begin(), categories.end(),
                        vector["category"].get<std::string>()) != categories.end());
        CHECK(std::find(certainties.begin(), certainties.end(),
                        vector["dispatch_certainty"].get<std::string>()) != certainties.end());
    }
    try {
        IrohEndpoint::parse("invalid");
        FAIL("invalid URI unexpectedly parsed");
    } catch (const IrohTransportError& error) {
        CHECK(error.stage() == IrohErrorStage::PARSE);
        CHECK(error.category() == IrohErrorCategory::INVALID_INPUT);
        CHECK(error.dispatch_certainty() == IrohDispatchCertainty::NOT_SENT);
    }
}

TEST_CASE("native Iroh provider is an explicit callable capability") {
    const auto provider = native_iroh_transport_provider();
    CHECK(static_cast<bool>(provider));
    CHECK(static_cast<bool>(native_iroh_http_transport_provider()));
    if (std::getenv("VGI_RPC_EXPECT_IROH_NATIVE")) CHECK(native_iroh_transport_available());
}

TEST_CASE("typed HTTP client reuses its state machine over an Iroh provider") {
    std::vector<std::string> paths;
    bool authorization_seen = false;
    const IrohHttpTransportProvider provider = [&](const IrohEndpoint& endpoint,
                                                   const IrohHttpRequest& request,
                                                   const IrohTransportOptions& options) {
        CHECK(endpoint.scheme == IrohEndpoint::Scheme::HTTPI);
        CHECK(endpoint.endpoint_id == ID);
        CHECK(options.no_relay);
        CHECK(request.max_response_bytes == 256ULL * 1024 * 1024);
        paths.push_back(request.method + " " + request.path);
        IrohHttpResponse response;
        response.status = 200;
        response.remote_endpoint_id = ID;
        response.headers = {{"VGI-Accept-Max-Response-Bytes-Support", "true"},
                            {"VGI-Max-Request-Bytes", "1048576"},
                            {"VGI-Max-Response-Bytes", "1048576"},
                            {"VGI-Supported-Encodings", "identity"}};
        if (request.method == "POST") {
            for (const auto& [name, value] : request.headers) {
                if (name == "Authorization" && value == "Bearer secret") {
                    authorization_seen = true;
                }
            }
            response.headers.emplace_back("Content-Type", "application/vnd.apache.arrow.stream");
            response.body = unary_response(42);
        }
        return response;
    };

    IrohTransportOptions transport_options;
    transport_options.no_relay = true;
    auto client = HttpClient::builder("httpi://" + ID + "/api/v1")
                      .iroh_transport_provider(provider)
                      .iroh_transport_options(transport_options)
                      .compression_level(std::nullopt)
                      .header("Authorization", "Bearer secret")
                      .build();
    const auto capabilities = client.capabilities();
    CHECK(capabilities.max_request_bytes == 1'048'576);
    const auto answer = client.call("answer", empty_request());
    REQUIRE(answer.batch);
    REQUIRE(answer.batch->num_rows() == 1);
    const auto values = std::static_pointer_cast<arrow::Int64Array>(answer.batch->column(0));
    CHECK(values->Value(0) == 42);
    CHECK(authorization_seen);
    REQUIRE(paths.size() == 2);
    CHECK(paths[0] == "OPTIONS /api/v1/health");
    CHECK(paths[1] == "POST /api/v1/answer");
}

TEST_CASE("HTTP-over-Iroh rejects a mismatched authenticated peer") {
    const IrohHttpTransportProvider provider = [](const IrohEndpoint&, const IrohHttpRequest&,
                                                  const IrohTransportOptions&) -> IrohHttpResponse {
        return {200, {{"VGI-Accept-Max-Response-Bytes-Support", "true"}}, {}, std::string(64, 'f')};
    };
    auto client = HttpClient::builder("httpi://" + ID).iroh_transport_provider(provider).build();
    try {
        (void)client.capabilities();
        FAIL("mismatched Iroh identity unexpectedly accepted");
    } catch (const HttpClientError& error) {
        CHECK(error.kind() == HttpClientErrorKind::AUTHENTICATION);
    }
}

TEST_CASE("HTTP-over-Iroh bounds provider responses before HTTP decoding") {
    const IrohHttpTransportProvider provider = [](const IrohEndpoint&, const IrohHttpRequest&,
                                                  const IrohTransportOptions&) -> IrohHttpResponse {
        return {200, {}, std::string((64 * 1024) + 1, 'x'), ID};
    };
    HttpClientConfig config;
    config.max_response_bytes = 64 * 1024;
    config.max_encoded_response_bytes = 64 * 1024;
    config.max_decoded_response_bytes = 64 * 1024;
    config.accepted_max_response_bytes = 64 * 1024;
    auto client = HttpClient::builder("httpi://" + ID)
                      .config(config)
                      .prefix("")
                      .iroh_transport_provider(provider)
                      .build();
    try {
        (void)client.capabilities();
        FAIL("oversized Iroh response unexpectedly accepted");
    } catch (const HttpClientError& error) {
        CHECK(error.kind() == HttpClientErrorKind::LIMIT);
    }
}

TEST_CASE("Iroh Arrow status detail retains portable fields and message") {
    IrohStatusDetail detail(IrohErrorStage::READ, IrohErrorCategory::TIMEOUT,
                            IrohDispatchCertainty::SENT, "deadline exceeded");
    CHECK(detail.stage() == IrohErrorStage::READ);
    CHECK(detail.category() == IrohErrorCategory::TIMEOUT);
    CHECK(detail.dispatch_certainty() == IrohDispatchCertainty::SENT);
    CHECK(detail.ToString().find("deadline exceeded") != std::string::npos);
}

TEST_CASE("raw client explicitly rejects httpi") {
    const IrohTransportProvider unused = [](const IrohEndpoint&,
                                            const IrohTransportOptions&) -> ClientTransport {
        FAIL("httpi must be rejected before provider dispatch");
        throw std::runtime_error("unreachable");
    };
    try {
        (void)connect_iroh_transport("httpi://" + ID, unused);
        FAIL("httpi unexpectedly dispatched through raw provider");
    } catch (const IrohTransportError& error) {
        CHECK(error.category() == IrohErrorCategory::UNSUPPORTED);
        CHECK(error.dispatch_certainty() == IrohDispatchCertainty::NOT_SENT);
    }
}

TEST_CASE("native C ABI connects to a live Iroh Arrow-mux worker", "[iroh][integration]") {
    const auto* endpoint = std::getenv("VGI_RPC_IROH_TEST_ENDPOINT");
    if (!endpoint) SKIP("set VGI_RPC_IROH_TEST_ENDPOINT to run the live native test");
    const auto local_id = native_iroh_endpoint_id();
    IrohTransportOptions alternate;
    alternate.connect_timeout = std::chrono::seconds(7);
    alternate.io_timeout = std::chrono::seconds(19);
    alternate.no_relay = true;
    REQUIRE(local_id == native_iroh_endpoint_id(alternate));
    IrohTransportOptions configured;
    configured.secret_key.emplace();
    configured.secret_key->fill(7);
    const auto configured_id = native_iroh_endpoint_id(configured);
    REQUIRE(configured_id != local_id);
    configured.connect_timeout = std::chrono::seconds(11);
    configured.no_relay = true;
    REQUIRE(native_iroh_endpoint_id(configured) == configured_id);
    configured.secret_key->fill(0);
    auto client = RpcClient::connect_iroh(std::string("iroh://") + endpoint,
                                          native_iroh_transport_provider());
    const auto description = client.describe();
    REQUIRE(!description.protocol_name.empty());
    REQUIRE(!description.methods.empty());
    client.close();
}

TEST_CASE("native C ABI connects to a live HTTP-over-Iroh worker", "[iroh][integration]") {
    const auto* endpoint = std::getenv("VGI_RPC_IROH_HTTP_TEST_ENDPOINT");
    if (!endpoint) SKIP("set VGI_RPC_IROH_HTTP_TEST_ENDPOINT to run the live native HTTP test");
    IrohTransportOptions transport;
    if (const auto* direct = std::getenv("VGI_RPC_IROH_HTTP_TEST_DIRECT_ADDRESSES")) {
        std::string remaining(direct);
        while (!remaining.empty()) {
            const auto separator = remaining.find(',');
            const auto address = remaining.substr(0, separator);
            if (!address.empty()) transport.direct_addresses.push_back(address);
            if (separator == std::string::npos) break;
            remaining.erase(0, separator + 1);
        }
    }
    if (!transport.direct_addresses.empty()) {
        transport.no_relay = true;
    }
    auto client = HttpClient::builder(std::string("httpi://") + endpoint)
                      .iroh_transport_options(std::move(transport))
                      .build();
    const auto capabilities = client.capabilities();
    REQUIRE(capabilities.accept_max_response_bytes_support);
    const auto description = client.describe();
    REQUIRE(!description.protocol_name.empty());
    REQUIRE(!description.methods.empty());
}
