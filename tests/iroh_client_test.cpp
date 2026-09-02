// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/client.h"

#include <algorithm>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>

using namespace vgi_rpc;

namespace {
const std::string ID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
}

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
    if (std::getenv("VGI_RPC_EXPECT_IROH_NATIVE")) CHECK(native_iroh_transport_available());
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
