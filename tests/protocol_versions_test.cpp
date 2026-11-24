// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Protocol version negotiation (PROTOCOL_VERSIONS_V1.md): the advertisement
// syntax, the client's choice, and the server's window, gate and refusal.

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/metadata.h"
#include "vgi_rpc/protocol_versions.h"
#include "vgi_rpc/server.h"

#include <arrow/util/key_value_metadata.h>

#include <memory>
#include <string>

using namespace vgi_rpc;

namespace {

bool has(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

std::shared_ptr<arrow::KeyValueMetadata> request(const std::string& version,
                                                 const std::string& client_versions = "") {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    if (!version.empty()) md->Append(keys::PROTOCOL_VERSION, version);
    if (!client_versions.empty()) md->Append(keys::PROTOCOL_VERSIONS_CLIENT, client_versions);
    return md;
}

std::unique_ptr<Server> build(bool widen) {
    ServerBuilder builder;
    builder.protocol("Svc.v3").protocol_version("3.4.1");
    builder.add_protocol(ProtocolBuilder("Other.v1", "1.2.0"));
    builder.add_protocol(ProtocolBuilder("Plain.v1"));
    if (widen) builder.protocol_version_range("Svc.v3", "3.3-3.4");
    return builder.build();
}

}  // namespace

TEST_CASE("advertisements parse tolerantly and render both ends", "[protocol-versions]") {
    CHECK(VersionRange::parse("3.4").to_string() == "3.4-3.4");
    CHECK(VersionRange::parse(" 3.3 - 3.4.9 ").to_string() == "3.3-3.4");
    CHECK_THROWS(VersionRange::parse("3.5-3.4"));
    CHECK_THROWS(VersionRange::parse("03.4"));
    const auto parsed = parse_versions("vgi.v3=3.4-3.4, junk, x=1.0-0.1, =1.0, vgi.v3=9.9, s=1.0");
    REQUIRE(parsed.size() == 2);
    CHECK(parsed.at("vgi.v3").to_string() == "3.4-3.4");  // first wins
    CHECK(parsed.at("s").to_string() == "1.0-1.0");
    CHECK(parse_versions("").empty());
    CHECK(format_versions({{"a", VersionRange::parse("1.0-1.2")},
                           {"b", VersionRange::parse("2.0")}}) == "a=1.0-1.2, b=2.0-2.0");
    CHECK(highest_common(VersionRange::parse("3.4-3.5"), VersionRange::parse("3.4-3.4")) ==
          MajorMinor{3, 4});
    CHECK_FALSE(highest_common(VersionRange::parse("3.5-3.6"), VersionRange::parse("3.4-3.4")));
}

TEST_CASE("a client chooses per endpoint and keeps its patch", "[protocol-versions]") {
    const auto client = ClientVersioning::make("P", "3.5.2", "3.4-3.5");
    CHECK(client.advertised() == "P=3.4-3.5");
    CHECK_THROWS(ClientVersioning::make("P", "3.5.2", "3.3-3.4"));
    EndpointNegotiation endpoint;
    CHECK(endpoint.wire_version(client) == "3.5.2");  // nothing learned: its max
    endpoint.observe("P=3.4-3.4");
    CHECK(endpoint.wire_version(client) == "3.4.2");
    endpoint.observe("P=3.6-3.7");
    try {
        (void)endpoint.choose(client);
        FAIL("disjoint windows were accepted");
    } catch (const NoCommonVersionError& e) {
        CHECK(has(e.what(), "3.4-3.5"));
        CHECK(has(e.what(), "3.6-3.7"));
        CHECK(has(e.what(), "too old"));
    }
    endpoint.observe("");  // present but empty: P is not gated
    CHECK(endpoint.wire_version(client) == "3.5.2");

    // A pre-spec refusal names one version, and the client then speaks it.
    EndpointNegotiation legacy;
    legacy.observe_refusal("P", "protocol_version_mismatch",
                           "VGI client/worker protocol_version mismatch for protocol 'P'.\n"
                           "  Client: 3.5.2\n  Server: 3.4.0\n  Direction: server is too old.",
                           RemoteStatus{});
    CHECK(legacy.wire_version(client) == "3.4.2");
}

TEST_CASE("the server advertises exactly its gated windows", "[protocol-versions]") {
    CHECK(build(false)->protocol_versions() == "Svc.v3=3.4-3.4, Other.v1=1.2-1.2");
    CHECK(build(true)->protocol_versions() == "Svc.v3=3.3-3.4, Other.v1=1.2-1.2");
    ServerBuilder unknown;
    unknown.protocol("Svc.v3").protocol_version("3.4.0");
    unknown.protocol_version_range("Plain.v1", "1.0-1.0");
    CHECK_THROWS_AS(unknown.build(), std::invalid_argument);
    ServerBuilder none;
    none.protocol("Plain.v1");
    CHECK(none.build()->protocol_versions().empty());
}

TEST_CASE("the gate accepts the window and refuses with both sides named", "[protocol-versions]") {
    auto server = build(true);
    const auto& svc = server->application_protocols().front();
    CHECK(server->protocol_version_error(svc, request("3.3.9")).empty());
    CHECK(server->protocol_version_error(svc, request("3.4.0")).empty());

    const auto too_new = server->protocol_version_error(svc, request("3.6.0", "Svc.v3=3.5-3.6"));
    CHECK(has(too_new,
              "protocol_version_unsupported: client speaks Svc.v3 3.6 (supports 3.5-3.6); "
              "this server supports Svc.v3 3.3-3.4. The server is too old (the client "
              "is too new for it)."));
    const auto too_old = server->protocol_version_error(svc, request("2.9.0"));
    CHECK(has(too_old, "The client is too old (this server is too new for it)"));
    CHECK(has(server->protocol_version_error(svc, request("")), "did not send"));
    CHECK(has(server->protocol_version_error(svc, request("3.x")), "malformed"));

    const auto refusal = server->protocol_version_refusal(svc, request("3.6.0"), too_new, "r1");
    REQUIRE(refusal.custom_metadata);
    CHECK(refusal.custom_metadata->Get(keys::PROTOCOL_VERSIONS).ValueOr("") ==
          "Svc.v3=3.3-3.4, Other.v1=1.2-1.2");
    CHECK(refusal.custom_metadata->Get(keys::ERROR_KIND).ValueOr("") ==
          "protocol_version_unsupported");
    CHECK(has(refusal.custom_metadata->Get(keys::ERROR_DETAILS).ValueOr(""), "vgi_rpc.ErrorInfo"));
}
