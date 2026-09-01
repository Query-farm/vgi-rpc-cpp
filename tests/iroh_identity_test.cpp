// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "vgi_rpc/iroh_identity.h"

using namespace vgi_rpc;

namespace {

constexpr const char* kEndpoint =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

PeerResolutionContext context(
    std::optional<std::string> peer = "127.0.0.1",
    std::map<std::string, std::vector<std::string>> headers = {
        {IROH_FORWARDED_ENDPOINT_HEADER, {kEndpoint}}}) {
    PeerResolutionContext result;
    result.transport = "http";
    result.immediate_peer = std::move(peer);
    result.headers = std::move(headers);
    return result;
}

IrohForwardedHeaderOptions options() {
    return {"production-mesh", {"127.0.0.1"}};
}

}  // namespace

TEST_CASE("forwarded Iroh HTTP identity is stable and locally namespaced", "[identity][iroh]") {
    const auto result = iroh_forwarded_header_provider(options())(context());
    REQUIRE(result.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(result.identities.size() == 1);
    const auto& identity = result.identities.front();
    REQUIRE(identity.provider() == "iroh");
    REQUIRE(identity.evidence_source() == "http_proxy");
    REQUIRE(identity.assurance() == IdentityAssurance::CONFIGURED_PROXY);
    REQUIRE(identity.issuer() == "production-mesh");
    REQUIRE(identity.transport() == "http");
    REQUIRE(identity.subject_kind() == PeerSubjectKind::ENDPOINT);
    REQUIRE(identity.subject_key() == std::optional<std::string>(kEndpoint));
    REQUIRE(identity.subject_stability() == SubjectStability::STABLE);
    REQUIRE(identity.subject_verified());
    REQUIRE(identity.attributes() == nlohmann::json{{"original_assurance", "cryptographic_peer"}});
    REQUIRE(identity.source_address() == std::optional<std::string>(kEndpoint));
    REQUIRE(identity.proxy_address() == std::optional<std::string>("127.0.0.1"));

    const PeerEvidenceSet evidence({result});
    const auto auth = peer_identity_primary("iroh")(evidence, AuthContext::anonymous());
    REQUIRE(auth.authenticated);
    REQUIRE(auth.principal == std::optional<std::string>(
                                  std::string("peer/iroh/production-mesh/") + kEndpoint));
}

TEST_CASE("forwarded Iroh HTTP identity fails closed", "[identity][iroh]") {
    const auto provider = iroh_forwarded_header_provider(options());
    REQUIRE(provider(context("192.0.2.1")).status == PeerIdentityStatus::UNTRUSTED_PROXY);
    REQUIRE(provider(context("::ffff:127.0.0.1")).status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(provider(context("127.0.0.1", {})).status == PeerIdentityStatus::NO_MATCH);
    REQUIRE(provider(context("127.0.0.1", {{IROH_FORWARDED_ENDPOINT_HEADER, {kEndpoint, kEndpoint}}}))
                .status == PeerIdentityStatus::INVALID);
    REQUIRE(provider(context("127.0.0.1", {{IROH_FORWARDED_ENDPOINT_HEADER, {"00"}}}))
                .status == PeerIdentityStatus::INVALID);

    std::string uppercase(kEndpoint);
    uppercase[10] = 'A';
    REQUIRE(provider(context("127.0.0.1", {{IROH_FORWARDED_ENDPOINT_HEADER, {uppercase}}}))
                .status == PeerIdentityStatus::INVALID);

    auto duplicates = context();
    duplicates.headers["vgi-forwarded-iroh-endpoint"] = {kEndpoint};
    REQUIRE(provider(duplicates).status == PeerIdentityStatus::INVALID);

    auto control = context();
    control.headers[IROH_FORWARDED_ENDPOINT_HEADER] = {std::string(kEndpoint) + "\r"};
    REQUIRE(provider(control).status == PeerIdentityStatus::INVALID);
}

TEST_CASE("forwarded Iroh HTTP provider requires an explicit exact trust boundary",
          "[identity][iroh]") {
    REQUIRE_THROWS_AS(iroh_forwarded_header_provider({"production-mesh", {}}),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(iroh_forwarded_header_provider({"production-mesh", {"localhost"}}),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(iroh_forwarded_header_provider({"bad\nissuer", {"127.0.0.1"}}),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(
        iroh_forwarded_header_provider({"production-mesh", {"127.0.0.1", "::ffff:127.0.0.1"}}),
        std::invalid_argument);
}
