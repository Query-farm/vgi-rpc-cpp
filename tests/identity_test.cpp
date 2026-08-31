#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/identity.h"

using namespace vgi_rpc;

static PeerIdentity stable_identity() {
    return PeerIdentity("spiffe", "test", IdentityAssurance::CRYPTOGRAPHIC_PEER,
                        "spiffe://example.org", "tcp", PeerSubjectKind::WORKLOAD,
                        "spiffe://example.org/workload", SubjectStability::STABLE, true);
}

TEST_CASE("peer identity matches shared principal and binding vector") {
    auto identity = stable_identity();
    PeerEvidenceSet evidence({PeerIdentityResult::available(identity)});
    REQUIRE(identity.canonical_principal() ==
            "peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fworkload");
    REQUIRE(evidence.binding_digest({"spiffe"}) ==
            "948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367");
}

TEST_CASE("primary authentication emits the normalized identity claim contract") {
    PeerEvidenceSet evidence({PeerIdentityResult::available(stable_identity())});
    const auto auth = peer_identity_primary("spiffe")(evidence, AuthContext::anonymous());

    REQUIRE(auth.domain == "spiffe");
    REQUIRE(auth.authenticated);
    REQUIRE(auth.principal == std::optional<std::string>("peer/spiffe/spiffe%3A%2F%2Fexample.org/"
                                                         "spiffe%3A%2F%2Fexample.org%2Fworkload"));
    REQUIRE(auth.claims ==
            nlohmann::json{{"issuer", "spiffe://example.org"},
                           {"subject_kind", "workload"},
                           {"assurance", "cryptographic_peer"},
                           {"evidence_source", "test"},
                           {"subject", "spiffe://example.org/workload"},
                           {"peer_evidence_binding",
                            "948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367"}});
}

TEST_CASE("all-of authentication retains selected-primary and application claims") {
    PeerIdentity tailscale("tailscale", "localapi", IdentityAssurance::LOCAL_DAEMON, "tailnet:test",
                           "tcp", PeerSubjectKind::TAGGED_NODE, "node:n1", SubjectStability::STABLE,
                           true);
    PeerEvidenceSet evidence({PeerIdentityResult::available(stable_identity()),
                              PeerIdentityResult::available(std::move(tailscale))});
    AuthContext application{"bearer", true, "alice", {{"scope", "run"}}};
    bool linked = false;
    auto policy = all_of_peer_identities(
        {"spiffe", "tailscale"},
        [&](const AuthContext& observed,
            const std::map<std::string, std::reference_wrapper<const PeerIdentity>>& identities) {
            REQUIRE(observed.principal == std::optional<std::string>("alice"));
            REQUIRE(identities.size() == 2);
            REQUIRE(identities.at("tailscale").get().subject_key() ==
                    std::optional<std::string>("node:n1"));
            linked = true;
        },
        "tailscale");
    const auto auth = policy(evidence, application);

    REQUIRE(linked);
    REQUIRE(auth.domain == "tailscale");
    REQUIRE(auth.authenticated);
    REQUIRE(auth.principal ==
            std::optional<std::string>("peer/tailscale/tailnet%3Atest/node%3An1"));
    REQUIRE(auth.claims ==
            nlohmann::json{{"issuer", "tailnet:test"},
                           {"subject_kind", "tagged_node"},
                           {"assurance", "local_daemon"},
                           {"evidence_source", "localapi"},
                           {"subject", "node:n1"},
                           {"application_domain", "bearer"},
                           {"application_principal", "alice"},
                           {"peer_evidence_binding",
                            "5d792c707745d04c8f30f39c622a2e5a5eef5670e7d37fa354c1ecdd52ca977b"}});
}

TEST_CASE("peer binding ignores routing topology but not capabilities") {
    auto topology = [](std::string source, std::string proxy, nlohmann::json capabilities) {
        return PeerIdentity("spiffe", "test", IdentityAssurance::CRYPTOGRAPHIC_PEER,
                            "spiffe://example.org", "tcp", PeerSubjectKind::WORKLOAD,
                            "spiffe://example.org/workload", SubjectStability::STABLE, true,
                            nlohmann::json::object(), std::move(capabilities), false,
                            std::move(source), std::move(proxy));
    };
    PeerEvidenceSet first({PeerIdentityResult::available(
        topology("100.64.0.1:40001", "10.0.0.10", nlohmann::json::object()))});
    PeerEvidenceSet second({PeerIdentityResult::available(
        topology("100.64.0.1:49999", "10.0.0.11", nlohmann::json::object()))});
    REQUIRE(first.binding_digest({"spiffe"}) == second.binding_digest({"spiffe"}));
    PeerEvidenceSet changed({PeerIdentityResult::available(
        topology("100.64.0.1:49999", "10.0.0.11", {{"query.farm/run", nlohmann::json::array()}}))});
    REQUIRE(first.binding_digest({"spiffe"}) != changed.binding_digest({"spiffe"}));
}

TEST_CASE("require accepts capability-only evidence but primary rejects it") {
    PeerIdentity capability("tailscale", "serve", IdentityAssurance::CONFIGURED_PROXY,
                            "tailnet:test", "http", PeerSubjectKind::UNKNOWN, std::nullopt,
                            SubjectStability::NONE, false, nlohmann::json::object(),
                            {{"query.farm/can-run", {{{"worker", "analytics"}}}}}, true);
    PeerEvidenceSet evidence({PeerIdentityResult::available(std::move(capability))});
    AuthContext application{"bearer", true, "alice", nlohmann::json::object()};
    const auto required = require_peer_identity("tailscale")(evidence, application);
    REQUIRE(required.principal == std::optional<std::string>("alice"));
    REQUIRE_THROWS_AS(peer_identity_primary("tailscale")(evidence, AuthContext::anonymous()),
                      PeerIdentityRejected);
}

TEST_CASE("peer evidence rejects malformed unicode and over-deep JSON") {
    REQUIRE_THROWS_AS(PeerIdentity("spiffe", "test", IdentityAssurance::LOCAL_DAEMON, "issuer",
                                   "tcp", PeerSubjectKind::WORKLOAD, std::string("bad\xED\xA0\x80"),
                                   SubjectStability::STABLE, true),
                      std::invalid_argument);
    nlohmann::json deep = nlohmann::json::object();
    for (int i = 0; i < 17; ++i) deep = nlohmann::json{{"next", deep}};
    REQUIRE_THROWS_AS(
        PeerIdentity("test", "test", IdentityAssurance::LOCAL_DAEMON, "issuer", "tcp",
                     PeerSubjectKind::UNKNOWN, std::nullopt, SubjectStability::NONE, false, deep),
        std::invalid_argument);
}

TEST_CASE("resolution context rejects duplicate and unsafe headers") {
    PeerResolutionContext duplicate;
    duplicate.transport = "http";
    duplicate.headers = {{"X-Peer", {"one", "two"}}};
    REQUIRE_THROWS_AS(duplicate.validate(), PeerIdentityRejected);

    PeerResolutionContext case_varied;
    case_varied.transport = "http";
    case_varied.headers = {{"X-Peer", {"one"}}, {"x-peer", {"two"}}};
    REQUIRE_THROWS_AS(case_varied.validate(), PeerIdentityRejected);

    PeerResolutionContext control;
    control.transport = "http";
    control.headers = {{"X-Peer", {"one\r\ntwo"}}};
    REQUIRE_THROWS_AS(control.validate(), PeerIdentityRejected);
}
