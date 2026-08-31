// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

enum class PeerIdentityStatus {
    OFF, NOT_APPLICABLE, AVAILABLE, UNAVAILABLE, PERMISSION_DENIED, NO_MATCH, INVALID,
    UNTRUSTED_PROXY,
};
enum class IdentityAssurance { CRYPTOGRAPHIC_PEER, LOCAL_DAEMON, CONFIGURED_PROXY };
enum class PeerSubjectKind { USER, TAGGED_NODE, WORKLOAD, ENDPOINT, UNKNOWN };
enum class SubjectStability { STABLE, LOGIN, NONE };

struct VGI_RPC_EXPORT AuthContext {
    std::string domain;
    bool authenticated = false;
    std::optional<std::string> principal;
    nlohmann::json claims = nlohmann::json::object();

    static AuthContext anonymous();
};

struct VGI_RPC_EXPORT PeerResolutionContext {
    std::string transport;
    std::optional<std::string> immediate_peer;
    std::optional<std::string> source_endpoint;
    std::optional<std::string> asserted_peer;
    std::optional<std::string> destination_address;
    std::optional<std::string> authority;
    std::optional<std::string> service_name;
    std::map<std::string, std::vector<std::string>> headers;
    nlohmann::json metadata = nlohmann::json::object();
    std::chrono::steady_clock::time_point deadline{};

    void validate() const;
    std::optional<std::string> header(const std::string& name) const;
    std::chrono::steady_clock::duration remaining_budget() const noexcept;
};

class VGI_RPC_EXPORT PeerIdentity {
public:
    PeerIdentity(std::string provider, std::string evidence_source, IdentityAssurance assurance,
                 std::string issuer, std::string transport,
                 PeerSubjectKind subject_kind = PeerSubjectKind::UNKNOWN,
                 std::optional<std::string> subject_key = std::nullopt,
                 SubjectStability subject_stability = SubjectStability::NONE,
                 bool subject_verified = false,
                 nlohmann::json attributes = nlohmann::json::object(),
                 nlohmann::json capabilities = nlohmann::json::object(),
                 bool capabilities_verified = false,
                 std::optional<std::string> source_address = std::nullopt,
                 std::optional<std::string> proxy_address = std::nullopt);

    const std::string& provider() const noexcept { return provider_; }
    const std::string& evidence_source() const noexcept { return evidence_source_; }
    IdentityAssurance assurance() const noexcept { return assurance_; }
    const std::string& issuer() const noexcept { return issuer_; }
    const std::string& transport() const noexcept { return transport_; }
    PeerSubjectKind subject_kind() const noexcept { return subject_kind_; }
    const std::optional<std::string>& subject_key() const noexcept { return subject_key_; }
    SubjectStability subject_stability() const noexcept { return subject_stability_; }
    bool subject_verified() const noexcept { return subject_verified_; }
    const nlohmann::json& attributes() const noexcept { return attributes_; }
    const nlohmann::json& capabilities() const noexcept { return capabilities_; }
    bool capabilities_verified() const noexcept { return capabilities_verified_; }
    const std::optional<std::string>& source_address() const noexcept { return source_address_; }
    const std::optional<std::string>& proxy_address() const noexcept { return proxy_address_; }
    std::string canonical_principal() const;

private:
    std::string provider_, evidence_source_, issuer_, transport_;
    IdentityAssurance assurance_;
    PeerSubjectKind subject_kind_;
    std::optional<std::string> subject_key_;
    SubjectStability subject_stability_;
    bool subject_verified_;
    nlohmann::json attributes_, capabilities_;
    bool capabilities_verified_;
    std::optional<std::string> source_address_, proxy_address_;
};

struct VGI_RPC_EXPORT PeerIdentityResult {
    std::string provider;
    PeerIdentityStatus status = PeerIdentityStatus::OFF;
    std::vector<PeerIdentity> identities;

    void validate() const;
    static PeerIdentityResult available(PeerIdentity identity);
};

class VGI_RPC_EXPORT PeerIdentityUnavailable : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
class VGI_RPC_EXPORT PeerIdentityRejected : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class VGI_RPC_EXPORT PeerEvidenceSet {
public:
    PeerEvidenceSet() = default;
    explicit PeerEvidenceSet(std::vector<PeerIdentityResult> results);

    const std::vector<PeerIdentity>& identities() const noexcept { return identities_; }
    PeerIdentityStatus status(const std::string& provider) const noexcept;
    std::vector<std::reference_wrapper<const PeerIdentity>> for_provider(
        const std::string& provider) const;
    std::vector<std::reference_wrapper<const PeerIdentity>> eligible_subjects(
        const std::string& provider) const;
    const PeerIdentity& unique_verified_subject(const std::string& provider) const;
    void require_available_provider(const std::string& provider) const;
    const PeerIdentity& require_usable_provider(const std::string& provider) const;
    std::string binding_digest(const std::vector<std::string>& providers,
                               const AuthContext* application_auth = nullptr) const;

private:
    std::vector<PeerIdentity> identities_;
    std::map<std::string, PeerIdentityStatus> statuses_;
};

using PeerIdentityProvider =
    std::function<PeerIdentityResult(const PeerResolutionContext& context)>;
using PeerAuthenticationPolicy =
    std::function<AuthContext(const PeerEvidenceSet& evidence, const AuthContext& existing)>;
using PeerIdentityLinker = std::function<void(
    const AuthContext&, const std::map<std::string, std::reference_wrapper<const PeerIdentity>>&)>;

VGI_RPC_EXPORT AuthContext observe_peer_identity(const PeerEvidenceSet&, const AuthContext&);
VGI_RPC_EXPORT PeerAuthenticationPolicy require_peer_identity(std::string provider);
VGI_RPC_EXPORT PeerAuthenticationPolicy peer_identity_primary(std::string provider);
VGI_RPC_EXPORT PeerAuthenticationPolicy any_of_peer_identities(std::vector<std::string> providers);
VGI_RPC_EXPORT PeerAuthenticationPolicy all_of_peer_identities(
    std::vector<std::string> providers, PeerIdentityLinker linker,
    std::optional<std::string> principal_provider = std::nullopt);

}  // namespace vgi_rpc
