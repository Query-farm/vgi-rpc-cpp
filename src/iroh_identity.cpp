// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/iroh_identity.h"

#include <algorithm>
#include <optional>
#include <set>
#include <utility>

#include "identity_provider_internal.h"
#include "iroh_identity_internal.h"

namespace vgi_rpc {
namespace {

using identity_internal::IpAddress;
using identity_internal::has_control;
using identity_internal::parse_exact_ip;
using identity_internal::result;
using identity_internal::valid_utf8;

constexpr const char* kProvider = "iroh";

}  // namespace

namespace iroh_internal {

void validate_issuer(const std::string& issuer) {
    if (issuer.empty() || !valid_utf8(issuer) || has_control(issuer))
        throw std::invalid_argument(
            "Iroh issuer must be a non-empty Unicode string without controls");
}

bool canonical_endpoint(const std::string& endpoint) noexcept {
    return endpoint.size() == 64 &&
           std::all_of(endpoint.begin(), endpoint.end(), [](unsigned char value) {
               return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
           });
}

PeerIdentity forwarded_identity(const std::string& endpoint, const std::string& issuer,
                                const std::string& transport,
                                const std::string& evidence_source,
                                const std::string& proxy_address) {
    validate_issuer(issuer);
    if (!canonical_endpoint(endpoint))
        throw std::invalid_argument("Iroh EndpointId must be canonical lowercase hexadecimal");
    return PeerIdentity(kProvider, evidence_source, IdentityAssurance::CONFIGURED_PROXY, issuer,
                        transport, PeerSubjectKind::ENDPOINT, endpoint, SubjectStability::STABLE,
                        true, {{"original_assurance", "cryptographic_peer"}},
                        nlohmann::json::object(), false, endpoint, proxy_address);
}

}  // namespace iroh_internal

PeerIdentityProvider iroh_forwarded_header_provider(IrohForwardedHeaderOptions options) {
    iroh_internal::validate_issuer(options.issuer);
    if (options.trusted_proxy_addresses.empty())
        throw std::invalid_argument("at least one exact Iroh bridge address is required");
    std::set<IpAddress> trusted;
    for (const auto& value : options.trusted_proxy_addresses) {
        const auto address = parse_exact_ip(value);
        if (!address)
            throw std::invalid_argument("Iroh bridge address is not an exact IPv4 or IPv6 address");
        if (!trusted.insert(*address).second)
            throw std::invalid_argument("duplicate Iroh bridge address");
    }
    return [issuer = std::move(options.issuer), trusted = std::move(trusted)](
               const PeerResolutionContext& context) {
        const auto immediate = context.immediate_peer
                                   ? parse_exact_ip(*context.immediate_peer)
                                   : std::optional<IpAddress>{};
        if (!immediate || trusted.find(*immediate) == trusted.end())
            return result(kProvider, PeerIdentityStatus::UNTRUSTED_PROXY);
        try {
            context.validate();
            const auto endpoint = context.header(IROH_FORWARDED_ENDPOINT_HEADER);
            if (!endpoint) return result(kProvider, PeerIdentityStatus::NO_MATCH);
            if (!iroh_internal::canonical_endpoint(*endpoint))
                return result(kProvider, PeerIdentityStatus::INVALID);
            return PeerIdentityResult::available(iroh_internal::forwarded_identity(
                *endpoint, issuer, "http", "http_proxy", *context.immediate_peer));
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

}  // namespace vgi_rpc
