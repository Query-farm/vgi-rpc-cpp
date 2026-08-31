// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/identity.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <set>

#include "vgi_rpc/crypto.h"

namespace vgi_rpc {
namespace {

constexpr size_t kMaxJsonBytes = 65'536;
constexpr size_t kMaxJsonDepth = 16;
constexpr size_t kMaxJsonValues = 4'096;
constexpr size_t kMaxHeaderFields = 256;
constexpr size_t kMaxHeaderBytes = 65'536;

bool valid_utf8(const std::string& value) {
    size_t i = 0;
    while (i < value.size()) {
        const auto first = static_cast<unsigned char>(value[i++]);
        if (first < 0x80) continue;
        size_t continuation = 0;
        uint32_t code = 0;
        if ((first & 0xe0) == 0xc0) {
            continuation = 1;
            code = first & 0x1f;
        } else if ((first & 0xf0) == 0xe0) {
            continuation = 2;
            code = first & 0x0f;
        } else if ((first & 0xf8) == 0xf0) {
            continuation = 3;
            code = first & 0x07;
        } else
            return false;
        if (i + continuation > value.size()) return false;
        for (size_t n = 0; n < continuation; ++n) {
            const auto byte = static_cast<unsigned char>(value[i++]);
            if ((byte & 0xc0) != 0x80) return false;
            code = (code << 6) | (byte & 0x3f);
        }
        if ((continuation == 1 && code < 0x80) || (continuation == 2 && code < 0x800) ||
            (continuation == 3 && code < 0x10000) || code > 0x10ffff ||
            (code >= 0xd800 && code <= 0xdfff))
            return false;
    }
    return true;
}

void require_text(const std::string& value, const char* field) {
    if (!valid_utf8(value))
        throw std::invalid_argument(std::string(field) + " contains invalid UTF-8");
}

void require_header_text(const std::string& value, const char* field) {
    require_text(value, field);
    if (std::any_of(value.begin(), value.end(),
                    [](char byte) { return byte == '\r' || byte == '\n' || byte == '\0'; }))
        throw PeerIdentityRejected(std::string(field) + " contains a control character");
}

void validate_json(const nlohmann::json& value, size_t depth, size_t& count) {
    if (depth > kMaxJsonDepth)
        throw std::invalid_argument("peer evidence exceeds JSON depth limit");
    if (++count > kMaxJsonValues)
        throw std::invalid_argument("peer evidence exceeds JSON value limit");
    if (value.is_string())
        require_text(value.get_ref<const std::string&>(), "peer evidence string");
    if (value.is_number_float() && !std::isfinite(value.get<double>()))
        throw std::invalid_argument("peer evidence numbers must be finite");
    if (value.is_object()) {
        for (const auto& [key, item] : value.items()) {
            require_text(key, "peer evidence key");
            validate_json(item, depth + 1, count);
        }
    } else if (value.is_array()) {
        for (const auto& item : value) validate_json(item, depth + 1, count);
    }
}

void validate_json_object(const nlohmann::json& value) {
    if (!value.is_object()) throw std::invalid_argument("peer evidence must be a JSON object");
    size_t count = 0;
    validate_json(value, 1, count);
    if (value.dump().size() > kMaxJsonBytes)
        throw std::invalid_argument("peer evidence exceeds JSON byte limit");
}

std::string percent(const std::string& value) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    for (const unsigned char byte : value) {
        if (std::isalnum(byte) || byte == '-' || byte == '.' || byte == '_' || byte == '~')
            out += char(byte);
        else {
            out += '%';
            out += hex[byte >> 4];
            out += hex[byte & 15];
        }
    }
    return out;
}

std::string wire(PeerIdentityStatus value) {
    switch (value) {
        case PeerIdentityStatus::OFF: return "off";
        case PeerIdentityStatus::NOT_APPLICABLE: return "not_applicable";
        case PeerIdentityStatus::AVAILABLE: return "available";
        case PeerIdentityStatus::UNAVAILABLE: return "unavailable";
        case PeerIdentityStatus::PERMISSION_DENIED: return "permission_denied";
        case PeerIdentityStatus::NO_MATCH: return "no_match";
        case PeerIdentityStatus::INVALID: return "invalid";
        case PeerIdentityStatus::UNTRUSTED_PROXY: return "untrusted_proxy";
    }
    throw std::invalid_argument("invalid peer status");
}
std::string wire(IdentityAssurance value) {
    switch (value) {
        case IdentityAssurance::CRYPTOGRAPHIC_PEER: return "cryptographic_peer";
        case IdentityAssurance::LOCAL_DAEMON: return "local_daemon";
        case IdentityAssurance::CONFIGURED_PROXY: return "configured_proxy";
    }
    throw std::invalid_argument("invalid assurance");
}
std::string wire(PeerSubjectKind value) {
    switch (value) {
        case PeerSubjectKind::USER: return "user";
        case PeerSubjectKind::TAGGED_NODE: return "tagged_node";
        case PeerSubjectKind::WORKLOAD: return "workload";
        case PeerSubjectKind::ENDPOINT: return "endpoint";
        case PeerSubjectKind::UNKNOWN: return "unknown";
    }
    throw std::invalid_argument("invalid subject kind");
}
std::string wire(SubjectStability value) {
    switch (value) {
        case SubjectStability::STABLE: return "stable";
        case SubjectStability::LOGIN: return "login";
        case SubjectStability::NONE: return "none";
    }
    throw std::invalid_argument("invalid subject stability");
}
void add_field(crypto::Sha256& digest, const std::string& field) {
    uint8_t length[8];
    uint64_t size = field.size();
    for (int index = 7; index >= 0; --index) {
        length[index] = uint8_t(size & 0xff);
        size >>= 8;
    }
    digest.update(length, sizeof(length));
    digest.update(field);
}

AuthContext with_binding(const AuthContext& auth, const PeerEvidenceSet& evidence,
                         const std::vector<std::string>& providers,
                         const AuthContext* application = nullptr) {
    AuthContext result = auth;
    result.claims["peer_evidence_binding"] = evidence.binding_digest(providers, application);
    return result;
}

void add_primary_identity_claims(AuthContext& auth, const PeerIdentity& identity) {
    auth.claims["issuer"] = identity.issuer();
    auth.claims["subject_kind"] = wire(identity.subject_kind());
    auth.claims["assurance"] = wire(identity.assurance());
    auth.claims["evidence_source"] = identity.evidence_source();
    auth.claims["subject"] = identity.subject_key().value();
}
}  // namespace

AuthContext AuthContext::anonymous() {
    return {};
}

void PeerResolutionContext::validate() const {
    if (transport.empty()) throw std::invalid_argument("transport is required");
    require_text(transport, "transport");
    for (const auto* value : {&immediate_peer, &source_endpoint, &asserted_peer,
                              &destination_address, &authority, &service_name})
        if (*value) require_text(**value, "peer resolution field");
    validate_json_object(metadata);
    if (headers.size() > kMaxHeaderFields)
        throw PeerIdentityRejected("too many peer identity headers");
    std::set<std::string> normalized;
    size_t header_bytes = 0;
    for (const auto& [name, values] : headers) {
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return char(std::tolower(c)); });
        if (!normalized.insert(lower).second)
            throw PeerIdentityRejected("case-varied duplicate header");
        if (values.size() > 1) throw PeerIdentityRejected("duplicate peer identity header");
        require_header_text(name, "header name");
        header_bytes += name.size();
        for (const auto& value : values) {
            require_header_text(value, "header value");
            header_bytes += value.size();
        }
        if (header_bytes > kMaxHeaderBytes)
            throw PeerIdentityRejected("peer identity headers exceed byte limit");
    }
}
std::optional<std::string> PeerResolutionContext::header(const std::string& name) const {
    std::string sought = name;
    std::transform(sought.begin(), sought.end(), sought.begin(),
                   [](unsigned char c) { return char(std::tolower(c)); });
    for (const auto& [candidate, values] : headers) {
        std::string lower = candidate;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return char(std::tolower(c)); });
        if (lower == sought) {
            if (values.size() > 1) throw PeerIdentityRejected("duplicate peer identity header");
            return values.empty() ? std::nullopt : std::optional<std::string>(values.front());
        }
    }
    return std::nullopt;
}
std::chrono::steady_clock::duration PeerResolutionContext::remaining_budget() const noexcept {
    return std::max(deadline - std::chrono::steady_clock::now(),
                    std::chrono::steady_clock::duration::zero());
}

PeerIdentity::PeerIdentity(std::string provider, std::string evidence_source,
                           IdentityAssurance assurance, std::string issuer, std::string transport,
                           PeerSubjectKind subject_kind, std::optional<std::string> subject_key,
                           SubjectStability subject_stability, bool subject_verified,
                           nlohmann::json attributes, nlohmann::json capabilities,
                           bool capabilities_verified, std::optional<std::string> source_address,
                           std::optional<std::string> proxy_address)
    : provider_(std::move(provider)),
      evidence_source_(std::move(evidence_source)),
      issuer_(std::move(issuer)),
      transport_(std::move(transport)),
      assurance_(assurance),
      subject_kind_(subject_kind),
      subject_key_(std::move(subject_key)),
      subject_stability_(subject_stability),
      subject_verified_(subject_verified),
      attributes_(std::move(attributes)),
      capabilities_(std::move(capabilities)),
      capabilities_verified_(capabilities_verified),
      source_address_(std::move(source_address)),
      proxy_address_(std::move(proxy_address)) {
    if (provider_.empty() || evidence_source_.empty() || issuer_.empty() || transport_.empty())
        throw std::invalid_argument(
            "provider, evidence source, issuer, and transport are required");
    require_text(provider_, "provider");
    require_text(evidence_source_, "evidence source");
    require_text(issuer_, "issuer");
    require_text(transport_, "transport");
    (void)wire(assurance_);
    (void)wire(subject_kind_);
    (void)wire(subject_stability_);
    if (subject_key_) require_text(*subject_key_, "subject key");
    if (subject_verified_ && (!subject_key_ || subject_key_->empty()))
        throw std::invalid_argument("verified identity requires a subject key");
    if (!subject_key_ && subject_stability_ != SubjectStability::NONE)
        throw std::invalid_argument("subjectless identity must have none stability");
    validate_json_object(attributes_);
    validate_json_object(capabilities_);
}
std::string PeerIdentity::canonical_principal() const {
    if (!subject_key_) throw std::logic_error("subjectless evidence has no canonical principal");
    return "peer/" + percent(provider_) + "/" + percent(issuer_) + "/" + percent(*subject_key_);
}

void PeerIdentityResult::validate() const {
    if (provider.empty()) throw std::invalid_argument("peer result provider is required");
    (void)wire(status);
    if ((status == PeerIdentityStatus::AVAILABLE) != !identities.empty())
        throw std::invalid_argument("only available results may carry identities");
    for (const auto& identity : identities)
        if (identity.provider() != provider)
            throw std::invalid_argument("peer result provider mismatch");
}
PeerIdentityResult PeerIdentityResult::available(PeerIdentity identity) {
    PeerIdentityResult result{
        identity.provider(), PeerIdentityStatus::AVAILABLE, {std::move(identity)}};
    result.validate();
    return result;
}

PeerEvidenceSet::PeerEvidenceSet(std::vector<PeerIdentityResult> results) {
    for (auto& result : results) {
        result.validate();
        if (!statuses_.emplace(result.provider, result.status).second)
            throw std::invalid_argument("duplicate peer provider");
        std::move(result.identities.begin(), result.identities.end(),
                  std::back_inserter(identities_));
    }
}
PeerIdentityStatus PeerEvidenceSet::status(const std::string& provider) const noexcept {
    const auto found = statuses_.find(provider);
    return found == statuses_.end() ? PeerIdentityStatus::OFF : found->second;
}
std::vector<std::reference_wrapper<const PeerIdentity>> PeerEvidenceSet::for_provider(
    const std::string& provider) const {
    std::vector<std::reference_wrapper<const PeerIdentity>> out;
    for (const auto& identity : identities_)
        if (identity.provider() == provider) out.emplace_back(identity);
    return out;
}
std::vector<std::reference_wrapper<const PeerIdentity>> PeerEvidenceSet::eligible_subjects(
    const std::string& provider) const {
    auto all = for_provider(provider);
    all.erase(std::remove_if(all.begin(), all.end(),
                             [](const auto& identity) {
                                 const auto& value = identity.get();
                                 return !value.subject_verified() || !value.subject_key() ||
                                        value.subject_stability() != SubjectStability::STABLE;
                             }),
              all.end());
    return all;
}
const PeerIdentity& PeerEvidenceSet::unique_verified_subject(const std::string& provider) const {
    auto found = eligible_subjects(provider);
    if (found.size() != 1)
        throw PeerIdentityRejected("provider did not produce one verified stable subject");
    return found.front();
}
void PeerEvidenceSet::require_available_provider(const std::string& provider) const {
    const auto outcome = status(provider);
    if (outcome == PeerIdentityStatus::UNAVAILABLE ||
        outcome == PeerIdentityStatus::PERMISSION_DENIED)
        throw PeerIdentityUnavailable("peer identity provider is unavailable");
    if (outcome == PeerIdentityStatus::INVALID || outcome == PeerIdentityStatus::UNTRUSTED_PROXY)
        throw PeerIdentityRejected("peer identity provider rejected evidence");
    if (outcome != PeerIdentityStatus::AVAILABLE || for_provider(provider).empty())
        throw PeerIdentityRejected("peer identity provider did not produce evidence");
}
const PeerIdentity& PeerEvidenceSet::require_usable_provider(const std::string& provider) const {
    require_available_provider(provider);
    return unique_verified_subject(provider);
}
std::string PeerEvidenceSet::binding_digest(const std::vector<std::string>& providers,
                                            const AuthContext* application_auth) const {
    std::set<std::string> selected(providers.begin(), providers.end());
    crypto::Sha256 digest;
    for (const auto& provider : selected) {
        add_field(digest, provider);
        add_field(digest, wire(status(provider)));
        std::vector<std::vector<std::string>> rows;
        for (const auto& ref : for_provider(provider)) {
            const auto& i = ref.get();
            rows.push_back({i.provider(), i.issuer(), i.subject_key().value_or(""),
                            wire(i.assurance()), i.evidence_source(), i.transport(),
                            wire(i.subject_kind()), wire(i.subject_stability()),
                            i.subject_verified() ? "true" : "false",
                            i.capabilities_verified() ? "true" : "false", "", "",
                            i.attributes().dump(), i.capabilities().dump()});
        }
        std::sort(rows.begin(), rows.end());
        for (const auto& row : rows)
            for (const auto& field : row) add_field(digest, field);
    }
    if (application_auth) {
        add_field(digest, "application_auth");
        add_field(digest, application_auth->domain);
        add_field(digest, application_auth->principal.value_or(""));
    }
    return digest.hex_digest();
}

AuthContext observe_peer_identity(const PeerEvidenceSet&, const AuthContext& auth) {
    return auth;
}
PeerAuthenticationPolicy require_peer_identity(std::string provider) {
    return
        [provider = std::move(provider)](const PeerEvidenceSet& evidence, const AuthContext& auth) {
            evidence.require_available_provider(provider);
            return with_binding(auth, evidence, {provider});
        };
}
PeerAuthenticationPolicy peer_identity_primary(std::string provider) {
    return [provider = std::move(provider)](const PeerEvidenceSet& evidence, const AuthContext&) {
        const auto& identity = evidence.require_usable_provider(provider);
        AuthContext auth{provider, true, identity.canonical_principal(), nlohmann::json::object()};
        add_primary_identity_claims(auth, identity);
        auth.claims["peer_evidence_binding"] = evidence.binding_digest({provider});
        return auth;
    };
}
PeerAuthenticationPolicy any_of_peer_identities(std::vector<std::string> providers) {
    if (providers.empty()) throw std::invalid_argument("at least one provider is required");
    return [providers = std::move(providers)](const PeerEvidenceSet& evidence,
                                              const AuthContext& auth) {
        for (const auto& provider : providers) {
            const auto status = evidence.status(provider);
            if (status == PeerIdentityStatus::INVALID ||
                status == PeerIdentityStatus::UNTRUSTED_PROXY ||
                evidence.eligible_subjects(provider).size() > 1)
                throw PeerIdentityRejected("invalid or ambiguous peer evidence");
        }
        if (auth.authenticated) return auth;
        for (const auto& provider : providers)
            if (evidence.eligible_subjects(provider).size() == 1)
                return peer_identity_primary(provider)(evidence, auth);
        for (const auto& provider : providers)
            if (evidence.status(provider) == PeerIdentityStatus::UNAVAILABLE ||
                evidence.status(provider) == PeerIdentityStatus::PERMISSION_DENIED)
                throw PeerIdentityUnavailable(
                    "no usable factor and a peer provider is unavailable");
        throw PeerIdentityRejected("no provider produced a verified subject");
    };
}
PeerAuthenticationPolicy all_of_peer_identities(std::vector<std::string> providers,
                                                PeerIdentityLinker linker,
                                                std::optional<std::string> principal_provider) {
    if (providers.empty() || !linker)
        throw std::invalid_argument("all-of requires providers and linker");
    const std::string primary = principal_provider.value_or(providers.front());
    if (std::find(providers.begin(), providers.end(), primary) == providers.end())
        throw std::invalid_argument("principal provider must be configured");
    return [providers = std::move(providers), linker = std::move(linker), primary](
               const PeerEvidenceSet& evidence, const AuthContext& application) {
        if (!application.authenticated)
            throw PeerIdentityRejected("all-of requires application auth");
        std::map<std::string, std::reference_wrapper<const PeerIdentity>> identities;
        for (const auto& provider : providers)
            identities.emplace(provider, evidence.require_usable_provider(provider));
        linker(application, identities);
        const auto& identity = identities.at(primary).get();
        AuthContext auth{primary, true, identity.canonical_principal(), nlohmann::json::object()};
        add_primary_identity_claims(auth, identity);
        auth.claims["application_domain"] = application.domain;
        auth.claims["application_principal"] = application.principal.value_or("");
        auth.claims["peer_evidence_binding"] = evidence.binding_digest(providers, &application);
        return auth;
    };
}

}  // namespace vgi_rpc
