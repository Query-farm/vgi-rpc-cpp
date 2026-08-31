// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/spiffe_proxy_identity.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <stdexcept>

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "identity_provider_internal.h"

namespace vgi_rpc {
namespace {
using namespace identity_internal;

constexpr const char* kProvider = "spiffe";

struct Boundary {
    std::set<std::string> trust_domains;
    std::set<IpAddress> trusted_proxies;
    size_t max_bytes;

    bool trusts(const std::optional<std::string>& peer) const {
        if (!peer) return false;
        const auto address = parse_exact_ip(*peer);
        return address && trusted_proxies.contains(*address);
    }
};

bool valid_trust_domain(const std::string& value) {
    if (value.empty() || value.size() > 255 ||
        (!std::islower(static_cast<unsigned char>(value[0])) &&
         !std::isdigit(static_cast<unsigned char>(value[0]))))
        return false;
    if (!std::islower(static_cast<unsigned char>(value.back())) &&
        !std::isdigit(static_cast<unsigned char>(value.back())))
        return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || std::isdigit(c) || c == '.' || c == '_' || c == '-';
    });
}

Boundary make_boundary(const SpiffeProxyOptions& options) {
    if (options.trust_domains.empty() || options.trusted_proxy_addresses.empty())
        throw std::invalid_argument("SPIFFE trust domains and exact proxies are required");
    Boundary boundary{{}, {}, options.max_header_bytes == 0 ? 16'384 : options.max_header_bytes};
    for (const auto& domain : options.trust_domains) {
        if (!valid_trust_domain(domain)) throw std::invalid_argument("invalid SPIFFE trust domain");
        boundary.trust_domains.insert(domain);
    }
    for (const auto& value : options.trusted_proxy_addresses) {
        auto address = parse_exact_ip(value);
        if (!address)
            throw std::invalid_argument("trusted SPIFFE proxy is not an exact IP address");
        boundary.trusted_proxies.insert(*address);
    }
    return boundary;
}

std::string validate_id(const std::string& value, const std::set<std::string>& domains) {
    if (value.empty() || value.size() > 2048 || !valid_utf8(value) ||
        value.find('%') != std::string::npos || value.rfind("spiffe://", 0) != 0)
        throw std::invalid_argument("invalid SPIFFE ID size or encoding");
    if (std::any_of(value.begin(), value.end(),
                    [](unsigned char c) { return c < 0x20 || c > 0x7e; }))
        throw std::invalid_argument("SPIFFE ID must contain canonical ASCII");
    const auto authority_start = std::string("spiffe://").size();
    const auto slash = value.find('/', authority_start);
    if (slash == std::string::npos) throw std::invalid_argument("SPIFFE ID has no path");
    const auto domain = value.substr(authority_start, slash - authority_start);
    if (!valid_trust_domain(domain) || !domains.contains(domain) ||
        domain.find(':') != std::string::npos || domain.find('@') != std::string::npos)
        throw std::invalid_argument("SPIFFE trust domain is not allowed");
    if (value.find_first_of("?#", slash) != std::string::npos)
        throw std::invalid_argument("SPIFFE ID contains URL parameters");
    const auto path = value.substr(slash);
    if (path.size() < 2 || path.back() == '/')
        throw std::invalid_argument("non-canonical SPIFFE path");
    size_t start = 1;
    while (start < path.size()) {
        const auto end = path.find('/', start);
        const auto segment =
            path.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (segment.empty() || segment == "." || segment == ".." ||
            !std::all_of(segment.begin(), segment.end(), [](unsigned char c) {
                return std::isalnum(c) || c == '.' || c == '_' || c == '-';
            }))
            throw std::invalid_argument("non-canonical SPIFFE path segment");
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return domain;
}

std::string percent_decode(const std::string& value, bool certificate) {
    std::string out;
    out.reserve(value.size());
    auto hex = [](unsigned char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < value.size(); ++i) {
        unsigned char byte = value[i];
        if (byte == '%') {
            if (i + 2 >= value.size() || hex(value[i + 1]) < 0 || hex(value[i + 2]) < 0)
                throw std::invalid_argument("invalid percent escape");
            byte = static_cast<unsigned char>((hex(value[i + 1]) << 4) | hex(value[i + 2]));
            i += 2;
        }
        if (certificate) {
            if (byte > 0x7f || (byte < 0x20 && byte != '\r' && byte != '\n') || byte == 0x7f)
                throw std::invalid_argument("invalid decoded certificate header");
        } else if (byte < 0x20 || byte == 0x7f) {
            throw std::invalid_argument("invalid percent-decoded field");
        }
        out.push_back(char(byte));
    }
    if (!valid_utf8(out)) throw std::invalid_argument("invalid percent-decoded UTF-8");
    return out;
}

using X509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using BioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;

X509Ptr decode_certificate(const std::string& raw, size_t max_bytes) {
    if (raw.size() > max_bytes || !valid_utf8(raw) || has_control(raw))
        throw std::invalid_argument("invalid SPIFFE certificate header");
    const auto decoded = percent_decode(raw, true);
    if (decoded.size() > max_bytes) throw std::invalid_argument("oversized decoded certificate");
    BioPtr bio(BIO_new_mem_buf(decoded.data(), static_cast<int>(decoded.size())), BIO_free);
    if (!bio) throw std::runtime_error("cannot allocate certificate parser");
    X509Ptr certificate(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr), X509_free);
    if (!certificate) throw std::invalid_argument("invalid PEM certificate");
    std::array<char, 256> trailing{};
    int count = 0;
    while ((count = BIO_read(bio.get(), trailing.data(), int(trailing.size()))) > 0)
        if (!std::all_of(trailing.begin(), trailing.begin() + count,
                         [](unsigned char c) { return std::isspace(c); }))
            throw std::invalid_argument("certificate header must contain one certificate");
    return certificate;
}

bool extension_critical(X509* cert, int nid, bool& present) {
    const int index = X509_get_ext_by_NID(cert, nid, -1);
    present = index >= 0;
    return present && X509_EXTENSION_get_critical(X509_get_ext(cert, index)) == 1;
}

PeerIdentity identity_from_certificate(X509* cert, const Boundary& boundary,
                                       const std::string& evidence_source,
                                       const PeerResolutionContext& context) {
    if (X509_cmp_current_time(X509_get0_notBefore(cert)) > 0 ||
        X509_cmp_current_time(X509_get0_notAfter(cert)) < 0)
        throw std::invalid_argument("X.509-SVID outside validity period");

    bool san_present = false;
    const bool san_critical = extension_critical(cert, NID_subject_alt_name, san_present);
    std::unique_ptr<GENERAL_NAMES, decltype(&GENERAL_NAMES_free)> names(
        static_cast<GENERAL_NAMES*>(X509_get_ext_d2i(cert, NID_subject_alt_name, nullptr, nullptr)),
        GENERAL_NAMES_free);
    std::vector<std::string> uris;
    if (names) {
        for (int i = 0; i < sk_GENERAL_NAME_num(names.get()); ++i) {
            const GENERAL_NAME* name = sk_GENERAL_NAME_value(names.get(), i);
            if (name->type != GEN_URI) continue;
            const auto* bytes = ASN1_STRING_get0_data(name->d.uniformResourceIdentifier);
            const int size = ASN1_STRING_length(name->d.uniformResourceIdentifier);
            if (size < 0 || std::find(bytes, bytes + size, 0) != bytes + size)
                throw std::invalid_argument("invalid URI SAN");
            uris.emplace_back(reinterpret_cast<const char*>(bytes), size_t(size));
        }
    }
    if (uris.size() != 1) throw std::invalid_argument("X.509-SVID requires exactly one URI SAN");
    const auto trust_domain = validate_id(uris.front(), boundary.trust_domains);
    if (X509_NAME_entry_count(X509_get_subject_name(cert)) == 0 && (!san_present || !san_critical))
        throw std::invalid_argument("subjectless X.509-SVID requires critical SAN");

    std::unique_ptr<BASIC_CONSTRAINTS, decltype(&BASIC_CONSTRAINTS_free)> constraints(
        static_cast<BASIC_CONSTRAINTS*>(
            X509_get_ext_d2i(cert, NID_basic_constraints, nullptr, nullptr)),
        BASIC_CONSTRAINTS_free);
    if (!constraints || constraints->ca)
        throw std::invalid_argument("X.509-SVID leaf cannot be CA");

    bool key_usage_present = false;
    const bool key_usage_critical = extension_critical(cert, NID_key_usage, key_usage_present);
    std::unique_ptr<ASN1_BIT_STRING, decltype(&ASN1_BIT_STRING_free)> usage(
        static_cast<ASN1_BIT_STRING*>(X509_get_ext_d2i(cert, NID_key_usage, nullptr, nullptr)),
        ASN1_BIT_STRING_free);
    if (!key_usage_present || !key_usage_critical || !usage ||
        !ASN1_BIT_STRING_get_bit(usage.get(), 0) || ASN1_BIT_STRING_get_bit(usage.get(), 5) ||
        ASN1_BIT_STRING_get_bit(usage.get(), 6))
        throw std::invalid_argument("invalid X.509-SVID key usage");

    bool eku_present = false;
    (void)extension_critical(cert, NID_ext_key_usage, eku_present);
    if (eku_present) {
        std::unique_ptr<EXTENDED_KEY_USAGE, decltype(&EXTENDED_KEY_USAGE_free)> eku(
            static_cast<EXTENDED_KEY_USAGE*>(
                X509_get_ext_d2i(cert, NID_ext_key_usage, nullptr, nullptr)),
            EXTENDED_KEY_USAGE_free);
        bool client = false, server = false;
        if (eku) {
            for (int i = 0; i < sk_ASN1_OBJECT_num(eku.get()); ++i) {
                const int nid = OBJ_obj2nid(sk_ASN1_OBJECT_value(eku.get(), i));
                client = client || nid == NID_client_auth;
                server = server || nid == NID_server_auth;
            }
        }
        if (!client || !server)
            throw std::invalid_argument("invalid X.509-SVID extended key usage");
    }

    return PeerIdentity(kProvider, evidence_source, IdentityAssurance::CONFIGURED_PROXY,
                        "spiffe://" + trust_domain, "http", PeerSubjectKind::WORKLOAD, uris.front(),
                        SubjectStability::STABLE, true, nlohmann::json::object(),
                        nlohmann::json::object(), false, context.asserted_peer,
                        context.immediate_peer);
}

PeerIdentityProvider certificate_provider(SpiffeX509HeaderOptions options,
                                          bool require_verification) {
    auto boundary = make_boundary(options);
    if (!valid_header_name(options.certificate_header) ||
        (require_verification && !valid_header_name(options.verification_header)) ||
        (require_verification &&
         lower_ascii(options.certificate_header) == lower_ascii(options.verification_header)) ||
        options.evidence_source.empty() || has_control(options.verification_value))
        throw std::invalid_argument("invalid or ambiguous SPIFFE proxy headers");
    return [boundary = std::move(boundary), options = std::move(options),
            require_verification](const PeerResolutionContext& context) {
        if (!boundary.trusts(context.immediate_peer))
            return result(kProvider, PeerIdentityStatus::UNTRUSTED_PROXY);
        try {
            context.validate();
            const auto raw = context.header(options.certificate_header);
            if (!raw || raw->empty()) return result(kProvider, PeerIdentityStatus::NO_MATCH);
            if (require_verification) {
                const auto verified = context.header(options.verification_header);
                if (!verified || verified->size() > 64 || *verified != options.verification_value)
                    return result(kProvider, PeerIdentityStatus::INVALID);
            }
            return PeerIdentityResult::available(
                identity_from_certificate(decode_certificate(*raw, boundary.max_bytes).get(),
                                          boundary, options.evidence_source, context));
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

std::vector<std::string> split_xfcc(const std::string& value, char delimiter) {
    std::vector<std::string> parts;
    std::string current;
    bool quoted = false, escaped = false;
    for (char c : value) {
        if (escaped) {
            if (c != '"' && c != '\\') throw std::invalid_argument("unsupported XFCC escape");
            current.push_back(c);
            escaped = false;
        } else if (quoted && c == '\\') {
            escaped = true;
        } else if (c == '"') {
            quoted = !quoted;
            current.push_back(c);
        } else if (c == delimiter && !quoted) {
            parts.push_back(std::move(current));
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    if (quoted || escaped) throw std::invalid_argument("unterminated XFCC quoted value");
    parts.push_back(std::move(current));
    return parts;
}

std::string trim_ascii(std::string value) {
    const auto non_space = [](unsigned char c) { return !std::isspace(c); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), non_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), non_space).base(), value.end());
    return value;
}

std::map<std::string, std::vector<std::string>> parse_xfcc(const std::string& raw,
                                                           size_t max_bytes) {
    if (raw.empty() || raw.size() > max_bytes || !valid_utf8(raw) || has_control(raw) ||
        std::any_of(raw.begin(), raw.end(), [](unsigned char c) { return c > 0x7f; }))
        throw std::invalid_argument("invalid Envoy XFCC");
    const auto elements = split_xfcc(raw, ',');
    if (elements.size() != 1 || trim_ascii(elements.front()).empty())
        throw std::invalid_argument("Envoy XFCC must contain one element");
    static const std::set<std::string> allowed = {"by",      "hash", "cert", "chain",
                                                  "subject", "uri",  "dns",  "issuer"};
    std::map<std::string, std::vector<std::string>> fields;
    for (auto pair : split_xfcc(elements.front(), ';')) {
        pair = trim_ascii(std::move(pair));
        const auto equals = pair.find('=');
        if (equals == std::string::npos) throw std::invalid_argument("malformed XFCC field");
        const auto key_raw = trim_ascii(pair.substr(0, equals));
        const auto key = lower_ascii(key_raw);
        if (key_raw.empty() || !std::isalpha(static_cast<unsigned char>(key_raw.front())) ||
            !std::all_of(key_raw.begin(), key_raw.end(),
                         [](unsigned char c) { return std::isalnum(c) || c == '_' || c == '-'; }) ||
            !allowed.contains(key))
            throw std::invalid_argument("unknown XFCC field");
        auto value = trim_ascii(pair.substr(equals + 1));
        if (!value.empty() && (value.front() == '"' || value.back() == '"')) {
            if (value.size() < 2 || value.front() != '"' || value.back() != '"')
                throw std::invalid_argument("malformed quoted XFCC value");
            value = value.substr(1, value.size() - 2);
        } else if (value.empty() || value.find_first_of(",;=") != std::string::npos) {
            throw std::invalid_argument("invalid unquoted XFCC value");
        }
        if (key == "by" || key == "uri" || key == "cert" || key == "chain")
            value = percent_decode(value, false);
        if (key != "by" && key != "uri" && key != "dns" && fields.contains(key))
            throw std::invalid_argument("duplicate XFCC singleton");
        fields[key].push_back(std::move(value));
    }
    return fields;
}

PeerIdentity spiffe_header_identity(const std::string& id, const std::string& trust_domain,
                                    const std::string& source, nlohmann::json attributes,
                                    const PeerResolutionContext& context) {
    return PeerIdentity(
        kProvider, source, IdentityAssurance::CONFIGURED_PROXY, "spiffe://" + trust_domain, "http",
        PeerSubjectKind::WORKLOAD, id, SubjectStability::STABLE, true, std::move(attributes),
        nlohmann::json::object(), false, context.asserted_peer, context.immediate_peer);
}
}  // namespace

std::string validate_spiffe_id(const std::string& value,
                               const std::vector<std::string>& trust_domains) {
    std::set<std::string> domains;
    for (const auto& domain : trust_domains) {
        if (!valid_trust_domain(domain)) throw std::invalid_argument("invalid SPIFFE trust domain");
        domains.insert(domain);
    }
    return validate_id(value, domains);
}

PeerIdentityProvider spiffe_x509_header_provider(SpiffeX509HeaderOptions options) {
    if (options.certificate_header.empty()) options.certificate_header = "X-SSL-Client-Cert";
    if (options.verification_value.empty()) options.verification_value = "true";
    if (options.evidence_source.empty()) options.evidence_source = "verified_certificate_header";
    if (options.verification_header.empty())
        throw std::invalid_argument("SPIFFE verification header is required");
    return certificate_provider(std::move(options), true);
}

PeerIdentityProvider nginx_spiffe_provider(SpiffeProxyOptions options) {
    SpiffeX509HeaderOptions profile;
    static_cast<SpiffeProxyOptions&>(profile) = std::move(options);
    profile.certificate_header = "X-SSL-Client-Cert";
    profile.verification_header = "X-SSL-Client-Verify";
    profile.verification_value = "SUCCESS";
    profile.evidence_source = "nginx_mtls";
    return certificate_provider(std::move(profile), true);
}

PeerIdentityProvider aws_alb_spiffe_provider(SpiffeProxyOptions options) {
    SpiffeX509HeaderOptions profile;
    static_cast<SpiffeProxyOptions&>(profile) = std::move(options);
    profile.certificate_header = "X-Amzn-Mtls-Clientcert-Leaf";
    profile.evidence_source = "aws_alb_mtls_verify";
    return certificate_provider(std::move(profile), false);
}

PeerIdentityProvider azure_application_gateway_spiffe_provider(SpiffeProxyOptions options) {
    SpiffeX509HeaderOptions profile;
    static_cast<SpiffeProxyOptions&>(profile) = std::move(options);
    profile.certificate_header = "X-Client-Certificate";
    profile.verification_header = "X-Client-Certificate-Verification";
    profile.verification_value = "SUCCESS";
    profile.evidence_source = "azure_application_gateway_mtls_strict";
    return certificate_provider(std::move(profile), true);
}

PeerIdentityProvider gcp_load_balancer_spiffe_provider(GcpSpiffeOptions options) {
    if (options.spiffe_id_header.empty()) options.spiffe_id_header = "X-Client-Cert-Spiffe-Id";
    if (options.present_header.empty()) options.present_header = "X-Client-Cert-Present";
    if (options.chain_verified_header.empty())
        options.chain_verified_header = "X-Client-Cert-Chain-Verified";
    if (options.error_header.empty()) options.error_header = "X-Client-Cert-Error";
    auto boundary = make_boundary(options);
    std::set<std::string> headers;
    for (const auto* header : {&options.spiffe_id_header, &options.present_header,
                               &options.chain_verified_header, &options.error_header}) {
        if (!valid_header_name(*header) || !headers.insert(lower_ascii(*header)).second)
            throw std::invalid_argument("invalid or duplicate GCP mTLS header");
    }
    return [boundary = std::move(boundary),
            options = std::move(options)](const PeerResolutionContext& context) {
        if (!boundary.trusts(context.immediate_peer))
            return result(kProvider, PeerIdentityStatus::UNTRUSTED_PROXY);
        try {
            context.validate();
            const auto id = context.header(options.spiffe_id_header);
            const auto present = context.header(options.present_header);
            const auto verified = context.header(options.chain_verified_header);
            const auto failure = context.header(options.error_header);
            if (present && *present == "false" && (!verified || *verified == "false") && !id)
                return result(kProvider, PeerIdentityStatus::NO_MATCH);
            if (!present || *present != "true" || !verified || *verified != "true" ||
                (failure && !failure->empty()) || !id || id->empty())
                return result(kProvider, PeerIdentityStatus::INVALID);
            const auto domain = validate_id(*id, boundary.trust_domains);
            return PeerIdentityResult::available(spiffe_header_identity(
                *id, domain, "gcp_load_balancer_mtls",
                {{"client_certificate_present", true}, {"client_certificate_chain_verified", true}},
                context));
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

PeerIdentityProvider envoy_xfcc_spiffe_provider(EnvoyXfccSpiffeOptions options) {
    if (options.header.empty()) options.header = "X-Forwarded-Client-Cert";
    auto boundary = make_boundary(options);
    if (!valid_header_name(options.header))
        throw std::invalid_argument("invalid Envoy XFCC header");
    return [boundary = std::move(boundary),
            header = std::move(options.header)](const PeerResolutionContext& context) {
        if (!boundary.trusts(context.immediate_peer))
            return result(kProvider, PeerIdentityStatus::UNTRUSTED_PROXY);
        try {
            context.validate();
            const auto raw = context.header(header);
            if (!raw) return result(kProvider, PeerIdentityStatus::NO_MATCH);
            const auto fields = parse_xfcc(*raw, boundary.max_bytes);
            const auto uri = fields.find("uri"), hash = fields.find("hash");
            if (uri == fields.end() || uri->second.size() != 1 || hash == fields.end() ||
                hash->second.size() != 1 || hash->second.front().size() != 64 ||
                !std::all_of(hash->second.front().begin(), hash->second.front().end(),
                             [](unsigned char c) { return std::isxdigit(c); }))
                return result(kProvider, PeerIdentityStatus::INVALID);
            const auto domain = validate_id(uri->second.front(), boundary.trust_domains);
            nlohmann::json attributes = {{"certificate_sha256", lower_ascii(hash->second.front())}};
            const auto by = fields.find("by");
            if (by != fields.end()) attributes["proxy_identities"] = by->second;
            return PeerIdentityResult::available(
                spiffe_header_identity(uri->second.front(), domain, "envoy_xfcc_sanitize_set",
                                       std::move(attributes), context));
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

}  // namespace vgi_rpc
