// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Live-Tailnet qualification adapter for the C++ implementation. This is a
// conformance binary, not a production proxy, ingress, or alternate worker API.

#include <vgi_rpc/annotated_batch.h>
#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/call_context.h>
#include <vgi_rpc/client.h>
#include <vgi_rpc/crypto.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/identity.h>
#include <vgi_rpc/proxy_protocol_v2.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/tailscale_identity.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;
using namespace vgi_rpc;

constexpr std::string_view kProvider = "tailscale";

struct Expectation {
    std::string issuer;
    std::string transport;
    std::string evidence_source;
    IdentityAssurance assurance = IdentityAssurance::LOCAL_DAEMON;
    PeerSubjectKind subject_kind = PeerSubjectKind::UNKNOWN;
    SubjectStability subject_stability = SubjectStability::NONE;
    std::string capability;
    std::optional<std::string> capability_target_kind;
    std::optional<std::string> capability_target_value;
    std::optional<std::string> tag;
    bool authenticated = false;
    bool proxy_present = false;
    std::optional<std::string> spoofed_subject_fingerprint;
};

class Options {
public:
    Options(int argc, char** argv, int first) {
        static const std::set<std::string> flags = {"--expect-authenticated", "--expect-proxy"};
        for (int index = first; index < argc; ++index) {
            const std::string name = argv[index];
            if (!name.starts_with("--"))
                throw std::invalid_argument("unexpected positional argument");
            if (!seen_.insert(name).second)
                throw std::invalid_argument("duplicate option: " + name);
            if (flags.contains(name)) {
                flags_.insert(name);
                continue;
            }
            if (index + 1 >= argc || std::string_view(argv[index + 1]).starts_with("--")) {
                throw std::invalid_argument("option requires a value: " + name);
            }
            values_.emplace(name, argv[++index]);
        }
    }

    std::string require(const std::string& name) const {
        const auto value = values_.find(name);
        if (value == values_.end() || value->second.empty()) {
            throw std::invalid_argument("required option is missing: " + name);
        }
        return value->second;
    }

    std::optional<std::string> optional(const std::string& name) const {
        const auto value = values_.find(name);
        return value == values_.end() ? std::nullopt : std::optional<std::string>(value->second);
    }

    std::string value_or(const std::string& name, std::string fallback) const {
        const auto value = optional(name);
        return value ? *value : std::move(fallback);
    }

    bool flag(const std::string& name) const { return flags_.contains(name); }

    void allow(std::initializer_list<std::string_view> allowed) const {
        std::set<std::string> names;
        for (const auto name : allowed) names.emplace(name);
        for (const auto& name : seen_) {
            if (!names.contains(name)) throw std::invalid_argument("unsupported option: " + name);
        }
    }

private:
    std::map<std::string, std::string> values_;
    std::set<std::string> flags_;
    std::set<std::string> seen_;
};

uint16_t parse_port(const std::string& value) {
    unsigned int port = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), port);
    if (error != std::errc{} || end != value.data() + value.size() || port == 0 || port > 65535) {
        throw std::invalid_argument("port must be between 1 and 65535");
    }
    return static_cast<uint16_t>(port);
}

IdentityAssurance parse_assurance(const std::string& value) {
    if (value == "cryptographic_peer") return IdentityAssurance::CRYPTOGRAPHIC_PEER;
    if (value == "local_daemon") return IdentityAssurance::LOCAL_DAEMON;
    if (value == "configured_proxy") return IdentityAssurance::CONFIGURED_PROXY;
    throw std::invalid_argument("unknown identity assurance");
}

PeerSubjectKind parse_subject_kind(const std::string& value) {
    if (value == "user") return PeerSubjectKind::USER;
    if (value == "tagged_node") return PeerSubjectKind::TAGGED_NODE;
    if (value == "workload") return PeerSubjectKind::WORKLOAD;
    if (value == "endpoint") return PeerSubjectKind::ENDPOINT;
    if (value == "unknown") return PeerSubjectKind::UNKNOWN;
    throw std::invalid_argument("unknown subject kind");
}

SubjectStability parse_stability(const std::string& value) {
    if (value == "stable") return SubjectStability::STABLE;
    if (value == "login") return SubjectStability::LOGIN;
    if (value == "none") return SubjectStability::NONE;
    throw std::invalid_argument("unknown subject stability");
}

std::string_view assurance_name(IdentityAssurance value) {
    switch (value) {
        case IdentityAssurance::CRYPTOGRAPHIC_PEER: return "cryptographic_peer";
        case IdentityAssurance::LOCAL_DAEMON: return "local_daemon";
        case IdentityAssurance::CONFIGURED_PROXY: return "configured_proxy";
    }
    throw std::logic_error("unknown identity assurance");
}

std::string_view subject_kind_name(PeerSubjectKind value) {
    switch (value) {
        case PeerSubjectKind::USER: return "user";
        case PeerSubjectKind::TAGGED_NODE: return "tagged_node";
        case PeerSubjectKind::WORKLOAD: return "workload";
        case PeerSubjectKind::ENDPOINT: return "endpoint";
        case PeerSubjectKind::UNKNOWN: return "unknown";
    }
    throw std::logic_error("unknown subject kind");
}

std::string_view stability_name(SubjectStability value) {
    switch (value) {
        case SubjectStability::STABLE: return "stable";
        case SubjectStability::LOGIN: return "login";
        case SubjectStability::NONE: return "none";
    }
    throw std::logic_error("unknown subject stability");
}

bool is_sha256_hex(const std::string& value) {
    return value.size() == 64 && std::all_of(value.begin(), value.end(), [](unsigned char byte) {
               return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
           });
}

std::string sha256_hex(const std::string& value) {
    crypto::Sha256 digest;
    digest.update(value);
    return digest.hex_digest();
}

bool json_array_contains(const nlohmann::json& value, const std::string& wanted) {
    return value.is_array() && std::any_of(value.begin(), value.end(), [&](const auto& item) {
               return item.is_string() && item.template get<std::string>() == wanted;
           });
}

bool capability_target_matches(const nlohmann::json& attributes, const Expectation& expected,
                               bool redact_destination_value) {
    if (!expected.capability_target_kind) {
        const auto target = attributes.find("capability_target");
        return target == attributes.end() || target->is_null();
    }
    const auto target = attributes.find("capability_target");
    if (target == attributes.end() || !target->is_object()) {
        return false;
    }
    const auto kind = target->find("kind");
    if (kind == target->end() || !kind->is_string() ||
        kind->get<std::string>() != *expected.capability_target_kind)
        return false;
    if (expected.capability_target_value) {
        return target->value("value", "") == *expected.capability_target_value;
    }
    if (*expected.capability_target_kind == "destination_ip") {
        // Public snapshots redact the IP value; server-side evidence retains it.
        return redact_destination_value
                   ? !target->contains("value")
                   : target->contains("value") && (*target)["value"].is_string() &&
                         !(*target)["value"].get<std::string>().empty();
    }
    return true;
}

void validate_evidence_and_auth(const PeerEvidenceSet& evidence, const AuthContext& auth,
                                const Expectation& expected) {
    if (evidence.status(std::string(kProvider)) != PeerIdentityStatus::AVAILABLE) {
        throw std::runtime_error("Tailscale evidence was not available");
    }
    const auto identities = evidence.for_provider(std::string(kProvider));
    if (identities.size() != 1) {
        throw std::runtime_error("expected exactly one Tailscale identity");
    }
    const PeerIdentity& identity = identities.front().get();
    const bool expected_subject = expected.subject_stability != SubjectStability::NONE;
    const auto tags = identity.attributes().find("tags");
    const bool tag_matches = !expected.tag || (tags != identity.attributes().end() &&
                                               json_array_contains(*tags, *expected.tag));
    const auto capability = identity.capabilities().find(expected.capability);
    if (identity.issuer() != expected.issuer || identity.transport() != expected.transport ||
        identity.evidence_source() != expected.evidence_source ||
        identity.assurance() != expected.assurance ||
        identity.subject_kind() != expected.subject_kind ||
        identity.subject_stability() != expected.subject_stability ||
        identity.subject_verified() != expected_subject ||
        identity.subject_key().has_value() != expected_subject ||
        !identity.capabilities_verified() || capability == identity.capabilities().end() ||
        !tag_matches || !capability_target_matches(identity.attributes(), expected, false) ||
        identity.proxy_address().has_value() != expected.proxy_present) {
        throw std::runtime_error("unexpected Tailscale identity context");
    }

    if (auth.authenticated != expected.authenticated ||
        auth.domain != (expected.authenticated ? kProvider : "")) {
        throw std::runtime_error("unexpected Tailscale authentication context");
    }
    if (expected.authenticated) {
        if (!auth.principal || *auth.principal != identity.canonical_principal()) {
            throw std::runtime_error("authentication principal did not match peer identity");
        }
        const std::map<std::string, std::string> expected_claims = {
            {"issuer", expected.issuer},
            {"subject_kind", std::string(subject_kind_name(expected.subject_kind))},
            {"assurance", std::string(assurance_name(expected.assurance))},
            {"evidence_source", expected.evidence_source},
            {"subject", *identity.subject_key()},
        };
        if (auth.claims.size() != expected_claims.size() + 1) {
            throw std::runtime_error("primary authentication contained unexpected claims");
        }
        for (const auto& [name, value] : expected_claims) {
            if (!auth.claims.contains(name) || !auth.claims[name].is_string() ||
                auth.claims[name].get<std::string>() != value) {
                throw std::runtime_error("primary authentication claims did not match evidence");
            }
        }
    } else if (auth.principal) {
        throw std::runtime_error("anonymous evidence produced an authentication principal");
    }
    const auto binding = auth.claims.find("peer_evidence_binding");
    const auto expected_binding = evidence.binding_digest({std::string(kProvider)});
    if (binding == auth.claims.end() || !binding->is_string() ||
        binding->get<std::string>() != expected_binding || !is_sha256_hex(expected_binding)) {
        throw std::runtime_error("peer-evidence binding was absent or incorrect");
    }
}

Expectation client_expectation(const Options& options, std::string transport) {
    Expectation expected;
    expected.issuer = options.require("--expected-issuer");
    expected.transport = std::move(transport);
    expected.evidence_source = options.require("--expected-evidence-source");
    expected.assurance = parse_assurance(options.require("--expected-assurance"));
    expected.subject_kind = parse_subject_kind(options.require("--expected-subject-kind"));
    expected.subject_stability = parse_stability(options.require("--expected-subject-stability"));
    expected.capability = options.require("--expected-capability");
    expected.capability_target_kind = options.optional("--expected-target-kind");
    expected.capability_target_value = options.optional("--expected-target-value");
    expected.tag = options.optional("--expected-tag");
    expected.authenticated = options.flag("--expect-authenticated");
    expected.proxy_present = options.flag("--expect-proxy");
    if (const auto login = options.optional("--spoof-login")) {
        expected.spoofed_subject_fingerprint = sha256_hex("login:" + *login);
    }
    return expected;
}

nlohmann::json validate_snapshot(const std::string& raw, const Expectation& expected) {
    const auto snapshot = nlohmann::json::parse(raw);
    if (!snapshot.is_object() || snapshot.value("provider_status", nlohmann::json::object()) !=
                                     nlohmann::json{{kProvider, "available"}}) {
        throw std::runtime_error("snapshot did not contain available Tailscale evidence");
    }
    const auto identities = snapshot.find("identities");
    if (identities == snapshot.end() || !identities->is_array() || identities->size() != 1) {
        throw std::runtime_error("snapshot did not contain exactly one identity");
    }
    const auto& identity = identities->front();
    const auto& auth = snapshot.at("auth");
    const bool expected_subject = expected.subject_stability != SubjectStability::NONE;
    const auto fingerprint = identity.find("subject_fingerprint");
    const bool fingerprint_present = fingerprint != identity.end() && fingerprint->is_string();
    const auto principal_fingerprint = auth.find("principal_fingerprint");
    const bool principal_present =
        principal_fingerprint != auth.end() && principal_fingerprint->is_string();
    const auto capability_names = identity.find("capability_names");
    const auto tags = identity.find("tags");
    const bool tag_matches =
        !expected.tag || (tags != identity.end() && json_array_contains(*tags, *expected.tag));
    const bool spoof_resistant =
        !expected.spoofed_subject_fingerprint || !fingerprint_present ||
        fingerprint->get<std::string>() != *expected.spoofed_subject_fingerprint;
    const bool expected_domain = expected.authenticated;
    const bool domain_matches = expected_domain
                                    ? auth.value("domain", "") == kProvider
                                    : auth.contains("domain") && auth["domain"].is_null();

    if (identity.value("provider", "") != kProvider ||
        identity.value("issuer", "") != expected.issuer ||
        identity.value("transport", "") != expected.transport ||
        identity.value("evidence_source", "") != expected.evidence_source ||
        identity.value("assurance", "") != assurance_name(expected.assurance) ||
        identity.value("subject_kind", "") != subject_kind_name(expected.subject_kind) ||
        identity.value("subject_stability", "") != stability_name(expected.subject_stability)) {
        throw std::runtime_error("snapshot identity shape did not match expectations");
    }
    if (identity.value("subject_verified", false) != expected_subject ||
        fingerprint_present != expected_subject ||
        (fingerprint_present && !is_sha256_hex(fingerprint->get<std::string>()))) {
        throw std::runtime_error("snapshot subject shape did not match expectations");
    }
    if (!identity.value("capabilities_verified", false)) {
        throw std::runtime_error("snapshot capabilities were not verified");
    }
    if (capability_names == identity.end() ||
        !json_array_contains(*capability_names, expected.capability)) {
        throw std::runtime_error("snapshot required capability was absent");
    }
    if (!tag_matches) throw std::runtime_error("snapshot required tag was absent");
    if (!capability_target_matches(identity, expected, true)) {
        throw std::runtime_error("snapshot capability target did not match expectations");
    }
    if (identity.value("proxy_present", false) != expected.proxy_present) {
        throw std::runtime_error("snapshot proxy evidence did not match expectations");
    }
    if (auth.value("authenticated", false) != expected.authenticated || !domain_matches ||
        auth.value("principal_matches_identity", false) != expected.authenticated ||
        !auth.value("peer_evidence_binding_present", false) ||
        principal_present != expected.authenticated ||
        (principal_present && !is_sha256_hex(principal_fingerprint->get<std::string>()))) {
        throw std::runtime_error("snapshot authentication shape did not match expectations");
    }
    if (!spoof_resistant) {
        throw std::runtime_error("snapshot trusted a client-supplied Serve identity header");
    }
    return snapshot;
}

std::shared_ptr<arrow::RecordBatch> empty_batch() {
    return arrow::RecordBatch::Make(arrow::schema({}), 0,
                                    std::vector<std::shared_ptr<arrow::Array>>{});
}

std::string snapshot_result(const AnnotatedBatch& result) {
    if (!result.batch || result.batch->num_rows() != 1 || result.batch->num_columns() != 1) {
        throw std::runtime_error("snapshot RPC returned an invalid Arrow shape");
    }
    const auto values = std::dynamic_pointer_cast<arrow::StringArray>(result.batch->column(0));
    if (!values || values->IsNull(0)) {
        throw std::runtime_error("snapshot RPC result was not a non-null string");
    }
    return values->GetString(0);
}

void assert_stable_snapshots(const std::string& first_raw, const std::string& second_raw,
                             const Expectation& expected) {
    const auto first = validate_snapshot(first_raw, expected);
    const auto second = validate_snapshot(second_raw, expected);
    if (first != second) throw std::runtime_error("identity evidence changed between probe calls");
}

void run_tcp_client(int argc, char** argv) {
    const Options options(argc, argv, 2);
    options.allow({"--host", "--port", "--proxy", "--expected-issuer", "--expected-evidence-source",
                   "--expected-assurance", "--expected-subject-kind",
                   "--expected-subject-stability", "--expected-capability", "--expected-tag",
                   "--expected-target-kind", "--expected-target-value", "--expect-authenticated",
                   "--expect-proxy"});
    const auto host = options.require("--host");
    const auto port = parse_port(options.require("--port"));
    SocketTransportOptions transport;
    transport.connect_timeout = 20s;
    transport.read_timeout = 20s;
    transport.write_timeout = 20s;
    transport.proxy = options.optional("--proxy");
    RpcClientOptions client_options;
    client_options.protocol_version = "2.0.0";
    auto client = RpcClient::connect_tcp(host, port, client_options, transport);
    const auto expected = client_expectation(options, "tcp");
    const auto first = snapshot_result(client.call_unary("snapshot", empty_batch()));
    const auto second = snapshot_result(client.call_unary("snapshot", empty_batch()));
    assert_stable_snapshots(first, second, expected);
    client.close();
    std::cout << "C++ TCP client Tailnet probe passed\n";
}

void run_http_client(int argc, char** argv) {
    const Options options(argc, argv, 2);
    options.allow({"--url", "--spoof-login", "--expected-issuer", "--expected-evidence-source",
                   "--expected-assurance", "--expected-subject-kind",
                   "--expected-subject-stability", "--expected-capability", "--expected-tag",
                   "--expected-target-kind", "--expected-target-value", "--expect-authenticated",
                   "--expect-proxy"});
    const auto spoof_login = options.require("--spoof-login");
    auto builder = HttpClient::builder(options.require("--url"));
    builder.protocol_version("2.0.0").header("Tailscale-User-Login", spoof_login);
    auto client = builder.build();
    const auto expected = client_expectation(options, "http");
    const auto request = AnnotatedBatch::data(empty_batch());
    const auto expected_schema = arrow::schema({arrow::field("result", arrow::utf8(), false)});
    const auto first = snapshot_result(
        client.call("snapshot", request, expected_schema, CallOptions::with_timeout(20s)));
    const auto second = snapshot_result(
        client.call("snapshot", request, expected_schema, CallOptions::with_timeout(20s)));
    assert_stable_snapshots(first, second, expected);
    std::cout << "C++ HTTP client Tailnet probe passed\n";
}

std::unique_ptr<Server> build_probe_server(Expectation expected) {
    const auto params = arrow::schema({arrow::field("value", arrow::utf8(), false)});
    const auto result = arrow::schema({arrow::field("result", arrow::utf8(), false)});
    return ServerBuilder()
        .add_unary(
            "echo_string", params, result,
            [expected = std::move(expected), result](const Request& request, CallContext& context) {
                validate_evidence_and_auth(context.peer_evidence(), context.auth(), expected);
                arrow::StringBuilder builder;
                VGI_RPC_THROW_NOT_OK(builder.Append(request.get<std::string>("value")));
                return Result::value(result, {unwrap(builder.Finish())});
            })
        .enable_describe("ConformanceService")
        .protocol_version("2.0.0")
        .build();
}

void run_tcp_server(int argc, char** argv) {
    const Options options(argc, argv, 2);
    options.allow({"--host", "--port", "--issuer", "--localapi-socket", "--expected-capability",
                   "--expected-tag"});
    const auto issuer = options.require("--issuer");
    Expectation expected{
        .issuer = issuer,
        .transport = "tcp",
        .evidence_source = "localapi",
        .assurance = IdentityAssurance::LOCAL_DAEMON,
        .subject_kind = PeerSubjectKind::TAGGED_NODE,
        .subject_stability = SubjectStability::STABLE,
        .capability = options.require("--expected-capability"),
        .capability_target_kind = "destination_ip",
        .capability_target_value = std::nullopt,
        .tag = options.require("--expected-tag"),
        .authenticated = true,
        .proxy_present = false,
        .spoofed_subject_fingerprint = std::nullopt,
    };
    TailscaleLocalAPIOptions localapi;
    localapi.issuer = issuer;
    localapi.unix_socket =
        options.value_or("--localapi-socket", "/var/run/tailscale/tailscaled.sock");
    auto provider = tailscale_localapi_identity_provider(std::move(localapi));
    auto policy = peer_identity_primary(std::string(kProvider));
    TcpServerOptions server_options;
    server_options.resolve_identity = [provider = std::move(provider), policy = std::move(policy)](
                                          const PeerResolutionContext& context) mutable {
        PeerEvidenceSet evidence({provider(context)});
        auto auth = policy(evidence, AuthContext::anonymous());
        return TcpServerOptions::ResolvedIdentity{std::move(auth), std::move(evidence)};
    };
    auto server = build_probe_server(std::move(expected));
    server->serve_tcp(options.value_or("--host", "0.0.0.0"),
                      parse_port(options.value_or("--port", "19400")), server_options);
}

nlohmann::json tcp_snapshot_fixture() {
    return {
        {"provider_status", {{kProvider, "available"}}},
        {"identities",
         {{{"provider", kProvider},
           {"issuer", "tailnet:test"},
           {"evidence_source", "localapi"},
           {"assurance", "local_daemon"},
           {"transport", "tcp"},
           {"subject_kind", "tagged_node"},
           {"subject_stability", "stable"},
           {"subject_verified", true},
           {"subject_fingerprint", std::string(64, 'a')},
           {"tags", {"tag:vgi-client"}},
           {"capability_names", {"query.farm/cap"}},
           {"capabilities_verified", true},
           {"capability_target", {{"kind", "destination_ip"}}},
           {"proxy_present", false}}}},
        {"auth",
         {{"authenticated", true},
          {"domain", kProvider},
          {"principal_fingerprint", std::string(64, 'b')},
          {"principal_matches_identity", true},
          {"peer_evidence_binding_present", true}}},
    };
}

Expectation tcp_expectation() {
    return {
        .issuer = "tailnet:test",
        .transport = "tcp",
        .evidence_source = "localapi",
        .assurance = IdentityAssurance::LOCAL_DAEMON,
        .subject_kind = PeerSubjectKind::TAGGED_NODE,
        .subject_stability = SubjectStability::STABLE,
        .capability = "query.farm/cap",
        .capability_target_kind = "destination_ip",
        .capability_target_value = std::nullopt,
        .tag = "tag:vgi-client",
        .authenticated = true,
        .proxy_present = false,
        .spoofed_subject_fingerprint = std::nullopt,
    };
}

void require_rejected(nlohmann::json snapshot, const Expectation& expected,
                      const nlohmann::json::json_pointer& pointer, nlohmann::json replacement) {
    snapshot[pointer] = std::move(replacement);
    try {
        (void)validate_snapshot(snapshot.dump(), expected);
    } catch (const std::exception&) {
        return;
    }
    throw std::runtime_error("invalid snapshot fixture was accepted");
}

void run_self_test() {
    const auto expected = tcp_expectation();
    const auto snapshot = tcp_snapshot_fixture();
    const auto& fixture_target = snapshot["identities"][0]["capability_target"];
    if (!fixture_target.is_object()) throw std::runtime_error("invalid self-test target fixture");
    if (!fixture_target.contains("kind") || !fixture_target["kind"].is_string())
        throw std::runtime_error("invalid self-test target kind fixture");
    if (fixture_target.contains("value"))
        throw std::runtime_error("self-test snapshot leaked a destination value");
    (void)validate_snapshot(snapshot.dump(), expected);
    require_rejected(snapshot, expected, nlohmann::json::json_pointer("/identities/0/issuer"),
                     "tailnet:other");
    require_rejected(snapshot, expected, nlohmann::json::json_pointer("/auth/domain"), "bearer");
    require_rejected(snapshot, expected,
                     nlohmann::json::json_pointer("/auth/principal_matches_identity"), false);
    require_rejected(snapshot, expected,
                     nlohmann::json::json_pointer("/auth/peer_evidence_binding_present"), false);
    require_rejected(snapshot, expected,
                     nlohmann::json::json_pointer("/identities/0/capability_target/kind"), "node");

    const std::string spoof_login = "attacker@example.invalid";
    Expectation serve{
        .issuer = "tailnet:test",
        .transport = "http",
        .evidence_source = "serve_proxy",
        .assurance = IdentityAssurance::CONFIGURED_PROXY,
        .subject_kind = PeerSubjectKind::UNKNOWN,
        .subject_stability = SubjectStability::NONE,
        .capability = "query.farm/cap",
        .capability_target_kind = std::nullopt,
        .capability_target_value = std::nullopt,
        .tag = std::nullopt,
        .authenticated = false,
        .proxy_present = true,
        .spoofed_subject_fingerprint = sha256_hex("login:" + spoof_login),
    };
    nlohmann::json serve_snapshot = {
        {"provider_status", {{kProvider, "available"}}},
        {"identities",
         {{{"provider", kProvider},
           {"issuer", "tailnet:test"},
           {"evidence_source", "serve_proxy"},
           {"assurance", "configured_proxy"},
           {"transport", "http"},
           {"subject_kind", "unknown"},
           {"subject_stability", "none"},
           {"subject_verified", false},
           {"subject_fingerprint", nullptr},
           {"tags", nlohmann::json::array()},
           {"capability_names", {"query.farm/cap"}},
           {"capabilities_verified", true},
           {"capability_target", nullptr},
           {"proxy_present", true}}}},
        {"auth",
         {{"authenticated", false},
          {"domain", nullptr},
          {"principal_fingerprint", nullptr},
          {"principal_matches_identity", false},
          {"peer_evidence_binding_present", true}}},
    };
    (void)validate_snapshot(serve_snapshot.dump(), serve);
    serve_snapshot["identities"][0]["subject_kind"] = "user";
    serve_snapshot["identities"][0]["subject_stability"] = "login";
    serve_snapshot["identities"][0]["subject_verified"] = true;
    serve_snapshot["identities"][0]["subject_fingerprint"] = sha256_hex("login:" + spoof_login);
    try {
        (void)validate_snapshot(serve_snapshot.dump(), serve);
        throw std::runtime_error("spoofed Serve login was accepted as peer identity");
    } catch (const std::runtime_error& error) {
        if (std::string_view(error.what()) == "spoofed Serve login was accepted as peer identity") {
            throw;
        }
    }

    PeerIdentity identity(
        std::string(kProvider), "localapi", IdentityAssurance::LOCAL_DAEMON, "tailnet:test", "tcp",
        PeerSubjectKind::TAGGED_NODE, "node:stable-id", SubjectStability::STABLE, true,
        {{"tags", {"tag:vgi-client"}},
         {"capability_target", {{"kind", "destination_ip"}, {"value", "100.64.0.9"}}}},
        {{"query.farm/cap", nlohmann::json::array()}}, true);
    PeerEvidenceSet evidence({PeerIdentityResult::available(std::move(identity))});
    const auto auth =
        peer_identity_primary(std::string(kProvider))(evidence, AuthContext::anonymous());
    validate_evidence_and_auth(evidence, auth, expected);
    auto wrong_auth = auth;
    wrong_auth.domain = "bearer";
    try {
        validate_evidence_and_auth(evidence, wrong_auth, expected);
        throw std::runtime_error("unbound authentication context was accepted");
    } catch (const std::runtime_error& error) {
        if (std::string_view(error.what()) == "unbound authentication context was accepted") {
            throw;
        }
    }
    std::cout << "C++ Tailnet adapter self-test passed\n";
}

void run(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--self-test") {
        run_self_test();
        return;
    }
    if (argc < 2) {
        throw std::invalid_argument(
            "usage: vgi-rpc-tailnet-cpp client-tcp|client-http|server-tcp [options]");
    }
    const std::string_view mode = argv[1];
    if (mode == "client-tcp") return run_tcp_client(argc, argv);
    if (mode == "client-http") return run_http_client(argc, argv);
    if (mode == "server-tcp") return run_tcp_server(argc, argv);
    if (mode == "server-http") {
        throw std::invalid_argument(
            "server-http is unsupported: C++ HTTP serving does not expose peer identity hooks");
    }
    throw std::invalid_argument("unknown adapter mode");
}

}  // namespace

int main(int argc, char** argv) {
    try {
        run(argc, argv);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "vgi-rpc-tailnet-cpp: " << error.what() << '\n';
        return 1;
    }
}
