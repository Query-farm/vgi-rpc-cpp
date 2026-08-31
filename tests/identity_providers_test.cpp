// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <array>
#include <cctype>
#include <chrono>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <httplib.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#ifndef _WIN32
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif

#include "vgi_rpc/spiffe_proxy_identity.h"
#include "vgi_rpc/tailscale_identity.h"

using namespace std::chrono_literals;
using namespace vgi_rpc;

namespace {
PeerResolutionContext resolution(std::map<std::string, std::vector<std::string>> headers = {}) {
    PeerResolutionContext context;
    context.transport = "http";
    context.immediate_peer = "127.0.0.1";
    context.asserted_peer = "100.64.0.10:4242";
    context.headers = std::move(headers);
    return context;
}

SpiffeProxyOptions spiffe_options() {
    return {{"example.org"}, {"127.0.0.1"}, 16'384};
}

PeerIdentityStatus vector_status(const std::string& value) {
    if (value == "available") return PeerIdentityStatus::AVAILABLE;
    if (value == "invalid") return PeerIdentityStatus::INVALID;
    if (value == "no_match") return PeerIdentityStatus::NO_MATCH;
    if (value == "not_applicable") return PeerIdentityStatus::NOT_APPLICABLE;
    throw std::invalid_argument("unknown identity vector status");
}

std::map<std::string, std::vector<std::string>> vector_headers(const nlohmann::json& value) {
    std::map<std::string, std::vector<std::string>> headers;
    for (const auto& [name, raw] : value.items()) headers[name] = {raw.get<std::string>()};
    return headers;
}

std::string percent_encode(const std::string& value) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~') {
            out.push_back(char(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 15]);
        }
    }
    return out;
}

std::string test_svid(const std::string& uri = "spiffe://example.org/workload") {
    using Key = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
    using KeyContext = std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)>;
    using Certificate = std::unique_ptr<X509, decltype(&X509_free)>;
    using Bio = std::unique_ptr<BIO, decltype(&BIO_free)>;
    KeyContext key_context(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
    REQUIRE(key_context);
    REQUIRE(EVP_PKEY_keygen_init(key_context.get()) == 1);
    REQUIRE(EVP_PKEY_CTX_set_rsa_keygen_bits(key_context.get(), 2048) == 1);
    EVP_PKEY* raw_key = nullptr;
    REQUIRE(EVP_PKEY_keygen(key_context.get(), &raw_key) == 1);
    Key key(raw_key, EVP_PKEY_free);
    Certificate certificate(X509_new(), X509_free);
    REQUIRE(certificate);
    REQUIRE(X509_set_version(certificate.get(), 2) == 1);
    REQUIRE(ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1) == 1);
    REQUIRE(X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60));
    REQUIRE(X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600));
    REQUIRE(X509_set_pubkey(certificate.get(), key.get()) == 1);
    X509_NAME* subject = X509_get_subject_name(certificate.get());
    REQUIRE(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                       reinterpret_cast<const unsigned char*>("test"), -1, -1,
                                       0) == 1);
    REQUIRE(X509_set_issuer_name(certificate.get(), subject) == 1);
    auto add_extension = [&](int nid, const std::string& value) {
        X509_EXTENSION* raw =
            X509V3_EXT_conf_nid(nullptr, nullptr, nid, const_cast<char*>(value.c_str()));
        REQUIRE(raw);
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
            raw, X509_EXTENSION_free);
        REQUIRE(X509_add_ext(certificate.get(), extension.get(), -1) == 1);
    };
    add_extension(NID_basic_constraints, "critical,CA:FALSE");
    add_extension(NID_key_usage, "critical,digitalSignature");
    add_extension(NID_ext_key_usage, "clientAuth,serverAuth");
    add_extension(NID_subject_alt_name, "URI:" + uri);
    REQUIRE(X509_sign(certificate.get(), key.get(), EVP_sha256()) > 0);
    Bio bio(BIO_new(BIO_s_mem()), BIO_free);
    REQUIRE(bio);
    REQUIRE(PEM_write_bio_X509(bio.get(), certificate.get()) == 1);
    BUF_MEM* memory = nullptr;
    BIO_get_mem_ptr(bio.get(), &memory);
    REQUIRE(memory);
    return percent_encode(std::string(memory->data, memory->length));
}

class TestHttpServer {
public:
    explicit TestHttpServer(httplib::Server::Handler handler) {
        server_.Get("/localapi/v0/whois", std::move(handler));
        port_ = server_.bind_to_any_port("127.0.0.1");
        REQUIRE(port_ > 0);
        thread_ = std::jthread([this] { server_.listen_after_bind(); });
    }
    ~TestHttpServer() { server_.stop(); }
    std::string endpoint() const { return "http://127.0.0.1:" + std::to_string(port_); }

private:
    httplib::Server server_;
    int port_ = 0;
    std::jthread thread_;
};

std::string whois_json(bool tagged = false) {
    nlohmann::json payload = {
        {"Node",
         {{"StableID", "n123CNTRL"},
          {"Name", "client.example.ts.net."},
          {"Tags", tagged ? nlohmann::json{"tag:batch-worker"} : nlohmann::json::array()}}},
        {"UserProfile",
         {{"ID", 123}, {"LoginName", "alice@example.com"}, {"DisplayName", "Alice"}}},
        {"CapMap", {{"example.com/cap/run", {{{"queue", "blue"}}}}}}};
    return payload.dump();
}
}  // namespace

TEST_CASE("Tailscale Serve fails closed and preserves login stability") {
    auto provider = tailscale_serve_identity_provider({"tailnet:example", {"127.0.0.1"}, 16'384});
    auto context = resolution({{"Tailscale-User-Login", {"alice@example.com"}},
                               {"Tailscale-User-Name", {"=?utf-8?q?Ferris_B=C3=BCller?="}}});
    auto available = provider(context);
    REQUIRE(available.status == PeerIdentityStatus::AVAILABLE);
    const auto& identity = available.identities.front();
    REQUIRE(identity.assurance() == IdentityAssurance::CONFIGURED_PROXY);
    REQUIRE(identity.subject_key() == std::optional<std::string>("login:alice@example.com"));
    REQUIRE(identity.subject_stability() == SubjectStability::LOGIN);
    REQUIRE(identity.attributes()["user_display_name"] == "Ferris Büller");

    context.headers = {{"Tailscale-User-Login", {"alice@example.com", "admin@example.com"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    context.headers = {{"Tailscale-User-Login", {"alice@example.com"}},
                       {"tailscale-user-login", {"admin@example.com"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    context.headers = {
        {"Tailscale-App-Capabilities", {R"({"example.com/cap/run":[],"example.com/cap/run":[]})"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    context.headers = {{"Tailscale-User-Login", {"=?utf-8?b?YWxpY2U=?="}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    context.headers = {{"Tailscale-User-Login", {"alice\x1f@example.com"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    context.immediate_peer = "127.0.0.2";
    context.headers = {{"Tailscale-User-Login", {"admin@example.com"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::UNTRUSTED_PROXY);
}

TEST_CASE("Tailscale Serve accepts capability-only evidence and rejects Funnel") {
    auto provider = tailscale_serve_identity_provider({"tailnet:example", {"127.0.0.1"}, 16'384});
    auto context = resolution({{"Tailscale-App-Capabilities",
                                {R"({"example.com/cap/monitoring":[{"role":"reader"}]})"}}});
    const auto capability = provider(context);
    REQUIRE(capability.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE_FALSE(capability.identities.front().subject_key());
    REQUIRE(capability.identities.front().capabilities_verified());
    context.headers = {{"Tailscale-Funnel-Request", {"?1"}},
                       {"Tailscale-User-Login", {"alice@example.com"}}};
    REQUIRE(provider(context).status == PeerIdentityStatus::NOT_APPLICABLE);
}

TEST_CASE("SPIFFE proxy profiles require exact trust and strict evidence") {
    const auto certificate = test_svid();
    auto nginx = nginx_spiffe_provider(spiffe_options());
    auto context =
        resolution({{"X-SSL-Client-Cert", {certificate}}, {"X-SSL-Client-Verify", {"SUCCESS"}}});
    const auto available = nginx(context);
    REQUIRE(available.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(available.identities.front().subject_key() ==
            std::optional<std::string>("spiffe://example.org/workload"));
    REQUIRE(available.identities.front().assurance() == IdentityAssurance::CONFIGURED_PROXY);
    context.headers.erase("X-SSL-Client-Verify");
    REQUIRE(nginx(context).status == PeerIdentityStatus::INVALID);

    auto azure = azure_application_gateway_spiffe_provider(spiffe_options());
    context.headers = {{"X-Client-Certificate", {certificate}},
                       {"X-Client-Certificate-Verification", {"SUCCESS"}}};
    REQUIRE(azure(context).status == PeerIdentityStatus::AVAILABLE);
    context.headers.erase("X-Client-Certificate-Verification");
    REQUIRE(azure(context).status == PeerIdentityStatus::INVALID);

    auto aws = aws_alb_spiffe_provider(spiffe_options());
    context.headers = {{"X-Amzn-Mtls-Clientcert-Leaf", {certificate}}};
    REQUIRE(aws(context).status == PeerIdentityStatus::AVAILABLE);
    context.headers = {{"X-Amzn-Mtls-Clientcert-Leaf", {test_svid("spiffe://other.org/workload")}}};
    REQUIRE(aws(context).status == PeerIdentityStatus::INVALID);
    context.headers = {{"X-Amzn-Mtls-Clientcert-Leaf", {"%ZZ"}}};
    REQUIRE(aws(context).status == PeerIdentityStatus::INVALID);
    context.immediate_peer = "localhost";
    REQUIRE(aws(context).status == PeerIdentityStatus::UNTRUSTED_PROXY);
}

TEST_CASE("GCP and Envoy SPIFFE evidence reject missing and ambiguous signals") {
    auto gcp = gcp_load_balancer_spiffe_provider(GcpSpiffeOptions{spiffe_options()});
    auto context = resolution({{"X-Client-Cert-Present", {"true"}},
                               {"X-Client-Cert-Chain-Verified", {"true"}},
                               {"X-Client-Cert-Spiffe-Id", {"spiffe://example.org/client"}}});
    REQUIRE(gcp(context).status == PeerIdentityStatus::AVAILABLE);
    context.headers["X-Client-Cert-Chain-Verified"] = {"false"};
    REQUIRE(gcp(context).status == PeerIdentityStatus::INVALID);

    auto envoy = envoy_xfcc_spiffe_provider(EnvoyXfccSpiffeOptions{spiffe_options()});
    const std::string valid = "By=spiffe://mesh.example/proxy;Hash=" + std::string(64, 'a') +
                              ";URI=\"spiffe://example.org/client\"";
    context = resolution({{"X-Forwarded-Client-Cert", {valid}}});
    REQUIRE(envoy(context).status == PeerIdentityStatus::AVAILABLE);
    for (const auto& invalid :
         {valid + ",Hash=" + std::string(64, 'b') + ";URI=spiffe://example.org/other",
          std::string("URI=spiffe://example.org/client"),
          std::string("Hash=") + std::string(64, 'a') +
              ";URI=spiffe://example.org/one;URI=spiffe://example.org/two",
          std::string("Unknown=x;Hash=") + std::string(64, 'a') +
              ";URI=spiffe://example.org/client"}) {
        context.headers = {{"X-Forwarded-Client-Cert", {invalid}}};
        REQUIRE(envoy(context).status == PeerIdentityStatus::INVALID);
    }
}

TEST_CASE("Tailscale LocalAPI is deadline-bound, uncached, and scopes capabilities") {
    std::atomic<int> requests{0};
    std::atomic<bool> request_correct{true};
    TestHttpServer server([&](const httplib::Request& request, httplib::Response& response) {
        ++requests;
        request_correct = request.get_header_value("Host") == "local-tailscaled.sock" &&
                          request.get_header_value("Authorization") == "Basic OnNlY3JldA==" &&
                          request.get_param_value("addr") == "100.64.0.10:4242" &&
                          request.get_param_value("proto") == "tcp" &&
                          request.get_param_value("svc_name") == "svc:analytics";
        response.set_content(whois_json(false), "application/json");
    });
    auto provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), "secret", 2s, 65'536, 32'768});
    auto context = resolution();
    context.asserted_peer.reset();
    context.source_endpoint = "100.64.0.10:4242";
    context.service_name = "svc:analytics";
    context.destination_address = "192.0.2.10:9400";
    const auto first = provider(context);
    const auto second = provider(context);
    REQUIRE(first.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(second.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(requests == 2);
    REQUIRE(request_correct);
    const auto& identity = first.identities.front();
    REQUIRE(identity.assurance() == IdentityAssurance::LOCAL_DAEMON);
    REQUIRE(identity.subject_key() == std::optional<std::string>("user:123"));
    REQUIRE(identity.source_address() == std::optional<std::string>("100.64.0.10"));
    REQUIRE(identity.attributes()["capability_target"]["kind"] == "service");
}

TEST_CASE("Tailscale LocalAPI separates status and malformed-response outcomes") {
    std::atomic<int> mode{0};
    TestHttpServer server([&](const httplib::Request&, httplib::Response& response) {
        if (mode == 0) {
            response.status = 403;
            response.set_content("{}", "application/json");
        } else if (mode == 1) {
            response.set_content(R"({"Node":{},"Node":{},"UserProfile":{"ID":1}})",
                                 "application/json");
        } else {
            std::this_thread::sleep_for(150ms);
            response.set_content(whois_json(), "application/json");
        }
    });
    auto context = resolution();
    auto provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), {}, 1s, 65'536, 32'768});
    REQUIRE(provider(context).status == PeerIdentityStatus::PERMISSION_DENIED);
    mode = 1;
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    mode = 2;
    provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), {}, 20ms, 65'536, 32'768});
    REQUIRE(provider(context).status == PeerIdentityStatus::UNAVAILABLE);
}

TEST_CASE("Tailscale LocalAPI bounds response headers and body framing") {
    std::atomic<int> mode{0};
    TestHttpServer server([&](const httplib::Request&, httplib::Response& response) {
        if (mode == 0) {
            response.set_header("X-Oversized", std::string(2'048, 'x'));
            response.set_content(whois_json(), "application/json");
        } else if (mode == 1) {
            response.set_content(std::string(128, 'x'), "application/json");
        } else {
            response.headers.emplace("Content-Type", "application/json");
            response.headers.emplace("Content-Type", "application/json");
            response.body = whois_json();
        }
    });
    auto context = resolution();
    auto provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), {}, 1s, 65'536, 512});
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    mode = 1;
    provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), {}, 1s, 64, 32'768});
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
    mode = 2;
    provider = tailscale_localapi_identity_provider(
        {"tailnet:example", {}, server.endpoint(), {}, 1s, 65'536, 32'768});
    REQUIRE(provider(context).status == PeerIdentityStatus::INVALID);
}

#ifndef _WIN32
TEST_CASE("Tailscale LocalAPI supports direct Unix-socket WhoIs") {
    const std::string path = "/tmp/vgi-ts-" + std::to_string(::getpid()) + ".sock";
    ::unlink(path.c_str());
    const int listener = ::socket(AF_UNIX, SOCK_STREAM, 0);
    REQUIRE(listener >= 0);
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
    REQUIRE(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    REQUIRE(::listen(listener, 1) == 0);
    std::jthread server([&] {
        const int client = ::accept(listener, nullptr, nullptr);
        if (client < 0) return;
        std::array<char, 4096> request{};
        (void)::recv(client, request.data(), request.size(), 0);
        const auto body = whois_json(true);
        const auto response =
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
            std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
        (void)::send(client, response.data(), response.size(), 0);
        ::close(client);
    });
    auto provider =
        tailscale_localapi_identity_provider({"tailnet:example", path, {}, {}, 1s, 65'536, 32'768});
    auto context = resolution();
    context.destination_address = "[2001:db8::8]:443";
    const auto available = provider(context);
    REQUIRE(available.status == PeerIdentityStatus::AVAILABLE);
    REQUIRE(available.identities.front().subject_kind() == PeerSubjectKind::TAGGED_NODE);
    REQUIRE(available.identities.front().subject_key() ==
            std::optional<std::string>("node:n123CNTRL"));
    ::close(listener);
    ::unlink(path.c_str());
}
#endif

TEST_CASE("SPIFFE IDs and provider configuration accept no implicit trust") {
    REQUIRE(validate_spiffe_id("spiffe://example.org/ns/default/sa/worker", {"example.org"}) ==
            "example.org");
    for (const auto& value : {"spiffe://example.org/a%2Fb", "spiffe://example.org/a//b",
                              "spiffe://example.org/a/../b", "spiffe://other.org/a"})
        REQUIRE_THROWS_AS(validate_spiffe_id(value, {"example.org"}), std::invalid_argument);
    REQUIRE_THROWS_AS(tailscale_serve_identity_provider({"tailnet", {"localhost"}}),
                      std::invalid_argument);
    REQUIRE_THROWS_AS(nginx_spiffe_provider({{"example.org"}, {"127.0.0.0/8"}}),
                      std::invalid_argument);
}

TEST_CASE("canonical transport identity vectors") {
    std::ifstream stream(VGI_RPC_IDENTITY_VECTORS);
    REQUIRE(stream);
    const auto vectors = nlohmann::json::parse(stream);
    REQUIRE(vectors["version"] == 1);

    for (const auto& item : vectors["spiffe_id_cases"]) {
        const auto expected = item["expected"].get<std::string>();
        if (expected == "valid") {
            REQUIRE(validate_spiffe_id(item["value"], {"example.org"}) == "example.org");
        } else {
            REQUIRE_THROWS_AS(validate_spiffe_id(item["value"], {"example.org"}),
                              std::invalid_argument);
        }
    }

    auto envoy = envoy_xfcc_spiffe_provider(EnvoyXfccSpiffeOptions{spiffe_options()});
    for (const auto& item : vectors["envoy_xfcc_cases"]) {
        auto context =
            resolution({{"X-Forwarded-Client-Cert", {item["value"].get<std::string>()}}});
        REQUIRE(envoy(context).status == vector_status(item["expected"]));
    }

    auto gcp = gcp_load_balancer_spiffe_provider(GcpSpiffeOptions{spiffe_options()});
    for (const auto& item : vectors["gcp_cases"]) {
        auto context = resolution(vector_headers(item["headers"]));
        REQUIRE(gcp(context).status == vector_status(item["expected"]));
    }

    auto tailscale = tailscale_serve_identity_provider({"tailnet:example", {"127.0.0.1"}, 16'384});
    for (const auto& item : vectors["tailscale_serve_cases"]) {
        auto context = resolution(vector_headers(item["headers"]));
        const auto result = tailscale(context);
        REQUIRE(result.status == vector_status(item["expected"]));
        if (item.contains("subject_stability") && result.status == PeerIdentityStatus::AVAILABLE) {
            const auto expected = item["subject_stability"].get<std::string>();
            REQUIRE(result.identities.front().subject_stability() ==
                    (expected == "login" ? SubjectStability::LOGIN : SubjectStability::NONE));
        }
    }
}
