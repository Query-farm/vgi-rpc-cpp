// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Sealed grants (WIRE_PROTOCOL.md §16, IDENTITY_V1_SPEC.md §9).
//
// The format half is driven by the reference's published vectors
// (`tests/data/grant_token_vectors.json`, a verbatim copy of
// `vgi_rpc/conformance/grant_token_vectors.json`): a minter that agrees only
// with its own verifier is worth nothing.  The chain half runs against a real
// HTTP server, because the property that matters -- a bad `vgig1.` token stops
// the chain and never reaches `resolve_token` -- is a property of the order the
// transport consults things in, which no unit of the verifier can show.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/crypto.h>
#include <vgi_rpc/grants.h>
#include <vgi_rpc/http_config.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/token_identity.h>
#include <vgi_rpc/wire.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>
#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#include <unistd.h>

using namespace vgi_rpc;

namespace {

const nlohmann::json& vectors() {
    // Binary mode and nlohmann's own UTF-8 parsing: the audience in
    // `audience_unicode_no_scopes` is non-ASCII and must reach the AAD as the
    // exact UTF-8 bytes, never re-decoded through a platform code page.
    static const nlohmann::json loaded = [] {
        std::ifstream in(VGI_RPC_GRANT_VECTORS, std::ios::binary);
        REQUIRE(in.good());
        std::stringstream buffer;
        buffer << in.rdbuf();
        return nlohmann::json::parse(buffer.str());
    }();
    return loaded;
}

std::string hex(const std::string& bytes) {
    return crypto::hex_encode(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

GrantKey key_from_b64(const std::string& text) {
    return GrantKeys::parse({text}).keys.front();
}

/// The vector defaults with a case's overrides applied.
GrantKeys keys_for(const nlohmann::json& item) {
    const auto& defaults = vectors().at("defaults");
    const auto& encoded = item.contains("verify_keys_b64") ? item.at("verify_keys_b64")
                                                           : defaults.at("verify_keys_b64");
    return GrantKeys::parse(
        encoded.get<std::vector<std::string>>(),
        item.value("audience", defaults.at("audience").get<std::string>()),
        item.value("max_ttl_seconds", defaults.at("max_ttl_seconds").get<int64_t>()),
        item.value("clock_skew_seconds", defaults.at("clock_skew_seconds").get<int64_t>()));
}

double now_for(const nlohmann::json& item) {
    return item.value("now", vectors().at("defaults").at("now").get<double>());
}

double wall_now() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

GrantKeys fixture_keys() {
    GrantKeys keys;
    GrantKey current{};
    GrantKey previous{};
    for (uint8_t i = 0; i < 32; ++i) {
        current[i] = static_cast<uint8_t>(0x10 + i);
        previous[i] = static_cast<uint8_t>(0x30 + i);
    }
    keys.keys = {current, previous};
    keys.audience = "conformance";
    keys.max_ttl_seconds = 3600;
    return keys;
}

}  // namespace

// ---------------------------------------------------------------------------
// The published vectors
// ---------------------------------------------------------------------------

TEST_CASE("every mint vector reproduces the exact token", "[grants]") {
    const auto& mints = vectors().at("mint");
    REQUIRE(mints.size() >= 4);
    for (const auto& item : mints) {
        INFO(item.at("name").get<std::string>());
        GrantKeys keys = GrantKeys::parse({item.at("minting_key_b64").get<std::string>()},
                                          item.at("audience").get<std::string>(),
                                          item.at("max_ttl_seconds").get<int64_t>());
        const auto& request = item.at("request");
        GrantMintOverrides overrides;
        overrides.now = item.at("now").get<int64_t>();
        overrides.grant_id = request.at("grant_id").get<std::string>();
        const auto nonce = crypto::hex_decode(item.at("nonce_hex").get<std::string>());
        REQUIRE(nonce);
        REQUIRE(nonce->size() == crypto::kAeadNonceBytes);
        std::array<uint8_t, crypto::kAeadNonceBytes> fixed{};
        std::copy(nonce->begin(), nonce->end(), fixed.begin());
        overrides.nonce = fixed;

        const auto minted = mint_grant_token(keys, request.at("principal").get<std::string>(),
                                             request.at("scopes").get<std::vector<std::string>>(),
                                             request.at("purpose").get<std::string>(),
                                             request.at("ttl_seconds").get<int64_t>(), overrides);

        const std::string kid = grant_key_id(keys.keys.front());
        CHECK(hex(kid) == item.at("kid_hex").get<std::string>());
        CHECK(hex(keys.aad(kid)) == item.at("aad_hex").get<std::string>());
        CHECK(hex(encode_grant_payload(minted.claims)) ==
              item.at("payload_hex").get<std::string>());
        CHECK(minted.token == item.at("token").get<std::string>());

        const auto& claims = item.at("claims");
        CHECK(minted.claims.issued_at == claims.at("issued_at").get<int64_t>());
        CHECK(minted.claims.expires_at == claims.at("expires_at").get<int64_t>());

        // And the verifier accepts its own minter's vector, claim for claim.
        const auto verified =
            verify_grant_token(keys, minted.token, static_cast<double>(*overrides.now));
        CHECK(verified.principal == claims.at("principal").get<std::string>());
        CHECK(verified.scopes == claims.at("scopes").get<std::vector<std::string>>());
        CHECK(verified.purpose == claims.at("purpose").get<std::string>());
        CHECK(verified.grant_id == claims.at("grant_id").get<std::string>());
    }
}

TEST_CASE("every accept vector verifies", "[grants]") {
    for (const auto& item : vectors().at("accept")) {
        INFO(item.at("name").get<std::string>());
        CHECK_NOTHROW(
            verify_grant_token(keys_for(item), item.at("token").get<std::string>(), now_for(item)));
    }
}

TEST_CASE("every reject vector is refused, with the published expired flag", "[grants]") {
    const auto& rejects = vectors().at("reject");
    REQUIRE(rejects.size() >= 9);
    for (const auto& item : rejects) {
        INFO(item.at("name").get<std::string>());
        bool refused = false;
        try {
            verify_grant_token(keys_for(item), item.at("token").get<std::string>(), now_for(item));
        } catch (const GrantInvalidError& e) {
            refused = true;
            CHECK(e.expired() == item.at("expired").get<bool>());
        }
        CHECK(refused);
    }
}

TEST_CASE("the minting key is the first, and every key verifies", "[grants]") {
    GrantKeys keys = fixture_keys();
    const auto minted = mint_grant_token(keys, "alice", {"read"}, "p", 60);
    CHECK(crypto::base64url_decode(minted.token.substr(6))->substr(0, 8) ==
          grant_key_id(keys.keys[0]));

    GrantKeys previous_only = keys;
    previous_only.keys = {keys.keys[1]};
    const auto old = mint_grant_token(previous_only, "bob", {}, "p", 60);
    CHECK(verify_grant_token(keys, old.token).principal == "bob");
}

TEST_CASE("a minted lifetime is capped and a non-positive one refused", "[grants]") {
    GrantKeys keys = fixture_keys();
    const auto minted = mint_grant_token(keys, "alice", {}, "p", 10'000'000);
    CHECK(minted.claims.expires_at - minted.claims.issued_at == 3600);
    CHECK_THROWS_AS(mint_grant_token(keys, "alice", {}, "p", 0), std::invalid_argument);
    CHECK_THROWS_AS(mint_grant_token(keys, "alice", {}, "p", -5), std::invalid_argument);
}

TEST_CASE("only the exact prefix is a grant", "[grants]") {
    GrantKeys keys = fixture_keys();
    const auto minted = mint_grant_token(keys, "alice", {}, "p", 60);
    for (const std::string& token :
         {"VGIG1." + minted.token.substr(6), "vgig2." + minted.token.substr(6),
          minted.token.substr(6), " " + minted.token}) {
        INFO(token.substr(0, 8));
        CHECK_THROWS_AS(verify_grant_token(keys, token), GrantInvalidError);
    }
    CHECK_THROWS_AS(verify_grant_token(keys, std::string(kGrantTokenPrefix) +
                                                 std::string(kMaxGrantTokenChars, 'A')),
                    GrantInvalidError);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_CASE("grant keys parse from standard base64, padded or not", "[grants][config]") {
    const std::string padded = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    const auto a = GrantKeys::parse({padded});
    const auto b = GrantKeys::parse({"  " + padded.substr(0, padded.size() - 1) + "\n"});
    CHECK(a.keys == b.keys);
    CHECK(a.keys.front()[31] == 0x1f);
}

TEST_CASE("a malformed grant configuration is refused", "[grants][config]") {
    const std::string good = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    CHECK_THROWS_AS(GrantKeys::parse({}), std::invalid_argument);
    CHECK_THROWS_AS(GrantKeys::parse({"not base64!"}), std::invalid_argument);
    CHECK_THROWS_AS(GrantKeys::parse({"AAAA"}), std::invalid_argument);  // 3 bytes
    // The url-safe alphabet is not the configured spelling.
    CHECK_THROWS_AS(GrantKeys::parse({"-_" + good.substr(2)}), std::invalid_argument);
    CHECK_THROWS_AS(GrantKeys::parse({good, good}), std::invalid_argument);
    CHECK_THROWS_AS(GrantKeys::parse({good}, "", 0), std::invalid_argument);
    CHECK_THROWS_AS(GrantKeys::parse({good}, "", -1), std::invalid_argument);
}

TEST_CASE("grant configuration reads the environment", "[grants][config]") {
    const std::string k1 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    const std::string k2 = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";
    std::map<std::string, std::string> env;
    const auto getenv = [&env](const char* name) -> std::optional<std::string> {
        const auto found = env.find(name);
        if (found == env.end()) return std::nullopt;
        return found->second;
    };

    CHECK_FALSE(GrantKeys::from_env(getenv).has_value());
    env[kGrantKeysEnv] = "   ";
    CHECK_FALSE(GrantKeys::from_env(getenv).has_value());

    env[kGrantKeysEnv] = k1 + ", " + k2 + ",";
    auto keys = GrantKeys::from_env(getenv);
    REQUIRE(keys);
    CHECK(keys->keys.size() == 2);
    CHECK(keys->keys[0] == key_from_b64(k1));
    CHECK(keys->audience.empty());
    CHECK(keys->max_ttl_seconds == kDefaultGrantMaxTtlSeconds);

    env[kGrantAudienceEnv] = "prod";
    env[kGrantMaxTtlEnv] = "3600";
    keys = GrantKeys::from_env(getenv);
    CHECK(keys->audience == "prod");
    CHECK(keys->max_ttl_seconds == 3600);

    env[kGrantMaxTtlEnv] = "soon";
    CHECK_THROWS_AS(GrantKeys::from_env(getenv), std::invalid_argument);
    env[kGrantMaxTtlEnv] = "0";
    CHECK_THROWS_AS(GrantKeys::from_env(getenv), std::invalid_argument);
    env.erase(kGrantMaxTtlEnv);
    env[kGrantKeysEnv] = k1 + ",garbage";
    CHECK_THROWS_AS(GrantKeys::from_env(getenv), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// IdentityImpl and the builder
// ---------------------------------------------------------------------------

namespace {

AuthContext fresh_caller(const std::string& principal) {
    AuthContext auth;
    auth.domain = "conformance";
    auth.authenticated = true;
    auth.principal = principal;
    auth.claims["auth_time"] = wall_now() - 10;
    return auth;
}

}  // namespace

TEST_CASE("grant keys without a hook make the framework the minter", "[grants][identity]") {
    IdentityOptions options;
    options.grant_keys = fixture_keys();
    IdentityImpl impl(std::move(options));
    CHECK(impl.offered_methods() == std::set<std::string>{"issue_grant"});

    const auto grant = impl.issue_grant("nightly", {"read", "write"}, 600, fresh_caller("alice"));
    REQUIRE(grant.token.rfind(kGrantTokenPrefix, 0) == 0);
    const auto claims = verify_grant_token(fixture_keys(), grant.token);
    CHECK(claims.principal == "alice");
    CHECK(claims.scopes == std::vector<std::string>{"read", "write"});
    CHECK(claims.purpose == "nightly");
    CHECK(claims.grant_id == grant.grant_id);
    CHECK(claims.grant_id.size() == 32);
    CHECK(grant.expires_at == static_cast<double>(claims.expires_at));
    CHECK_THROWS_AS(impl.issue_grant("p", {}, 0, fresh_caller("alice")), GrantRefusedError);
}

TEST_CASE("a worker's own mint hook wins over sealed grants", "[grants][identity]") {
    IdentityOptions options;
    options.grant_keys = fixture_keys();
    options.mint_grant = [](const std::string& principal, const std::string&,
                            const std::vector<std::string>&,
                            int64_t) { return IssuedGrant{"custom:" + principal, 1.0, "id"}; };
    IdentityImpl impl(std::move(options));
    CHECK(impl.issue_grant("p", {}, 60, fresh_caller("alice")).token == "custom:alice");
}

TEST_CASE("no grant keys changes nothing", "[grants][identity]") {
    ServerBuilder builder;
    builder.grant_keys(std::nullopt);
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    auto server = builder.build();
    CHECK(server->identity() == nullptr);
}

TEST_CASE("grant keys alone host issue_grant", "[grants][identity]") {
    ServerBuilder builder;
    builder.grant_keys(fixture_keys());
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    auto server = builder.build();
    REQUIRE(server->identity() != nullptr);
    CHECK(server->identity()->offered_methods() == std::set<std::string>{"issue_grant"});
    CHECK(server->identity()->grant_keys().has_value());
}

TEST_CASE("an identity without the configured keys is refused", "[grants][identity]") {
    IdentityOptions options;
    options.mint_grant = [](const std::string&, const std::string&, const std::vector<std::string>&,
                            int64_t) { return IssuedGrant{}; };
    ServerBuilder builder;
    builder.identity(std::make_shared<IdentityImpl>(std::move(options)));
    builder.grant_keys(fixture_keys());
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    CHECK_THROWS_AS(builder.build(), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// The HTTP authentication chain
// ---------------------------------------------------------------------------

namespace {

constexpr const char* kArrow = "application/vnd.apache.arrow.stream";

std::shared_ptr<arrow::Schema> whoami_schema() {
    return arrow::schema({arrow::field("result", arrow::utf8(), /*nullable=*/false)});
}

std::atomic<int> g_resolver_calls{0};

std::optional<TokenIdentity> fixture_resolver(const std::string& token) {
    ++g_resolver_calls;
    if (token == "unknown") return std::nullopt;
    if (token == "outage") throw IdentityUnavailableError("down", 5);
    if (token == "auth-outage") throw AuthUnavailableError("down", 7);
    return TokenIdentity{"subject", "named", 300};
}

/// A server hosting `whoami` plus Identity with the fixture keys and resolver.
class ChainServer {
public:
    explicit ChainServer(HttpConfig cfg = {}, bool with_resolver = true,
                         const std::string& access_log = "") {
        ServerBuilder builder;
        builder.protocol("Chain");
        builder.add_unary("whoami", empty_schema(), whoami_schema(),
                          [](const Request&, CallContext& ctx) -> Result {
                              const auto& auth = ctx.auth();
                              nlohmann::json body = {{"authenticated", auth.authenticated},
                                                     {"claims", auth.claims},
                                                     {"domain", auth.domain},
                                                     {"principal", auth.principal.value_or("")}};
                              arrow::StringBuilder b;
                              VGI_RPC_THROW_NOT_OK(b.Append(body.dump()));
                              return Result::value(whoami_schema(), {unwrap(b.Finish())});
                          });
        IdentityOptions options;
        if (with_resolver) {
            options.resolve_token = fixture_resolver;
            options.introspect_principals = {"introspector"};
        }
        options.grant_keys = fixture_keys();
        builder.identity(std::make_shared<IdentityImpl>(std::move(options)));
        builder.grant_keys(fixture_keys());
        if (!access_log.empty()) builder.access_log(access_log);
        server_ = builder.build();

        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.on_listen = [this](int port) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                port_ = port;
            }
            ready_.notify_all();
        };
        thread_ = std::thread([this, cfg]() { server_->serve_http(cfg); });
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait_for(lock, std::chrono::seconds(10), [this]() { return port_ != 0; });
        REQUIRE(port_ != 0);
    }

    ~ChainServer() { thread_.detach(); }

    struct Reply {
        int status = 0;
        std::string reason;
        std::string retry_after;
        std::optional<nlohmann::json> who;
    };

    Reply whoami(const std::optional<std::string>& authorization) const {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(keys::METHOD, "whoami");
        metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
        metadata->Append(keys::PROTOCOL, "Chain");
        auto sink = unwrap(arrow::io::BufferOutputStream::Create());
        write_ipc_stream(
            sink, empty_schema(),
            {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), std::move(metadata))});
        auto buffer = unwrap(sink->Finish());
        const std::string body(reinterpret_cast<const char*>(buffer->data()),
                               static_cast<size_t>(buffer->size()));

        httplib::Client client("127.0.0.1", port_);
        client.set_read_timeout(10, 0);
        httplib::Headers headers;
        if (authorization) headers.emplace("Authorization", *authorization);
        auto response = client.Post("/vgi/Chain/whoami", headers, body, kArrow);
        REQUIRE(response);
        Reply reply;
        reply.status = response->status;
        reply.reason = response->get_header_value("VGI-Auth-Reason");
        reply.retry_after = response->get_header_value("Retry-After");
        if (response->status == 200) {
            auto input = std::make_shared<arrow::io::BufferReader>(
                std::make_shared<arrow::Buffer>(response->body));
            auto reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(input));
            std::shared_ptr<arrow::RecordBatch> batch;
            while (true) {
                VGI_RPC_THROW_NOT_OK(reader->ReadNext(&batch));
                if (!batch) break;
                if (batch->num_rows() == 1 && batch->schema()->field(0)->name() == "result") {
                    const auto column =
                        std::static_pointer_cast<arrow::StringArray>(batch->column(0));
                    reply.who = nlohmann::json::parse(column->GetString(0));
                }
            }
        }
        return reply;
    }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

std::string grant_for(const std::string& principal, int64_t ttl = 600,
                      std::optional<int64_t> now = std::nullopt, GrantKeys keys = fixture_keys()) {
    GrantMintOverrides overrides;
    overrides.now = now;
    return mint_grant_token(keys, principal, {"read"}, "chain", ttl, overrides).token;
}

}  // namespace

TEST_CASE("a sealed grant authenticates as its principal, with no auth_time", "[grants][http]") {
    ChainServer server;
    const auto reply = server.whoami("Bearer " + grant_for("alice"));
    REQUIRE(reply.status == 200);
    REQUIRE(reply.who);
    CHECK((*reply.who)["domain"] == "grant");
    CHECK((*reply.who)["principal"] == "alice");
    CHECK((*reply.who)["claims"]["scopes"] == nlohmann::json::array({"read"}));
    CHECK((*reply.who)["claims"]["purpose"] == "chain");
    CHECK_FALSE((*reply.who)["claims"].contains("auth_time"));
}

TEST_CASE("a bad grant is 401 and never reaches resolve_token", "[grants][http]") {
    ChainServer server;
    const auto good = grant_for("alice");
    const size_t mid = good.size() - 10;
    std::string tampered = good;
    tampered[mid] = good[mid] == 'A' ? 'B' : 'A';
    GrantKeys stranger = fixture_keys();
    stranger.keys = {GrantKey{}};
    stranger.keys.front().fill(0x77);

    const int before = g_resolver_calls.load();
    for (const auto& token :
         {tampered, good + "=", grant_for("alice", 600, std::nullopt, stranger)}) {
        const auto reply = server.whoami("Bearer " + token);
        CHECK(reply.status == 401);
        CHECK(reply.reason == "invalid_credential");
    }
    const auto expired =
        server.whoami("Bearer " + grant_for("alice", 60, static_cast<int64_t>(wall_now()) - 3000));
    CHECK(expired.status == 401);
    CHECK(expired.reason == "expired_credential");
    CHECK(g_resolver_calls.load() == before);
}

TEST_CASE("a grant inside the clock skew is accepted", "[grants][http]") {
    ChainServer server;
    const auto reply =
        server.whoami("Bearer " + grant_for("alice", 60, static_cast<int64_t>(wall_now()) - 90));
    CHECK(reply.status == 200);
}

TEST_CASE("only the exact prefix routes to the grant verifier", "[grants][http]") {
    ChainServer server;
    const auto body = grant_for("alice").substr(6);
    for (const auto& token : {"vgig2." + body, body}) {
        const auto reply = server.whoami("Bearer " + token);
        REQUIRE(reply.status == 200);
        CHECK((*reply.who)["domain"] == "token");
    }
}

TEST_CASE("resolve_token backs bearer authentication", "[grants][http]") {
    ChainServer server;
    const auto resolved = server.whoami(std::string("Bearer opaque"));
    REQUIRE(resolved.status == 200);
    CHECK(*resolved.who == nlohmann::json{{"authenticated", true},
                                          {"claims", {{"token_name", "named"}}},
                                          {"domain", "token"},
                                          {"principal", "subject"}});
    CHECK(server.whoami(std::string("Bearer unknown")).status == 401);
    const auto outage = server.whoami(std::string("Bearer outage"));
    CHECK(outage.status == 503);
    CHECK(outage.retry_after == "5");
    const auto auth_outage = server.whoami(std::string("Bearer auth-outage"));
    CHECK(auth_outage.status == 503);
    CHECK(auth_outage.retry_after == "7");

    const int before = g_resolver_calls.load();
    CHECK(server.whoami(std::string("Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln")).status ==
          401);
    CHECK(server.whoami(std::string("Bearer ") + std::string(kMaxTokenBytes + 1, 'x')).status ==
          401);
    // (A blank bearer cannot be put on the wire -- HTTP strips trailing
    // whitespace from a field value -- so that guard is pinned in
    // token_identity_test against reject_jws_shaped directly.)
    CHECK(server.whoami(std::string("Basic abc")).status == 401);
    CHECK(g_resolver_calls.load() == before);
}

TEST_CASE("no credential stays anonymous", "[grants][http]") {
    ChainServer server;
    const auto reply = server.whoami(std::nullopt);
    REQUIRE(reply.status == 200);
    CHECK((*reply.who)["authenticated"] == false);
}

TEST_CASE("a grant without a resolver never falls through", "[grants][http]") {
    ChainServer server(HttpConfig{}, /*with_resolver=*/false);
    CHECK(server.whoami("Bearer " + grant_for("alice")).status == 200);
    CHECK(server.whoami(std::string("Bearer opaque")).status == 401);
}

TEST_CASE("the deployment's bearer authenticator runs first", "[grants][http]") {
    HttpConfig cfg;
    cfg.bearer_authenticate = [](const std::string& token) -> std::optional<AuthContext> {
        if (token != "static-token") return std::nullopt;
        AuthContext auth;
        auth.domain = "bearer";
        auth.authenticated = true;
        auth.principal = "static-user";
        return auth;
    };
    ChainServer server(cfg);
    const auto as_static = server.whoami(std::string("Bearer static-token"));
    REQUIRE(as_static.status == 200);
    CHECK((*as_static.who)["domain"] == "bearer");
    const auto as_grant = server.whoami("Bearer " + grant_for("alice"));
    REQUIRE(as_grant.status == 200);
    CHECK((*as_grant.who)["domain"] == "grant");
    // With a deployment bearer authenticator, no credential is a refusal.
    CHECK(server.whoami(std::nullopt).status == 401);
}

TEST_CASE("bearer alternatives are not OR-ed beside a peer policy", "[grants][http]") {
    ServerBuilder builder;
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    builder.grant_keys(fixture_keys());
    auto server = builder.build();
    HttpConfig cfg;
    cfg.port = 0;
    cfg.peer_identity_providers.push_back([](const PeerResolutionContext&) {
        PeerIdentityResult result;
        result.provider = "x";
        return result;
    });
    cfg.peer_authentication_policy = peer_identity_primary("x");
    CHECK_THROWS_AS(server->serve_http(cfg), std::invalid_argument);
}

TEST_CASE("the access log records how a bearer authenticated", "[grants][http]") {
    const auto path = std::filesystem::temp_directory_path() /
                      ("vgi-grants-access-" + std::to_string(::getpid()) + ".jsonl");
    std::filesystem::remove(path);
    {
        ChainServer server(HttpConfig{}, /*with_resolver=*/true, path.string());
        REQUIRE(server.whoami("Bearer " + grant_for("alice")).status == 200);
        REQUIRE(server.whoami(std::string("Bearer opaque")).status == 200);
        REQUIRE(server.whoami(std::nullopt).status == 200);
    }
    // A record is written after its response, so wait for the last one.
    std::vector<nlohmann::json> records;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (records.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        records.clear();
        std::ifstream in(path);
        for (std::string line; std::getline(in, line);) {
            records.push_back(nlohmann::json::parse(line));
        }
        if (records.size() < 3) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::filesystem::remove(path);
    REQUIRE(records.size() == 3);
    CHECK(records[0]["principal"] == "alice");
    CHECK(records[0]["auth_domain"] == "grant");
    CHECK(records[0]["authenticated"] == true);
    CHECK(records[1]["principal"] == "subject");
    CHECK(records[1]["auth_domain"] == "token");
    CHECK(records[2]["principal"] == "");
    CHECK(records[2]["authenticated"] == false);
}
