// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `HttpConfig::authenticate_every_request` and `HttpConfig::www_authenticate`:
// a deployment with exactly one way in (an Orchard hosted worker) refuses every
// request without a credential -- health GET, static assets and unknown paths
// included -- before the body is read.  Only `HEAD /health` and CORS preflights
// are exempt.  These are properties of where the transport checks, so they run
// against a real server.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/errors.h>
#include <vgi_rpc/http_config.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/wire.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>
#include <httplib.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

using namespace vgi_rpc;

namespace {

constexpr const char* kArrow = "application/vnd.apache.arrow.stream";
constexpr const char* kChallenge = "Bearer error=\"invalid_token\"";

std::atomic<int> g_authenticator_calls{0};

std::optional<AuthContext> fixture_bearer(const std::string& token) {
    ++g_authenticator_calls;
    if (token == "good") {
        AuthContext auth;
        auth.domain = "fixture";
        auth.authenticated = true;
        auth.principal = "alice";
        return auth;
    }
    if (token == "lapsed") throw AuthRejectedError(AuthReason::EXPIRED_CREDENTIAL);
    if (token == "outage") throw AuthUnavailableError("down", 9);
    if (token == "anonymous") return AuthContext::anonymous();
    return std::nullopt;
}

class GuardedServer {
public:
    explicit GuardedServer(bool every_request = true) {
        ServerBuilder builder;
        builder.protocol("Guarded");
        builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
        server_ = builder.build();

        HttpConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.cors_origin = "*";
        cfg.max_request_bytes = 1024;
        cfg.bearer_authenticate = fixture_bearer;
        cfg.authenticate_every_request = every_request;
        cfg.www_authenticate = kChallenge;
        cfg.static_assets["/"] = {"<h1>landing</h1>", "text/html", std::nullopt};
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
    ~GuardedServer() { thread_.detach(); }

    httplib::Result send(const std::string& method, const std::string& path,
                         const std::optional<std::string>& bearer,
                         httplib::Headers headers = {}) const {
        httplib::Client client("127.0.0.1", port_);
        client.set_read_timeout(10, 0);
        if (bearer) headers.emplace("Authorization", "Bearer " + *bearer);
        if (method == "HEAD") return client.Head(path, headers);
        if (method == "OPTIONS") return client.Options(path, headers);
        if (method == "POST") return client.Post(path, headers, noop_body(), kArrow);
        return client.Get(path, headers);
    }

    // Raw bytes, for requests a client library will not send: a body
    // announced and never delivered.
    std::string raw(const std::string& request) const {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(fd >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        timeval tv{5, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        REQUIRE(::send(fd, request.data(), request.size(), 0) ==
                static_cast<ssize_t>(request.size()));
        std::string reply;
        char buf[4096];
        while (true) {
            const auto got = ::recv(fd, buf, sizeof(buf), 0);
            if (got <= 0) break;
            reply.append(buf, static_cast<size_t>(got));
            if (reply.find("\r\n\r\n") != std::string::npos) break;
        }
        ::close(fd);
        return reply;
    }

private:
    static std::string noop_body() {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(keys::METHOD, "noop");
        metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
        metadata->Append(keys::PROTOCOL, "Guarded");
        auto sink = unwrap(arrow::io::BufferOutputStream::Create());
        write_ipc_stream(
            sink, empty_schema(),
            {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), std::move(metadata))});
        auto buffer = unwrap(sink->Finish());
        return std::string(reinterpret_cast<const char*>(buffer->data()),
                           static_cast<size_t>(buffer->size()));
    }

    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

void check_refused(const httplib::Result& res, const std::string& reason) {
    REQUIRE(res);
    CHECK(res->status == 401);
    CHECK(res->get_header_value("VGI-Auth-Reason") == reason);
    CHECK(res->get_header_value("WWW-Authenticate") == kChallenge);
    // The capability headers ride the refusal, so a client can still learn them.
    CHECK(res->has_header("VGI-Supported-Encodings"));
    CHECK(res->get_header_value("Access-Control-Allow-Origin") == "*");
}

}  // namespace

TEST_CASE("only HEAD /health and preflights are anonymous", "[http][auth]") {
    GuardedServer server;
    const auto head = server.send("HEAD", "/health", std::nullopt);
    REQUIRE(head);
    CHECK(head->status == 200);
    CHECK(head->has_header("VGI-Supported-Encodings"));
    CHECK(server.send("HEAD", "/vgi/health", std::nullopt)->status == 200);

    const auto preflight =
        server.send("OPTIONS", "/vgi/Guarded/noop", std::nullopt,
                    {{"Origin", "https://shell.example.com"},
                     {"Access-Control-Request-Method", "POST"},
                     {"Access-Control-Request-Headers", "authorization"}});
    REQUIRE(preflight);
    CHECK(preflight->status / 100 == 2);
    CHECK(preflight->get_header_value("Access-Control-Allow-Origin") == "*");

    for (const auto& [method, path] : std::vector<std::pair<std::string, std::string>>{
             {"GET", "/health"},
             {"OPTIONS", "/health"},
             {"GET", "/"},
             {"GET", "/no/such/path"},
             {"HEAD", "/"},
             {"POST", "/vgi/Guarded/noop"},
             {"POST", "/vgi/Guarded/unknown_method"}}) {
        INFO(method << " " << path);
        check_refused(server.send(method, path, std::nullopt, {{"Origin", "https://x.test"}}),
                      "missing_credential");
    }
}

TEST_CASE("a credential is checked on every path", "[http][auth]") {
    GuardedServer server;
    CHECK(server.send("GET", "/health", std::string("good"))->status == 200);
    CHECK(server.send("GET", "/", std::string("good"))->status == 200);
    CHECK(server.send("GET", "/no/such/path", std::string("good"))->status == 404);
    CHECK(server.send("POST", "/vgi/Guarded/noop", std::string("good"))->status == 200);

    check_refused(server.send("GET", "/health", std::string("nope")), "invalid_credential");
    check_refused(server.send("GET", "/health", std::string("lapsed")), "expired_credential");
    check_refused(server.send("POST", "/vgi/Guarded/noop", std::string("lapsed")),
                  "expired_credential");
    // An authenticator that answers anonymous does not open the door.
    check_refused(server.send("GET", "/health", std::string("anonymous")), "invalid_credential");
    check_refused(server.send("GET", "/health", std::nullopt, {{"Authorization", "Basic eDp5"}}),
                  "invalid_credential");

    const auto outage = server.send("POST", "/vgi/Guarded/noop", std::string("outage"));
    REQUIRE(outage);
    CHECK(outage->status == 503);
    CHECK(outage->get_header_value("Retry-After") == "9");
}

TEST_CASE("the refusal comes before the body is read", "[http][auth]") {
    GuardedServer server;
    // A body far over the 1 KiB request cap, announced and never sent: a
    // server that read or size-checked first would answer 413 or wait.
    const auto reply = server.raw(
        "POST /vgi/Guarded/noop HTTP/1.1\r\nHost: x\r\n"
        "Content-Type: application/vnd.apache.arrow.stream\r\n"
        "Content-Encoding: zstd\r\nContent-Length: 100000000\r\n\r\n");
    CHECK(reply.rfind("HTTP/1.1 401", 0) == 0);
    CHECK(reply.find("missing_credential") != std::string::npos);

    // A client waiting on `Expect: 100-continue` is refused instead of told to go ahead.
    const auto expect = server.raw(
        "POST /vgi/Guarded/noop HTTP/1.1\r\nHost: x\r\n"
        "Content-Type: application/vnd.apache.arrow.stream\r\n"
        "Authorization: Bearer nope\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n");
    CHECK(expect.rfind("HTTP/1.1 401", 0) == 0);
    CHECK(expect.find("100 Continue") == std::string::npos);
}

TEST_CASE("without the flag health and unknown paths stay anonymous", "[http][auth]") {
    GuardedServer server(/*every_request=*/false);
    CHECK(server.send("GET", "/health", std::nullopt)->status == 200);
    CHECK(server.send("GET", "/no/such/path", std::nullopt)->status == 404);
    // The RPC route still needs the bearer, and now says it was missing.
    check_refused(server.send("POST", "/vgi/Guarded/noop", std::nullopt), "missing_credential");
}
