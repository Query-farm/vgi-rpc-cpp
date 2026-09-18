// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Protocol routing over HTTP: `{prefix}/{protocol}/{method}`.
//
// Against a real server rather than the routing function alone, because the
// property that matters most cannot be seen from inside it: cpp-httplib
// percent-decodes `req.path` before a handler runs, so a server that routed on
// the decoded path would admit `%52` as `R` and the percent ban would be
// unenforceable.  That is the trap every port hit in its own dialect -- Go's
// ServeMux and Java's getPathInfo() decode too, and .NET decodes client-side,
// which is how one port's first three cases passed against a server that was
// never actually tested.  Only a request put on the wire can tell.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_config.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/reflection.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/wire.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>
#include <httplib.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

using namespace vgi_rpc;

namespace {

constexpr const char* kProtocol = "RouteProbe";
constexpr const char* kArrow = "application/vnd.apache.arrow.stream";

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::int64(), /*nullable=*/false)});
}

Result echo_handler(const Request& req, CallContext&) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(req.get<int64_t>("value")));
    return Result::value(arrow::RecordBatch::Make(value_schema(), 1, {unwrap(builder.Finish())}));
}

/// One `Server::serve_http` on a background thread, with its bound port.
class RoutingServer {
public:
    RoutingServer() {
        ServerBuilder builder;
        builder.add_unary("echo", value_schema(), value_schema(), echo_handler);
        // Declares the routing key *and* registers a reserved
        // `__transport_options__`, so the reserved-vs-namespaced cases below
        // have a real reserved method to be wrong about. Without one they would
        // pass against a server that simply has no such method either way.
        builder.protocol(kProtocol);
        builder.enable_transport_options();
        server_ = builder.build();

        HttpConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.prefix = "/vgi";
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

    ~RoutingServer() {
        // cpp-httplib owns the accept loop inside serve_http; the process ends
        // the thread. Detach rather than join so one wedged case cannot hang
        // the whole binary.
        thread_.detach();
    }

    int port() const { return port_; }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

/// One request body: a `{value: 1}` batch (or an empty one) with RPC metadata.
std::string request_body(const std::string& method, std::optional<std::string> protocol,
                         bool empty_params = false) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::METHOD, method);
    metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    if (protocol) metadata->Append(keys::PROTOCOL, *protocol);

    auto schema = empty_params ? empty_schema() : value_schema();
    std::shared_ptr<arrow::RecordBatch> batch;
    if (empty_params) {
        batch = make_empty_batch(schema);
    } else {
        arrow::Int64Builder builder;
        VGI_RPC_THROW_NOT_OK(builder.Append(1));
        batch = arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
    }

    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(sink, schema, {AnnotatedBatch::with_metadata(batch, std::move(metadata))});
    auto buffer = unwrap(sink->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

struct Reply {
    int status = 0;
    std::string body;
};

Reply post(int port, const std::string& path, const std::string& body) {
    httplib::Client client("127.0.0.1", port);
    client.set_read_timeout(10, 0);
    auto response = client.Post(path, body, kArrow);
    REQUIRE(response);
    return Reply{response->status, response->body};
}

/// Whether an Arrow error body carries this `vgi_rpc.error_kind`.
bool has_error_kind(const std::string& body, const std::string& kind) {
    return body.find(kind) != std::string::npos;
}

}  // namespace

TEST_CASE("an application method is addressed under its protocol", "[http-routing]") {
    RoutingServer server;
    auto reply = post(server.port(), std::string("/vgi/") + kProtocol + "/echo",
                      request_body("echo", kProtocol));
    CHECK(reply.status == 200);
}

TEST_CASE("the flat method path is no longer a route", "[http-routing]") {
    // One method, one address. Serving both shapes would give an intermediary
    // two ways to reach the same handler, only one of which names a protocol.
    RoutingServer server;
    auto reply = post(server.port(), "/vgi/echo", request_body("echo", kProtocol));
    CHECK(reply.status == 404);
}

TEST_CASE("a percent in the protocol segment is refused without decoding", "[http-routing]") {
    RoutingServer server;
    // "%52outeProbe" percent-decodes to exactly the hosted name. A server that
    // routed on the decoded path would dispatch this; the ban exists because
    // then the edge and the worker read different strings.
    auto encoded = post(server.port(), "/vgi/%52outeProbe/echo", request_body("echo", kProtocol));
    CHECK(encoded.status == 404);

    // And the undisguised spelling of the same name still works, so the case
    // above is the percent ban rather than a broken route.
    auto plain = post(server.port(), std::string("/vgi/") + kProtocol + "/echo",
                      request_body("echo", kProtocol));
    CHECK(plain.status == 200);
}

TEST_CASE("an ungrammatical protocol name is refused before it is looked up", "[http-routing]") {
    // `[A-Za-z_][A-Za-z0-9_.]*`, at most 255 bytes. Checked before the lookup
    // so a request-supplied string never reaches an error message, a log field
    // or a metric label -- which is also why the answer names nothing.
    RoutingServer server;
    for (const std::string& name : {std::string("9leading-digit"), std::string("has space"),
                                    std::string("has/slash"), std::string(256, 'x')}) {
        auto reply = post(server.port(), "/vgi/" + name + "/echo", request_body("echo", name));
        CHECK(reply.status == 404);
        CHECK(reply.body.find(name) == std::string::npos);
    }
}

TEST_CASE("the path and the routing key must agree", "[http-routing]") {
    // Unspecified this is the Content-Length/Transfer-Encoding shape: the edge
    // applies policy to the protocol in the path while the worker dispatches
    // the one in the metadata.
    RoutingServer server;
    auto reply = post(server.port(), std::string("/vgi/") + kProtocol + "/echo",
                      request_body("echo", "SomethingElse"));
    CHECK(reply.status == 400);
    CHECK(has_error_kind(reply.body, ERROR_KIND_PROTOCOL_NOT_SUPPORTED));
}

TEST_CASE("an absent routing key is accepted on HTTP", "[http-routing]") {
    // Deliberately permissive, and only here: the path segment has already
    // resolved the binding, and the shared conformance harness requires a 200
    // for exactly this request. The raw transports still refuse it, where the
    // metadata is the only carrier. What it gives up is that a path rewrite by
    // an intermediary stops being detectable -- taken knowingly.
    RoutingServer server;
    auto reply = post(server.port(), std::string("/vgi/") + kProtocol + "/echo",
                      request_body("echo", std::nullopt));
    CHECK(reply.status == 200);
}

TEST_CASE("unhosted protocol and absent method are different answers", "[http-routing]") {
    // A client probing for an optional method depends on telling "I do not
    // speak that protocol" from "I speak it but not that method".
    RoutingServer server;
    auto unhosted = post(server.port(), "/vgi/NotHosted/echo", request_body("echo", "NotHosted"));
    CHECK(unhosted.status == 404);
    CHECK(has_error_kind(unhosted.body, ERROR_KIND_PROTOCOL_NOT_SUPPORTED));
    // Nor does *this* branch echo the name it was given. Both routing refusals
    // are request-supplied-string-free, which is the property the grammar check
    // exists to make safe to rely on.
    CHECK(unhosted.body.find("NotHosted") == std::string::npos);

    auto absent = post(server.port(), std::string("/vgi/") + kProtocol + "/nope",
                       request_body("nope", kProtocol));
    CHECK(absent.status == 404);
    CHECK(has_error_kind(absent.body, ERROR_KIND_METHOD_NOT_IMPLEMENTED));
}

TEST_CASE("reflection is reached like any other protocol", "[http-routing]") {
    // Not a special path: `{prefix}/vgi_rpc.Reflection.v1/{method}`, by the
    // same routing key as everything else.
    RoutingServer server;
    auto listed =
        post(server.port(), std::string("/vgi/") + kReflectionProtocolName + "/list_protocols",
             request_body("list_protocols", kReflectionProtocolName));
    CHECK(listed.status == 200);
    CHECK(listed.body.find(kProtocol) != std::string::npos);

    auto absent = post(server.port(), std::string("/vgi/") + kReflectionProtocolName + "/nope",
                       request_body("nope", kReflectionProtocolName));
    CHECK(absent.status == 404);
    CHECK(has_error_kind(absent.body, ERROR_KIND_METHOD_NOT_IMPLEMENTED));
}

TEST_CASE("a reserved name is not a protocol's method", "[http-routing]") {
    // `__name__` methods are server-level surface owned by no protocol, so they
    // resolve flat and nowhere else.  Under a protocol the answer is
    // `method_not_implemented` and not "no route": the protocol *is* hosted,
    // and a capability probe depends on being told which of the two it hit.
    RoutingServer server;
    auto flat =
        post(server.port(), "/vgi/__transport_options__",
             request_body(TRANSPORT_OPTIONS_METHOD_NAME, std::nullopt, /*empty_params=*/true));
    CHECK(flat.status == 200);

    auto namespaced =
        post(server.port(), std::string("/vgi/") + kProtocol + "/__transport_options__",
             request_body(TRANSPORT_OPTIONS_METHOD_NAME, kProtocol, /*empty_params=*/true));
    CHECK(namespaced.status == 404);
    CHECK(has_error_kind(namespaced.body, ERROR_KIND_METHOD_NOT_IMPLEMENTED));
}

TEST_CASE("__describe__ is refused with the name of its replacement", "[http-routing]") {
    // Over HTTP as on the raw transports: a stale client told only "unknown
    // method" cannot tell "retired" from "this server was built without
    // introspection", and the two need opposite fixes.  Both shapes answer the
    // same way, because a stale client may have either address baked in.
    RoutingServer server;
    for (const std::string& path :
         {std::string("/vgi/__describe__"), std::string("/vgi/") + kProtocol + "/__describe__"}) {
        auto res = post(server.port(), path,
                        request_body("__describe__", std::nullopt, /*empty_params=*/true));
        CHECK(res.status == 404);
        CHECK(has_error_kind(res.body, ERROR_KIND_METHOD_NOT_IMPLEMENTED));
        CHECK(res.body.find(kReflectionProtocolName) != std::string::npos);
        CHECK(res.body.find("list_protocols") != std::string::npos);
    }
}

TEST_CASE("the retired __introspect_token__ route is not served", "[http-routing]") {
    // Introspection is the `vgi_rpc.Identity.v1` protocol.  The pre-0.46 HTTP
    // JSON route it replaced is retired (IDENTITY_V1_SPEC §8): a second surface
    // is a second set of guards to keep identical, and it had already drifted.
    // So the name gets no bespoke handler -- it is an unknown reserved method,
    // answered in Arrow like any other, and never the route's JSON.
    RoutingServer server;
    auto res = post(server.port(), "/vgi/__introspect_token__",
                    request_body("__introspect_token__", std::nullopt, /*empty_params=*/true));
    CHECK(res.status == 404);
    CHECK(has_error_kind(res.body, ERROR_KIND_METHOD_NOT_IMPLEMENTED));

    // The JSON body the route used to accept is refused before any dispatch.
    httplib::Client client("127.0.0.1", server.port());
    client.set_read_timeout(10, 0);
    for (const std::string& path :
         {std::string("/vgi/__introspect_token__"), std::string("/__introspect_token__")}) {
        auto json = client.Post(path, R"({"token":"anything"})", "application/json");
        REQUIRE(json);
        CHECK(json->status != 200);
        CHECK(json->body.find("principal") == std::string::npos);
    }

    // Nor does capability discovery advertise it.
    auto health = client.Get("/vgi/health");
    REQUIRE(health);
    CHECK_FALSE(health->has_header("VGI-Token-Introspection"));
}
