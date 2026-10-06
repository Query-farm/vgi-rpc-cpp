// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The error model's local rules (WIRE_PROTOCOL.md §8): the details cap, the
// catalog rules, client-side tolerance, retryability, and the hosting API's
// construction-time refusals.  Wire behaviour is the shared conformance
// suite's job; these pin the vectors in MULTI_PROTOCOL_HOSTING.md §3 that a
// conformance run cannot reach.

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/errors.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/token_identity.h"

#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>

#include "vgi_rpc/wire.h"

#include <string>

using namespace vgi_rpc;
using json = nlohmann::json;

namespace {

json padded(size_t n) {
    return json::array(
        {json{{"@type", "vgi_rpc.ErrorInfo"}, {"metadata", {{"p", std::string(n, 'x')}}}}});
}

}  // namespace

TEST_CASE("V5: the cap is 4096 UTF-8 bytes and drops the whole array", "[errors]") {
    REQUIRE(encode_error_details(padded(4045)).has_value());
    REQUIRE(encode_error_details(padded(4045))->size() == 4096);
    REQUIRE_FALSE(encode_error_details(padded(4046)).has_value());

    // Under the cap in characters, over it in bytes.
    std::string e_acute;
    for (int i = 0; i < 2048; ++i) e_acute += "\xc3\xa9";
    json localized = json::array({LocalizedMessage{"fr", e_acute}.to_json()});
    REQUIRE_FALSE(encode_error_details(localized).has_value());
}

TEST_CASE("V6: arrays breaking the catalog rules are dropped at emission", "[errors]") {
    const auto retry = RetryInfo{1}.to_json();
    REQUIRE_FALSE(encode_error_details(json::array({retry, retry})).has_value());
    REQUIRE_FALSE(encode_error_details(json::array({json{{"@type", "vgi_rpc.Made.Up"}}})));
    REQUIRE_FALSE(encode_error_details(json::array({json{{"@type", "Unqualified"}}})));
    REQUIRE_FALSE(encode_error_details(json::array({json{{"note", "no type"}}})));
    REQUIRE(encode_error_details(json::array({json{{"@type", "my.Proto.Detail"}}})));
    REQUIRE_THROWS_AS(StatusError("x", Code::INTERNAL, "", json::array({retry, retry})),
                      std::invalid_argument);
}

TEST_CASE("V7: clients tolerate malformed details", "[errors]") {
    REQUIRE(decode_error_details("{\"not\":\"an array\"}").empty());
    const auto details = decode_error_details(
        R"([1, {"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":"soon"},
            {"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":-1},
            {"@type":"vgi_rpc.ErrorInfo","metadata":{"k":1}},
            {"@type":"x.Unknown"}])");
    REQUIRE(details.size() == 4);  // the bare number is not an object
    RemoteStatus status("UNAVAILABLE", "", details);
    REQUIRE(status.details().empty());
    REQUIRE_FALSE(status.retry_info().has_value());
    REQUIRE(status.error_details().size() == 4);
}

TEST_CASE("V3: retryability follows the code", "[errors]") {
    const auto with_retry = json::array({RetryInfo{3}.to_json()});
    REQUIRE(is_retryable("UNAVAILABLE", json::array()));
    REQUIRE(is_retryable("RESOURCE_EXHAUSTED", with_retry));
    REQUIRE_FALSE(is_retryable("RESOURCE_EXHAUSTED", json::array()));
    REQUIRE_FALSE(is_retryable("ABORTED", with_retry));
    REQUIRE_FALSE(is_retryable("INTERNAL", with_retry));
    REQUIRE_FALSE(is_retryable("", json::array()));
    REQUIRE_FALSE(is_retryable("SOMETHING", json::array()));
}

TEST_CASE("every EXCEPTION batch carries a code, mirrored in log_extra", "[errors]") {
    auto md = make_error_metadata("ValueError", "boom");
    REQUIRE(get_metadata_value(md, keys::ERROR_CODE) == "UNKNOWN");
    REQUIRE(md->FindKey(keys::ERROR_KIND) < 0);

    auto kinded = make_error_metadata("X", "y", "", "", "identity_unavailable");
    REQUIRE(get_metadata_value(kinded, keys::ERROR_CODE) == "UNAVAILABLE");

    IdentityUnavailableError unavailable("down", 9);
    auto from_exc = make_exception_metadata(unavailable, "", "");
    REQUIRE(get_metadata_value(from_exc, keys::ERROR_CODE) == "UNAVAILABLE");
    REQUIRE(json::parse(get_metadata_value(from_exc, keys::ERROR_DETAILS)) ==
            json::array({RetryInfo{9}.to_json()}));
    const auto extra = json::parse(get_metadata_value(from_exc, keys::LOG_EXTRA));
    REQUIRE(extra["error_code"] == "UNAVAILABLE");
    REQUIRE(extra["error_kind"] == "identity_unavailable");
    REQUIRE(extra["error_details"].is_array());
    REQUIRE_FALSE(extra.contains("traceback"));
}

TEST_CASE("identity hooks: the transport-auth error is translated, hint kept", "[errors]") {
    IdentityOptions options;
    options.resolve_token = [](const std::string&) -> std::optional<TokenIdentity> {
        throw AuthUnavailableError("authority down", 7);
    };
    options.introspect_principals = {"asker"};
    IdentityImpl impl(std::move(options));
    AuthContext auth = AuthContext::anonymous();
    auth.authenticated = true;
    auth.principal = "asker";
    try {
        impl.introspect_token("opaque", auth);
        FAIL("expected identity_unavailable");
    } catch (const IdentityUnavailableError& e) {
        REQUIRE(e.retry_after() == 7);
        REQUIRE(e.kind() == "identity_unavailable");
    }
}

TEST_CASE("hosting: reserved, repeated and orphaned protocol names are refused", "[errors]") {
    auto params = arrow::schema({});
    auto noop = [](const Request&, CallContext&) {};
    {
        ServerBuilder builder;
        builder.protocol("vgi_rpc.Reflection.v1");
        REQUIRE_THROWS_AS(builder.build(), std::invalid_argument);
    }
    {
        ServerBuilder builder;
        builder.protocol("Primary.v1");
        builder.add_protocol(
            std::move(ProtocolBuilder("vgi_rpc.Sneaky.v1").add_void("m", params, noop)));
        REQUIRE_THROWS_AS(builder.build(), std::invalid_argument);
    }
    {
        ServerBuilder builder;
        builder.protocol("Primary.v1");
        builder.add_protocol(ProtocolBuilder("Primary.v1"));
        REQUIRE_THROWS_AS(builder.build(), std::invalid_argument);
    }
    {
        ServerBuilder builder;  // no primary name: nothing to route extras by
        builder.add_protocol(ProtocolBuilder("Extra.v1"));
        REQUIRE_THROWS_AS(builder.build(), std::invalid_argument);
    }
    {
        ServerBuilder builder;
        builder.protocol("Primary.v1");
        builder.add_protocol(ProtocolBuilder("Extra.v1"));
        auto server = builder.build();
        REQUIRE(server->application_protocols().size() == 2);
        REQUIRE(server->application_protocols()[1].name == "Extra.v1");
        REQUIRE(server->include_tracebacks());
    }
}

TEST_CASE("hosting: registration after build() fails loudly", "[errors]") {
    auto params = arrow::schema(arrow::FieldVector{});
    ServerBuilder builder;
    builder.protocol("Primary.v1");
    auto server = builder.build();
    REQUIRE_THROWS_AS(builder.add_protocol(ProtocolBuilder("Late.v1")), std::logic_error);
    REQUIRE_THROWS_AS(builder.add_void("late", params, [](const Request&, CallContext&) {}),
                      std::logic_error);
    REQUIRE_THROWS_AS(builder.protocol("Other.v1"), std::logic_error);
    REQUIRE(server->application_protocols().size() == 1);
}

namespace {

// One unary request for `method` on `protocol`, served over the pipe
// transport; returns the decoded `log_extra` of the EXCEPTION batch.
json exception_extra(Server& server, const std::string& protocol, const std::string& method) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, method);
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    if (!protocol.empty()) md->Append(keys::PROTOCOL, protocol);
    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(empty_schema());
    ab.custom_metadata = md;
    write_ipc_stream(sink, empty_schema(), {ab});
    auto in = std::make_shared<arrow::io::BufferReader>(sink->Finish().ValueUnsafe());
    auto out = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    REQUIRE(server.serve_one(in, out));
    auto reply =
        read_ipc_stream(std::make_shared<arrow::io::BufferReader>(out->Finish().ValueUnsafe()));
    REQUIRE(reply.has_value());
    for (const auto& batch : reply->batches) {
        if (classify_batch(batch) == BatchType::EXCEPTION) {
            return json::parse(get_metadata_value(batch.custom_metadata, keys::LOG_EXTRA));
        }
    }
    FAIL("no EXCEPTION batch");
    return json();
}

std::unique_ptr<Server> raising_server(std::optional<bool> include) {
    ServerBuilder builder;
    builder.protocol("Primary.v1");
    builder.add_void("boom", arrow::schema(arrow::FieldVector{}),
                     [](const Request&, CallContext&) { throw std::runtime_error("kaboom"); });
    if (include) builder.include_tracebacks(*include);
    return builder.build();
}

}  // namespace

TEST_CASE("tracebacks: on by default, synthesized, and one switch turns them off", "[errors]") {
    {
        auto server = raising_server(std::nullopt);
        // A raising handler: names the binding and method, and the error.
        const auto raised = exception_extra(*server, "Primary.v1", "boom");
        const auto trace = raised.value("traceback", "");
        REQUIRE(trace.find("Primary.v1/boom") != std::string::npos);
        REQUIRE(trace.find("RuntimeError: kaboom") != std::string::npos);
        // A framework refusal is an EXCEPTION batch too, and carries one.
        const auto refused = exception_extra(*server, "Primary.v1", "no_such_method");
        REQUIRE_FALSE(refused.value("traceback", "").empty());
        REQUIRE(refused.value("error_code", "") == "UNIMPLEMENTED");
    }
    {
        auto server = raising_server(false);
        const auto raised = exception_extra(*server, "Primary.v1", "boom");
        REQUIRE_FALSE(raised.contains("traceback"));
        REQUIRE(raised.value("error_code", "") == "UNKNOWN");
        REQUIRE_FALSE(exception_extra(*server, "Unhosted.v1", "boom").contains("traceback"));
    }
}
