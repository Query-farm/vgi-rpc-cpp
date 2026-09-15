// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Reflection.v1` describes itself.
//
// This port used to register reflection's binding without registering its
// methods into it, so `describe("vgi_rpc.Reflection.v1")` returned an empty
// method list and the binding hashed to `fafffd66…` rather than the reference's
// `3c7db4ca…`.  A client discovers a server the documented way -- `list_protocols`,
// then `describe` for each protocol it cares about -- and that sequence
// advertised reflection and then said it had no methods, so a client could not
// learn how to call the protocol it was already calling.
//
// It survived because the port was internally *self-consistent*: `list_protocols`
// advertised the same digest its own `describe` returned, so nothing local could
// see it and only a port-to-port comparison could.  Hence this file: the digest
// is pinned against the cross-port reference, and so is the two-method shape it
// is a fingerprint of -- either alone would let the other drift.

#include "vgi_rpc/reflection.h"

#include "vgi_rpc/client_description.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/protocol_hash.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/wire.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/key_value_metadata.h>
#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>

using namespace vgi_rpc;

namespace {

/// The cross-port digest of `vgi_rpc.Reflection.v1`, from the Python reference.
constexpr const char* kReflectionHash =
    "3c7db4cae8cdfc93dc4a76e73b8b759e18e45e6a5811adba4e520366344b919a";

constexpr const char* kApplicationProtocol = "ReflectionProbe";

std::vector<HashMethod> hash_entries(const std::unordered_map<std::string, MethodInfo>& methods) {
    std::vector<HashMethod> entries;
    for (const auto& [name, info] : methods) {
        entries.push_back(HashMethod{name, "unary", UnaryHasReturn(info),
                                     info.header_schema != nullptr, info.params_schema,
                                     info.result_schema, info.header_schema});
    }
    return entries;
}

/// A server hosting an ordinary application protocol alongside reflection.
std::unique_ptr<Server> make_server() {
    ServerBuilder builder;
    builder.protocol(kApplicationProtocol);
    builder.add_unary("echo",
                      arrow::schema({arrow::field("value", arrow::utf8(), /*nullable=*/false)}),
                      arrow::schema({arrow::field("result", arrow::utf8(), /*nullable=*/false)}),
                      [](const Request&, CallContext&) { return Result::void_result(); });
    return builder.build();
}

/// Drive one reflection call through the pipe transport and return the reply.
AnnotatedBatch call_reflection(Server& server, const std::string& method,
                               const std::shared_ptr<arrow::RecordBatch>& params) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::METHOD, method);
    metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    metadata->Append(keys::PROTOCOL, kReflectionProtocolName);

    auto request_sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    AnnotatedBatch annotated;
    annotated.batch = params;
    annotated.custom_metadata = metadata;
    write_ipc_stream(request_sink, params->schema(), {annotated});

    auto input = std::make_shared<arrow::io::BufferReader>(request_sink->Finish().ValueUnsafe());
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    REQUIRE(server.serve_one(input, output));

    auto reply = std::make_shared<arrow::io::BufferReader>(output->Finish().ValueUnsafe());
    auto contents = read_ipc_stream(reply);
    REQUIRE(contents.has_value());
    REQUIRE(contents->batches.size() == 1);
    return contents->batches[0];
}

/// A one-row `{protocol: <name>}` params batch.
std::shared_ptr<arrow::RecordBatch> describe_params(const std::string& protocol) {
    arrow::StringBuilder builder;
    REQUIRE(builder.Append(protocol).ok());
    std::shared_ptr<arrow::Array> column;
    REQUIRE(builder.Finish(&column).ok());
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("protocol", arrow::utf8(), /*nullable=*/false)}), 1, {column});
}

}  // namespace

// ── The cross-port hash vector ────────────────────────────────────────

TEST_CASE("reflection protocol hash matches the reference", "[reflection]") {
    auto hash = BindingHash(kReflectionProtocolName, ReflectionMethods());
    REQUIRE(hash.ok());
    CHECK(*hash == kReflectionHash);
}

// The canonical preimage, so a digest mismatch is a JSON diff rather than a
// guess.  Every field of the shape is visible here: both methods unary, both
// returning the framework's single `result: binary` column, `describe` taking
// one `utf8` parameter and `list_protocols` taking none, and neither carrying a
// header.
TEST_CASE("reflection canonical description spells both methods", "[reflection]") {
    auto description =
        CanonicalDescription(kReflectionProtocolName, hash_entries(ReflectionMethods()));
    REQUIRE(description.ok());
    CHECK(*description ==
          R"({"methods":[{"has_header":false,"has_return":true,"name":"describe",)"
          R"("params":[{"name":"protocol","nullable":false,"type":"utf8"}],)"
          R"("result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"},)"
          R"({"has_header":false,"has_return":true,"name":"list_protocols","params":[],)"
          R"("result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"}],)"
          R"("protocol":"vgi_rpc.Reflection.v1"})");
}

// ── The shape the digest is a fingerprint of ──────────────────────────

TEST_CASE("reflection hosts exactly the two methods it answers", "[reflection]") {
    // Pinned separately from the digest: a hash vector alone would still pass
    // if the table and the dispatcher drifted apart together, and the failure
    // this file exists to prevent is precisely a table that does not match what
    // the protocol answers.
    auto methods = ReflectionMethods();
    CHECK(methods.size() == 2);
    REQUIRE(methods.count("describe") == 1);
    REQUIRE(methods.count("list_protocols") == 1);

    for (const auto& name : {"describe", "list_protocols"}) {
        const auto& info = methods.at(name);
        CHECK(info.method_type == MethodType::UNARY);
        CHECK(UnaryHasReturn(info));
        CHECK(StreamKindFor(info) == "");
        CHECK(info.header_schema == nullptr);
        REQUIRE(info.result_schema != nullptr);
        REQUIRE(info.result_schema->num_fields() == 1);
        CHECK(info.result_schema->field(0)->name() == "result");
        CHECK(info.result_schema->field(0)->type()->Equals(arrow::binary()));
        CHECK_FALSE(info.result_schema->field(0)->nullable());
    }

    const auto& describe = methods.at("describe");
    REQUIRE(describe.params_schema != nullptr);
    REQUIRE(describe.params_schema->num_fields() == 1);
    CHECK(describe.params_schema->field(0)->name() == "protocol");
    CHECK(describe.params_schema->field(0)->type()->Equals(arrow::utf8()));
    CHECK_FALSE(describe.params_schema->field(0)->nullable());

    // Present and empty, not absent: the canonical description distinguishes
    // "takes no parameters" from "parameters unstated".
    const auto& list = methods.at("list_protocols");
    REQUIRE(list.params_schema != nullptr);
    CHECK(list.params_schema->num_fields() == 0);
}

// ── End to end, the way a client actually discovers it ────────────────

TEST_CASE("describe reports reflection's own methods over the wire", "[reflection]") {
    // The sequence a client is documented to follow.  Nothing below inspects
    // the method table directly; it reads what the server says about itself.
    auto server = make_server();
    auto described = decode_service_description(
        call_reflection(*server, "describe", describe_params(kReflectionProtocolName)));

    CHECK(described.protocol_name == kReflectionProtocolName);
    CHECK(described.protocol_hash == kReflectionHash);
    REQUIRE(described.methods.size() == 2);

    const auto* describe = described.method("describe");
    REQUIRE(describe != nullptr);
    CHECK(describe->method_type == "unary");
    CHECK(describe->has_return);
    CHECK_FALSE(describe->has_header);
    REQUIRE(describe->params_schema != nullptr);
    CHECK(describe->params_schema->num_fields() == 1);
    CHECK(describe->params_schema->field(0)->name() == "protocol");

    const auto* list = described.method("list_protocols");
    REQUIRE(list != nullptr);
    CHECK(list->method_type == "unary");
    CHECK(list->has_return);
    REQUIRE(list->params_schema != nullptr);
    CHECK(list->params_schema->num_fields() == 0);
}

TEST_CASE("list_protocols advertises the digest describe returns", "[reflection]") {
    // The self-consistency that hid the bug is still required -- it was never
    // the problem.  The problem was that both halves agreed on the wrong value,
    // which is why the vector above pins it and this only pins the agreement.
    auto server = make_server();
    auto listing = decode_protocol_list(
        call_reflection(*server, "list_protocols", make_empty_batch(empty_schema())));

    const ReflectedProtocol* reflection = nullptr;
    for (const auto& protocol : listing.protocols) {
        if (protocol.protocol == kReflectionProtocolName) reflection = &protocol;
    }
    REQUIRE(reflection != nullptr);
    CHECK(reflection->protocol_hash == kReflectionHash);

    auto described = decode_service_description(
        call_reflection(*server, "describe", describe_params(kReflectionProtocolName)));
    CHECK(described.protocol_hash == reflection->protocol_hash);

    // And the application protocol is still its own binding: reflection gaining
    // a method table must not have moved anything else's digest.
    const auto* application = listing.application();
    REQUIRE(application != nullptr);
    CHECK(application->protocol == kApplicationProtocol);
    auto expected = BindingHash(kApplicationProtocol, server->methods());
    REQUIRE(expected.ok());
    CHECK(application->protocol_hash == *expected);
    CHECK(application->protocol_hash != std::string(kReflectionHash));
}
