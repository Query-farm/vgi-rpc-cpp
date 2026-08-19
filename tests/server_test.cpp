// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <atomic>
#include <future>
#include <thread>
#include <vector>

using namespace vgi_rpc;

// ── Helpers ──────────────────────────────────────────────────────────

namespace {

// Write a request IPC stream into a buffer.
std::shared_ptr<arrow::Buffer> make_request_buffer(
    const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::KeyValueMetadata>& md) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    AnnotatedBatch ab;
    ab.batch = batch;
    ab.custom_metadata = md;
    write_ipc_stream(sink, schema, {ab});
    return sink->Finish().ValueUnsafe();
}

// Build a valid request buffer with method + version metadata.
std::shared_ptr<arrow::Buffer> make_valid_request(
    const std::string& method, const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, method);
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    return make_request_buffer(schema, batch, md);
}

// Read the response from a buffer and return the IPC contents.
IpcStreamContents read_response(const std::shared_ptr<arrow::Buffer>& buf) {
    auto source = std::make_shared<arrow::io::BufferReader>(buf);
    auto result = read_ipc_stream(source);
    REQUIRE(result.has_value());
    return std::move(*result);
}

// Extract exception_type from an ERROR batch's metadata.
std::string get_error_type(const IpcStreamContents& contents) {
    for (const auto& ab : contents.batches) {
        if (classify_batch(ab) == BatchType::EXCEPTION) {
            auto extra_idx = ab.custom_metadata->FindKey(keys::LOG_EXTRA);
            if (extra_idx >= 0) {
                auto extra = nlohmann::json::parse(ab.custom_metadata->value(extra_idx));
                return extra.value("exception_type", "");
            }
        }
    }
    return "";
}

// Build a simple echo server for testing.
std::unique_ptr<Server> make_echo_server() {
    auto schema = arrow::schema({arrow::field("value", arrow::utf8())});

    ServerBuilder builder;
    builder.add_unary("echo", schema, schema, [](const Request& req, CallContext&) -> Result {
        auto val = req.get<std::string>("value");
        arrow::StringBuilder sb;
        REQUIRE(sb.Append(val).ok());
        auto arr = *sb.Finish();
        return Result::value(req.schema(), {arr});
    });
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    return builder.build();
}

// Run a single request through a server and return the response buffer.
std::shared_ptr<arrow::Buffer> run_request(Server& server,
                                           const std::shared_ptr<arrow::Buffer>& request_buf) {
    auto input = std::make_shared<arrow::io::BufferReader>(request_buf);
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    server.serve_one(input, output);
    return output->Finish().ValueUnsafe();
}

}  // anonymous namespace

// ── Protocol Error Tests ─────────────────────────────────────────────

TEST_CASE("serve_one: missing method metadata -> ProtocolError", "[server]") {
    auto server = make_echo_server();
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    // No METHOD key
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    auto request_buf = make_request_buffer(schema, batch, md);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "ProtocolError");
}

TEST_CASE("serve_one: missing version metadata -> VersionError", "[server]") {
    auto server = make_echo_server();
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "echo");
    // No VERSION key
    auto request_buf = make_request_buffer(schema, batch, md);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "VersionError");
}

TEST_CASE("serve_one: wrong version -> VersionError", "[server]") {
    auto server = make_echo_server();
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "echo");
    md->Append(keys::REQUEST_VERSION, "999");
    auto request_buf = make_request_buffer(schema, batch, md);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "VersionError");
}

TEST_CASE("serve_one: unknown method -> AttributeError", "[server]") {
    auto server = make_echo_server();
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto request_buf = make_valid_request("nonexistent", schema, batch);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "AttributeError");
}

// ── Exception Mapping Tests ──────────────────────────────────────────

TEST_CASE("serve_one: handler throws invalid_argument -> ValueError", "[server]") {
    auto schema = empty_schema();
    ServerBuilder builder;
    builder.add_unary("throw_ia", schema, schema, [](const Request&, CallContext&) -> Result {
        throw std::invalid_argument("bad arg");
    });
    auto server = builder.build();
    auto request_buf = make_valid_request("throw_ia", schema, make_empty_batch(schema));

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "ValueError");
}

TEST_CASE("serve_one: handler throws out_of_range -> IndexError", "[server]") {
    auto schema = empty_schema();
    ServerBuilder builder;
    builder.add_unary("throw_oor", schema, schema, [](const Request&, CallContext&) -> Result {
        throw std::out_of_range("index out");
    });
    auto server = builder.build();
    auto request_buf = make_valid_request("throw_oor", schema, make_empty_batch(schema));

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "IndexError");
}

TEST_CASE("serve_one: handler throws logic_error -> TypeError", "[server]") {
    auto schema = empty_schema();
    ServerBuilder builder;
    builder.add_unary("throw_le", schema, schema, [](const Request&, CallContext&) -> Result {
        throw std::logic_error("type problem");
    });
    auto server = builder.build();
    auto request_buf = make_valid_request("throw_le", schema, make_empty_batch(schema));

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "TypeError");
}

TEST_CASE("serve_one: handler throws exception -> RuntimeError", "[server]") {
    auto schema = empty_schema();
    ServerBuilder builder;
    builder.add_unary("throw_re", schema, schema, [](const Request&, CallContext&) -> Result {
        throw std::runtime_error("runtime issue");
    });
    auto server = builder.build();
    auto request_buf = make_valid_request("throw_re", schema, make_empty_batch(schema));

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    REQUIRE(get_error_type(contents) == "RuntimeError");
}

// ── Successful Round-trip Tests ──────────────────────────────────────

TEST_CASE("serve_one: successful unary echo round-trip", "[server]") {
    auto server = make_echo_server();
    auto schema = arrow::schema({arrow::field("value", arrow::utf8())});
    arrow::StringBuilder sb;
    REQUIRE(sb.Append("hello").ok());
    auto arr = *sb.Finish();
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto request_buf = make_valid_request("echo", schema, batch);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);

    // Find data batch
    bool found_data = false;
    for (const auto& ab : contents.batches) {
        if (classify_batch(ab) == BatchType::DATA && ab.batch->num_rows() > 0) {
            auto col =
                std::dynamic_pointer_cast<arrow::StringArray>(ab.batch->GetColumnByName("value"));
            REQUIRE(col);
            REQUIRE(col->GetString(0) == "hello");
            found_data = true;
        }
    }
    REQUIRE(found_data);
}

TEST_CASE("serve_one: CallContext reports the explicit transport kind", "[server]") {
    TransportKind observed = TransportKind::PIPE;
    ServerBuilder builder;
    builder.add_void("observe", empty_schema(),
                     [&](const Request&, CallContext& ctx) { observed = ctx.transport_kind(); });
    auto server = builder.build();
    auto request_buf =
        make_valid_request("observe", empty_schema(), make_empty_batch(empty_schema()));
    auto input = std::make_shared<arrow::io::BufferReader>(request_buf);
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();

    REQUIRE(server->serve_one(input, output, TransportKind::TCP));
    REQUIRE(observed == TransportKind::TCP);
}

TEST_CASE("on_serve_start serializes concurrent callers and fires once after success",
          "[server][lifecycle]") {
    std::atomic<int> attempts = 0;
    std::atomic<TransportKind> observed_kind = TransportKind::PIPE;
    std::promise<void> entered_promise;
    auto entered = entered_promise.get_future();
    std::promise<void> release_promise;
    auto release = release_promise.get_future().share();

    ServerBuilder builder;
    builder.on_serve_start([&](TransportKind kind) {
        observed_kind = kind;
        ++attempts;
        entered_promise.set_value();
        release.wait();
    });
    auto server = builder.build();

    std::vector<std::future<void>> notifications;
    for (int i = 0; i < 8; ++i) {
        notifications.push_back(std::async(
            std::launch::async, [&]() { server->notify_serve_start(TransportKind::HTTP); }));
    }

    REQUIRE(entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    REQUIRE(attempts == 1);
    release_promise.set_value();
    for (auto& notification : notifications) notification.get();

    server->notify_serve_start(TransportKind::HTTP);
    REQUIRE(attempts == 1);
    REQUIRE(observed_kind == TransportKind::HTTP);
}

TEST_CASE("on_serve_start retries after a synchronous failure", "[server][lifecycle]") {
    int attempts = 0;
    ServerBuilder builder;
    builder.on_serve_start([&](TransportKind) {
        if (attempts++ == 0) throw std::runtime_error("transient startup failure");
    });
    auto server = builder.build();

    REQUIRE_THROWS_AS(server->notify_serve_start(TransportKind::HTTP), std::runtime_error);
    REQUIRE_NOTHROW(server->notify_serve_start(TransportKind::HTTP));
    REQUIRE_NOTHROW(server->notify_serve_start(TransportKind::HTTP));
    REQUIRE(attempts == 2);
}

TEST_CASE("serve_one: void handler round-trip", "[server]") {
    auto server = make_echo_server();
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto request_buf = make_valid_request("noop", schema, batch);

    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);
    // Should have at least one DATA batch (void result)
    bool found = false;
    for (const auto& ab : contents.batches) {
        if (classify_batch(ab) == BatchType::DATA) {
            found = true;
        }
    }
    REQUIRE(found);
}

// ── EOF Test ─────────────────────────────────────────────────────────

TEST_CASE("serve_one: EOF (empty input) returns false", "[server]") {
    auto server = make_echo_server();
    auto empty_buf = std::make_shared<arrow::Buffer>("");
    auto input = std::make_shared<arrow::io::BufferReader>(empty_buf);
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    REQUIRE_FALSE(server->serve_one(input, output));
}

// ── ServerBuilder Guard Tests ────────────────────────────────────────

TEST_CASE("ServerBuilder: double build() throws", "[server]") {
    ServerBuilder builder;
    builder.add_void("test", empty_schema(), [](const Request&, CallContext&) {});
    auto server = builder.build();
    REQUIRE(server != nullptr);
    REQUIRE_THROWS_AS(builder.build(), std::logic_error);
}

TEST_CASE("ServerBuilder: duplicate method name throws", "[server]") {
    ServerBuilder builder;
    builder.add_void("dup", empty_schema(), [](const Request&, CallContext&) {});
    REQUIRE_THROWS_AS(builder.add_void("dup", empty_schema(), [](const Request&, CallContext&) {}),
                      std::logic_error);
}

// ── ServerBuilder::server_id() Test ──────────────────────────────────

TEST_CASE("ServerBuilder: custom server_id is used", "[server]") {
    ServerBuilder builder;
    builder.server_id("my-test-id");
    builder.add_void("test", empty_schema(), [](const Request&, CallContext&) {});
    auto server = builder.build();
    REQUIRE(server->server_id() == "my-test-id");
}

TEST_CASE("ServerBuilder: default server_id is random", "[server]") {
    ServerBuilder builder;
    builder.add_void("test", empty_schema(), [](const Request&, CallContext&) {});
    auto server = builder.build();
    REQUIRE_FALSE(server->server_id().empty());
}

// ── Null Schema Validation Tests ─────────────────────────────────────

TEST_CASE("ServerBuilder: null params_schema in add_unary throws", "[server]") {
    ServerBuilder builder;
    REQUIRE_THROWS_AS(builder.add_unary("bad", nullptr, empty_schema(),
                                        [](const Request&, CallContext&) -> Result {
                                            return Result::void_result();
                                        }),
                      std::invalid_argument);
}

TEST_CASE("ServerBuilder: null result_schema in add_unary throws", "[server]") {
    ServerBuilder builder;
    REQUIRE_THROWS_AS(builder.add_unary("bad", empty_schema(), nullptr,
                                        [](const Request&, CallContext&) -> Result {
                                            return Result::void_result();
                                        }),
                      std::invalid_argument);
}

TEST_CASE("ServerBuilder: null params_schema in add_void throws", "[server]") {
    ServerBuilder builder;
    REQUIRE_THROWS_AS(builder.add_void("bad", nullptr, [](const Request&, CallContext&) {}),
                      std::invalid_argument);
}

TEST_CASE("ServerBuilder: null params_schema in add_producer throws", "[server]") {
    auto schema = arrow::schema({arrow::field("x", arrow::int32())});
    ServerBuilder builder;
    REQUIRE_THROWS_AS(
        builder.add_producer("bad", nullptr, schema,
                             [](const Request&, CallContext&) -> Stream { return {}; }),
        std::invalid_argument);
}

TEST_CASE("ServerBuilder: null output_schema in add_producer throws", "[server]") {
    ServerBuilder builder;
    REQUIRE_THROWS_AS(
        builder.add_producer("bad", empty_schema(), nullptr,
                             [](const Request&, CallContext&) -> Stream { return {}; }),
        std::invalid_argument);
}

TEST_CASE("ServerBuilder: null schemas in add_exchange throw", "[server]") {
    auto schema = arrow::schema({arrow::field("x", arrow::int32())});
    ServerBuilder builder;
    REQUIRE_THROWS_AS(
        builder.add_exchange("bad", nullptr, schema, schema,
                             [](const Request&, CallContext&) -> Stream { return {}; }),
        std::invalid_argument);
    REQUIRE_THROWS_AS(
        builder.add_exchange("bad", schema, nullptr, schema,
                             [](const Request&, CallContext&) -> Stream { return {}; }),
        std::invalid_argument);
    REQUIRE_THROWS_AS(
        builder.add_exchange("bad", schema, schema, nullptr,
                             [](const Request&, CallContext&) -> Stream { return {}; }),
        std::invalid_argument);
}

// ── Describe Introspection Tests ─────────────────────────────────────

TEST_CASE("describe: lists registered methods", "[server][describe]") {
    auto schema = arrow::schema({arrow::field("value", arrow::utf8())});

    ServerBuilder builder;
    builder.add_unary("echo", schema, schema, [](const Request& req, CallContext&) -> Result {
        return Result::void_result();
    });
    builder.add_void("noop", empty_schema(), [](const Request&, CallContext&) {});
    builder.add_producer("produce", empty_schema(), schema,
                         [](const Request&, CallContext&) -> Stream { return {}; });
    builder.enable_describe("test_protocol");
    auto server = builder.build();

    // Call __describe__
    auto request_buf =
        make_valid_request(DESCRIBE_METHOD_NAME, empty_schema(), make_empty_batch(empty_schema()));
    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);

    // Find the data batch with method information
    bool found_data = false;
    for (const auto& ab : contents.batches) {
        if (classify_batch(ab) == BatchType::DATA && ab.batch->num_rows() > 0) {
            found_data = true;
            auto name_col = ab.batch->GetColumnByName("name");
            REQUIRE(name_col != nullptr);

            // Collect all method names
            auto str_arr = std::dynamic_pointer_cast<arrow::StringArray>(name_col);
            REQUIRE(str_arr != nullptr);
            std::vector<std::string> methods;
            for (int64_t i = 0; i < str_arr->length(); ++i) {
                methods.push_back(str_arr->GetString(i));
            }

            // Should contain echo, noop, produce
            REQUIRE(std::find(methods.begin(), methods.end(), "echo") != methods.end());
            REQUIRE(std::find(methods.begin(), methods.end(), "noop") != methods.end());
            REQUIRE(std::find(methods.begin(), methods.end(), "produce") != methods.end());

            // __describe__ itself should NOT be listed
            REQUIRE(std::find(methods.begin(), methods.end(), DESCRIBE_METHOD_NAME) ==
                    methods.end());

            // Check protocol_name in custom_metadata
            if (ab.custom_metadata) {
                auto proto_idx = ab.custom_metadata->FindKey(keys::PROTOCOL_NAME);
                if (proto_idx >= 0) {
                    REQUIRE(ab.custom_metadata->value(proto_idx) == "test_protocol");
                }
            }
        }
    }
    REQUIRE(found_data);
}

TEST_CASE("describe: method types are correct", "[server][describe]") {
    auto schema = arrow::schema({arrow::field("value", arrow::utf8())});

    ServerBuilder builder;
    builder.add_unary("my_unary", schema, schema,
                      [](const Request&, CallContext&) -> Result { return Result::void_result(); });
    builder.add_producer("my_stream", empty_schema(), schema,
                         [](const Request&, CallContext&) -> Stream { return {}; });
    builder.enable_describe();
    auto server = builder.build();

    auto request_buf =
        make_valid_request(DESCRIBE_METHOD_NAME, empty_schema(), make_empty_batch(empty_schema()));
    auto response_buf = run_request(*server, request_buf);
    auto contents = read_response(response_buf);

    for (const auto& ab : contents.batches) {
        if (classify_batch(ab) == BatchType::DATA && ab.batch->num_rows() > 0) {
            auto name_col =
                std::dynamic_pointer_cast<arrow::StringArray>(ab.batch->GetColumnByName("name"));
            auto type_col = std::dynamic_pointer_cast<arrow::StringArray>(
                ab.batch->GetColumnByName("method_type"));
            REQUIRE(name_col != nullptr);
            REQUIRE(type_col != nullptr);

            for (int64_t i = 0; i < name_col->length(); ++i) {
                auto name = name_col->GetString(i);
                auto type = type_col->GetString(i);
                if (name == "my_unary") REQUIRE(type == "unary");
                if (name == "my_stream") REQUIRE(type == "stream");
            }
        }
    }
}
