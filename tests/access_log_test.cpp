// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Every access record names the protocol that owns the method it describes.
//
// docs/access-log-spec.md §3: `protocol` is "the wire name of the protocol that
// owns the dispatched method ... not a server-wide default", and
// `protocol_hash` is that protocol's canonical digest, "the registry key when
// decoding archived records".
//
// This is the one failure in the multi-service change that fails *silently*. A
// record labelled with the server's primary is well-formed, passes the JSON
// schema, and feeds a plausible dashboard, while a consumer keying on
// `protocol_hash` decodes it against the wrong protocol's description. Three
// ports shipped exactly that, and nothing caught it: for an *application*
// method the primary IS the owning binding, so only a call to a secondary
// protocol can tell the two apart. Hence the reflection call below -- that is
// the whole point of this file, not an incidental extra case.

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include "vgi_rpc/metadata.h"
#include "vgi_rpc/reflection.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/wire.h"

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <regex>
#include <string>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kProtocol = "AccessLogProbe";

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::utf8())});
}

/// A one-row `{value: "hi"}` batch, or a zero-column one for a no-arg method.
std::shared_ptr<arrow::RecordBatch> probe_batch(bool empty) {
    if (empty) return make_empty_batch(empty_schema());
    arrow::StringBuilder builder;
    REQUIRE(builder.Append("hi").ok());
    std::shared_ptr<arrow::Array> column;
    REQUIRE(builder.Finish(&column).ok());
    return arrow::RecordBatch::Make(value_schema(), 1, {column});
}

std::shared_ptr<arrow::Buffer> request_buffer(const std::string& method,
                                              const std::string& protocol, bool empty_params) {
    auto batch = probe_batch(empty_params);
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::METHOD, method);
    metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    metadata->Append(keys::PROTOCOL, protocol);

    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    AnnotatedBatch annotated;
    annotated.batch = batch;
    annotated.custom_metadata = metadata;
    write_ipc_stream(sink, batch->schema(), {annotated});
    return sink->Finish().ValueUnsafe();
}

void run(Server& server, const std::string& method, const std::string& protocol,
         bool empty_params) {
    auto input =
        std::make_shared<arrow::io::BufferReader>(request_buffer(method, protocol, empty_params));
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    server.serve_one(input, output);
}

/// A log file that removes itself, so a failing assertion cannot leave the next
/// run reading somebody else's records.
class TempLog {
public:
    TempLog()
        : path_(std::filesystem::temp_directory_path() /
                ("vgi_access_" + std::to_string(counter_++) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
                 ".jsonl")) {}
    ~TempLog() {
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }
    std::string str() const { return path_.string(); }

    std::vector<nlohmann::json> records() const {
        std::vector<nlohmann::json> out;
        std::ifstream in(path_);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            auto record = nlohmann::json::parse(line);
            if (record.value("logger", "") == "vgi_rpc.access") out.push_back(record);
        }
        return out;
    }

private:
    std::filesystem::path path_;
    static inline int counter_ = 0;
};

/// The single record for `method`, or nothing when none was written.
std::optional<nlohmann::json> record_for(const TempLog& log, const std::string& method) {
    for (const auto& record : log.records()) {
        if (record.value("method", "") == method) return std::optional<nlohmann::json>(record);
    }
    return std::nullopt;
}

std::unique_ptr<Server> make_server(const TempLog& log) {
    ServerBuilder builder;
    builder.protocol(kProtocol);
    builder.access_log(log.str());
    builder.add_unary("echo", value_schema(), value_schema(),
                      [](const Request& request, CallContext&) -> Result {
                          arrow::StringBuilder out;
                          REQUIRE(out.Append(request.get<std::string>("value")).ok());
                          return Result::value(request.schema(), {*out.Finish()});
                      });
    return builder.build();
}

}  // namespace

TEST_CASE("an application call is filed under the application protocol", "[access-log]") {
    TempLog log;
    auto server = make_server(log);
    run(*server, "echo", kProtocol, /*empty_params=*/false);

    auto record = record_for(log, "echo");
    REQUIRE(record.has_value());
    CHECK(record->value("protocol", "") == kProtocol);
    // The canonical digest, which is also what `list_protocols` advertises: the
    // hash is only usable as a registry key if the value in the archive is the
    // value a client compared against.
    auto expected = BindingHash(kProtocol, server->methods());
    REQUIRE(expected.ok());
    CHECK(record->value("protocol_hash", "") == *expected);
}

TEST_CASE("a reflection call is filed under vgi_rpc.Reflection.v1", "[access-log]") {
    // The case that can catch a server-wide default, and the only one that can.
    TempLog log;
    auto server = make_server(log);
    run(*server, "list_protocols", kReflectionProtocolName, /*empty_params=*/true);

    auto record = record_for(log, "list_protocols");
    REQUIRE(record.has_value());
    CHECK(record->value("protocol", "") == std::string(kReflectionProtocolName));

    // Reflection hosts no registered methods of its own, so its canonical
    // description is over an empty method set.
    auto expected = BindingHash(kReflectionProtocolName, {});
    REQUIRE(expected.ok());
    CHECK(record->value("protocol_hash", "") == *expected);

    // And the part that matters most: the digest is not the application's. A
    // record naming one protocol while carrying another's hash is worse than
    // either field being wrong alone -- it decodes cleanly against the wrong
    // description, and nothing about it looks wrong.
    auto application = BindingHash(kProtocol, server->methods());
    REQUIRE(application.ok());
    CHECK(record->value("protocol_hash", "") != *application);
}

TEST_CASE("the message line names the owning protocol too", "[access-log]") {
    // `message` is what a human greps. Leaving it on the server-wide default
    // would make a reflection call read as an application call in exactly the
    // place an operator looks first.
    TempLog log;
    auto server = make_server(log);
    run(*server, "list_protocols", kReflectionProtocolName, /*empty_params=*/true);

    auto record = record_for(log, "list_protocols");
    REQUIRE(record.has_value());
    CHECK(record->value("message", "") ==
          std::string(kReflectionProtocolName) + ".list_protocols ok");
}

TEST_CASE("a refused reflection method still names reflection", "[access-log]") {
    // The error path is a separate emit site from the success path, and a
    // record only written correctly when the call succeeds is a record that is
    // wrong exactly when someone is reading the log.
    TempLog log;
    auto server = make_server(log);
    run(*server, "no_such_reflection_method", kReflectionProtocolName, /*empty_params=*/true);

    auto record = record_for(log, "no_such_reflection_method");
    REQUIRE(record.has_value());
    CHECK(record->value("status", "") == "error");
    CHECK(record->value("protocol", "") == std::string(kReflectionProtocolName));
}

// ── The structural guard ─────────────────────────────────────────────
//
// The tests above catch the two emit sites that exist. They cannot catch a
// *seventh* site added later that hardcodes the server's primary -- and that is
// how this bug arrived in the first place: the sites are spread across two
// transports, a stream path and two framework protocols, and each was written
// in isolation from the others.
//
// `AccessRecord` has no default constructor, so a new site must supply both
// identity fields; that much the compiler already enforces. What it cannot
// enforce is that the pair comes from *one* binding. So the shape is asserted
// against the source itself: both arguments must be `.name`/`.hash` of the same
// object, which is the property that makes a record self-consistent.

TEST_CASE("every access record is built from one binding's identity", "[access-log]") {
    static constexpr const char* kSources[] = {
        VGI_RPC_SOURCE_DIR "/src/pipe_transport.cpp",
        VGI_RPC_SOURCE_DIR "/src/http_transport.cpp",
        VGI_RPC_SOURCE_DIR "/src/socket_transport.cpp",
        VGI_RPC_SOURCE_DIR "/src/token_identity.cpp",
        VGI_RPC_SOURCE_DIR "/src/server.cpp",
    };
    // `X.name, X.hash` for one and the same X -- the back-reference is the
    // whole assertion. `X` is a binding member or the parameter one is passed
    // through as; a literal, a server field or a mismatched pair fails.
    const std::regex construction(R"(AccessRecord\s+\w+\s*\(([^;]*?)\))");
    const std::regex identity_pair(R"(^(owner|[a-z_]+_binding_)\.name,\s*\1\.hash$)");

    int found = 0;
    for (const char* path : kSources) {
        std::ifstream in(path);
        REQUIRE(in.good());
        const std::string source((std::istreambuf_iterator<char>(in)),
                                 std::istreambuf_iterator<char>());
        for (auto it = std::sregex_iterator(source.begin(), source.end(), construction);
             it != std::sregex_iterator(); ++it) {
            ++found;
            std::string args = (*it)[1].str();
            // Collapse the line wrapping clang-format may have introduced.
            args = std::regex_replace(args, std::regex(R"(\s+)"), " ");
            INFO("in " << path << ": AccessRecord(" << args << ")");
            CHECK(std::regex_match(args, identity_pair));
        }
    }
    // A guard that matched nothing would pass forever. Two sites today; the
    // count is deliberately a floor rather than an equality, so adding a
    // correct site does not fail the build.
    CHECK(found >= 2);
}
