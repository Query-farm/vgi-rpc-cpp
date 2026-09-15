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

#include "vgi_rpc/http_config.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/reflection.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/stream.h"
#include "vgi_rpc/wire.h"

#include <httplib.h>

#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>
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

    // Reflection's canonical description covers the two methods it answers,
    // the same table `describe` reports.
    auto expected = BindingHash(kReflectionProtocolName, ReflectionMethods());
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
    // A guard that matched nothing would pass forever. Six sites today --
    // three on the pipe (unary, stream, framework) and three on HTTP (stream
    // init, stream continuation, `__upload_url__`) -- and the count is
    // deliberately a floor rather than an equality, so adding a correct site
    // does not fail the build.
    CHECK(found >= 6);
}

// ── HTTP streams ─────────────────────────────────────────────────────
//
// A stream over HTTP is not one dispatch the server runs to completion: it is
// an `init` and a continuation per turn, each its own request, each its own
// RPC call by docs/access-log-spec.md's counting rule. This port served all of
// them and logged none, and nothing caught it -- the record validator checks
// the records that exist, so *no* records validate perfectly and the gap reads
// as a clean log rather than an unexamined one.
//
// Which is why these assert presence first and shape second, and why they
// drive a stream that genuinely takes several turns: a producer that finishes
// inside `init` would make the continuation half of the rule vacuous.

namespace {

/// Emits one row per turn for `turns` turns, then finishes.
class CountdownState : public ProducerState {
public:
    explicit CountdownState(int64_t turns) : remaining_(turns) {}

    void produce(OutputCollector& out, CallContext&) override {
        if (remaining_ <= 0) {
            out.finish();
            return;
        }
        arrow::StringBuilder builder;
        if (!builder.Append("row").ok()) throw std::runtime_error("append failed");
        std::shared_ptr<arrow::Array> column;
        if (!builder.Finish(&column).ok()) throw std::runtime_error("finish failed");
        out.emit_batch(arrow::RecordBatch::Make(value_schema(), 1, {column}));
        if (--remaining_ == 0) out.finish();
    }

private:
    int64_t remaining_;
};

/// Throws something `run_producer_turns`' `catch (const std::exception&)`
/// does not convert into an error batch, so the throw leaves `handle_rpc`
/// entirely -- the shape a failed IPC write inside `build_body` has.
class ThrowingState : public ProducerState {
public:
    void produce(OutputCollector&, CallContext&) override { throw 42; }
};

/// The number of turns `countdown` runs for. Three, so the stream has an init,
/// a non-terminal continuation and a terminal one -- the three record shapes
/// the spec distinguishes.
constexpr int64_t kCountdownTurns = 3;

/// A `Server::serve_http` on a background thread, writing an access log.
class HttpLogServer {
public:
    explicit HttpLogServer(const TempLog& log, const std::string& external_storage_url = "") {
        ServerBuilder builder;
        builder.protocol(kProtocol);
        builder.access_log(log.str());
        builder.add_unary("echo", value_schema(), value_schema(),
                          [](const Request& request, CallContext&) -> Result {
                              return Result::value(request.batch());
                          });
        builder.add_producer("countdown", empty_schema(), value_schema(),
                             [](const Request&, CallContext&) -> Stream {
                                 return Stream{value_schema(), empty_schema(),
                                               std::make_shared<CountdownState>(kCountdownTurns),
                                               nullptr};
                             });
        builder.add_producer("throwing", empty_schema(), value_schema(),
                             [](const Request&, CallContext&) -> Stream {
                                 return Stream{value_schema(), empty_schema(),
                                               std::make_shared<ThrowingState>(), nullptr};
                             });
        server_ = builder.build();

        HttpConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.prefix = "/vgi";
        cfg.external_storage_url = external_storage_url;
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

    // cpp-httplib owns the accept loop inside serve_http; detach rather than
    // join so one wedged case cannot hang the whole binary.
    ~HttpLogServer() { thread_.detach(); }

    int port() const { return port_; }
    const Server& rpc() const { return *server_; }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";

std::string buffer_string(const std::shared_ptr<arrow::Buffer>& buffer) {
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

/// A stream `init` body: zero-row params with the routing metadata.
std::string init_body(const std::string& method) {
    return buffer_string(request_buffer(method, kProtocol, /*empty_params=*/true));
}

/// A continuation body: a tick carrying the tokens the last turn handed back.
std::string continuation_body(const std::string& cursor, const std::string& call_token) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::STATE_B64, cursor);
    if (!call_token.empty()) metadata->Append(keys::CALL_STATE_B64, call_token);

    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    write_ipc_stream(
        sink, empty_schema(),
        {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), std::move(metadata))});
    return buffer_string(sink->Finish().ValueUnsafe());
}

/// The first value of `key` across a response's batches, or "" when absent.
std::string response_metadata(const std::string& body, const std::string& key) {
    auto reader =
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::string(body)));
    auto contents = read_ipc_stream(reader);
    if (!contents) return "";
    for (const auto& annotated : contents->batches) {
        const std::string value = get_metadata_value(annotated.custom_metadata, key);
        if (!value.empty()) return value;
    }
    return "";
}

/// Drive `countdown` to exhaustion over HTTP, returning the number of turns.
int drive_countdown(int port) {
    httplib::Client client("127.0.0.1", port);
    client.set_read_timeout(10, 0);

    auto response = client.Post("/vgi/AccessLogProbe/countdown/init", init_body("countdown"),
                                kArrowContentType);
    REQUIRE(response);
    REQUIRE(response->status == 200);

    int turns = 1;
    std::string cursor = response_metadata(response->body, keys::STATE_B64);
    const std::string call_token = response_metadata(response->body, keys::CALL_STATE_B64);
    while (!cursor.empty()) {
        response = client.Post("/vgi/AccessLogProbe/countdown/exchange",
                               continuation_body(cursor, call_token), kArrowContentType);
        REQUIRE(response);
        REQUIRE(response->status == 200);
        ++turns;
        cursor = response_metadata(response->body, keys::STATE_B64);
    }
    return turns;
}

/// Every access record for `method`, in the order they were written.
std::vector<nlohmann::json> records_for(const TempLog& log, const std::string& method) {
    std::vector<nlohmann::json> out;
    for (const auto& record : log.records()) {
        if (record.value("method", "") == method) out.push_back(record);
    }
    return out;
}

}  // namespace

TEST_CASE("an HTTP stream files one record per turn", "[access-log]") {
    TempLog log;
    HttpLogServer server(log);
    const int turns = drive_countdown(server.port());
    CHECK(turns == kCountdownTurns);

    auto records = records_for(log, "countdown");
    // The assertion the whole section exists for. A transport that logs unary
    // calls and not streams drops the calls that run longest and move the most
    // data, and leaves a log that still passes every schema check.
    REQUIRE(records.size() == static_cast<size_t>(turns));
}

TEST_CASE("a stream's records share one stream_id", "[access-log]") {
    // Without this a reader cannot reassemble a call from its turns, which is
    // the only thing that makes per-turn records more useful than a counter.
    TempLog log;
    HttpLogServer server(log);
    drive_countdown(server.port());

    auto records = records_for(log, "countdown");
    REQUIRE(records.size() >= 2);
    const std::string first = records.front().value("stream_id", "");
    CHECK(first.size() == 32);
    CHECK(first.find_first_not_of("0123456789abcdef") == std::string::npos);
    for (const auto& record : records) {
        CHECK(record.value("method_type", "") == "stream");
        CHECK(record.value("stream_id", "") == first);
    }
}

TEST_CASE("request_data rides the init record and only the init record", "[access-log]") {
    // §5's rules key off the record's shape rather than a method name, and
    // "is this the init" is readable only from request_data's presence. A port
    // that repeated it on every continuation would make a stream's turns
    // indistinguishable from n separate calls.
    TempLog log;
    HttpLogServer server(log);
    drive_countdown(server.port());

    auto records = records_for(log, "countdown");
    REQUIRE(records.size() >= 2);
    CHECK(records.front().contains("request_data"));
    for (size_t i = 1; i < records.size(); ++i) {
        CHECK_FALSE(records[i].contains("request_data"));
    }
}

TEST_CASE("response_state is absent exactly on the turn that ends the stream", "[access-log]") {
    // The one thing the state fields make readable from the log alone: which
    // turn handed a continuation back and which one closed the stream. C++
    // keeps stream state in-process, so what travels is the registry cursor --
    // in plaintext, which is what §4.4 asks to be logged.
    TempLog log;
    HttpLogServer server(log);
    drive_countdown(server.port());

    auto records = records_for(log, "countdown");
    REQUIRE(records.size() >= 2);
    for (size_t i = 0; i + 1 < records.size(); ++i) {
        INFO("turn " << i);
        CHECK(records[i].contains("response_state"));
    }
    CHECK_FALSE(records.back().contains("response_state"));

    // request_state is the mirror image: absent on the init, present on every
    // continuation including the terminal one, because a turn that resumed a
    // stream resumed it from somewhere.
    CHECK_FALSE(records.front().contains("request_state"));
    for (size_t i = 1; i < records.size(); ++i) {
        INFO("turn " << i);
        CHECK(records[i].contains("request_state"));
    }
}

TEST_CASE("a stream record names the binding that owns the method", "[access-log]") {
    // Same failure as the unary case above, at a site that was added later:
    // the pair has to come from one binding, and a stream's owner is the
    // application's.
    TempLog log;
    HttpLogServer server(log);
    drive_countdown(server.port());

    auto records = records_for(log, "countdown");
    REQUIRE_FALSE(records.empty());
    auto expected = BindingHash(kProtocol, server.rpc().methods());
    REQUIRE(expected.ok());
    for (const auto& record : records) {
        CHECK(record.value("protocol", "") == kProtocol);
        CHECK(record.value("protocol_hash", "") == *expected);
    }
}

// ── `__upload_url__` ─────────────────────────────────────────────────
//
// A framework endpoint owned by no protocol, which §3 says logs the server's
// primary -- prescribed rather than tolerated, because there is no other
// protocol it could honestly be filed under. Emitting *nothing* is still a
// gap, and it was one: this route is reachable, dispatched and answered, and
// left no trace.

namespace {

/// The four-endpoint object store's `/alloc` half, which is all
/// `__upload_url__` needs: it vends URL pairs and never touches the bytes.
class FakeAllocServer {
public:
    FakeAllocServer() {
        svr_.Post("/alloc", [](const httplib::Request&, httplib::Response& res) {
            nlohmann::json body;
            // `object_url` is the pre-split spelling the client still reads as
            // its fallback, and reads with `.at` -- omitting it is a 500.
            body["object_url"] = "http://127.0.0.1:1/blob";
            body["upload_url"] = "http://127.0.0.1:1/blob/put";
            body["download_url"] = "http://127.0.0.1:1/blob/get";
            // The charset is load bearing, unhappily.  vgi's external-storage
            // client advertises `Accept-Encoding: br, gzip, deflate` (cpp-httplib
            // adds it unasked) while calling `set_decompress(false)`, so a store
            // that honours the advertisement hands back a body the client cannot
            // parse.  cpp-httplib compresses on the Content-Type alone, and the
            // charset parameter takes this response out of its compressible set.
            // That mismatch in `make_client` is a real bug and not this test's
            // subject; working around it here keeps the two separable.
            res.set_content(body.dump(), "application/json; charset=utf-8");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        REQUIRE(port_ > 0);
        thread_ = std::thread([this]() { (void)svr_.listen_after_bind(); });
        svr_.wait_until_ready();
    }

    ~FakeAllocServer() {
        svr_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

private:
    httplib::Server svr_;
    std::thread thread_;
    int port_ = 0;
};

}  // namespace

TEST_CASE("__upload_url__ files a record under the server's primary", "[access-log]") {
    TempLog log;
    FakeAllocServer storage;
    HttpLogServer server(log, storage.url());

    httplib::Client client("127.0.0.1", server.port());
    client.set_read_timeout(10, 0);
    auto response =
        client.Post("/vgi/__upload_url__/init", init_body("__upload_url__"), kArrowContentType);
    REQUIRE(response);
    REQUIRE(response->status == 200);

    auto record = record_for(log, "__upload_url__");
    REQUIRE(record.has_value());
    CHECK(record->value("method_type", "") == "unary");
    CHECK(record->value("status", "") == "ok");
    CHECK(record->value("protocol", "") == kProtocol);
    auto expected = BindingHash(kProtocol, server.rpc().methods());
    REQUIRE(expected.ok());
    CHECK(record->value("protocol_hash", "") == *expected);
    CHECK(record->contains("request_data"));
}

TEST_CASE("a turn that leaves by exception does not file an 'ok' record", "[access-log]") {
    // Not every throw on the stream paths is funnelled into an error body: one
    // that escapes `handle_rpc` reaches cpp-httplib's exception handler and
    // becomes a 500.  The record is written from a destructor, so it is written
    // either way -- and a record saying `ok` beside a 500 is the one an
    // operator would trust while chasing the failure.
    TempLog log;
    HttpLogServer server(log);

    httplib::Client client("127.0.0.1", server.port());
    client.set_read_timeout(10, 0);
    auto response =
        client.Post("/vgi/AccessLogProbe/throwing/init", init_body("throwing"), kArrowContentType);
    REQUIRE(response);
    CHECK(response->status == 500);

    auto record = record_for(log, "throwing");
    REQUIRE(record.has_value());
    CHECK(record->value("status", "") == "error");
    CHECK_FALSE(record->value("error_message", "").empty());
    CHECK_FALSE(record->contains("response_state"));
}
