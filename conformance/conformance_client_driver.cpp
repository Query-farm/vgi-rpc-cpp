// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Conformance client driver: the JSONL bridge that lets the shared Python
// conformance suite drive *this port's client*.
//
// The suite normally points the reference Python client at a foreign server.
// This is the other direction: the Python side owns value marshaling and hands
// this process Arrow IPC bytes plus a method name, and `RpcClient` /
// `HttpClient` do all the real wire framing, transport I/O, stream lockstep,
// HTTP continuation and log/error envelope parsing.  This file only relays.
//
// The control protocol is specified in the Python reference's
// `tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md`.  That document, not this
// file, is the contract; where a C++ convenience would diverge from it, the
// document wins.  In particular this driver must never default a method name
// or routing key, infer a stream kind from a method name, resolve an external
// pointer itself, retry, or normalise an error type — every one of those turns
// a client defect into a passing run.

#include "vgi_rpc/access_log.h"
#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/client.h"
#include "vgi_rpc/client_description.h"
#include "vgi_rpc/client_external.h"
#include "vgi_rpc/errors.h"
#include "vgi_rpc/http_client.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <nlohmann/json.hpp>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#else
#include <unistd.h>
#endif

using nlohmann::json;
using namespace vgi_rpc;

namespace {

// ---------------------------------------------------------------------------
// The control channel.
//
// stdout is the control channel and nothing else: one response object per
// line.  A stray `printf` from a library — or from this file — desynchronises
// the whole run and is reported as a protocol failure somewhere unrelated, so
// the real descriptor is moved out of the way at startup and fd 1 is pointed
// at stderr.  Anything that writes to "stdout" afterwards lands in the
// diagnostics where it belongs.
// ---------------------------------------------------------------------------

int g_control_fd = -1;

void claim_control_channel() {
#if defined(_WIN32)
    g_control_fd = _dup(1);
    if (g_control_fd >= 0) _setmode(g_control_fd, _O_BINARY);
    (void)_dup2(2, 1);
#else
    g_control_fd = ::dup(1);
    (void)::dup2(2, 1);
#endif
    if (g_control_fd < 0) {
        std::cerr << "conformance client driver: cannot duplicate stdout\n";
        std::exit(2);
    }
}

void write_response(const json& value) {
    std::string line = value.dump();
    line.push_back('\n');
    size_t written = 0;
    while (written < line.size()) {
#if defined(_WIN32)
        const int n = _write(g_control_fd, line.data() + written,
                             static_cast<unsigned>(line.size() - written));
#else
        const ssize_t n = ::write(g_control_fd, line.data() + written, line.size() - written);
#endif
        if (n <= 0) {
            if (errno == EINTR) continue;
            return;  // the harness is gone; nothing useful is left to say
        }
        written += static_cast<size_t>(n);
    }
}

json driver_error(const std::string& text) {
    return json{{"ok", false}, {"error", text}};
}

// ---------------------------------------------------------------------------
// Arrow IPC framing.
//
// Every `*_b64` field carries a complete IPC *stream* holding exactly one
// batch and its custom metadata.  The driver reads exactly one batch, hands it
// and the whole metadata map to the client, and re-serialises whatever comes
// back — it never decodes a value.
// ---------------------------------------------------------------------------

std::string b64_encode(const std::string& bytes) {
    return base64_encode(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

// Standard base64 (RFC 4648 §4, padded).  Rejects anything else rather than
// silently decoding a partial payload.
std::optional<std::string> b64_decode(const std::string& text) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    int8_t table[256];
    std::memset(table, -1, sizeof(table));
    for (int i = 0; i < 64; ++i) table[static_cast<unsigned char>(kAlphabet[i])] = int8_t(i);

    std::string out;
    out.reserve(text.size() / 4 * 3);
    uint32_t accumulator = 0;
    int bits = 0;
    size_t padding = 0;
    for (const char c : text) {
        if (c == '=') {
            ++padding;
            continue;
        }
        if (padding > 0) return std::nullopt;  // data after padding
        const int8_t value = table[static_cast<unsigned char>(c)];
        if (value < 0) return std::nullopt;
        accumulator = (accumulator << 6) | static_cast<uint32_t>(value);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((accumulator >> bits) & 0xFF));
        }
    }
    if (padding > 2 || (text.size() % 4) != 0) return std::nullopt;
    return out;
}

// Parse a request/input IPC stream down to its single (batch, metadata).
//
// The buffer must *own* its bytes: Arrow decodes a stream zero-copy, so the
// returned batch's arrays are views into it and a borrowed view of a local
// string hands the client empty columns once that string goes out of scope.
AnnotatedBatch read_one_batch(std::string bytes) {
    auto reader =
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes)));
    auto contents = read_ipc_stream(reader);
    if (!contents || contents->batches.empty()) {
        throw std::runtime_error("empty IPC stream (no batch)");
    }
    return contents->batches.front();
}

std::string write_one_batch(const AnnotatedBatch& value) {
    if (!value.batch) throw std::runtime_error("cannot serialise a null batch");
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    write_ipc_stream(sink, value.batch->schema(), {value});
    auto buffer = sink->Finish().ValueOrDie();
    return {reinterpret_cast<const char*>(buffer->data()), static_cast<size_t>(buffer->size())};
}

std::string batch_b64(const AnnotatedBatch& value) {
    return b64_encode(write_one_batch(value));
}

// A schema field carries an IPC stream too; only its schema message is read,
// so a stream over an empty batch is the cheapest thing that satisfies it.
json schema_b64(const std::shared_ptr<arrow::Schema>& schema) {
    if (!schema) return nullptr;
    return batch_b64(AnnotatedBatch::data(make_empty_batch(schema)));
}

// ---------------------------------------------------------------------------
// Errors and logs.
//
// `ok` describes the driver, `error` describes the call.  A transport failure
// the client library surfaces as its own error type is a *call* error, so
// every client-thrown exception below becomes `{"ok": true, "error": {...}}`
// and only a malformed control line or an unsupported op gets `ok: false`.
//
// `error_type` is asserted verbatim by the suite, so it is taken from whatever
// the client decoded and never translated into a C++ class name.
// ---------------------------------------------------------------------------

json error_object(const std::string& error_type, const std::string& message) {
    return json{{"error_type", error_type}, {"error_message", message}, {"traceback", ""}};
}

json error_from_exception(const std::exception& error) {
    if (const auto* remote = dynamic_cast<const RpcRemoteError*>(&error)) {
        return error_object(remote->exception_type(), remote->what());
    }
    if (const auto* raw = dynamic_cast<const RpcException*>(&error)) {
        return error_object(raw->exception_type(), raw->what());
    }
    if (const auto* kinded = dynamic_cast<const KindedError*>(&error)) {
        return error_object(kinded->exception_type(), kinded->what());
    }
    // Everything else is the client library reporting a transport, HTTP or
    // decode failure of its own.  Still a call error, and still carrying the
    // class name the client chose.
    return error_object(exception_type_of(error), error.what());
}

json log_to_json(const Message& message) {
    json extra = json::object();
    if (message.extra.is_object()) {
        for (const auto& [key, value] : message.extra.items()) {
            extra[key] = value.is_string() ? value.get<std::string>() : value.dump();
        }
    }
    return json{{"level", log_level_to_string(message.level)},
                {"message", message.message},
                {"extra", std::move(extra)}};
}

Message message_from_log_batch(const AnnotatedBatch& value) {
    Message message{
        log_level_from_string(get_metadata_value(value.custom_metadata, keys::LOG_LEVEL)),
        get_metadata_value(value.custom_metadata, keys::LOG_MESSAGE), json::object()};
    const std::string extra = get_metadata_value(value.custom_metadata, keys::LOG_EXTRA);
    if (!extra.empty()) {
        json parsed = json::parse(extra, nullptr, /*allow_exceptions=*/false);
        if (!parsed.is_discarded() && parsed.is_object()) message.extra = std::move(parsed);
    }
    return message;
}

// ---------------------------------------------------------------------------
// The driver itself.
//
// Flat dispatch: stream ops read mutable session fields rather than owning a
// nested read loop.  Legal because the harness never interleaves a non-stream
// op with an open stream and never sends a stream op after a terminal event —
// and a second `stream_open` is refused outright rather than leaking the
// first.
// ---------------------------------------------------------------------------

class Driver {
public:
    bool dispatch(const json& request);
    void shutdown();

private:
    // Connection.
    json op_connect(const json& request);
    // Calls.
    json op_unary(const json& request);
    json op_describe();
    json op_stream_open(const json& request);
    // Stream.
    json op_tick(const json& request);
    json op_next_with_token();
    json op_exchange(const json& request);
    json op_cancel();
    json op_close();
    // HTTP-only.
    json op_capabilities();
    json op_request_upload_urls(const json& request);
    json op_session_begin(const json& request);
    json op_session_token();
    json op_session_echo_headers();
    json op_session_detach();
    json op_session_end();

    bool connected() const { return raw_.has_value() || http_.has_value(); }
    HttpSessionView* session() { return sessions_.empty() ? nullptr : &sessions_.back().view; }
    bool streaming() const { return raw_stream_.has_value() || http_stream_.has_value(); }
    void end_stream();
    json drain_logs();
    json describe_to_json(const ServiceDescription& description) const;
    ClientLogHandler make_log_sink();

    std::optional<RpcClient> raw_;
    std::optional<HttpClient> http_;
    // A stack, not a single view: the harness nests session scopes (it opens a
    // second session inside an open one to prove a draining server refuses new
    // ones, then keeps using the first), so ending a scope must restore the one
    // it was opened inside rather than leave the connection unbound.
    struct Session {
        HttpSessionView view;
        /// `session_detach` handed the token to the caller; the session
        /// outlives this scope and `session_end` must not delete it.
        bool detached = false;
    };
    std::vector<Session> sessions_;
    std::optional<ClientStream> raw_stream_;
    std::optional<HttpStreamSession> http_stream_;
    std::vector<Message> logs_;
};

ClientLogHandler Driver::make_log_sink() {
    return [this](const Message& message) { logs_.push_back(message); };
}

json Driver::drain_logs() {
    json out = json::array();
    for (const auto& message : logs_) out.push_back(log_to_json(message));
    logs_.clear();
    return out;
}

void Driver::end_stream() {
    // Release rather than abandon.  A raw stream reserves the connection until
    // it is closed, and its destructor deliberately does not drain -- so a
    // driver that merely dropped the object at end-of-stream would leave the
    // connection unusable for the next call, and `close`/`cancel` are the only
    // stream ops the harness sends after a terminal event (it sends neither).
    if (raw_stream_) {
        try {
            raw_stream_->close();
        } catch (const std::exception&) {
            // Already torn down, or the peer is gone; either way the stream is
            // over and a teardown failure must not become a second error.
        }
        raw_stream_.reset();
    }
    if (http_stream_) {
        http_stream_->close();
        http_stream_.reset();
    }
}

// --- connect ---------------------------------------------------------------

json Driver::op_connect(const json& request) {
    if (connected()) return driver_error("already connected");

    const std::string transport = request.value("transport", std::string{});
    // The routing key.  Required, and never defaulted: over HTTP the reference
    // server routes only {protocol}/{method}, so a substituted name hides
    // exactly the class of bug this whole exercise exists to catch.
    if (!request.contains("protocol") || !request["protocol"].is_string()) {
        return driver_error("connect requires a string 'protocol' routing key");
    }
    const std::string protocol = request["protocol"].get<std::string>();
    const json target = request.value("target", json());

    auto argv_target = [&]() -> std::vector<std::string> {
        if (!target.is_array())
            throw std::runtime_error(transport + " target must be an argv array");
        std::vector<std::string> argv;
        for (const auto& item : target) {
            if (!item.is_string()) throw std::runtime_error("argv entries must be strings");
            argv.push_back(item.get<std::string>());
        }
        if (argv.empty()) throw std::runtime_error("argv array is empty");
        return argv;
    };

    try {
        RpcClientOptions options;
        options.protocol = protocol;
        options.on_log = make_log_sink();

        if (transport == "stdio") {
            raw_.emplace(RpcClient::spawn(argv_target(), options));
        } else if (transport == "shm") {
            options.shared_memory_bytes =
                static_cast<size_t>(request.value("shm_size", int64_t{4 * 1024 * 1024}));
            raw_.emplace(RpcClient::spawn(argv_target(), options));
        } else if (transport == "unix") {
            if (!target.is_string()) return driver_error("unix target must be a path string");
            raw_.emplace(RpcClient::connect_unix(target.get<std::string>(), options));
        } else if (transport == "tcp") {
            if (!target.is_string()) return driver_error("tcp target must be a host:port string");
            const std::string address = target.get<std::string>();
            const size_t colon = address.rfind(':');
            std::string host = "127.0.0.1";
            std::string port_text = address;
            if (colon != std::string::npos) {
                if (colon > 0) host = address.substr(0, colon);
                port_text = address.substr(colon + 1);
            }
            const int port = std::stoi(port_text);
            if (port <= 0 || port > 65535) return driver_error("tcp port out of range");
            raw_.emplace(RpcClient::connect_tcp(host, static_cast<uint16_t>(port), options));
        } else if (transport == "http") {
            if (!target.is_string()) return driver_error("http target must be a url string");

            HttpClientConfig config;
            // `connect` carries a whole URL, never a mount prefix, so the
            // library's own /vgi default must not be inherited here.
            config.prefix = "";
            config.protocol = protocol;
            config.on_log = [this](const AnnotatedBatch& value) {
                logs_.push_back(message_from_log_batch(value));
            };
            // compression_level is tri-state: absent picks the client default,
            // null disables request compression, an integer selects that level.
            if (request.contains("compression_level")) {
                const auto& level = request["compression_level"];
                config.compression_level =
                    level.is_null() ? std::optional<int>{} : std::optional<int>{level.get<int>()};
            }
            if (request.contains("headers") && request["headers"].is_object()) {
                for (const auto& [name, value] : request["headers"].items()) {
                    if (value.is_string()) config.headers[name] = value.get<std::string>();
                }
            }

            auto builder = HttpClient::builder(target.get<std::string>());
            builder.config(std::move(config));
            // The *client under test* does every external transfer: resolving
            // a pointer here, or on the Python side, would make the test pass
            // without the client ever doing the work.  What the driver does
            // choose is the URL policy, because the control protocol has no
            // field for one and the whole conformance fixture is loopback plain
            // HTTP -- the fake object store vends `http://127.0.0.1/...` for
            // both pointer downloads and vended upload URLs.  This is the
            // policy that exists for exactly that, and it is not a loosening:
            // it additionally requires every resolved address to be loopback.
            //
            // Unconditional rather than gated on `external`, which says who
            // resolves a *pointer batch*.  Upload-URL vending and client-side
            // request externalization are neither, and the harness asks for
            // them on connections that never set the flag.
            ClientExternalHttpOptions external;
            external.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
            builder.external_http_options(external);
            http_.emplace(builder.build());
        } else {
            return driver_error("unknown transport: " + transport);
        }
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
    return json{{"ok", true}};
}

// --- unary -----------------------------------------------------------------

json Driver::op_unary(const json& request) {
    if (!connected()) return driver_error("not connected");

    AnnotatedBatch call;
    std::string method;
    try {
        const auto bytes = b64_decode(request.value("request_b64", std::string{}));
        if (!bytes) return driver_error("base64 decode: request_b64 is not standard base64");
        call = read_one_batch(std::move(*bytes));
        method = get_metadata_value(call.custom_metadata, keys::METHOD);
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
    // A request batch with no method name is a bug in the harness or in this
    // driver's IPC reader.  Fail loudly rather than defaulting: an earlier
    // driver generation defaulted, and every lost-metadata bug was then
    // reported as a retired-method error instead.
    if (method.empty()) {
        return driver_error(std::string("request metadata names no ") + keys::METHOD);
    }

    try {
        AnnotatedBatch result = raw_ ? raw_->call_unary(method, call.batch, call.custom_metadata)
                                     : (session() ? session()->call(method, call, nullptr, {})
                                                  : http_->call(method, call, nullptr, {}));
        return json{{"ok", true},
                    {"result_b64", batch_b64(result)},
                    {"logs", drain_logs()},
                    {"error", nullptr}};
    } catch (const std::exception& error) {
        return json{{"ok", true},
                    {"result_b64", nullptr},
                    {"logs", drain_logs()},
                    {"error", error_from_exception(error)}};
    }
}

// --- describe --------------------------------------------------------------

json Driver::describe_to_json(const ServiceDescription& description) const {
    json methods = json::array();
    for (const auto& [name, method] : description.methods) {
        json entry{{"name", method.name},
                   {"method_type", method.method_type},
                   {"has_return", method.has_return},
                   {"has_header", method.has_header},
                   {"params_schema_b64", schema_b64(method.params_schema)},
                   {"result_schema_b64", schema_b64(method.result_schema)},
                   {"header_schema_b64", schema_b64(method.header_schema)}};
        entry["is_exchange"] =
            method.is_exchange.has_value() ? json(*method.is_exchange) : json(nullptr);
        methods.push_back(std::move(entry));
    }
    return json{{"protocol_name", description.protocol_name},
                {"request_version", description.request_version},
                {"describe_version", description.describe_version},
                {"protocol_hash", description.protocol_hash},
                {"server_id", description.server_id},
                {"protocol_version", description.protocol_version},
                {"methods", std::move(methods)}};
}

json Driver::op_describe() {
    if (!connected()) return driver_error("not connected");
    try {
        // Two round trips on vgi_rpc.Reflection.v1 — list_protocols, then
        // describe on the one hosted protocol that is not framework surface —
        // which is what the client's own `describe()` already performs.
        const ServiceDescription description =
            raw_ ? raw_->describe() : (session() ? session()->describe() : http_->describe());
        return json{{"ok", true},
                    {"describe", describe_to_json(description)},
                    {"logs", drain_logs()},
                    {"error", nullptr}};
    } catch (const std::exception& error) {
        return json{{"ok", true},
                    {"describe", nullptr},
                    {"logs", drain_logs()},
                    {"error", error_from_exception(error)}};
    }
}

// --- stream_open -----------------------------------------------------------

json Driver::op_stream_open(const json& request) {
    if (!connected()) return driver_error("not connected");
    if (streaming()) return driver_error("a stream is already open on this connection");

    AnnotatedBatch call;
    std::string method;
    try {
        const auto bytes = b64_decode(request.value("request_b64", std::string{}));
        if (!bytes) return driver_error("base64 decode: request_b64 is not standard base64");
        call = read_one_batch(std::move(*bytes));
        method = get_metadata_value(call.custom_metadata, keys::METHOD);
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
    if (method.empty()) {
        return driver_error(std::string("request metadata names no ") + keys::METHOD);
    }
    // Authoritative, derived by the harness from the protocol declaration.
    // Never inferred from the method name: a name-prefix heuristic is a
    // fixture-shaped accident that does not survive the next method added.
    const bool is_exchange = request.value("is_exchange", false);
    const bool has_header = request.value("has_header", false);

    try {
        std::optional<AnnotatedBatch> header;
        if (raw_) {
            raw_stream_.emplace(
                is_exchange
                    ? raw_->open_exchange(method, call.batch, has_header, call.custom_metadata)
                    : raw_->open_producer(method, call.batch, has_header, call.custom_metadata));
            header = raw_stream_->header();
        } else {
            // The schemas are a prediction the client adopts from the first
            // response; a relay has none to offer, which is what the dynamic
            // (null) form is for.
            HttpSessionView* view = session();
            http_stream_.emplace(
                is_exchange
                    ? (view ? view->open_stream_exchange(method, call, nullptr, nullptr, has_header)
                            : http_->open_stream_exchange(method, call, nullptr, nullptr,
                                                          has_header))
                    : (view ? view->open_producer(method, call, nullptr, has_header)
                            : http_->open_producer(method, call, nullptr, has_header)));
            header = http_stream_->header();
        }
        json header_value = header.has_value() ? json(batch_b64(*header)) : json(nullptr);
        return json{{"ok", true}, {"header_b64", std::move(header_value)}, {"logs", drain_logs()}};
    } catch (const std::exception& error) {
        // The driver did carry out the op; the server refused.  `ok: false`
        // would mix the two channels and leave the shim an object where the
        // protocol documents a string.
        end_stream();
        return json{{"ok", true},
                    {"header_b64", nullptr},
                    {"logs", drain_logs()},
                    {"error", error_from_exception(error)}};
    }
}

// --- stream ops ------------------------------------------------------------

json Driver::op_tick(const json& request) {
    if (!streaming()) return driver_error("no stream is open");

    std::shared_ptr<arrow::KeyValueMetadata> tick_metadata;
    if (request.contains("input_b64") && request["input_b64"].is_string()) {
        // The stream carries an empty batch whose custom metadata is the
        // per-tick metadata to send upstream: read the metadata, ignore the
        // batch.
        try {
            const auto bytes = b64_decode(request["input_b64"].get<std::string>());
            if (!bytes) return driver_error("base64 decode: input_b64 is not standard base64");
            tick_metadata = read_one_batch(std::move(*bytes)).custom_metadata;
        } catch (const std::exception& error) {
            return driver_error(error.what());
        }
    }

    try {
        std::optional<AnnotatedBatch> value =
            raw_stream_ ? raw_stream_->tick(tick_metadata) : http_stream_->tick(tick_metadata);
        if (!value) {
            end_stream();
            return json{{"ok", true},
                        {"done", true},
                        {"batch_b64", nullptr},
                        {"logs", drain_logs()},
                        {"error", nullptr}};
        }
        return json{{"ok", true},
                    {"done", false},
                    {"batch_b64", batch_b64(*value)},
                    {"logs", drain_logs()},
                    {"error", nullptr}};
    } catch (const std::exception& error) {
        end_stream();
        return json{{"ok", true},
                    {"done", true},
                    {"batch_b64", nullptr},
                    {"logs", drain_logs()},
                    {"error", error_from_exception(error)}};
    }
}

json Driver::op_next_with_token() {
    if (!streaming()) return driver_error("no stream is open");
    try {
        if (raw_stream_) {
            // Byte-stream transports carry no resumable stream state: the
            // live stream object *is* the continuation handle, so there is no
            // token to hand back.
            std::optional<AnnotatedBatch> value = raw_stream_->tick(nullptr);
            if (!value) {
                end_stream();
                return json{{"ok", true},       {"done", true},         {"batch_b64", nullptr},
                            {"token", nullptr}, {"logs", drain_logs()}, {"error", nullptr}};
            }
            return json{
                {"ok", true},       {"done", false},        {"batch_b64", batch_b64(*value)},
                {"token", nullptr}, {"logs", drain_logs()}, {"error", nullptr}};
        }
        std::optional<HttpStreamBatch> value = http_stream_->next_with_token();
        if (!value) {
            end_stream();
            return json{{"ok", true},       {"done", true},         {"batch_b64", nullptr},
                        {"token", nullptr}, {"logs", drain_logs()}, {"error", nullptr}};
        }
        json token =
            value->resume_token.empty()
                ? json(nullptr)
                : json(base64_encode(value->resume_token.data(), value->resume_token.size()));
        return json{{"ok", true},
                    {"done", false},
                    {"batch_b64", batch_b64(value->value)},
                    {"token", std::move(token)},
                    {"logs", drain_logs()},
                    {"error", nullptr}};
    } catch (const std::exception& error) {
        end_stream();
        return json{{"ok", true},           {"done", true},
                    {"batch_b64", nullptr}, {"token", nullptr},
                    {"logs", drain_logs()}, {"error", error_from_exception(error)}};
    }
}

json Driver::op_exchange(const json& request) {
    if (!streaming()) return driver_error("no stream is open");

    AnnotatedBatch input;
    try {
        const auto bytes = b64_decode(request.value("input_b64", std::string{}));
        if (!bytes) return driver_error("base64 decode: input_b64 is not standard base64");
        input = read_one_batch(std::move(*bytes));
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }

    try {
        std::optional<AnnotatedBatch> value =
            raw_stream_ ? raw_stream_->exchange(input.batch, input.custom_metadata)
                        : http_stream_->exchange(input);
        if (!value) {
            end_stream();
            return json{{"ok", true},
                        {"done", true},
                        {"batch_b64", nullptr},
                        {"logs", drain_logs()},
                        {"error", nullptr}};
        }
        return json{{"ok", true},
                    {"done", false},
                    {"batch_b64", batch_b64(*value)},
                    {"logs", drain_logs()},
                    {"error", nullptr}};
    } catch (const std::exception& error) {
        end_stream();
        return json{{"ok", true},
                    {"done", true},
                    {"batch_b64", nullptr},
                    {"logs", drain_logs()},
                    {"error", error_from_exception(error)}};
    }
}

json Driver::op_cancel() {
    // A cancel with no stream open is a successful no-op.
    if (streaming()) {
        try {
            if (raw_stream_) {
                raw_stream_->cancel();
            } else {
                http_stream_->cancel();
            }
        } catch (const std::exception&) {
            // Cancellation is best effort and terminal either way.
        }
        end_stream();
    }
    return json{{"ok", true}, {"logs", drain_logs()}};
}

json Driver::op_close() {
    if (streaming()) {
        try {
            if (raw_stream_) {
                raw_stream_->close();
            } else {
                http_stream_->close();
            }
        } catch (const std::exception&) {
            // Releasing a stream must not turn teardown into a failure.
        }
        end_stream();
    }
    return json{{"ok", true}};
}

// --- HTTP-only ops ---------------------------------------------------------

json Driver::op_capabilities() {
    try {
        const HttpServerCapabilities caps =
            session() ? session()->capabilities() : http_->capabilities();
        auto optional_int = [](const std::optional<int64_t>& value) {
            return value.has_value() ? json(*value) : json(nullptr);
        };
        return json{{"ok", true},
                    {"caps", json{{"sticky_enabled", caps.sticky_enabled},
                                  {"sticky_default_ttl", optional_int(caps.sticky_default_ttl)},
                                  {"sticky_echo_headers", caps.sticky_echo_headers},
                                  {"upload_url_support", caps.upload_url_support},
                                  {"max_request_bytes", optional_int(caps.max_request_bytes)},
                                  {"max_response_bytes", optional_int(caps.max_response_bytes)},
                                  {"max_externalized_response_bytes",
                                   optional_int(caps.max_externalized_response_bytes)},
                                  {"externalization_enabled", caps.externalization_enabled},
                                  {"max_upload_bytes", optional_int(caps.max_upload_bytes)},
                                  {"supported_encodings", caps.supported_encodings}}}};
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
}

json Driver::op_request_upload_urls(const json& request) {
    try {
        const int64_t count = request.value("count", int64_t{1});
        const auto urls =
            session() ? session()->request_upload_urls(count) : http_->request_upload_urls(count);
        json list = json::array();
        for (const auto& url : urls) {
            // The protocol asks for integer Unix seconds; the client carries
            // microseconds.
            json expires = url.expires_at_us.has_value() ? json(*url.expires_at_us / 1'000'000)
                                                         : json(nullptr);
            list.push_back(json{{"upload_url", url.upload_url},
                                {"download_url", url.download_url},
                                {"expires_at", std::move(expires)}});
        }
        return json{{"ok", true}, {"urls", std::move(list)}};
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
}

json Driver::op_session_begin(const json& request) {
    try {
        std::optional<std::string> token;
        if (request.contains("token") && request["token"].is_string()) {
            const auto text = request["token"].get<std::string>();
            // A null, absent or empty token all mean "let the server mint one".
            if (!text.empty()) token = text;
        }
        sessions_.push_back(Session{http_->with_session_token(std::move(token)), false});
        return json{{"ok", true}};
    } catch (const std::exception& error) {
        return driver_error(error.what());
    }
}

json Driver::op_session_token() {
    HttpSessionView* view = session();
    if (!view) return json{{"ok", true}, {"token", nullptr}};
    const auto token = view->current_session_token();
    return json{{"ok", true}, {"token", token ? json(*token) : json(nullptr)}};
}

json Driver::op_session_echo_headers() {
    json headers = json::object();
    if (HttpSessionView* view = session()) {
        for (const auto& [name, value] : view->current_echo_headers()) headers[name] = value;
    }
    return json{{"ok", true}, {"headers", std::move(headers)}};
}

json Driver::op_session_detach() {
    HttpSessionView* view = session();
    if (!view) return json{{"ok", true}, {"token", nullptr}};
    const auto token = view->detach();
    sessions_.back().detached = true;
    return json{{"ok", true}, {"token", token ? json(*token) : json(nullptr)}};
}

json Driver::op_session_end() {
    if (!sessions_.empty()) {
        // `close()` is idempotent and a no-op once `detach()` has forgotten
        // the token, so a detached session is left alive by construction.
        if (!sessions_.back().detached) sessions_.back().view.close();
        sessions_.pop_back();
    }
    return json{{"ok", true}};
}

// --- dispatch --------------------------------------------------------------

void Driver::shutdown() {
    // In order: terminate any stream still open, then release the transport.
    // Closing the connection is part of the contract, not an optimisation: the
    // harness runs thousands of connections, and a leaked subprocess or socket
    // per connection exhausts the runner rather than failing a test.
    if (streaming()) {
        try {
            if (raw_stream_) {
                raw_stream_->cancel();
            } else {
                http_stream_->cancel();
            }
        } catch (const std::exception&) {
        }
        end_stream();
    }
    while (!sessions_.empty()) {
        try {
            if (!sessions_.back().detached) sessions_.back().view.close();
        } catch (const std::exception&) {
        }
        sessions_.pop_back();
    }
    if (raw_) {
        try {
            raw_->close();
        } catch (const std::exception&) {
        }
        raw_.reset();
    }
    http_.reset();
}

bool Driver::dispatch(const json& request) {
    const std::string op = request.value("op", std::string{});

    if (op == "shutdown") {
        shutdown();
        write_response(json{{"ok", true}});
        return false;
    }

    static const char* kHttpOnly[] = {"capabilities",  "request_upload_urls",  "session_begin",
                                      "session_token", "session_echo_headers", "session_detach",
                                      "session_end"};
    bool http_only = false;
    for (const char* name : kHttpOnly) http_only = http_only || op == name;
    if (http_only) {
        if (!connected()) {
            write_response(driver_error("not connected"));
            return true;
        }
        if (!http_) {
            write_response(driver_error("op requires http transport"));
            return true;
        }
    }

    json response;
    if (op == "connect") {
        response = op_connect(request);
    } else if (op == "unary") {
        response = op_unary(request);
    } else if (op == "describe") {
        response = op_describe();
    } else if (op == "stream_open") {
        response = op_stream_open(request);
    } else if (op == "tick") {
        response = op_tick(request);
    } else if (op == "next_with_token") {
        response = op_next_with_token();
    } else if (op == "exchange") {
        response = op_exchange(request);
    } else if (op == "cancel") {
        response = op_cancel();
    } else if (op == "close") {
        response = op_close();
    } else if (op == "capabilities") {
        response = op_capabilities();
    } else if (op == "request_upload_urls") {
        response = op_request_upload_urls(request);
    } else if (op == "session_begin") {
        response = op_session_begin(request);
    } else if (op == "session_token") {
        response = op_session_token();
    } else if (op == "session_echo_headers") {
        response = op_session_echo_headers();
    } else if (op == "session_detach") {
        response = op_session_detach();
    } else if (op == "session_end") {
        response = op_session_end();
    } else {
        response = driver_error("unknown op: " + op);
    }
    write_response(response);
    return true;
}

}  // namespace

int main() {
    claim_control_channel();
    std::ios::sync_with_stdio(false);

    Driver driver;
    std::string line;
    while (std::getline(std::cin, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;
        json request = json::parse(line, nullptr, /*allow_exceptions=*/false);
        if (request.is_discarded() || !request.is_object()) {
            write_response(driver_error("bad json: control line is not a JSON object"));
            continue;
        }
        try {
            if (!driver.dispatch(request)) return 0;
        } catch (const std::exception& error) {
            write_response(driver_error(std::string("driver failure: ") + error.what()));
        }
    }
    // EOF on stdin is a shutdown, with the same teardown and no response.
    driver.shutdown();
    return 0;
}
