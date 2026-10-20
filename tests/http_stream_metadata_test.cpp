// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// A stream turn's input metadata over HTTP.
//
// The custom metadata on a stream's input batch is application data, and VGI
// depends on it per input: `vgi.cache.if_none_match` / `if_modified_since` ride
// every exchange input (conditional revalidation), `vgi_pushdown_filters`
// deltas ride every tick.  The pipe transport hands it to `process()` with the
// batch it arrived on.  Over HTTP each turn is its own request, and exchange
// input used to be rebuilt as a bare data batch -- nothing failed, a worker
// simply never saw a validator, and DuckDB's `cache/exchange_revalidate.test`
// was the first thing to notice.
//
// So the methods here report exactly the metadata they were handed and the
// cases compare that with what went on the wire.  Against a real server over
// real HTTP: the requests are built by hand -- the test plays the client --
// and every response is the server's own.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/crypto.h>
#include <vgi_rpc/client_external.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/http_config.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/server.h>
#include <vgi_rpc/stream.h>
#include <vgi_rpc/wire.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <catch2/catch_test_macros.hpp>
#include <httplib.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kProtocol = "MetadataProbe";
constexpr const char* kArrow = "application/vnd.apache.arrow.stream";

std::shared_ptr<arrow::Schema> input_schema() {
    return arrow::schema({arrow::field("value", arrow::int64())});
}

std::shared_ptr<arrow::Schema> seen_schema() {
    return arrow::schema({arrow::field("seen", arrow::utf8())});
}

std::shared_ptr<arrow::Schema> count_schema() {
    return arrow::schema({arrow::field("count", arrow::int64(), /*nullable=*/false)});
}

/// `k=v` pairs, sorted by key and `;`-joined; empty when there is no metadata.
std::string render(const std::shared_ptr<arrow::KeyValueMetadata>& md) {
    if (!md) return "";
    std::vector<std::pair<std::string, std::string>> pairs;
    for (int64_t i = 0; i < md->size(); ++i) pairs.emplace_back(md->key(i), md->value(i));
    std::sort(pairs.begin(), pairs.end());
    std::string out;
    for (const auto& [key, value] : pairs) {
        if (!out.empty()) out += ";";
        out += key + "=" + value;
    }
    return out;
}

void emit_seen(OutputCollector& out, const std::shared_ptr<arrow::KeyValueMetadata>& md) {
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(render(md)));
    out.emit_arrays({unwrap(builder.Finish())});
}

/// One output row per input, naming the metadata the input was handed.
class ReportingExchange : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        emit_seen(out, input.custom_metadata);
    }
};

/// A producer that overrides `process`, not `produce`: the tick's metadata is
/// the thing under test, and `ProducerState` drops the tick on the floor.
class ReportingProducer : public StreamState {
public:
    explicit ReportingProducer(int64_t count) : count_(count) {}

    void process(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        emit_seen(out, input.custom_metadata);
        if (++turns_ >= count_) out.finish();
    }

private:
    int64_t count_;
    int64_t turns_ = 0;
};

/// One output row per input, carrying the per-batch keys a VGI worker puts on
/// its exchange output, plus a log batch ahead of it.  It also sets the
/// framing cursor's own key, which the transport must overwrite.
class AnnotatingExchange : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        const auto value = std::static_pointer_cast<arrow::Int64Array>(input.batch->column(0));
        const std::string index = std::to_string(value->Value(0));
        out.client_log(LogLevel::INFO, "turn " + index);
        arrow::StringBuilder builder;
        VGI_RPC_THROW_NOT_OK(builder.Append("row " + index));
        auto md = std::make_shared<arrow::KeyValueMetadata>();
        md->Append("vgi_rpc.parent_row#b64", "AAAAAA==");
        md->Append("vgi_batch_index", index);
        md->Append("vgi.cache.max_age", "60");
        md->Append(keys::STATE_B64, "forged-by-worker");
        out.emit_batch(arrow::RecordBatch::Make(seen_schema(), 1, {unwrap(builder.Finish())}), md);
    }
};

/// One data row per tick, then a final tick that only logs and finishes: the
/// log-only cycle the externalizer must leave inline.
class LogFinishingProducer : public ProducerState {
public:
    explicit LogFinishingProducer(int64_t count) : count_(count) {}

    void produce(OutputCollector& out, CallContext&) override {
        if (turns_ >= count_) {
            out.client_log(LogLevel::INFO, std::string(256, 'x'));
            out.finish();
            return;
        }
        arrow::StringBuilder builder;
        VGI_RPC_THROW_NOT_OK(builder.Append("row " + std::to_string(turns_++)));
        out.emit_arrays({unwrap(builder.Finish())});
    }

private:
    int64_t count_;
    int64_t turns_ = 0;
};

/// A minimal object store: `POST /alloc` hands out a URL that `PUT` fills and
/// `GET` serves, as the HTTP external-storage backend expects.  The test can
/// also write an object directly and read back whatever the server uploaded.
class LoopbackStorage {
public:
    LoopbackStorage() {
        server_.Post("/alloc", [this](const httplib::Request&, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mutex_);
            const std::string id = std::to_string(next_id_++);
            // Not application/json: httplib would compress that for a client
            // advertising Accept-Encoding, and this is a fixture, not a test
            // of the storage client's content negotiation.
            res.set_content("{\"object_url\": \"" + base_url() + "/obj/" + id + "\"}",
                            "application/octet-stream");
        });
        server_.Put(R"(/obj/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mutex_);
            objects_[req.matches[1].str()] = req.body;
            ++uploads_;
            res.status = 204;
        });
        server_.Get(R"(/obj/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = objects_.find(req.matches[1].str());
            if (it == objects_.end()) {
                res.status = 404;
                return;
            }
            res.set_content(it->second, "application/octet-stream");
        });
        port_ = server_.bind_to_any_port("127.0.0.1");
        REQUIRE(port_ > 0);
        thread_ = std::thread([this] { (void)server_.listen_after_bind(); });
        for (int attempt = 0; attempt < 1000 && !server_.is_running(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        REQUIRE(server_.is_running());
    }

    ~LoopbackStorage() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string base_url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    /// Store `body` and return the URL it is served from.
    std::string put(std::string body) {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::string id = std::to_string(next_id_++);
        objects_[id] = std::move(body);
        return base_url() + "/obj/" + id;
    }

    /// The object behind a URL this store handed out.
    std::string get(const std::string& url) {
        const std::string prefix = base_url() + "/obj/";
        REQUIRE(url.rfind(prefix, 0) == 0);
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = objects_.find(url.substr(prefix.size()));
        REQUIRE(it != objects_.end());
        return it->second;
    }

    /// How many objects the server under test has uploaded.
    int uploads() {
        std::lock_guard<std::mutex> lock(mutex_);
        return uploads_;
    }

private:
    httplib::Server server_;
    std::mutex mutex_;
    std::map<std::string, std::string> objects_;
    int next_id_ = 0;
    int uploads_ = 0;
    int port_ = 0;
    std::thread thread_;
};

/// One `Server::serve_http` on a background thread, with its bound port.
class ProbeServer {
public:
    explicit ProbeServer(const std::string& storage_url = "",
                         std::optional<int64_t> externalize_threshold = std::nullopt) {
        ServerBuilder builder;
        builder.add_exchange("exchange_probe", empty_schema(), input_schema(), seen_schema(),
                             [](const Request&, CallContext&) {
                                 return Stream{seen_schema(), input_schema(),
                                               std::make_shared<ReportingExchange>(), nullptr};
                             });
        builder.add_producer(
            "produce_probe", count_schema(), seen_schema(), [](const Request& req, CallContext&) {
                return Stream{seen_schema(), empty_schema(),
                              std::make_shared<ReportingProducer>(req.get<int64_t>("count")),
                              nullptr};
            });
        builder.add_exchange("annotated_exchange", empty_schema(), input_schema(), seen_schema(),
                             [](const Request&, CallContext&) {
                                 return Stream{seen_schema(), input_schema(),
                                               std::make_shared<AnnotatingExchange>(), nullptr};
                             });
        builder.add_producer(
            "log_finishing", count_schema(), seen_schema(), [](const Request& req, CallContext&) {
                return Stream{seen_schema(), empty_schema(),
                              std::make_shared<LogFinishingProducer>(req.get<int64_t>("count")),
                              nullptr};
            });
        builder.protocol(kProtocol);
        server_ = builder.build();

        HttpConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 0;
        cfg.prefix = "/vgi";
        if (!storage_url.empty()) {
            cfg.external_storage_url = storage_url;
            cfg.external_url_validator = [](const std::string&) {};  // loopback http
        }
        if (externalize_threshold) {
            cfg.externalize_threshold = *externalize_threshold;
            // The threshold otherwise doubles as the inline request ceiling,
            // and these cases externalize responses, not requests.
            cfg.max_request_bytes = 1 << 20;
        }
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

    ~ProbeServer() {
        // cpp-httplib owns the accept loop inside serve_http; the process ends
        // the thread.  Detach so one wedged case cannot hang the binary, and
        // leak the server the detached loop is still running on rather than
        // free it underneath that loop.
        thread_.detach();
        (void)server_.release();
    }

    int port() const { return port_; }

private:
    std::unique_ptr<Server> server_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable ready_;
    int port_ = 0;
};

std::shared_ptr<arrow::KeyValueMetadata> metadata(
    std::initializer_list<std::pair<std::string, std::string>> entries) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    for (const auto& [key, value] : entries) md->Append(key, value);
    return md;
}

std::string ipc_body(const std::shared_ptr<arrow::Schema>& schema,
                     std::shared_ptr<arrow::RecordBatch> batch,
                     std::shared_ptr<arrow::KeyValueMetadata> md) {
    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(sink, schema,
                     {AnnotatedBatch::with_metadata(std::move(batch), std::move(md))});
    auto buffer = unwrap(sink->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

/// A stream-opening request: the dispatch envelope plus any extra keys.
std::string init_body(const std::string& method, std::shared_ptr<arrow::RecordBatch> params,
                      std::initializer_list<std::pair<std::string, std::string>> extra = {}) {
    auto md = metadata({{keys::METHOD, method},
                        {keys::REQUEST_VERSION, REQUEST_VERSION_VALUE},
                        {keys::PROTOCOL, kProtocol}});
    for (const auto& [key, value] : extra) md->Append(key, value);
    auto schema = params->schema();
    return ipc_body(schema, std::move(params), std::move(md));
}

std::shared_ptr<arrow::RecordBatch> count_params(int64_t count) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(count));
    return arrow::RecordBatch::Make(count_schema(), 1, {unwrap(builder.Finish())});
}

std::shared_ptr<arrow::RecordBatch> input_batch(int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(input_schema(), 1, {unwrap(builder.Finish())});
}

struct Turn {
    std::vector<std::string> seen;        // one entry per data row, in order
    std::string cursor;                   // the next cursor, when one came back
    std::string call_token;               // only /init hands one over
    std::vector<AnnotatedBatch> batches;  // the response as it came off the wire
};

Turn post(int port, const std::string& path, const std::string& body) {
    httplib::Client client("127.0.0.1", port);
    client.set_read_timeout(10, 0);
    // Uncompressed, so the body is the IPC stream as written.
    httplib::Headers headers{{"Accept-Encoding", "identity"},
                             {"X-VGI-Accept-Encoding", "identity"}};
    auto response = client.Post(path, headers, body, kArrow);
    REQUIRE(response);
    INFO("POST " << path << " -> " << response->status);
    REQUIRE(response->status == 200);
    REQUIRE_FALSE(response->has_header("X-VGI-RPC-Error"));

    auto buffer = arrow::Buffer::FromString(response->body);
    auto contents = read_ipc_stream(std::make_shared<arrow::io::BufferReader>(buffer));
    REQUIRE(contents);
    Turn turn;
    turn.batches = contents->batches;
    for (const auto& ab : contents->batches) {
        if (ab.batch->num_rows() > 0 && ab.batch->schema()->GetFieldIndex("seen") >= 0) {
            auto column =
                std::static_pointer_cast<arrow::StringArray>(ab.batch->GetColumnByName("seen"));
            for (int64_t i = 0; i < column->length(); ++i)
                turn.seen.push_back(column->GetString(i));
        }
        if (const auto cursor = get_metadata_value(ab.custom_metadata, keys::STATE_B64);
            !cursor.empty()) {
            turn.cursor = cursor;
        }
        if (const auto call = get_metadata_value(ab.custom_metadata, keys::CALL_STATE_B64);
            !call.empty()) {
            turn.call_token = call;
        }
    }
    return turn;
}

/// The transport's own keys, which must never reach application code.
void check_no_transport_keys(const std::string& seen) {
    CHECK(seen.find(keys::STATE_B64) == std::string::npos);
    CHECK(seen.find(keys::CALL_STATE_B64) == std::string::npos);
    CHECK(seen.find(keys::CANCEL) == std::string::npos);
}

const std::string kExchangePath = std::string("/vgi/") + kProtocol + "/exchange_probe";
const std::string kProducePath = std::string("/vgi/") + kProtocol + "/produce_probe";
const std::string kAnnotatedPath = std::string("/vgi/") + kProtocol + "/annotated_exchange";

bool is_log(const AnnotatedBatch& ab) {
    return ab.batch->num_rows() == 0 && ab.custom_metadata &&
           ab.custom_metadata->FindKey(keys::LOG_LEVEL) >= 0;
}

/// The location of the one pointer batch in a response, or empty when the
/// response carried none.
std::string pointer_url(const Turn& turn) {
    std::string url;
    for (const auto& ab : turn.batches) {
        if (const auto location = get_metadata_value(ab.custom_metadata, keys::LOCATION);
            !location.empty()) {
            REQUIRE(url.empty());
            REQUIRE(ab.batch->num_rows() == 0);
            url = location;
        }
    }
    return url;
}

/// Fetch, verify and decode the object a pointer batch names.
IpcStreamContents resolve(LoopbackStorage& storage, const Turn& turn) {
    const std::string url = pointer_url(turn);
    REQUIRE_FALSE(url.empty());
    const std::string payload = storage.get(url);
    crypto::Sha256 digest;
    digest.update(payload);
    std::string sha256;
    for (const auto& ab : turn.batches) {
        if (const auto v = get_metadata_value(ab.custom_metadata, keys::LOCATION_SHA256);
            !v.empty()) {
            sha256 = v;
        }
    }
    CHECK(sha256 == digest.hex_digest());
    auto contents = read_ipc_stream(
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(payload)));
    REQUIRE(contents);
    return std::move(*contents);
}

HttpClient probe_client(int port, std::function<void(const AnnotatedBatch&)> on_log = {}) {
    HttpClientConfig config;
    config.prefix = "/vgi";
    config.protocol = kProtocol;
    config.on_log = std::move(on_log);
    ClientExternalHttpOptions external;
    external.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
    return HttpClient::builder("http://127.0.0.1:" + std::to_string(port))
        .config(config)
        .external_http_options(external)
        .build();
}

}  // namespace

TEST_CASE("an exchange turn hands process() its input batch's metadata", "[http-stream-metadata]") {
    ProbeServer server;
    const Turn init = post(server.port(), kExchangePath + "/init",
                           init_body("exchange_probe", make_empty_batch(empty_schema())));
    REQUIRE_FALSE(init.cursor.empty());
    REQUIRE_FALSE(init.call_token.empty());

    // Three turns, each carrying its OWN metadata: a server that froze the
    // first turn's and replayed it, or that carried one turn's into the next,
    // fails a later turn rather than passing on the first.  The last carries
    // nothing of its own, so it must see nothing.
    const std::vector<std::vector<std::pair<std::string, std::string>>> per_turn = {
        {{"vgi.cache.if_none_match", "\"etag-1\""}},
        {{"vgi.cache.if_none_match", "\"etag-2\""},
         {"vgi.cache.if_modified_since", "Tue, 15 Sep 2026 12:00:00 GMT"}},
        {},
    };
    const std::vector<std::string> expected = {
        "vgi.cache.if_none_match=\"etag-1\"",
        "vgi.cache.if_modified_since=Tue, 15 Sep 2026 12:00:00 GMT;"
        "vgi.cache.if_none_match=\"etag-2\"",
        "",
    };

    std::string cursor = init.cursor;
    for (size_t i = 0; i < per_turn.size(); ++i) {
        auto md = metadata({{keys::STATE_B64, cursor}, {keys::CALL_STATE_B64, init.call_token}});
        for (const auto& [key, value] : per_turn[i]) md->Append(key, value);
        const Turn turn = post(server.port(), kExchangePath + "/exchange",
                               ipc_body(input_schema(), input_batch(static_cast<int64_t>(i)), md));
        INFO("exchange turn " << i + 1);
        REQUIRE(turn.seen.size() == 1);
        CHECK(turn.seen[0] == expected[i]);
        check_no_transport_keys(turn.seen[0]);
        REQUIRE_FALSE(turn.cursor.empty());
        cursor = turn.cursor;
    }
}

TEST_CASE("an externalized exchange input delivers the payload's metadata, with provenance",
          "[http-stream-metadata]") {
    LoopbackStorage storage;
    ProbeServer server(storage.base_url());
    const Turn init = post(server.port(), kExchangePath + "/init",
                           init_body("exchange_probe", make_empty_batch(empty_schema())));
    REQUIRE_FALSE(init.cursor.empty());

    // The whole inline turn goes to storage, as a client externalizing an
    // oversized request does; the pointer carries the tokens too.  The two
    // disagree on `vgi.probe.side` so the case can tell which one was read:
    // resolved metadata is the fetched batch's, never the pointer's (§12).
    auto tokens = [&]() {
        return metadata({{keys::STATE_B64, init.cursor}, {keys::CALL_STATE_B64, init.call_token}});
    };
    auto inner_md = tokens();
    inner_md->Append("vgi.cache.if_none_match", "\"etag-external\"");
    inner_md->Append("vgi.probe.side", "payload");
    const std::string payload = ipc_body(input_schema(), input_batch(7), inner_md);
    const std::string url = storage.put(payload);

    crypto::Sha256 digest;
    digest.update(payload);
    auto pointer_md = tokens();
    pointer_md->Append("vgi.probe.side", "pointer");
    pointer_md->Append(keys::LOCATION, url);
    pointer_md->Append(keys::LOCATION_SHA256, digest.hex_digest());
    const Turn turn =
        post(server.port(), kExchangePath + "/exchange",
             ipc_body(input_schema(), make_empty_batch(input_schema()), std::move(pointer_md)));

    REQUIRE(turn.seen.size() == 1);
    const std::string& seen = turn.seen[0];
    INFO("seen: " << seen);
    CHECK(seen.find("vgi.cache.if_none_match=\"etag-external\"") != std::string::npos);
    CHECK(seen.find("vgi.probe.side=payload") != std::string::npos);
    CHECK(seen.find("vgi.probe.side=pointer") == std::string::npos);
    // The reader stamps where the payload came from, in full ...
    CHECK(seen.find(std::string(keys::LOCATION_SOURCE) + "=" + url) != std::string::npos);
    CHECK(seen.find(std::string(keys::LOCATION_FETCH_MS) + "=") != std::string::npos);
    // ... and the pointer itself is gone.
    CHECK(seen.find(std::string(keys::LOCATION) + "=") == std::string::npos);
    CHECK(seen.find(keys::LOCATION_SHA256) == std::string::npos);
    check_no_transport_keys(seen);
}

TEST_CASE("a producer turn hands process() its tick's metadata, less the transport's",
          "[http-stream-metadata]") {
    ProbeServer server;
    // /init drives the first tick, so its request metadata is that tick's --
    // the only way a first-tick validator reaches a producer over HTTP.
    const Turn init = post(server.port(), kProducePath + "/init",
                           init_body("produce_probe", count_params(3),
                                     {{"vgi.cache.if_none_match", "\"etag-init\""}}));
    REQUIRE(init.seen.size() == 1);
    CHECK(init.seen[0].find("vgi.cache.if_none_match=\"etag-init\"") != std::string::npos);
    check_no_transport_keys(init.seen[0]);
    REQUIRE_FALSE(init.cursor.empty());
    REQUIRE_FALSE(init.call_token.empty());

    // Each continuation's tick carries its own filter delta.  Exact equality:
    // the tokens that addressed the turn are bookkeeping, not tick metadata,
    // and the cursor is a sealed token user code must not be able to read.
    std::string cursor = init.cursor;
    for (const std::string delta : {"delta-1", "delta-2"}) {
        auto md = metadata({{keys::STATE_B64, cursor},
                            {keys::CALL_STATE_B64, init.call_token},
                            {"vgi_pushdown_filters", delta}});
        const Turn turn = post(server.port(), kProducePath + "/exchange",
                               ipc_body(empty_schema(), make_empty_batch(empty_schema()), md));
        INFO("producer continuation " << delta);
        REQUIRE(turn.seen.size() == 1);
        CHECK(turn.seen[0] == "vgi_pushdown_filters=" + delta);
        cursor = turn.cursor;
    }
}

// Exchange output is externalized like producer output.  Every other port
// does so; this one wrote every exchange turn inline whatever its size, so a
// configured externalize_threshold silently bounded nothing on the exchange
// path and an oversized turn was refused at the response cap instead.
TEST_CASE("an over-threshold exchange turn is externalized with its metadata and cursor",
          "[http-stream-metadata][external]") {
    LoopbackStorage storage;
    ProbeServer server(storage.base_url(), /*externalize_threshold=*/1);
    const Turn init = post(server.port(), kAnnotatedPath + "/init",
                           init_body("annotated_exchange", make_empty_batch(empty_schema())));
    REQUIRE_FALSE(init.cursor.empty());
    REQUIRE_FALSE(init.call_token.empty());

    std::string cursor = init.cursor;
    for (int64_t i = 0; i < 3; ++i) {
        INFO("exchange turn " << i + 1);
        auto md = metadata({{keys::STATE_B64, cursor}, {keys::CALL_STATE_B64, init.call_token}});
        const Turn turn = post(server.port(), kAnnotatedPath + "/exchange",
                               ipc_body(input_schema(), input_batch(i), md));
        // Nothing but the pointer on the wire: the log went with the data.
        REQUIRE(turn.batches.size() == 1);
        CHECK(turn.seen.empty());
        CHECK(storage.uploads() == i + 1);

        const IpcStreamContents resolved = resolve(storage, turn);
        REQUIRE(resolved.batches.size() == 2);
        CHECK(is_log(resolved.batches[0]));
        CHECK(get_metadata_value(resolved.batches[0].custom_metadata, keys::LOG_MESSAGE) ==
              "turn " + std::to_string(i));
        const AnnotatedBatch& data = resolved.batches[1];
        REQUIRE(data.batch->num_rows() == 1);
        CHECK(std::static_pointer_cast<arrow::StringArray>(data.batch->column(0))->GetString(0) ==
              "row " + std::to_string(i));
        // The worker's own per-batch keys ride in the resolved object ...
        CHECK(get_metadata_value(data.custom_metadata, "vgi_rpc.parent_row#b64") == "AAAAAA==");
        CHECK(get_metadata_value(data.custom_metadata, "vgi_batch_index") == std::to_string(i));
        CHECK(get_metadata_value(data.custom_metadata, "vgi.cache.max_age") == "60");
        // ... and so does the cursor, applied last so the worker's same-named
        // key cannot stand in for it.
        const std::string next = get_metadata_value(data.custom_metadata, keys::STATE_B64);
        REQUIRE_FALSE(next.empty());
        CHECK(next != "forged-by-worker");
        CHECK(data.custom_metadata->FindKey(keys::LOCATION) < 0);
        // The next turn resumes from the cursor read off the resolved batch.
        cursor = next;
    }
}

TEST_CASE("a below-threshold exchange turn stays inline", "[http-stream-metadata][external]") {
    LoopbackStorage storage;
    ProbeServer server(storage.base_url(), /*externalize_threshold=*/1 << 20);
    const Turn init = post(server.port(), kAnnotatedPath + "/init",
                           init_body("annotated_exchange", make_empty_batch(empty_schema())));
    std::string cursor = init.cursor;
    for (int64_t i = 0; i < 2; ++i) {
        auto md = metadata({{keys::STATE_B64, cursor}, {keys::CALL_STATE_B64, init.call_token}});
        const Turn turn = post(server.port(), kAnnotatedPath + "/exchange",
                               ipc_body(input_schema(), input_batch(i), md));
        CHECK(pointer_url(turn).empty());
        REQUIRE(turn.seen.size() == 1);
        CHECK(turn.seen[0] == "row " + std::to_string(i));
        REQUIRE_FALSE(turn.cursor.empty());
        CHECK(turn.cursor != "forged-by-worker");
        cursor = turn.cursor;
    }
    CHECK(storage.uploads() == 0);
}

TEST_CASE("the HTTP client resolves externalized exchange turns and continues the stream",
          "[http-stream-metadata][external]") {
    LoopbackStorage storage;
    ProbeServer server(storage.base_url(), /*externalize_threshold=*/1);
    std::vector<std::string> logs;
    auto client = probe_client(server.port(), [&](const AnnotatedBatch& ab) {
        logs.push_back(get_metadata_value(ab.custom_metadata, keys::LOG_MESSAGE));
    });
    auto session = client.open_exchange("annotated_exchange",
                                        AnnotatedBatch::data(make_empty_batch(empty_schema())),
                                        input_schema(), seen_schema());
    for (int64_t i = 0; i < 3; ++i) {
        INFO("exchange turn " << i + 1);
        const AnnotatedBatch out = session.exchange(AnnotatedBatch::data(input_batch(i)));
        REQUIRE(out.batch->num_rows() == 1);
        CHECK(std::static_pointer_cast<arrow::StringArray>(out.batch->column(0))->GetString(0) ==
              "row " + std::to_string(i));
        CHECK(get_metadata_value(out.custom_metadata, "vgi_rpc.parent_row#b64") == "AAAAAA==");
        CHECK(get_metadata_value(out.custom_metadata, "vgi_batch_index") == std::to_string(i));
        CHECK(get_metadata_value(out.custom_metadata, "vgi.cache.max_age") == "60");
    }
    CHECK(storage.uploads() == 3);
    CHECK(logs == std::vector<std::string>{"turn 0", "turn 1", "turn 2"});
}

// The reference's maybe_externalize_collector leaves a cycle with no data
// batch inline however large it is.  This port externalized one, and since a
// resolved payload must hold exactly one data batch, a producer whose last
// turn only logged and finished failed in the client.
TEST_CASE("a log-only producer cycle stays inline under a tiny threshold",
          "[http-stream-metadata][external]") {
    LoopbackStorage storage;
    ProbeServer server(storage.base_url(), /*externalize_threshold=*/1);
    const std::string path = std::string("/vgi/") + kProtocol + "/log_finishing";

    // On the wire: the data turn is externalized, the log-only finish is not.
    const Turn init =
        post(server.port(), path + "/init", init_body("log_finishing", count_params(1)));
    CHECK_FALSE(pointer_url(init).empty());
    REQUIRE_FALSE(init.cursor.empty());
    auto md = metadata({{keys::STATE_B64, init.cursor}, {keys::CALL_STATE_B64, init.call_token}});
    const Turn last = post(server.port(), path + "/exchange",
                           ipc_body(empty_schema(), make_empty_batch(empty_schema()), md));
    CHECK(pointer_url(last).empty());
    REQUIRE(last.batches.size() == 1);
    CHECK(is_log(last.batches[0]));
    CHECK(last.cursor.empty());
    CHECK(storage.uploads() == 1);

    // And through the client, which refuses a pointer to a data-less payload.
    auto client = probe_client(server.port());
    auto stream =
        client.open_producer("log_finishing", AnnotatedBatch::data(count_params(2)), seen_schema());
    int rows = 0;
    while (auto batch = stream.tick()) rows += static_cast<int>(batch->batch->num_rows());
    CHECK(rows == 2);
    CHECK(stream.finished());
}
