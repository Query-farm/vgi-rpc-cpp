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

/// A GET-only object store: the test writes an object and the server under
/// test fetches it, as it would from any storage behind an external pointer.
class LoopbackStorage {
public:
    LoopbackStorage() {
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
        const std::string id = std::to_string(objects_.size());
        objects_[id] = std::move(body);
        return base_url() + "/obj/" + id;
    }

private:
    httplib::Server server_;
    std::mutex mutex_;
    std::map<std::string, std::string> objects_;
    int port_ = 0;
    std::thread thread_;
};

/// One `Server::serve_http` on a background thread, with its bound port.
class ProbeServer {
public:
    explicit ProbeServer(const std::string& storage_url = "") {
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
    std::vector<std::string> seen;  // one entry per data row, in order
    std::string cursor;             // the next cursor, when one came back
    std::string call_token;         // only /init hands one over
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
