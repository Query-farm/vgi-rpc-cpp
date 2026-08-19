// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/wire.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::int64(), true)});
}

std::shared_ptr<arrow::RecordBatch> value_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
}

AnnotatedBatch empty_request() {
    return AnnotatedBatch::data(make_empty_batch(empty_schema()));
}

std::string encode_response(const std::shared_ptr<arrow::Schema>& schema,
                            std::vector<AnnotatedBatch> batches) {
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, schema, batches);
    auto buffer = unwrap(output->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

std::string unary_response(const std::shared_ptr<arrow::Schema>& schema, int64_t value) {
    return encode_response(schema, {AnnotatedBatch::data(value_batch(schema, value))});
}

std::string exchange_init_response(const std::shared_ptr<arrow::Schema>& schema,
                                   const std::string& cursor, const std::string& call_state) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::STATE_B64, cursor);
    metadata->Append(keys::CALL_STATE_B64, call_state);
    return encode_response(
        schema, {AnnotatedBatch::with_metadata(make_empty_batch(schema), std::move(metadata))});
}

std::string ambiguous_exchange_response(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<AnnotatedBatch> batches;
    for (int64_t value : {10, 20}) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(keys::STATE_B64, "next-" + std::to_string(value));
        batches.push_back(
            AnnotatedBatch::with_metadata(value_batch(schema, value), std::move(metadata)));
    }
    return encode_response(schema, std::move(batches));
}

std::string pointer_response(const std::shared_ptr<arrow::Schema>& schema, const char* key) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(key, "untrusted-pointer");
    return encode_response(
        schema, {AnnotatedBatch::with_metadata(value_batch(schema, 99), std::move(metadata))});
}

bool request_has_cancel(const httplib::Request& request) {
    auto buffer = arrow::Buffer::FromString(request.body);
    auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) return false;
    const auto& metadata = contents->batches[0].custom_metadata;
    return get_metadata_value(metadata, keys::CANCEL) == "1" &&
           !get_metadata_value(metadata, keys::STATE_B64).empty() &&
           !get_metadata_value(metadata, keys::CALL_STATE_B64).empty();
}

bool request_omits_pointer_controls(const httplib::Request& request) {
    auto buffer = arrow::Buffer::FromString(request.body);
    auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) return false;
    const auto& metadata = contents->batches[0].custom_metadata;
    if (!metadata) return true;
    for (const char* key :
         {keys::LOCATION, keys::LOCATION_SHA256, keys::LOCATION_SOURCE, keys::LOCATION_FETCH_MS,
          keys::SHM_OFFSET, keys::SHM_LENGTH, keys::SHM_SOURCE, keys::SHM_SEGMENT_NAME,
          keys::SHM_SEGMENT_SIZE, keys::TRANSPORT_SHM}) {
        if (metadata->FindKey(key) >= 0) return false;
    }
    return true;
}

class FaultServer {
public:
    explicit FaultServer(std::shared_ptr<arrow::Schema> schema)
        : schema_(std::move(schema)), good_response_(unary_response(schema_, 7)) {
        install_handlers();
        port_ = server_.bind_to_any_port("127.0.0.1");
        if (port_ <= 0) throw std::runtime_error("failed to bind local HTTP fault server");
        thread_ = std::thread([this] { (void)server_.listen_after_bind(); });
        for (int i = 0; i < 1000 && !server_.is_running(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!server_.is_running()) {
            stop();
            throw std::runtime_error("local HTTP fault server did not start");
        }
    }

    ~FaultServer() { stop(); }

    FaultServer(const FaultServer&) = delete;
    FaultServer& operator=(const FaultServer&) = delete;

    std::string origin() const { return "http://127.0.0.1:" + std::to_string(port_); }
    const std::string& good_response() const { return good_response_; }

    std::atomic<int> preflight_requests{0};
    std::atomic<int> recovery_requests{0};
    std::atomic<int> ambiguous_exchange_requests{0};
    std::atomic<int> explicit_close_exchange_requests{0};
    std::atomic<int> destructor_exchange_requests{0};
    std::atomic<int> cancel_exchange_requests{0};
    std::atomic<bool> cancel_metadata_valid{false};
    std::atomic<bool> pointer_metadata_sanitized{false};

private:
    void arrow_response(httplib::Response& response, const std::string& body) {
        response.set_content(body, kArrowContentType);
    }

    void install_init(const std::string& method) {
        server_.Post(
            "/" + method + "/init", [this](const httplib::Request&, httplib::Response& response) {
                arrow_response(response, exchange_init_response(schema_, "cursor", "call-state"));
            });
    }

    void install_handlers() {
        server_.Post("/preflight", [this](const httplib::Request&, httplib::Response& response) {
            ++preflight_requests;
            arrow_response(response, good_response_);
        });
        server_.Post("/ok", [this](const httplib::Request&, httplib::Response& response) {
            ++recovery_requests;
            arrow_response(response, good_response_);
        });

        server_.Post("/fixed-large", [this](const httplib::Request&, httplib::Response& response) {
            response.set_content(std::string(good_response_.size() + 4096, 'f'), kArrowContentType);
        });
        server_.Post(
            "/chunked-large", [this](const httplib::Request&, httplib::Response& response) {
                auto payload = std::make_shared<std::string>(good_response_.size() + 4096, 'c');
                response.set_chunked_content_provider(
                    kArrowContentType, [payload](size_t offset, httplib::DataSink& sink) {
                        if (offset >= payload->size()) {
                            sink.done();
                            return true;
                        }
                        const size_t count = std::min<size_t>(128, payload->size() - offset);
                        if (!sink.write(payload->data() + offset, count)) return false;
                        if (offset + count == payload->size()) sink.done();
                        return true;
                    });
            });
        server_.Post("/unsupported-encoding",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("Content-Encoding", "br");
                     });
        server_.Post("/unsupported-type",
                     [this](const httplib::Request&, httplib::Response& response) {
                         response.set_content(good_response_, "application/octet-stream");
                     });
        server_.Post("/header-error", [this](const httplib::Request&, httplib::Response& response) {
            arrow_response(response, good_response_);
            response.set_header("X-VGI-RPC-Error", "true");
        });
        server_.Post("/arrow-400", [this](const httplib::Request&, httplib::Response& response) {
            arrow_response(response, good_response_);
            response.status = 400;
        });
        server_.Post("/metadata-sanitize",
                     [this](const httplib::Request& request, httplib::Response& response) {
                         pointer_metadata_sanitized.store(request_omits_pointer_controls(request));
                         arrow_response(response, good_response_);
                     });
        server_.Post("/external-pointer",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, pointer_response(schema_, keys::LOCATION));
                     });
        server_.Post("/shm-pointer", [this](const httplib::Request&, httplib::Response& response) {
            arrow_response(response, pointer_response(schema_, keys::SHM_OFFSET));
        });

        install_init("ambiguous");
        server_.Post("/ambiguous/exchange",
                     [this](const httplib::Request&, httplib::Response& response) {
                         ++ambiguous_exchange_requests;
                         arrow_response(response, ambiguous_exchange_response(schema_));
                     });

        install_init("explicit-close");
        server_.Post("/explicit-close/exchange",
                     [this](const httplib::Request&, httplib::Response& response) {
                         ++explicit_close_exchange_requests;
                         arrow_response(response, good_response_);
                     });
        install_init("destructor");
        server_.Post("/destructor/exchange",
                     [this](const httplib::Request&, httplib::Response& response) {
                         ++destructor_exchange_requests;
                         arrow_response(response, good_response_);
                     });

        install_init("cancel");
        server_.Post("/cancel/exchange", [this](const httplib::Request& request,
                                                httplib::Response& response) {
            ++cancel_exchange_requests;
            try {
                cancel_metadata_valid.store(request_has_cancel(request));
            } catch (const std::exception&) {
                cancel_metadata_valid.store(false);
            }
            arrow_response(response, encode_response(empty_schema(), {empty_request()}));
        });
    }

    void stop() noexcept {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::string good_response_;
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
};

template <typename Function>
HttpClientError require_http_client_error(Function&& function, const std::string& context) {
    try {
        std::forward<Function>(function)();
    } catch (const HttpClientError& error) {
        return error;
    }
    throw std::runtime_error(context + " did not throw HttpClientError");
}

void test_request_preflight(const FaultServer& server, const std::string& origin) {
    HttpClientConfig config;
    config.prefix = "";
    config.max_request_bytes = 1;
    HttpClient client(origin, config);
    (void)require_http_client_error([&] { (void)client.call("preflight", empty_request()); },
                                    "request preflight cap");
    require(server.preflight_requests.load() == 0,
            "request exceeding the local preflight cap reached the server");
}

void test_response_caps(FaultServer& server, const std::string& origin,
                        const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    config.max_response_bytes = static_cast<int64_t>(server.good_response().size() + 32);
    HttpClient client(origin, config);

    (void)require_http_client_error(
        [&] { (void)client.call("fixed-large", empty_request(), schema); },
        "fixed-length response cap");
    auto fixed_recovery = client.call("ok", empty_request(), schema);
    require(fixed_recovery.batch && fixed_recovery.batch->num_rows() == 1,
            "client did not recover after fixed-length response overflow");

    (void)require_http_client_error(
        [&] { (void)client.call("chunked-large", empty_request(), schema); },
        "chunked response cap");
    auto chunked_recovery = client.call("ok", empty_request(), schema);
    require(chunked_recovery.batch && chunked_recovery.batch->num_rows() == 1,
            "client did not recover after chunked response overflow");
    require(server.recovery_requests.load() == 2,
            "response-cap recovery requests did not both reach the server");
}

void test_response_headers(const std::string& origin,
                           const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);

    const auto encoding_error = require_http_client_error(
        [&] { (void)client.call("unsupported-encoding", empty_request(), schema); },
        "unsupported Content-Encoding");
    require(std::string(encoding_error.what()).find("Content-Encoding") != std::string::npos,
            "unsupported Content-Encoding produced the wrong error");

    const auto type_error = require_http_client_error(
        [&] { (void)client.call("unsupported-type", empty_request(), schema); },
        "unsupported Content-Type");
    require(std::string(type_error.what()).find("Content-Type") != std::string::npos,
            "unsupported Content-Type produced the wrong error");

    const auto rpc_header_error = require_http_client_error(
        [&] { (void)client.call("header-error", empty_request(), schema); },
        "X-VGI-RPC-Error without an exception envelope");
    require(rpc_header_error.http_status() == 200,
            "X-VGI-RPC-Error failure did not preserve HTTP 200 status");
    require(std::string(rpc_header_error.what()).find("without an exception batch") !=
                std::string::npos,
            "X-VGI-RPC-Error without an exception produced the wrong error");

    const auto external_error = require_http_client_error(
        [&] { (void)client.call("external-pointer", empty_request(), schema); },
        "non-empty external pointer");
    require(std::string(external_error.what()).find("external-location") != std::string::npos,
            "non-empty external pointer was not rejected as a control batch");

    const auto shm_error = require_http_client_error(
        [&] { (void)client.call("shm-pointer", empty_request(), schema); },
        "non-empty shared-memory pointer");
    require(std::string(shm_error.what()).find("shared-memory") != std::string::npos,
            "non-empty shared-memory pointer was not rejected as a control batch");

    const auto status_error =
        require_http_client_error([&] { (void)client.call("arrow-400", empty_request(), schema); },
                                  "valid Arrow body under HTTP 400");
    require(status_error.http_status() == 400,
            "valid Arrow body under HTTP 400 did not preserve failure status");
    auto recovered = client.call("ok", empty_request(), schema);
    require(recovered.batch && recovered.batch->num_rows() == 1,
            "client did not recover after valid Arrow under HTTP 400");
}

void test_pointer_metadata_sanitization(FaultServer& server, const std::string& origin,
                                        const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    for (const char* key :
         {keys::LOCATION, keys::LOCATION_SHA256, keys::LOCATION_SOURCE, keys::LOCATION_FETCH_MS,
          keys::SHM_OFFSET, keys::SHM_LENGTH, keys::SHM_SOURCE, keys::SHM_SEGMENT_NAME,
          keys::SHM_SEGMENT_SIZE, keys::TRANSPORT_SHM}) {
        metadata->Append(key, "attacker-controlled");
    }
    (void)client.call(
        "metadata-sanitize",
        AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), std::move(metadata)),
        schema);
    require(server.pointer_metadata_sanitized.load(),
            "external-location or SHM control metadata reached the server");
}

void test_config_validation(const std::string& origin) {
    const auto require_invalid = [&](HttpClientConfig config, const std::string& context) {
        try {
            HttpClient client(origin, std::move(config));
        } catch (const std::invalid_argument&) {
            return;
        }
        throw std::runtime_error(context + " was accepted");
    };

    HttpClientConfig reserved;
    reserved.headers["Content-Type"] = "text/plain";
    require_invalid(std::move(reserved), "reserved transport header");

    HttpClientConfig crlf;
    crlf.headers["X-Test"] = "safe\r\ninjected: true";
    require_invalid(std::move(crlf), "CRLF-bearing header");

    HttpClientConfig credentials;
    credentials.headers["Authorization"] = "Bearer secret";
    require_invalid(std::move(credentials), "cleartext credential header");

    HttpClientConfig zero_cap;
    zero_cap.max_response_bytes = 0;
    require_invalid(std::move(zero_cap), "unbounded/zero response cap");
}

void test_ambiguous_exchange(FaultServer& server, const std::string& origin,
                             const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);
    auto session = client.open_exchange("ambiguous", empty_request(), schema, schema);
    (void)require_http_client_error(
        [&] { (void)session.exchange(AnnotatedBatch::data(value_batch(schema, 1))); },
        "ambiguous exchange response");
    require(!session.active(), "ambiguous exchange response did not poison the session");
    require(server.ambiguous_exchange_requests.load() == 1,
            "ambiguous exchange request did not reach the server exactly once");
    (void)require_http_client_error(
        [&] { (void)session.exchange(AnnotatedBatch::data(value_batch(schema, 2))); },
        "second exchange on poisoned session");
    require(server.ambiguous_exchange_requests.load() == 1,
            "poisoned exchange session resent an ambiguous request");
}

void test_local_close_and_cancel(FaultServer& server, const std::string& origin,
                                 const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);

    {
        auto session = client.open_exchange("explicit-close", empty_request(), schema, schema);
        session.close();
        session.close();
    }
    require(server.explicit_close_exchange_requests.load() == 0,
            "close emitted an exchange request");

    {
        auto session = client.open_exchange("destructor", empty_request(), schema, schema);
        require(session.active(), "new destructor-test session was not active");
    }
    require(server.destructor_exchange_requests.load() == 0,
            "exchange session destructor emitted a request");

    {
        auto session = client.open_exchange("cancel", empty_request(), schema, schema);
        session.cancel();
        session.cancel();
    }
    require(server.cancel_exchange_requests.load() == 1,
            "explicit cancel did not emit exactly one request");
    require(server.cancel_metadata_valid.load(),
            "explicit cancel request omitted cancel, cursor, or call-state metadata");
}

}  // namespace

int main() {
    try {
        const auto schema = value_schema();
        FaultServer server(schema);
        const std::string origin = server.origin();
        test_request_preflight(server, origin);
        test_response_caps(server, origin, schema);
        test_response_headers(origin, schema);
        test_pointer_metadata_sanitization(server, origin, schema);
        test_config_validation(origin);
        test_ambiguous_exchange(server, origin, schema);
        test_local_close_and_cancel(server, origin, schema);
    } catch (const std::exception& error) {
        std::cerr << "native HTTP client fault regression failed: " << error.what() << "\n";
        return 1;
    }
    return 0;
}
