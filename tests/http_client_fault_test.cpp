// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/wire.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>
#include <zstd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
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

AnnotatedBatch incompressible_request() {
    const auto schema = arrow::schema({arrow::field("x", arrow::binary(), true)});
    arrow::BinaryBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(""));
    return AnnotatedBatch::data(arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())}));
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

std::string zstd_encode(const std::string& body) {
    std::string encoded(ZSTD_compressBound(body.size()), '\0');
    const size_t size = ZSTD_compress(encoded.data(), encoded.size(), body.data(), body.size(), 3);
    if (ZSTD_isError(size)) throw std::runtime_error(ZSTD_getErrorName(size));
    encoded.resize(size);
    return encoded;
}

std::string request_body(const httplib::Request& request) {
    if (request.get_header_value("Content-Encoding") != "zstd") return request.body;
    const unsigned long long size =
        ZSTD_getFrameContentSize(request.body.data(), request.body.size());
    // cpp-httplib transparently decodes request bodies before invoking a
    // handler while retaining Content-Encoding.  In that case the bytes are
    // already Arrow rather than a zstd frame.
    if (size == ZSTD_CONTENTSIZE_ERROR) return request.body;
    if (size == ZSTD_CONTENTSIZE_UNKNOWN || size > 64 * 1024 * 1024) {
        throw std::runtime_error("invalid bounded zstd request in fault test");
    }
    std::string decoded(static_cast<size_t>(size), '\0');
    const size_t actual =
        ZSTD_decompress(decoded.data(), decoded.size(), request.body.data(), request.body.size());
    if (ZSTD_isError(actual) || actual != decoded.size()) {
        throw std::runtime_error("cannot decode zstd request in fault test");
    }
    return decoded;
}

bool request_has_cancel(const httplib::Request& request) {
    auto buffer = arrow::Buffer::FromString(request_body(request));
    auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) return false;
    const auto& metadata = contents->batches[0].custom_metadata;
    return get_metadata_value(metadata, keys::CANCEL) == "1" &&
           !get_metadata_value(metadata, keys::STATE_B64).empty() &&
           !get_metadata_value(metadata, keys::CALL_STATE_B64).empty();
}

std::string request_metadata_value(const httplib::Request& request, const char* key) {
    auto buffer = arrow::Buffer::FromString(request_body(request));
    auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) return {};
    return get_metadata_value(contents->batches[0].custom_metadata, key);
}

bool request_omits_pointer_controls(const httplib::Request& request) {
    auto buffer = arrow::Buffer::FromString(request_body(request));
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
    explicit FaultServer(std::shared_ptr<arrow::Schema> schema,
                         bool advertise_response_budget = true)
        : schema_(std::move(schema)),
          good_response_(unary_response(schema_, 7)),
          advertise_response_budget_(advertise_response_budget) {
        install_handlers();
        server_.set_post_routing_handler(
            [this](const httplib::Request&, httplib::Response& response) {
                const bool omit = response.has_header("X-Test-Omit-Budget-Support");
                response.headers.erase("X-Test-Omit-Budget-Support");
                if (advertise_response_budget_ && !omit &&
                    !response.has_header("VGI-Accept-Max-Response-Bytes-Support")) {
                    response.set_header("VGI-Accept-Max-Response-Bytes-Support", "true");
                }
            });
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
    void advertised_response_budget(std::string value, bool duplicate = false) {
        std::lock_guard<std::mutex> lock(capability_mutex_);
        advertised_response_budget_ = std::move(value);
        duplicate_advertised_response_budget_ = duplicate;
    }

    std::atomic<int> preflight_requests{0};
    std::atomic<int> encoded_request_cap_requests{0};
    std::atomic<int> recovery_requests{0};
    std::atomic<int> ambiguous_exchange_requests{0};
    std::atomic<int> explicit_close_exchange_requests{0};
    std::atomic<int> destructor_exchange_requests{0};
    std::atomic<int> cancel_exchange_requests{0};
    std::atomic<bool> cancel_metadata_valid{false};
    std::atomic<bool> pointer_metadata_sanitized{false};
    std::atomic<bool> compressed_request_valid{false};
    std::atomic<int> fallback_requests{0};
    std::atomic<int> advertised_identity_requests{0};
    std::atomic<int> exchange_fallback_requests{0};
    std::atomic<bool> fallback_request_ids_valid{true};
    std::atomic<bool> sticky_headers_valid{false};
    std::atomic<bool> sticky_block_entered{false};
    std::atomic<bool> sticky_block_release{false};
    std::atomic<bool> transport_block_entered{false};
    std::atomic<bool> transport_block_release{false};
    std::atomic<int> post_retry_requests{0};
    std::atomic<int> producer_retry_requests{0};
    std::atomic<bool> accepted_response_budget_seen{false};
    std::atomic<int64_t> last_accepted_response_budget{0};

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
        server_.Options(
            "/health", [this](const httplib::Request& request, httplib::Response& response) {
                response.status = 204;
                if (advertise_response_budget_) {
                    response.set_header("VGI-Accept-Max-Response-Bytes-Support", "true");
                }
                accepted_response_budget_seen.store(
                    request.get_header_value("VGI-Accept-Max-Response-Bytes") ==
                    std::to_string(256LL * 1024 * 1024));
                last_accepted_response_budget.store(
                    std::stoll(request.get_header_value("VGI-Accept-Max-Response-Bytes")));
                response.set_header("VGI-Sticky-Enabled", "true");
                response.set_header("VGI-Sticky-Default-TTL", "45");
                response.set_header("VGI-Sticky-Echo-Headers", "X-Tenant, X-Region");
                response.set_header("VGI-Max-Request-Bytes", "123456");
                {
                    std::lock_guard<std::mutex> lock(capability_mutex_);
                    response.set_header("VGI-Max-Response-Bytes", advertised_response_budget_);
                    if (duplicate_advertised_response_budget_) {
                        response.headers.emplace("VGI-Max-Response-Bytes", "65537");
                    }
                }
                response.set_header("VGI-Max-Externalized-Response-Bytes", "345678");
                response.set_header("VGI-Externalization-Enabled", "true");
                response.set_header("VGI-Upload-URL-Support", "true");
                response.set_header("VGI-Max-Upload-Bytes", "456789");
                response.set_header("VGI-Supported-Encodings", "ZSTD, gzip");
            });
        server_.Post("/preflight", [this](const httplib::Request&, httplib::Response& response) {
            ++preflight_requests;
            arrow_response(response, good_response_);
        });
        server_.Post("/encoded-request-cap",
                     [this](const httplib::Request&, httplib::Response& response) {
                         ++encoded_request_cap_requests;
                         arrow_response(response, good_response_);
                     });
        server_.Post("/ok", [this](const httplib::Request& request, httplib::Response& response) {
            ++recovery_requests;
            accepted_response_budget_seen.store(
                accepted_response_budget_seen.load() &&
                request.get_header_value("VGI-Accept-Max-Response-Bytes") ==
                    std::to_string(256LL * 1024 * 1024));
            last_accepted_response_budget.store(
                std::stoll(request.get_header_value("VGI-Accept-Max-Response-Bytes")));
            arrow_response(response, good_response_);
        });
        server_.Post("/support-missing",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("X-Test-Omit-Budget-Support", "1");
                     });
        server_.Post("/support-duplicate",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("VGI-Accept-Max-Response-Bytes-Support", "true");
                         response.headers.emplace("VGI-Accept-Max-Response-Bytes-Support", "true");
                     });
        server_.Post("/support-uppercase",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("VGI-Accept-Max-Response-Bytes-Support", "TRUE");
                     });
        server_.Post(
            "/compressed", [this](const httplib::Request& request, httplib::Response& response) {
                compressed_request_valid.store(
                    request.get_header_value("Content-Encoding") == "zstd" &&
                    request.get_header_value("Accept-Encoding").find("zstd") != std::string::npos &&
                    request.get_header_value("X-VGI-Accept-Encoding").find("zstd") !=
                        std::string::npos &&
                    !request_body(request).empty());
                arrow_response(response, zstd_encode(good_response_));
                response.set_header("Content-Encoding", "ZSTD");
                response.set_header("VGI-Supported-Encodings", "zstd");
            });
        server_.Post("/decoded-large",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, zstd_encode(std::string(256 * 1024, 'd')));
                         response.set_header("Content-Encoding", "zstd");
                     });
        server_.Post("/encoded-large",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, std::string(128 * 1024, 'z'));
                         response.set_header("Content-Encoding", "zstd");
                     });
        server_.Post("/truncated-zstd",
                     [this](const httplib::Request&, httplib::Response& response) {
                         std::string encoded = zstd_encode(good_response_);
                         encoded.resize(encoded.size() / 2);
                         arrow_response(response, encoded);
                         response.set_header("Content-Encoding", "zstd");
                     });
        server_.Post(
            "/fallback", [this](const httplib::Request& request, httplib::Response& response) {
                const int attempt = fallback_requests.fetch_add(1);
                const std::string header_id = request.get_header_value("X-Request-ID");
                const std::string arrow_id = request_metadata_value(request, keys::REQUEST_ID);
                {
                    std::lock_guard<std::mutex> lock(fallback_mutex_);
                    if (attempt == 0) fallback_request_id_ = header_id;
                    fallback_request_ids_valid.store(
                        fallback_request_ids_valid.load() && !header_id.empty() &&
                        header_id != "caller-spoof" && header_id == arrow_id &&
                        header_id == fallback_request_id_);
                }
                if (request.get_header_value("Content-Encoding") == "zstd") {
                    response.status = 415;
                    response.set_content("unsupported", "text/plain");
                    response.set_header("VGI-Supported-Encodings", "");
                    return;
                }
                arrow_response(response, good_response_);
            });
        server_.Post("/advertise-none",
                     [this](const httplib::Request& request, httplib::Response& response) {
                         if (request.get_header_value("Content-Encoding").empty()) {
                             ++advertised_identity_requests;
                         }
                         arrow_response(response, good_response_);
                         response.set_header("VGI-Supported-Encodings", "");
                     });
        server_.Post(
            "/row-log-metadata", [this](const httplib::Request&, httplib::Response& response) {
                auto metadata = std::make_shared<arrow::KeyValueMetadata>();
                metadata->Append(keys::LOG_LEVEL, "EXCEPTION");
                metadata->Append(keys::LOG_MESSAGE, "application-owned metadata");
                arrow_response(
                    response,
                    encode_response(schema_, {AnnotatedBatch::with_metadata(
                                                 value_batch(schema_, 88), std::move(metadata))}));
            });

        server_.Post("/sticky-open", [this](const httplib::Request& request,
                                            httplib::Response& response) {
            sticky_headers_valid.store(request.get_header_value("VGI-Session-Accept") == "true" &&
                                       request.get_header_value("VGI-Session").empty());
            arrow_response(response, good_response_);
            response.set_header("VGI-Session", "sticky-token");
            response.set_header("VGI-Echo-X-Route", "worker-a");
        });
        server_.Post("/sticky-oversized", [this](const httplib::Request& request,
                                                 httplib::Response& response) {
            sticky_headers_valid.store(sticky_headers_valid.load() &&
                                       request.get_header_value("VGI-Session") == "sticky-token" &&
                                       request.get_header_value("X-Route") == "worker-a");
            arrow_response(response, good_response_);
            response.set_header("VGI-Session", "must-not-persist");
            for (int index = 0; index < 10; ++index) {
                response.set_header("VGI-Echo-X-Route-" + std::to_string(index),
                                    std::string(7000, 'x'));
            }
        });
        server_.Post("/sticky-reserved",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("VGI-Session", "must-not-persist");
                         response.set_header("VGI-Echo-Content-Type", "text/plain");
                     });
        server_.Post("/sticky-credential",
                     [this](const httplib::Request&, httplib::Response& response) {
                         arrow_response(response, good_response_);
                         response.set_header("VGI-Session", "must-not-persist");
                         response.set_header("VGI-Echo-Authorization", "Bearer reflected");
                     });
        server_.Post("/sticky-malformed", [](const httplib::Request&, httplib::Response& response) {
            response.set_content("not an Arrow stream", kArrowContentType);
            response.set_header("VGI-Session", "must-not-persist");
            response.set_header("VGI-Echo-X-Route", "wrong-worker");
        });
        server_.Post("/sticky-recover", [this](const httplib::Request& request,
                                               httplib::Response& response) {
            sticky_headers_valid.store(sticky_headers_valid.load() &&
                                       request.get_header_value("VGI-Session") == "sticky-token" &&
                                       request.get_header_value("X-Route") == "worker-a");
            arrow_response(response, good_response_);
        });
        server_.Post("/sticky-block", [this](const httplib::Request&, httplib::Response& response) {
            sticky_block_entered.store(true);
            while (!sticky_block_release.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            arrow_response(response, good_response_);
        });
        server_.Post("/transport-block",
                     [this](const httplib::Request&, httplib::Response& response) {
                         transport_block_entered.store(true);
                         while (!transport_block_release.load()) {
                             std::this_thread::sleep_for(std::chrono::milliseconds(1));
                         }
                         arrow_response(response, good_response_);
                     });
        server_.Post("/post-retry", [this](const httplib::Request&, httplib::Response& response) {
            if (post_retry_requests.fetch_add(1) < 2) {
                response.status = 503;
                response.set_content("retry", "text/plain");
                return;
            }
            arrow_response(response, good_response_);
        });
        install_init("producer-retry");
        server_.Post("/producer-retry/exchange",
                     [this](const httplib::Request&, httplib::Response& response) {
                         if (producer_retry_requests.fetch_add(1) < 2) {
                             response.status = 503;
                             response.set_content("retry", "text/plain");
                             return;
                         }
                         arrow_response(response, good_response_);
                     });
        server_.Delete("/__session__", [](const httplib::Request&, httplib::Response& response) {
            response.status = 204;
        });

        server_.Post("/fixed-large", [this](const httplib::Request&, httplib::Response& response) {
            response.set_content(std::string(128 * 1024, 'f'), kArrowContentType);
        });
        server_.Post(
            "/chunked-large", [this](const httplib::Request&, httplib::Response& response) {
                auto payload = std::make_shared<std::string>(128 * 1024, 'c');
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

        install_init("codec-fallback");
        server_.Post("/codec-fallback/exchange", [this](const httplib::Request& request,
                                                        httplib::Response& response) {
            ++exchange_fallback_requests;
            if (request.get_header_value("Content-Encoding") == "zstd") {
                response.status = 415;
                response.set_content("unsupported", "text/plain");
                response.set_header("VGI-Supported-Encodings", "");
                return;
            }
            auto metadata = std::make_shared<arrow::KeyValueMetadata>();
            metadata->Append(keys::STATE_B64, "next-cursor");
            arrow_response(
                response,
                encode_response(schema_, {AnnotatedBatch::with_metadata(value_batch(schema_, 41),
                                                                        std::move(metadata))}));
        });
    }

    void stop() noexcept {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::string good_response_;
    bool advertise_response_budget_ = true;
    std::mutex capability_mutex_;
    std::string advertised_response_budget_ = "234567";
    bool duplicate_advertised_response_budget_ = false;
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    std::mutex fallback_mutex_;
    std::string fallback_request_id_;
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

void test_encoded_request_cap(const FaultServer& server, const std::string& origin) {
    const auto request = incompressible_request();
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::METHOD, "encoded-request-cap");
    metadata->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    metadata->Append(keys::REQUEST_ID, "0123456789abcdef");
    const std::string raw =
        encode_response(request.batch->schema(),
                        {AnnotatedBatch::with_metadata(request.batch, std::move(metadata))});

    HttpClientConfig config;
    config.prefix = "";
    config.max_request_bytes = static_cast<int64_t>(raw.size());
    // The fastest zstd mode emits this deliberately tiny, literal-heavy frame
    // slightly larger than its Arrow input.  That pins the post-compression
    // guard, distinct from the existing decoded IPC preflight test.
    config.compression_level = -131072;
    HttpClient client(origin, config);
    const auto error = require_http_client_error(
        [&] { (void)client.call("encoded-request-cap", request); }, "encoded request cap");
    require(std::string(error.what()).find("encoded HTTP RPC request") != std::string::npos,
            "encoded request cap produced the wrong error");
    require(server.encoded_request_cap_requests.load() == 0,
            "request exceeding the encoded cap reached the server");
}

void test_response_caps(FaultServer& server, const std::string& origin,
                        const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    config.max_response_bytes = 8 * 1024 * 1024;
    config.accepted_max_response_bytes = 64 * 1024;
    HttpClient client(origin, config);

    const auto fixed_error = require_http_client_error(
        [&] { (void)client.call("fixed-large", empty_request(), schema); },
        "fixed-length response cap");
    require(std::string(fixed_error.what()).find("max_decoded_response_bytes (65536)") !=
                std::string::npos,
            "identity response was not bounded by the decoded cap during receipt");
    auto fixed_recovery = client.call("ok", empty_request(), schema);
    require(fixed_recovery.batch && fixed_recovery.batch->num_rows() == 1,
            "client did not recover after fixed-length response overflow");

    const auto chunked_error = require_http_client_error(
        [&] { (void)client.call("chunked-large", empty_request(), schema); },
        "chunked response cap");
    require(std::string(chunked_error.what()).find("max_decoded_response_bytes (65536)") !=
                std::string::npos,
            "chunked identity response was not bounded by the decoded cap during receipt");
    auto chunked_recovery = client.call("ok", empty_request(), schema);
    require(chunked_recovery.batch && chunked_recovery.batch->num_rows() == 1,
            "client did not recover after chunked response overflow");
    require(server.recovery_requests.load() == 2,
            "response-cap recovery requests did not both reach the server");
}

void test_compression(FaultServer& server, const std::string& origin,
                      const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    config.max_encoded_response_bytes = 64 * 1024;
    config.max_decoded_response_bytes = 1024 * 1024;
    HttpClient client(origin, config);

    auto compressed = client.call("compressed", empty_request(), schema);
    require(compressed.batch && compressed.batch->num_rows() == 1,
            "zstd response did not decode to one batch");
    require(server.compressed_request_valid.load(),
            "client did not send a valid compressed request and codec advertisements");

    HttpClientConfig decoded_cap;
    decoded_cap.prefix = "";
    decoded_cap.max_encoded_response_bytes = 64 * 1024;
    decoded_cap.max_decoded_response_bytes = 64 * 1024;
    HttpClient decoded_limited(origin, decoded_cap);
    const auto decoded_error = require_http_client_error(
        [&] { (void)decoded_limited.call("decoded-large", empty_request(), schema); },
        "decoded zstd response cap");
    require(
        std::string(decoded_error.what()).find("max_decoded_response_bytes") != std::string::npos,
        "decoded zstd cap produced the wrong error");

    HttpClientConfig encoded_cap;
    encoded_cap.prefix = "";
    encoded_cap.max_encoded_response_bytes = 64 * 1024;
    encoded_cap.max_decoded_response_bytes = 1024 * 1024;
    HttpClient encoded_limited(origin, encoded_cap);
    const auto encoded_error = require_http_client_error(
        [&] { (void)encoded_limited.call("encoded-large", empty_request(), schema); },
        "encoded zstd response cap");
    require(
        std::string(encoded_error.what()).find("max_encoded_response_bytes") != std::string::npos,
        "encoded zstd cap produced the wrong error");

    const auto truncated_error = require_http_client_error(
        [&] { (void)client.call("truncated-zstd", empty_request(), schema); },
        "truncated zstd response");
    require(std::string(truncated_error.what()).find("zstd") != std::string::npos,
            "truncated zstd response produced the wrong error");
}

void test_compression_negotiation(FaultServer& server, const std::string& origin,
                                  const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient fallback(origin, config);
    auto spoofed = empty_request();
    spoofed.custom_metadata = std::make_shared<arrow::KeyValueMetadata>();
    spoofed.custom_metadata->Append(keys::REQUEST_ID, "caller-spoof");
    auto result = fallback.call("fallback", spoofed, schema);
    require(result.batch && result.batch->num_rows() == 1,
            "415 compression fallback did not return data");
    require(server.fallback_requests.load() == 2,
            "415 compression fallback did not make exactly two attempts");
    require(server.fallback_request_ids_valid.load(),
            "logical request ID diverged between Arrow, HTTP, or 415 retry attempts");

    HttpClient advertised(origin, config);
    (void)advertised.call("advertise-none", empty_request(), schema);
    (void)advertised.call("advertise-none", empty_request(), schema);
    require(server.advertised_identity_requests.load() == 1,
            "present-but-empty encoding advertisement did not disable later request compression");

    HttpClient exchange_client(origin, config);
    auto session = exchange_client.open_exchange("codec-fallback", empty_request(), schema, schema);
    auto output = session.exchange(AnnotatedBatch::data(value_batch(schema, 1)));
    require(output.batch && output.batch->num_rows() == 1 && session.active(),
            "unambiguous exchange 415 fallback did not preserve the session");
    require(server.exchange_fallback_requests.load() == 2,
            "exchange 415 fallback did not make exactly two attempts");
    session.close();
}

void test_post_retry_requires_idempotency(FaultServer& server, const std::string& origin,
                                          const std::shared_ptr<arrow::Schema>& schema) {
    RetryPolicy policy;
    policy.initial_backoff = std::chrono::milliseconds(0);
    policy.retryable_status_codes = {503};
    auto client = HttpClient::builder(origin).prefix("").retry_policy(policy).build();
    const auto error =
        require_http_client_error([&] { (void)client.call("post-retry", empty_request(), schema); },
                                  "non-idempotent POST status");
    require(error.http_status() == 503 && server.post_retry_requests.load() == 1,
            "default POST call retried after dispatch without idempotency opt-in");

    CallOptions idempotent;
    idempotent.idempotent = true;
    const auto result = client.call("post-retry", empty_request(), schema, idempotent);
    require(result.batch && server.post_retry_requests.load() == 3,
            "explicitly idempotent POST did not use the configured retry policy");
}

void test_producer_retry_requires_idempotency(FaultServer& server, const std::string& origin,
                                              const std::shared_ptr<arrow::Schema>& schema) {
    RetryPolicy policy;
    policy.initial_backoff = std::chrono::milliseconds(0);
    policy.retryable_status_codes = {503};
    auto client = HttpClient::builder(origin).prefix("").retry_policy(policy).build();

    auto default_session = client.open_producer("producer-retry", empty_request(), schema);
    const auto error = require_http_client_error([&] { (void)default_session.tick(); },
                                                 "non-idempotent producer continuation");
    require(error.http_status() == 503 && server.producer_retry_requests.load() == 1,
            "producer continuation retried without idempotency opt-in");

    auto idempotent_session = client.open_producer("producer-retry", empty_request(), schema);
    CallOptions idempotent;
    idempotent.idempotent = true;
    const auto result = idempotent_session.tick(idempotent);
    require(result && result->batch && server.producer_retry_requests.load() == 3,
            "explicitly idempotent producer continuation did not use retry policy");
}

void test_capabilities_and_row_metadata(FaultServer& server, const std::string& origin,
                                        const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);
    const auto caps = client.capabilities();
    require(caps.accept_max_response_bytes_support,
            "accepted response-budget capability was not parsed");
    require(caps.sticky_enabled && caps.sticky_default_ttl == 45,
            "sticky capability fields were not parsed");
    require(caps.sticky_echo_headers == std::vector<std::string>({"X-Tenant", "X-Region"}),
            "sticky echo capability list was not parsed");
    require(caps.upload_url_support && caps.externalization_enabled,
            "externalization capability flags were not parsed");
    require(caps.max_request_bytes == 123456 && caps.max_response_bytes == 234567 &&
                caps.max_externalized_response_bytes == 345678 && caps.max_upload_bytes == 456789,
            "capability byte limits were not parsed");
    require(caps.supported_encodings == std::vector<std::string>({"zstd", "gzip"}),
            "capability encodings were not normalized");

    auto result = client.call("row-log-metadata", empty_request(), schema);
    require(result.batch && result.batch->num_rows() == 1,
            "non-empty application batch with log metadata was swallowed as control");
    require(server.accepted_response_budget_seen.load(),
            "client omitted its accepted response budget on OPTIONS or POST");
}

void test_response_budget_support_is_mandatory(const std::shared_ptr<arrow::Schema>& schema) {
    FaultServer legacy(schema, false);
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(legacy.origin(), config);
    const auto error =
        require_http_client_error([&] { (void)client.call("ok", empty_request(), schema); },
                                  "missing response-budget support capability");
    require(error.kind() == HttpClientErrorKind::PROTOCOL &&
                std::string(error.what()).find("VGI-Accept-Max-Response-Bytes-Support") !=
                    std::string::npos,
            "missing response-budget support did not fail closed");
    require(legacy.recovery_requests.load() == 0,
            "RPC reached a server without response-budget support");
}

void test_response_budget_support_is_required_on_every_response(
    const std::string& origin, const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    for (const std::string method : {"support-missing", "support-duplicate", "support-uppercase"}) {
        HttpClient client(origin, config);
        const auto error = require_http_client_error(
            [&] { (void)client.call(method, empty_request(), schema); }, method);
        require(error.kind() == HttpClientErrorKind::PROTOCOL &&
                    std::string(error.what()).find("VGI-Accept-Max-Response-Bytes-Support") !=
                        std::string::npos,
                method + " response-budget support did not fail closed");
    }
}

void test_advertised_response_budget_is_strict_and_bounds_decoded_bytes(
    FaultServer& server, const std::string& origin, const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig narrowed;
    narrowed.prefix = "";
    narrowed.accepted_max_response_bytes = 512 * 1024;
    narrowed.max_encoded_response_bytes = 512 * 1024;
    narrowed.max_decoded_response_bytes = 1024 * 1024;
    HttpClient client(origin, narrowed);
    const auto narrowed_error = require_http_client_error(
        [&] { (void)client.call("decoded-large", empty_request(), schema); },
        "advertised decoded response cap");
    require(std::string(narrowed_error.what()).find("234567") != std::string::npos,
            "advertised response cap did not narrow the decoded limit independently of the "
            "encoded safety limit");

    for (const auto& [value, duplicate] :
         {std::pair{std::string("invalid"), false}, std::pair{std::string("65535"), false},
          std::pair{std::string("65536"), true}}) {
        server.advertised_response_budget(value, duplicate);
        HttpClient invalid(origin, HttpClientConfig{.prefix = ""});
        const auto error = require_http_client_error([&] { (void)invalid.capabilities(); },
                                                     "invalid advertised response cap");
        require(error.kind() == HttpClientErrorKind::PROTOCOL &&
                    std::string(error.what()).find("VGI-Max-Response-Bytes") != std::string::npos,
                "malformed or duplicate advertised response cap did not fail closed");
    }
    server.advertised_response_budget("234567");
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

void test_sticky_session_hardening(FaultServer& server, const std::string& origin,
                                   const std::shared_ptr<arrow::Schema>& schema) {
    HttpClientConfig config;
    config.prefix = "";
    HttpClient client(origin, config);
    auto session = client.with_session_token();
    (void)session.call("sticky-open", empty_request(), schema);
    require(session.current_session_token() == std::optional<std::string>{"sticky-token"},
            "sticky response token was not captured");
    const auto original_echo = session.current_echo_headers();
    require(original_echo.size() == 1 && original_echo.begin()->second == "worker-a",
            "sticky routing echo header was not captured");

    CallOptions spoofed_route;
    spoofed_route.headers["X-Route"] = "wrong-worker";
    const auto require_atomic_rejection = [&](const char* method) {
        const auto error = require_http_client_error(
            [&] { (void)session.call(method, empty_request(), schema, spoofed_route); }, method);
        require(error.kind() == HttpClientErrorKind::PROTOCOL,
                std::string(method) + " did not fail as a protocol error");
        require(session.current_session_token() == std::optional<std::string>{"sticky-token"} &&
                    session.current_echo_headers() == original_echo,
                std::string(method) + " persisted partial untrusted session metadata");
    };
    require_atomic_rejection("sticky-oversized");
    require_atomic_rejection("sticky-reserved");
    require_atomic_rejection("sticky-credential");
    (void)require_http_client_error(
        [&] { (void)session.call("sticky-malformed", empty_request(), schema); },
        "sticky-malformed");
    require(session.current_session_token() == std::optional<std::string>{"sticky-token"} &&
                session.current_echo_headers() == original_echo,
            "malformed Arrow response persisted staged sticky metadata");
    (void)session.call("sticky-recover", empty_request(), schema, spoofed_route);
    require(server.sticky_headers_valid.load(),
            "session routing headers did not override caller headers or survive rejection");

    for (const auto& headers :
         {std::map<std::string, std::string>{{"X-Test", "bad\r\nvalue"}},
          std::map<std::string, std::string>{{"Content-Type", "text/plain"}},
          std::map<std::string, std::string>{{"Authorization", "Bearer reflected"}}}) {
        bool rejected = false;
        try {
            (void)client.with_session_token(std::nullopt, headers);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "invalid initial sticky echo metadata was accepted");
    }
    HttpClientConfig credential_opt_in;
    credential_opt_in.prefix = "";
    credential_opt_in.allow_insecure_credentials = true;
    auto credential_client =
        HttpClient::builder(origin).config(std::move(credential_opt_in)).build();
    bool credential_echo_rejected = false;
    try {
        (void)credential_client.with_session_token(std::nullopt,
                                                   {{"Authorization", "Bearer reflected"}});
    } catch (const std::invalid_argument&) {
        credential_echo_rejected = true;
    }
    require(credential_echo_rejected,
            "sticky credential echo was accepted after transport credential opt-in");
    (void)client.call("ok", empty_request(), schema);
    session.close();
    session.close();

    auto blocked = client.with_session_token(std::string("sticky-token"), original_echo);
    std::exception_ptr request_failure;
    std::thread request([&] {
        try {
            (void)blocked.call("sticky-block", empty_request(), schema);
        } catch (...) {
            request_failure = std::current_exception();
        }
    });
    for (int attempt = 0; attempt < 1000 && !server.sticky_block_entered.load(); ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    require(server.sticky_block_entered.load(), "blocking sticky request did not reach the server");
    const auto close_started = std::chrono::steady_clock::now();
    blocked.close();
    const auto close_elapsed = std::chrono::steady_clock::now() - close_started;
    require(close_elapsed < std::chrono::milliseconds(250),
            "sticky close waited unboundedly on an in-flight request");
    require(!blocked.active(), "bounded sticky close did not retire local state");
    server.sticky_block_release.store(true);
    request.join();
    require(request_failure != nullptr,
            "response completed successfully after its sticky view was locally closed");
    try {
        std::rethrow_exception(request_failure);
    } catch (const HttpClientError& error) {
        require(
            std::string(error.what()).find("closed before response commit") != std::string::npos,
            "post-close sticky response produced the wrong error");
    }

    auto transport_contended =
        client.with_session_token(std::string("sticky-token"), original_echo);
    std::exception_ptr transport_failure;
    std::thread transport_request([&] {
        try {
            (void)client.call("transport-block", empty_request(), schema);
        } catch (...) {
            transport_failure = std::current_exception();
        }
    });
    for (int attempt = 0; attempt < 1000 && !server.transport_block_entered.load(); ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    require(server.transport_block_entered.load(),
            "transport-contention request did not reach the server");
    const auto transport_close_started = std::chrono::steady_clock::now();
    transport_contended.close();
    require(
        std::chrono::steady_clock::now() - transport_close_started < std::chrono::milliseconds(250),
        "sticky close waited unboundedly on the shared transport mutex");
    server.transport_block_release.store(true);
    transport_request.join();
    if (transport_failure) std::rethrow_exception(transport_failure);

    std::atomic<int> auth_calls{0};
    auto callback_client = HttpClient::builder(origin)
                               .prefix("")
                               .auth_callback([&](const HttpAuthRequest&) {
                                   ++auth_calls;
                                   std::this_thread::sleep_for(std::chrono::seconds(1));
                                   return std::map<std::string, std::string>{};
                               })
                               .build();
    auto callback_session = callback_client.with_session_token(std::string("sticky-token"));
    const auto callback_close_started = std::chrono::steady_clock::now();
    callback_session.close();
    require(std::chrono::steady_clock::now() - callback_close_started <
                    std::chrono::milliseconds(500) &&
                auth_calls.load() == 0,
            "sticky teardown invoked an unbounded authentication callback");
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

    HttpClientConfig reserved_budget;
    reserved_budget.headers["vGi-AcCePt-MaX-ReSpOnSe-ByTeS"] = "123";
    require_invalid(std::move(reserved_budget), "reserved response-budget header");

    HttpClientConfig crlf;
    crlf.headers["X-Test"] = "safe\r\ninjected: true";
    require_invalid(std::move(crlf), "CRLF-bearing header");

    HttpClientConfig credentials;
    credentials.headers["Authorization"] = "Bearer secret";
    require_invalid(std::move(credentials), "cleartext credential header");

    HttpClientConfig zero_cap;
    zero_cap.max_response_bytes = 0;
    require_invalid(std::move(zero_cap), "unbounded/zero response cap");

    HttpClientConfig negative_encoded_cap;
    negative_encoded_cap.max_encoded_response_bytes = -1;
    require_invalid(std::move(negative_encoded_cap), "negative encoded response cap");

    HttpClientConfig zero_accepted;
    zero_accepted.accepted_max_response_bytes = 0;
    require_invalid(std::move(zero_accepted), "zero accepted response cap");

    HttpClientConfig below_floor_accepted;
    below_floor_accepted.accepted_max_response_bytes = (64 * 1024) - 1;
    require_invalid(std::move(below_floor_accepted), "below-floor accepted response cap");

    HttpClientConfig unsafe_accepted;
    unsafe_accepted.accepted_max_response_bytes = 9'007'199'254'740'992LL;
    require_invalid(std::move(unsafe_accepted), "unsafe-integer accepted response cap");

    HttpClientConfig explicit_caps;
    explicit_caps.max_response_bytes = 0;
    explicit_caps.max_encoded_response_bytes = 64 * 1024;
    explicit_caps.max_decoded_response_bytes = 64 * 1024;
    HttpClient explicit_client(origin, std::move(explicit_caps));

    HttpClientConfig below_floor_decoded;
    below_floor_decoded.max_decoded_response_bytes = (64 * 1024) - 1;
    require_invalid(std::move(below_floor_decoded), "below-floor decoded response cap");

    HttpClientConfig below_floor_encoded;
    below_floor_encoded.max_encoded_response_bytes = (64 * 1024) - 1;
    require_invalid(std::move(below_floor_encoded), "below-floor encoded response cap");
}

void test_accepted_budget_controls_decoded_willingness(
    const std::shared_ptr<arrow::Schema>& schema) {
    FaultServer server(schema);
    HttpClientConfig config;
    config.prefix = "";
    config.max_response_bytes = 256 * 1024 * 1024;
    config.max_encoded_response_bytes = 256 * 1024;
    config.accepted_max_response_bytes = 1024LL * 1024 * 1024;
    (void)HttpClient(server.origin(), config).capabilities();
    require(server.last_accepted_response_budget.load() == 256 * 1024,
            "explicit encoded ceiling was omitted from the advertised identity budget");

    (void)HttpClient::builder(server.origin())
        .prefix("")
        .accepted_max_response_bytes(1024LL * 1024 * 1024)
        .build()
        .capabilities();
    require(server.last_accepted_response_budget.load() == 1024LL * 1024 * 1024,
            "accepted-budget setter did not raise the default encoded and decoded ceilings");

    (void)HttpClient::builder(server.origin())
        .prefix("")
        .accepted_max_response_bytes(192 * 1024)
        .response_limits(80 * 1024, 96 * 1024)
        .build()
        .capabilities();
    require(server.last_accepted_response_budget.load() == 80 * 1024,
            "explicit encoded cap was not advertised after accepted-budget setter");

    (void)HttpClient::builder(server.origin())
        .prefix("")
        .response_limits(80 * 1024, 96 * 1024)
        .accepted_max_response_bytes(192 * 1024)
        .build()
        .capabilities();
    require(server.last_accepted_response_budget.load() == 80 * 1024,
            "accepted-budget and response-limit setters were order dependent");

    (void)HttpClient::builder(server.origin())
        .prefix("")
        .accepted_max_response_bytes(192 * 1024)
        .response_limits(256 * 1024, 96 * 1024)
        .build()
        .capabilities();
    require(server.last_accepted_response_budget.load() == 96 * 1024,
            "explicit decoded cap was omitted from the advertised identity budget");

    (void)HttpClient::builder(server.origin())
        .prefix("")
        .response_limits(256 * 1024, 96 * 1024)
        .accepted_max_response_bytes(192 * 1024)
        .build()
        .capabilities();
    require(server.last_accepted_response_budget.load() == 96 * 1024,
            "explicit decoded cap was order-dependent with accepted-budget setter");
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
        test_encoded_request_cap(server, origin);
        test_response_caps(server, origin, schema);
        test_compression(server, origin, schema);
        test_compression_negotiation(server, origin, schema);
        test_post_retry_requires_idempotency(server, origin, schema);
        test_producer_retry_requires_idempotency(server, origin, schema);
        test_capabilities_and_row_metadata(server, origin, schema);
        test_advertised_response_budget_is_strict_and_bounds_decoded_bytes(server, origin, schema);
        test_response_budget_support_is_required_on_every_response(origin, schema);
        test_response_headers(origin, schema);
        test_pointer_metadata_sanitization(server, origin, schema);
        test_sticky_session_hardening(server, origin, schema);
        test_config_validation(origin);
        test_accepted_budget_controls_decoded_willingness(schema);
        test_ambiguous_exchange(server, origin, schema);
        test_local_close_and_cancel(server, origin, schema);
        test_response_budget_support_is_mandatory(schema);
    } catch (const std::exception& error) {
        std::cerr << "native HTTP client fault regression failed: " << error.what() << "\n";
        return 1;
    }
    return 0;
}
