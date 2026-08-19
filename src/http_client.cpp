// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/http_client.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <limits>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>

namespace vgi_rpc {

namespace {

constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";

void replace_metadata(std::shared_ptr<arrow::KeyValueMetadata>& metadata, const std::string& key,
                      const std::string& value) {
    if (!metadata) metadata = std::make_shared<arrow::KeyValueMetadata>();
    if (const int64_t index = metadata->FindKey(key); index >= 0) {
        (void)metadata->Delete(index);
    }
    metadata->Append(key, value);
}

std::shared_ptr<arrow::KeyValueMetadata> sanitized_metadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& original) {
    auto metadata = original ? original->Copy() : std::make_shared<arrow::KeyValueMetadata>();
    for (const char* key :
         {keys::METHOD, keys::REQUEST_VERSION, keys::PROTOCOL_VERSION, keys::STATE_B64,
          keys::CALL_STATE_B64, keys::STREAM_STATE, keys::CANCEL, keys::LOCATION,
          keys::LOCATION_SHA256, keys::LOCATION_SOURCE, keys::LOCATION_FETCH_MS, keys::SHM_OFFSET,
          keys::SHM_LENGTH, keys::SHM_SOURCE, keys::SHM_SEGMENT_NAME, keys::SHM_SEGMENT_SIZE,
          keys::TRANSPORT_SHM}) {
        while (true) {
            const int64_t index = metadata->FindKey(key);
            if (index < 0) break;
            (void)metadata->Delete(index);
        }
    }
    return metadata;
}

std::shared_ptr<arrow::KeyValueMetadata> request_metadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& original, const std::string& method,
    const std::string& protocol_version) {
    auto metadata = sanitized_metadata(original);
    replace_metadata(metadata, keys::METHOD, method);
    replace_metadata(metadata, keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    if (!protocol_version.empty()) {
        replace_metadata(metadata, keys::PROTOCOL_VERSION, protocol_version);
    }
    return metadata;
}

std::shared_ptr<arrow::KeyValueMetadata> strip_transport_metadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata) {
    if (!metadata) return nullptr;
    auto copy = metadata->Copy();
    for (const char* key : {keys::STATE_B64, keys::CALL_STATE_B64}) {
        if (const int64_t index = copy->FindKey(key); index >= 0) {
            (void)copy->Delete(index);
        }
    }
    return copy->size() == 0 ? nullptr : std::move(copy);
}

std::string encode_ipc(const AnnotatedBatch& batch,
                       const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                       int64_t max_request_bytes) {
    if (!batch.batch) throw std::invalid_argument("RPC request batch must not be null");
    auto counter = std::make_shared<arrow::io::MockOutputStream>();
    write_ipc_stream(counter, batch.batch->schema(),
                     {AnnotatedBatch::with_metadata(batch.batch, metadata)});
    const int64_t encoded_bytes = counter->GetExtentBytesWritten();
    if (encoded_bytes > max_request_bytes) {
        throw HttpClientError("HTTP RPC request exceeds max_request_bytes (" +
                              std::to_string(encoded_bytes) + " > " +
                              std::to_string(max_request_bytes) + ")");
    }
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, batch.batch->schema(),
                     {AnnotatedBatch::with_metadata(batch.batch, metadata)});
    auto buffer = unwrap(output->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

bool schema_equals(const std::shared_ptr<arrow::Schema>& actual,
                   const std::shared_ptr<arrow::Schema>& expected) {
    return actual && expected && actual->Equals(*expected, /*check_metadata=*/true);
}

std::string schema_mismatch(const char* direction, const std::shared_ptr<arrow::Schema>& expected,
                            const std::shared_ptr<arrow::Schema>& actual) {
    return std::string(direction) + " schema mismatch: expected " +
           (expected ? expected->ToString() : "<null>") + ", got " +
           (actual ? actual->ToString() : "<null>");
}

std::optional<int64_t> positive_header_int(const httplib::Response& response,
                                           const std::string& name) {
    if (!response.has_header(name)) return std::nullopt;
    try {
        const std::string raw = response.get_header_value(name);
        size_t consumed = 0;
        const int64_t value = std::stoll(raw, &consumed);
        if (consumed != raw.size() || value < 0) return std::nullopt;
        return value;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

struct BoundedHttpResponse {
    int status = 0;
    bool rpc_error = false;
    std::string body;
};

struct DecodedResponse {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<AnnotatedBatch> data;
    std::vector<AnnotatedBatch> control;
};

enum class ResponseShape {
    UNARY,
    EXCHANGE_INIT,
    EXCHANGE_TURN,
};

RpcRemoteError remote_error(const AnnotatedBatch& batch, int status) {
    const auto& metadata = batch.custom_metadata;
    std::string message = get_metadata_value(metadata, keys::LOG_MESSAGE, "remote RPC error");
    std::string exception_type = "RpcError";
    std::string error_kind = get_metadata_value(metadata, keys::ERROR_KIND);
    const std::string extra = get_metadata_value(metadata, keys::LOG_EXTRA);
    if (!extra.empty()) {
        try {
            const auto parsed = nlohmann::json::parse(extra);
            exception_type = parsed.value("exception_type", exception_type);
            if (error_kind.empty()) error_kind = parsed.value("error_kind", std::string());
        } catch (const std::exception&) {
        }
    }
    return RpcRemoteError(std::move(exception_type), std::move(message), std::move(error_kind),
                          get_metadata_value(metadata, keys::SERVER_ID),
                          get_metadata_value(metadata, keys::REQUEST_ID), status);
}

DecodedResponse decode_response(const BoundedHttpResponse& response, const HttpClientConfig& config,
                                ResponseShape shape) {
    // IPC arrays may retain slices of their source buffer.  FromString owns a
    // copy whose shared lifetime follows those slices; wrapping response.body
    // would leave returned batches pointing at a destroyed std::string.
    auto buffer = arrow::Buffer::FromString(response.body);
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    std::optional<IpcStreamContents> contents;
    try {
        contents = read_ipc_stream(input);
    } catch (const std::exception& error) {
        throw HttpClientError(std::string("invalid Arrow IPC response: ") + error.what(),
                              response.status);
    }
    if (!contents) {
        throw HttpClientError("HTTP RPC response contained no Arrow IPC stream", response.status);
    }

    DecodedResponse decoded;
    decoded.schema = contents->schema;
    for (auto& batch : contents->batches) {
        const auto& metadata = batch.custom_metadata;
        const auto has = [&](const char* key) { return metadata && metadata->FindKey(key) >= 0; };

        // Protocol controls take precedence over row count.  In particular,
        // a malformed non-empty pointer must not become application data just
        // because classify_batch's general-purpose heuristic sees rows first.
        if (has(keys::LOG_LEVEL) && has(keys::LOG_MESSAGE)) {
            if (get_metadata_value(metadata, keys::LOG_LEVEL) == "EXCEPTION") {
                throw remote_error(batch, response.status);
            }
            if (config.on_log) config.on_log(batch);
            continue;
        }
        if (has(keys::LOCATION)) {
            throw HttpClientError(
                "external-location response batches are not supported by HttpClient",
                response.status);
        }
        if (has(keys::SHM_OFFSET)) {
            throw HttpClientError("shared-memory response batches are invalid over HTTP",
                                  response.status);
        }
        if (has(keys::STREAM_STATE)) {
            throw HttpClientError("legacy stream-state control batch is unsupported",
                                  response.status);
        }

        const bool has_cursor = has(keys::STATE_B64);
        if (has_cursor && shape == ResponseShape::EXCHANGE_INIT) {
            if (!batch.batch || batch.batch->num_rows() != 0) {
                throw HttpClientError("exchange init returned a non-empty state control batch",
                                      response.status);
            }
            decoded.control.push_back(std::move(batch));
            continue;
        }
        if (has_cursor && shape == ResponseShape::UNARY) {
            throw HttpClientError("unexpected stream control metadata in unary response",
                                  response.status);
        }

        // During an exchange turn the cursor is attached to the application
        // batch.  It remains data even when it contains zero rows; zero rows
        // are not an end-of-stream marker in the HTTP exchange protocol.
        decoded.data.push_back(std::move(batch));
    }
    if (response.rpc_error) {
        throw HttpClientError("worker set X-VGI-RPC-Error without an exception batch",
                              response.status);
    }
    if (response.status < 200 || response.status >= 300) {
        throw HttpClientError(
            "HTTP RPC response returned Arrow data with status " + std::to_string(response.status),
            response.status);
    }
    return decoded;
}

void validate_method(const std::string& method) {
    if (method.empty() || method.find_first_of("/?#") != std::string::npos) {
        throw std::invalid_argument("RPC method must be a non-empty path segment");
    }
}

std::string ascii_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

void validate_headers(const HttpClientConfig& config) {
    const std::vector<std::string> reserved = {
        "accept-encoding",   "content-encoding", "content-length", "content-type", "host",
        "transfer-encoding", "x-request-id",
    };
    const std::vector<std::string> credentials = {"authorization", "cookie", "proxy-authorization"};
    for (const auto& [name, value] : config.headers) {
        if (name.empty() || name.find_first_of("\r\n:") != std::string::npos ||
            value.find_first_of("\r\n") != std::string::npos) {
            throw std::invalid_argument("HTTP client header contains invalid characters");
        }
        const std::string lower = ascii_lower(name);
        if (std::find(reserved.begin(), reserved.end(), lower) != reserved.end()) {
            throw std::invalid_argument("HTTP client header is transport-reserved: " + name);
        }
        if (!config.allow_insecure_credentials &&
            std::find(credentials.begin(), credentials.end(), lower) != credentials.end()) {
            throw std::invalid_argument(
                "credentials over plain HTTP require allow_insecure_credentials=true");
        }
    }
}

}  // namespace

class HttpClientState {
public:
    HttpClientState(std::string base_url, HttpClientConfig client_config)
        : config(std::move(client_config)), client(std::move(base_url)) {
        if (config.max_request_bytes <= 0 || config.max_response_bytes <= 0) {
            throw std::invalid_argument("HTTP client request and response caps must be positive");
        }
        validate_headers(config);
        client.set_keep_alive(config.keep_alive);
        client.set_decompress(false);
        client.set_connection_timeout(config.connection_timeout_seconds);
        client.set_read_timeout(config.read_timeout_seconds);
        client.set_write_timeout(config.write_timeout_seconds);
        if (config.prefix.empty()) return;
        if (config.prefix.front() != '/') config.prefix.insert(config.prefix.begin(), '/');
        while (config.prefix.size() > 1 && config.prefix.back() == '/') config.prefix.pop_back();
    }

    BoundedHttpResponse post(const std::string& path, const std::string& body) {
        std::lock_guard<std::mutex> lock(mutex);
        int64_t request_cap = config.max_request_bytes;
        if (server_max_request_bytes &&
            (request_cap < 0 || *server_max_request_bytes < request_cap)) {
            request_cap = *server_max_request_bytes;
        }
        if (body.size() > static_cast<uint64_t>(request_cap)) {
            throw HttpClientError("HTTP RPC request exceeds max_request_bytes (" +
                                  std::to_string(body.size()) + " > " +
                                  std::to_string(request_cap) + ")");
        }

        httplib::Headers headers;
        for (const auto& [name, value] : config.headers) headers.emplace(name, value);
        headers.emplace("Accept-Encoding", "identity");
        headers.emplace("X-Request-ID", random_hex(16));

        BoundedHttpResponse response;
        bool response_too_large = false;
        auto result =
            client.Post(path, headers, body, kArrowContentType, [&](const char* data, size_t size) {
                if (size > static_cast<uint64_t>(config.max_response_bytes) ||
                    response.body.size() >
                        static_cast<uint64_t>(config.max_response_bytes) - size) {
                    response_too_large = true;
                    return false;
                }
                response.body.append(data, size);
                return true;
            });
        if (response_too_large) {
            throw HttpClientError("HTTP RPC response exceeds max_response_bytes (" +
                                  std::to_string(config.max_response_bytes) + ")");
        }
        if (!result) {
            throw HttpClientError(std::string("HTTP RPC transport failed: ") +
                                  httplib::to_string(result.error()));
        }
        response.status = result->status;
        response.rpc_error = result->get_header_value("X-VGI-RPC-Error") == "true";
        if (auto cap = positive_header_int(*result, "VGI-Max-Request-Bytes")) {
            server_max_request_bytes = cap;
        }
        const std::string content_encoding = result->get_header_value("Content-Encoding");
        if (!content_encoding.empty() && content_encoding != "identity") {
            throw HttpClientError("unsupported HTTP response Content-Encoding: " + content_encoding,
                                  response.status);
        }
        const std::string content_type = result->get_header_value("Content-Type");
        if (response.status < 200 || response.status >= 300) {
            // Framework errors may use an HTTP error status while still
            // carrying the standard Arrow exception envelope.  Let the RPC
            // decoder preserve its type/kind/request id in that case.
            if (content_type.rfind(kArrowContentType, 0) == 0) return response;
            std::string detail = response.body;
            if (detail.size() > 512) detail.resize(512);
            throw HttpClientError("HTTP RPC request failed with status " +
                                      std::to_string(response.status) +
                                      (detail.empty() ? "" : ": " + detail),
                                  response.status);
        }
        if (content_type.rfind(kArrowContentType, 0) != 0) {
            throw HttpClientError("HTTP RPC response has unsupported Content-Type: " +
                                      (content_type.empty() ? "<missing>" : content_type),
                                  response.status);
        }
        return response;
    }

    int64_t request_cap() {
        std::lock_guard<std::mutex> lock(mutex);
        if (server_max_request_bytes) {
            return std::min(config.max_request_bytes, *server_max_request_bytes);
        }
        return config.max_request_bytes;
    }

    std::string path(const std::string& method, const char* suffix = "") const {
        return (config.prefix == "/" ? std::string() : config.prefix) + "/" + method + suffix;
    }

    HttpClientConfig config;

private:
    httplib::Client client;
    std::mutex mutex;
    std::optional<int64_t> server_max_request_bytes;
};

RpcRemoteError::RpcRemoteError(std::string exception_type, std::string message,
                               std::string error_kind, std::string server_id,
                               std::string request_id, int http_status)
    : HttpClientError(exception_type + ": " + message, http_status),
      exception_type_(std::move(exception_type)),
      error_kind_(std::move(error_kind)),
      server_id_(std::move(server_id)),
      request_id_(std::move(request_id)) {}

HttpClient::HttpClient(std::string base_url, HttpClientConfig config) {
    const size_t scheme = base_url.find("://");
    if (scheme == std::string::npos || base_url.substr(0, scheme) != "http") {
        throw std::invalid_argument("HttpClient base_url must use http://");
    }
    const size_t authority_begin = scheme + 3;
    const size_t delimiter = base_url.find_first_of("/?#", authority_begin);
    const std::string authority = base_url.substr(authority_begin, delimiter - authority_begin);
    if (authority.empty()) {
        throw std::invalid_argument("HttpClient base_url must contain a host");
    }
    if (authority.find('@') != std::string::npos && !config.allow_insecure_credentials) {
        throw std::invalid_argument(
            "credentials over plain HTTP require allow_insecure_credentials=true");
    }
    if (delimiter != std::string::npos && base_url[delimiter] != '/') {
        throw std::invalid_argument("HttpClient base_url must not contain query or fragment");
    }
    if (delimiter != std::string::npos) {
        while (base_url.size() > scheme + 3 && base_url.back() == '/') base_url.pop_back();
        if (base_url.find('/', authority_begin) != std::string::npos) {
            throw std::invalid_argument("HttpClient base_url must not contain a path; use prefix");
        }
    }
    state_ = std::make_shared<HttpClientState>(std::move(base_url), std::move(config));
}

HttpClient::~HttpClient() = default;

AnnotatedBatch HttpClient::call(const std::string& method, const AnnotatedBatch& request,
                                std::shared_ptr<arrow::Schema> expected_output_schema) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    const auto metadata =
        request_metadata(request.custom_metadata, method, state_->config.protocol_version);
    const auto response =
        state_->post(state_->path(method), encode_ipc(request, metadata, state_->request_cap()));
    auto decoded = decode_response(response, state_->config, ResponseShape::UNARY);
    if (expected_output_schema && !schema_equals(decoded.schema, expected_output_schema)) {
        throw HttpClientError(
            schema_mismatch("RPC response", expected_output_schema, decoded.schema));
    }
    if (decoded.data.size() != 1) {
        throw HttpClientError("unary RPC response must contain exactly one data batch");
    }
    decoded.data[0].custom_metadata = strip_transport_metadata(decoded.data[0].custom_metadata);
    return std::move(decoded.data[0]);
}

class HttpExchangeSession::Impl {
public:
    std::shared_ptr<HttpClientState> state;
    std::string method;
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::Schema> output_schema;
    std::string cursor;
    std::string call_state;
    bool is_active = true;
};

HttpExchangeSession HttpClient::open_exchange(const std::string& method,
                                              const AnnotatedBatch& request,
                                              std::shared_ptr<arrow::Schema> input_schema,
                                              std::shared_ptr<arrow::Schema> output_schema) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    if (!input_schema || !output_schema) {
        throw std::invalid_argument("exchange input and output schemas must not be null");
    }
    const auto metadata =
        request_metadata(request.custom_metadata, method, state_->config.protocol_version);
    const auto response = state_->post(state_->path(method, "/init"),
                                       encode_ipc(request, metadata, state_->request_cap()));
    auto decoded = decode_response(response, state_->config, ResponseShape::EXCHANGE_INIT);
    if (!schema_equals(decoded.schema, output_schema)) {
        throw HttpClientError(
            schema_mismatch("exchange init response", output_schema, decoded.schema));
    }

    auto impl = std::make_unique<HttpExchangeSession::Impl>();
    impl->state = state_;
    impl->method = method;
    impl->input_schema = std::move(input_schema);
    impl->output_schema = std::move(output_schema);
    if (!decoded.data.empty()) {
        throw HttpClientError("exchange init unexpectedly returned application data");
    }
    for (const auto& batch : decoded.control) {
        const std::string cursor = get_metadata_value(batch.custom_metadata, keys::STATE_B64);
        if (!cursor.empty()) impl->cursor = cursor;
        const std::string call_state =
            get_metadata_value(batch.custom_metadata, keys::CALL_STATE_B64);
        if (!call_state.empty()) impl->call_state = call_state;
    }
    if (impl->cursor.empty() || impl->call_state.empty()) {
        throw HttpClientError("exchange init response must contain cursor and call-state tokens");
    }
    return HttpExchangeSession(std::move(impl));
}

HttpExchangeSession::HttpExchangeSession(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

HttpExchangeSession::~HttpExchangeSession() {
    close();
}

HttpExchangeSession::HttpExchangeSession(HttpExchangeSession&&) noexcept = default;

HttpExchangeSession& HttpExchangeSession::operator=(HttpExchangeSession&& other) noexcept {
    if (this != &other) {
        close();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

AnnotatedBatch HttpExchangeSession::exchange(const AnnotatedBatch& input) {
    if (!impl_ || !impl_->is_active) throw HttpClientError("exchange session is closed");
    if (!input.batch) throw std::invalid_argument("exchange input batch must not be null");
    if (!schema_equals(input.batch->schema(), impl_->input_schema)) {
        throw std::invalid_argument(
            schema_mismatch("exchange input", impl_->input_schema, input.batch->schema()));
    }

    auto metadata = sanitized_metadata(input.custom_metadata);
    replace_metadata(metadata, keys::STATE_B64, impl_->cursor);
    if (!impl_->call_state.empty()) {
        replace_metadata(metadata, keys::CALL_STATE_B64, impl_->call_state);
    }
    const auto body = encode_ipc(input, metadata, impl_->state->request_cap());
    // Once the request leaves this process, failure is ambiguous: the worker
    // may have advanced the state even if its reply was lost.  Retire the old
    // cursor before POST and reactivate only after a complete response yields
    // a new one, preventing accidental duplicate execution.
    impl_->is_active = false;
    const auto response = impl_->state->post(impl_->state->path(impl_->method, "/exchange"), body);
    auto decoded = decode_response(response, impl_->state->config, ResponseShape::EXCHANGE_TURN);
    if (!schema_equals(decoded.schema, impl_->output_schema)) {
        throw HttpClientError(
            schema_mismatch("exchange response", impl_->output_schema, decoded.schema));
    }
    if (decoded.data.size() != 1) {
        throw HttpClientError("exchange response must contain exactly one data batch");
    }
    auto output = std::move(decoded.data[0]);
    if (!schema_equals(output.batch->schema(), impl_->output_schema)) {
        throw HttpClientError(
            schema_mismatch("exchange output", impl_->output_schema, output.batch->schema()));
    }
    const std::string cursor = get_metadata_value(output.custom_metadata, keys::STATE_B64);
    if (cursor.empty()) {
        throw HttpClientError("exchange response did not contain a continuation token");
    }
    impl_->cursor = cursor;
    impl_->is_active = true;
    output.custom_metadata = strip_transport_metadata(output.custom_metadata);
    return output;
}

void HttpExchangeSession::close() noexcept {
    if (!impl_ || !impl_->is_active) return;
    impl_->is_active = false;
}

void HttpExchangeSession::cancel() noexcept {
    if (!impl_ || !impl_->is_active) return;
    impl_->is_active = false;
    try {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        replace_metadata(metadata, keys::STATE_B64, impl_->cursor);
        if (!impl_->call_state.empty()) {
            replace_metadata(metadata, keys::CALL_STATE_B64, impl_->call_state);
        }
        replace_metadata(metadata, keys::CANCEL, "1");
        const AnnotatedBatch cancel = AnnotatedBatch::data(make_empty_batch(empty_schema()));
        (void)impl_->state->post(impl_->state->path(impl_->method, "/exchange"),
                                 encode_ipc(cancel, metadata, impl_->state->request_cap()));
    } catch (const std::exception&) {
        // Cancellation is best-effort.  The opaque token's own expiry remains
        // the fallback when the peer is unavailable.
    }
}

bool HttpExchangeSession::active() const noexcept {
    return impl_ && impl_->is_active;
}

}  // namespace vgi_rpc
