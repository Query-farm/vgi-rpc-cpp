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
#include <zstd.h>

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
constexpr const char* kZstdEncoding = "zstd";

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
          keys::REQUEST_ID, keys::CALL_STATE_B64, keys::STREAM_STATE, keys::CANCEL, keys::LOCATION,
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
    const std::string& protocol_version, const std::string& request_id) {
    auto metadata = sanitized_metadata(original);
    replace_metadata(metadata, keys::METHOD, method);
    replace_metadata(metadata, keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    replace_metadata(metadata, keys::REQUEST_ID, request_id);
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
    std::string content_type;
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
        if (batch.batch && batch.batch->num_rows() == 0 && has(keys::LOG_LEVEL) &&
            has(keys::LOG_MESSAGE)) {
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

std::string trim_ascii(std::string value) {
    const auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
    return value;
}

bool includes_encoding(const std::string& raw, const std::string& wanted) {
    std::istringstream stream(raw);
    std::string item;
    while (std::getline(stream, item, ',')) {
        if (ascii_lower(trim_ascii(std::move(item))) == wanted) return true;
    }
    return false;
}

bool includes_encoding_list(const std::vector<std::string>& values, const std::string& wanted) {
    return std::any_of(values.begin(), values.end(), [&](const std::string& value) {
        return ascii_lower(trim_ascii(value)) == wanted;
    });
}

std::vector<std::string> parse_list(const std::string& raw, bool lowercase = false) {
    std::vector<std::string> values;
    std::istringstream stream(raw);
    std::string item;
    while (std::getline(stream, item, ',')) {
        item = trim_ascii(std::move(item));
        if (item.empty()) continue;
        values.push_back(lowercase ? ascii_lower(std::move(item)) : std::move(item));
    }
    return values;
}

void harvest_capabilities(const httplib::Response& response, HttpServerCapabilities& caps) {
    if (response.has_header("VGI-Sticky-Enabled")) {
        caps.sticky_enabled =
            ascii_lower(response.get_header_value("VGI-Sticky-Enabled")) == "true";
    }
    if (auto value = positive_header_int(response, "VGI-Sticky-Default-TTL")) {
        caps.sticky_default_ttl = value;
    }
    if (response.has_header("VGI-Sticky-Echo-Headers")) {
        caps.sticky_echo_headers = parse_list(response.get_header_value("VGI-Sticky-Echo-Headers"));
    }
    if (response.has_header("VGI-Upload-URL-Support")) {
        caps.upload_url_support =
            ascii_lower(response.get_header_value("VGI-Upload-URL-Support")) == "true";
    }
    if (auto value = positive_header_int(response, "VGI-Max-Request-Bytes")) {
        caps.max_request_bytes = value;
    }
    if (auto value = positive_header_int(response, "VGI-Max-Response-Bytes")) {
        caps.max_response_bytes = value;
    }
    if (auto value = positive_header_int(response, "VGI-Max-Externalized-Response-Bytes")) {
        caps.max_externalized_response_bytes = value;
    }
    if (response.has_header("VGI-Externalization-Enabled")) {
        caps.externalization_enabled =
            ascii_lower(response.get_header_value("VGI-Externalization-Enabled")) == "true";
    }
    if (auto value = positive_header_int(response, "VGI-Max-Upload-Bytes")) {
        caps.max_upload_bytes = value;
    }
    if (response.has_header("VGI-Supported-Encodings")) {
        caps.supported_encodings =
            parse_list(response.get_header_value("VGI-Supported-Encodings"), true);
    }
}

std::string encode_zstd(const std::string& body, int level, int64_t max_encoded_bytes) {
    struct Deleter {
        void operator()(ZSTD_CCtx* context) const noexcept { (void)ZSTD_freeCCtx(context); }
    };
    std::unique_ptr<ZSTD_CCtx, Deleter> context(ZSTD_createCCtx());
    if (!context) throw HttpClientError("cannot allocate zstd request encoder");
    for (const auto [parameter, value] :
         {std::pair{ZSTD_c_compressionLevel, level}, std::pair{ZSTD_c_contentSizeFlag, 1}}) {
        const size_t configured = ZSTD_CCtx_setParameter(context.get(), parameter, value);
        if (ZSTD_isError(configured)) {
            throw HttpClientError(std::string("cannot configure zstd request encoder: ") +
                                  ZSTD_getErrorName(configured));
        }
    }
    const size_t pledged = ZSTD_CCtx_setPledgedSrcSize(context.get(), body.size());
    if (ZSTD_isError(pledged)) {
        throw HttpClientError(std::string("cannot size zstd request input: ") +
                              ZSTD_getErrorName(pledged));
    }

    ZSTD_inBuffer input{body.data(), body.size(), 0};
    std::string encoded;
    encoded.reserve(static_cast<size_t>(std::min<int64_t>(max_encoded_bytes, 64 * 1024)));
    size_t remaining = 1;
    do {
        const size_t previous_input = input.pos;
        char chunk[64 * 1024];
        ZSTD_outBuffer output{chunk, sizeof(chunk), 0};
        remaining = ZSTD_compressStream2(context.get(), &output, &input, ZSTD_e_end);
        if (ZSTD_isError(remaining)) {
            throw HttpClientError(std::string("cannot compress HTTP RPC request: ") +
                                  ZSTD_getErrorName(remaining));
        }
        if (output.pos > static_cast<uint64_t>(max_encoded_bytes) ||
            encoded.size() > static_cast<uint64_t>(max_encoded_bytes) - output.pos) {
            throw HttpClientError("encoded HTTP RPC request exceeds max_request_bytes (" +
                                  std::to_string(max_encoded_bytes) + ")");
        }
        encoded.append(chunk, output.pos);
        if (output.pos == 0 && input.pos == previous_input && remaining != 0) {
            throw HttpClientError("zstd request encoder made no progress");
        }
    } while (remaining != 0);
    return encoded;
}

int zstd_window_log_for_limit(int64_t max_bytes) {
    // A streaming encoder may advertise a 512 KiB window even for a tiny
    // frame.  Permit that interoperable floor while preventing a frame header
    // from making the decoder allocate an attacker-selected history window.
    uint64_t bounded = std::max<uint64_t>(static_cast<uint64_t>(max_bytes), 512 * 1024);
    int log = 0;
    uint64_t value = bounded - 1;
    while (value != 0) {
        ++log;
        value >>= 1;
    }
    return std::clamp(log, 19, 31);
}

std::string decode_zstd(const std::string& encoded, int64_t max_decoded_bytes, int http_status) {
    struct Deleter {
        void operator()(ZSTD_DCtx* context) const noexcept { (void)ZSTD_freeDCtx(context); }
    };
    std::unique_ptr<ZSTD_DCtx, Deleter> context(ZSTD_createDCtx());
    if (!context) throw HttpClientError("cannot allocate zstd response decoder", http_status);
    const size_t configured = ZSTD_DCtx_setParameter(context.get(), ZSTD_d_windowLogMax,
                                                     zstd_window_log_for_limit(max_decoded_bytes));
    if (ZSTD_isError(configured)) {
        throw HttpClientError(
            std::string("cannot bound zstd response window: ") + ZSTD_getErrorName(configured),
            http_status);
    }

    ZSTD_inBuffer input{encoded.data(), encoded.size(), 0};
    std::string decoded;
    decoded.reserve(static_cast<size_t>(std::min<int64_t>(max_decoded_bytes, 64 * 1024)));
    size_t remaining = 1;
    do {
        const size_t previous_input = input.pos;
        char chunk[64 * 1024];
        ZSTD_outBuffer output{chunk, sizeof(chunk), 0};
        remaining = ZSTD_decompressStream(context.get(), &output, &input);
        if (ZSTD_isError(remaining)) {
            throw HttpClientError(
                std::string("invalid zstd HTTP RPC response: ") + ZSTD_getErrorName(remaining),
                http_status);
        }
        if (output.pos > static_cast<uint64_t>(max_decoded_bytes) ||
            decoded.size() > static_cast<uint64_t>(max_decoded_bytes) - output.pos) {
            throw HttpClientError("decoded HTTP RPC response exceeds max_decoded_response_bytes (" +
                                      std::to_string(max_decoded_bytes) + ")",
                                  http_status);
        }
        decoded.append(chunk, output.pos);
        if (output.pos == 0 && input.pos == previous_input && remaining != 0) {
            throw HttpClientError("truncated zstd HTTP RPC response", http_status);
        }
    } while (input.pos < input.size || remaining != 0);
    return decoded;
}

void validate_headers(const HttpClientConfig& config) {
    const std::vector<std::string> reserved = {
        "accept-encoding",   "content-encoding", "content-length",        "content-type", "host",
        "transfer-encoding", "x-request-id",     "x-vgi-accept-encoding",
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
        if (config.max_request_bytes <= 0 || config.max_response_bytes < 0 ||
            config.max_encoded_response_bytes < 0 || config.max_decoded_response_bytes < 0) {
            throw std::invalid_argument(
                "HTTP client request and response caps must not be negative");
        }
        max_encoded_response_bytes = config.max_encoded_response_bytes > 0
                                         ? config.max_encoded_response_bytes
                                         : config.max_response_bytes;
        max_decoded_response_bytes = config.max_decoded_response_bytes > 0
                                         ? config.max_decoded_response_bytes
                                         : config.max_response_bytes;
        if (max_encoded_response_bytes <= 0 || max_decoded_response_bytes <= 0) {
            throw std::invalid_argument(
                "HTTP client encoded and decoded response caps must resolve to positive values");
        }
        validate_headers(config);
        client.set_keep_alive(config.keep_alive);
        client.set_decompress(false);
        client.set_connection_timeout(config.connection_timeout_seconds);
        client.set_read_timeout(config.read_timeout_seconds);
        client.set_write_timeout(config.write_timeout_seconds);
        send_compressed = config.compression_level.has_value();
        if (config.prefix.empty()) return;
        if (config.prefix.front() != '/') config.prefix.insert(config.prefix.begin(), '/');
        while (config.prefix.size() > 1 && config.prefix.back() == '/') config.prefix.pop_back();
    }

    BoundedHttpResponse post(const std::string& path, const std::string& body,
                             const std::string& request_id) {
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

        auto send = [&](bool compress) {
            httplib::Headers headers;
            for (const auto& [name, value] : config.headers) headers.emplace(name, value);
            // The VGI preference header prevents generic HTTP stacks from
            // choosing gzip simply because they injected it before zstd.
            headers.emplace("Accept-Encoding", "zstd, identity");
            headers.emplace("X-VGI-Accept-Encoding", "zstd, identity");
            headers.emplace("X-Request-ID", request_id);
            std::string encoded;
            const std::string* payload = &body;
            if (compress) {
                encoded = encode_zstd(body, *config.compression_level, request_cap);
                payload = &encoded;
                headers.emplace("Content-Encoding", kZstdEncoding);
            }

            BoundedHttpResponse response;
            bool response_too_large = false;
            auto result = client.Post(
                path, headers, *payload, kArrowContentType, [&](const char* data, size_t size) {
                    if (size > static_cast<uint64_t>(max_encoded_response_bytes) ||
                        response.body.size() >
                            static_cast<uint64_t>(max_encoded_response_bytes) - size) {
                        response_too_large = true;
                        return false;
                    }
                    response.body.append(data, size);
                    return true;
                });
            if (response_too_large) {
                throw HttpClientError(
                    "encoded HTTP RPC response exceeds max_encoded_response_bytes (" +
                    std::to_string(max_encoded_response_bytes) + ")");
            }
            if (!result) {
                throw HttpClientError(std::string("HTTP RPC transport failed: ") +
                                      httplib::to_string(result.error()));
            }
            response.status = result->status;
            response.rpc_error = result->get_header_value("X-VGI-RPC-Error") == "true";
            response.content_type = result->get_header_value("Content-Type");
            harvest_capabilities(*result, server_capabilities);
            if (server_capabilities.max_request_bytes) {
                server_max_request_bytes = server_capabilities.max_request_bytes;
            }
            if (result->has_header("VGI-Supported-Encodings")) {
                const std::string supported = result->get_header_value("VGI-Supported-Encodings");
                if (!includes_encoding(supported, kZstdEncoding)) send_compressed = false;
            }

            std::string content_encoding = result->get_header_value("Content-Encoding");
            if (content_encoding.empty()) {
                content_encoding = result->get_header_value("X-VGI-Content-Encoding");
            }
            content_encoding = ascii_lower(trim_ascii(std::move(content_encoding)));
            if (content_encoding.empty() || content_encoding == "identity") {
                if (response.body.size() > static_cast<uint64_t>(max_decoded_response_bytes)) {
                    throw HttpClientError(
                        "decoded HTTP RPC response exceeds max_decoded_response_bytes (" +
                            std::to_string(max_decoded_response_bytes) + ")",
                        response.status);
                }
            } else if (content_encoding == kZstdEncoding) {
                response.body =
                    decode_zstd(response.body, max_decoded_response_bytes, response.status);
            } else {
                throw HttpClientError(
                    "unsupported HTTP response Content-Encoding: " + content_encoding,
                    response.status);
            }
            return response;
        };

        const bool compressed_attempt = send_compressed;
        BoundedHttpResponse response = send(compressed_attempt);
        // A 415 definitively rejects the request before dispatch.  Retrying
        // this one status with identity is therefore safe even for exchange;
        // every transport/parse failure remains ambiguous and is never retried.
        if (response.status == 415 && compressed_attempt) {
            send_compressed = false;
            response = send(false);
        }
        const std::string& content_type = response.content_type;
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

    HttpServerCapabilities capabilities() {
        std::lock_guard<std::mutex> lock(mutex);
        if (capabilities_fetched) return server_capabilities;
        httplib::Headers headers;
        for (const auto& [name, value] : config.headers) headers.emplace(name, value);
        headers.emplace("Accept-Encoding", "identity");
        headers.emplace("X-Request-ID", random_hex(16));
        auto result = client.Options(path("health"), headers);
        if (!result) {
            throw HttpClientError(std::string("HTTP capability discovery failed: ") +
                                  httplib::to_string(result.error()));
        }
        if (result->status < 200 || result->status >= 300) {
            throw HttpClientError(
                "HTTP capability discovery returned status " + std::to_string(result->status),
                result->status);
        }
        // OPTIONS is the complete model.  Reset first so absent boolean and
        // optional headers cannot retain a value harvested from an earlier RPC.
        server_capabilities = HttpServerCapabilities{};
        harvest_capabilities(*result, server_capabilities);
        if (server_capabilities.max_request_bytes) {
            server_max_request_bytes = server_capabilities.max_request_bytes;
        }
        if (!includes_encoding_list(server_capabilities.supported_encodings, kZstdEncoding)) {
            send_compressed = false;
        }
        capabilities_fetched = true;
        return server_capabilities;
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
    int64_t max_encoded_response_bytes = 0;
    int64_t max_decoded_response_bytes = 0;
    bool send_compressed = false;
    bool capabilities_fetched = false;
    HttpServerCapabilities server_capabilities;
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
    const std::string request_id = random_hex(16);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response = state_->post(
        state_->path(method), encode_ipc(request, metadata, state_->request_cap()), request_id);
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

HttpServerCapabilities HttpClient::capabilities() const {
    if (!state_) throw HttpClientError("HttpClient is moved from");
    return state_->capabilities();
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
    const std::string request_id = random_hex(16);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response =
        state_->post(state_->path(method, "/init"),
                     encode_ipc(request, metadata, state_->request_cap()), request_id);
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
    const std::string request_id = random_hex(16);
    replace_metadata(metadata, keys::REQUEST_ID, request_id);
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
    const auto response =
        impl_->state->post(impl_->state->path(impl_->method, "/exchange"), body, request_id);
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
        const std::string request_id = random_hex(16);
        replace_metadata(metadata, keys::REQUEST_ID, request_id);
        replace_metadata(metadata, keys::STATE_B64, impl_->cursor);
        if (!impl_->call_state.empty()) {
            replace_metadata(metadata, keys::CALL_STATE_B64, impl_->call_state);
        }
        replace_metadata(metadata, keys::CANCEL, "1");
        const AnnotatedBatch cancel = AnnotatedBatch::data(make_empty_batch(empty_schema()));
        (void)impl_->state->post(impl_->state->path(impl_->method, "/exchange"),
                                 encode_ipc(cancel, metadata, impl_->state->request_cap()),
                                 request_id);
    } catch (const std::exception&) {
        // Cancellation is best-effort.  The opaque token's own expiry remains
        // the fallback when the peer is unavailable.
    }
}

bool HttpExchangeSession::active() const noexcept {
    return impl_ && impl_->is_active;
}

}  // namespace vgi_rpc
