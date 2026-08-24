// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/http_client.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/buffer.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <zstd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <sstream>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace vgi_rpc {

namespace {

constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";
constexpr const char* kZstdEncoding = "zstd";
constexpr size_t kMaxStructuredErrorBodyBytes = 4096;
constexpr size_t kMaxStructuredErrorHeaderBytes = 1024;

bool deadline_expired(const CallOptions& options) {
    return options.deadline && std::chrono::steady_clock::now() >= *options.deadline;
}

void check_call_active(const CallOptions& options, const std::string& method,
                       const std::string& request_id) {
    if (options.stop_token.stop_requested()) {
        throw HttpClientError(HttpClientErrorKind::CANCELLED, "HTTP RPC call was cancelled", 0,
                              method, request_id);
    }
    if (deadline_expired(options)) {
        throw HttpClientError(HttpClientErrorKind::TIMEOUT, "HTTP RPC call deadline exceeded", 0,
                              method, request_id);
    }
}

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

std::string external_pointer_body(const std::string& original_body, const std::string& download_url,
                                  int64_t max_bytes) {
    auto input =
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(original_body));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) {
        throw HttpClientError("externalized request must contain exactly one Arrow batch");
    }
    const int64_t consumed = unwrap(input->Tell(), "cannot inspect externalized request");
    if (consumed != static_cast<int64_t>(original_body.size())) {
        throw HttpClientError("externalized request contains trailing Arrow IPC data");
    }
    auto metadata = contents->batches.front().custom_metadata
                        ? contents->batches.front().custom_metadata->Copy()
                        : std::make_shared<arrow::KeyValueMetadata>();
    replace_metadata(metadata, keys::LOCATION, download_url);
    crypto::Sha256 digest;
    digest.update(original_body);
    replace_metadata(metadata, keys::LOCATION_SHA256, digest.hex_digest());
    return encode_ipc(AnnotatedBatch::data(make_empty_batch(contents->schema)), metadata,
                      max_bytes);
}

std::shared_ptr<arrow::Schema> upload_url_request_schema() {
    return arrow::schema({arrow::field("count", arrow::int64(), true)});
}

std::shared_ptr<arrow::Schema> upload_url_response_schema() {
    return arrow::schema(
        {arrow::field("upload_url", arrow::utf8(), true),
         arrow::field("download_url", arrow::utf8(), true),
         arrow::field("expires_at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), true)});
}

AnnotatedBatch upload_url_request(int64_t count) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(count));
    return AnnotatedBatch::data(
        arrow::RecordBatch::Make(upload_url_request_schema(), 1, {unwrap(builder.Finish())}));
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
    std::string www_authenticate;
    std::string retry_after;
    std::string auth_reason;
    std::string body;
    struct StickyUpdate {
        std::optional<std::string> token;
        std::map<std::string, std::string> echo_headers;
    };
    std::optional<StickyUpdate> sticky_update;
    // Keeps this session's request/response/Arrow-validation transaction
    // serialized until its staged routing state is either committed or rejected.
    std::unique_ptr<std::unique_lock<std::timed_mutex>> sticky_guard;
};

std::string bounded_text(std::string value, size_t limit) {
    if (value.size() > limit) value.resize(limit);
    return value;
}

std::string safe_error_detail(std::string value, size_t limit) {
    value = bounded_text(std::move(value), limit);
    for (char& character : value) {
        const auto byte = static_cast<unsigned char>(character);
        if (byte < 0x20 || byte == 0x7f) character = ' ';
    }
    return value;
}

struct DecodedResponse {
    std::shared_ptr<arrow::Schema> schema;
    std::optional<AnnotatedBatch> header;
    std::vector<AnnotatedBatch> data;
    std::vector<AnnotatedBatch> control;
};

enum class ResponseShape {
    UNARY,
    EXCHANGE_INIT,
    EXCHANGE_TURN,
    STREAM_INIT,
    PRODUCER_TURN,
};

[[noreturn]] void throw_remote_error(const AnnotatedBatch& batch, int status) {
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
    const std::string server_id = get_metadata_value(metadata, keys::SERVER_ID);
    const std::string request_id = get_metadata_value(metadata, keys::REQUEST_ID);
    if (exception_type == "SessionLostError") {
        throw HttpSessionLostError(std::move(message), std::move(error_kind), server_id, request_id,
                                   status);
    }
    throw RpcRemoteError(std::move(exception_type), std::move(message), std::move(error_kind),
                         server_id, request_id, status);
}

DecodedResponse decode_response(const BoundedHttpResponse& response, const HttpClientConfig& config,
                                ResponseShape shape, bool has_header = false,
                                ClientExternalHttp* external = nullptr) {
    // IPC arrays may retain slices of their source buffer.  FromString owns a
    // copy whose shared lifetime follows those slices; wrapping response.body
    // would leave returned batches pointing at a destroyed std::string.
    auto buffer = arrow::Buffer::FromString(response.body);
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    DecodedResponse decoded;
    auto read_stream = [&]() -> IpcStreamContents {
        try {
            auto contents = read_ipc_stream(input);
            if (!contents) {
                throw HttpClientError("HTTP RPC response contained no Arrow IPC stream",
                                      response.status);
            }
            return std::move(*contents);
        } catch (const HttpClientError&) {
            throw;
        } catch (const std::exception& error) {
            throw HttpClientError(std::string("invalid Arrow IPC response: ") + error.what(),
                                  response.status);
        }
    };

    auto process_batches = [&](IpcStreamContents contents, ResponseShape stream_shape,
                               bool header_stream) {
        if (header_stream) {
            std::vector<AnnotatedBatch> header_data;
            for (auto& batch : contents.batches) {
                if (batch.custom_metadata && batch.custom_metadata->FindKey(keys::LOCATION) >= 0) {
                    if (!external) {
                        throw HttpClientError(
                            "external-location stream header received while resolution is disabled",
                            response.status);
                    }
                    try {
                        batch = external->resolve_pointer(batch, config.on_log);
                    } catch (const ExternalHttpError& error) {
                        throw HttpClientError(
                            HttpClientErrorKind::PROTOCOL,
                            std::string("failed to resolve external-location stream header: ") +
                                error.what(),
                            response.status, {}, {});
                    }
                }
                const auto level = get_metadata_value(batch.custom_metadata, keys::LOG_LEVEL);
                if (batch.batch && batch.batch->num_rows() == 0 && !level.empty()) {
                    if (level == "EXCEPTION") throw_remote_error(batch, response.status);
                    if (config.on_log) config.on_log(batch);
                    continue;
                }
                header_data.push_back(std::move(batch));
            }
            if (header_data.size() != 1) {
                throw HttpClientError("stream header response must contain exactly one data batch",
                                      response.status);
            }
            decoded.header = std::move(header_data.front());
            return;
        }

        decoded.schema = contents.schema;
        for (auto& batch : contents.batches) {
            if (batch.custom_metadata && batch.custom_metadata->FindKey(keys::LOCATION) >= 0) {
                if (!external) {
                    throw HttpClientError(
                        "external-location response received while resolution is disabled",
                        response.status);
                }
                try {
                    batch = external->resolve_pointer(batch, config.on_log);
                } catch (const ExternalHttpError& error) {
                    throw HttpClientError(
                        HttpClientErrorKind::PROTOCOL,
                        std::string("failed to resolve external-location response: ") +
                            error.what(),
                        response.status, {}, {});
                }
            }
            const auto& metadata = batch.custom_metadata;
            const auto has = [&](const char* key) {
                return metadata && metadata->FindKey(key) >= 0;
            };

            // Protocol controls take precedence over row count.  In particular,
            // a malformed non-empty pointer must not become application data just
            // because classify_batch's general-purpose heuristic sees rows first.
            if (batch.batch && batch.batch->num_rows() == 0 && has(keys::LOG_LEVEL) &&
                has(keys::LOG_MESSAGE)) {
                if (get_metadata_value(metadata, keys::LOG_LEVEL) == "EXCEPTION") {
                    throw_remote_error(batch, response.status);
                }
                if (config.on_log) config.on_log(batch);
                continue;
            }
            if (has(keys::LOCATION)) {
                throw HttpClientError("nested external-location response is forbidden",
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
            if (has_cursor && (stream_shape == ResponseShape::EXCHANGE_INIT ||
                               stream_shape == ResponseShape::STREAM_INIT ||
                               stream_shape == ResponseShape::PRODUCER_TURN)) {
                if (!batch.batch || batch.batch->num_rows() != 0) {
                    throw HttpClientError("exchange init returned a non-empty state control batch",
                                          response.status);
                }
                decoded.control.push_back(std::move(batch));
                continue;
            }
            if (has_cursor && stream_shape == ResponseShape::UNARY) {
                throw HttpClientError("unexpected stream control metadata in unary response",
                                      response.status);
            }

            // During an exchange turn the cursor is attached to the application
            // batch.  It remains data even when it contains zero rows; zero rows
            // are not an end-of-stream marker in the HTTP exchange protocol.
            decoded.data.push_back(std::move(batch));
        }
    };

    if (has_header) process_batches(read_stream(), shape, true);
    process_batches(read_stream(), shape, false);

    const int64_t consumed = unwrap(input->Tell(), "cannot inspect HTTP RPC response position");
    if (consumed != buffer->size()) {
        throw HttpClientError("HTTP RPC response contains an unexpected trailing IPC stream",
                              response.status);
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

void validate_headers(const std::map<std::string, std::string>& headers,
                      bool allow_insecure_credentials, bool secure_transport) {
    const std::vector<std::string> reserved = {
        "accept-encoding",   "content-encoding", "content-length",        "content-type", "host",
        "transfer-encoding", "x-request-id",     "x-vgi-accept-encoding",
    };
    const std::vector<std::string> credentials = {"authorization", "cookie", "proxy-authorization"};
    for (const auto& [name, value] : headers) {
        if (name.empty() || name.find_first_of("\r\n:") != std::string::npos ||
            value.find_first_of("\r\n") != std::string::npos) {
            throw std::invalid_argument("HTTP client header contains invalid characters");
        }
        const std::string lower = ascii_lower(name);
        if (std::find(reserved.begin(), reserved.end(), lower) != reserved.end()) {
            throw std::invalid_argument("HTTP client header is transport-reserved: " + name);
        }
        if (!secure_transport && !allow_insecure_credentials &&
            std::find(credentials.begin(), credentials.end(), lower) != credentials.end()) {
            throw std::invalid_argument(
                "credentials over plain HTTP require allow_insecure_credentials=true");
        }
    }
}

void validate_sticky_echo_headers(const std::map<std::string, std::string>& headers,
                                  bool allow_insecure_credentials, bool secure_transport) {
    validate_headers(headers, allow_insecure_credentials, secure_transport);
    if (headers.size() > 100) {
        throw std::invalid_argument("sticky session has too many echo headers");
    }
    size_t total_bytes = 0;
    for (const auto& [name, value] : headers) {
        const std::string lower = ascii_lower(name);
        if (lower == "authorization" || lower == "cookie" || lower == "proxy-authorization") {
            throw std::invalid_argument("credential-bearing sticky echo headers are forbidden: " +
                                        name);
        }
        if (name.size() > 1024 || value.size() > 8192 || total_bytes > 64 * 1024 ||
            name.size() > 64 * 1024 - total_bytes) {
            throw std::invalid_argument("sticky session echo-header state is too large");
        }
        total_bytes += name.size();
        if (value.size() > 64 * 1024 - total_bytes) {
            throw std::invalid_argument("sticky session echo-header state is too large");
        }
        total_bytes += value.size();
    }
}

void merge_headers(httplib::Headers& destination,
                   const std::map<std::string, std::string>& source) {
    for (const auto& [name, value] : source) {
        destination.erase(name);
        destination.emplace(name, value);
    }
}

bool is_retryable_transport_error(HttpClientErrorKind kind) {
    return kind == HttpClientErrorKind::TRANSPORT || kind == HttpClientErrorKind::TIMEOUT;
}

HttpClientErrorKind classify_transport_error(httplib::Error error, const CallOptions& options,
                                             bool secure_transport = false, int tls_error = 0,
                                             unsigned long tls_backend_error = 0) {
    if (options.stop_token.stop_requested()) return HttpClientErrorKind::CANCELLED;
    // cpp-httplib's aggregate max timeout can surface as Read/Write on some
    // TLS/socket backends and may return just before our steady-clock check
    // because its millisecond timeout is integral.  Treat that sub-tick edge
    // as the deadline it represents without misclassifying an early failure.
    const bool at_deadline =
        options.deadline &&
        std::chrono::steady_clock::now() + std::chrono::milliseconds(2) >= *options.deadline;
    if (at_deadline || error == httplib::Error::Timeout ||
        error == httplib::Error::ConnectionTimeout) {
        return HttpClientErrorKind::TIMEOUT;
    }
    if (tls_error != 0 || tls_backend_error != 0 ||
        (secure_transport && (error == httplib::Error::Read || error == httplib::Error::Write)) ||
        error == httplib::Error::SSLConnection || error == httplib::Error::SSLLoadingCerts ||
        error == httplib::Error::SSLServerVerification ||
        error == httplib::Error::SSLServerHostnameVerification) {
        return HttpClientErrorKind::TLS;
    }
    return HttpClientErrorKind::TRANSPORT;
}

bool contains_status(const RetryPolicy& policy, int status) {
    return std::find(policy.retryable_status_codes.begin(), policy.retryable_status_codes.end(),
                     status) != policy.retryable_status_codes.end();
}

std::chrono::milliseconds retry_delay(const RetryPolicy& policy, uint32_t attempt) {
    if (attempt == 0) return std::chrono::milliseconds::zero();
    double milliseconds = static_cast<double>(policy.initial_backoff.count()) *
                          std::pow(policy.multiplier, static_cast<double>(attempt - 1));
    milliseconds = std::min(milliseconds, static_cast<double>(policy.max_backoff.count()));
    if (policy.jitter > 0.0) {
        const std::string entropy = random_hex(8);
        const uint64_t sample = std::stoull(entropy, nullptr, 16);
        const double fraction =
            static_cast<double>(sample) / static_cast<double>(std::numeric_limits<uint64_t>::max());
        milliseconds += milliseconds * policy.jitter * (fraction * 2.0 - 1.0);
    }
    if (!std::isfinite(milliseconds)) milliseconds = policy.max_backoff.count();
    return std::chrono::milliseconds(static_cast<int64_t>(std::max(0.0, std::floor(milliseconds))));
}

void wait_for_retry(std::chrono::milliseconds delay, const CallOptions& options,
                    const std::string& method, const std::string& request_id) {
    const auto end = std::chrono::steady_clock::now() + delay;
    while (std::chrono::steady_clock::now() < end) {
        check_call_active(options, method, request_id);
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            end - std::chrono::steady_clock::now());
        std::this_thread::sleep_for(std::min(remaining, std::chrono::milliseconds(10)));
    }
}

struct ParsedBaseUrl {
    std::string value;
    bool secure = false;
    bool has_userinfo = false;
};

ParsedBaseUrl parse_base_url(std::string base_url) {
    const size_t scheme = base_url.find("://");
    if (scheme == std::string::npos) {
        throw std::invalid_argument("HttpClient base_url must use http:// or https://");
    }
    const std::string scheme_name = base_url.substr(0, scheme);
    if (scheme_name != "http" && scheme_name != "https") {
        throw std::invalid_argument("HttpClient base_url must use http:// or https://");
    }
    const size_t authority_begin = scheme + 3;
    const size_t delimiter = base_url.find_first_of("/?#", authority_begin);
    const std::string authority = base_url.substr(authority_begin, delimiter - authority_begin);
    if (authority.empty()) throw std::invalid_argument("HttpClient base_url must contain a host");
    if (delimiter != std::string::npos && base_url[delimiter] != '/') {
        throw std::invalid_argument("HttpClient base_url must not contain query or fragment");
    }
    if (delimiter != std::string::npos) {
        while (base_url.size() > authority_begin && base_url.back() == '/') base_url.pop_back();
        if (base_url.find('/', authority_begin) != std::string::npos) {
            throw std::invalid_argument("HttpClient base_url must not contain a path; use prefix");
        }
    }
    return {std::move(base_url), scheme_name == "https", authority.find('@') != std::string::npos};
}

std::string logical_request_id(const CallOptions& options) {
    if (!options.request_id) return random_hex(16);
    if (options.request_id->empty() || options.request_id->size() > 256 ||
        options.request_id->find_first_of("\r\n") != std::string::npos) {
        throw std::invalid_argument(
            "CallOptions request_id must be 1-256 characters without CR/LF");
    }
    return *options.request_id;
}

std::vector<uint8_t> pack_resume_token(const std::string& cursor, const std::string& call_state) {
    if (cursor.size() > std::numeric_limits<uint32_t>::max()) {
        throw HttpClientError("stream cursor is too large to export");
    }
    const uint32_t size = static_cast<uint32_t>(cursor.size());
    std::vector<uint8_t> token(4 + cursor.size() + call_state.size());
    token[0] = static_cast<uint8_t>(size);
    token[1] = static_cast<uint8_t>(size >> 8);
    token[2] = static_cast<uint8_t>(size >> 16);
    token[3] = static_cast<uint8_t>(size >> 24);
    std::memcpy(token.data() + 4, cursor.data(), cursor.size());
    std::memcpy(token.data() + 4 + cursor.size(), call_state.data(), call_state.size());
    return token;
}

std::pair<std::string, std::string> unpack_resume_token(const std::vector<uint8_t>& token) {
    if (token.size() < 4) throw std::invalid_argument("resume token is too short");
    const uint32_t cursor_size =
        static_cast<uint32_t>(token[0]) | (static_cast<uint32_t>(token[1]) << 8) |
        (static_cast<uint32_t>(token[2]) << 16) | (static_cast<uint32_t>(token[3]) << 24);
    if (static_cast<uint64_t>(cursor_size) + 4 > token.size()) {
        throw std::invalid_argument("resume token cursor length exceeds the token size");
    }
    const auto* bytes = reinterpret_cast<const char*>(token.data());
    return {std::string(bytes + 4, cursor_size),
            std::string(bytes + 4 + cursor_size, token.size() - 4 - cursor_size)};
}

void validate_retry_policy(const RetryPolicy& policy) {
    if (policy.max_attempts == 0 || policy.initial_backoff.count() < 0 ||
        policy.max_backoff.count() < 0 || !std::isfinite(policy.multiplier) ||
        policy.multiplier <= 0.0 || !std::isfinite(policy.jitter) || policy.jitter < 0.0 ||
        policy.jitter > 1.0) {
        throw std::invalid_argument("invalid HTTP retry policy");
    }
    for (const int status : policy.retryable_status_codes) {
        if (status < 100 || status > 599) {
            throw std::invalid_argument("HTTP retry status codes must be between 100 and 599");
        }
    }
}

}  // namespace

RetryPolicy RetryPolicy::disabled() {
    RetryPolicy policy;
    policy.max_attempts = 1;
    return policy;
}

CallOptions CallOptions::with_timeout(std::chrono::milliseconds timeout) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() + timeout;
    return options;
}

HttpClientError::HttpClientError(std::string message, int http_status)
    : std::runtime_error(std::move(message)), http_status_(http_status) {}

HttpClientError::HttpClientError(HttpClientErrorKind kind, std::string message, int http_status,
                                 std::string method, std::string request_id,
                                 std::string response_body, std::string retry_after,
                                 std::string auth_reason)
    : std::runtime_error(std::move(message)),
      kind_(kind),
      http_status_(http_status),
      method_(std::move(method)),
      request_id_(std::move(request_id)),
      response_body_(bounded_text(std::move(response_body), kMaxStructuredErrorBodyBytes)),
      retry_after_(bounded_text(std::move(retry_after), kMaxStructuredErrorHeaderBytes)),
      auth_reason_(bounded_text(std::move(auth_reason), kMaxStructuredErrorHeaderBytes)) {}

HttpAuthenticationError::HttpAuthenticationError(std::string message, int http_status,
                                                 std::string method, std::string request_id,
                                                 std::string response_body,
                                                 std::string www_authenticate,
                                                 std::string retry_after, std::string auth_reason)
    : HttpClientError(HttpClientErrorKind::AUTHENTICATION, std::move(message), http_status,
                      std::move(method), std::move(request_id), std::move(response_body),
                      std::move(retry_after), std::move(auth_reason)),
      www_authenticate_(bounded_text(std::move(www_authenticate), kMaxStructuredErrorHeaderBytes)) {
}

class HttpStickySessionState {
public:
    mutable std::timed_mutex mutex;
    std::optional<std::string> token;
    std::map<std::string, std::string> echo_headers;
    std::atomic<bool> locally_closed{false};
};

class HttpClientState {
public:
    HttpClientState(ParsedBaseUrl base_url, HttpClientConfig client_config, RetryPolicy retry,
                    TlsOptions tls, HttpAuthCallback auth,
                    std::optional<ClientExternalHttpOptions> external_options)
        : config(std::move(client_config)),
          retry_policy(std::move(retry)),
          tls_options(std::move(tls)),
          auth_callback(std::move(auth)),
          secure_transport(base_url.secure) {
        if (external_options) {
            external_max_upload_bytes = external_options->max_upload_bytes;
            external_http = std::make_unique<ClientExternalHttp>(*external_options);
        }
        if (config.max_request_bytes <= 0 || config.max_response_bytes < 0 ||
            config.max_encoded_response_bytes < 0 || config.max_decoded_response_bytes < 0) {
            throw std::invalid_argument(
                "HTTP client request and response caps must not be negative");
        }
        validate_retry_policy(retry_policy);
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
        validate_headers(config.headers, config.allow_insecure_credentials, secure_transport);
        if (base_url.has_userinfo && !secure_transport && !config.allow_insecure_credentials) {
            throw std::invalid_argument(
                "credentials over plain HTTP require allow_insecure_credentials=true");
        }
        const bool has_cert = !tls_options.client_certificate_file.empty();
        const bool has_key = !tls_options.client_private_key_file.empty();
        if (has_cert != has_key) {
            throw std::invalid_argument(
                "mTLS requires both client certificate and private key files");
        }
        if (!secure_transport && (!tls_options.ca_file.empty() || has_cert ||
                                  tls_options.insecure_skip_verification_for_testing)) {
            throw std::invalid_argument("TLS options require an https:// base URL");
        }
        client =
            std::make_unique<httplib::Client>(base_url.value, tls_options.client_certificate_file,
                                              tls_options.client_private_key_file);
        if (!client->is_valid()) {
            throw HttpClientError(HttpClientErrorKind::TLS, "failed to initialize HTTP/TLS client",
                                  0, {}, {});
        }
        client->set_keep_alive(config.keep_alive);
        client->set_follow_location(false);
        client->set_decompress(false);
        client->set_connection_timeout(config.connection_timeout_seconds);
        client->set_read_timeout(config.read_timeout_seconds);
        client->set_write_timeout(config.write_timeout_seconds);
        if (secure_transport) {
            client->enable_server_certificate_verification(
                !tls_options.insecure_skip_verification_for_testing);
            client->enable_server_hostname_verification(
                !tls_options.insecure_skip_verification_for_testing);
            if (!tls_options.ca_file.empty()) client->set_ca_cert_path(tls_options.ca_file);
        }
        send_compressed = config.compression_level.has_value();
        if (!config.prefix.empty() && config.prefix.front() != '/') {
            config.prefix.insert(config.prefix.begin(), '/');
        }
        while (config.prefix.size() > 1 && config.prefix.back() == '/') config.prefix.pop_back();
    }

    BoundedHttpResponse post_inline(
        const std::string& method, const std::string& request_path, const std::string& body,
        const std::string& request_id, const CallOptions& options, bool retryable,
        const std::shared_ptr<HttpStickySessionState>& sticky_session = nullptr) {
        check_call_active(options, method, request_id);
        std::map<std::string, std::string> auth_headers;
        if (auth_callback) {
            try {
                // Deliberately before the transport mutex: credential refresh
                // may block or re-enter this client.
                auth_headers = auth_callback(HttpAuthRequest{method, request_path, request_id});
                validate_headers(auth_headers, config.allow_insecure_credentials, secure_transport);
            } catch (const std::exception& error) {
                throw HttpClientError(
                    HttpClientErrorKind::AUTHENTICATION,
                    std::string("HTTP credential callback failed: ") + error.what(), 0, method,
                    request_id);
            } catch (...) {
                throw HttpClientError(
                    HttpClientErrorKind::AUTHENTICATION,
                    "HTTP credential callback failed with a non-standard exception", 0, method,
                    request_id);
            }
        }
        validate_headers(options.headers, config.allow_insecure_credentials, secure_transport);

        std::unique_lock<std::timed_mutex> session_lock;
        if (sticky_session) {
            session_lock =
                std::unique_lock<std::timed_mutex>(sticky_session->mutex, std::defer_lock);
            if (!options.deadline && !options.stop_token.stop_possible()) {
                session_lock.lock();
            } else {
                while (!session_lock.try_lock_for(std::chrono::milliseconds(10))) {
                    check_call_active(options, method, request_id);
                }
            }
            if (sticky_session->locally_closed.load()) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "HTTP sticky-session view is closed", 0, method, request_id);
            }
        }

        std::unique_lock<std::timed_mutex> lock(mutex, std::defer_lock);
        if (!options.deadline && !options.stop_token.stop_possible()) {
            lock.lock();
        } else {
            while (!lock.try_lock_for(std::chrono::milliseconds(10))) {
                check_call_active(options, method, request_id);
            }
        }
        check_call_active(options, method, request_id);

        int64_t request_cap = config.max_request_bytes;
        if (server_max_request_bytes && *server_max_request_bytes < request_cap) {
            request_cap = *server_max_request_bytes;
        }
        if (body.size() > static_cast<uint64_t>(request_cap)) {
            throw HttpClientError(HttpClientErrorKind::LIMIT,
                                  "HTTP RPC request exceeds max_request_bytes (" +
                                      std::to_string(body.size()) + " > " +
                                      std::to_string(request_cap) + ")",
                                  0, method, request_id);
        }

        auto send_once = [&](bool compress) {
            check_call_active(options, method, request_id);
            httplib::Headers headers;
            merge_headers(headers, config.headers);
            merge_headers(headers, auth_headers);
            merge_headers(headers, options.headers);
            if (sticky_session) merge_headers(headers, sticky_session->echo_headers);
            if (sticky_session) {
                headers.erase("VGI-Session-Accept");
                headers.emplace("VGI-Session-Accept", "true");
                headers.erase("VGI-Session");
                if (sticky_session->token) {
                    headers.emplace("VGI-Session", *sticky_session->token);
                }
            }
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
            if (options.deadline) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    *options.deadline - std::chrono::steady_clock::now());
                if (remaining <= std::chrono::milliseconds::zero()) {
                    check_call_active(options, method, request_id);
                }
                client->set_max_timeout(std::max<int64_t>(1, remaining.count()));
            } else {
                client->set_max_timeout(0);
            }
            std::stop_callback stop_callback(options.stop_token, [this] { client->stop(); });
            check_call_active(options, method, request_id);
            auto result =
                client->Post(request_path, headers, *payload, kArrowContentType,
                             [&](const char* data, size_t size) {
                                 if (size > static_cast<uint64_t>(max_encoded_response_bytes) ||
                                     response.body.size() >
                                         static_cast<uint64_t>(max_encoded_response_bytes) - size) {
                                     response_too_large = true;
                                     return false;
                                 }
                                 response.body.append(data, size);
                                 return true;
                             });
            client->set_max_timeout(0);
            if (response_too_large) {
                throw HttpClientError(
                    HttpClientErrorKind::LIMIT,
                    "encoded HTTP RPC response exceeds max_encoded_response_bytes (" +
                        std::to_string(max_encoded_response_bytes) + ")",
                    0, method, request_id);
            }
            if (!result) {
                int tls_error = 0;
                unsigned long tls_backend_error = 0;
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
                tls_error = result.ssl_error();
                tls_backend_error = result.ssl_backend_error();
#endif
                const HttpClientErrorKind kind = classify_transport_error(
                    result.error(), options, secure_transport, tls_error, tls_backend_error);
                throw HttpClientError(
                    kind,
                    std::string("HTTP RPC transport failed: ") + httplib::to_string(result.error()),
                    0, method, request_id);
            }
            response.status = result->status;
            response.rpc_error = result->get_header_value("X-VGI-RPC-Error") == "true";
            response.content_type = result->get_header_value("Content-Type");
            response.www_authenticate = result->get_header_value("WWW-Authenticate");
            response.retry_after = result->get_header_value("Retry-After");
            response.auth_reason = result->get_header_value("VGI-Auth-Reason");
            if (sticky_session) {
                response.sticky_update =
                    stage_sticky_response(*result, sticky_session, method, request_id);
            }
            harvest_capabilities(*result, server_capabilities);
            if (server_capabilities.max_request_bytes) {
                server_max_request_bytes = server_capabilities.max_request_bytes;
            }
            if (result->has_header("VGI-Supported-Encodings") &&
                !includes_encoding(result->get_header_value("VGI-Supported-Encodings"),
                                   kZstdEncoding)) {
                send_compressed = false;
            }

            std::string content_encoding = result->get_header_value("Content-Encoding");
            if (content_encoding.empty()) {
                content_encoding = result->get_header_value("X-VGI-Content-Encoding");
            }
            content_encoding = ascii_lower(trim_ascii(std::move(content_encoding)));
            if (content_encoding.empty() || content_encoding == "identity") {
                if (response.body.size() > static_cast<uint64_t>(max_decoded_response_bytes)) {
                    throw HttpClientError(
                        HttpClientErrorKind::LIMIT,
                        "decoded HTTP RPC response exceeds max_decoded_response_bytes (" +
                            std::to_string(max_decoded_response_bytes) + ")",
                        response.status, method, request_id);
                }
            } else if (content_encoding == kZstdEncoding) {
                response.body =
                    decode_zstd(response.body, max_decoded_response_bytes, response.status);
            } else {
                throw HttpClientError(
                    HttpClientErrorKind::PROTOCOL,
                    "unsupported HTTP response Content-Encoding: " + content_encoding,
                    response.status, method, request_id);
            }
            return response;
        };

        auto send = [&](bool compress) {
            const uint32_t attempts =
                retryable && options.idempotent ? retry_policy.max_attempts : 1;
            for (uint32_t attempt = 0; attempt < attempts; ++attempt) {
                if (attempt > 0) {
                    wait_for_retry(retry_delay(retry_policy, attempt), options, method, request_id);
                }
                try {
                    auto response = send_once(compress);
                    if (response.status != 415 && attempt + 1 < attempts &&
                        contains_status(retry_policy, response.status)) {
                        continue;
                    }
                    return response;
                } catch (const HttpClientError& error) {
                    if (attempt + 1 >= attempts || !is_retryable_transport_error(error.kind()) ||
                        error.kind() == HttpClientErrorKind::CANCELLED ||
                        deadline_expired(options)) {
                        throw;
                    }
                }
            }
            throw HttpClientError(HttpClientErrorKind::TRANSPORT,
                                  "HTTP RPC request exhausted retries", 0, method, request_id);
        };

        const bool compressed_attempt = send_compressed;
        BoundedHttpResponse response = send(compressed_attempt);
        if (response.status == 415 && compressed_attempt) {
            send_compressed = false;
            response = send(false);
        }
        auto retain_session_lock = [&](BoundedHttpResponse value) {
            if (sticky_session && session_lock.owns_lock()) {
                value.sticky_guard =
                    std::make_unique<std::unique_lock<std::timed_mutex>>(std::move(session_lock));
            }
            return value;
        };
        const std::string& content_type = response.content_type;
        if (response.status == 401 || response.status == 403) {
            throw HttpAuthenticationError(
                "HTTP RPC authentication failed with status " + std::to_string(response.status),
                response.status, method, request_id, response.body, response.www_authenticate,
                response.retry_after, response.auth_reason);
        }
        if (response.status < 200 || response.status >= 300) {
            if (response.status == 413 && external_http) {
                return retain_session_lock(std::move(response));
            }
            if (content_type.rfind(kArrowContentType, 0) == 0) {
                return retain_session_lock(std::move(response));
            }
            const std::string detail = safe_error_detail(response.body, 512);
            throw HttpClientError(HttpClientErrorKind::HTTP_STATUS,
                                  "HTTP RPC request failed with status " +
                                      std::to_string(response.status) +
                                      (detail.empty() ? "" : ": " + detail),
                                  response.status, method, request_id, response.body,
                                  response.retry_after, response.auth_reason);
        }
        if (content_type.rfind(kArrowContentType, 0) != 0) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "HTTP RPC response has unsupported Content-Type: " +
                                      (content_type.empty() ? "<missing>" : content_type),
                                  response.status, method, request_id, response.body);
        }
        return retain_session_lock(std::move(response));
    }

    BoundedHttpResponse post(
        const std::string& method, const std::string& request_path, const std::string& body,
        const std::string& request_id, const CallOptions& options, bool retryable,
        const std::shared_ptr<HttpStickySessionState>& sticky_session = nullptr) {
        bool externalized = false;
        std::string request_body = body;
        auto cached_caps = [&] {
            std::lock_guard<std::timed_mutex> lock(mutex);
            return server_capabilities;
        };

        // A body larger than the local inline ceiling cannot be sent merely to
        // discover a server's 413 behavior. Discover capabilities first, then
        // either externalize within the separately bounded upload ceiling or
        // fail before transport.
        if (request_body.size() > static_cast<uint64_t>(config.max_request_bytes)) {
            (void)capabilities(random_hex(16), options, sticky_session);
        }
        auto caps = cached_caps();
        const int64_t inline_cap = caps.max_request_bytes
                                       ? std::min(config.max_request_bytes, *caps.max_request_bytes)
                                       : config.max_request_bytes;
        if (request_body.size() > static_cast<uint64_t>(inline_cap) && external_http &&
            caps.upload_url_support) {
            request_body = externalize_request(body, options, sticky_session);
            externalized = true;
        }

        auto response = post_inline(method, request_path, request_body, request_id, options,
                                    retryable, sticky_session);
        // A 413 is a pre-dispatch rejection, so replacing the body with a
        // method-bound pointer and retrying once is safe even for exchange.
        if (response.status == 413 && !externalized && external_http) {
            caps = cached_caps();
            if (caps.upload_url_support) {
                response.sticky_guard.reset();
                request_body = externalize_request(body, options, sticky_session);
                response = post_inline(method, request_path, request_body, request_id, options,
                                       retryable, sticky_session);
            }
        }
        if (response.status == 413 && response.content_type.rfind(kArrowContentType, 0) != 0) {
            const std::string detail = safe_error_detail(response.body, 512);
            throw HttpClientError(
                HttpClientErrorKind::HTTP_STATUS,
                "HTTP RPC request failed with status 413" + (detail.empty() ? "" : ": " + detail),
                413, method, request_id, response.body, response.retry_after, response.auth_reason);
        }
        return response;
    }

    std::vector<HttpUploadUrl> request_upload_urls(
        int64_t count, const CallOptions& options,
        const std::shared_ptr<HttpStickySessionState>& sticky_session = nullptr) {
        if (count < 1 || count > 100) {
            throw std::invalid_argument("upload URL count must be between 1 and 100");
        }
        if (!external_http) {
            throw HttpClientError(HttpClientErrorKind::LIMIT,
                                  "upload URL requests require external URL validation", 0, {}, {});
        }
        const std::string control_method = "__upload_url__";
        const std::string control_id = logical_request_id(options);
        const auto metadata =
            request_metadata(nullptr, control_method, config.protocol_version, control_id);
        const std::string control_body =
            encode_ipc(upload_url_request(count), metadata, config.max_request_bytes);
        const auto response = post_inline(control_method, path(control_method, "/init"),
                                          control_body, control_id, options, true, sticky_session);
        auto decoded = decode(response, ResponseShape::UNARY, false, nullptr, sticky_session);
        const auto expected_schema = upload_url_response_schema();
        if (!schema_equals(decoded.schema, expected_schema) || decoded.data.size() != 1 ||
            !decoded.data.front().batch || decoded.data.front().batch->num_rows() != count) {
            throw HttpClientError("upload URL response has an invalid Arrow shape");
        }
        const auto& batch = decoded.data.front().batch;
        auto uploads = std::dynamic_pointer_cast<arrow::StringArray>(batch->column(0));
        auto downloads = std::dynamic_pointer_cast<arrow::StringArray>(batch->column(1));
        auto expirations = std::dynamic_pointer_cast<arrow::TimestampArray>(batch->column(2));
        if (!uploads || !downloads || !expirations) {
            throw HttpClientError("upload URL response has invalid Arrow column types");
        }
        std::vector<HttpUploadUrl> urls;
        urls.reserve(static_cast<size_t>(count));
        for (int64_t row = 0; row < count; ++row) {
            if (uploads->IsNull(row) || downloads->IsNull(row)) {
                throw HttpClientError("upload URL response contains a null URL");
            }
            HttpUploadUrl url;
            url.upload_url = uploads->GetString(row);
            url.download_url = downloads->GetString(row);
            if (!expirations->IsNull(row)) url.expires_at_us = expirations->Value(row);
            try {
                external_http->validate_url(url.upload_url);
                external_http->validate_url(url.download_url);
            } catch (const ExternalHttpError& error) {
                throw HttpClientError(
                    HttpClientErrorKind::PROTOCOL,
                    std::string("upload URL response violates external URL policy: ") +
                        error.what(),
                    response.status, control_method, control_id);
            }
            urls.push_back(std::move(url));
        }
        return urls;
    }

    void delete_session_best_effort(
        const std::string& token, const std::map<std::string, std::string>& echo_headers) noexcept {
        try {
            const std::string request_id = random_hex(16);
            const std::string request_path = path("__session__");
            validate_sticky_echo_headers(echo_headers, config.allow_insecure_credentials,
                                         secure_transport);

            std::unique_lock<std::timed_mutex> lock(mutex, std::defer_lock);
            if (!lock.try_lock_for(std::chrono::milliseconds(50))) return;
            httplib::Headers headers;
            merge_headers(headers, config.headers);
            merge_headers(headers, echo_headers);
            headers.erase("VGI-Session");
            headers.emplace("VGI-Session", token);
            headers.emplace("X-Request-ID", request_id);

            httplib::Request request;
            request.method = "DELETE";
            request.path = request_path;
            request.headers = std::move(headers);
            request.content_receiver = [](const char*, size_t, uint64_t, uint64_t) { return true; };
            httplib::Response response;
            httplib::Error error = httplib::Error::Success;
            client->set_max_timeout(250);
            try {
                (void)client->send(request, response, error);
            } catch (...) {
                client->set_max_timeout(0);
                throw;
            }
            client->set_max_timeout(0);
        } catch (...) {
        }
    }

    HttpServerCapabilities capabilities(
        const std::string& request_id, const CallOptions& options,
        const std::shared_ptr<HttpStickySessionState>& sticky_session = nullptr) {
        check_call_active(options, "OPTIONS", request_id);
        std::map<std::string, std::string> auth_headers;
        const std::string health_path = path("health");
        if (auth_callback) {
            try {
                // Keep refresh and re-entrant credential acquisition out of
                // the serialized transport section, just like ordinary RPCs.
                auth_headers = auth_callback(HttpAuthRequest{"OPTIONS", health_path, request_id});
                validate_headers(auth_headers, config.allow_insecure_credentials, secure_transport);
            } catch (const std::exception& error) {
                throw HttpClientError(
                    HttpClientErrorKind::AUTHENTICATION,
                    std::string("HTTP credential callback failed: ") + error.what(), 0, "OPTIONS",
                    request_id);
            } catch (...) {
                throw HttpClientError(
                    HttpClientErrorKind::AUTHENTICATION,
                    "HTTP credential callback failed with a non-standard exception", 0, "OPTIONS",
                    request_id);
            }
        }
        validate_headers(options.headers, config.allow_insecure_credentials, secure_transport);
        std::unique_lock<std::timed_mutex> session_lock;
        if (sticky_session) {
            session_lock =
                std::unique_lock<std::timed_mutex>(sticky_session->mutex, std::defer_lock);
            while (!session_lock.try_lock_for(std::chrono::milliseconds(10))) {
                check_call_active(options, "OPTIONS", request_id);
            }
            if (sticky_session->locally_closed.load()) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "HTTP sticky-session view is closed", 0, "OPTIONS",
                                      request_id);
            }
        }
        std::unique_lock<std::timed_mutex> lock(mutex, std::defer_lock);
        while (!lock.try_lock_for(std::chrono::milliseconds(10))) {
            check_call_active(options, "OPTIONS", request_id);
        }
        if (capabilities_fetched) return server_capabilities;

        const uint32_t attempts = retry_policy.max_attempts;
        for (uint32_t attempt = 0; attempt < attempts; ++attempt) {
            if (attempt > 0) {
                wait_for_retry(retry_delay(retry_policy, attempt), options, "OPTIONS", request_id);
            }
            check_call_active(options, "OPTIONS", request_id);
            httplib::Headers headers;
            merge_headers(headers, config.headers);
            merge_headers(headers, auth_headers);
            merge_headers(headers, options.headers);
            if (sticky_session) {
                merge_headers(headers, sticky_session->echo_headers);
                headers.erase("VGI-Session-Accept");
                headers.emplace("VGI-Session-Accept", "true");
                headers.erase("VGI-Session");
                if (sticky_session->token) {
                    headers.emplace("VGI-Session", *sticky_session->token);
                }
            }
            headers.emplace("Accept-Encoding", "identity");
            headers.emplace("X-Request-ID", request_id);
            if (options.deadline) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    *options.deadline - std::chrono::steady_clock::now());
                client->set_max_timeout(std::max<int64_t>(1, remaining.count()));
            }
            std::stop_callback stop_callback(options.stop_token, [this] { client->stop(); });
            check_call_active(options, "OPTIONS", request_id);
            httplib::Request request;
            request.method = "OPTIONS";
            request.path = health_path;
            request.headers = std::move(headers);
            bool response_too_large = false;
            request.content_receiver = [&](const char* data, size_t size, uint64_t, uint64_t) {
                if (size > static_cast<uint64_t>(max_encoded_response_bytes) ||
                    request.body.size() >
                        static_cast<uint64_t>(max_encoded_response_bytes) - size) {
                    response_too_large = true;
                    return false;
                }
                request.body.append(data, size);
                return true;
            };
            httplib::Response response;
            httplib::Error transport_error = httplib::Error::Success;
            const bool sent = client->send(request, response, transport_error);
            client->set_max_timeout(0);
            if (response_too_large) {
                throw HttpClientError(
                    HttpClientErrorKind::LIMIT,
                    "HTTP capability response exceeds max_encoded_response_bytes (" +
                        std::to_string(max_encoded_response_bytes) + ")",
                    0, "OPTIONS", request_id);
            }
            if (!sent) {
                if (attempt + 1 < attempts && !options.stop_token.stop_requested() &&
                    !deadline_expired(options)) {
                    continue;
                }
                const HttpClientErrorKind kind =
                    classify_transport_error(transport_error, options, secure_transport);
                throw HttpClientError(kind,
                                      std::string("HTTP capability discovery failed: ") +
                                          httplib::to_string(transport_error),
                                      0, "OPTIONS", request_id);
            }
            // cpp-httplib routes a streaming receiver into Request::body;
            // move it onto the response-shaped object used below.
            response.body = std::move(request.body);
            std::optional<BoundedHttpResponse::StickyUpdate> sticky_update;
            if (sticky_session) {
                sticky_update =
                    stage_sticky_response(response, sticky_session, "OPTIONS", request_id);
            }
            if (response.status == 401 || response.status == 403) {
                throw HttpAuthenticationError("HTTP capability authentication failed",
                                              response.status, "OPTIONS", request_id, response.body,
                                              response.get_header_value("WWW-Authenticate"),
                                              response.get_header_value("Retry-After"),
                                              response.get_header_value("VGI-Auth-Reason"));
            }
            if (response.status < 200 || response.status >= 300) {
                if (attempt + 1 < attempts && contains_status(retry_policy, response.status)) {
                    continue;
                }
                throw HttpClientError(
                    HttpClientErrorKind::HTTP_STATUS,
                    "HTTP capability discovery returned status " + std::to_string(response.status),
                    response.status, "OPTIONS", request_id, response.body,
                    response.get_header_value("Retry-After"),
                    response.get_header_value("VGI-Auth-Reason"));
            }
            if (sticky_update) {
                if (sticky_session->locally_closed.load()) {
                    throw HttpClientError(
                        HttpClientErrorKind::PROTOCOL,
                        "HTTP sticky-session view was closed before response commit",
                        response.status, "OPTIONS", request_id);
                }
                sticky_session->token = std::move(sticky_update->token);
                sticky_session->echo_headers = std::move(sticky_update->echo_headers);
            }
            server_capabilities = HttpServerCapabilities{};
            harvest_capabilities(response, server_capabilities);
            if (server_capabilities.max_request_bytes) {
                server_max_request_bytes = server_capabilities.max_request_bytes;
            }
            if (!includes_encoding_list(server_capabilities.supported_encodings, kZstdEncoding)) {
                send_compressed = false;
            }
            capabilities_fetched = true;
            return server_capabilities;
        }
        throw HttpClientError(HttpClientErrorKind::TRANSPORT,
                              "HTTP capability discovery exhausted retries", 0, "OPTIONS",
                              request_id);
    }

    int64_t request_cap() {
        std::lock_guard<std::timed_mutex> lock(mutex);
        if (server_max_request_bytes) {
            return std::min(config.max_request_bytes, *server_max_request_bytes);
        }
        return config.max_request_bytes;
    }

    int64_t request_serialization_cap() {
        std::lock_guard<std::timed_mutex> lock(mutex);
        int64_t limit = config.max_request_bytes;
        if (external_http) limit = std::max(limit, external_max_upload_bytes);
        if (server_capabilities.max_upload_bytes) {
            limit = std::min(limit, *server_capabilities.max_upload_bytes);
        }
        return limit;
    }

    std::string path(const std::string& method, const char* suffix = "") const {
        return (config.prefix == "/" ? std::string() : config.prefix) + "/" + method + suffix;
    }

    HttpClientConfig config;

    ClientExternalHttp* external() const noexcept { return external_http.get(); }

    DecodedResponse decode(const BoundedHttpResponse& response, ResponseShape shape,
                           bool has_header, ClientExternalHttp* external_resolver,
                           const std::shared_ptr<HttpStickySessionState>& sticky_session) {
        if (sticky_session && (!response.sticky_guard || !response.sticky_guard->owns_lock())) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "sticky response lost its serialization guard", response.status,
                                  {}, {});
        }
        if (sticky_session && sticky_session->locally_closed.load()) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "HTTP sticky-session view was closed before response commit",
                                  response.status, {}, {});
        }
        try {
            auto decoded = decode_response(response, config, shape, has_header, external_resolver);
            commit_sticky_response(response, sticky_session);
            return decoded;
        } catch (const RpcRemoteError&) {
            // A valid Arrow exception envelope is a protocol-valid response;
            // it may intentionally rotate or close the sticky session.
            commit_sticky_response(response, sticky_session);
            throw;
        }
    }

    void validate_sticky_seed(const std::map<std::string, std::string>& echo_headers) const {
        validate_sticky_echo_headers(echo_headers, config.allow_insecure_credentials,
                                     secure_transport);
    }

private:
    BoundedHttpResponse::StickyUpdate stage_sticky_response(
        const httplib::Response& response,
        const std::shared_ptr<HttpStickySessionState>& sticky_session, const std::string& method,
        const std::string& request_id) {
        auto next_token = sticky_session->token;
        auto next_echo_headers = sticky_session->echo_headers;
        const std::string token = response.get_header_value("VGI-Session");
        if (!token.empty()) {
            if (token.size() > 8192 || token.find_first_of("\r\n") != std::string::npos) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "server returned an invalid sticky-session token",
                                      response.status, method, request_id);
            }
            next_token = token;
        }
        constexpr std::string_view echo_prefix = "vgi-echo-";
        for (const auto& [name, value] : response.headers) {
            if (ascii_lower(name).rfind(echo_prefix, 0) != 0) continue;
            std::string replay_name = name.substr(echo_prefix.size());
            if (replay_name.size() > 1024 || value.size() > 8192) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "server returned an oversized sticky echo header",
                                      response.status, method, request_id);
            }
            std::map<std::string, std::string> candidate{{replay_name, value}};
            try {
                validate_sticky_echo_headers(candidate, config.allow_insecure_credentials,
                                             secure_transport);
            } catch (const std::exception& error) {
                throw HttpClientError(
                    HttpClientErrorKind::PROTOCOL,
                    std::string("server returned an invalid sticky echo header: ") + error.what(),
                    response.status, method, request_id);
            }
            for (auto it = next_echo_headers.begin(); it != next_echo_headers.end();) {
                if (ascii_lower(it->first) == ascii_lower(replay_name)) {
                    it = next_echo_headers.erase(it);
                } else {
                    ++it;
                }
            }
            next_echo_headers.insert_or_assign(std::move(replay_name), value);
        }
        size_t echo_bytes = 0;
        for (const auto& [name, value] : next_echo_headers) {
            if (name.size() > 1024 || value.size() > 8192 || echo_bytes > 64 * 1024 ||
                name.size() > 64 * 1024 - echo_bytes) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "server returned too much sticky echo-header state",
                                      response.status, method, request_id);
            }
            echo_bytes += name.size();
            if (value.size() > 64 * 1024 - echo_bytes) {
                throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                      "server returned too much sticky echo-header state",
                                      response.status, method, request_id);
            }
            echo_bytes += value.size();
        }
        if (next_echo_headers.size() > 100) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "server returned too many sticky echo headers", response.status,
                                  method, request_id);
        }
        if (ascii_lower(trim_ascii(response.get_header_value("VGI-Session-Close"))) == "true") {
            next_token.reset();
            next_echo_headers.clear();
        }
        return {std::move(next_token), std::move(next_echo_headers)};
    }

    static void commit_sticky_response(
        const BoundedHttpResponse& response,
        const std::shared_ptr<HttpStickySessionState>& sticky_session) {
        if (!sticky_session || !response.sticky_update) return;
        if (sticky_session->locally_closed.load()) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "HTTP sticky-session view was closed before response commit",
                                  response.status, {}, {});
        }
        sticky_session->token = response.sticky_update->token;
        sticky_session->echo_headers = response.sticky_update->echo_headers;
    }

    std::string externalize_request(const std::string& body, const CallOptions& options,
                                    const std::shared_ptr<HttpStickySessionState>& sticky_session) {
        if (!external_http) {
            throw HttpClientError(HttpClientErrorKind::LIMIT, "request externalization is disabled",
                                  0, {}, {});
        }
        HttpServerCapabilities caps;
        {
            std::lock_guard<std::timed_mutex> lock(mutex);
            caps = server_capabilities;
        }
        if (!caps.upload_url_support) {
            throw HttpClientError(HttpClientErrorKind::LIMIT,
                                  "server does not advertise upload URL support", 0, {}, {});
        }
        int64_t upload_cap = external_max_upload_bytes;
        if (caps.max_upload_bytes) upload_cap = std::min(upload_cap, *caps.max_upload_bytes);
        if (body.size() > static_cast<uint64_t>(upload_cap)) {
            throw HttpClientError(HttpClientErrorKind::LIMIT,
                                  "externalized request exceeds max_upload_bytes (" +
                                      std::to_string(body.size()) + " > " +
                                      std::to_string(upload_cap) + ")",
                                  0, {}, {});
        }

        const auto urls = request_upload_urls(1, options, sticky_session);
        const std::string& upload_url = urls.front().upload_url;
        const std::string& download_url = urls.front().download_url;
        try {
            external_http->put(upload_url, body, kArrowContentType);
        } catch (const ExternalHttpError& error) {
            throw HttpClientError(HttpClientErrorKind::TRANSPORT,
                                  std::string("request externalization failed: ") + error.what(), 0,
                                  {}, {});
        }
        return external_pointer_body(body, download_url, config.max_request_bytes);
    }

    std::unique_ptr<httplib::Client> client;
    std::timed_mutex mutex;
    RetryPolicy retry_policy;
    TlsOptions tls_options;
    HttpAuthCallback auth_callback;
    std::unique_ptr<ClientExternalHttp> external_http;
    int64_t external_max_upload_bytes = 0;
    bool secure_transport = false;
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
    : HttpClientError(HttpClientErrorKind::REMOTE, exception_type + ": " + message, http_status, {},
                      request_id),
      exception_type_(std::move(exception_type)),
      error_kind_(std::move(error_kind)),
      server_id_(std::move(server_id)),
      request_id_(std::move(request_id)) {}

HttpSessionLostError::HttpSessionLostError(std::string message, std::string error_kind,
                                           std::string server_id, std::string request_id,
                                           int http_status)
    : RpcRemoteError("SessionLostError", std::move(message), std::move(error_kind),
                     std::move(server_id), std::move(request_id), http_status) {}

class HttpClientBuilder::Impl {
public:
    explicit Impl(std::string base_url) : base_url(parse_base_url(std::move(base_url))) {}

    ParsedBaseUrl base_url;
    HttpClientConfig config;
    RetryPolicy retry_policy;
    TlsOptions tls_options;
    HttpAuthCallback auth_callback;
    std::optional<ClientExternalHttpOptions> external_options{ClientExternalHttpOptions{}};
};

HttpClientBuilder::HttpClientBuilder(std::string base_url)
    : impl_(std::make_unique<Impl>(std::move(base_url))) {}

HttpClientBuilder::~HttpClientBuilder() = default;

HttpClientBuilder::HttpClientBuilder(const HttpClientBuilder& other)
    : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr) {}

HttpClientBuilder& HttpClientBuilder::operator=(const HttpClientBuilder& other) {
    if (this != &other) {
        impl_ = other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr;
    }
    return *this;
}

HttpClientBuilder::HttpClientBuilder(HttpClientBuilder&&) noexcept = default;

HttpClientBuilder& HttpClientBuilder::operator=(HttpClientBuilder&&) noexcept = default;

HttpClientBuilder& HttpClientBuilder::config(HttpClientConfig config) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config = std::move(config);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::prefix(std::string prefix) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config.prefix = std::move(prefix);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::protocol_version(std::string version) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config.protocol_version = std::move(version);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::header(std::string name, std::string value) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config.headers.insert_or_assign(std::move(name), std::move(value));
    return *this;
}

HttpClientBuilder& HttpClientBuilder::compression_level(std::optional<int> level) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config.compression_level = level;
    return *this;
}

HttpClientBuilder& HttpClientBuilder::response_limits(int64_t max_encoded_bytes,
                                                      int64_t max_decoded_bytes) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->config.max_encoded_response_bytes = max_encoded_bytes;
    impl_->config.max_decoded_response_bytes = max_decoded_bytes;
    return *this;
}

HttpClientBuilder& HttpClientBuilder::retry_policy(RetryPolicy policy) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->retry_policy = std::move(policy);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::auth_callback(HttpAuthCallback callback) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->auth_callback = std::move(callback);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::tls_options(TlsOptions options) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->tls_options = std::move(options);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::custom_ca_file(std::string path) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->tls_options.ca_file = std::move(path);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::client_certificate(std::string certificate_file,
                                                         std::string private_key_file) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->tls_options.client_certificate_file = std::move(certificate_file);
    impl_->tls_options.client_private_key_file = std::move(private_key_file);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::dangerous_disable_tls_verification_for_testing(
    bool disabled) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->tls_options.insecure_skip_verification_for_testing = disabled;
    return *this;
}

HttpClientBuilder& HttpClientBuilder::external_http_options(ClientExternalHttpOptions options) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->external_options = std::move(options);
    return *this;
}

HttpClientBuilder& HttpClientBuilder::disable_external_locations(bool disabled) {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    impl_->external_options =
        disabled ? std::nullopt
                 : std::optional<ClientExternalHttpOptions>{ClientExternalHttpOptions{}};
    return *this;
}

HttpClient HttpClientBuilder::build() const {
    if (!impl_) throw std::logic_error("HttpClientBuilder is moved from");
    return HttpClient(std::make_shared<HttpClientState>(
        impl_->base_url, impl_->config, impl_->retry_policy, impl_->tls_options,
        impl_->auth_callback, impl_->external_options));
}

HttpClientBuilder HttpClient::builder(std::string base_url) {
    return HttpClientBuilder(std::move(base_url));
}

HttpClient::HttpClient(std::string base_url, HttpClientConfig config)
    : state_(std::make_shared<HttpClientState>(parse_base_url(std::move(base_url)),
                                               std::move(config), RetryPolicy{}, TlsOptions{},
                                               HttpAuthCallback{}, ClientExternalHttpOptions{})) {}

HttpClient::HttpClient(std::shared_ptr<HttpClientState> state,
                       std::shared_ptr<HttpStickySessionState> sticky_session)
    : state_(std::move(state)), sticky_session_(std::move(sticky_session)) {}

HttpClient::~HttpClient() = default;

AnnotatedBatch HttpClient::call(const std::string& method, const AnnotatedBatch& request,
                                std::shared_ptr<arrow::Schema> expected_output_schema,
                                const CallOptions& options) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    const std::string request_id = logical_request_id(options);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response =
        state_->post(method, state_->path(method),
                     encode_ipc(request, metadata, state_->request_serialization_cap()), request_id,
                     options, true, sticky_session_);
    auto decoded =
        state_->decode(response, ResponseShape::UNARY, false, state_->external(), sticky_session_);
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

HttpServerCapabilities HttpClient::capabilities(const CallOptions& options) const {
    if (!state_) throw HttpClientError("HttpClient is moved from");
    return state_->capabilities(logical_request_id(options), options, sticky_session_);
}

ServiceDescription HttpClient::describe(const CallOptions& options) const {
    const AnnotatedBatch request = AnnotatedBatch::data(make_empty_batch(empty_schema()));
    return parse_service_description(call("__describe__", request, nullptr, options));
}

std::vector<HttpUploadUrl> HttpClient::request_upload_urls(int64_t count,
                                                           const CallOptions& options) const {
    if (!state_) throw HttpClientError("HttpClient is moved from");
    return state_->request_upload_urls(count, options, sticky_session_);
}

HttpSessionView HttpClient::with_session_token(
    std::optional<std::string> token, std::map<std::string, std::string> echo_headers) const {
    if (!state_) throw HttpClientError("HttpClient is moved from");
    return HttpSessionView(state_, std::move(token), std::move(echo_headers));
}

class HttpSessionView::Impl {
public:
    Impl(std::shared_ptr<HttpClientState> state,
         std::shared_ptr<HttpStickySessionState> sticky_session)
        : state(std::move(state)),
          sticky_session(std::move(sticky_session)),
          client(this->state, this->sticky_session) {}

    std::shared_ptr<HttpClientState> state;
    std::shared_ptr<HttpStickySessionState> sticky_session;
    HttpClient client;
};

HttpSessionView::HttpSessionView(std::shared_ptr<HttpClientState> state,
                                 std::optional<std::string> token,
                                 std::map<std::string, std::string> echo_headers) {
    if (token && (token->empty() || token->size() > 8192 ||
                  token->find_first_of("\r\n") != std::string::npos)) {
        throw std::invalid_argument(
            "initial sticky-session token must be 1-8192 characters without CR/LF");
    }
    state->validate_sticky_seed(echo_headers);
    auto sticky_session = std::make_shared<HttpStickySessionState>();
    sticky_session->token = std::move(token);
    sticky_session->echo_headers = std::move(echo_headers);
    impl_ = std::make_unique<Impl>(std::move(state), std::move(sticky_session));
}

HttpSessionView::~HttpSessionView() {
    close();
}

HttpSessionView::HttpSessionView(HttpSessionView&&) noexcept = default;

HttpSessionView& HttpSessionView::operator=(HttpSessionView&& other) noexcept {
    if (this != &other) {
        close();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

AnnotatedBatch HttpSessionView::call(const std::string& method, const AnnotatedBatch& request,
                                     std::shared_ptr<arrow::Schema> expected_output_schema,
                                     const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.call(method, request, std::move(expected_output_schema), options);
}

HttpServerCapabilities HttpSessionView::capabilities(const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.capabilities(options);
}

ServiceDescription HttpSessionView::describe(const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.describe(options);
}

std::vector<HttpUploadUrl> HttpSessionView::request_upload_urls(int64_t count,
                                                                const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.request_upload_urls(count, options);
}

HttpExchangeSession HttpSessionView::open_exchange(const std::string& method,
                                                   const AnnotatedBatch& request,
                                                   std::shared_ptr<arrow::Schema> input_schema,
                                                   std::shared_ptr<arrow::Schema> output_schema,
                                                   const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.open_exchange(method, request, std::move(input_schema),
                                       std::move(output_schema), options);
}

HttpStreamSession HttpSessionView::open_producer(const std::string& method,
                                                 const AnnotatedBatch& request,
                                                 std::shared_ptr<arrow::Schema> output_schema,
                                                 bool has_header,
                                                 const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.open_producer(method, request, std::move(output_schema), has_header,
                                       options);
}

HttpStreamSession HttpSessionView::open_stream_exchange(
    const std::string& method, const AnnotatedBatch& request,
    std::shared_ptr<arrow::Schema> input_schema, std::shared_ptr<arrow::Schema> output_schema,
    bool has_header, const CallOptions& options) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.open_stream_exchange(method, request, std::move(input_schema),
                                              std::move(output_schema), has_header, options);
}

HttpStreamSession HttpSessionView::resume_stream(
    const std::string& method, const std::vector<uint8_t>& resume_token,
    std::shared_ptr<arrow::Schema> output_schema) const {
    if (!impl_) throw HttpClientError("HTTP sticky-session view is moved from");
    return impl_->client.resume_stream(method, resume_token, std::move(output_schema));
}

std::optional<std::string> HttpSessionView::current_session_token() const {
    if (!impl_) return std::nullopt;
    std::lock_guard<std::timed_mutex> lock(impl_->sticky_session->mutex);
    return impl_->sticky_session->token;
}

std::map<std::string, std::string> HttpSessionView::current_echo_headers() const {
    if (!impl_) return {};
    std::lock_guard<std::timed_mutex> lock(impl_->sticky_session->mutex);
    return impl_->sticky_session->echo_headers;
}

std::optional<std::string> HttpSessionView::detach() {
    if (!impl_) return std::nullopt;
    std::lock_guard<std::timed_mutex> lock(impl_->sticky_session->mutex);
    auto token = std::move(impl_->sticky_session->token);
    impl_->sticky_session->token.reset();
    impl_->sticky_session->echo_headers.clear();
    impl_->sticky_session->locally_closed.store(true);
    return token;
}

void HttpSessionView::close() noexcept {
    if (!impl_) return;
    try {
        if (impl_->sticky_session->locally_closed.exchange(true)) return;
        std::unique_lock<std::timed_mutex> lock(impl_->sticky_session->mutex, std::defer_lock);
        // Destruction is local and bounded even if another request is stuck.
        // The server TTL is the safety net when we cannot safely snapshot a
        // concurrently rotating token for DELETE.
        if (!lock.try_lock_for(std::chrono::milliseconds(50))) return;
        auto token = std::move(impl_->sticky_session->token);
        auto echo_headers = std::move(impl_->sticky_session->echo_headers);
        impl_->sticky_session->token.reset();
        impl_->sticky_session->echo_headers.clear();
        lock.unlock();
        if (token) impl_->state->delete_session_best_effort(*token, echo_headers);
    } catch (...) {
    }
}

bool HttpSessionView::active() const noexcept {
    return impl_ && !impl_->sticky_session->locally_closed.load();
}

class HttpExchangeSession::Impl {
public:
    std::shared_ptr<HttpClientState> state;
    std::shared_ptr<HttpStickySessionState> sticky_session;
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
                                              std::shared_ptr<arrow::Schema> output_schema,
                                              const CallOptions& options) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    if (!input_schema || !output_schema) {
        throw std::invalid_argument("exchange input and output schemas must not be null");
    }
    const std::string request_id = logical_request_id(options);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response =
        state_->post(method, state_->path(method, "/init"),
                     encode_ipc(request, metadata, state_->request_serialization_cap()), request_id,
                     options, true, sticky_session_);
    auto decoded = state_->decode(response, ResponseShape::EXCHANGE_INIT, false, state_->external(),
                                  sticky_session_);
    if (!schema_equals(decoded.schema, output_schema)) {
        throw HttpClientError(
            schema_mismatch("exchange init response", output_schema, decoded.schema));
    }

    auto impl = std::make_unique<HttpExchangeSession::Impl>();
    impl->state = state_;
    impl->sticky_session = sticky_session_;
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

AnnotatedBatch HttpExchangeSession::exchange(const AnnotatedBatch& input,
                                             const CallOptions& options) {
    if (!impl_ || !impl_->is_active) throw HttpClientError("exchange session is closed");
    if (!input.batch) throw std::invalid_argument("exchange input batch must not be null");
    if (!schema_equals(input.batch->schema(), impl_->input_schema)) {
        throw std::invalid_argument(
            schema_mismatch("exchange input", impl_->input_schema, input.batch->schema()));
    }

    auto metadata = sanitized_metadata(input.custom_metadata);
    const std::string request_id = logical_request_id(options);
    replace_metadata(metadata, keys::REQUEST_ID, request_id);
    replace_metadata(metadata, keys::STATE_B64, impl_->cursor);
    if (!impl_->call_state.empty()) {
        replace_metadata(metadata, keys::CALL_STATE_B64, impl_->call_state);
    }
    const auto body = encode_ipc(input, metadata, impl_->state->request_serialization_cap());
    // Once the request leaves this process, failure is ambiguous: the worker
    // may have advanced the state even if its reply was lost.  Retire the old
    // cursor before POST and reactivate only after a complete response yields
    // a new one, preventing accidental duplicate execution.
    impl_->is_active = false;
    const auto response =
        impl_->state->post(impl_->method, impl_->state->path(impl_->method, "/exchange"), body,
                           request_id, options, false, impl_->sticky_session);
    auto decoded = impl_->state->decode(response, ResponseShape::EXCHANGE_TURN, false,
                                        impl_->state->external(), impl_->sticky_session);
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
        (void)impl_->state->post(
            impl_->method, impl_->state->path(impl_->method, "/exchange"),
            encode_ipc(cancel, metadata, impl_->state->request_serialization_cap()), request_id,
            CallOptions{}, false, impl_->sticky_session);
    } catch (const std::exception&) {
        // Cancellation is best-effort.  The opaque token's own expiry remains
        // the fallback when the peer is unavailable.
    }
}

bool HttpExchangeSession::active() const noexcept {
    return impl_ && impl_->is_active;
}

class HttpStreamSession::Impl {
public:
    std::shared_ptr<HttpClientState> state;
    std::shared_ptr<HttpStickySessionState> sticky_session;
    std::string method;
    HttpStreamKind stream_kind = HttpStreamKind::PRODUCER;
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::Schema> output_schema;
    std::optional<AnnotatedBatch> stream_header;
    std::deque<AnnotatedBatch> pending;
    std::string cursor;
    std::string call_state;
    bool is_finished = false;
    bool is_closed = false;

    void update_controls(const DecodedResponse& decoded) {
        cursor.clear();
        for (const auto& batch : decoded.control) {
            const std::string next = get_metadata_value(batch.custom_metadata, keys::STATE_B64);
            if (!next.empty()) cursor = next;
            const std::string call =
                get_metadata_value(batch.custom_metadata, keys::CALL_STATE_B64);
            if (!call.empty()) call_state = call;
        }
        if (cursor.empty()) is_finished = true;
    }

    std::shared_ptr<arrow::KeyValueMetadata> continuation_metadata(const std::string& request_id,
                                                                   bool cancel = false) const {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        replace_metadata(metadata, keys::REQUEST_ID, request_id);
        replace_metadata(metadata, keys::STATE_B64, cursor);
        if (!call_state.empty()) replace_metadata(metadata, keys::CALL_STATE_B64, call_state);
        if (cancel) replace_metadata(metadata, keys::CANCEL, "1");
        return metadata;
    }
};

namespace {

std::unique_ptr<HttpStreamSession::Impl> make_http_stream_impl(
    const std::shared_ptr<HttpClientState>& state, std::string method, HttpStreamKind kind,
    std::shared_ptr<arrow::Schema> input_schema, std::shared_ptr<arrow::Schema> output_schema,
    DecodedResponse decoded,
    const std::shared_ptr<HttpStickySessionState>& sticky_session = nullptr) {
    if (!schema_equals(decoded.schema, output_schema)) {
        throw HttpClientError(
            schema_mismatch("stream init response", output_schema, decoded.schema));
    }
    if (kind == HttpStreamKind::PRODUCER && decoded.data.size() > 1) {
        throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                              "producer init response contains multiple data batches", 0, method,
                              {});
    }
    auto impl = std::make_unique<HttpStreamSession::Impl>();
    impl->state = state;
    impl->sticky_session = sticky_session;
    impl->method = std::move(method);
    impl->stream_kind = kind;
    impl->input_schema = std::move(input_schema);
    impl->output_schema = std::move(output_schema);
    impl->stream_header = std::move(decoded.header);
    if (impl->stream_header) {
        impl->stream_header->custom_metadata =
            strip_transport_metadata(impl->stream_header->custom_metadata);
    }
    for (auto& batch : decoded.data) {
        batch.custom_metadata = strip_transport_metadata(batch.custom_metadata);
        impl->pending.push_back(std::move(batch));
    }
    impl->update_controls(decoded);
    return impl;
}

}  // namespace

HttpStreamSession HttpClient::open_producer(const std::string& method,
                                            const AnnotatedBatch& request,
                                            std::shared_ptr<arrow::Schema> output_schema,
                                            bool has_header, const CallOptions& options) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    if (!output_schema) throw std::invalid_argument("producer output schema must not be null");
    const std::string request_id = logical_request_id(options);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response =
        state_->post(method, state_->path(method, "/init"),
                     encode_ipc(request, metadata, state_->request_serialization_cap()), request_id,
                     options, true, sticky_session_);
    auto decoded = state_->decode(response, ResponseShape::STREAM_INIT, has_header,
                                  state_->external(), sticky_session_);
    return HttpStreamSession(make_http_stream_impl(state_, method, HttpStreamKind::PRODUCER,
                                                   empty_schema(), std::move(output_schema),
                                                   std::move(decoded), sticky_session_));
}

HttpStreamSession HttpClient::open_stream_exchange(const std::string& method,
                                                   const AnnotatedBatch& request,
                                                   std::shared_ptr<arrow::Schema> input_schema,
                                                   std::shared_ptr<arrow::Schema> output_schema,
                                                   bool has_header,
                                                   const CallOptions& options) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    if (!input_schema || !output_schema) {
        throw std::invalid_argument("exchange input and output schemas must not be null");
    }
    const std::string request_id = logical_request_id(options);
    const auto metadata = request_metadata(request.custom_metadata, method,
                                           state_->config.protocol_version, request_id);
    const auto response =
        state_->post(method, state_->path(method, "/init"),
                     encode_ipc(request, metadata, state_->request_serialization_cap()), request_id,
                     options, true, sticky_session_);
    auto decoded = state_->decode(response, ResponseShape::STREAM_INIT, has_header,
                                  state_->external(), sticky_session_);
    if (!decoded.data.empty()) {
        throw HttpClientError("exchange init unexpectedly returned application data");
    }
    auto impl =
        make_http_stream_impl(state_, method, HttpStreamKind::EXCHANGE, std::move(input_schema),
                              std::move(output_schema), std::move(decoded), sticky_session_);
    if (impl->cursor.empty()) {
        throw HttpClientError("exchange init response did not contain a continuation token");
    }
    impl->is_finished = false;
    return HttpStreamSession(std::move(impl));
}

HttpStreamSession HttpClient::resume_stream(const std::string& method,
                                            const std::vector<uint8_t>& resume_token,
                                            std::shared_ptr<arrow::Schema> output_schema) const {
    validate_method(method);
    if (!state_) throw HttpClientError("HttpClient is moved from");
    if (!output_schema) throw std::invalid_argument("producer output schema must not be null");
    auto [cursor, call_state] = unpack_resume_token(resume_token);
    if (cursor.empty()) throw std::invalid_argument("resume token contains an empty cursor");
    auto impl = std::make_unique<HttpStreamSession::Impl>();
    impl->state = state_;
    impl->sticky_session = sticky_session_;
    impl->method = method;
    impl->stream_kind = HttpStreamKind::PRODUCER;
    impl->input_schema = empty_schema();
    impl->output_schema = std::move(output_schema);
    impl->cursor = std::move(cursor);
    impl->call_state = std::move(call_state);
    return HttpStreamSession(std::move(impl));
}

HttpStreamSession::HttpStreamSession(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
HttpStreamSession::~HttpStreamSession() {
    close();
}
HttpStreamSession::HttpStreamSession(HttpStreamSession&&) noexcept = default;
HttpStreamSession& HttpStreamSession::operator=(HttpStreamSession&& other) noexcept {
    if (this != &other) {
        close();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

HttpStreamKind HttpStreamSession::kind() const noexcept {
    return impl_ ? impl_->stream_kind : HttpStreamKind::PRODUCER;
}

const std::optional<AnnotatedBatch>& HttpStreamSession::header() const noexcept {
    static const std::optional<AnnotatedBatch> empty;
    return impl_ ? impl_->stream_header : empty;
}

bool HttpStreamSession::finished() const noexcept {
    return !impl_ || impl_->is_finished || impl_->is_closed;
}

std::optional<AnnotatedBatch> HttpStreamSession::tick(const CallOptions& options) {
    if (!impl_ || impl_->is_closed) throw HttpClientError("stream session is closed");
    if (impl_->stream_kind != HttpStreamKind::PRODUCER) {
        throw std::logic_error("tick is only valid for producer streams");
    }
    while (true) {
        if (!impl_->pending.empty()) {
            auto value = std::move(impl_->pending.front());
            impl_->pending.pop_front();
            return value;
        }
        if (impl_->is_finished || impl_->cursor.empty()) {
            impl_->is_finished = true;
            return std::nullopt;
        }
        const std::string request_id = logical_request_id(options);
        auto metadata = impl_->continuation_metadata(request_id);
        const AnnotatedBatch request = AnnotatedBatch::data(make_empty_batch(empty_schema()));
        // Producing the next batch may have application side effects even
        // though the continuation cursor itself is immutable. Replay only
        // when the caller has explicitly declared this turn idempotent.
        const auto response = impl_->state->post(
            impl_->method, impl_->state->path(impl_->method, "/exchange"),
            encode_ipc(request, metadata, impl_->state->request_serialization_cap()), request_id,
            options, true, impl_->sticky_session);
        auto decoded = impl_->state->decode(response, ResponseShape::PRODUCER_TURN, false,
                                            impl_->state->external(), impl_->sticky_session);
        if (!schema_equals(decoded.schema, impl_->output_schema)) {
            throw HttpClientError(
                schema_mismatch("producer response", impl_->output_schema, decoded.schema));
        }
        if (decoded.data.size() > 1) {
            throw HttpClientError(HttpClientErrorKind::PROTOCOL,
                                  "producer response contains multiple data batches", 0,
                                  impl_->method, request_id);
        }
        for (auto& batch : decoded.data) {
            batch.custom_metadata = strip_transport_metadata(batch.custom_metadata);
            impl_->pending.push_back(std::move(batch));
        }
        impl_->update_controls(decoded);
    }
}

std::optional<HttpStreamBatch> HttpStreamSession::next_with_token(const CallOptions& options) {
    if (impl_ && impl_->pending.size() > 1) {
        throw HttpClientError(
            "next_with_token requires at most one application batch per response");
    }
    auto value = tick(options);
    if (!value) return std::nullopt;
    if (impl_->pending.size() > 0) {
        throw HttpClientError(
            "next_with_token requires at most one application batch per response");
    }
    HttpStreamBatch result;
    result.value = std::move(*value);
    if (!impl_->cursor.empty()) {
        result.resume_token = pack_resume_token(impl_->cursor, impl_->call_state);
    }
    return result;
}

std::optional<AnnotatedBatch> HttpStreamSession::exchange(const AnnotatedBatch& input,
                                                          const CallOptions& options) {
    if (!impl_ || impl_->is_closed || impl_->is_finished) {
        throw HttpClientError("exchange stream session is closed");
    }
    if (impl_->stream_kind != HttpStreamKind::EXCHANGE) {
        throw std::logic_error("exchange is only valid for exchange streams");
    }
    if (!input.batch) throw std::invalid_argument("exchange input batch must not be null");
    if (!schema_equals(input.batch->schema(), impl_->input_schema)) {
        throw std::invalid_argument(
            schema_mismatch("exchange input", impl_->input_schema, input.batch->schema()));
    }
    const std::string request_id = logical_request_id(options);
    auto metadata = sanitized_metadata(input.custom_metadata);
    replace_metadata(metadata, keys::REQUEST_ID, request_id);
    replace_metadata(metadata, keys::STATE_B64, impl_->cursor);
    if (!impl_->call_state.empty()) {
        replace_metadata(metadata, keys::CALL_STATE_B64, impl_->call_state);
    }
    impl_->is_closed = true;
    const auto response =
        impl_->state->post(impl_->method, impl_->state->path(impl_->method, "/exchange"),
                           encode_ipc(input, metadata, impl_->state->request_serialization_cap()),
                           request_id, options, false, impl_->sticky_session);
    auto decoded = impl_->state->decode(response, ResponseShape::EXCHANGE_TURN, false,
                                        impl_->state->external(), impl_->sticky_session);
    if (!schema_equals(decoded.schema, impl_->output_schema)) {
        throw HttpClientError(
            schema_mismatch("exchange response", impl_->output_schema, decoded.schema));
    }
    if (decoded.data.empty()) {
        impl_->is_finished = true;
        return std::nullopt;
    }
    if (decoded.data.size() != 1) {
        throw HttpClientError("exchange response must contain at most one data batch");
    }
    auto output = std::move(decoded.data.front());
    impl_->cursor = get_metadata_value(output.custom_metadata, keys::STATE_B64);
    impl_->is_finished = impl_->cursor.empty();
    impl_->is_closed = false;
    output.custom_metadata = strip_transport_metadata(output.custom_metadata);
    return output;
}

void HttpStreamSession::seek_to_token(const std::vector<uint8_t>& resume_token) {
    if (!impl_ || impl_->is_closed) throw HttpClientError("stream session is closed");
    if (impl_->stream_kind != HttpStreamKind::PRODUCER) {
        throw std::logic_error("seek_to_token is only valid for producer streams");
    }
    auto [cursor, call_state] = unpack_resume_token(resume_token);
    if (cursor.empty()) throw std::invalid_argument("resume token contains an empty cursor");
    impl_->pending.clear();
    impl_->cursor = std::move(cursor);
    impl_->call_state = std::move(call_state);
    impl_->is_finished = false;
}

void HttpStreamSession::close() noexcept {
    if (!impl_) return;
    impl_->is_closed = true;
    impl_->pending.clear();
}

void HttpStreamSession::cancel() noexcept {
    if (!impl_ || impl_->is_closed || impl_->is_finished || impl_->cursor.empty()) {
        close();
        return;
    }
    try {
        const std::string request_id = random_hex(16);
        auto metadata = impl_->continuation_metadata(request_id, true);
        const AnnotatedBatch request = AnnotatedBatch::data(make_empty_batch(empty_schema()));
        (void)impl_->state->post(
            impl_->method, impl_->state->path(impl_->method, "/exchange"),
            encode_ipc(request, metadata, impl_->state->request_serialization_cap()), request_id,
            CallOptions{}, false, impl_->sticky_session);
    } catch (const std::exception&) {
    }
    close();
}

}  // namespace vgi_rpc
