// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client.h"
#include "vgi_rpc/http_client.h"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>

#ifdef VGI_RPC_WITH_IROH_CABI
#include <vgi_iroh.h>
#endif

namespace vgi_rpc {

#ifdef VGI_RPC_WITH_IROH_CABI
namespace {

IrohErrorStage error_stage(uint32_t value) {
    return value >= static_cast<uint32_t>(IrohErrorStage::PARSE) &&
                   value <= static_cast<uint32_t>(IrohErrorStage::INTERNAL)
               ? static_cast<IrohErrorStage>(value)
               : IrohErrorStage::INTERNAL;
}

IrohErrorCategory error_category(uint32_t value) {
    return value >= static_cast<uint32_t>(IrohErrorCategory::INVALID_INPUT) &&
                   value <= static_cast<uint32_t>(IrohErrorCategory::INTERNAL)
               ? static_cast<IrohErrorCategory>(value)
               : IrohErrorCategory::INTERNAL;
}

IrohDispatchCertainty dispatch_certainty(uint32_t value) {
    return value <= static_cast<uint32_t>(IrohDispatchCertainty::SENT)
               ? static_cast<IrohDispatchCertainty>(value)
               : IrohDispatchCertainty::UNKNOWN;
}

IrohTransportError cabi_error(const char* operation, const vgi_iroh_error& error) {
    size_t message_size = 0;
    while (message_size < VGI_IROH_ERROR_MESSAGE_CAPACITY && error.message[message_size] != '\0') {
        ++message_size;
    }
    return IrohTransportError(
        std::string("Iroh ") + operation + " failed: " + std::string(error.message, message_size),
        error_stage(error.stage), error_category(error.category),
        dispatch_certainty(error.dispatch_certainty));
}

arrow::Status cabi_status(const char* operation, const vgi_iroh_error& error) {
    const auto failure = cabi_error(operation, error);
    return arrow::Status::IOError(failure.what())
        .WithDetail(std::make_shared<IrohStatusDetail>(
            failure.stage(), failure.category(), failure.dispatch_certainty(), failure.what()));
}

uint8_t cancel_check(void* userdata) noexcept {
    if (!userdata) return 0;
    try {
        const auto& callback = *static_cast<const std::function<bool()>*>(userdata);
        return callback && callback() ? 1 : 0;
    } catch (...) {
        return 1;
    }
}

void validate_hint(const std::string& hint) {
    if (hint.empty() || std::any_of(hint.begin(), hint.end(),
                                    [](unsigned char c) { return c <= 0x1f || c == 0x7f; })) {
        throw IrohTransportError(
            "Iroh relay and direct-address hints must be non-empty and contain no controls",
            IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
            IrohDispatchCertainty::NOT_SENT);
    }
}

struct EndpointConfiguration {
    std::optional<std::array<uint8_t, SHA256_DIGEST_LENGTH>> secret_fingerprint;
    std::vector<std::string> relay_urls;
    bool no_relay;
    std::chrono::milliseconds connect_timeout;
    std::chrono::milliseconds io_timeout;

    bool operator==(const EndpointConfiguration&) const = default;
};

struct NativeEndpoint {
    vgi_iroh_endpoint* endpoint = nullptr;
    EndpointConfiguration configuration;

    ~NativeEndpoint() {
        if (endpoint) {
            vgi_iroh_endpoint_cancel(endpoint);
            vgi_iroh_endpoint_free(endpoint);
        }
    }
};

class EndpointPool {
public:
    std::shared_ptr<NativeEndpoint> get(const IrohTransportOptions& options) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto& effective_secret = options.secret_key ? *options.secret_key : process_secret();
        std::optional<std::array<uint8_t, SHA256_DIGEST_LENGTH>> fingerprint;
        fingerprint.emplace();
        SHA256(effective_secret.data(), effective_secret.size(), fingerprint->data());
        EndpointConfiguration requested{fingerprint, options.relay_urls, options.no_relay,
                                        options.connect_timeout, options.io_timeout};
        for (const auto& endpoint : endpoints_) {
            if (endpoint->configuration == requested) return endpoint;
        }

        std::vector<const char*> relays;
        relays.reserve(options.relay_urls.size());
        for (const auto& relay : options.relay_urls) relays.push_back(relay.c_str());
        std::array<char, 65> secret{};
        constexpr char hex[] = "0123456789abcdef";
        for (size_t i = 0; i < effective_secret.size(); ++i) {
            secret[i * 2] = hex[effective_secret[i] >> 4];
            secret[i * 2 + 1] = hex[effective_secret[i] & 0xf];
        }
        vgi_iroh_endpoint_config config{
            VGI_IROH_ABI_VERSION,
            secret.data(),
            options.no_relay ? VGI_IROH_RELAY_DISABLED
                             : (relays.empty() ? VGI_IROH_RELAY_DEFAULT : VGI_IROH_RELAY_CUSTOM),
            relays.empty() ? nullptr : relays.data(),
            relays.size(),
            static_cast<uint64_t>(options.connect_timeout.count()),
            static_cast<uint64_t>(options.io_timeout.count())};
        auto candidate = std::make_shared<NativeEndpoint>();
        candidate->configuration = std::move(requested);
        vgi_iroh_error error{};
        const auto result = vgi_iroh_endpoint_create(&config, &candidate->endpoint, &error);
        std::fill(secret.begin(), secret.end(), '\0');
        if (result != VGI_IROH_OK) throw cabi_error("endpoint creation", error);
        endpoints_.push_back(std::move(candidate));
        return endpoints_.back();
    }

private:
    static const std::array<uint8_t, 32>& process_secret() {
        // Private process-lifetime material is required to keep identity stable while endpoint
        // instances vary by timeout/relay policy. It is never exposed or copied into public state.
        static const auto* secret = [] {
            auto* value = new std::array<uint8_t, 32>();
            if (RAND_bytes(value->data(), static_cast<int>(value->size())) != 1) {
                delete value;
                throw IrohTransportError("could not generate process Iroh identity",
                                         IrohErrorStage::BIND, IrohErrorCategory::INTERNAL,
                                         IrohDispatchCertainty::NOT_SENT);
            }
            return value;
        }();
        return *secret;
    }

    std::mutex mutex_;
    std::vector<std::shared_ptr<NativeEndpoint>> endpoints_;
};

const std::shared_ptr<EndpointPool>& process_endpoint_pool() {
    // Process-lifetime by design: destroying a native Tokio runtime during C++ static
    // teardown can run after Rust/TLS teardown. The OS reclaims this one endpoint.
    static const auto* pool = new std::shared_ptr<EndpointPool>(std::make_shared<EndpointPool>());
    return *pool;
}

struct NativeState {
    std::shared_ptr<NativeEndpoint> endpoint;
    vgi_iroh_stream* stream = nullptr;
    std::function<bool()> cancel_callback;
    std::mutex close_mutex;
    bool finished = false;
    bool cancelled = false;

    ~NativeState() {
        if (stream) {
            vgi_iroh_stream_cancel(stream);
            vgi_iroh_stream_free(stream);
        }
    }

    void cancel() noexcept {
        std::lock_guard<std::mutex> lock(close_mutex);
        if (cancelled) return;
        cancelled = true;
        if (stream) vgi_iroh_stream_cancel(stream);
    }
};

class IrohInputStream final : public arrow::io::InputStream {
public:
    explicit IrohInputStream(std::shared_ptr<NativeState> state) : state_(std::move(state)) {}
    arrow::Status Close() override {
        if (!closed_) state_->cancel();
        closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        if (closed_) return arrow::Status::IOError("Iroh input stream is closed");
        if (nbytes < 0) return arrow::Status::Invalid("negative Iroh read length");
        int64_t total = 0;
        while (total < nbytes) {
            if (state_->cancel_callback && cancel_check(&state_->cancel_callback)) {
                state_->cancel();
                return arrow::Status::Cancelled("Iroh read cancelled")
                    .WithDetail(std::make_shared<IrohStatusDetail>(
                        IrohErrorStage::CANCEL, IrohErrorCategory::CANCELLED,
                        IrohDispatchCertainty::UNKNOWN, "Iroh read cancelled"));
            }
            size_t read = 0;
            vgi_iroh_error error{};
            const size_t chunk = static_cast<size_t>(std::min<int64_t>(nbytes - total, 64 << 20));
            uint8_t timed_out = 0;
            const auto result =
                state_->cancel_callback
                    ? vgi_iroh_stream_read_timeout(state_->stream,
                                                   static_cast<uint8_t*>(out) + total, chunk, 50,
                                                   &read, &timed_out, &error)
                    : vgi_iroh_stream_read(state_->stream, static_cast<uint8_t*>(out) + total,
                                           chunk, &read, &error);
            if (result != VGI_IROH_OK) return cabi_status("read", error);
            if (timed_out) continue;
            if (read == 0) break;
            total += static_cast<int64_t>(read);
        }
        position_ += total;
        return total;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(const int64_t read, Read(nbytes, buffer->mutable_data()));
        if (read < nbytes) RETURN_NOT_OK(buffer->Resize(read, false));
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
    }

private:
    std::shared_ptr<NativeState> state_;
    bool closed_ = false;
    int64_t position_ = 0;
};

class IrohOutputStream final : public arrow::io::OutputStream {
public:
    explicit IrohOutputStream(std::shared_ptr<NativeState> state) : state_(std::move(state)) {}
    arrow::Status Close() override {
        if (closed_) return arrow::Status::OK();
        closed_ = true;
        std::lock_guard<std::mutex> lock(state_->close_mutex);
        if (state_->finished || state_->cancelled) return arrow::Status::OK();
        vgi_iroh_error error{};
        if (vgi_iroh_stream_finish(state_->stream, &error) != VGI_IROH_OK) {
            return cabi_status("finish", error);
        }
        state_->finished = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }
    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (closed_) return arrow::Status::IOError("Iroh output stream is closed");
        if (nbytes < 0) return arrow::Status::Invalid("negative Iroh write length");
        vgi_iroh_error error{};
        const auto result =
            state_->cancel_callback
                ? vgi_iroh_stream_write_cancellable(
                      state_->stream, static_cast<const uint8_t*>(data),
                      static_cast<size_t>(nbytes), cancel_check, &state_->cancel_callback, &error)
                : vgi_iroh_stream_write(state_->stream, static_cast<const uint8_t*>(data),
                                        static_cast<size_t>(nbytes), &error);
        if (result != VGI_IROH_OK) {
            return cabi_status("write", error);
        }
        position_ += nbytes;
        return arrow::Status::OK();
    }
    arrow::Status Flush() override { return arrow::Status::OK(); }

private:
    std::shared_ptr<NativeState> state_;
    bool closed_ = false;
    int64_t position_ = 0;
};

ClientTransport open_native(const std::shared_ptr<EndpointPool>& pool,
                            const IrohEndpoint& remote_endpoint,
                            const IrohTransportOptions& options) {
    if (vgi_iroh_abi_version() != VGI_IROH_ABI_VERSION) {
        throw IrohTransportError("linked vgi-iroh C ABI version does not match this SDK",
                                 IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    for (const auto& relay : options.relay_urls) validate_hint(relay);
    if (options.remote_relay_url) validate_hint(*options.remote_relay_url);
    for (const auto& address : options.direct_addresses) validate_hint(address);
    auto endpoint = pool->get(options);
    auto state = std::make_shared<NativeState>();
    state->endpoint = endpoint;
    state->cancel_callback = options.cancel_check;
    std::vector<const char*> direct_addresses;
    direct_addresses.reserve(options.direct_addresses.size());
    for (const auto& address : options.direct_addresses)
        direct_addresses.push_back(address.c_str());
    vgi_iroh_remote remote{remote_endpoint.endpoint_id.c_str(),
                           options.remote_relay_url ? options.remote_relay_url->c_str() : nullptr,
                           direct_addresses.empty() ? nullptr : direct_addresses.data(),
                           direct_addresses.size()};
    vgi_iroh_error error{};
    const auto result =
        options.cancel_check
            ? vgi_iroh_stream_open_cancellable(endpoint->endpoint, &remote, cancel_check,
                                               &state->cancel_callback, &state->stream, &error)
            : vgi_iroh_stream_open(endpoint->endpoint, &remote, &state->stream, &error);
    if (result != VGI_IROH_OK) {
        throw cabi_error("stream open", error);
    }
    return ClientTransport::from_streams(std::make_shared<IrohInputStream>(state),
                                         std::make_shared<IrohOutputStream>(state));
}

std::string http_remote_id(const vgi_iroh_http_response* response) {
    size_t required = 0;
    vgi_iroh_error error{};
    if (vgi_iroh_http_response_remote_id(response, nullptr, 0, &required, &error) != VGI_IROH_OK) {
        throw cabi_error("HTTP remote identity", error);
    }
    std::string id(required, '\0');
    if (vgi_iroh_http_response_remote_id(response, id.data(), id.size(), &required, &error) !=
        VGI_IROH_OK) {
        throw cabi_error("HTTP remote identity", error);
    }
    if (required != 65 || id.size() != 65 || id.back() != '\0') {
        throw IrohTransportError("Iroh HTTP response returned a non-canonical EndpointId",
                                 IrohErrorStage::READ, IrohErrorCategory::PROTOCOL,
                                 IrohDispatchCertainty::SENT);
    }
    id.pop_back();
    return id;
}

IrohHttpResponse open_native_http(const std::shared_ptr<EndpointPool>& pool,
                                  const IrohEndpoint& remote_endpoint,
                                  const IrohHttpRequest& request,
                                  const IrohTransportOptions& options) {
    if (vgi_iroh_abi_version() != VGI_IROH_ABI_VERSION) {
        throw IrohTransportError("linked vgi-iroh C ABI version does not match this SDK",
                                 IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    if (remote_endpoint.scheme != IrohEndpoint::Scheme::HTTPI || request.max_response_bytes == 0 ||
        request.max_response_header_bytes == 0) {
        throw IrohTransportError("invalid bounded HTTP-over-Iroh request", IrohErrorStage::PARSE,
                                 IrohErrorCategory::INVALID_INPUT, IrohDispatchCertainty::NOT_SENT);
    }
    if (options.no_relay && !options.relay_urls.empty()) {
        throw IrohTransportError("no_relay and relay_urls are mutually exclusive",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    if (options.connect_timeout <= std::chrono::milliseconds::zero() ||
        options.io_timeout <= std::chrono::milliseconds::zero()) {
        throw IrohTransportError("Iroh timeouts must be positive", IrohErrorStage::PARSE,
                                 IrohErrorCategory::INVALID_INPUT, IrohDispatchCertainty::NOT_SENT);
    }
    for (const auto& relay : options.relay_urls) validate_hint(relay);
    if (options.remote_relay_url) validate_hint(*options.remote_relay_url);
    for (const auto& address : options.direct_addresses) validate_hint(address);
    auto endpoint = pool->get(options);

    std::vector<const char*> direct_addresses;
    direct_addresses.reserve(options.direct_addresses.size());
    for (const auto& address : options.direct_addresses)
        direct_addresses.push_back(address.c_str());
    vgi_iroh_remote remote{remote_endpoint.endpoint_id.c_str(),
                           options.remote_relay_url ? options.remote_relay_url->c_str() : nullptr,
                           direct_addresses.empty() ? nullptr : direct_addresses.data(),
                           direct_addresses.size()};
    std::vector<vgi_iroh_header> headers;
    headers.reserve(request.headers.size());
    for (const auto& [name, value] : request.headers) {
        headers.push_back({reinterpret_cast<const uint8_t*>(name.data()), name.size(),
                           reinterpret_cast<const uint8_t*>(value.data()), value.size()});
    }
    vgi_iroh_http_request native_request{request.method.c_str(),
                                         request.path.c_str(),
                                         headers.empty() ? nullptr : headers.data(),
                                         headers.size(),
                                         reinterpret_cast<const uint8_t*>(request.body.data()),
                                         request.body.size()};
    vgi_iroh_http_response* raw = nullptr;
    vgi_iroh_error error{};
    auto cancellation = request.cancel_check;
    const auto result = cancellation ? vgi_iroh_http_request_start_cancellable(
                                           endpoint->endpoint, &remote, &native_request,
                                           cancel_check, &cancellation, &raw, &error)
                                     : vgi_iroh_http_request_start(endpoint->endpoint, &remote,
                                                                   &native_request, &raw, &error);
    if (result != VGI_IROH_OK) throw cabi_error("HTTP request", error);
    std::unique_ptr<vgi_iroh_http_response, decltype(&vgi_iroh_http_response_free)> response(
        raw, &vgi_iroh_http_response_free);

    IrohHttpResponse output;
    output.remote_endpoint_id = http_remote_id(raw);
    if (output.remote_endpoint_id != remote_endpoint.endpoint_id) {
        vgi_iroh_http_response_cancel(raw);
        throw IrohTransportError(
            "Iroh HTTP authenticated peer identity did not match the requested EndpointId",
            IrohErrorStage::READ, IrohErrorCategory::AUTHENTICATION, IrohDispatchCertainty::SENT);
    }
    output.status = vgi_iroh_http_response_status(raw);
    const size_t header_count = vgi_iroh_http_response_header_count(raw);
    if (header_count > 1024 || header_count > request.max_response_header_bytes) {
        vgi_iroh_http_response_cancel(raw);
        throw IrohTransportError("Iroh HTTP response contains too many headers",
                                 IrohErrorStage::READ, IrohErrorCategory::RESOURCE_EXHAUSTED,
                                 IrohDispatchCertainty::SENT);
    }
    output.headers.reserve(header_count);
    size_t header_bytes = 0;
    for (size_t index = 0; index < header_count; ++index) {
        size_t name_size = 0;
        size_t value_size = 0;
        if (vgi_iroh_http_response_header(raw, index, nullptr, 0, &name_size, nullptr, 0,
                                          &value_size, &error) != VGI_IROH_OK) {
            throw cabi_error("HTTP response header", error);
        }
        if (name_size > request.max_response_header_bytes - header_bytes ||
            value_size > request.max_response_header_bytes - header_bytes - name_size) {
            vgi_iroh_http_response_cancel(raw);
            throw IrohTransportError("Iroh HTTP response headers exceed the configured limit",
                                     IrohErrorStage::READ, IrohErrorCategory::RESOURCE_EXHAUSTED,
                                     IrohDispatchCertainty::SENT);
        }
        std::string name(name_size, '\0');
        std::string value(value_size, '\0');
        if (vgi_iroh_http_response_header(raw, index, reinterpret_cast<uint8_t*>(name.data()),
                                          name.size(), &name_size,
                                          reinterpret_cast<uint8_t*>(value.data()), value.size(),
                                          &value_size, &error) != VGI_IROH_OK) {
            throw cabi_error("HTTP response header", error);
        }
        header_bytes += name.size() + value.size();
        output.headers.emplace_back(std::move(name), std::move(value));
    }

    std::array<uint8_t, 64 * 1024> chunk{};
    auto last_progress = std::chrono::steady_clock::now();
    while (true) {
        if (cancellation && cancel_check(&cancellation)) {
            vgi_iroh_http_response_cancel(raw);
            throw IrohTransportError("Iroh HTTP response read cancelled", IrohErrorStage::CANCEL,
                                     IrohErrorCategory::CANCELLED, IrohDispatchCertainty::SENT);
        }
        size_t count = 0;
        uint8_t timed_out = 0;
        const auto read_result =
            cancellation
                ? vgi_iroh_http_response_read_timeout(raw, chunk.data(), chunk.size(), 50, &count,
                                                      &timed_out, &error)
                : vgi_iroh_http_response_read(raw, chunk.data(), chunk.size(), &count, &error);
        if (read_result != VGI_IROH_OK) throw cabi_error("HTTP response read", error);
        if (timed_out) {
            if (std::chrono::steady_clock::now() - last_progress >= options.io_timeout) {
                vgi_iroh_http_response_cancel(raw);
                throw IrohTransportError("Iroh HTTP response exceeded its idle timeout",
                                         IrohErrorStage::READ, IrohErrorCategory::TIMEOUT,
                                         IrohDispatchCertainty::SENT);
            }
            continue;
        }
        if (count == 0) break;
        if (count > request.max_response_bytes ||
            output.body.size() > request.max_response_bytes - count) {
            vgi_iroh_http_response_cancel(raw);
            throw IrohTransportError("Iroh HTTP response body exceeds the configured limit",
                                     IrohErrorStage::READ, IrohErrorCategory::RESOURCE_EXHAUSTED,
                                     IrohDispatchCertainty::SENT);
        }
        output.body.append(reinterpret_cast<const char*>(chunk.data()), count);
        last_progress = std::chrono::steady_clock::now();
    }
    return output;
}

}  // namespace
#endif

IrohTransportProvider native_iroh_transport_provider() {
#ifdef VGI_RPC_WITH_IROH_CABI
    return [](const IrohEndpoint& endpoint, const IrohTransportOptions& options) {
        return open_native(process_endpoint_pool(), endpoint, options);
    };
#else
    return [](const IrohEndpoint&, const IrohTransportOptions&) -> ClientTransport {
        throw IrohTransportError(
            "iroh:// requires vgi-rpc-c++ built with VGI_RPC_WITH_IROH_CABI and a version-matched "
            "vgi-iroh library",
            IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED, IrohDispatchCertainty::NOT_SENT);
    };
#endif
}

IrohHttpTransportProvider native_iroh_http_transport_provider() {
#ifdef VGI_RPC_WITH_IROH_CABI
    return [](const IrohEndpoint& endpoint, const IrohHttpRequest& request,
              const IrohTransportOptions& options) {
        return open_native_http(process_endpoint_pool(), endpoint, request, options);
    };
#else
    return [](const IrohEndpoint&, const IrohHttpRequest&,
              const IrohTransportOptions&) -> IrohHttpResponse {
        throw IrohTransportError(
            "httpi:// requires vgi-rpc-c++ built with VGI_RPC_WITH_IROH_CABI and a "
            "version-matched vgi-iroh library",
            IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED, IrohDispatchCertainty::NOT_SENT);
    };
#endif
}

std::string native_iroh_endpoint_id(const IrohTransportOptions& options) {
#ifdef VGI_RPC_WITH_IROH_CABI
    if (options.no_relay && !options.relay_urls.empty()) {
        throw IrohTransportError("no_relay and relay_urls are mutually exclusive",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    if (options.connect_timeout <= std::chrono::milliseconds::zero() ||
        options.io_timeout <= std::chrono::milliseconds::zero()) {
        throw IrohTransportError("Iroh timeouts must be positive", IrohErrorStage::PARSE,
                                 IrohErrorCategory::INVALID_INPUT, IrohDispatchCertainty::NOT_SENT);
    }
    for (const auto& relay : options.relay_urls) validate_hint(relay);
    const auto endpoint = process_endpoint_pool()->get(options);
    std::array<char, 65> id{};
    size_t required = 0;
    vgi_iroh_error error{};
    if (vgi_iroh_endpoint_id(endpoint->endpoint, id.data(), id.size(), &required, &error) !=
        VGI_IROH_OK) {
        throw cabi_error("endpoint ID", error);
    }
    if (required != id.size() || id.back() != '\0') {
        throw IrohTransportError("native Iroh endpoint returned a non-canonical EndpointId",
                                 IrohErrorStage::BIND, IrohErrorCategory::PROTOCOL,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    return std::string(id.data(), id.size() - 1);
#else
    (void)options;
    throw IrohTransportError(
        "local EndpointId requires vgi-rpc-c++ built with VGI_RPC_WITH_IROH_CABI",
        IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED, IrohDispatchCertainty::NOT_SENT);
#endif
}

bool native_iroh_transport_available() noexcept {
#ifdef VGI_RPC_WITH_IROH_CABI
    return vgi_iroh_abi_version() == VGI_IROH_ABI_VERSION;
#else
    return false;
#endif
}

bool native_iroh_http_transport_available() noexcept {
    return native_iroh_transport_available();
}

}  // namespace vgi_rpc
