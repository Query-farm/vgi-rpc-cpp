// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/shm.h"
#include "vgi_rpc/wire.h"

#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/key_value_metadata.h>
#include <nlohmann/json.hpp>

#include <exception>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace vgi_rpc {

namespace {

std::shared_ptr<arrow::KeyValueMetadata> copy_metadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata) {
    return metadata ? std::static_pointer_cast<arrow::KeyValueMetadata>(metadata->Copy())
                    : std::make_shared<arrow::KeyValueMetadata>();
}

void replace_metadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                      const std::string& key, const std::string& value) {
    int64_t index = metadata->FindKey(key);
    while (index >= 0) {
        (void)metadata->Delete(index);
        index = metadata->FindKey(key);
    }
    metadata->Append(key, value);
}

void remove_metadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
                     const std::string& key) {
    int64_t index = metadata->FindKey(key);
    while (index >= 0) {
        (void)metadata->Delete(index);
        index = metadata->FindKey(key);
    }
}

void strip_transport_controls(const std::shared_ptr<arrow::KeyValueMetadata>& metadata) {
    for (const char* key :
         {keys::SHM_OFFSET, keys::SHM_LENGTH, keys::SHM_SOURCE, keys::SHM_SEGMENT_NAME,
          keys::SHM_SEGMENT_SIZE, keys::LOCATION, keys::LOCATION_SHA256, keys::LOCATION_SOURCE,
          keys::LOCATION_FETCH_MS, keys::CANCEL, keys::STATE_B64, keys::CALL_STATE_B64,
          keys::STREAM_STATE}) {
        remove_metadata(metadata, key);
    }
}

std::shared_ptr<arrow::KeyValueMetadata> request_metadata(
    const std::string& method, const std::string& protocol_version,
    const std::shared_ptr<arrow::KeyValueMetadata>& extras,
    const std::shared_ptr<ShmSegment>& shm) {
    if (method.empty()) throw std::invalid_argument("RPC method must not be empty");
    auto metadata = copy_metadata(extras);
    strip_transport_controls(metadata);
    // Framework-owned keys win over caller extras so a caller cannot make the
    // request id logged locally disagree with the one sent on the wire.
    replace_metadata(metadata, keys::METHOD, method);
    replace_metadata(metadata, keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    replace_metadata(metadata, keys::REQUEST_ID, random_hex(32));
    if (protocol_version.empty()) {
        int64_t index = metadata->FindKey(keys::PROTOCOL_VERSION);
        while (index >= 0) {
            (void)metadata->Delete(index);
            index = metadata->FindKey(keys::PROTOCOL_VERSION);
        }
    } else {
        replace_metadata(metadata, keys::PROTOCOL_VERSION, protocol_version);
    }
    if (shm) {
        std::string name = shm->name();
        while (!name.empty() && name.front() == '/') name.erase(name.begin());
        replace_metadata(metadata, keys::SHM_SEGMENT_NAME, name);
        replace_metadata(metadata, keys::SHM_SEGMENT_SIZE, std::to_string(shm->size()));
    }
    return metadata;
}

RpcException remote_exception(const AnnotatedBatch& response) {
    const std::string message =
        get_metadata_value(response.custom_metadata, keys::LOG_MESSAGE, "remote RPC exception");
    std::string exception_type = "RemoteError";
    const std::string extra = get_metadata_value(response.custom_metadata, keys::LOG_EXTRA);
    if (!extra.empty()) {
        try {
            const auto object = nlohmann::json::parse(extra);
            if (object.is_object()) {
                if (const auto it = object.find("exception_type");
                    it != object.end() && it->is_string()) {
                    exception_type = it->get<std::string>();
                }
            }
        } catch (const nlohmann::json::exception&) {
            // The primary message is still useful, and older peers did not
            // promise that LOG_EXTRA was parseable on exception envelopes.
        }
    }
    return RpcException(exception_type, message,
                        get_metadata_value(response.custom_metadata, keys::ERROR_KIND),
                        get_metadata_value(response.custom_metadata, keys::SERVER_ID),
                        get_metadata_value(response.custom_metadata, keys::REQUEST_ID));
}

Message log_message(const AnnotatedBatch& response) {
    const std::string level = get_metadata_value(response.custom_metadata, keys::LOG_LEVEL);
    const std::string text = get_metadata_value(response.custom_metadata, keys::LOG_MESSAGE);
    Message message{log_level_from_string(level), text, nlohmann::json::object()};
    const std::string extra = get_metadata_value(response.custom_metadata, keys::LOG_EXTRA);
    if (!extra.empty()) {
        try {
            message.extra = nlohmann::json::parse(extra);
        } catch (const nlohmann::json::exception& error) {
            throw std::runtime_error("invalid JSON in RPC log metadata: " +
                                     std::string(error.what()));
        }
    }
    return message;
}

AnnotatedBatch copy_batch_with_metadata(const arrow::RecordBatchWithMetadata& batch_with_metadata) {
    AnnotatedBatch result;
    result.batch = batch_with_metadata.batch;
    if (batch_with_metadata.custom_metadata) {
        result.custom_metadata = std::static_pointer_cast<arrow::KeyValueMetadata>(
            batch_with_metadata.custom_metadata->Copy());
    }
    return result;
}

}  // namespace

class RpcClient::Impl {
public:
    Impl(ClientTransport value, RpcClientOptions value_options)
        : transport(std::move(value)),
          input(transport.input()),
          output(transport.output()),
          options(std::move(value_options)) {}

    void reserve_call() {
        std::lock_guard<std::mutex> lock(mutex);
        if (closed || !transport.is_open()) throw std::runtime_error("RPC client is closed");
        if (active) {
            throw std::logic_error(
                "raw RPC client already has an active call; close its stream before reusing it");
        }
        active = true;
    }

    void release_call() noexcept {
        std::lock_guard<std::mutex> lock(mutex);
        active = false;
    }

    void abort_transport() noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex);
            closed = true;
            active = false;
        }
        try {
            transport.close();
        } catch (...) {
        }
    }

    void dispatch_log(const AnnotatedBatch& response) const {
        if (options.on_log) options.on_log(log_message(response));
    }

    AnnotatedBatch resolve_data(AnnotatedBatch response) const {
        switch (classify_batch(response)) {
            case BatchType::SHM_POINTER: {
                if (!shm) {
                    throw std::runtime_error(
                        "server returned a shared-memory pointer without a negotiated segment");
                }
                int64_t free_offset = -1;
                response.batch =
                    resolve_shm_batch(response.batch, &response.custom_metadata, shm, &free_offset);
                if (free_offset >= 0) shm->free_alloc(free_offset);
                return response;
            }
            case BatchType::EXTERNAL_POINTER:
                throw std::runtime_error(
                    "external-location batches are not supported by the raw native client");
            case BatchType::STATE_TOKEN:
                throw std::runtime_error(
                    "state-token batch is invalid on a live raw stream transport");
            default: return response;
        }
    }

    ClientTransport transport;
    std::shared_ptr<arrow::io::InputStream> input;
    std::shared_ptr<arrow::io::OutputStream> output;
    RpcClientOptions options;
    std::shared_ptr<ShmSegment> shm;
    mutable std::mutex mutex;
    bool active = false;
    bool closed = false;
};

namespace {

template <typename ClientImpl>
class CallReservation {
public:
    explicit CallReservation(const std::shared_ptr<ClientImpl>& client) : client_(client) {
        client_->reserve_call();
    }
    ~CallReservation() {
        if (client_) client_->release_call();
    }
    CallReservation(const CallReservation&) = delete;
    CallReservation& operator=(const CallReservation&) = delete;

    void transfer() noexcept { client_.reset(); }

private:
    std::shared_ptr<ClientImpl> client_;
};

template <typename ClientImpl>
std::optional<AnnotatedBatch> read_substream(const std::shared_ptr<ClientImpl>& client) {
    const auto contents = read_ipc_stream(client->input);
    if (!contents) throw std::runtime_error("transport closed before RPC response schema");

    std::optional<AnnotatedBatch> data;
    for (auto response : contents->batches) {
        switch (classify_batch(response)) {
            case BatchType::LOG: client->dispatch_log(response); break;
            case BatchType::EXCEPTION: throw remote_exception(response);
            case BatchType::DATA:
            case BatchType::SHM_POINTER:
                if (data) throw std::runtime_error("RPC response contains multiple data batches");
                data = client->resolve_data(std::move(response));
                break;
            case BatchType::EXTERNAL_POINTER:
            case BatchType::STATE_TOKEN: (void)client->resolve_data(std::move(response));
        }
    }
    return data;
}

}  // namespace

class ClientStream::Impl {
public:
    Impl(std::shared_ptr<RpcClient::Impl> value_client, ClientStreamKind value_kind,
         std::optional<AnnotatedBatch> value_header)
        : client(std::move(value_client)), kind(value_kind), header(std::move(value_header)) {}

    ~Impl() {
        // A destructor must not enter the protocol drain: an unresponsive
        // peer could otherwise block it forever. Abandoning a live lockstep
        // stream necessarily poisons that connection, so shut the transport
        // down using its bounded/local close path.
        if (!closed) {
            client->abort_transport();
            released = true;
            closed = true;
        }
    }

    void ensure_input_writer(const std::shared_ptr<arrow::Schema>& schema) {
        if (input_writer) {
            if (!input_schema->Equals(*schema, /*check_metadata=*/true)) {
                throw std::invalid_argument(
                    "all exchange inputs in one stream must use the same Arrow schema");
            }
            return;
        }
        input_writer = unwrap(arrow::ipc::MakeStreamWriter(client->output, schema),
                              "cannot open stream input writer");
        input_schema = schema;
    }

    void ensure_output_reader() {
        if (output_reader) return;
        output_reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(client->input),
                               "cannot open stream output reader");
    }

    std::optional<AnnotatedBatch> read_next_data() {
        try {
            ensure_output_reader();
            while (true) {
                auto next = unwrap(output_reader->ReadNext(), "cannot read stream response");
                if (!next.batch) {
                    finished = true;
                    return std::nullopt;
                }
                auto response = copy_batch_with_metadata(next);
                switch (classify_batch(response)) {
                    case BatchType::LOG: client->dispatch_log(response); break;
                    case BatchType::EXCEPTION:
                        finished = true;
                        drain_reader(output_reader);
                        throw remote_exception(response);
                    case BatchType::DATA:
                    case BatchType::SHM_POINTER: return client->resolve_data(std::move(response));
                    case BatchType::EXTERNAL_POINTER:
                    case BatchType::STATE_TOKEN: (void)client->resolve_data(std::move(response));
                }
            }
        } catch (const RpcException&) {
            throw;
        } catch (...) {
            closed = true;
            released = true;
            client->abort_transport();
            throw;
        }
    }

    void require_step_allowed(ClientStreamKind expected, const char* operation) const {
        if (kind != expected)
            throw std::logic_error(std::string(operation) + " on wrong stream kind");
        if (cancelled || closed)
            throw std::logic_error(std::string(operation) + " after stream close");
    }

    void send_cancel() {
        const auto schema = input_schema ? input_schema : empty_schema();
        ensure_input_writer(schema);
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(keys::CANCEL, "1");
        VGI_RPC_THROW_NOT_OK(
            input_writer->WriteRecordBatch(*make_empty_batch(schema), std::move(metadata)));
        VGI_RPC_THROW_NOT_OK(client->output->Flush());
    }

    void release() noexcept {
        if (!released) {
            released = true;
            client->release_call();
        }
    }

    void drain_output() {
        ensure_output_reader();
        std::exception_ptr pending;
        while (true) {
            auto next = unwrap(output_reader->ReadNext(), "cannot drain stream response");
            if (!next.batch) break;
            auto response = copy_batch_with_metadata(next);
            if (classify_batch(response) == BatchType::LOG) {
                client->dispatch_log(response);
            } else if (classify_batch(response) == BatchType::EXCEPTION && !pending) {
                pending = std::make_exception_ptr(remote_exception(response));
            } else if (classify_batch(response) == BatchType::SHM_POINTER) {
                // A peer may emit one final value after the caller chose to
                // close. Resolve it solely to release the allocator region.
                (void)client->resolve_data(std::move(response));
            }
        }
        finished = true;
        if (pending) std::rethrow_exception(pending);
    }

    void cancel() {
        if (cancelled || closed) {
            cancelled = true;
            release();
            return;
        }
        if (finished) {
            cancelled = true;
            closed = true;
            try {
                if (input_writer) {
                    VGI_RPC_THROW_NOT_OK(input_writer->Close());
                } else {
                    ensure_input_writer(empty_schema());
                    VGI_RPC_THROW_NOT_OK(input_writer->Close());
                }
                release();
            } catch (...) {
                released = true;
                client->abort_transport();
                throw;
            }
            return;
        }
        try {
            send_cancel();
            cancelled = true;
            drain_output();
            if (input_writer) VGI_RPC_THROW_NOT_OK(input_writer->Close());
            closed = true;
            release();
        } catch (...) {
            cancelled = true;
            closed = true;
            released = true;
            client->abort_transport();
            throw;
        }
    }

    void close() {
        if (closed) {
            release();
            return;
        }
        closed = true;
        try {
            if (input_writer) {
                VGI_RPC_THROW_NOT_OK(input_writer->Close());
            } else {
                ensure_input_writer(empty_schema());
                VGI_RPC_THROW_NOT_OK(input_writer->Close());
            }
            drain_output();
            release();
        } catch (...) {
            released = true;
            client->abort_transport();
            throw;
        }
    }

    std::shared_ptr<RpcClient::Impl> client;
    ClientStreamKind kind;
    std::optional<AnnotatedBatch> header;
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> input_writer;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> output_reader;
    bool cancelled = false;
    bool finished = false;
    bool closed = false;
    bool released = false;
};

RpcException::RpcException(std::string exception_type, std::string message, std::string error_kind,
                           std::string server_id, std::string request_id)
    : std::runtime_error(std::move(message)),
      exception_type_(std::move(exception_type)),
      error_kind_(std::move(error_kind)),
      server_id_(std::move(server_id)),
      request_id_(std::move(request_id)) {}

ClientStream::ClientStream(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ClientStream::~ClientStream() = default;
ClientStream::ClientStream(ClientStream&&) noexcept = default;
ClientStream& ClientStream::operator=(ClientStream&&) noexcept = default;

ClientStreamKind ClientStream::kind() const noexcept {
    return impl_->kind;
}

const std::optional<AnnotatedBatch>& ClientStream::header() const noexcept {
    return impl_->header;
}

bool ClientStream::finished() const noexcept {
    return impl_->finished;
}

std::optional<AnnotatedBatch> ClientStream::tick() {
    impl_->require_step_allowed(ClientStreamKind::PRODUCER, "tick");
    if (impl_->finished) return std::nullopt;
    try {
        impl_->ensure_input_writer(empty_schema());
        VGI_RPC_THROW_NOT_OK(
            impl_->input_writer->WriteRecordBatch(*make_empty_batch(empty_schema())));
        VGI_RPC_THROW_NOT_OK(impl_->client->output->Flush());
    } catch (...) {
        impl_->closed = true;
        impl_->released = true;
        impl_->client->abort_transport();
        throw;
    }
    return impl_->read_next_data();
}

std::optional<AnnotatedBatch> ClientStream::exchange(
    const std::shared_ptr<arrow::RecordBatch>& input,
    std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    impl_->require_step_allowed(ClientStreamKind::EXCHANGE, "exchange");
    if (impl_->finished) return std::nullopt;
    if (!input) throw std::invalid_argument("exchange input batch must not be null");
    if (impl_->input_schema &&
        !impl_->input_schema->Equals(*input->schema(), /*check_metadata=*/true)) {
        throw std::invalid_argument(
            "all exchange inputs in one stream must use the same Arrow schema");
    }
    int64_t outbound_shm_offset = -1;
    try {
        impl_->ensure_input_writer(input->schema());
        auto outbound = input;
        if (impl_->client->shm) {
            metadata = copy_metadata(metadata);
            strip_transport_controls(metadata);
            outbound = maybe_write_to_shm(input, &metadata, impl_->client->shm);
            const std::string offset = get_metadata_value(metadata, keys::SHM_OFFSET);
            if (!offset.empty()) outbound_shm_offset = std::stoll(offset);
        } else if (metadata) {
            metadata = copy_metadata(metadata);
            strip_transport_controls(metadata);
        }
        if (metadata) {
            VGI_RPC_THROW_NOT_OK(
                impl_->input_writer->WriteRecordBatch(*outbound, std::move(metadata)));
        } else {
            VGI_RPC_THROW_NOT_OK(impl_->input_writer->WriteRecordBatch(*outbound));
        }
        VGI_RPC_THROW_NOT_OK(impl_->client->output->Flush());
    } catch (...) {
        if (outbound_shm_offset >= 0 && impl_->client->shm) {
            impl_->client->shm->free_alloc(outbound_shm_offset);
        }
        impl_->closed = true;
        impl_->released = true;
        impl_->client->abort_transport();
        throw;
    }
    return impl_->read_next_data();
}

void ClientStream::cancel() {
    impl_->cancel();
}

void ClientStream::close() {
    impl_->close();
}

RpcClient::RpcClient(ClientTransport transport, const RpcClientOptions& options)
    : impl_(std::make_shared<Impl>(std::move(transport), options)) {
    if (options.shared_memory_bytes > 0) (void)enable_shared_memory(options.shared_memory_bytes);
}

RpcClient::~RpcClient() = default;
RpcClient::RpcClient(RpcClient&&) noexcept = default;
RpcClient& RpcClient::operator=(RpcClient&&) noexcept = default;

RpcClient RpcClient::spawn(const std::vector<std::string>& argv,
                           const RpcClientOptions& client_options,
                           const SubprocessTransportOptions& transport_options) {
    return RpcClient(ClientTransport::spawn(argv, transport_options), client_options);
}

RpcClient RpcClient::connect_unix(const std::string& path, const RpcClientOptions& options,
                                  const SocketTransportOptions& transport_options) {
    return RpcClient(ClientTransport::connect_unix(path, transport_options), options);
}

RpcClient RpcClient::connect_pipe(const std::string& pipe_name, const RpcClientOptions& options,
                                  const SocketTransportOptions& transport_options) {
    return RpcClient(ClientTransport::connect_pipe(pipe_name, transport_options), options);
}

RpcClient RpcClient::connect_tcp(const std::string& host, uint16_t port,
                                 const RpcClientOptions& options,
                                 const SocketTransportOptions& transport_options) {
    return RpcClient(ClientTransport::connect_tcp(host, port, transport_options), options);
}

AnnotatedBatch RpcClient::call_unary(const std::string& method,
                                     const std::shared_ptr<arrow::RecordBatch>& params,
                                     std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    if (!params) throw std::invalid_argument("RPC params batch must not be null");
    auto request_md =
        request_metadata(method, impl_->options.protocol_version, metadata, impl_->shm);
    CallReservation reservation(impl_);
    try {
        write_ipc_stream(impl_->output, params->schema(),
                         {AnnotatedBatch::with_metadata(params, std::move(request_md))});
        VGI_RPC_THROW_NOT_OK(impl_->output->Flush());
        auto response = read_substream(impl_);
        if (!response) throw std::runtime_error("RPC response contains no data batch");
        return std::move(*response);
    } catch (const RpcException&) {
        throw;
    } catch (...) {
        impl_->abort_transport();
        throw;
    }
}

ClientStream RpcClient::open_stream(const std::string& method,
                                    const std::shared_ptr<arrow::RecordBatch>& params,
                                    bool has_header,
                                    std::shared_ptr<arrow::KeyValueMetadata> metadata,
                                    ClientStreamKind kind) {
    if (!params) throw std::invalid_argument("RPC params batch must not be null");
    auto request_md =
        request_metadata(method, impl_->options.protocol_version, metadata, impl_->shm);
    CallReservation reservation(impl_);
    try {
        write_ipc_stream(impl_->output, params->schema(),
                         {AnnotatedBatch::with_metadata(params, std::move(request_md))});
        VGI_RPC_THROW_NOT_OK(impl_->output->Flush());
    } catch (...) {
        impl_->abort_transport();
        throw;
    }

    std::optional<AnnotatedBatch> header;
    if (has_header) {
        try {
            header = read_substream(impl_);
            if (!header) throw std::runtime_error("stream response contains no declared header");
        } catch (const RpcException&) {
            // Factory errors are written before the server drains the input
            // substream. Complete that empty substream so this connection can
            // be reused after the exception.
            try {
                auto writer = unwrap(arrow::ipc::MakeStreamWriter(impl_->output, empty_schema()));
                VGI_RPC_THROW_NOT_OK(writer->Close());
                VGI_RPC_THROW_NOT_OK(impl_->output->Flush());
            } catch (...) {
                impl_->abort_transport();
            }
            throw;
        } catch (...) {
            impl_->abort_transport();
            throw;
        }
    }
    auto stream =
        ClientStream(std::make_unique<ClientStream::Impl>(impl_, kind, std::move(header)));
    reservation.transfer();
    return stream;
}

ClientStream RpcClient::open_producer(const std::string& method,
                                      const std::shared_ptr<arrow::RecordBatch>& params,
                                      bool has_header,
                                      std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    return open_stream(method, params, has_header, std::move(metadata), ClientStreamKind::PRODUCER);
}

ClientStream RpcClient::open_exchange(const std::string& method,
                                      const std::shared_ptr<arrow::RecordBatch>& params,
                                      bool has_header,
                                      std::shared_ptr<arrow::KeyValueMetadata> metadata) {
    return open_stream(method, params, has_header, std::move(metadata), ClientStreamKind::EXCHANGE);
}

ServiceDescription RpcClient::describe() {
    return parse_service_description(
        call_unary(DESCRIBE_METHOD_NAME, make_empty_batch(empty_schema())));
}

ClientTransportOptions RpcClient::transport_options() {
    const auto response =
        call_unary(TRANSPORT_OPTIONS_METHOD_NAME, make_empty_batch(empty_schema()));
    ClientTransportOptions options;
    options.raw = copy_metadata(response.custom_metadata);
    options.shm = get_metadata_value(response.custom_metadata, keys::TRANSPORT_SHM) == "true";
    return options;
}

bool RpcClient::enable_shared_memory(size_t bytes) {
    if (bytes <= kShmHeaderSize) {
        throw std::invalid_argument(
            "shared-memory segment must be larger than its allocator header");
    }
    if (impl_->shm) return true;
    if (!shm_available() || !transport_options().shm) return false;
    // Darwin limits POSIX SHM names to 31 bytes including the leading slash.
    const std::string name = "vgi_rpc_" + random_hex(20);
    auto segment = ShmSegment::create(name, bytes);
    if (!segment) throw std::runtime_error("cannot create POSIX shared-memory segment");
    impl_->shm = std::move(segment);
    return true;
}

bool RpcClient::shared_memory_enabled() const noexcept {
    return impl_ && static_cast<bool>(impl_->shm);
}

uint32_t RpcClient::shared_memory_live_allocations() const noexcept {
    return impl_ && impl_->shm ? impl_->shm->live_allocations() : 0;
}

void RpcClient::close() {
    if (!impl_) return;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->active) {
            throw std::logic_error("cannot close RPC client while a stream is active");
        }
        if (impl_->closed) return;
        impl_->closed = true;
    }
    impl_->shm.reset();
    impl_->transport.close();
}

}  // namespace vgi_rpc
