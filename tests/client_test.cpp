// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/client.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/shm.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/type.h>

#include <chrono>
#include <atomic>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <thread>

#ifndef _WIN32
#include <sys/socket.h>
#include <unistd.h>
#endif

using namespace vgi_rpc;

namespace {

class RecordingOutputStream final : public arrow::io::OutputStream {
public:
    arrow::Status Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return static_cast<int64_t>(bytes_.size()); }
    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (closed_) return arrow::Status::IOError("write after close");
        bytes_.append(static_cast<const char*>(data), static_cast<size_t>(nbytes));
        return arrow::Status::OK();
    }
    arrow::Status Flush() override { return arrow::Status::OK(); }

    const std::string& bytes() const noexcept { return bytes_; }

private:
    std::string bytes_;
    bool closed_ = false;
};

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::int64())});
}

std::shared_ptr<arrow::RecordBatch> value_batch(int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(value_schema(), 1, {unwrap(builder.Finish())});
}

std::shared_ptr<arrow::Buffer> encoded_response(const std::shared_ptr<arrow::Schema>& schema,
                                                const std::vector<AnnotatedBatch>& batches) {
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, schema, batches);
    return unwrap(output->Finish());
}

struct InjectedClient {
    std::shared_ptr<RecordingOutputStream> output;
    RpcClient client;

    explicit InjectedClient(std::shared_ptr<arrow::Buffer> response,
                            const RpcClientOptions& options = {})
        : output(std::make_shared<RecordingOutputStream>()),
          client(ClientTransport::from_streams(
                     std::make_shared<arrow::io::BufferReader>(std::move(response)), output),
                 options) {}
};

#ifndef _WIN32
struct SocketOwner {
    explicit SocketOwner(int value) : fd(value) {}
    ~SocketOwner() { close(); }
    void close() noexcept {
        if (fd >= 0) {
            (void)::shutdown(fd, SHUT_RDWR);
            (void)::close(fd);
            fd = -1;
        }
    }
    int fd;
};

class OwningFdInputStream final : public FdInputStream {
public:
    explicit OwningFdInputStream(std::shared_ptr<SocketOwner> owner)
        : FdInputStream(owner->fd), owner_(std::move(owner)) {}
    arrow::Status Close() override {
        const auto status = FdInputStream::Close();
        owner_->close();
        return status;
    }

private:
    std::shared_ptr<SocketOwner> owner_;
};

class OwningFdOutputStream final : public FdOutputStream {
public:
    explicit OwningFdOutputStream(std::shared_ptr<SocketOwner> owner)
        : FdOutputStream(owner->fd), owner_(std::move(owner)) {}
    arrow::Status Close() override {
        const auto status = FdOutputStream::Close();
        owner_->close();
        return status;
    }

private:
    std::shared_ptr<SocketOwner> owner_;
};
#endif

int64_t value_of(const AnnotatedBatch& response) {
    return std::static_pointer_cast<arrow::Int64Array>(response.batch->column(0))->Value(0);
}

}  // namespace

TEST_CASE("raw unary client owns reserved request metadata and dispatches logs", "[client]") {
    auto log_md = std::make_shared<arrow::KeyValueMetadata>();
    log_md->Append(keys::LOG_LEVEL, "INFO");
    log_md->Append(keys::LOG_MESSAGE, "working");
    log_md->Append(keys::LOG_EXTRA, R"({"part":2})");

    std::vector<Message> logs;
    RpcClientOptions options;
    options.protocol_version = "2026-08";
    options.on_log = [&](const Message& message) { logs.push_back(message); };
    InjectedClient fixture(
        encoded_response(value_schema(),
                         {AnnotatedBatch::with_metadata(make_empty_batch(value_schema()), log_md),
                          AnnotatedBatch::data(value_batch(42))}),
        options);

    auto caller_md = std::make_shared<arrow::KeyValueMetadata>();
    caller_md->Append(keys::METHOD, "spoofed");
    caller_md->Append(keys::REQUEST_ID, "spoofed");
    caller_md->Append("application.tag", "kept");
    const auto response =
        fixture.client.call_unary("answer", make_empty_batch(empty_schema()), std::move(caller_md));

    REQUIRE(value_of(response) == 42);
    REQUIRE(logs.size() == 1);
    REQUIRE(logs[0].message == "working");
    REQUIRE(logs[0].extra.at("part") == 2);

    auto request_input = std::make_shared<arrow::io::BufferReader>(
        arrow::Buffer::FromString(fixture.output->bytes()));
    const auto request = read_ipc_stream(request_input);
    REQUIRE(request);
    REQUIRE(request->batches.size() == 1);
    const auto& metadata = request->batches[0].custom_metadata;
    REQUIRE(get_metadata_value(metadata, keys::METHOD) == "answer");
    REQUIRE(get_metadata_value(metadata, keys::REQUEST_VERSION) == REQUEST_VERSION_VALUE);
    REQUIRE(get_metadata_value(metadata, keys::PROTOCOL_VERSION) == "2026-08");
    REQUIRE(get_metadata_value(metadata, keys::REQUEST_ID).size() == 32);
    REQUIRE(get_metadata_value(metadata, "application.tag") == "kept");
}

TEST_CASE("raw unary client preserves structured remote exceptions", "[client]") {
    auto error = make_error_metadata("ValueError", "bad value", "worker-1", "request-1",
                                     ERROR_KIND_SESSION_LOST);
    const auto error_response = encoded_response(
        empty_schema(), {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), error)});
    const auto success_response =
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(8))});
    std::string combined(reinterpret_cast<const char*>(error_response->data()),
                         static_cast<size_t>(error_response->size()));
    combined.append(reinterpret_cast<const char*>(success_response->data()),
                    static_cast<size_t>(success_response->size()));
    InjectedClient fixture(arrow::Buffer::FromString(std::move(combined)));

    try {
        (void)fixture.client.call_unary("fail", make_empty_batch(empty_schema()));
        FAIL("expected RpcException");
    } catch (const RpcException& exception) {
        REQUIRE(exception.exception_type() == "ValueError");
        REQUIRE(std::string(exception.what()) == "bad value");
        REQUIRE(exception.error_kind() == ERROR_KIND_SESSION_LOST);
        REQUIRE(exception.server_id() == "worker-1");
        REQUIRE(exception.request_id() == "request-1");
    }
    // A fully consumed remote exception is an application result, not a
    // framing failure; the ordered transport remains reusable.
    REQUIRE(value_of(fixture.client.call_unary("succeed", make_empty_batch(empty_schema()))) == 8);
}

TEST_CASE("raw producer defers response open and writes a tick substream", "[client]") {
    InjectedClient fixture(
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(7))}));
    auto stream = fixture.client.open_producer("numbers", make_empty_batch(empty_schema()));

    const auto response = stream.tick();
    REQUIRE(response);
    REQUIRE(value_of(*response) == 7);
    REQUIRE_THROWS_AS(fixture.client.call_unary("busy", make_empty_batch(empty_schema())),
                      std::logic_error);
    stream.close();

    auto request_input = std::make_shared<arrow::io::BufferReader>(
        arrow::Buffer::FromString(fixture.output->bytes()));
    const auto params = read_ipc_stream(request_input);
    const auto ticks = read_ipc_stream(request_input);
    REQUIRE(params);
    REQUIRE(ticks);
    REQUIRE(get_metadata_value(params->batches[0].custom_metadata, keys::METHOD) == "numbers");
    REQUIRE(ticks->schema->num_fields() == 0);
    REQUIRE(ticks->batches.size() == 1);
    REQUIRE(ticks->batches[0].batch->num_rows() == 0);
}

TEST_CASE("raw producer parses its optional header substream", "[client]") {
    const auto header_buffer =
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(3))});
    const auto output_buffer =
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(4))});
    std::string combined(reinterpret_cast<const char*>(header_buffer->data()),
                         static_cast<size_t>(header_buffer->size()));
    combined.append(reinterpret_cast<const char*>(output_buffer->data()),
                    static_cast<size_t>(output_buffer->size()));
    InjectedClient fixture(arrow::Buffer::FromString(std::move(combined)));

    auto stream =
        fixture.client.open_producer("with_header", make_empty_batch(empty_schema()), true);
    REQUIRE(stream.header());
    REQUIRE(value_of(*stream.header()) == 3);
    const auto response = stream.tick();
    REQUIRE(response);
    REQUIRE(value_of(*response) == 4);
    stream.close();
}

TEST_CASE("raw stream cancellation sends the cancellation sentinel", "[client]") {
    InjectedClient fixture(
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(9))}));
    auto stream = fixture.client.open_producer("numbers", make_empty_batch(empty_schema()));
    stream.cancel();

    auto request_input = std::make_shared<arrow::io::BufferReader>(
        arrow::Buffer::FromString(fixture.output->bytes()));
    REQUIRE(read_ipc_stream(request_input));
    const auto cancellation = read_ipc_stream(request_input);
    REQUIRE(cancellation);
    REQUIRE(cancellation->batches.size() == 1);
    REQUIRE(get_metadata_value(cancellation->batches[0].custom_metadata, keys::CANCEL) == "1");
}

TEST_CASE("abandoning a raw stream aborts its transport instead of draining", "[client]") {
    InjectedClient fixture(
        encoded_response(value_schema(), {AnnotatedBatch::data(value_batch(1))}));
    {
        auto stream = fixture.client.open_producer("numbers", make_empty_batch(empty_schema()));
        REQUIRE_FALSE(stream.finished());
    }
    REQUIRE_THROWS_AS(fixture.client.call_unary("after-abort", make_empty_batch(empty_schema())),
                      std::runtime_error);
    REQUIRE(fixture.output->closed());
}

TEST_CASE("raw transport options parses open-ended capability metadata", "[client]") {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(keys::TRANSPORT_SHM, "true");
    metadata->Append("vgi_rpc.transport.future", "yes");
    InjectedClient fixture(encoded_response(
        empty_schema(),
        {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()), metadata)}));

    const auto options = fixture.client.transport_options();
    REQUIRE(options.shm);
    REQUIRE(get_metadata_value(options.raw, "vgi_rpc.transport.future") == "yes");
}

#ifndef _WIN32
TEST_CASE("negotiated SHM externalizes exchange input and releases its allocation",
          "[client][shm]") {
    int sockets[2] = {-1, -1};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    timeval timeout{5, 0};
    for (const int fd : sockets) {
        REQUIRE(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        REQUIRE(::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
    }
    std::exception_ptr server_error;
    std::atomic<bool> received_pointer{false};
    std::atomic<bool> returned_pointer{false};

    std::thread server([&] {
        try {
            auto input = std::make_shared<FdInputStream>(sockets[1]);
            auto output = std::make_shared<FdOutputStream>(sockets[1]);

            const auto options_request = read_ipc_stream(input);
            if (!options_request) throw std::runtime_error("missing transport-options request");
            auto options_md = std::make_shared<arrow::KeyValueMetadata>();
            options_md->Append(keys::TRANSPORT_SHM, "true");
            write_ipc_stream(output, empty_schema(),
                             {AnnotatedBatch::with_metadata(make_empty_batch(empty_schema()),
                                                            std::move(options_md))});
            VGI_RPC_THROW_NOT_OK(output->Flush());

            const auto params = read_ipc_stream(input);
            if (!params || params->batches.size() != 1) {
                throw std::runtime_error("missing exchange params");
            }
            const auto& request_md = params->batches[0].custom_metadata;
            const std::string name = get_metadata_value(request_md, keys::SHM_SEGMENT_NAME);
            const size_t size = static_cast<size_t>(
                std::stoull(get_metadata_value(request_md, keys::SHM_SEGMENT_SIZE)));
            auto segment = ShmSegment::attach(name, size);
            if (!segment) throw std::runtime_error("cannot attach client SHM segment");

            auto stream_reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(input));
            auto incoming = unwrap(stream_reader->ReadNext());
            if (!incoming.batch) throw std::runtime_error("missing exchange input");
            auto incoming_md = incoming.custom_metadata
                                   ? std::static_pointer_cast<arrow::KeyValueMetadata>(
                                         incoming.custom_metadata->Copy())
                                   : nullptr;
            received_pointer.store(!get_metadata_value(incoming_md, keys::SHM_OFFSET).empty());
            int64_t free_offset = -1;
            auto resolved = resolve_shm_batch(incoming.batch, &incoming_md, segment, &free_offset);
            if (free_offset < 0) throw std::runtime_error("exchange input was not in SHM");
            segment->free_alloc(free_offset);

            if (resolved->num_rows() != 1) {
                throw std::runtime_error("resolved exchange input has the wrong row count");
            }
            std::shared_ptr<arrow::KeyValueMetadata> response_md;
            auto response = maybe_write_to_shm(resolved, &response_md, segment);
            returned_pointer.store(!get_metadata_value(response_md, keys::SHM_OFFSET).empty());
            auto stream_writer = unwrap(arrow::ipc::MakeStreamWriter(output, resolved->schema()));
            VGI_RPC_THROW_NOT_OK(stream_writer->WriteRecordBatch(*response, response_md));
            VGI_RPC_THROW_NOT_OK(output->Flush());
            auto eos = unwrap(stream_reader->ReadNext());
            if (eos.batch) throw std::runtime_error("expected exchange input EOS");
            VGI_RPC_THROW_NOT_OK(stream_writer->Close());
            VGI_RPC_THROW_NOT_OK(output->Flush());
        } catch (...) {
            server_error = std::current_exception();
        }
        (void)::close(sockets[1]);
        sockets[1] = -1;
    });

    RpcClientOptions client_options;
    client_options.shared_memory_bytes = 1024 * 1024;
    auto socket_owner = std::make_shared<SocketOwner>(sockets[0]);
    RpcClient client(
        ClientTransport::from_streams(std::make_shared<OwningFdInputStream>(socket_owner),
                                      std::make_shared<OwningFdOutputStream>(socket_owner)),
        client_options);
    REQUIRE(client.shared_memory_enabled());
    auto stream = client.open_exchange("echo", make_empty_batch(empty_schema()));

    arrow::BinaryBuilder builder;
    const std::string payload(256 * 1024, 'x');
    VGI_RPC_THROW_NOT_OK(builder.Append(payload));
    const auto schema = arrow::schema({arrow::field("payload", arrow::binary())});
    const auto batch = arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
    const auto response = stream.exchange(batch);
    REQUIRE(response);
    REQUIRE(response->batch->num_rows() == 1);
    REQUIRE(received_pointer.load());
    REQUIRE(returned_pointer.load());
    REQUIRE(client.shared_memory_live_allocations() == 0);
    stream.close();
    client.close();
    sockets[0] = -1;
    server.join();
    if (server_error) std::rethrow_exception(server_error);
}

TEST_CASE("subprocess transport shutdown escalates and reaps within configured bounds",
          "[client][transport]") {
    SubprocessTransportOptions options;
    options.stderr_mode = ClientStderrMode::DISCARD;
    options.close_grace = std::chrono::milliseconds(10);
    options.terminate_grace = std::chrono::milliseconds(10);
    auto transport = ClientTransport::spawn({"/usr/bin/tail", "-f", "/dev/null"}, options);

    const auto start = std::chrono::steady_clock::now();
    transport.close();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    REQUIRE(elapsed < std::chrono::seconds(1));
    REQUIRE_FALSE(transport.is_open());
}

TEST_CASE("socket transport rejects a non-positive connect deadline", "[client][transport]") {
    SocketTransportOptions options;
    options.connect_timeout = std::chrono::milliseconds::zero();
    REQUIRE_THROWS_AS(ClientTransport::connect_unix("/vgi-rpc-does-not-exist", options),
                      std::invalid_argument);
}
#endif
