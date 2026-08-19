// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/client.h>
#include <vgi_rpc/metadata.h>

#include <arrow/array.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace vgi_rpc;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

std::shared_ptr<arrow::Schema> typed_schema() {
    auto nullable_item = [](std::shared_ptr<arrow::DataType> type) {
        return arrow::field("item", std::move(type), /*nullable=*/true);
    };
    return arrow::schema({
        arrow::field("nullable_float", arrow::float64(), true),
        arrow::field("tags", arrow::list(nullable_item(arrow::utf8())), true),
        arrow::field("category", arrow::dictionary(arrow::int16(), arrow::utf8()), true),
        arrow::field("event_time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), true),
        arrow::field("amount", arrow::decimal128(18, 4), true),
        arrow::field(
            "nested",
            arrow::struct_(
                {arrow::field("name", arrow::utf8(), true),
                 arrow::field("scores", arrow::list(nullable_item(arrow::int32())), true)}),
            true),
    });
}

std::shared_ptr<arrow::RecordBatch> null_batch(const std::shared_ptr<arrow::Schema>& schema,
                                               int64_t rows) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(static_cast<size_t>(schema->num_fields()));
    for (const auto& field : schema->fields()) {
        arrays.push_back(unwrap(arrow::MakeArrayOfNull(field->type(), rows)));
    }
    return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

std::shared_ptr<arrow::RecordBatch> populated_batch(const std::shared_ptr<arrow::Schema>& schema) {
    arrow::DoubleBuilder float_builder;
    VGI_RPC_THROW_NOT_OK(float_builder.Append(1.5));

    auto tag_values = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder tags_builder(arrow::default_memory_pool(), tag_values,
                                    schema->field(1)->type());
    VGI_RPC_THROW_NOT_OK(tags_builder.Append());
    VGI_RPC_THROW_NOT_OK(tag_values->Append("alpha"));
    VGI_RPC_THROW_NOT_OK(tag_values->AppendNull());
    VGI_RPC_THROW_NOT_OK(tag_values->Append("omega"));

    arrow::Int16Builder index_builder;
    VGI_RPC_THROW_NOT_OK(index_builder.Append(0));
    arrow::StringBuilder dictionary_builder;
    VGI_RPC_THROW_NOT_OK(dictionary_builder.Append("blue"));
    auto category = unwrap(arrow::DictionaryArray::FromArrays(schema->field(2)->type(),
                                                              unwrap(index_builder.Finish()),
                                                              unwrap(dictionary_builder.Finish())));

    arrow::TimestampBuilder time_builder(schema->field(3)->type(), arrow::default_memory_pool());
    VGI_RPC_THROW_NOT_OK(time_builder.Append(1787056496000000LL));

    arrow::Decimal128Builder amount_builder(schema->field(4)->type(), arrow::default_memory_pool());
    VGI_RPC_THROW_NOT_OK(amount_builder.Append(unwrap(arrow::Decimal128::FromString("1234.5000"))));

    arrow::StringBuilder name_builder;
    VGI_RPC_THROW_NOT_OK(name_builder.Append("sample"));
    auto score_values = std::make_shared<arrow::Int32Builder>();
    const auto nested_type = std::static_pointer_cast<arrow::StructType>(schema->field(5)->type());
    arrow::ListBuilder scores_builder(arrow::default_memory_pool(), score_values,
                                      nested_type->field(1)->type());
    VGI_RPC_THROW_NOT_OK(scores_builder.Append());
    VGI_RPC_THROW_NOT_OK(score_values->Append(1));
    VGI_RPC_THROW_NOT_OK(score_values->AppendNull());
    VGI_RPC_THROW_NOT_OK(score_values->Append(3));
    auto nested = unwrap(arrow::StructArray::Make(
        {unwrap(name_builder.Finish()), unwrap(scores_builder.Finish())}, nested_type->fields()));

    return arrow::RecordBatch::Make(
        schema, 1,
        {unwrap(float_builder.Finish()), unwrap(tags_builder.Finish()), std::move(category),
         unwrap(time_builder.Finish()), unwrap(amount_builder.Finish()), std::move(nested)});
}

std::shared_ptr<arrow::RecordBatch> bytes_params(const std::string& value) {
    arrow::BinaryBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    const auto schema = arrow::schema({arrow::field("value", arrow::binary(), false)});
    return arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
}

std::string bytes_result(const AnnotatedBatch& value) {
    require(value.batch != nullptr, "unary response has no data batch");
    require(value.batch->num_columns() == 1 && value.batch->num_rows() == 1,
            "unary byte response has the wrong shape");
    const auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(value.batch->column(0));
    require(array != nullptr, "unary byte response has the wrong Arrow type");
    return array->GetString(0);
}

std::shared_ptr<arrow::RecordBatch> producer_params(int64_t count, int64_t payload_bytes) {
    arrow::Int64Builder count_builder;
    arrow::Int64Builder payload_builder;
    VGI_RPC_THROW_NOT_OK(count_builder.Append(count));
    VGI_RPC_THROW_NOT_OK(payload_builder.Append(payload_bytes));
    const auto schema = arrow::schema({arrow::field("count", arrow::int64(), false),
                                       arrow::field("payload_bytes", arrow::int64(), false)});
    return arrow::RecordBatch::Make(
        schema, 1, {unwrap(count_builder.Finish()), unwrap(payload_builder.Finish())});
}

int64_t producer_index(const AnnotatedBatch& value) {
    require(value.batch != nullptr && value.batch->num_rows() == 1,
            "producer returned a malformed batch");
    const auto array = std::dynamic_pointer_cast<arrow::Int64Array>(value.batch->column(0));
    require(array != nullptr, "producer index has the wrong Arrow type");
    return array->Value(0);
}

void require_echo(ClientStream& stream, const std::shared_ptr<arrow::RecordBatch>& input,
                  const std::string& transport, bool expect_shm) {
    const auto output = stream.exchange(input);
    require(output.has_value(), transport + ": exchange ended before returning its echo");
    require(output->batch->schema()->Equals(*input->schema(), /*check_metadata=*/true),
            transport + ": exchange changed the declared schema");
    require(output->batch->Equals(*input, /*check_metadata=*/true),
            transport + ": exchange changed the batch data");
    if (expect_shm && input->num_rows() > 0) {
        require(!get_metadata_value(output->custom_metadata, keys::SHM_SOURCE).empty(),
                transport + ": exchange response did not traverse negotiated SHM");
    }
}

void exercise_client(RpcClient& client, const std::string& transport, bool expect_shm) {
    require(client.shared_memory_enabled() == expect_shm,
            transport + ": shared-memory negotiation returned an unexpected result");

    const auto description = client.describe();
    require(description.protocol_name == "ClientConformanceService",
            transport + ": describe returned the wrong protocol");
    require(description.describe_version == "4", transport + ": describe was not version 4");
    require(description.protocol_hash.size() == 64,
            transport + ": describe returned an invalid protocol hash");
    require(description.method("__transport_options__") == nullptr,
            transport + ": synthetic transport options leaked into describe");
    const auto* unary_method = description.method("echo_bytes");
    const auto* producer_method = description.method("producer_sequence");
    const auto* exchange_method = description.method("typed_exchange");
    require(unary_method && unary_method->method_type == "unary",
            transport + ": describe lost the unary method shape");
    require(producer_method && producer_method->method_type == "stream" &&
                producer_method->is_exchange == false,
            transport + ": describe lost the producer method shape");
    require(exchange_method && exchange_method->method_type == "stream" &&
                exchange_method->is_exchange == true,
            transport + ": describe lost the exchange method shape");

    std::string payload(256 * 1024, '\0');
    for (size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<char>(index % 251);
    }
    const auto unary = client.call_unary("echo_bytes", bytes_params(payload));
    require(bytes_result(unary) == payload, transport + ": unary bytes did not round-trip");
    if (expect_shm) {
        require(!get_metadata_value(unary.custom_metadata, keys::SHM_SOURCE).empty(),
                transport + ": unary response did not traverse negotiated SHM");
        require(client.shared_memory_live_allocations() == 0,
                transport + ": unary response leaked a shared-memory allocation");
    }

    auto producer = client.open_producer("producer_sequence", producer_params(3, 4));
    std::vector<int64_t> produced;
    while (const auto item = producer.tick()) produced.push_back(producer_index(*item));
    require(produced == std::vector<int64_t>{0, 1, 2},
            transport + ": producer did not yield 0, 1, 2 exactly once");
    require(producer.finished(), transport + ": producer did not observe output EOS");
    require(!producer.tick(), transport + ": terminal producer replayed a batch");
    producer.close();
    require(client.shared_memory_live_allocations() == 0,
            transport + ": producer leaked a shared-memory allocation");

    const auto schema = typed_schema();
    require(exchange_method->params_schema->num_fields() == 0,
            transport + ": typed_exchange params schema is not empty");
    auto exchange = client.open_exchange("typed_exchange", make_empty_batch(empty_schema()), false);
    require_echo(exchange, null_batch(schema, 1), transport, expect_shm);
    require_echo(exchange, null_batch(schema, 0), transport, /*expect_shm=*/false);
    require_echo(exchange, populated_batch(schema), transport, expect_shm);
    exchange.close();

    require(client.shared_memory_live_allocations() == 0,
            transport + ": client leaked a shared-memory allocation");
}

#ifndef _WIN32
const char* python_executable() {
    const char* python = std::getenv("VGI_RPC_PYTHON");
    return python && *python ? python : "python3";
}

class SocketWorker {
public:
    explicit SocketWorker(std::vector<std::string> arguments) {
        int output[2];
        if (::pipe(output) != 0) throw std::runtime_error("cannot create worker discovery pipe");
        pid_ = ::fork();
        if (pid_ < 0) {
            ::close(output[0]);
            ::close(output[1]);
            throw std::runtime_error("cannot fork Python raw worker");
        }
        if (pid_ == 0) {
            ::close(output[0]);
            if (::dup2(output[1], STDOUT_FILENO) < 0) _exit(126);
            ::close(output[1]);
            const int null_fd = ::open("/dev/null", O_WRONLY);
            if (null_fd >= 0) {
                (void)::dup2(null_fd, STDERR_FILENO);
                ::close(null_fd);
            }
            std::vector<std::string> argv = {python_executable(), "-m",
                                             "vgi_rpc.conformance.client_worker"};
            argv.insert(argv.end(), arguments.begin(), arguments.end());
            std::vector<char*> raw_argv;
            raw_argv.reserve(argv.size() + 1);
            for (auto& argument : argv) raw_argv.push_back(argument.data());
            raw_argv.push_back(nullptr);
            ::execvp(raw_argv[0], raw_argv.data());
            _exit(127);
        }
        ::close(output[1]);
        output_fd_ = output[0];
        try {
            discovery_ = read_discovery(std::chrono::seconds(10));
        } catch (...) {
            stop();
            throw;
        }
    }

    ~SocketWorker() { stop(); }
    SocketWorker(const SocketWorker&) = delete;
    SocketWorker& operator=(const SocketWorker&) = delete;

    const std::string& discovery() const noexcept { return discovery_; }

private:
    std::string read_discovery(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string line;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            pollfd descriptor{output_fd_, POLLIN | POLLHUP, 0};
            const int result = ::poll(&descriptor, 1, static_cast<int>(remaining.count()));
            if (result < 0 && errno == EINTR) continue;
            if (result < 0) throw std::runtime_error("cannot poll Python worker discovery");
            if (result == 0) break;
            char character = '\0';
            const ssize_t count = ::read(output_fd_, &character, 1);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) break;
            if (character == '\n') return line;
            if (line.size() >= 4096) throw std::runtime_error("worker discovery line is too long");
            line.push_back(character);
        }
        throw std::runtime_error("Python raw worker did not announce readiness within 10 seconds");
    }

    void stop() noexcept {
        if (pid_ > 0) {
            (void)::kill(pid_, SIGTERM);
            int status = 0;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < deadline) {
                const pid_t result = ::waitpid(pid_, &status, WNOHANG);
                if (result == pid_ || (result < 0 && errno == ECHILD)) {
                    pid_ = -1;
                    break;
                }
                if (result < 0 && errno != EINTR) break;
                ::usleep(10000);
            }
            if (pid_ > 0) {
                (void)::kill(pid_, SIGKILL);
                while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
                }
                pid_ = -1;
            }
        }
        if (output_fd_ >= 0) {
            ::close(output_fd_);
            output_fd_ = -1;
        }
    }

    pid_t pid_ = -1;
    int output_fd_ = -1;
    std::string discovery_;
};

SocketTransportOptions socket_options() {
    SocketTransportOptions options;
    options.connect_timeout = std::chrono::seconds(5);
    options.read_timeout = std::chrono::seconds(10);
    options.write_timeout = std::chrono::seconds(10);
    return options;
}

RpcClientOptions client_options() {
    RpcClientOptions options;
    options.shared_memory_bytes = 4 * 1024 * 1024;
    return options;
}

bool python_worker_available() {
    const pid_t pid = ::fork();
    if (pid < 0) return false;
    if (pid == 0) {
        const int null_fd = ::open("/dev/null", O_RDWR);
        if (null_fd >= 0) {
            (void)::dup2(null_fd, STDOUT_FILENO);
            (void)::dup2(null_fd, STDERR_FILENO);
            ::close(null_fd);
        }
        ::execlp(python_executable(), python_executable(), "-c",
                 "import vgi_rpc.conformance.client_worker", static_cast<char*>(nullptr));
        _exit(127);
    }

    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        const pid_t result = ::waitpid(pid, &status, WNOHANG);
        if (result == pid) return WIFEXITED(status) && WEXITSTATUS(status) == 0;
        if (result < 0 && errno != EINTR) return false;
        ::usleep(10000);
    }
    (void)::kill(pid, SIGKILL);
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    return false;
}

void test_stdio() {
    SubprocessTransportOptions transport;
    transport.stderr_mode = ClientStderrMode::DISCARD;
    transport.close_grace = std::chrono::seconds(2);
    transport.terminate_grace = std::chrono::seconds(2);
    auto client = RpcClient::spawn(
        {python_executable(), "-m", "vgi_rpc.conformance.client_worker", "--stdio"},
        client_options(), transport);
    exercise_client(client, "stdio", /*expect_shm=*/true);
    client.close();
}

void test_unix() {
    char directory_template[] = "/tmp/vgi-rpc-cpp-client-XXXXXX";
    char* directory = ::mkdtemp(directory_template);
    if (!directory) throw std::runtime_error("cannot create Unix worker temporary directory");
    char resolved_directory[PATH_MAX];
    if (!::realpath(directory, resolved_directory)) {
        (void)::rmdir(directory);
        throw std::runtime_error("cannot resolve Unix worker temporary directory");
    }
    const std::string socket_path = std::string(resolved_directory) + "/worker.sock";
    try {
        {
            SocketWorker worker({"--unix", socket_path});
            require(worker.discovery() == "UNIX:" + socket_path,
                    "Unix worker announced the wrong socket path");
            auto client = RpcClient::connect_unix(socket_path, client_options(), socket_options());
            exercise_client(client, "unix", /*expect_shm=*/true);
            client.close();
        }
        (void)::unlink(socket_path.c_str());
        (void)::rmdir(directory);
    } catch (...) {
        (void)::unlink(socket_path.c_str());
        (void)::rmdir(directory);
        throw;
    }
}

void test_tcp() {
    SocketWorker worker({"--tcp", "127.0.0.1:0"});
    const std::string prefix = "TCP:127.0.0.1:";
    require(worker.discovery().rfind(prefix, 0) == 0,
            "TCP worker announced an unexpected address: " + worker.discovery());
    const auto raw_port = worker.discovery().substr(prefix.size());
    size_t consumed = 0;
    const int port = std::stoi(raw_port, &consumed);
    require(consumed == raw_port.size() && port > 0 && port <= 65535,
            "TCP worker announced an invalid port");
    auto client = RpcClient::connect_tcp("127.0.0.1", static_cast<uint16_t>(port), client_options(),
                                         socket_options());
    exercise_client(client, "tcp", /*expect_shm=*/true);
    client.close();
}
#endif

}  // namespace

int main() {
#ifdef _WIN32
    std::cerr << "raw Unix/TCP native-client conformance is POSIX-only\n";
    return 77;
#else
    try {
        if (!python_worker_available()) {
            std::cerr << "Python vgi_rpc.conformance.client_worker is unavailable\n";
            return 77;
        }
        // Force both peers to exercise SHM for ordinary non-empty batches.
        // The Python worker inherits this environment variable.
        if (::setenv("VGI_RPC_SHM_MIN_BATCH_BYTES", "1", 1) != 0) {
            throw std::runtime_error("cannot configure shared-memory test threshold");
        }
        test_stdio();
        test_unix();
        test_tcp();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "native raw-client integration failed: " << error.what() << '\n';
        return 1;
    }
#endif
}
