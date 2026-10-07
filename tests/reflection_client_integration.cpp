// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `list_protocols` / `describe_protocol`: reflection over a held connection.
//
// Every transport, against this port's conformance worker *and* the Python
// reference conformance server, because the point of the API is that it reuses
// whatever connection the caller already has: an `RpcClient` asks over its own
// byte stream, an `HttpClient` (or `HttpSessionView`) over its own connection
// pool and settings. A green run against our own server proves only that the
// two halves of this port agree; the reference leg is the one that counts.
//
// Both conformance servers host the primary (`ConformanceService`), then
// `conformance.Secondary.v1`, then `vgi_rpc.Reflection.v1`. The Python server
// hosts reflection only with `--describe` (`enable_describe` defaults to
// false), which is what the no-reflection cases use; this port's server always
// hosts it.
//
// Environment:
//   VGI_RPC_PYTHON  interpreter with the reference `vgi_rpc` importable
//                   (default python3); the reference leg is skipped when the
//                   import fails, and nothing else is.

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/client.h>
#include <vgi_rpc/errors.h>
#include <vgi_rpc/http_client.h>

#include "../src/reflection_client_internal.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <httplib.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <limits.h>
#include <memory>
#include <mutex>
#include <optional>
#include <poll.h>
#include <regex>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kPrimary = "ConformanceService";
constexpr const char* kSecondary = "conformance.Secondary.v1";
constexpr const char* kReflection = "vgi_rpc.Reflection.v1";

const char* python_executable() {
    const char* python = std::getenv("VGI_RPC_PYTHON");
    return python && *python ? python : "python3";
}

// --- processes ----------------------------------------------------------------

/// fork/exec argv, optionally wiring the child's stdin/stdout to pipes.
pid_t spawn_process(const std::vector<std::string>& argv, int* child_stdin, int* child_stdout) {
    int in_pipe[2] = {-1, -1};
    int out_pipe[2] = {-1, -1};
    if (child_stdin && ::pipe(in_pipe) != 0) throw std::runtime_error("pipe failed");
    if (::pipe(out_pipe) != 0) throw std::runtime_error("pipe failed");
    const pid_t pid = ::fork();
    if (pid < 0) throw std::runtime_error("fork failed");
    if (pid == 0) {
        if (child_stdin) {
            ::dup2(in_pipe[0], STDIN_FILENO);
            ::close(in_pipe[0]);
            ::close(in_pipe[1]);
        } else {
            const int null_fd = ::open("/dev/null", O_RDONLY);
            if (null_fd >= 0) ::dup2(null_fd, STDIN_FILENO);
        }
        ::dup2(out_pipe[1], STDOUT_FILENO);
        ::close(out_pipe[0]);
        ::close(out_pipe[1]);
        if (!std::getenv("VGI_REFLECTION_TEST_STDERR")) {
            const int null_fd = ::open("/dev/null", O_WRONLY);
            if (null_fd >= 0) ::dup2(null_fd, STDERR_FILENO);
        }
        std::vector<std::string> copy = argv;
        std::vector<char*> raw;
        for (auto& argument : copy) raw.push_back(argument.data());
        raw.push_back(nullptr);
        ::execvp(raw[0], raw.data());
        _exit(127);
    }
    if (child_stdin) {
        ::close(in_pipe[0]);
        *child_stdin = in_pipe[1];
    }
    ::close(out_pipe[1]);
    *child_stdout = out_pipe[0];
    return pid;
}

void reap(pid_t pid) noexcept {
    if (pid <= 0) return;
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    (void)::kill(pid, SIGTERM);
    while (std::chrono::steady_clock::now() < deadline) {
        const pid_t result = ::waitpid(pid, &status, WNOHANG);
        if (result == pid || (result < 0 && errno == ECHILD)) return;
        ::usleep(10000);
    }
    (void)::kill(pid, SIGKILL);
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
}

bool python_reference_available() {
    static const bool available = [] {
        int out = -1;
        const pid_t pid = spawn_process(
            {python_executable(), "-c",
             "import vgi_rpc.conformance._cli, vgi_rpc.conformance.secondary, waitress"},
            nullptr, &out);
        int status = 0;
        while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        ::close(out);
        return WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }();
    return available;
}

/// A listening server that announces `PREFIX:...` on its first stdout line.
class Listener {
public:
    explicit Listener(const std::vector<std::string>& argv) {
        pid_ = spawn_process(argv, nullptr, &out_);
        try {
            line_ = read_line(std::chrono::seconds(20));
        } catch (...) {
            stop();
            throw;
        }
    }
    ~Listener() { stop(); }
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;

    const std::string& line() const noexcept { return line_; }

private:
    std::string read_line(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string line;
        while (std::chrono::steady_clock::now() < deadline) {
            pollfd descriptor{out_, POLLIN | POLLHUP, 0};
            const int result = ::poll(&descriptor, 1, 100);
            if (result < 0 && errno == EINTR) continue;
            if (result <= 0) continue;
            char c = '\0';
            const ssize_t count = ::read(out_, &c, 1);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) break;
            if (c == '\n') return line;
            line.push_back(c);
        }
        throw std::runtime_error("server did not announce its address");
    }
    void stop() noexcept {
        reap(pid_);
        pid_ = -1;
        if (out_ >= 0) ::close(out_);
        out_ = -1;
    }

    pid_t pid_ = -1;
    int out_ = -1;
    std::string line_;
};

void wait_for_port(int port) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(static_cast<uint16_t>(port));
        ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
        const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
        ::close(fd);
        if (rc == 0) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    throw std::runtime_error("HTTP server did not start listening");
}

std::string temp_socket_path() {
    char directory_template[] = "/tmp/vgi-rpc-refl-XXXXXX";
    char* directory = ::mkdtemp(directory_template);
    if (!directory) throw std::runtime_error("mkdtemp failed");
    return std::string(directory) + "/worker.sock";
}

// --- servers ------------------------------------------------------------------

enum class Server { CPP, PYTHON };
enum class Transport { PIPE, SHM_PIPE, UNIX, TCP, HTTP, HTTP_SESSION };

/// Where each conformance server mounts its HTTP routes.
std::string http_prefix(Server server) { return server == Server::CPP ? "/vgi" : ""; }

const char* server_name(Server server) { return server == Server::CPP ? "cpp" : "python"; }

const char* transport_name(Transport transport) {
    switch (transport) {
        case Transport::PIPE: return "pipe";
        case Transport::SHM_PIPE: return "shm_pipe";
        case Transport::UNIX: return "unix";
        case Transport::TCP: return "tcp";
        case Transport::HTTP: return "http";
        case Transport::HTTP_SESSION: return "http_session";
    }
    return "?";
}

/// argv for *server* on *transport*; `describe` only matters to Python.
std::vector<std::string> server_argv(Server server, Transport transport,
                                     const std::string& unix_path, bool describe = true) {
    std::vector<std::string> argv;
    if (server == Server::CPP) {
        argv = {VGI_RPC_CONFORMANCE_WORKER};
        switch (transport) {
            case Transport::PIPE:
            case Transport::SHM_PIPE: break;
            case Transport::UNIX: argv.insert(argv.end(), {"--unix", unix_path}); break;
            case Transport::TCP: argv.insert(argv.end(), {"--tcp", "127.0.0.1:0"}); break;
            case Transport::HTTP:
            case Transport::HTTP_SESSION:
                argv.insert(argv.end(), {"--http", "--port", "0"});
                break;
        }
        return argv;
    }
    argv = {python_executable(), "-m", "vgi_rpc.conformance._cli"};
    if (describe) argv.push_back("--describe");
    switch (transport) {
        case Transport::PIPE:
        case Transport::SHM_PIPE: argv.push_back("--pipe"); break;
        case Transport::UNIX: argv.insert(argv.end(), {"--unix", unix_path}); break;
        case Transport::TCP: argv.insert(argv.end(), {"--tcp", "127.0.0.1:0"}); break;
        case Transport::HTTP:
        case Transport::HTTP_SESSION: argv.insert(argv.end(), {"--http", "0"}); break;
    }
    return argv;
}

// --- calls --------------------------------------------------------------------

std::shared_ptr<arrow::RecordBatch> string_params(const std::string& value) {
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::utf8(), /*nullable=*/false)}), 1,
                                    {unwrap(builder.Finish())});
}

std::shared_ptr<arrow::RecordBatch> count_params(int64_t count) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(count));
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("count", arrow::int64(), /*nullable=*/false)}), 1,
                                    {unwrap(builder.Finish())});
}

std::string string_result(const AnnotatedBatch& value) {
    REQUIRE(value.batch);
    REQUIRE(value.batch->num_rows() == 1);
    const auto array = std::dynamic_pointer_cast<arrow::StringArray>(value.batch->column(0));
    REQUIRE(array);
    return array->GetString(0);
}

/// One held connection, whatever the transport.
class Conn {
public:
    virtual ~Conn() = default;
    virtual std::vector<HostedProtocol> list_protocols() = 0;
    virtual ServiceDescription describe_protocol(const std::string& name) = 0;
    virtual std::string echo(const std::string& value) = 0;
    virtual int produce(int64_t count) = 0;
};

class RawConn final : public Conn {
public:
    explicit RawConn(RpcClient client) : client_(std::move(client)) {}
    ~RawConn() override {
        try {
            client_.close();
        } catch (...) {
        }
    }
    std::vector<HostedProtocol> list_protocols() override { return client_.list_protocols(); }
    ServiceDescription describe_protocol(const std::string& name) override {
        return client_.describe_protocol(name);
    }
    std::string echo(const std::string& value) override {
        return string_result(client_.call_unary("echo_string", string_params(value)));
    }
    int produce(int64_t count) override {
        auto stream = client_.open_producer("produce_n", count_params(count));
        int batches = 0;
        while (stream.tick()) ++batches;
        stream.close();
        return batches;
    }
    RpcClient& client() { return client_; }

private:
    RpcClient client_;
};

/// `T` is HttpClient or HttpSessionView: the same surface, one held state.
template <typename T>
class HttpConn final : public Conn {
public:
    HttpConn(std::shared_ptr<HttpClient> owner, std::optional<T> view)
        : owner_(std::move(owner)), view_(std::move(view)) {}
    std::vector<HostedProtocol> list_protocols() override { return target().list_protocols(); }
    ServiceDescription describe_protocol(const std::string& name) override {
        return target().describe_protocol(name);
    }
    std::string echo(const std::string& value) override {
        return string_result(target().call("echo_string", AnnotatedBatch::data(string_params(value))));
    }
    int produce(int64_t count) override {
        auto stream =
            target().open_producer("produce_n", AnnotatedBatch::data(count_params(count)), nullptr);
        int batches = 0;
        while (stream.tick()) ++batches;
        return batches;
    }

private:
    const T& target() const {
        if constexpr (std::is_same_v<T, HttpClient>) {
            return *owner_;
        } else {
            return *view_;
        }
    }
    std::shared_ptr<HttpClient> owner_;
    std::optional<T> view_;
};

/// The primary declares a protocol version, and calls to it must carry it.
std::string version_of(const std::string& protocol) {
    return protocol == kPrimary ? "2.0.0" : "";
}

RpcClientOptions raw_options(const std::string& protocol = kPrimary) {
    RpcClientOptions options;
    options.protocol = protocol;
    options.protocol_version = version_of(protocol);
    return options;
}

SocketTransportOptions socket_options() {
    SocketTransportOptions options;
    options.connect_timeout = std::chrono::seconds(5);
    options.read_timeout = std::chrono::seconds(20);
    options.write_timeout = std::chrono::seconds(20);
    return options;
}

int announced_port(const std::string& line, const std::string& prefix) {
    REQUIRE(line.rfind(prefix, 0) == 0);
    const auto colon = line.rfind(':');
    return std::stoi(line.substr(colon + 1));
}

/// A live server plus one connection to it.
struct Fixture {
    std::unique_ptr<Listener> listener;
    std::string unix_path;
    std::unique_ptr<Conn> conn;
    int http_port = 0;

    ~Fixture() {
        conn.reset();
        listener.reset();
        if (!unix_path.empty()) {
            (void)::unlink(unix_path.c_str());
            (void)::rmdir(unix_path.substr(0, unix_path.rfind('/')).c_str());
        }
    }
};

std::unique_ptr<Fixture> connect_to(Server server, Transport transport, bool describe = true,
                              const std::string& protocol = kPrimary) {
    auto fixture = std::make_unique<Fixture>();
    if (transport == Transport::UNIX) fixture->unix_path = temp_socket_path();
    const auto argv = server_argv(server, transport, fixture->unix_path, describe);
    SubprocessTransportOptions spawn_options;
    if (!std::getenv("VGI_REFLECTION_TEST_STDERR")) {
        spawn_options.stderr_mode = ClientStderrMode::DISCARD;
    }
    switch (transport) {
        case Transport::PIPE:
        case Transport::SHM_PIPE: {
            auto options = raw_options(protocol);
            if (transport == Transport::SHM_PIPE) options.shared_memory_bytes = 4 * 1024 * 1024;
            auto client = RpcClient::spawn(argv, options, spawn_options);
            if (transport == Transport::SHM_PIPE) REQUIRE(client.shared_memory_enabled());
            fixture->conn = std::make_unique<RawConn>(std::move(client));
            break;
        }
        case Transport::UNIX: {
            fixture->listener = std::make_unique<Listener>(argv);
            REQUIRE(fixture->listener->line() == "UNIX:" + fixture->unix_path);
            // A server may announce the path a moment before it accepts.
            for (int attempt = 0;; ++attempt) {
                try {
                    fixture->conn = std::make_unique<RawConn>(RpcClient::connect_unix(
                        fixture->unix_path, raw_options(protocol), socket_options()));
                    break;
                } catch (const std::exception&) {
                    if (attempt >= 100) throw;
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            break;
        }
        case Transport::TCP: {
            fixture->listener = std::make_unique<Listener>(argv);
            const int port = announced_port(fixture->listener->line(), "TCP:");
            fixture->conn = std::make_unique<RawConn>(RpcClient::connect_tcp(
                "127.0.0.1", static_cast<uint16_t>(port), raw_options(protocol), socket_options()));
            break;
        }
        case Transport::HTTP:
        case Transport::HTTP_SESSION: {
            fixture->listener = std::make_unique<Listener>(argv);
            fixture->http_port = announced_port(fixture->listener->line(), "PORT:");
            wait_for_port(fixture->http_port);
            auto client = std::make_shared<HttpClient>(
                HttpClient::builder("http://127.0.0.1:" + std::to_string(fixture->http_port))
                    .prefix(http_prefix(server))
                    .protocol(protocol)
                    .protocol_version(version_of(protocol))
                    .build());
            if (transport == Transport::HTTP) {
                fixture->conn =
                    std::make_unique<HttpConn<HttpClient>>(client, std::optional<HttpClient>{});
            } else {
                fixture->conn = std::make_unique<HttpConn<HttpSessionView>>(
                    client, std::optional<HttpSessionView>{client->with_session_token()});
            }
            break;
        }
    }
    return fixture;
}

/// Catch any remote error from *call* as a RemoteStatus, with its dynamic type.
template <typename F>
std::pair<std::string, bool> remote_error_of(F&& call) {
    try {
        call();
    } catch (const ReflectionNotSupportedError& error) {
        return {error.error_kind(), true};
    } catch (const RpcException& error) {
        return {error.error_kind(), false};
    } catch (const RpcRemoteError& error) {
        return {error.error_kind(), false};
    }
    FAIL("call did not raise a remote error");
    return {};
}

bool is_hex64(const std::string& value) {
    static const std::regex pattern("[0-9a-f]{64}");
    return std::regex_match(value, pattern);
}

#define SKIP_UNLESS_AVAILABLE(server)                                                  \
    do {                                                                               \
        if ((server) == Server::PYTHON && !python_reference_available()) {            \
            SKIP("reference vgi_rpc not importable by " << python_executable());       \
        }                                                                              \
    } while (0)

}  // namespace

// --- every transport, both servers ----------------------------------------------

TEST_CASE("list_protocols lists the server's protocols in order on every transport",
          "[reflection][transport]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    const auto transport = GENERATE(Transport::PIPE, Transport::SHM_PIPE, Transport::UNIX,
                                    Transport::TCP, Transport::HTTP, Transport::HTTP_SESSION);
    CAPTURE(server_name(server), transport_name(transport));
    SKIP_UNLESS_AVAILABLE(server);
    auto fixture = connect_to(server, transport);

    const auto hosted = fixture->conn->list_protocols();
    REQUIRE(hosted.size() >= 3);
    CHECK(hosted[0].name == kPrimary);
    CHECK(hosted[1].name == kSecondary);
    CHECK(std::any_of(hosted.begin() + 2, hosted.end(),
                      [](const HostedProtocol& p) { return p.name == kReflection; }));
    for (const auto& protocol : hosted) CHECK(is_hex64(protocol.hash));
    CHECK_FALSE(hosted[0].deprecated);
    CHECK(hosted[0].deprecation_message.empty());
    CHECK(hosted[0].features.empty());
}

TEST_CASE("describe_protocol describes any hosted protocol on every transport",
          "[reflection][transport]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    const auto transport = GENERATE(Transport::PIPE, Transport::SHM_PIPE, Transport::UNIX,
                                    Transport::TCP, Transport::HTTP, Transport::HTTP_SESSION);
    CAPTURE(server_name(server), transport_name(transport));
    SKIP_UNLESS_AVAILABLE(server);
    auto fixture = connect_to(server, transport);

    const auto hosted = fixture->conn->list_protocols();
    const auto description = fixture->conn->describe_protocol(kPrimary);
    const auto reflection = fixture->conn->describe_protocol(kReflection);
    const auto secondary = fixture->conn->describe_protocol(kSecondary);

    CHECK(description.protocol_name == kPrimary);
    CHECK(description.protocol_hash == hosted[0].hash);
    CHECK_FALSE(description.server_id.empty());
    const auto* echo = description.method("echo_string");
    REQUIRE(echo != nullptr);
    CHECK(echo->method_type == "unary");
    const auto* produce = description.method("produce_n");
    REQUIRE(produce != nullptr);
    CHECK(produce->method_type == "stream");
    CHECK(produce->is_exchange == false);

    std::set<std::string> reflection_methods;
    for (const auto& [name, method] : reflection.methods) reflection_methods.insert(name);
    CHECK(reflection_methods == std::set<std::string>{"describe", "list_protocols"});

    CHECK(secondary.protocol_name == kSecondary);
    CHECK(secondary.protocol_hash == hosted[1].hash);
}

TEST_CASE("reflection leaves the held connection usable on every transport",
          "[reflection][transport]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    const auto transport = GENERATE(Transport::PIPE, Transport::SHM_PIPE, Transport::UNIX,
                                    Transport::TCP, Transport::HTTP, Transport::HTTP_SESSION);
    CAPTURE(server_name(server), transport_name(transport));
    SKIP_UNLESS_AVAILABLE(server);
    auto fixture = connect_to(server, transport);
    auto& conn = *fixture->conn;

    CHECK(conn.echo("a") == "a");
    (void)conn.list_protocols();
    (void)conn.describe_protocol(kPrimary);
    CHECK(conn.echo("b") == "b");
    CHECK(conn.produce(3) == 3);
    CHECK(conn.list_protocols()[0].name == kPrimary);
    CHECK(conn.echo("c") == "c");
}

TEST_CASE("describe_protocol of an unhosted name is protocol_not_supported, not no-reflection",
          "[reflection][transport]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    const auto transport = GENERATE(Transport::PIPE, Transport::SHM_PIPE, Transport::UNIX,
                                    Transport::TCP, Transport::HTTP, Transport::HTTP_SESSION);
    CAPTURE(server_name(server), transport_name(transport));
    SKIP_UNLESS_AVAILABLE(server);
    auto fixture = connect_to(server, transport);

    const auto [kind, not_supported] =
        remote_error_of([&] { (void)fixture->conn->describe_protocol("nope.v1"); });
    CHECK(kind == "protocol_not_supported");
    CHECK_FALSE(not_supported);
    CHECK(fixture->conn->echo("after") == "after");
}

TEST_CASE("the client's own protocol does not matter, only its connection",
          "[reflection][transport]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    const auto transport = GENERATE(Transport::PIPE, Transport::HTTP);
    const auto protocol = GENERATE(std::string(kSecondary), std::string());
    CAPTURE(server_name(server), transport_name(transport), protocol);
    SKIP_UNLESS_AVAILABLE(server);
    auto fixture = connect_to(server, transport, /*describe=*/true, protocol);
    const auto hosted = fixture->conn->list_protocols();
    REQUIRE(hosted.size() >= 2);
    CHECK(hosted[0].name == kPrimary);
    CHECK(hosted[1].name == kSecondary);
    CHECK(fixture->conn->describe_protocol(kPrimary).protocol_name == kPrimary);
}

// --- the held connection, not a new one ---------------------------------------

namespace {

/// Counts the bytes that cross the streams the caller handed the client.
/// A pipe end as an Arrow stream, counting what crosses it. Pipes are not
/// seekable, so Arrow's file streams (which lseek on open) cannot wrap them.
class CountingOutput final : public arrow::io::OutputStream {
public:
    CountingOutput(int fd, std::atomic<int64_t>* count) : fd_(fd), count_(count) {}
    ~CountingOutput() override { (void)Close(); }
    arrow::Status Close() override {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
        return arrow::Status::OK();
    }
    bool closed() const override { return fd_ < 0; }
    arrow::Result<int64_t> Tell() const override { return count_->load(); }
    arrow::Status Write(const void* data, int64_t nbytes) override {
        const auto* bytes = static_cast<const char*>(data);
        while (nbytes > 0) {
            const ssize_t n = ::write(fd_, bytes, static_cast<size_t>(nbytes));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return arrow::Status::IOError("write failed");
            *count_ += n;
            bytes += n;
            nbytes -= n;
        }
        return arrow::Status::OK();
    }
    arrow::Status Flush() override { return arrow::Status::OK(); }

private:
    int fd_;
    std::atomic<int64_t>* count_;
};

class CountingInput final : public arrow::io::InputStream {
public:
    CountingInput(int fd, std::atomic<int64_t>* count) : fd_(fd), count_(count) {}
    ~CountingInput() override { (void)Close(); }
    arrow::Status Close() override {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
        return arrow::Status::OK();
    }
    bool closed() const override { return fd_ < 0; }
    arrow::Result<int64_t> Tell() const override { return count_->load(); }
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        auto* bytes = static_cast<char*>(out);
        int64_t total = 0;
        while (total < nbytes) {
            const ssize_t n = ::read(fd_, bytes + total, static_cast<size_t>(nbytes - total));
            if (n < 0 && errno == EINTR) continue;
            if (n < 0) return arrow::Status::IOError("read failed");
            if (n == 0) break;
            total += n;
        }
        *count_ += total;
        return total;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(const int64_t n, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(n));
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
    }

private:
    int fd_;
    std::atomic<int64_t>* count_;
};

}  // namespace

TEST_CASE("RpcClient reflection rides the streams the caller handed it",
          "[reflection][connection]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    CAPTURE(server_name(server));
    SKIP_UNLESS_AVAILABLE(server);
    int to_child = -1;
    int from_child = -1;
    const pid_t pid = spawn_process(server_argv(server, Transport::PIPE, ""), &to_child, &from_child);
    std::atomic<int64_t> written{0};
    std::atomic<int64_t> read{0};
    {
        auto output = std::make_shared<CountingOutput>(to_child, &written);
        auto input = std::make_shared<CountingInput>(from_child, &read);
        RpcClient client(ClientTransport::from_streams(input, output), raw_options());

        CHECK(string_result(client.call_unary("echo_string", string_params("x"))) == "x");
        const int64_t written_before = written.load();
        const int64_t read_before = read.load();
        const auto hosted = client.list_protocols();
        REQUIRE_FALSE(hosted.empty());
        CHECK(hosted[0].name == kPrimary);
        // The request went out on, and the reply came back over, the held pair.
        CHECK(written.load() > written_before);
        CHECK(read.load() > read_before);

        const int64_t written_mid = written.load();
        CHECK(client.describe_protocol(kPrimary).protocol_name == kPrimary);
        CHECK(written.load() > written_mid);
        CHECK(string_result(client.call_unary("echo_string", string_params("y"))) == "y");
        client.close();
    }
    reap(pid);
}

TEST_CASE("HttpClient reflection rides the client's own state", "[reflection][connection]") {
    const auto server = GENERATE(Server::CPP, Server::PYTHON);
    CAPTURE(server_name(server));
    SKIP_UNLESS_AVAILABLE(server);
    Listener listener(server_argv(server, Transport::HTTP, ""));
    const int port = announced_port(listener.line(), "PORT:");
    wait_for_port(port);

    // The credential callback is per client: a request built anywhere but on
    // this client's state never reaches it.
    std::mutex mutex;
    std::vector<std::string> paths;
    auto client = HttpClient::builder("http://127.0.0.1:" + std::to_string(port))
                      .prefix(http_prefix(server))
                      .protocol(kPrimary)
                      .protocol_version(version_of(kPrimary))
                      .auth_callback([&](const HttpAuthRequest& request) {
                          std::lock_guard<std::mutex> lock(mutex);
                          paths.push_back(request.path);
                          return std::map<std::string, std::string>{};
                      })
                      .build();
    (void)client.list_protocols();
    (void)client.describe_protocol(kPrimary);
    std::lock_guard<std::mutex> lock(mutex);
    CHECK(std::count(paths.begin(), paths.end(),
                     http_prefix(server) + "/" + kReflection + "/list_protocols") == 2);
    CHECK(std::count(paths.begin(), paths.end(), http_prefix(server) + "/" + kReflection + "/describe") ==
          1);
}

// --- a server without reflection --------------------------------------------------

TEST_CASE("a server without reflection raises ReflectionNotSupportedError and stays usable",
          "[reflection][no-reflection]") {
    // The reference server built without enable_describe; this port's server
    // always hosts reflection, so it has no such mode to test.
    const auto transport = GENERATE(Transport::PIPE, Transport::TCP, Transport::HTTP);
    CAPTURE(transport_name(transport));
    SKIP_UNLESS_AVAILABLE(Server::PYTHON);
    auto fixture = connect_to(Server::PYTHON, transport, /*describe=*/false);

    CHECK(fixture->conn->echo("before") == "before");
    try {
        (void)fixture->conn->list_protocols();
        FAIL("list_protocols inferred a listing from a server without reflection");
    } catch (const ReflectionNotSupportedError& error) {
        CHECK(error.error_kind() == "protocol_not_supported");
        CHECK(error.error_code() == "UNIMPLEMENTED");
        CHECK(std::string(error.what()).find(kReflection) != std::string::npos);
        if (transport == Transport::HTTP) {
            CHECK(error.http_status() != 0);
        } else {
            CHECK(error.http_status() == 0);
        }
    }
    CHECK(fixture->conn->echo("after") == "after");

    // describe_protocol lists first, so it reports "no reflection", not
    // "no such protocol".
    const auto [kind, not_supported] =
        remote_error_of([&] { (void)fixture->conn->describe_protocol(kPrimary); });
    CHECK(not_supported);
    CHECK(kind == "protocol_not_supported");
    CHECK(fixture->conn->echo("again") == "again");
}

TEST_CASE("an HTTP server older than protocol routes (bare 404) is ReflectionNotSupportedError",
          "[reflection][no-reflection]") {
    httplib::Server server;
    server.Options(".*", [](const httplib::Request&, httplib::Response& response) {
        response.status = 204;
        response.set_header("VGI-Accept-Max-Response-Bytes-Support", "true");
    });
    server.Post(".*", [](const httplib::Request&, httplib::Response& response) {
        response.status = 404;
        response.set_header("VGI-Accept-Max-Response-Bytes-Support", "true");
        response.set_content("Not Found", "text/plain");
    });
    const int port = server.bind_to_any_port("127.0.0.1");
    REQUIRE(port > 0);
    std::thread thread([&] { (void)server.listen_after_bind(); });
    struct Stop {
        httplib::Server& server;
        std::thread& thread;
        ~Stop() {
            server.stop();
            if (thread.joinable()) thread.join();
        }
    } stop{server, thread};
    for (int i = 0; i < 1000 && !server.is_running(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto client =
            HttpClient::builder("http://127.0.0.1:" + std::to_string(port)).protocol(kPrimary).build();
        try {
            (void)client.list_protocols();
            FAIL("a bare 404 produced a listing");
        } catch (const ReflectionNotSupportedError& error) {
            CHECK(error.http_status() == 404);
            CHECK(error.exception_type() == "HttpError");
        }
        CHECK_THROWS_AS(client.describe_protocol(kPrimary), ReflectionNotSupportedError);
        // The client is still usable: a non-reflection call reaches the server.
        CHECK_NOTHROW(client.capabilities());
    }
}

TEST_CASE("older servers' not-hosted answers are classified, nothing else is",
          "[reflection][no-reflection]") {
    using detail::reflection_not_hosted;
    CHECK(reflection_not_hosted("protocol_not_supported", "UNIMPLEMENTED", "RpcError"));
    CHECK(reflection_not_hosted("method_not_implemented", "", ""));
    CHECK(reflection_not_hosted("", "UNIMPLEMENTED", ""));
    CHECK(reflection_not_hosted("", "", "ProtocolNotSupportedError"));
    CHECK(reflection_not_hosted("", "", "MethodNotImplementedError"));
    CHECK_FALSE(reflection_not_hosted("", "", "RpcError"));
    CHECK_FALSE(reflection_not_hosted("internal", "INTERNAL", "RuntimeError"));
    CHECK_FALSE(reflection_not_hosted("", "UNAVAILABLE", "AuthUnavailableError"));
}
