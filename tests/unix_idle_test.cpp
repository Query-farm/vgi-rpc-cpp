// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The launcher worker contract's idle self-termination (vgi_rpc.launcher in
// the reference): serve_unix with an idle timeout returns once no client has
// been connected for that long, and unlinks its socket.  Without it a worker
// the launcher spawned with --idle-timeout lived until the host rebooted.

#include <catch2/catch_test_macros.hpp>

#ifndef _WIN32

#include "../src/socket_transport_internal.h"
#include "vgi_rpc/server.h"

#include <arrow/type.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

std::string socket_path(const char* label) {
    static std::atomic<int> counter{0};
    return "/tmp/vgi-idle-" + std::string(label) + "-" + std::to_string(::getpid()) + "-" +
           std::to_string(counter++) + ".sock";
}

std::unique_ptr<vgi_rpc::Server> make_server() {
    vgi_rpc::ServerBuilder builder;
    builder.add_void("noop", arrow::schema(arrow::FieldVector{}),
                     [](const vgi_rpc::Request&, vgi_rpc::CallContext&) {});
    return builder.build();
}

bool path_exists(const std::string& path) {
    struct stat entry{};
    return ::lstat(path.c_str(), &entry) == 0;
}

// Connect once the listener is up; -1 if it never comes up.
int connect_when_listening(const std::string& path) {
    const auto deadline = Clock::now() + 5s;
    while (Clock::now() < deadline) {
        const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::memcpy(addr.sun_path, path.c_str(), path.size());
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) return fd;
        ::close(fd);
        std::this_thread::sleep_for(5ms);
    }
    return -1;
}

struct RunningServer {
    std::unique_ptr<vgi_rpc::Server> server = make_server();
    std::future<void> done;

    RunningServer(const std::string& path, vgi_rpc::UnixServerOptions options) {
        done = std::async(std::launch::async,
                          [this, path, options] { server->serve_unix(path, options); });
    }

    bool exited_within(std::chrono::milliseconds limit) {
        return done.wait_for(limit) == std::future_status::ready;
    }
};

vgi_rpc::UnixServerOptions short_idle(std::chrono::milliseconds timeout) {
    vgi_rpc::UnixServerOptions options;
    options.idle_timeout = timeout;
    options.startup_grace = timeout;
    return options;
}

}  // namespace

TEST_CASE("unix idle: exits after the timeout when no client ever connects", "[unix][idle]") {
    const auto path = socket_path("never");
    const auto started = Clock::now();
    RunningServer running(path, short_idle(200ms));

    REQUIRE(running.exited_within(5s));
    running.done.get();  // returned normally, not by throwing
    REQUIRE(Clock::now() - started >= 200ms);
    REQUIRE_FALSE(path_exists(path));
}

TEST_CASE("unix idle: a connected client pauses the clock", "[unix][idle]") {
    const auto path = socket_path("held");
    RunningServer running(path, short_idle(150ms));
    const int client = connect_when_listening(path);
    REQUIRE(client >= 0);

    // Four idle timeouts with the client connected: still serving.
    REQUIRE_FALSE(running.exited_within(600ms));
    REQUIRE(path_exists(path));

    ::close(client);
    REQUIRE(running.exited_within(5s));
    running.done.get();
    REQUIRE_FALSE(path_exists(path));
}

TEST_CASE("unix idle: the clock restarts when the last client disconnects", "[unix][idle]") {
    const auto path = socket_path("restart");
    RunningServer running(path, short_idle(300ms));
    const int first = connect_when_listening(path);
    const int second = connect_when_listening(path);
    REQUIRE(first >= 0);
    REQUIRE(second >= 0);

    // One of two leaving is not idle.
    ::close(first);
    REQUIRE_FALSE(running.exited_within(600ms));

    // The last one leaving starts a full timeout, measured from then -- not
    // from bind, and not from the first disconnect.
    ::close(second);
    const auto last_left = Clock::now();
    REQUIRE(running.exited_within(5s));
    REQUIRE(Clock::now() - last_left >= 250ms);
    REQUIRE_FALSE(path_exists(path));
}

TEST_CASE("unix idle: the startup grace defaults to max(idle_timeout, 60s)", "[unix][idle]") {
    using vgi_rpc::socket_detail::default_startup_grace;
    REQUIRE(default_startup_grace(1s) == 60s);
    REQUIRE(default_startup_grace(300s) == 300s);

    // Before any client the grace governs; after one, the idle timeout.
    using vgi_rpc::socket_detail::idle_deadline;
    using vgi_rpc::socket_detail::IdleSnapshot;
    const auto t0 = Clock::time_point{} + 1h;
    REQUIRE(idle_deadline(IdleSnapshot{0, false, {}}, t0, 1s, 60s) == t0 + 60s);
    REQUIRE(idle_deadline(IdleSnapshot{0, true, t0 + 5s}, t0, 1s, 60s) == t0 + 6s);
    REQUIRE_FALSE(idle_deadline(IdleSnapshot{1, true, t0}, t0, 1s, 60s).has_value());
}

TEST_CASE("unix idle: negative durations are refused before binding", "[unix][idle]") {
    const auto path = socket_path("negative");
    auto server = make_server();
    vgi_rpc::UnixServerOptions negative_timeout;
    negative_timeout.idle_timeout = -1ms;
    REQUIRE_THROWS_AS(server->serve_unix(path, negative_timeout), std::invalid_argument);
    vgi_rpc::UnixServerOptions negative_grace;
    negative_grace.idle_timeout = 1s;
    negative_grace.startup_grace = -1ms;
    REQUIRE_THROWS_AS(server->serve_unix(path, negative_grace), std::invalid_argument);
    REQUIRE_FALSE(path_exists(path));
}

#endif  // !_WIN32
