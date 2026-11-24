// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Storage HTTP timeouts: a slow transfer that keeps moving finishes in one
// attempt however long it takes, and a transfer where no bytes move fails
// within the stall timeout.  A loopback server trickles a GET body, drains a
// PUT body slowly through a small receive buffer, or goes silent.  The stall
// timeout is 3 s and each slow transfer takes about 6.4 s, longer than any
// whole-request deadline of that size would allow.  The PUT is 16 MiB so the
// client's kernel send buffer (up to 4 MiB on loopback) drains well inside
// the stall timeout.

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/external.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <strings.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

using namespace vgi_rpc;

namespace {

constexpr size_t kSlowGetBytes = 512 * 1024;
constexpr size_t kSlowPutBytes = 16 * 1024 * 1024;

std::atomic<int> slow_get{0}, slow_put{0}, stall_get{0}, stall_put{0}, flaky{0};
std::atomic<long> put_received{0};

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

bool send_all(int fd, const char* data, size_t n) {
    while (n > 0) {
        const ssize_t w = ::send(fd, data, n, 0);
        if (w <= 0) return false;
        data += w;
        n -= static_cast<size_t>(w);
    }
    return true;
}

void respond(int fd, const std::string& status, const std::string& body,
             const std::string& type = "text/plain") {
    const std::string head = "HTTP/1.1 " + status + "\r\nContent-Type: " + type +
                             "\r\nContent-Length: " + std::to_string(body.size()) +
                             "\r\nConnection: close\r\n\r\n";
    send_all(fd, head.data(), head.size());
    send_all(fd, body.data(), body.size());
}

class SlowServer {
public:
    SlowServer() {
        std::signal(SIGPIPE, SIG_IGN);
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(listener_ >= 0);
        int one = 1, small = 16 * 1024;
        ::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        // Inherited by accepted sockets: keeps the slow PUT out of a
        // multi-megabyte loopback buffer, so the client's writes wait on us.
        ::setsockopt(listener_, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE(::bind(listener_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        REQUIRE(::listen(listener_, 16) == 0);
        socklen_t size = sizeof(address);
        REQUIRE(::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &size) == 0);
        port_ = ntohs(address.sin_port);
        std::thread([this] { accept_loop(); }).detach();
    }

    std::string base() const { return "http://127.0.0.1:" + std::to_string(port_); }

private:
    void accept_loop() {
        for (;;) {
            const int fd = ::accept(listener_, nullptr, nullptr);
            if (fd < 0) {
                if (errno == EINTR) continue;
                return;
            }
            std::thread([this, fd] { serve(fd); }).detach();
        }
    }

    void serve(int fd) {
        std::string head;
        char buffer[48 * 1024];
        size_t header_end = std::string::npos;
        while (header_end == std::string::npos && head.size() < 65536) {
            const ssize_t r = ::recv(fd, buffer, sizeof(buffer), 0);
            if (r <= 0) {
                ::close(fd);
                return;
            }
            head.append(buffer, static_cast<size_t>(r));
            header_end = head.find("\r\n\r\n");
        }
        if (header_end == std::string::npos) {
            ::close(fd);
            return;
        }
        const std::string method = head.substr(0, head.find(' '));
        const size_t path_start = method.size() + 1;
        const std::string path = head.substr(path_start, head.find(' ', path_start) - path_start);
        long length = 0;
        for (size_t i = 0; i < header_end; ++i) {
            if (::strncasecmp(head.c_str() + i, "\r\nContent-Length:", 17) == 0) {
                length = std::strtol(head.c_str() + i + 17, nullptr, 10);
            }
        }
        long have = static_cast<long>(head.size() - header_end - 4);

        if (path == "/alloc") {
            respond(fd, "200 OK",
                    "{\"object_url\":\"" + base() + "/slow-put\",\"object_id\":\"obj-1\"}",
                    "application/json");
        } else if (path == "/slow-get" || path == "/objects/slow-get") {
            ++slow_get;
            const std::string line = "HTTP/1.1 200 OK\r\nContent-Length: " +
                                     std::to_string(kSlowGetBytes) + "\r\n\r\n";
            send_all(fd, line.data(), line.size());
            // 512 KiB in 8 KiB slices every 100 ms: about 6.4 s.
            const std::string chunk(kSlowGetBytes / 64, 'g');
            for (size_t sent = 0; sent < kSlowGetBytes;) {
                const size_t n = std::min(chunk.size(), kSlowGetBytes - sent);
                if (!send_all(fd, chunk.data(), n)) break;
                sent += n;
                sleep_ms(100);
            }
        } else if (path == "/slow-put") {
            ++slow_put;
            // 256 KiB every 100 ms: about 6.4 s for 16 MiB.
            long tick = 0;
            while (have < length) {
                const ssize_t r = ::recv(fd, buffer, sizeof(buffer), 0);
                if (r <= 0) {
                    ::close(fd);
                    return;
                }
                have += r;
                tick += r;
                if (tick >= 256 * 1024) {
                    tick = 0;
                    sleep_ms(100);
                }
            }
            put_received = have;
            respond(fd, "200 OK", "");
        } else if (path == "/stall-get") {
            ++stall_get;
            const std::string line =
                "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\npartial";
            send_all(fd, line.data(), line.size());
            char c;
            while (::recv(fd, &c, 1, 0) > 0) {
            }
        } else if (path == "/flaky") {
            const int n = flaky++;
            respond(fd, n == 0 ? "503 Service Unavailable" : "200 OK", n == 0 ? "busy" : "ok");
        } else {
            respond(fd, "404 Not Found", "");
        }
        ::close(fd);
    }

    int listener_ = -1;
    int port_ = 0;
};

SlowServer& server() {
    static SlowServer instance;
    return instance;
}

double seconds_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

ExternalStorageConfig config() {
    ExternalStorageConfig c;
    c.uri = server().base();
    c.stall_timeout_seconds = 3;
    return c;
}

}  // namespace

TEST_CASE("storage stall timeout reads VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS") {
    ::unsetenv(STORAGE_STALL_TIMEOUT_ENV);
    CHECK(storage_stall_timeout_seconds() == 30);
    ::setenv(STORAGE_STALL_TIMEOUT_ENV, "7", 1);
    CHECK(storage_stall_timeout_seconds() == 7);
    ::setenv(STORAGE_STALL_TIMEOUT_ENV, "0", 1);
    CHECK(storage_stall_timeout_seconds() == 30);
    ::setenv(STORAGE_STALL_TIMEOUT_ENV, "-5", 1);
    CHECK(storage_stall_timeout_seconds() == 30);
    ::setenv(STORAGE_STALL_TIMEOUT_ENV, "soon", 1);
    CHECK(storage_stall_timeout_seconds() == 30);
    ::unsetenv(STORAGE_STALL_TIMEOUT_ENV);
}

TEST_CASE("a slow download that keeps moving finishes in one attempt") {
    auto storage = make_external_storage(config());
    const auto start = std::chrono::steady_clock::now();
    const std::string body = storage->fetch(server().base() + "/slow-get");
    const double elapsed = seconds_since(start);
    CHECK(body.size() == kSlowGetBytes);
    CHECK(elapsed > 4.5);  // well past the 3 s stall timeout
    CHECK(slow_get == 1);
}

TEST_CASE("a slow upload that keeps moving finishes in one attempt") {
    auto storage = make_external_storage(config());
    const std::string payload(kSlowPutBytes, 'p');
    const auto start = std::chrono::steady_clock::now();
    storage->upload(payload, "");
    const double elapsed = seconds_since(start);
    CHECK(elapsed > 4.5);
    CHECK(slow_put == 1);
    CHECK(put_received == static_cast<long>(kSlowPutBytes));
}

TEST_CASE("a transient 503 is retried") {
    auto storage = make_external_storage(config());
    CHECK(storage->fetch(server().base() + "/flaky") == "ok");
    CHECK(flaky == 2);
}

TEST_CASE("a stalled download fails within the stall timeout") {
    auto c = config();
    c.max_attempts = 1;
    auto storage = make_external_storage(c);
    auto start = std::chrono::steady_clock::now();
    CHECK_THROWS(storage->fetch(server().base() + "/stall-get"));
    double elapsed = seconds_since(start);
    CHECK(elapsed >= 2.5);
    CHECK(elapsed < 3 + 3);
    CHECK(stall_get == 1);

    // A stall is transient: the default policy tries three times.
    c.max_attempts = 3;
    storage = make_external_storage(c);
    start = std::chrono::steady_clock::now();
    CHECK_THROWS(storage->fetch(server().base() + "/stall-get"));
    elapsed = seconds_since(start);
    CHECK(stall_get == 4);
    CHECK(elapsed < 3 * (3 + 3) + 2);
}
