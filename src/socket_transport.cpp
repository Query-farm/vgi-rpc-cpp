// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Unix-domain and TCP transports.  Both reuse the pipe transport's raw Arrow
// IPC framing with no HTTP envelope; they differ only in the listening socket,
// so the serve loop is shared and the two entry points just build the address.
//
// TCP carries no authentication and no TLS.  It is for trusted networks; the
// host defaults to loopback so a misread flag cannot bind the world.

#include "vgi_rpc/server.h"
#include "vgi_rpc/wire.h"

#include <cerrno>
#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

namespace vgi_rpc {

#ifndef _WIN32

// Serve one accepted connection to completion: request after request over the
// same socket, exactly as the pipe transport does over stdin/stdout, until the
// peer closes or the framing breaks.
void Server::serve_socket_fd(int fd, TransportKind transport_kind) {
    auto input = std::make_shared<FdInputStream>(fd);
    auto output = std::make_shared<FdOutputStream>(fd);
    ConnectionState connection;
    while (true) {
        try {
            if (!serve_one_with_state(input, output, transport_kind, connection)) break;
        } catch (const std::exception& e) {
            // One bad connection must not take the listener down; say what
            // happened and move on to the next peer.
            std::fprintf(stderr, "vgi_rpc: connection closed after error: %s\n", e.what());
            break;
        }
    }
}

namespace {

// Socket buffer asked for on an accepted Unix domain socket.  macOS gives one
// 8192 bytes by default — against ~64 KiB for a pipe — so a megabyte of Arrow
// costs 128 round trips through the kernel instead of a handful.  Best effort:
// the kernel clamps to its own maximum, and a refusal is not worth failing a
// connection over.
//
// Only Unix.  TCP already starts at 128 KiB and grows, setting SO_RCVBUF on it
// *disables* Linux's receive-window auto-tuning and pins the window at
// whatever we guessed, and measuring it on loopback showed no gain either way.
constexpr int kUnixSocketBufferBytes = 1 << 20;

void widen_socket_buffers(int fd) {
    const int size = kUnixSocketBufferBytes;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
}

// Persistent raw connections need dedicated execution slots: serving an
// accepted fd inline would prevent accept() from ever reaching the other
// clients until that first connection closed.  Bound both the workers and the
// pending queue so an unreachable or malicious peer cannot create an
// unbounded number of threads or retained descriptors.
class SocketConnectionPool {
public:
    explicit SocketConnectionPool(std::function<void(int)> serve) : serve_(std::move(serve)) {
        const unsigned detected = std::thread::hardware_concurrency();
        worker_count_ = detected == 0 ? 8u : std::max(2u, detected);
        max_pending_ = worker_count_ * 4;
        workers_.reserve(worker_count_);
        for (unsigned i = 0; i < worker_count_; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    SocketConnectionPool(const SocketConnectionPool&) = delete;
    SocketConnectionPool& operator=(const SocketConnectionPool&) = delete;

    ~SocketConnectionPool() { stop(); }

    bool submit(int fd) {
        std::unique_lock lock(mutex_);
        space_available_.wait(lock, [this] { return stopping_ || pending_.size() < max_pending_; });
        if (stopping_) return false;
        pending_.push_back(fd);
        work_available_.notify_one();
        return true;
    }

private:
    void worker_loop() {
        while (true) {
            int fd;
            {
                std::unique_lock lock(mutex_);
                work_available_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
                if (stopping_ && pending_.empty()) return;
                fd = pending_.front();
                pending_.pop_front();
                active_.insert(fd);
                space_available_.notify_one();
            }

            serve_(fd);
            ::close(fd);

            {
                std::lock_guard lock(mutex_);
                active_.erase(fd);
            }
        }
    }

    void stop() {
        std::vector<int> queued;
        std::vector<int> active;
        {
            std::lock_guard lock(mutex_);
            if (stopping_) return;
            stopping_ = true;
            queued.assign(pending_.begin(), pending_.end());
            pending_.clear();
            active.assign(active_.begin(), active_.end());
        }
        for (int fd : queued) ::close(fd);
        for (int fd : active) ::shutdown(fd, SHUT_RDWR);
        work_available_.notify_all();
        space_available_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }

    std::function<void(int)> serve_;
    unsigned worker_count_ = 0;
    size_t max_pending_ = 0;
    std::mutex mutex_;
    std::condition_variable work_available_;
    std::condition_variable space_available_;
    std::deque<int> pending_;
    std::unordered_set<int> active_;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};

// `is_tcp` picks the per-socket tuning: Nagle off for TCP, wider buffers for a
// Unix socket.  Neither setting means anything on the other family.
void accept_loop(int listen_fd, TransportKind transport_kind, std::function<void(int)> serve) {
    const bool is_tcp = transport_kind == TransportKind::TCP;
    SocketConnectionPool connections(std::move(serve));
    while (true) {
        int fd = ::accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "vgi_rpc: accept failed: %s\n", std::strerror(errno));
            break;
        }
        if (!is_tcp) {
            widen_socket_buffers(fd);
        } else {
            // Request/response over small writes is the shape Nagle exists to
            // coalesce, so it holds a reply back waiting for more to send —
            // and against a peer's delayed ACK that becomes the classic
            // tens-of-milliseconds stall.
            //
            // Measured on loopback this is a ~14% *loss* (tcp/pipe latency
            // ratio 2.34 -> 2.66, ratio rather than absolute because the
            // machine drifts more than the effect). That is loopback telling
            // on itself: with instant ACKs Nagle's downside never arrives,
            // while its coalescing still saves per-segment work. The stall it
            // prevents needs a real link to appear, and a real link is what
            // this transport is for.
            int one = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        }
        if (!connections.submit(fd)) {
            ::close(fd);
            break;
        }
    }
    ::close(listen_fd);
}

}  // namespace

void Server::serve_unix(const std::string& path) {
    // A leftover socket file from an unclean exit would make bind fail with
    // EADDRINUSE even though nothing is listening.
    ::unlink(path.c_str());

    int listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        throw std::runtime_error(std::string("cannot create unix socket: ") + std::strerror(errno));
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path.size() >= sizeof(addr.sun_path)) {
        ::close(listen_fd);
        throw std::runtime_error("unix socket path is too long: " + path);
    }
    std::memcpy(addr.sun_path, path.c_str(), path.size());

    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd);
        throw std::runtime_error("cannot bind " + path + ": " + std::strerror(errno));
    }
    if (::listen(listen_fd, 16) < 0) {
        ::close(listen_fd);
        throw std::runtime_error("cannot listen on " + path + ": " + std::strerror(errno));
    }

    try {
        notify_serve_start(TransportKind::UNIX);
    } catch (...) {
        ::close(listen_fd);
        ::unlink(path.c_str());
        throw;
    }

    // Discovery line, then flush: a launcher blocks on this to learn the
    // socket is ready, so buffering it would look like a hung worker.
    std::cout << "UNIX:" << path << std::endl;
    accept_loop(listen_fd, TransportKind::UNIX,
                [this](int fd) { serve_socket_fd(fd, TransportKind::UNIX); });
    ::unlink(path.c_str());
}

void Server::serve_tcp(const std::string& host, int port) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        throw std::runtime_error(std::string("cannot create tcp socket: ") + std::strerror(errno));
    }
    int one = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    const std::string bind_host = host.empty() ? "127.0.0.1" : host;
    if (::inet_pton(AF_INET, bind_host.c_str(), &addr.sin_addr) != 1) {
        ::close(listen_fd);
        throw std::runtime_error("not an IPv4 address: " + bind_host);
    }

    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd);
        throw std::runtime_error("cannot bind " + bind_host + ":" + std::to_string(port) + ": " +
                                 std::strerror(errno));
    }
    if (::listen(listen_fd, 16) < 0) {
        ::close(listen_fd);
        throw std::runtime_error(std::string("cannot listen: ") + std::strerror(errno));
    }

    try {
        notify_serve_start(TransportKind::TCP);
    } catch (...) {
        ::close(listen_fd);
        throw;
    }

    // Report the port actually bound, so a caller that asked for 0 learns it.
    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    int bound_port = port;
    if (::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
        bound_port = ntohs(bound.sin_port);
    }

    std::cout << "TCP:" << bind_host << ":" << bound_port << std::endl;
    accept_loop(listen_fd, TransportKind::TCP,
                [this](int fd) { serve_socket_fd(fd, TransportKind::TCP); });
}

#else  // _WIN32

void Server::serve_unix(const std::string&) {
    throw std::runtime_error("unix socket transport is not available on Windows");
}

void Server::serve_tcp(const std::string&, int) {
    throw std::runtime_error("tcp transport is not available on Windows");
}

#endif  // _WIN32

}  // namespace vgi_rpc
