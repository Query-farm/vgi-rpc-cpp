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
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

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

namespace {

// Serve one accepted connection to completion: request after request over the
// same socket, exactly as the pipe transport does over stdin/stdout, until the
// peer closes or the framing breaks.
void serve_connection(Server& server, int fd) {
    auto input = std::make_shared<FdInputStream>(fd);
    auto output = std::make_shared<FdOutputStream>(fd);
    while (true) {
        try {
            if (!server.serve_one(input, output)) break;
        } catch (const std::exception& e) {
            // One bad connection must not take the listener down; say what
            // happened and move on to the next peer.
            std::fprintf(stderr, "vgi_rpc: connection closed after error: %s\n", e.what());
            break;
        }
    }
}

// Accept and serve connections one at a time, forever.  Sequential by design:
// the framework's dispatch model is single-threaded, and a concurrent listener
// would need a lock around every handler that bought nothing.
void accept_loop(Server& server, int listen_fd) {
    while (true) {
        int fd = ::accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "vgi_rpc: accept failed: %s\n", std::strerror(errno));
            break;
        }
        serve_connection(server, fd);
        ::close(fd);
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
        throw std::runtime_error(std::string("cannot create unix socket: ") +
                                 std::strerror(errno));
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

    // Discovery line, then flush: a launcher blocks on this to learn the
    // socket is ready, so buffering it would look like a hung worker.
    std::cout << "UNIX:" << path << std::endl;
    accept_loop(*this, listen_fd);
    ::unlink(path.c_str());
}

void Server::serve_tcp(const std::string& host, int port) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        throw std::runtime_error(std::string("cannot create tcp socket: ") +
                                 std::strerror(errno));
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
        throw std::runtime_error("cannot bind " + bind_host + ":" + std::to_string(port) +
                                 ": " + std::strerror(errno));
    }
    if (::listen(listen_fd, 16) < 0) {
        ::close(listen_fd);
        throw std::runtime_error(std::string("cannot listen: ") + std::strerror(errno));
    }

    // Report the port actually bound, so a caller that asked for 0 learns it.
    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    int bound_port = port;
    if (::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
        bound_port = ntohs(bound.sin_port);
    }

    std::cout << "TCP:" << bind_host << ":" << bound_port << std::endl;
    accept_loop(*this, listen_fd);
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
