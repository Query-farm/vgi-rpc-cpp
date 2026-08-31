// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Unix-domain and TCP transports.  Both reuse the pipe transport's raw Arrow
// IPC framing with no HTTP envelope; they differ only in the listening socket,
// so the serve loop is shared and the two entry points just build the address.
//
// TCP carries no TLS. It is for trusted networks or a separately secured
// channel; TcpServerOptions can attach a connection-snapshot identity resolved
// directly or from a trusted PROXY v2 assertion. The host defaults to loopback
// so a misread flag cannot bind the world.

#include "vgi_rpc/server.h"
#include "vgi_rpc/proxy_protocol_v2.h"
#include "vgi_rpc/wire.h"
#include "socket_transport_internal.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
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
void Server::serve_socket_fd(int fd, TransportKind transport_kind, const AuthContext& auth,
                             const PeerEvidenceSet& peer_evidence,
                             std::chrono::steady_clock::time_point first_frame_deadline,
                             std::chrono::milliseconds idle_read_timeout,
                             std::chrono::milliseconds write_timeout) {
    const bool deadlines_enabled =
        first_frame_deadline != std::chrono::steady_clock::time_point::max() ||
        idle_read_timeout != std::chrono::milliseconds::max() ||
        write_timeout != std::chrono::milliseconds::max();
    std::shared_ptr<arrow::io::InputStream> input;
    std::shared_ptr<socket_detail::DeadlineFdInputStream> deadline_input;
    if (deadlines_enabled) {
        deadline_input = std::make_shared<socket_detail::DeadlineFdInputStream>(
            fd, first_frame_deadline, idle_read_timeout);
        input = deadline_input;
    } else {
        input = std::make_shared<FdInputStream>(fd);
    }
    std::shared_ptr<arrow::io::OutputStream> output;
    if (deadlines_enabled)
        output = std::make_shared<socket_detail::DeadlineFdOutputStream>(fd, write_timeout);
    else
        output = std::make_shared<FdOutputStream>(fd);
    ConnectionState connection;
    while (true) {
        try {
            if (!serve_one_with_state(input, output, transport_kind, connection, auth,
                                      peer_evidence,
                                      deadline_input ? std::function<void()>([deadline_input] {
                                          deadline_input->mark_first_frame_complete();
                                      })
                                                     : std::function<void()>{}))
                break;
        } catch (const std::exception&) {
            // Provider, callback, and framing errors may contain credentials
            // or peer-controlled bytes. Keep the operational event while
            // deliberately omitting exception text from normal logs.
            std::fprintf(stderr, "vgi_rpc: connection closed after transport error\n");
            break;
        }
    }
}

namespace {

using socket_detail::AcceptedPeer;

std::string socket_endpoint(const std::string& address, uint16_t port) {
    return address.find(':') == std::string::npos ? address + ":" + std::to_string(port)
                                                  : "[" + address + "]:" + std::to_string(port);
}

AcceptedPeer accepted_peer(const sockaddr_storage& storage) {
    std::array<char, INET6_ADDRSTRLEN> buffer{};
    uint16_t port = 0;
    const void* address = nullptr;
    if (storage.ss_family == AF_INET) {
        const auto& value = reinterpret_cast<const sockaddr_in&>(storage);
        address = &value.sin_addr;
        port = ntohs(value.sin_port);
    } else if (storage.ss_family == AF_INET6) {
        const auto& value = reinterpret_cast<const sockaddr_in6&>(storage);
        address = &value.sin6_addr;
        port = ntohs(value.sin6_port);
    } else {
        return {};
    }
    if (::inet_ntop(storage.ss_family, address, buffer.data(), buffer.size()) == nullptr) return {};
    std::string text(buffer.data());
    constexpr std::string_view mapped = "::ffff:";
    if (text.rfind(mapped, 0) == 0) text.erase(0, mapped.size());
    return {text, socket_endpoint(text, port), std::chrono::steady_clock::now()};
}

std::string local_endpoint(int fd) {
    sockaddr_storage storage{};
    socklen_t size = sizeof(storage);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&storage), &size) != 0) return {};
    return accepted_peer(storage).endpoint;
}

std::string normalize_ip(const std::string& value) {
    std::array<uint8_t, 16> bytes{};
    std::array<char, INET6_ADDRSTRLEN> output{};
    if (::inet_pton(AF_INET, value.c_str(), bytes.data()) == 1) {
        if (::inet_ntop(AF_INET, bytes.data(), output.data(), output.size()) == nullptr) return {};
        return output.data();
    }
    if (::inet_pton(AF_INET6, value.c_str(), bytes.data()) == 1) {
        if (::inet_ntop(AF_INET6, bytes.data(), output.data(), output.size()) == nullptr) return {};
        std::string result(output.data());
        constexpr std::string_view mapped = "::ffff:";
        if (result.rfind(mapped, 0) == 0) result.erase(0, mapped.size());
        return result;
    }
    return {};
}

void read_exact_until(int fd, std::span<uint8_t> output,
                      std::chrono::steady_clock::time_point deadline) {
    size_t offset = 0;
    while (offset < output.size()) {
        const auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::steady_clock::duration::zero())
            throw ProxyProtocolV2Error("PROXY v2 preamble timed out");
        const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
        pollfd ready{fd, POLLIN, 0};
        int status;
        do {
            const int timeout = static_cast<int>(
                std::clamp<int64_t>(millis.count(), 1, std::numeric_limits<int>::max()));
            status = ::poll(&ready, 1, timeout);
        } while (status < 0 && errno == EINTR);
        if (status == 0) throw ProxyProtocolV2Error("PROXY v2 preamble timed out");
        if (status < 0)
            throw ProxyProtocolV2Error(std::string("cannot read PROXY v2 preamble: ") +
                                       std::strerror(errno));
        ssize_t count;
        do {
            count = ::recv(fd, output.data() + offset, output.size() - offset, 0);
        } while (count < 0 && errno == EINTR);
        if (count <= 0) throw ProxyProtocolV2Error("truncated PROXY v2 preamble");
        offset += static_cast<size_t>(count);
    }
}

ProxyProtocolV2Address read_proxy_protocol_v2(
    int fd, const TcpServerOptions& options,
    std::chrono::steady_clock::time_point connection_setup_deadline) {
    const auto deadline = std::min(socket_detail::deadline_after(std::chrono::steady_clock::now(),
                                                                 options.proxy_preamble_timeout),
                                   connection_setup_deadline);
    std::array<uint8_t, 16> prefix{};
    read_exact_until(fd, prefix, deadline);
    const size_t total = proxy_protocol_v2_size(prefix, options.maximum_proxy_preamble_bytes);
    std::vector<uint8_t> preamble(total);
    std::copy(prefix.begin(), prefix.end(), preamble.begin());
    read_exact_until(fd, std::span<uint8_t>(preamble).subspan(prefix.size()), deadline);
    return parse_proxy_protocol_v2(preamble, options.maximum_proxy_preamble_bytes);
}

TcpServerOptions::ResolvedIdentity resolve_tcp_identity(
    int fd, const AcceptedPeer& peer, const TcpServerOptions& options,
    std::chrono::steady_clock::time_point connection_setup_deadline) {
    PeerResolutionContext context;
    context.transport = "tcp";
    if (!peer.address.empty()) context.immediate_peer = normalize_ip(peer.address);
    if (!peer.endpoint.empty()) context.source_endpoint = peer.endpoint;
    context.destination_address = local_endpoint(fd);
    if (!options.service_name.empty()) context.service_name = options.service_name;

    if (options.proxy_protocol_v2_required) {
        const std::string normalized_peer = normalize_ip(peer.address);
        const bool trusted =
            std::any_of(options.trusted_proxy_addresses.begin(),
                        options.trusted_proxy_addresses.end(), [&](const std::string& configured) {
                            return normalize_ip(configured) == normalized_peer;
                        });
        if (normalized_peer.empty() || !trusted)
            throw ProxyProtocolV2Error("immediate peer is not a trusted PROXY v2 sender");
        const auto asserted = read_proxy_protocol_v2(fd, options, connection_setup_deadline);
        context.asserted_peer = socket_endpoint(asserted.source_address, asserted.source_port);
        context.destination_address =
            socket_endpoint(asserted.destination_address, asserted.destination_port);
    }

    context.deadline = std::min(socket_detail::deadline_after(std::chrono::steady_clock::now(),
                                                              options.identity_resolution_timeout),
                                connection_setup_deadline);
    context.validate();
    if (context.remaining_budget() <= std::chrono::steady_clock::duration::zero())
        throw PeerIdentityUnavailable("connection setup budget exhausted");
    return options.resolve_identity ? options.resolve_identity(context)
                                    : TcpServerOptions::ResolvedIdentity{};
}

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

// `is_tcp` picks the per-socket tuning: Nagle off for TCP, wider buffers for a
// Unix socket.  Neither setting means anything on the other family.
void accept_loop(int listen_fd, TransportKind transport_kind, size_t maximum_active_connections,
                 size_t maximum_pending_connections,
                 std::function<void(int, const AcceptedPeer&)> serve) {
    const bool is_tcp = transport_kind == TransportKind::TCP;
    socket_detail::SocketConnectionPool connections(
        maximum_active_connections, maximum_pending_connections, std::move(serve),
        [](socket_detail::ConnectionFailure) {
            // Never include callback/provider/framing exception text here: it
            // may contain credentials or attacker-controlled bytes.
            std::fprintf(stderr, "vgi_rpc: connection rejected\n");
        });
    while (true) {
        sockaddr_storage peer_storage{};
        socklen_t peer_size = sizeof(peer_storage);
        int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&peer_storage), &peer_size);
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
        const auto peer = accepted_peer(peer_storage);
        if (!connections.submit(fd, peer)) {
            ::close(fd);
            // Saturation is connection-local. Keep accepting so a peer that
            // arrives after a permit is released can make progress.
            continue;
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
    accept_loop(listen_fd, TransportKind::UNIX, 32, 128,
                [this](int fd, const AcceptedPeer&) { serve_socket_fd(fd, TransportKind::UNIX); });
    ::unlink(path.c_str());
}

void Server::serve_tcp(const std::string& host, int port) {
    serve_tcp(host, port, TcpServerOptions{});
}

void Server::serve_tcp(const std::string& host, int port, const TcpServerOptions& options) {
    if (options.maximum_active_connections == 0 || options.maximum_pending_connections == 0 ||
        options.maximum_active_connections >
            std::numeric_limits<size_t>::max() - options.maximum_pending_connections ||
        options.connection_setup_timeout <= std::chrono::milliseconds::zero() ||
        options.idle_read_timeout <= std::chrono::milliseconds::zero() ||
        options.write_timeout <= std::chrono::milliseconds::zero() ||
        options.proxy_preamble_timeout <= std::chrono::milliseconds::zero() ||
        options.identity_resolution_timeout <= std::chrono::milliseconds::zero() ||
        options.maximum_proxy_preamble_bytes < 16)
        throw std::invalid_argument("TCP admission, timeout, and framing limits must be positive");
    if (options.proxy_protocol_v2_required && options.trusted_proxy_addresses.empty())
        throw std::invalid_argument("PROXY v2 requires at least one exact trusted proxy address");
    std::unordered_set<std::string> normalized_proxies;
    for (const auto& address : options.trusted_proxy_addresses) {
        const auto normalized = normalize_ip(address);
        if (normalized.empty())
            throw std::invalid_argument(
                "trusted proxy address is not an exact IPv4 or IPv6 address");
        if (!normalized_proxies.insert(normalized).second)
            throw std::invalid_argument("duplicate trusted proxy address");
    }

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
    const size_t admission_capacity =
        options.maximum_active_connections + options.maximum_pending_connections;
    const int listen_backlog =
        static_cast<int>(std::min<size_t>(admission_capacity, static_cast<size_t>(SOMAXCONN)));
    if (::listen(listen_fd, listen_backlog) < 0) {
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
    accept_loop(
        listen_fd, TransportKind::TCP, options.maximum_active_connections,
        options.maximum_pending_connections, [this, options](int fd, const AcceptedPeer& peer) {
            const auto setup_deadline =
                socket_detail::deadline_after(peer.accepted_at, options.connection_setup_timeout);
            const auto identity = resolve_tcp_identity(fd, peer, options, setup_deadline);
            serve_socket_fd(fd, TransportKind::TCP, identity.auth, identity.evidence,
                            setup_deadline, options.idle_read_timeout, options.write_timeout);
        });
}

#else  // _WIN32

void Server::serve_unix(const std::string&) {
    throw std::runtime_error("unix socket transport is not available on Windows");
}

void Server::serve_tcp(const std::string&, int) {
    throw std::runtime_error("tcp transport is not available on Windows");
}

void Server::serve_tcp(const std::string&, int, const TcpServerOptions&) {
    throw std::runtime_error("tcp transport is not available on Windows");
}

#endif  // _WIN32

}  // namespace vgi_rpc
