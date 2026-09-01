// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/tailscale_identity.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <stdexcept>
#include <string_view>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif

#include "identity_provider_internal.h"

namespace vgi_rpc {
namespace {
using namespace identity_internal;
using Clock = std::chrono::steady_clock;

constexpr const char* kProvider = "tailscale";
constexpr const char* kLocalApiHost = "local-tailscaled.sock";
constexpr const char* kDefaultSocket = "/var/run/tailscale/tailscaled.sock";

std::string trim_ascii(std::string value) {
    const auto keep = [](unsigned char c) { return c != ' ' && c != '\t'; };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), keep));
    value.erase(std::find_if(value.rbegin(), value.rend(), keep).base(), value.end());
    return value;
}

bool valid_issuer(const std::string& value) {
    return !value.empty() && valid_utf8(value) && !has_control(value);
}

std::string decode_q_word_body(const std::string& body) {
    std::string out;
    out.reserve(body.size());
    auto hex = [](unsigned char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < body.size(); ++i) {
        if (body[i] == '_') {
            out.push_back(' ');
        } else if (body[i] == '=') {
            if (i + 2 >= body.size() || hex(body[i + 1]) < 0 || hex(body[i + 2]) < 0)
                throw std::invalid_argument("invalid RFC 2047 Q escape");
            out.push_back(char((hex(body[i + 1]) << 4) | hex(body[i + 2])));
            i += 2;
        } else {
            const auto c = static_cast<unsigned char>(body[i]);
            if (c > 0x7f || c == '?') throw std::invalid_argument("invalid RFC 2047 Q body");
            out.push_back(char(c));
        }
    }
    return out;
}

std::string decode_serve_header(const std::string& value, size_t max_bytes) {
    if (value.size() > max_bytes || !valid_utf8(value) || has_control(value) ||
        std::any_of(value.begin(), value.end(), [](unsigned char c) { return c > 0x7f; }))
        throw std::invalid_argument("invalid or oversized Tailscale Serve header");
    if (value.rfind("=?", 0) != 0) return value;
    std::string decoded;
    size_t position = 0;
    while (position < value.size()) {
        if (position != 0) {
            if (value[position] != ' ') throw std::invalid_argument("invalid encoded-word spacing");
            while (position < value.size() && value[position] == ' ') ++position;
        }
        if (position + 10 > value.size() || value.compare(position, 2, "=?") != 0)
            throw std::invalid_argument("invalid RFC 2047 word");
        const auto charset_end = value.find('?', position + 2);
        const auto encoding_end =
            charset_end == std::string::npos ? std::string::npos : value.find('?', charset_end + 1);
        const auto word_end = encoding_end == std::string::npos
                                  ? std::string::npos
                                  : value.find("?=", encoding_end + 1);
        if (charset_end == std::string::npos || encoding_end == std::string::npos ||
            word_end == std::string::npos ||
            lower_ascii(value.substr(position + 2, charset_end - position - 2)) != "utf-8" ||
            lower_ascii(value.substr(charset_end + 1, encoding_end - charset_end - 1)) != "q")
            throw std::invalid_argument("only strict UTF-8 Q encoding is accepted");
        decoded += decode_q_word_body(value.substr(encoding_end + 1, word_end - encoding_end - 1));
        position = word_end + 2;
    }
    if (decoded.size() > max_bytes || !valid_utf8(decoded) || has_control(decoded))
        throw std::invalid_argument("invalid decoded Tailscale Serve header");
    return decoded;
}

nlohmann::json decode_capabilities(const std::string& raw, size_t max_bytes) {
    const auto decoded = decode_serve_header(raw, max_bytes);
    auto capabilities = parse_bounded_json(decoded, max_bytes);
    if (!capabilities.is_object()) throw std::invalid_argument("capabilities must be an object");
    for (const auto& [name, entries] : capabilities.items()) {
        if (name.empty() || name.size() > 512 || name.find('/') == std::string::npos ||
            has_control(name) || !entries.is_array())
            throw std::invalid_argument("invalid Tailscale capability");
        for (const auto& entry : entries)
            if (!entry.is_object())
                throw std::invalid_argument("capability entries must be objects");
    }
    return capabilities;
}

struct Endpoint {
    std::string unix_socket;
    std::string host;
    std::string port;
    std::string password;
};

Endpoint make_endpoint(const TailscaleLocalAPIOptions& options) {
    if (!options.unix_socket.empty()) {
#ifdef _WIN32
        throw std::invalid_argument("Tailscale LocalAPI Unix sockets are not supported on Windows");
#else
        if (options.unix_socket.find('\0') != std::string::npos ||
            !valid_utf8(options.unix_socket) ||
            options.unix_socket.size() >= sizeof(sockaddr_un::sun_path))
            throw std::invalid_argument("invalid Tailscale LocalAPI Unix socket path");
        return {options.unix_socket, {}, {}, {}};
#endif
    }
    if (options.endpoint.empty()) {
#ifdef _WIN32
        throw std::invalid_argument(
            "native Tailscale named-pipe discovery is unavailable; configure an HTTP endpoint");
#else
        return {kDefaultSocket, {}, {}, {}};
#endif
    }
    if (options.endpoint.rfind("http://", 0) != 0 ||
        options.endpoint.find_first_of("?#@", 7) != std::string::npos)
        throw std::invalid_argument("LocalAPI endpoint must be an HTTP origin");
    auto authority = options.endpoint.substr(7);
    if (!authority.empty() && authority.back() == '/') authority.pop_back();
    if (authority.empty() || authority.find('/') != std::string::npos || has_control(authority))
        throw std::invalid_argument("LocalAPI endpoint must not contain a path");
    std::string host, port = "80";
    if (authority.front() == '[') {
        const auto closing = authority.find(']');
        if (closing == std::string::npos) throw std::invalid_argument("invalid IPv6 endpoint");
        host = authority.substr(1, closing - 1);
        if (closing + 1 < authority.size()) {
            if (authority[closing + 1] != ':') throw std::invalid_argument("invalid endpoint port");
            port = authority.substr(closing + 2);
        }
    } else {
        const auto colon = authority.rfind(':');
        if (colon != std::string::npos) {
            if (authority.find(':') != colon)
                throw std::invalid_argument("IPv6 endpoint must use brackets");
            host = authority.substr(0, colon);
            port = authority.substr(colon + 1);
        } else {
            host = authority;
        }
    }
    unsigned port_number = 0;
    const auto [ptr, ec] = std::from_chars(port.data(), port.data() + port.size(), port_number);
    if (host.empty() || ec != std::errc{} || ptr != port.data() + port.size() || port_number == 0 ||
        port_number > 65'535)
        throw std::invalid_argument("invalid LocalAPI endpoint");
    return {{}, std::move(host), std::move(port), options.password};
}

class Unavailable : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
void close_socket(SocketHandle socket) {
    closesocket(socket);
}
int socket_error() {
    return WSAGetLastError();
}
bool would_block(int error) {
    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS;
}
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
void close_socket(SocketHandle socket) {
    ::close(socket);
}
int socket_error() {
    return errno;
}
bool would_block(int error) {
    return error == EAGAIN || error == EWOULDBLOCK || error == EINPROGRESS;
}
#endif

class Socket {
public:
    explicit Socket(SocketHandle value = kInvalidSocket) : value_(value) {}
    ~Socket() {
        if (value_ != kInvalidSocket) close_socket(value_);
    }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept : value_(other.value_) { other.value_ = kInvalidSocket; }
    SocketHandle get() const noexcept { return value_; }

private:
    SocketHandle value_;
};

int remaining_ms(Clock::time_point deadline) {
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    if (remaining <= std::chrono::milliseconds::zero())
        throw Unavailable("LocalAPI deadline expired");
    return int(std::min<int64_t>(remaining.count() + 1, std::numeric_limits<int>::max()));
}

void set_nonblocking(SocketHandle socket) {
#ifdef _WIN32
    u_long enabled = 1;
    if (ioctlsocket(socket, FIONBIO, &enabled) != 0) throw Unavailable("cannot configure socket");
#else
    const int flags = fcntl(socket, F_GETFL, 0);
    if (flags < 0 || fcntl(socket, F_SETFL, flags | O_NONBLOCK) < 0)
        throw Unavailable("cannot configure socket");
#endif
}

void wait_socket(SocketHandle socket, bool writing, Clock::time_point deadline) {
#ifdef _WIN32
    WSAPOLLFD descriptor{socket, short(writing ? POLLWRNORM : POLLRDNORM), 0};
    const int outcome = WSAPoll(&descriptor, 1, remaining_ms(deadline));
#else
    pollfd descriptor{socket, short(writing ? POLLOUT : POLLIN), 0};
    const int outcome = poll(&descriptor, 1, remaining_ms(deadline));
#endif
    if (outcome <= 0) throw Unavailable("LocalAPI socket timeout");
    if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        if (!(!writing && (descriptor.revents & POLLHUP)))
            throw Unavailable("LocalAPI socket error");
    }
}

Socket connect_endpoint(const Endpoint& endpoint, Clock::time_point deadline) {
#ifndef _WIN32
    if (!endpoint.unix_socket.empty()) {
        Socket socket(::socket(AF_UNIX, SOCK_STREAM, 0));
        if (socket.get() == kInvalidSocket) throw Unavailable("cannot create LocalAPI socket");
        set_nonblocking(socket.get());
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, endpoint.unix_socket.c_str(),
                    endpoint.unix_socket.size() + 1);
        if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            if (!would_block(socket_error())) throw Unavailable("cannot connect to tailscaled");
            wait_socket(socket.get(), true, deadline);
            int error = 0;
            socklen_t size = sizeof(error);
            if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &size) != 0 || error != 0)
                throw Unavailable("cannot connect to tailscaled");
        }
        return socket;
    }
#endif
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* raw = nullptr;
    if (getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &raw) != 0)
        throw Unavailable("cannot resolve LocalAPI endpoint");
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> addresses(raw, freeaddrinfo);
    for (auto* address = raw; address; address = address->ai_next) {
        Socket socket(::socket(address->ai_family, address->ai_socktype, address->ai_protocol));
        if (socket.get() == kInvalidSocket) continue;
        try {
            set_nonblocking(socket.get());
            if (::connect(socket.get(), address->ai_addr, static_cast<int>(address->ai_addrlen)) !=
                0) {
                if (!would_block(socket_error())) continue;
                wait_socket(socket.get(), true, deadline);
                int error = 0;
#ifdef _WIN32
                int size = sizeof(error);
                if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error),
                               &size) != 0 ||
                    error != 0)
#else
                socklen_t size = sizeof(error);
                if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &size) != 0 ||
                    error != 0)
#endif
                    continue;
            }
            return socket;
        } catch (const Unavailable&) {
            if (Clock::now() >= deadline) throw;
        }
    }
    throw Unavailable("cannot connect to LocalAPI endpoint");
}

void write_all(SocketHandle socket, const std::string& value, Clock::time_point deadline) {
    size_t sent = 0;
    while (sent < value.size()) {
        wait_socket(socket, true, deadline);
#ifdef _WIN32
        const int count = send(socket, value.data() + sent,
                               int(std::min<size_t>(value.size() - sent, INT_MAX)), 0);
#else
        const auto count = send(socket, value.data() + sent, value.size() - sent, MSG_NOSIGNAL);
#endif
        if (count > 0)
            sent += size_t(count);
        else if (count == 0 || !would_block(socket_error()))
            throw Unavailable("LocalAPI send failed");
    }
}

class SocketReader {
public:
    SocketReader(SocketHandle socket, Clock::time_point deadline, std::string initial = {})
        : socket_(socket), deadline_(deadline), buffer_(std::move(initial)) {}

    std::string line(size_t limit) {
        while (true) {
            const auto found = buffer_.find("\r\n", offset_);
            if (found != std::string::npos) {
                auto line = buffer_.substr(offset_, found - offset_);
                offset_ = found + 2;
                compact();
                return line;
            }
            if (buffer_.size() - offset_ > limit)
                throw std::invalid_argument("oversized HTTP line");
            receive();
        }
    }

    std::string exact(size_t size) {
        while (buffer_.size() - offset_ < size) receive();
        auto value = buffer_.substr(offset_, size);
        offset_ += size;
        compact();
        return value;
    }

    std::string until_eof(size_t limit) {
        std::string out;
        if (offset_ < buffer_.size())
            out.assign(buffer_.begin() + static_cast<ptrdiff_t>(offset_), buffer_.end());
        buffer_.clear();
        offset_ = 0;
        while (!eof_) {
            receive();
            out += buffer_;
            buffer_.clear();
            if (out.size() > limit) throw std::invalid_argument("oversized HTTP body");
        }
        return out;
    }

private:
    void receive() {
        if (eof_) throw Unavailable("truncated HTTP response");
        wait_socket(socket_, false, deadline_);
        std::array<char, 8192> bytes{};
#ifdef _WIN32
        const int count = recv(socket_, bytes.data(), int(bytes.size()), 0);
#else
        const auto count = recv(socket_, bytes.data(), bytes.size(), 0);
#endif
        if (count > 0)
            buffer_.append(bytes.data(), size_t(count));
        else if (count == 0)
            eof_ = true;
        else if (!would_block(socket_error()))
            throw Unavailable("LocalAPI receive failed");
    }
    void compact() {
        if (offset_ > 8192 && offset_ * 2 > buffer_.size()) {
            buffer_.erase(0, offset_);
            offset_ = 0;
        }
    }
    SocketHandle socket_;
    Clock::time_point deadline_;
    std::string buffer_;
    size_t offset_ = 0;
    bool eof_ = false;
};

std::string base64(const std::string& value) {
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    for (size_t i = 0; i < value.size(); i += 3) {
        const uint32_t chunk = uint32_t(uint8_t(value[i])) << 16 |
                               (i + 1 < value.size() ? uint32_t(uint8_t(value[i + 1])) << 8 : 0) |
                               (i + 2 < value.size() ? uint32_t(uint8_t(value[i + 2])) : 0);
        out.push_back(alphabet[(chunk >> 18) & 63]);
        out.push_back(alphabet[(chunk >> 12) & 63]);
        out.push_back(i + 1 < value.size() ? alphabet[(chunk >> 6) & 63] : '=');
        out.push_back(i + 2 < value.size() ? alphabet[chunk & 63] : '=');
    }
    return out;
}

std::string url_encode(const std::string& value) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~') {
            out.push_back(char(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 15]);
        }
    }
    return out;
}

struct HttpResponse {
    int status = 0;
    std::map<std::string, std::vector<std::string>> headers;
    std::string body;
};

HttpResponse request_whois(const Endpoint& endpoint, const std::string& target,
                           Clock::time_point deadline, size_t max_header_bytes,
                           size_t max_body_bytes) {
#ifdef _WIN32
    struct Winsock {
        Winsock() {
            WSADATA data{};
            if (WSAStartup(MAKEWORD(2, 2), &data) != 0) throw Unavailable("Winsock unavailable");
        }
        ~Winsock() { WSACleanup(); }
    } winsock;
#endif
    auto socket = connect_endpoint(endpoint, deadline);
    std::string request =
        "GET " + target + " HTTP/1.1\r\nHost: " + kLocalApiHost +
        "\r\nAccept: application/json\r\nAccept-Encoding: identity\r\nConnection: close\r\n";
    if (!endpoint.password.empty())
        request += "Authorization: Basic " + base64(":" + endpoint.password) + "\r\n";
    request += "\r\n";
    write_all(socket.get(), request, deadline);

    std::string head;
    std::array<char, 4096> bytes{};
    while (head.find("\r\n\r\n") == std::string::npos) {
        if (head.size() > max_header_bytes) throw std::invalid_argument("oversized HTTP headers");
        wait_socket(socket.get(), false, deadline);
#ifdef _WIN32
        const int count = recv(socket.get(), bytes.data(), int(bytes.size()), 0);
#else
        const auto count = recv(socket.get(), bytes.data(), bytes.size(), 0);
#endif
        if (count > 0)
            head.append(bytes.data(), size_t(count));
        else if (count == 0)
            throw Unavailable("truncated HTTP headers");
        else if (!would_block(socket_error()))
            throw Unavailable("LocalAPI receive failed");
    }
    const auto split = head.find("\r\n\r\n");
    if (split + 4 > max_header_bytes) throw std::invalid_argument("oversized HTTP headers");
    const auto header_block = head.substr(0, split);
    SocketReader reader(socket.get(), deadline, head.substr(split + 4));
    const auto status_end = header_block.find("\r\n");
    const auto status_line = header_block.substr(0, status_end);
    if (status_line.size() < 12 || status_line.rfind("HTTP/1.", 0) != 0 || status_line[8] != ' ')
        throw std::invalid_argument("invalid HTTP status line");
    int status = 0;
    const auto [status_ptr, status_ec] =
        std::from_chars(status_line.data() + 9, status_line.data() + 12, status);
    if (status_ec != std::errc{} || status_ptr != status_line.data() + 12)
        throw std::invalid_argument("invalid HTTP status");
    HttpResponse response;
    response.status = status;
    size_t start = status_end == std::string::npos ? header_block.size() : status_end + 2;
    while (start < header_block.size()) {
        const auto end = header_block.find("\r\n", start);
        const auto line =
            header_block.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (line.empty() || line.front() == ' ' || line.front() == '\t')
            throw std::invalid_argument("invalid folded HTTP header");
        const auto colon = line.find(':');
        if (colon == std::string::npos || !valid_header_name(line.substr(0, colon)))
            throw std::invalid_argument("invalid HTTP header");
        auto value = trim_ascii(line.substr(colon + 1));
        if (has_control(value)) throw std::invalid_argument("invalid HTTP header value");
        response.headers[lower_ascii(line.substr(0, colon))].push_back(std::move(value));
        if (end == std::string::npos) break;
        start = end + 2;
    }
    // WhoIs status is authoritative without a response body. In particular,
    // permission and daemon failures must not be reclassified because an
    // error page has unexpected framing or exceeds the success-body limit.
    if (response.status != 200) return response;
    const auto lengths = response.headers.find("content-length");
    const auto encodings = response.headers.find("transfer-encoding");
    if (lengths != response.headers.end() &&
        (lengths->second.size() != 1 || encodings != response.headers.end()))
        throw std::invalid_argument("ambiguous HTTP body framing");
    if (encodings != response.headers.end()) {
        if (encodings->second.size() != 1 || lower_ascii(encodings->second.front()) != "chunked")
            throw std::invalid_argument("unsupported HTTP transfer encoding");
        while (true) {
            const auto line = reader.line(max_header_bytes);
            const auto semicolon = line.find(';');
            const auto size_text = line.substr(0, semicolon);
            size_t chunk_size = 0;
            const auto [ptr, ec] = std::from_chars(
                size_text.data(), size_text.data() + size_text.size(), chunk_size, 16);
            if (ec != std::errc{} || ptr != size_text.data() + size_text.size())
                throw std::invalid_argument("invalid HTTP chunk");
            if (chunk_size == 0) {
                size_t trailers = 0;
                for (auto trailer = reader.line(max_header_bytes); !trailer.empty();
                     trailer = reader.line(max_header_bytes)) {
                    trailers += trailer.size() + 2;
                    if (trailers > max_header_bytes)
                        throw std::invalid_argument("oversized trailers");
                }
                break;
            }
            if (chunk_size > max_body_bytes - response.body.size())
                throw std::invalid_argument("oversized HTTP body");
            response.body += reader.exact(chunk_size);
            if (reader.exact(2) != "\r\n") throw std::invalid_argument("invalid HTTP chunk ending");
        }
    } else if (lengths != response.headers.end()) {
        size_t length = 0;
        const auto& raw = lengths->second.front();
        const auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), length);
        if (ec != std::errc{} || ptr != raw.data() + raw.size() || length > max_body_bytes)
            throw std::invalid_argument("invalid or oversized Content-Length");
        response.body = reader.exact(length);
    } else {
        response.body = reader.until_eof(max_body_bytes);
    }
    return response;
}

std::optional<std::string> optional_string(const nlohmann::json& object, const char* name) {
    const auto found = object.find(name);
    if (found == object.end() || found->is_null()) return std::nullopt;
    if (!found->is_string()) throw std::invalid_argument("WhoIs field must be a string");
    auto value = found->get<std::string>();
    if (has_control(value)) throw std::invalid_argument("WhoIs string contains controls");
    return value;
}

std::string destination_ip(const std::string& value) {
    std::string candidate = value;
    if (!parse_exact_ip(candidate)) {
        if (!value.empty() && value.front() == '[') {
            const auto closing = value.find(']');
            if (closing == std::string::npos || closing + 1 >= value.size() ||
                value[closing + 1] != ':')
                throw std::invalid_argument("destination must contain an IP address");
            candidate = value.substr(1, closing - 1);
        } else {
            const auto colon = value.rfind(':');
            if (colon == std::string::npos || value.find(':') != colon)
                throw std::invalid_argument("destination must contain an IP address");
            candidate = value.substr(0, colon);
        }
    }
    const auto address = parse_exact_ip(candidate);
    if (!address) throw std::invalid_argument("destination must contain an IP address");
    std::array<char, INET6_ADDRSTRLEN> text{};
    if (!inet_ntop(address->family, address->bytes.data(), text.data(), text.size()))
        throw std::invalid_argument("cannot canonicalize destination IP");
    return text.data();
}

PeerIdentity whois_identity(const nlohmann::json& payload, const std::string& issuer,
                            const PeerResolutionContext& context, const std::string& source,
                            nlohmann::json target,
                            std::optional<std::string> proxy_address = std::nullopt) {
    if (!payload.is_object() || !payload.contains("Node") || !payload["Node"].is_object())
        throw std::invalid_argument("WhoIs response is missing Node");
    const auto& node = payload["Node"];
    const auto stable_id = optional_string(node, "StableID").value_or("");
    const auto node_name = optional_string(node, "Name").value_or("");
    nlohmann::json tags = nlohmann::json::array();
    if (const auto found = node.find("Tags"); found != node.end() && !found->is_null()) {
        if (!found->is_array()) throw std::invalid_argument("WhoIs tags must be an array");
        for (const auto& raw : *found) {
            if (!raw.is_string()) throw std::invalid_argument("WhoIs tag must be a string");
            const auto tag = raw.get<std::string>();
            if (tag.rfind("tag:", 0) != 0 || has_control(tag))
                throw std::invalid_argument("invalid WhoIs tag");
            tags.push_back(tag);
        }
    }
    nlohmann::json capabilities = nlohmann::json::object();
    if (const auto found = payload.find("CapMap"); found != payload.end() && !found->is_null()) {
        if (!found->is_object()) throw std::invalid_argument("WhoIs CapMap must be an object");
        capabilities = *found;
    }
    for (const auto& [name, entries] : capabilities.items())
        if (name.empty() || has_control(name) || !entries.is_array())
            throw std::invalid_argument("invalid WhoIs capability");

    nlohmann::json attributes = {{"tags", tags}, {"capability_target", std::move(target)}};
    if (!stable_id.empty()) attributes["node_id"] = stable_id;
    if (!node_name.empty()) attributes["node_name"] = node_name;
    PeerSubjectKind kind = PeerSubjectKind::USER;
    std::string subject;
    if (!tags.empty()) {
        if (stable_id.empty()) throw std::invalid_argument("tagged WhoIs node lacks StableID");
        kind = PeerSubjectKind::TAGGED_NODE;
        subject = "node:" + stable_id;
    } else {
        const auto profile = payload.find("UserProfile");
        if (profile == payload.end() || !profile->is_object() || !profile->contains("ID"))
            throw std::invalid_argument("untagged WhoIs node lacks UserProfile");
        const auto& raw_id = (*profile)["ID"];
        uint64_t user_id = 0;
        if (raw_id.is_number_unsigned())
            user_id = raw_id.get<uint64_t>();
        else if (raw_id.is_number_integer() && raw_id.get<int64_t>() > 0)
            user_id = uint64_t(raw_id.get<int64_t>());
        if (user_id == 0) throw std::invalid_argument("WhoIs user ID must be positive integer");
        const auto id = std::to_string(user_id);
        subject = "user:" + id;
        attributes["user_id"] = id;
        if (const auto login = optional_string(*profile, "LoginName"); login && !login->empty())
            attributes["user_login"] = *login;
        if (const auto display = optional_string(*profile, "DisplayName");
            display && !display->empty())
            attributes["user_display_name"] = *display;
    }
    return PeerIdentity(kProvider, "localapi", IdentityAssurance::LOCAL_DAEMON, issuer,
                        context.transport, kind, subject, SubjectStability::STABLE, true,
                        std::move(attributes), std::move(capabilities), true,
                        destination_ip(source), std::move(proxy_address));
}
}  // namespace

PeerIdentityProvider tailscale_serve_identity_provider(TailscaleServeOptions options) {
    if (!valid_issuer(options.issuer) || options.trusted_proxy_addresses.empty())
        throw std::invalid_argument("invalid Tailscale Serve configuration");
    if (options.max_header_bytes == 0) options.max_header_bytes = 16'384;
    std::set<IpAddress> trusted;
    for (const auto& value : options.trusted_proxy_addresses) {
        auto address = parse_exact_ip(value);
        if (!address)
            throw std::invalid_argument("Tailscale Serve proxy must be an exact IP address");
        trusted.insert(*address);
    }
    return [issuer = std::move(options.issuer), trusted = std::move(trusted),
            limit = options.max_header_bytes](const PeerResolutionContext& context) {
        const auto peer =
            context.immediate_peer ? parse_exact_ip(*context.immediate_peer) : std::nullopt;
        if (!peer || !trusted.contains(*peer))
            return result(kProvider, PeerIdentityStatus::UNTRUSTED_PROXY);
        try {
            context.validate();
            const auto funnel = context.header("Tailscale-Funnel-Request");
            const auto login_raw = context.header("Tailscale-User-Login");
            const auto name_raw = context.header("Tailscale-User-Name");
            const auto profile_raw = context.header("Tailscale-User-Profile-Pic");
            const auto capabilities_raw = context.header("Tailscale-App-Capabilities");
            if (funnel) {
                return result(kProvider, *funnel == "?1" ? PeerIdentityStatus::NOT_APPLICABLE
                                                         : PeerIdentityStatus::INVALID);
            }
            const auto login = login_raw ? decode_serve_header(*login_raw, limit) : "";
            const auto display = name_raw ? decode_serve_header(*name_raw, limit) : "";
            if (profile_raw) (void)decode_serve_header(*profile_raw, limit);
            auto capabilities = capabilities_raw ? decode_capabilities(*capabilities_raw, limit)
                                                 : nlohmann::json::object();
            if ((login_raw && login.empty()) || ((name_raw || profile_raw) && login.empty()))
                return result(kProvider, PeerIdentityStatus::INVALID);
            if (login.empty() && capabilities.empty())
                return result(kProvider, PeerIdentityStatus::NO_MATCH);
            nlohmann::json attributes = nlohmann::json::object();
            auto kind = PeerSubjectKind::UNKNOWN;
            auto stability = SubjectStability::NONE;
            std::optional<std::string> subject;
            bool verified = false;
            if (!login.empty()) {
                attributes["user_login"] = login;
                kind = PeerSubjectKind::USER;
                stability = SubjectStability::LOGIN;
                subject = "login:" + login;
                verified = true;
            }
            if (!display.empty()) attributes["user_display_name"] = display;
            return PeerIdentityResult::available(PeerIdentity(
                kProvider, "serve_proxy", IdentityAssurance::CONFIGURED_PROXY, issuer, "http", kind,
                std::move(subject), stability, verified, std::move(attributes),
                std::move(capabilities), capabilities_raw.has_value(), context.asserted_peer,
                context.immediate_peer));
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

PeerIdentityProvider tailscale_localapi_identity_provider(TailscaleLocalAPIOptions options) {
    if (options.timeout == std::chrono::milliseconds::zero())
        options.timeout = std::chrono::milliseconds{5'000};
    if (options.max_response_bytes == 0) options.max_response_bytes = 65'536;
    if (options.max_response_header_bytes == 0) options.max_response_header_bytes = 32'768;
    if (!valid_issuer(options.issuer) ||
        (!options.unix_socket.empty() && !options.endpoint.empty()) ||
        !valid_utf8(options.password) || has_control(options.password) ||
        options.timeout < std::chrono::milliseconds::zero())
        throw std::invalid_argument("invalid Tailscale LocalAPI configuration");
    if (!options.unix_socket.empty() && !options.password.empty())
        throw std::invalid_argument("LocalAPI password is only valid with an HTTP endpoint");
    if (options.endpoint.empty() && !options.password.empty())
        throw std::invalid_argument("LocalAPI password requires an explicit HTTP endpoint");
    const auto endpoint = make_endpoint(options);
    return [issuer = std::move(options.issuer), endpoint, timeout = options.timeout,
            body_limit = options.max_response_bytes,
            header_limit =
                options.max_response_header_bytes](const PeerResolutionContext& context) {
        try {
            context.validate();
            std::optional<std::string> proxy_address;
            if (context.asserted_peer) {
                if (!context.immediate_peer || !parse_exact_ip(*context.immediate_peer))
                    return result(kProvider, PeerIdentityStatus::INVALID);
                proxy_address = destination_ip(*context.immediate_peer);
            }
            const auto source =
                context.asserted_peer
                    ? context.asserted_peer
                    : (context.source_endpoint ? context.source_endpoint : context.immediate_peer);
            if (!source) return result(kProvider, PeerIdentityStatus::NOT_APPLICABLE);
            if (source->size() > 4096 || !valid_utf8(*source) || has_control(*source))
                return result(kProvider, PeerIdentityStatus::INVALID);
            std::string query = "addr=" + url_encode(*source) + "&proto=tcp";
            nlohmann::json target = {{"kind", "node"}};
            if (context.service_name) {
                static const std::regex service(
                    R"(^svc:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$)");
                if (!std::regex_match(*context.service_name, service))
                    return result(kProvider, PeerIdentityStatus::INVALID);
                query += "&svc_name=" + url_encode(*context.service_name);
                target = {{"kind", "service"}, {"value", *context.service_name}};
            } else if (context.destination_address) {
                const auto destination = destination_ip(*context.destination_address);
                query += "&dst_ip=" + url_encode(destination);
                target = {{"kind", "destination_ip"}, {"value", destination}};
            }
            auto deadline = Clock::now() + timeout;
            if (context.deadline != Clock::time_point{} && context.deadline < deadline)
                deadline = context.deadline;
            if (deadline <= Clock::now()) return result(kProvider, PeerIdentityStatus::UNAVAILABLE);
            const auto response = request_whois(endpoint, "/localapi/v0/whois?" + query, deadline,
                                                header_limit, body_limit);
            if (response.status == 401 || response.status == 403)
                return result(kProvider, PeerIdentityStatus::PERMISSION_DENIED);
            if (response.status == 404) return result(kProvider, PeerIdentityStatus::NO_MATCH);
            if (response.status >= 500 && response.status <= 599)
                return result(kProvider, PeerIdentityStatus::UNAVAILABLE);
            if (response.status != 200) return result(kProvider, PeerIdentityStatus::INVALID);
            const auto content_type = response.headers.find("content-type");
            if (content_type == response.headers.end() || content_type->second.size() != 1)
                return result(kProvider, PeerIdentityStatus::INVALID);
            auto media_type = lower_ascii(trim_ascii(content_type->second.front()));
            if (const auto semicolon = media_type.find(';'); semicolon != std::string::npos)
                media_type = trim_ascii(media_type.substr(0, semicolon));
            if (media_type != "application/json")
                return result(kProvider, PeerIdentityStatus::INVALID);
            const auto payload = parse_bounded_json(response.body, body_limit);
            return PeerIdentityResult::available(whois_identity(
                payload, issuer, context, *source, std::move(target), std::move(proxy_address)));
        } catch (const Unavailable&) {
            return result(kProvider, PeerIdentityStatus::UNAVAILABLE);
        } catch (const std::exception&) {
            return result(kProvider, PeerIdentityStatus::INVALID);
        }
    };
}

}  // namespace vgi_rpc
