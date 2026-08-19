// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client_external.h"

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <httplib.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstring>
#include <limits>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace vgi_rpc {

namespace {

constexpr const char* kZstd = "zstd";

std::string ascii_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char character) {
        return static_cast<char>(std::tolower(character));
    });
    return value;
}

std::string trim_ascii(std::string value) {
    const auto visible = [](unsigned char character) { return !std::isspace(character); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), visible));
    value.erase(std::find_if(value.rbegin(), value.rend(), visible).base(), value.end());
    return value;
}

bool has_forbidden_url_character(const std::string& value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char character) {
        return character <= 0x20 || character == 0x7f || character == '\\';
    });
}

struct ParsedUrl {
    std::string scheme;
    std::string host;
    uint16_t port = 0;
    bool explicit_port = false;
    bool ipv6_literal = false;
    std::string path_and_query;

    std::string authority() const {
        std::string value = ipv6_literal ? "[" + host + "]" : host;
        const bool default_port =
            (scheme == "https" && port == 443) || (scheme == "http" && port == 80);
        if (explicit_port || !default_port) value += ":" + std::to_string(port);
        return value;
    }

    std::string origin() const { return scheme + "://" + authority(); }
    std::string full() const { return origin() + path_and_query; }
};

uint16_t parse_port(const std::string& value) {
    if (value.empty() ||
        !std::all_of(value.begin(), value.end(), [](unsigned char c) { return std::isdigit(c); })) {
        throw ExternalHttpError("external URL has an invalid port");
    }
    try {
        const unsigned long parsed = std::stoul(value);
        if (parsed == 0 || parsed > 65535)
            throw ExternalHttpError("external URL port is out of range");
        return static_cast<uint16_t>(parsed);
    } catch (const ExternalHttpError&) {
        throw;
    } catch (const std::exception&) {
        throw ExternalHttpError("external URL has an invalid port");
    }
}

ParsedUrl parse_url(const std::string& url) {
    if (url.empty() || has_forbidden_url_character(url) || url.find('#') != std::string::npos) {
        throw ExternalHttpError("external URL is invalid or contains a fragment");
    }
    const size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) throw ExternalHttpError("external URL must be absolute");

    ParsedUrl parsed;
    parsed.scheme = ascii_lower(url.substr(0, scheme_end));
    if (parsed.scheme != "https" && parsed.scheme != "http") {
        throw ExternalHttpError("external URL scheme is not HTTP(S)");
    }
    const size_t authority_start = scheme_end + 3;
    const size_t authority_end = url.find_first_of("/?", authority_start);
    const std::string authority = url.substr(
        authority_start,
        authority_end == std::string::npos ? std::string::npos : authority_end - authority_start);
    if (authority.empty() || authority.find('@') != std::string::npos) {
        throw ExternalHttpError("external URL has an empty host or forbidden userinfo");
    }

    std::string port;
    if (authority.front() == '[') {
        const size_t close = authority.find(']');
        if (close == std::string::npos || close == 1) {
            throw ExternalHttpError("external URL has an invalid IPv6 host");
        }
        parsed.host = authority.substr(1, close - 1);
        parsed.ipv6_literal = true;
        if (close + 1 < authority.size()) {
            if (authority[close + 1] != ':') {
                throw ExternalHttpError("external URL has invalid text after its IPv6 host");
            }
            port = authority.substr(close + 2);
            parsed.explicit_port = true;
        }
    } else {
        const size_t colon = authority.rfind(':');
        if (colon != std::string::npos) {
            if (authority.find(':') != colon) {
                throw ExternalHttpError("external URL IPv6 hosts must use brackets");
            }
            parsed.host = authority.substr(0, colon);
            port = authority.substr(colon + 1);
            parsed.explicit_port = true;
        } else {
            parsed.host = authority;
        }
    }
    parsed.host = ascii_lower(std::move(parsed.host));
    if (!parsed.ipv6_literal) {
        while (!parsed.host.empty() && parsed.host.back() == '.') parsed.host.pop_back();
        if (parsed.host.empty() || parsed.host.size() > 253 || parsed.host.front() == '.' ||
            parsed.host.back() == '.' || parsed.host.find("..") != std::string::npos ||
            !std::all_of(parsed.host.begin(), parsed.host.end(), [](unsigned char character) {
                return std::isalnum(character) || character == '-' || character == '.';
            })) {
            throw ExternalHttpError("external URL has an invalid hostname");
        }
    } else {
        if (parsed.host.find('%') != std::string::npos) {
            throw ExternalHttpError("external URL IPv6 zone identifiers are forbidden");
        }
        in6_addr address{};
        if (::inet_pton(AF_INET6, parsed.host.c_str(), &address) != 1) {
            throw ExternalHttpError("external URL has an invalid IPv6 host");
        }
        std::array<char, INET6_ADDRSTRLEN> canonical{};
        if (!::inet_ntop(AF_INET6, &address, canonical.data(), canonical.size())) {
            throw ExternalHttpError("external URL IPv6 host could not be normalized safely");
        }
        // cpp-httplib's universal HTTPS constructor accepts canonical
        // hexadecimal IPv6 literals but not their dotted-decimal spelling.
        parsed.host = ascii_lower(canonical.data());
    }

    parsed.port = parsed.explicit_port ? parse_port(port)
                                       : static_cast<uint16_t>(parsed.scheme == "https" ? 443 : 80);
    if (authority_end == std::string::npos) {
        parsed.path_and_query = "/";
    } else if (url[authority_end] == '?') {
        parsed.path_and_query = "/" + url.substr(authority_end);
    } else {
        parsed.path_and_query = url.substr(authority_end);
    }
    return parsed;
}

std::string normalize_path(std::string path) {
    const size_t query = path.find('?');
    const std::string suffix = query == std::string::npos ? std::string() : path.substr(query);
    if (query != std::string::npos) path.resize(query);
    std::vector<std::string> segments;
    std::istringstream input(path);
    std::string segment;
    while (std::getline(input, segment, '/')) {
        if (segment.empty() || segment == ".") continue;
        if (segment == "..") {
            if (!segments.empty()) segments.pop_back();
        } else {
            segments.push_back(std::move(segment));
        }
    }
    std::string normalized = "/";
    for (size_t i = 0; i < segments.size(); ++i) {
        if (i > 0) normalized.push_back('/');
        normalized += segments[i];
    }
    if (path.size() > 1 && path.back() == '/') normalized.push_back('/');
    return normalized + suffix;
}

std::string resolve_redirect(const ParsedUrl& base, const std::string& location) {
    if (location.empty() || has_forbidden_url_character(location) ||
        location.find('#') != std::string::npos) {
        throw ExternalHttpError("external redirect target is invalid or contains a fragment");
    }
    if (location.find("://") != std::string::npos) return location;
    if (location.rfind("//", 0) == 0) return base.scheme + ":" + location;
    if (location.front() == '/') return base.origin() + normalize_path(location);
    if (location.front() == '?') {
        const size_t query = base.path_and_query.find('?');
        const std::string path = base.path_and_query.substr(0, query);
        return base.origin() + path + location;
    }
    const size_t query = base.path_and_query.find('?');
    const std::string base_path = base.path_and_query.substr(0, query);
    const size_t slash = base_path.rfind('/');
    return base.origin() +
           normalize_path((slash == std::string::npos ? "/" : base_path.substr(0, slash + 1)) +
                          location);
}

bool in_ipv4_range(uint32_t address, uint32_t network, uint32_t mask) {
    return (address & mask) == network;
}

bool ipv4_loopback(uint32_t address) {
    return in_ipv4_range(address, 0x7f000000U, 0xff000000U);
}

bool ipv4_public(uint32_t address) {
    return !(in_ipv4_range(address, 0x00000000U, 0xff000000U) ||
             in_ipv4_range(address, 0x0a000000U, 0xff000000U) ||
             in_ipv4_range(address, 0x64400000U, 0xffc00000U) || ipv4_loopback(address) ||
             in_ipv4_range(address, 0xa9fe0000U, 0xffff0000U) ||
             in_ipv4_range(address, 0xac100000U, 0xfff00000U) ||
             in_ipv4_range(address, 0xc0000000U, 0xffffff00U) ||
             in_ipv4_range(address, 0xc0000200U, 0xffffff00U) ||
             in_ipv4_range(address, 0xc0a80000U, 0xffff0000U) ||
             in_ipv4_range(address, 0xc6120000U, 0xfffe0000U) ||
             in_ipv4_range(address, 0xc6336400U, 0xffffff00U) ||
             in_ipv4_range(address, 0xcb007100U, 0xffffff00U) ||
             in_ipv4_range(address, 0xe0000000U, 0xf0000000U) ||
             in_ipv4_range(address, 0xf0000000U, 0xf0000000U));
}

bool bytes_all_zero(const uint8_t* bytes, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        if (bytes[i] != 0) return false;
    }
    return true;
}

bool ipv6_loopback(const uint8_t* bytes) {
    return bytes_all_zero(bytes, 15) && bytes[15] == 1;
}

bool ipv6_public(const uint8_t* bytes) {
    if (bytes_all_zero(bytes, 16) || ipv6_loopback(bytes)) return false;
    if ((bytes[0] & 0xfe) == 0xfc) return false;                      // unique-local fc00::/7
    if (bytes[0] == 0xfe && (bytes[1] & 0xc0) == 0x80) return false;  // link-local
    if (bytes[0] == 0xff) return false;                               // multicast
    if (bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0x0d && bytes[3] == 0xb8) {
        return false;  // documentation 2001:db8::/32
    }
    if (bytes_all_zero(bytes, 10) && bytes[10] == 0xff && bytes[11] == 0xff) {
        uint32_t mapped = 0;
        std::memcpy(&mapped, bytes + 12, sizeof(mapped));
        return ipv4_public(ntohl(mapped));
    }
    // Deprecated IPv4-compatible addresses (::/96) can otherwise disguise
    // an internal IPv4 target without using the mapped-address marker.
    if (bytes_all_zero(bytes, 12)) {
        uint32_t compatible = 0;
        std::memcpy(&compatible, bytes + 12, sizeof(compatible));
        return ipv4_public(ntohl(compatible));
    }
    // Well-known NAT64 embeds an IPv4 target. Apply the IPv4 policy to it.
    if (bytes[0] == 0x00 && bytes[1] == 0x64 && bytes[2] == 0xff && bytes[3] == 0x9b &&
        bytes_all_zero(bytes + 4, 8)) {
        uint32_t translated = 0;
        std::memcpy(&translated, bytes + 12, sizeof(translated));
        return ipv4_public(ntohl(translated));
    }
    // Public IPv6 unicast is allocated from 2000::/3. This intentionally
    // rejects deprecated site-local, discard-only, benchmarking, transition,
    // and future-use space unless it is explicitly classified here.
    if ((bytes[0] & 0xe0) != 0x20) return false;
    if (bytes[0] == 0x20 && bytes[1] == 0x01) {
        if (bytes[2] == 0x00 && bytes[3] == 0x00) return false;           // Teredo
        if (bytes[2] == 0x00 && bytes[3] == 0x02) return false;           // benchmarking
        if (bytes[2] == 0x00 && (bytes[3] & 0xf0) == 0x10) return false;  // ORCHIDv1
        if (bytes[2] == 0x00 && (bytes[3] & 0xf0) == 0x20) return false;  // ORCHIDv2
    }
    if (bytes[0] == 0x20 && bytes[1] == 0x02) {  // 6to4 embeds an IPv4 target.
        uint32_t embedded = 0;
        std::memcpy(&embedded, bytes + 2, sizeof(embedded));
        return ipv4_public(ntohl(embedded));
    }
    return true;
}

struct ResolvedTarget {
    ParsedUrl url;
    std::string pinned_address;
};

#ifdef _WIN32
void ensure_winsock() {
    static const bool initialized = [] {
        WSADATA data{};
        if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) {
            throw ExternalHttpError("cannot initialize DNS resolver");
        }
        return true;
    }();
    (void)initialized;
}
#endif

ResolvedTarget validate_and_resolve(const std::string& raw_url, ExternalUrlPolicy policy) {
    ParsedUrl url = parse_url(raw_url);
    if (policy == ExternalUrlPolicy::PUBLIC_HTTPS && url.scheme != "https") {
        throw ExternalHttpError("external URL policy requires HTTPS");
    }
    if (policy == ExternalUrlPolicy::LOOPBACK_HTTP_TEST && url.scheme != "http") {
        throw ExternalHttpError("loopback test URL policy requires HTTP");
    }
    if (url.host == "localhost" || (url.host.size() > 10 && url.host.ends_with(".localhost"))) {
        if (policy != ExternalUrlPolicy::LOOPBACK_HTTP_TEST) {
            throw ExternalHttpError("external URL resolves to a non-public address");
        }
    }

#ifdef _WIN32
    ensure_winsock();
#endif
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* addresses = nullptr;
    const std::string service = std::to_string(url.port);
    if (::getaddrinfo(url.host.c_str(), service.c_str(), &hints, &addresses) != 0 || !addresses) {
        throw ExternalHttpError("external URL hostname could not be resolved");
    }

    std::string selected;
    try {
        for (const addrinfo* item = addresses; item; item = item->ai_next) {
            bool is_loopback = false;
            bool is_public = false;
            if (item->ai_family == AF_INET) {
                const auto* address = reinterpret_cast<const sockaddr_in*>(item->ai_addr);
                const uint32_t value = ntohl(address->sin_addr.s_addr);
                is_loopback = ipv4_loopback(value);
                is_public = ipv4_public(value);
            } else if (item->ai_family == AF_INET6) {
                const auto* address = reinterpret_cast<const sockaddr_in6*>(item->ai_addr);
                const auto* bytes = reinterpret_cast<const uint8_t*>(&address->sin6_addr);
                is_loopback = ipv6_loopback(bytes);
                is_public = ipv6_public(bytes);
            } else {
                continue;
            }
            const bool allowed =
                policy == ExternalUrlPolicy::PUBLIC_HTTPS ? is_public : is_loopback;
            if (!allowed) {
                throw ExternalHttpError("external URL resolves to an address forbidden by policy");
            }
            std::array<char, INET6_ADDRSTRLEN> numeric{};
            const void* source =
                item->ai_family == AF_INET
                    ? static_cast<const void*>(
                          &reinterpret_cast<const sockaddr_in*>(item->ai_addr)->sin_addr)
                    : static_cast<const void*>(
                          &reinterpret_cast<const sockaddr_in6*>(item->ai_addr)->sin6_addr);
            if (!::inet_ntop(item->ai_family, source, numeric.data(), numeric.size())) {
                throw ExternalHttpError("external URL address could not be rendered safely");
            }
            if (selected.empty()) selected = numeric.data();
        }
    } catch (...) {
        ::freeaddrinfo(addresses);
        throw;
    }
    ::freeaddrinfo(addresses);
    if (selected.empty()) throw ExternalHttpError("external URL resolved to no usable address");
    return {std::move(url), std::move(selected)};
}

int zstd_window_log_for_limit(int64_t max_bytes) {
    uint64_t bounded = std::max<uint64_t>(static_cast<uint64_t>(max_bytes), 512 * 1024);
    int log = 0;
    for (uint64_t value = bounded - 1; value != 0; value >>= 1) ++log;
    return std::clamp(log, 19, 31);
}

std::string decode_zstd_bounded(const std::string& encoded, int64_t max_decoded_bytes) {
    struct Deleter {
        void operator()(ZSTD_DCtx* context) const noexcept { (void)ZSTD_freeDCtx(context); }
    };
    std::unique_ptr<ZSTD_DCtx, Deleter> context(ZSTD_createDCtx());
    if (!context) throw ExternalHttpError("cannot allocate external zstd decoder");
    const size_t configured = ZSTD_DCtx_setParameter(context.get(), ZSTD_d_windowLogMax,
                                                     zstd_window_log_for_limit(max_decoded_bytes));
    if (ZSTD_isError(configured)) {
        throw ExternalHttpError("cannot configure bounded external zstd decoder");
    }

    ZSTD_inBuffer input{encoded.data(), encoded.size(), 0};
    std::string decoded;
    decoded.reserve(static_cast<size_t>(std::min<int64_t>(max_decoded_bytes, 64 * 1024)));
    size_t remaining = 1;
    do {
        const size_t previous_input = input.pos;
        char chunk[64 * 1024];
        ZSTD_outBuffer output{chunk, sizeof(chunk), 0};
        remaining = ZSTD_decompressStream(context.get(), &output, &input);
        if (ZSTD_isError(remaining)) throw ExternalHttpError("invalid external zstd response");
        if (output.pos > static_cast<uint64_t>(max_decoded_bytes) ||
            decoded.size() > static_cast<uint64_t>(max_decoded_bytes) - output.pos) {
            throw ExternalHttpError("external decoded response exceeds max_decoded_bytes");
        }
        decoded.append(chunk, output.pos);
        if (output.pos == 0 && input.pos == previous_input && remaining != 0) {
            throw ExternalHttpError("truncated external zstd response");
        }
    } while (input.pos < input.size || remaining != 0);
    return decoded;
}

void verify_checksum(const std::string& bytes, const std::string& expected) {
    if (expected.empty()) return;
    const auto decoded = crypto::hex_decode(expected);
    if (!decoded || decoded->size() != 32) {
        throw ExternalHttpError("external SHA-256 metadata is not a 64-character hex digest");
    }
    const auto actual =
        crypto::sha256(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    const std::string expected_bytes(reinterpret_cast<const char*>(decoded->data()),
                                     decoded->size());
    const std::string actual_bytes(reinterpret_cast<const char*>(actual.data()), actual.size());
    if (!crypto::constant_time_equal(expected_bytes, actual_bytes)) {
        throw ExternalHttpError("external response SHA-256 checksum mismatch");
    }
}

bool is_redirect(int status) {
    return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
}

void set_timeouts(httplib::Client& client, const ClientExternalHttpOptions& options) {
    client.set_connection_timeout(options.connect_timeout);
    client.set_read_timeout(options.read_timeout);
    client.set_write_timeout(options.write_timeout);
    client.set_follow_location(false);
    client.set_decompress(false);
#ifdef CPPHTTPLIB_SSL_ENABLED
    client.enable_server_certificate_verification(true);
    client.enable_server_hostname_verification(true);
#endif
}

struct HttpResponse {
    int status = 0;
    std::string location;
    std::string content_encoding;
    std::string body;
};

HttpResponse get_once(const ResolvedTarget& target, const ClientExternalHttpOptions& options) {
    httplib::Client client(target.url.origin());
    if (!client.is_valid())
        throw ExternalHttpError("external HTTP client could not be initialized");
    client.set_hostname_addr_map({{target.url.host, target.pinned_address}});
    set_timeouts(client, options);
    httplib::Headers headers{{"Accept-Encoding", "zstd, identity"}};
    HttpResponse response;
    bool cap_exceeded = false;
    bool redirect_aborted = false;
    auto result = client.Get(
        target.url.path_and_query, headers,
        [&](const httplib::Response& head) {
            response.status = head.status;
            response.location = head.get_header_value("Location");
            response.content_encoding = head.get_header_value("Content-Encoding");
            if (is_redirect(head.status)) {
                redirect_aborted = true;
                return false;
            }
            if (head.has_header("Content-Length")) {
                try {
                    const unsigned long long size =
                        std::stoull(head.get_header_value("Content-Length"));
                    if (size > static_cast<uint64_t>(options.max_encoded_bytes)) {
                        cap_exceeded = true;
                        return false;
                    }
                } catch (const std::exception&) {
                }
            }
            return true;
        },
        [&](const char* data, size_t size) {
            if (size > static_cast<uint64_t>(options.max_encoded_bytes) ||
                response.body.size() > static_cast<uint64_t>(options.max_encoded_bytes) - size) {
                cap_exceeded = true;
                return false;
            }
            response.body.append(data, size);
            return true;
        });
    if (cap_exceeded) throw ExternalHttpError("external response exceeds max_encoded_bytes");
    if (redirect_aborted) return response;
    if (!result) {
        throw ExternalHttpError(
            "external GET failed [url: " + redact_external_url(target.url.full()) + "]");
    }
    response.status = result->status;
    return response;
}

bool is_location_metadata(const std::string& key) {
    return key == keys::LOCATION || key == keys::LOCATION_SHA256 || key == keys::LOCATION_SOURCE ||
           key == keys::LOCATION_FETCH_MS;
}

void erase_metadata_key(arrow::KeyValueMetadata& metadata, const std::string& key) {
    int64_t index = metadata.FindKey(key);
    while (index >= 0) {
        (void)metadata.Delete(index);
        index = metadata.FindKey(key);
    }
}

std::shared_ptr<arrow::KeyValueMetadata> merge_resolution_metadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& outer,
    const std::shared_ptr<arrow::KeyValueMetadata>& inner, const std::string& source,
    double elapsed_ms) {
    auto result = std::make_shared<arrow::KeyValueMetadata>();
    if (outer) {
        for (int64_t index = 0; index < outer->size(); ++index) {
            if (!is_location_metadata(outer->key(index))) {
                result->Append(outer->key(index), outer->value(index));
            }
        }
    }
    if (inner) {
        for (int64_t index = 0; index < inner->size(); ++index) {
            const std::string& key = inner->key(index);
            if (is_location_metadata(key)) continue;
            // Implementations put continuation metadata on either side of
            // the indirection. Preserve both, with the fetched batch taking
            // precedence when a key appears in both places.
            erase_metadata_key(*result, key);
            result->Append(key, inner->value(index));
        }
    }
    for (const char* key : {keys::LOCATION_SOURCE, keys::LOCATION_FETCH_MS}) {
        erase_metadata_key(*result, key);
    }
    result->Append(keys::LOCATION_SOURCE, source);
    result->Append(keys::LOCATION_FETCH_MS, std::to_string(elapsed_ms));
    return result;
}

}  // namespace

std::string redact_external_url(const std::string& url) noexcept {
    try {
        const ParsedUrl parsed = parse_url(url);
        const size_t query = parsed.path_and_query.find('?');
        return parsed.origin() + parsed.path_and_query.substr(0, query);
    } catch (...) {
        return "<invalid-url>";
    }
}

class ClientExternalHttp::Impl {
public:
    explicit Impl(ClientExternalHttpOptions value) : options(std::move(value)) {
        if (options.max_redirects < 0 || options.max_redirects > 5) {
            throw std::invalid_argument("external max_redirects must be between 0 and 5");
        }
        if (options.max_encoded_bytes <= 0 || options.max_decoded_bytes <= 0 ||
            options.max_upload_bytes <= 0 || options.max_upload_response_bytes <= 0) {
            throw std::invalid_argument("external byte caps must be positive");
        }
        if (options.connect_timeout <= std::chrono::milliseconds::zero() ||
            options.read_timeout <= std::chrono::milliseconds::zero() ||
            options.write_timeout <= std::chrono::milliseconds::zero()) {
            throw std::invalid_argument("external HTTP timeouts must be positive");
        }
    }

    std::string fetch(const std::string& initial_url, const std::string& checksum) const {
        std::string current = initial_url;
        std::set<std::string> visited;
        for (int redirects = 0;; ++redirects) {
            if (!visited.insert(current).second) {
                throw ExternalHttpError(
                    "external redirect loop detected [url: " + redact_external_url(current) + "]");
            }
            const ResolvedTarget target = validate_and_resolve(current, options.url_policy);
            HttpResponse response = get_once(target, options);
            if (is_redirect(response.status)) {
                if (redirects >= options.max_redirects) {
                    throw ExternalHttpError("external redirect limit exceeded [url: " +
                                            redact_external_url(current) + "]");
                }
                current = resolve_redirect(target.url, response.location);
                continue;
            }
            if (response.status < 200 || response.status >= 300) {
                throw ExternalHttpError("external GET returned HTTP " +
                                        std::to_string(response.status) +
                                        " [url: " + redact_external_url(current) + "]");
            }
            std::string encoding = ascii_lower(trim_ascii(response.content_encoding));
            if (const size_t parameter = encoding.find(';'); parameter != std::string::npos) {
                encoding = trim_ascii(encoding.substr(0, parameter));
            }
            std::string decoded;
            if (encoding.empty() || encoding == "identity") {
                if (response.body.size() > static_cast<uint64_t>(options.max_decoded_bytes)) {
                    throw ExternalHttpError("external decoded response exceeds max_decoded_bytes");
                }
                decoded = std::move(response.body);
            } else if (encoding == kZstd) {
                decoded = decode_zstd_bounded(response.body, options.max_decoded_bytes);
            } else {
                throw ExternalHttpError("external response uses an unsupported Content-Encoding");
            }
            verify_checksum(decoded, checksum);
            return decoded;
        }
    }

    ClientExternalHttpOptions options;
};

ClientExternalHttp::ClientExternalHttp(const ClientExternalHttpOptions& options)
    : impl_(std::make_unique<Impl>(options)) {}
ClientExternalHttp::~ClientExternalHttp() = default;
ClientExternalHttp::ClientExternalHttp(ClientExternalHttp&&) noexcept = default;
ClientExternalHttp& ClientExternalHttp::operator=(ClientExternalHttp&&) noexcept = default;

std::string ClientExternalHttp::fetch(const std::string& url,
                                      const std::string& expected_sha256) const {
    return impl_->fetch(url, expected_sha256);
}

void ClientExternalHttp::validate_url(const std::string& url) const {
    (void)validate_and_resolve(url, impl_->options.url_policy);
}

AnnotatedBatch ClientExternalHttp::resolve_pointer(const AnnotatedBatch& pointer,
                                                   ExternalLogHandler on_log) const {
    if (!pointer.custom_metadata || pointer.custom_metadata->FindKey(keys::LOCATION) < 0) {
        return pointer;
    }
    if (!pointer.batch || pointer.batch->num_rows() != 0 ||
        pointer.custom_metadata->FindKey(keys::LOG_LEVEL) >= 0) {
        throw ExternalHttpError("malformed ExternalLocation pointer batch");
    }
    const std::string url = get_metadata_value(pointer.custom_metadata, keys::LOCATION);
    const std::string checksum = get_metadata_value(pointer.custom_metadata, keys::LOCATION_SHA256);
    const auto started = std::chrono::steady_clock::now();
    const std::string payload = impl_->fetch(url, checksum);
    auto input = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(payload));
    const auto contents = read_ipc_stream(input);
    if (!contents) throw ExternalHttpError("external payload contains no Arrow IPC stream");

    std::vector<AnnotatedBatch> data;
    for (const auto& batch : contents->batches) {
        if (batch.custom_metadata && batch.custom_metadata->FindKey(keys::LOCATION) >= 0) {
            throw ExternalHttpError("nested ExternalLocation pointer payload is forbidden");
        }
        const bool log = batch.batch && batch.batch->num_rows() == 0 && batch.custom_metadata &&
                         batch.custom_metadata->FindKey(keys::LOG_LEVEL) >= 0 &&
                         batch.custom_metadata->FindKey(keys::LOG_MESSAGE) >= 0;
        if (log) {
            if (get_metadata_value(batch.custom_metadata, keys::LOG_LEVEL) ==
                log_level_to_string(LogLevel::EXCEPTION)) {
                throw ExternalHttpError(
                    "external payload contains a remote exception: " +
                    get_metadata_value(batch.custom_metadata, keys::LOG_MESSAGE));
            }
            if (on_log) on_log(batch);
        } else {
            data.push_back(batch);
        }
    }
    if (data.size() != 1) {
        throw ExternalHttpError("external payload must contain exactly one data batch");
    }
    if (!contents->schema->Equals(*pointer.batch->schema(), /*check_metadata=*/true) ||
        !data[0].batch->schema()->Equals(*pointer.batch->schema(), /*check_metadata=*/true)) {
        throw ExternalHttpError("external payload Arrow schema does not match its pointer");
    }
    const double elapsed =
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started)
            .count();
    data[0].custom_metadata = merge_resolution_metadata(
        pointer.custom_metadata, data[0].custom_metadata, redact_external_url(url), elapsed);
    return std::move(data[0]);
}

void ClientExternalHttp::put(const std::string& url, const std::string& body,
                             const std::string& content_type) const {
    if (body.size() > static_cast<uint64_t>(impl_->options.max_upload_bytes)) {
        throw ExternalHttpError("external upload exceeds max_upload_bytes");
    }
    if (content_type.empty() || content_type.find_first_of("\r\n") != std::string::npos) {
        throw std::invalid_argument("external upload Content-Type is invalid");
    }
    const ResolvedTarget target = validate_and_resolve(url, impl_->options.url_policy);
    httplib::Client client(target.url.origin());
    if (!client.is_valid())
        throw ExternalHttpError("external HTTP client could not be initialized");
    client.set_hostname_addr_map({{target.url.host, target.pinned_address}});
    set_timeouts(client, impl_->options);
    httplib::Headers headers{{"Accept-Encoding", "identity"}};
    std::string response_body;
    bool cap_exceeded = false;
    auto response = client.Put(
        target.url.path_and_query, headers, body, content_type, [&](const char* data, size_t size) {
            if (size > static_cast<uint64_t>(impl_->options.max_upload_response_bytes) ||
                response_body.size() >
                    static_cast<uint64_t>(impl_->options.max_upload_response_bytes) - size) {
                cap_exceeded = true;
                return false;
            }
            response_body.append(data, size);
            return true;
        });
    if (cap_exceeded) throw ExternalHttpError("external upload response exceeds its byte cap");
    if (!response) {
        throw ExternalHttpError("external PUT failed [url: " + redact_external_url(url) + "]");
    }
    if (is_redirect(response->status)) {
        throw ExternalHttpError("external PUT redirect refused [url: " + redact_external_url(url) +
                                "]");
    }
    if (response->status < 200 || response->status >= 300) {
        throw ExternalHttpError("external PUT returned HTTP " + std::to_string(response->status) +
                                " [url: " + redact_external_url(url) + "]");
    }
}

}  // namespace vgi_rpc
