// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "http_socks5h.h"

#include "vgi_rpc/http_client.h"

#include <curl/curl.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>

namespace vgi_rpc {
namespace {

// Bound headers independently of the configured body limit. Response headers
// are attacker-controlled and libcurl delivers them before receive_body can
// enforce max_encoded_response_bytes.
constexpr size_t kMaximumResponseHeaderBytes = 64 * 1024;

struct CurlDeleter {
    void operator()(CURL* handle) const noexcept {
        if (handle) curl_easy_cleanup(handle);
    }
};

struct CurlHeadersDeleter {
    void operator()(curl_slist* headers) const noexcept {
        if (headers) curl_slist_free_all(headers);
    }
};

void ensure_curl_initialized() {
    static std::once_flag once;
    static CURLcode result = CURLE_FAILED_INIT;
    std::call_once(once, [] { result = curl_global_init(CURL_GLOBAL_DEFAULT); });
    if (result != CURLE_OK) {
        throw SocksHttpFailure(SocksHttpFailureKind::TRANSPORT,
                               "could not initialize the SOCKS5h HTTP transport");
    }
}

bool is_decimal_port(std::string_view value) {
    if (value.empty() || value.size() > 5) return false;
    unsigned port = 0;
    for (const char character : value) {
        if (character < '0' || character > '9') return false;
        port = port * 10 + static_cast<unsigned>(character - '0');
    }
    return port > 0 && port <= 65535;
}

std::string trim_header_value(std::string value) {
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n' || value.back() == ' ' ||
                              value.back() == '\t')) {
        value.pop_back();
    }
    const auto first = value.find_first_not_of(" \t");
    return first == std::string::npos ? std::string{} : value.substr(first);
}

struct TransferState {
    httplib::Response response;
    int64_t maximum_response_bytes = 0;
    int64_t maximum_encoded_response_bytes = 0;
    int64_t maximum_identity_response_bytes = 0;
    bool identity_limit_applied = false;
    bool response_too_large = false;
    size_t response_header_bytes = 0;
    bool response_headers_too_large = false;
    const CallOptions* options = nullptr;
};

size_t receive_body(char* data, size_t size, size_t count, void* opaque) {
    auto& state = *static_cast<TransferState*>(opaque);
    if (size != 0 && count > std::numeric_limits<size_t>::max() / size) return 0;
    const size_t bytes = size * count;
    const auto maximum = static_cast<uint64_t>(state.maximum_response_bytes);
    if (bytes > maximum || state.response.body.size() > maximum - bytes) {
        state.response_too_large = true;
        return 0;
    }
    state.response.body.append(data, bytes);
    return bytes;
}

size_t receive_header(char* data, size_t size, size_t count, void* opaque) {
    auto& state = *static_cast<TransferState*>(opaque);
    if (size != 0 && count > std::numeric_limits<size_t>::max() / size) return 0;
    const size_t bytes = size * count;
    if (bytes > kMaximumResponseHeaderBytes ||
        state.response_header_bytes > kMaximumResponseHeaderBytes - bytes) {
        state.response_headers_too_large = true;
        return 0;
    }
    state.response_header_bytes += bytes;
    std::string line(data, bytes);
    if (line.rfind("HTTP/", 0) == 0) {
        // A 100 Continue block precedes the final response. Only final headers
        // may influence authentication, capabilities, or response decoding.
        state.response.headers.clear();
        return bytes;
    }
    const auto colon = line.find(':');
    if (colon != std::string::npos) {
        const std::string name = line.substr(0, colon);
        const std::string value = trim_header_value(line.substr(colon + 1));
        if (!name.empty()) state.response.headers.emplace(name, value);
    } else if (line == "\r\n" || line == "\n") {
        std::string encoding =
            trim_header_value(state.response.get_header_value("Content-Encoding"));
        std::transform(encoding.begin(), encoding.end(), encoding.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (encoding.empty() || encoding == "identity") {
            int64_t identity_limit = state.maximum_identity_response_bytes;
            if (state.response.get_header_value_count("VGI-Max-Response-Bytes") == 1) {
                const std::string raw = state.response.get_header_value("VGI-Max-Response-Bytes");
                const bool digits = !raw.empty() && raw.front() != '0' &&
                                    std::all_of(raw.begin(), raw.end(), [](unsigned char ch) {
                                        return ch >= '0' && ch <= '9';
                                    });
                try {
                    size_t consumed = 0;
                    const int64_t parsed = digits ? std::stoll(raw, &consumed) : 0;
                    if (digits && consumed == raw.size() && parsed >= (64LL << 10) &&
                        parsed <= 9'007'199'254'740'991LL) {
                        identity_limit = std::min(identity_limit, parsed);
                    }
                } catch (const std::exception&) {
                    // The protocol layer reports malformed capability syntax.
                    // This callback only derives an early allocation ceiling.
                }
            }
            state.maximum_response_bytes =
                std::min(state.maximum_encoded_response_bytes, identity_limit);
            state.identity_limit_applied =
                state.maximum_response_bytes < state.maximum_encoded_response_bytes;
        }
    }
    return bytes;
}

int transfer_progress(void* opaque, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
    const auto& state = *static_cast<TransferState*>(opaque);
    if (!state.options) return 0;
    if (state.options->stop_token.stop_requested()) return 1;
    if (state.options->deadline && std::chrono::steady_clock::now() >= *state.options->deadline) {
        return 1;
    }
    return 0;
}

SocksHttpFailureKind failure_kind(CURLcode code, const CallOptions& options) {
    if (options.stop_token.stop_requested()) return SocksHttpFailureKind::CANCELLED;
    if (options.deadline && std::chrono::steady_clock::now() >= *options.deadline) {
        return SocksHttpFailureKind::TIMEOUT;
    }
    if (code == CURLE_OPERATION_TIMEDOUT) return SocksHttpFailureKind::TIMEOUT;
    switch (code) {
        case CURLE_SSL_CONNECT_ERROR:
        case CURLE_PEER_FAILED_VERIFICATION:
        case CURLE_SSL_CERTPROBLEM:
        case CURLE_SSL_CIPHER:
        case CURLE_SSL_CACERT_BADFILE: return SocksHttpFailureKind::TLS;
        default: return SocksHttpFailureKind::TRANSPORT;
    }
}

}  // namespace

void validate_socks5h_http_proxy_uri(const std::string& proxy_uri) {
    constexpr std::string_view scheme = "socks5h://";
    if (!proxy_uri.starts_with(scheme)) {
        throw std::invalid_argument("HTTP TCP proxy must use socks5h://");
    }
    const std::string_view authority(proxy_uri.data() + scheme.size(),
                                     proxy_uri.size() - scheme.size());
    if (authority.empty() || authority.find_first_of("/@?#") != std::string_view::npos) {
        throw std::invalid_argument(
            "HTTP SOCKS5h proxy must contain only a host and port without credentials");
    }
    std::string_view port;
    if (authority.front() == '[') {
        const auto closing = authority.find(']');
        if (closing == std::string_view::npos || closing + 1 >= authority.size() ||
            authority[closing + 1] != ':' || authority.find('[', 1) != std::string_view::npos) {
            throw std::invalid_argument("invalid bracketed IPv6 HTTP SOCKS5h proxy");
        }
        port = authority.substr(closing + 2);
    } else {
        const auto colon = authority.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || authority.find(':') != colon) {
            throw std::invalid_argument("HTTP SOCKS5h proxy must be socks5h://host:port");
        }
        port = authority.substr(colon + 1);
    }
    if (!is_decimal_port(port)) {
        throw std::invalid_argument("HTTP SOCKS5h proxy port is invalid");
    }
}

SocksHttpClient::SocksHttpClient(std::string base_url, std::string proxy_uri,
                                 int connect_timeout_seconds, int read_timeout_seconds,
                                 int write_timeout_seconds, const TlsOptions& tls_options)
    : base_url_(std::move(base_url)),
      proxy_uri_(std::move(proxy_uri)),
      connect_timeout_seconds_(connect_timeout_seconds),
      read_timeout_seconds_(read_timeout_seconds),
      write_timeout_seconds_(write_timeout_seconds),
      ca_file_(tls_options.ca_file),
      client_certificate_file_(tls_options.client_certificate_file),
      client_private_key_file_(tls_options.client_private_key_file),
      insecure_skip_verification_for_testing_(tls_options.insecure_skip_verification_for_testing) {
    validate_socks5h_http_proxy_uri(proxy_uri_);
    ensure_curl_initialized();
}

httplib::Response SocksHttpClient::send(const std::string& method, const std::string& path,
                                        const httplib::Headers& headers, const std::string& body,
                                        int64_t maximum_encoded_response_bytes,
                                        int64_t maximum_identity_response_bytes,
                                        const CallOptions& options) const {
    ensure_curl_initialized();
    std::unique_ptr<CURL, CurlDeleter> handle(curl_easy_init());
    if (!handle) {
        throw SocksHttpFailure(SocksHttpFailureKind::TRANSPORT,
                               "could not create a SOCKS5h HTTP request");
    }
    curl_slist* raw_headers = nullptr;
    for (const auto& [name, value] : headers) {
        curl_slist* next = curl_slist_append(raw_headers, (name + ": " + value).c_str());
        if (!next) {
            curl_slist_free_all(raw_headers);
            throw std::bad_alloc();
        }
        raw_headers = next;
    }
    std::unique_ptr<curl_slist, CurlHeadersDeleter> owned_headers(raw_headers);

    TransferState state;
    state.maximum_response_bytes = maximum_encoded_response_bytes;
    state.maximum_encoded_response_bytes = maximum_encoded_response_bytes;
    state.maximum_identity_response_bytes = maximum_identity_response_bytes;
    state.options = &options;
    const std::string url = base_url_ + path;
    curl_easy_setopt(handle.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_PROXY, proxy_uri_.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_PROXYTYPE, CURLPROXY_SOCKS5_HOSTNAME);
    curl_easy_setopt(handle.get(), CURLOPT_PROXYAUTH, CURLAUTH_NONE);
    curl_easy_setopt(handle.get(), CURLOPT_SOCKS5_AUTH, CURLAUTH_NONE);
    curl_easy_setopt(handle.get(), CURLOPT_NOPROXY, "");
    curl_easy_setopt(handle.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(handle.get(), CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(handle.get(), CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(handle.get(), CURLOPT_HTTPHEADER, owned_headers.get());
    curl_easy_setopt(handle.get(), CURLOPT_CUSTOMREQUEST, method.c_str());
    if (!body.empty() || method == "POST") {
        curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDS, body.data());
        curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDSIZE_LARGE,
                         static_cast<curl_off_t>(body.size()));
    }
    curl_easy_setopt(handle.get(), CURLOPT_CONNECTTIMEOUT_MS,
                     static_cast<long>(connect_timeout_seconds_) * 1000L);
    int inactivity_timeout = 0;
    if (read_timeout_seconds_ > 0 && write_timeout_seconds_ > 0) {
        inactivity_timeout = std::min(read_timeout_seconds_, write_timeout_seconds_);
    } else {
        inactivity_timeout = std::max(read_timeout_seconds_, write_timeout_seconds_);
    }
    if (inactivity_timeout > 0) {
        curl_easy_setopt(handle.get(), CURLOPT_LOW_SPEED_LIMIT, 1L);
        curl_easy_setopt(handle.get(), CURLOPT_LOW_SPEED_TIME,
                         static_cast<long>(inactivity_timeout));
    }
    if (options.deadline) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            *options.deadline - std::chrono::steady_clock::now());
        curl_easy_setopt(handle.get(), CURLOPT_TIMEOUT_MS,
                         static_cast<long>(std::max<int64_t>(1, remaining.count())));
    }
    curl_easy_setopt(handle.get(), CURLOPT_WRITEFUNCTION, receive_body);
    curl_easy_setopt(handle.get(), CURLOPT_WRITEDATA, &state);
    curl_easy_setopt(handle.get(), CURLOPT_HEADERFUNCTION, receive_header);
    curl_easy_setopt(handle.get(), CURLOPT_HEADERDATA, &state);
    curl_easy_setopt(handle.get(), CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(handle.get(), CURLOPT_XFERINFOFUNCTION, transfer_progress);
    curl_easy_setopt(handle.get(), CURLOPT_XFERINFODATA, &state);
    curl_easy_setopt(handle.get(), CURLOPT_SSL_VERIFYPEER,
                     insecure_skip_verification_for_testing_ ? 0L : 1L);
    curl_easy_setopt(handle.get(), CURLOPT_SSL_VERIFYHOST,
                     insecure_skip_verification_for_testing_ ? 0L : 2L);
    if (!ca_file_.empty()) curl_easy_setopt(handle.get(), CURLOPT_CAINFO, ca_file_.c_str());
    if (!client_certificate_file_.empty()) {
        curl_easy_setopt(handle.get(), CURLOPT_SSLCERT, client_certificate_file_.c_str());
        curl_easy_setopt(handle.get(), CURLOPT_SSLKEY, client_private_key_file_.c_str());
    }

    const CURLcode result = curl_easy_perform(handle.get());
    if (state.response_headers_too_large) {
        throw SocksHttpFailure(SocksHttpFailureKind::LIMIT,
                               "HTTP response headers exceed the transport byte limit");
    }
    if (state.response_too_large) {
        throw SocksHttpFailure(SocksHttpFailureKind::LIMIT,
                               state.identity_limit_applied
                                   ? "identity HTTP response exceeds the decoded byte limit"
                                   : "encoded HTTP response exceeds the configured byte limit");
    }
    if (result != CURLE_OK) {
        throw SocksHttpFailure(
            failure_kind(result, options),
            std::string("SOCKS5h HTTP transport failed: ") + curl_easy_strerror(result));
    }
    long status = 0;
    curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &status);
    state.response.status = static_cast<int>(status);
    return std::move(state.response);
}

}  // namespace vgi_rpc
