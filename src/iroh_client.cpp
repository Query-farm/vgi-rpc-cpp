// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client.h"

#include <cctype>
#include <stdexcept>

namespace vgi_rpc {

std::string IrohStatusDetail::ToString() const {
    return "stage=" + std::to_string(static_cast<uint32_t>(stage_)) +
           ", category=" + std::to_string(static_cast<uint32_t>(category_)) +
           ", dispatch_certainty=" +
           std::to_string(static_cast<uint32_t>(dispatch_certainty_)) +
           (message_.empty() ? std::string{} : ", message=" + message_);
}
namespace {

uint8_t hex_nibble(char value) {
    if (value >= '0' && value <= '9') return static_cast<uint8_t>(value - '0');
    if (value >= 'a' && value <= 'f') return static_cast<uint8_t>(value - 'a' + 10);
    throw IrohTransportError(
        "Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters",
        IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
        IrohDispatchCertainty::NOT_SENT);
}

void validate_path(const std::string& path) {
    if (path.size() > 1 && path.back() == '/') {
        throw IrohTransportError("httpi:// base paths cannot have a trailing empty segment",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    if (path.find("//") != std::string::npos) {
        throw IrohTransportError("httpi:// base paths cannot contain empty segments",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    size_t start = 1;
    while (start <= path.size()) {
        const size_t end = path.find('/', start);
        const std::string segment = path.substr(start, end - start);
        if (segment == "." || segment == "..") {
            throw IrohTransportError("httpi:// base paths cannot contain dot segments",
                                     IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                     IrohDispatchCertainty::NOT_SENT);
        }
        if (end == std::string::npos) break;
        start = end + 1;
    }
    for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] == '%' &&
            (i + 2 >= path.size() || !std::isxdigit(static_cast<unsigned char>(path[i + 1])) ||
             !std::isxdigit(static_cast<unsigned char>(path[i + 2])))) {
            throw IrohTransportError("httpi:// base path contains an invalid percent escape",
                                     IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                     IrohDispatchCertainty::NOT_SENT);
        }
        if (path[i] == '%') {
            const auto decoded = static_cast<uint8_t>((hex_nibble(path[i + 1]) << 4) |
                                                       hex_nibble(path[i + 2]));
            if (decoded == '.' || decoded == '/' || decoded == '\\' || decoded <= 0x20 ||
                decoded == 0x7f) {
                throw IrohTransportError(
                    "httpi:// base path contains an encoded dot, separator, or control",
                    IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                    IrohDispatchCertainty::NOT_SENT);
            }
            i += 2;
        }
    }
}

}  // namespace

IrohEndpoint IrohEndpoint::parse(const std::string& uri) {
    for (unsigned char value : uri) {
        if (value <= 0x20 || value == 0x7f || value == '\\' || value == '?' || value == '#') {
            throw IrohTransportError("invalid VGI Iroh endpoint URI", IrohErrorStage::PARSE,
                                     IrohErrorCategory::INVALID_INPUT,
                                     IrohDispatchCertainty::NOT_SENT);
        }
    }
    Scheme scheme;
    size_t prefix;
    if (uri.rfind("iroh://", 0) == 0) {
        scheme = Scheme::IROH;
        prefix = 7;
    } else if (uri.rfind("httpi://", 0) == 0) {
        scheme = Scheme::HTTPI;
        prefix = 8;
    } else {
        throw IrohTransportError("VGI Iroh endpoint scheme must be iroh:// or httpi://",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    const size_t slash = uri.find('/', prefix);
    const std::string id = uri.substr(prefix, slash == std::string::npos ? slash : slash - prefix);
    if (id.size() != 64) {
        throw IrohTransportError(
            "Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters",
            IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
            IrohDispatchCertainty::NOT_SENT);
    }
    std::array<uint8_t, 32> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<uint8_t>((hex_nibble(id[i * 2]) << 4) | hex_nibble(id[i * 2 + 1]));
    }
    std::string path = slash == std::string::npos ? "" : uri.substr(slash);
    if (scheme == Scheme::IROH && !path.empty()) {
        throw IrohTransportError("iroh:// endpoints cannot contain a path",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    validate_path(path);
    if (path == "/") path.clear();
    return IrohEndpoint{scheme, id, bytes, path,
                        scheme == Scheme::IROH ? IROH_ARROW_MUX_ALPN : IROH_HTTP_ALPN};
}

ClientTransport connect_iroh_transport(const std::string& raw_endpoint,
                                       const IrohTransportProvider& provider,
                                       const IrohTransportOptions& options) {
    if (!provider) {
        throw IrohTransportError("an Iroh native transport provider is required",
                                 IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    const auto endpoint = IrohEndpoint::parse(raw_endpoint);
    if (endpoint.scheme != IrohEndpoint::Scheme::IROH) {
        throw IrohTransportError(
            "raw RpcClient requires iroh://; httpi:// requires an iroh-http/2 client",
            IrohErrorStage::BIND, IrohErrorCategory::UNSUPPORTED,
            IrohDispatchCertainty::NOT_SENT);
    }
    if (options.no_relay && !options.relay_urls.empty()) {
        throw IrohTransportError("no_relay and relay_urls are mutually exclusive",
                                 IrohErrorStage::PARSE, IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    if (options.connect_timeout <= std::chrono::milliseconds::zero() ||
        options.io_timeout <= std::chrono::milliseconds::zero()) {
        throw IrohTransportError("Iroh timeouts must be positive", IrohErrorStage::PARSE,
                                 IrohErrorCategory::INVALID_INPUT,
                                 IrohDispatchCertainty::NOT_SENT);
    }
    return provider(endpoint, options);
}

RpcClient RpcClient::connect_iroh(const std::string& endpoint,
                                  const IrohTransportProvider& provider,
                                  const RpcClientOptions& options,
                                  const IrohTransportOptions& transport_options) {
    return RpcClient(connect_iroh_transport(endpoint, provider, transport_options), options);
}

}  // namespace vgi_rpc
