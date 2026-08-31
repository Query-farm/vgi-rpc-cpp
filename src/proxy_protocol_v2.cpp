// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/proxy_protocol_v2.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <string_view>

#ifndef _WIN32
#include <arpa/inet.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace vgi_rpc {
namespace {

constexpr std::array<uint8_t, 12> kSignature = {0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d,
                                                0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a};
constexpr size_t kFixedBytes = 16;
constexpr uint8_t kVersion2 = 0x20;
constexpr uint8_t kProxyCommand = 0x01;
constexpr uint8_t kTcpIpv4 = 0x11;
constexpr uint8_t kTcpIpv6 = 0x21;

uint16_t network_u16(const uint8_t* data) {
    return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8) | data[1]);
}

std::string numeric_address(int family, const uint8_t* bytes) {
    std::array<char, INET6_ADDRSTRLEN> buffer{};
    if (::inet_ntop(family, bytes, buffer.data(), static_cast<socklen_t>(buffer.size())) == nullptr)
        throw ProxyProtocolV2Error("PROXY v2 contains an invalid IP address");
    std::string value(buffer.data());
    constexpr std::string_view mapped = "::ffff:";
    if (value.size() > mapped.size() &&
        std::equal(mapped.begin(), mapped.end(), value.begin(), [](char left, char right) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(left))) ==
                   static_cast<char>(std::tolower(static_cast<unsigned char>(right)));
        }))
        value.erase(0, mapped.size());
    return value;
}

void validate_tlvs(std::span<const uint8_t> body, size_t address_bytes) {
    size_t offset = address_bytes;
    while (offset < body.size()) {
        if (body.size() - offset < 3)
            throw ProxyProtocolV2Error("PROXY v2 contains a truncated TLV header");
        const size_t length = network_u16(body.data() + offset + 1);
        offset += 3;
        if (length > body.size() - offset)
            throw ProxyProtocolV2Error("PROXY v2 contains a truncated TLV value");
        offset += length;
    }
}

}  // namespace

size_t proxy_protocol_v2_size(std::span<const uint8_t> prefix, size_t maximum_bytes) {
    if (prefix.size() != kFixedBytes)
        throw ProxyProtocolV2Error("PROXY v2 fixed preamble must be exactly 16 bytes");
    if (!std::equal(kSignature.begin(), kSignature.end(), prefix.begin()))
        throw ProxyProtocolV2Error("missing PROXY v2 signature");
    if ((prefix[12] & 0xf0) != kVersion2)
        throw ProxyProtocolV2Error("unsupported PROXY protocol version");
    const size_t total = kFixedBytes + network_u16(prefix.data() + 14);
    if (total > maximum_bytes)
        throw ProxyProtocolV2Error("PROXY v2 preamble exceeds configured limit");
    return total;
}

ProxyProtocolV2Address parse_proxy_protocol_v2(std::span<const uint8_t> preamble,
                                               size_t maximum_bytes) {
    if (preamble.size() < kFixedBytes)
        throw ProxyProtocolV2Error("truncated PROXY v2 fixed preamble");
    const size_t expected = proxy_protocol_v2_size(preamble.first(kFixedBytes), maximum_bytes);
    if (preamble.size() != expected)
        throw ProxyProtocolV2Error("truncated or overlong PROXY v2 preamble");
    if ((preamble[12] & 0x0f) != kProxyCommand)
        throw ProxyProtocolV2Error("PROXY v2 LOCAL command is not accepted");

    const auto body = preamble.subspan(kFixedBytes);
    ProxyProtocolV2Address result;
    size_t address_bytes = 0;
    if (preamble[13] == kTcpIpv4) {
        address_bytes = 12;
        if (body.size() < address_bytes)
            throw ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv4 address block");
        result.source_address = numeric_address(AF_INET, body.data());
        result.destination_address = numeric_address(AF_INET, body.data() + 4);
        result.source_port = network_u16(body.data() + 8);
        result.destination_port = network_u16(body.data() + 10);
    } else if (preamble[13] == kTcpIpv6) {
        address_bytes = 36;
        if (body.size() < address_bytes)
            throw ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv6 address block");
        result.source_address = numeric_address(AF_INET6, body.data());
        result.destination_address = numeric_address(AF_INET6, body.data() + 16);
        result.source_port = network_u16(body.data() + 32);
        result.destination_port = network_u16(body.data() + 34);
    } else {
        throw ProxyProtocolV2Error("PROXY v2 requires TCP over IPv4 or IPv6");
    }
    validate_tlvs(body, address_bytes);
    return result;
}

}  // namespace vgi_rpc
