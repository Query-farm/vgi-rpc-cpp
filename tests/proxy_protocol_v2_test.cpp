// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/proxy_protocol_v2.h"

#include <array>
#include <cstdint>
#include <vector>

using namespace vgi_rpc;

namespace {

std::vector<uint8_t> prefix(uint8_t command, uint8_t family, uint16_t body_bytes) {
    return {0x0d,
            0x0a,
            0x0d,
            0x0a,
            0x00,
            0x0d,
            0x0a,
            0x51,
            0x55,
            0x49,
            0x54,
            0x0a,
            static_cast<uint8_t>(0x20 | command),
            family,
            static_cast<uint8_t>(body_bytes >> 8),
            static_cast<uint8_t>(body_bytes)};
}

std::vector<uint8_t> ipv4_header() {
    auto value = prefix(0x01, 0x11, 12);
    const std::array<uint8_t, 12> address = {192, 0, 2, 7, 198, 51, 100, 9, 0x30, 0x39, 0x24, 0xb8};
    value.insert(value.end(), address.begin(), address.end());
    return value;
}

std::vector<uint8_t> ipv6_header() {
    auto value = prefix(0x01, 0x21, 36);
    const std::array<uint8_t, 36> address = {
        0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,    1,    0x20, 0x01,
        0x0d, 0xb8, 0,    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0x01, 0xbb, 0x24, 0xb8};
    value.insert(value.end(), address.begin(), address.end());
    return value;
}

}  // namespace

TEST_CASE("PROXY v2 parses TCP over IPv4 and IPv6", "[proxy-v2]") {
    const auto ipv4 = parse_proxy_protocol_v2(ipv4_header());
    REQUIRE(ipv4.source_address == "192.0.2.7");
    REQUIRE(ipv4.destination_address == "198.51.100.9");
    REQUIRE(ipv4.source_port == 12345);
    REQUIRE(ipv4.destination_port == 9400);

    const auto ipv6 = parse_proxy_protocol_v2(ipv6_header());
    REQUIRE(ipv6.source_address == "2001:db8::1");
    REQUIRE(ipv6.destination_address == "2001:db8::2");
    REQUIRE(ipv6.source_port == 443);
    REQUIRE(ipv6.destination_port == 9400);
}

TEST_CASE("PROXY v2 validates and ignores bounded unknown TLVs", "[proxy-v2]") {
    auto value = ipv4_header();
    value[15] = 18;  // 12 address bytes + type/length + 3-byte value.
    value.insert(value.end(), {0xee, 0x00, 0x03, 0xaa, 0xbb, 0xcc});
    REQUIRE(parse_proxy_protocol_v2(value).source_address == "192.0.2.7");

    value.pop_back();
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(value), ProxyProtocolV2Error);
}

TEST_CASE("PROXY v2 rejects unsafe commands families and sizes", "[proxy-v2]") {
    auto local = ipv4_header();
    local[12] = 0x20;
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(local), ProxyProtocolV2Error);

    auto udp = ipv4_header();
    udp[13] = 0x12;
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(udp), ProxyProtocolV2Error);

    auto unspec = prefix(0x01, 0x00, 0);
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(unspec), ProxyProtocolV2Error);

    auto truncated = ipv4_header();
    truncated.pop_back();
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(truncated), ProxyProtocolV2Error);

    auto overlong = ipv4_header();
    overlong.push_back(0);
    REQUIRE_THROWS_AS(parse_proxy_protocol_v2(overlong), ProxyProtocolV2Error);

    auto oversized = prefix(0x01, 0x11, 600);
    REQUIRE_THROWS_AS(proxy_protocol_v2_size(oversized), ProxyProtocolV2Error);
}
