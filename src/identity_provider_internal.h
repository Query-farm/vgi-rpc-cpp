// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

#include <nlohmann/json.hpp>

#include "vgi_rpc/identity.h"

namespace vgi_rpc::identity_internal {

inline bool valid_utf8(const std::string& value) {
    size_t i = 0;
    while (i < value.size()) {
        const auto first = static_cast<unsigned char>(value[i++]);
        if (first < 0x80) continue;
        size_t continuation = 0;
        uint32_t code = 0;
        if ((first & 0xe0) == 0xc0) {
            continuation = 1;
            code = first & 0x1f;
        } else if ((first & 0xf0) == 0xe0) {
            continuation = 2;
            code = first & 0x0f;
        } else if ((first & 0xf8) == 0xf0) {
            continuation = 3;
            code = first & 0x07;
        } else {
            return false;
        }
        if (i + continuation > value.size()) return false;
        for (size_t n = 0; n < continuation; ++n) {
            const auto byte = static_cast<unsigned char>(value[i++]);
            if ((byte & 0xc0) != 0x80) return false;
            code = (code << 6) | (byte & 0x3f);
        }
        if ((continuation == 1 && code < 0x80) || (continuation == 2 && code < 0x800) ||
            (continuation == 3 && code < 0x10000) || code > 0x10ffff ||
            (code >= 0xd800 && code <= 0xdfff))
            return false;
    }
    return true;
}

inline bool has_control(const std::string& value) {
    return std::any_of(value.begin(), value.end(),
                       [](unsigned char c) { return c < 0x20 || c == 0x7f; });
}

inline std::string lower_ascii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return char(std::tolower(c)); });
    return value;
}

struct IpAddress {
    int family = 0;
    std::array<unsigned char, 16> bytes{};

    bool operator<(const IpAddress& other) const noexcept {
        return family < other.family || (family == other.family && bytes < other.bytes);
    }
};

inline std::optional<IpAddress> parse_exact_ip(const std::string& text) {
    IpAddress result;
    in_addr v4{};
    if (inet_pton(AF_INET, text.c_str(), &v4) == 1) {
        result.family = AF_INET;
        std::copy_n(reinterpret_cast<const unsigned char*>(&v4), 4, result.bytes.begin());
        return result;
    }
    in6_addr v6{};
    if (inet_pton(AF_INET6, text.c_str(), &v6) != 1) return std::nullopt;
    const auto* raw = reinterpret_cast<const unsigned char*>(&v6);
    static constexpr unsigned char mapped_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (std::equal(std::begin(mapped_prefix), std::end(mapped_prefix), raw)) {
        result.family = AF_INET;
        std::copy_n(raw + 12, 4, result.bytes.begin());
    } else {
        result.family = AF_INET6;
        std::copy_n(raw, 16, result.bytes.begin());
    }
    return result;
}

inline bool valid_header_name(const std::string& value) {
    if (value.empty()) return false;
    static const std::string punctuation = "!#$%&'*+-.^_`|~";
    return std::all_of(value.begin(), value.end(), [&](unsigned char c) {
        return std::isalnum(c) || punctuation.find(char(c)) != std::string::npos;
    });
}

class BoundedJsonSax : public nlohmann::json_sax<nlohmann::json> {
public:
    BoundedJsonSax(size_t max_depth, size_t max_values)
        : max_depth_(max_depth), max_values_(max_values) {}

    bool null() override { return value(); }
    bool boolean(bool) override { return value(); }
    bool number_integer(number_integer_t) override { return value(); }
    bool number_unsigned(number_unsigned_t) override { return value(); }
    bool number_float(number_float_t value, const string_t&) override {
        return std::isfinite(value) && this->value();
    }
    bool string(string_t& value) override { return valid_utf8(value) && this->value(); }
    bool binary(binary_t&) override { return false; }
    bool start_object(std::size_t) override {
        if (!value() || stack_.size() + 1 > max_depth_) return false;
        stack_.emplace_back(std::in_place);
        return true;
    }
    bool key(string_t& value) override {
        return valid_utf8(value) && !stack_.empty() && stack_.back() &&
               stack_.back()->insert(value).second;
    }
    bool end_object() override {
        if (stack_.empty()) return false;
        stack_.pop_back();
        return true;
    }
    bool start_array(std::size_t) override {
        if (!value() || stack_.size() + 1 > max_depth_) return false;
        stack_.emplace_back(std::nullopt);
        return true;
    }
    bool end_array() override { return end_object(); }
    bool parse_error(std::size_t, const std::string&, const nlohmann::detail::exception&) override {
        return false;
    }

private:
    bool value() { return ++values_ <= max_values_; }
    size_t max_depth_, max_values_, values_ = 0;
    std::vector<std::optional<std::set<std::string>>> stack_;
};

inline nlohmann::json parse_bounded_json(const std::string& text, size_t max_bytes,
                                         size_t max_depth = 16, size_t max_values = 4'096) {
    if (text.size() > max_bytes || !valid_utf8(text))
        throw std::invalid_argument("invalid or oversized JSON");
    BoundedJsonSax validator(max_depth, max_values);
    if (!nlohmann::json::sax_parse(text, &validator, nlohmann::json::input_format_t::json, true))
        throw std::invalid_argument("invalid, duplicate-key, or over-complex JSON");
    return nlohmann::json::parse(text, nullptr, true, false);
}

inline PeerIdentityResult result(const std::string& provider, PeerIdentityStatus status) {
    PeerIdentityResult out{provider, status, {}};
    out.validate();
    return out;
}

}  // namespace vgi_rpc::identity_internal
